from __future__ import annotations

from collections import Counter
import re
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import (
    AnnouncementRawRecord,
    EventBatchIngestRequest,
    EventConnectorFailureRepairRequest,
    EventConnectorFailureRepairResult,
    EventConnectorFailureStatus,
    EventConnectorManualReplayItem,
    EventConnectorManualReplayRequest,
    EventConnectorManualReplayResult,
    EventConnectorOverviewResult,
    EventConnectorRepairReplayItemRequest,
    EventConnectorRepairReplayRequest,
    EventConnectorRepairReplayResult,
    EventConnectorRecord,
    EventConnectorRegisterRequest,
    EventConnectorReplayRequest,
    EventConnectorReplayResult,
    EventConnectorSLAAlertSyncResult,
    EventConnectorSLAAlertStateRecord,
    EventConnectorSLAAlertStateSummary,
    EventConnectorSLOHistory,
    EventConnectorSLOPoint,
    EventConnectorSLABreach,
    EventConnectorSLABreachType,
    EventConnectorSLAPolicy,
    EventConnectorSLAReport,
    EventConnectorSLAStatus,
    EventConnectorRunRecord,
    EventConnectorRunRequest,
    EventConnectorRunResult,
    EventConnectorRunStatus,
    EventConnectorSourceStateRecord,
    EventConnectorType,
    EventNormalizeIngestRequest,
    EventNormalizeIngestResult,
    EventNormalizePreviewRequest,
    EventNormalizePreviewResult,
    OpsEventStats,
    SignalLevel,
)
from trading_assistant.governance.announcement_connectors import build_announcement_connector
from trading_assistant.governance.event_connector_store import EventConnectorStore
from trading_assistant.governance.event_nlp import EventStandardizer
from trading_assistant.governance.event_service import EventService


class EventConnectorService:
    def __init__(
        self,
        *,
        event_service: EventService,
        connector_store: EventConnectorStore,
        standardizer: EventStandardizer | None = None,
        default_sla_policy: EventConnectorSLAPolicy | None = None,
    ) -> None:
        self.event_service = event_service
        self.store = connector_store
        self.standardizer = standardizer or EventStandardizer()
        self.default_sla_policy = default_sla_policy or EventConnectorSLAPolicy()

    def register_connector(self, req: EventConnectorRegisterRequest) -> int:
        source = self.event_service.store.get_source(req.source_name)
        if source is None:
            raise KeyError(f"event source '{req.source_name}' not found")
        row_id = self.store.register_connector(req)
        connector = self.store.get_connector(req.connector_name)
        if connector is not None:
            matrix = self._resolve_source_matrix(connector)
            self._sync_source_state_registry(connector=connector, source_matrix=matrix)
        return row_id

    def list_connectors(self, limit: int = 200, enabled_only: bool = False) -> list[EventConnectorRecord]:
        return self.store.list_connectors(limit=limit, enabled_only=enabled_only)

    def list_runs(self, connector_name: str | None = None, limit: int = 200) -> list[EventConnectorRunRecord]:
        return self.store.list_runs(connector_name=connector_name, limit=limit)

    def list_source_states(
        self,
        *,
        connector_name: str | None = None,
        limit: int = 500,
    ) -> list[EventConnectorSourceStateRecord]:
        return self.store.list_source_states(connector_name=connector_name, limit=limit)

    def list_failures(
        self,
        connector_name: str | None = None,
        status: EventConnectorFailureStatus | None = None,
        error_keyword: str | None = None,
        limit: int = 200,
    ):
        return self.store.list_failures(
            connector_name=connector_name,
            status=status,
            error_keyword=error_keyword,
            limit=limit,
        )

    def normalize_preview(self, req: EventNormalizePreviewRequest) -> EventNormalizePreviewResult:
        return self.standardizer.normalize_preview(req)

    def normalize_and_ingest(self, req: EventNormalizeIngestRequest) -> EventNormalizeIngestResult:
        preview = self.normalize_preview(req)
        if preview.errors and not req.allow_partial:
            raise ValueError(f"normalize failed with {len(preview.errors)} errors")

        ingest = None
        if preview.normalized:
            ingest = self.event_service.ingest(
                EventBatchIngestRequest(
                    source_name=req.source_name,
                    events=[item.event for item in preview.normalized],
                )
            )
        return EventNormalizeIngestResult(
            source_name=req.source_name,
            preview=preview,
            ingest=ingest,
        )

    def run_connector(self, req: EventConnectorRunRequest) -> EventConnectorRunResult:
        connector = self.store.get_connector(req.connector_name)
        if connector is None:
            raise KeyError(f"connector '{req.connector_name}' not found")
        source = self.event_service.store.get_source(connector.source_name)
        if source is None:
            raise KeyError(f"event source '{connector.source_name}' not found")

        checkpoint = self.store.get_checkpoint(connector.connector_name)
        checkpoint_before = checkpoint.checkpoint_cursor if checkpoint else None
        source_matrix = self._resolve_source_matrix(connector)
        self._sync_source_state_registry(connector=connector, source_matrix=source_matrix)
        source_state_index = {
            x.source_key: x for x in self.store.list_source_states(connector_name=connector.connector_name, limit=500)
        }
        candidate_index = {item["source_key"]: item for item in source_matrix}
        ordered_states, failover_enabled, max_candidates = self._order_source_states_for_run(
            connector=connector,
            states=[source_state_index[item["source_key"]] for item in source_matrix if item["source_key"] in source_state_index],
        )

        now = datetime.now(timezone.utc)
        run = EventConnectorRunRecord(
            run_id=uuid4().hex,
            connector_name=connector.connector_name,
            source_name=connector.source_name,
            started_at=now,
            status=EventConnectorRunStatus.RUNNING,
            triggered_by=req.triggered_by,
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_before,
            details={
                "enabled": connector.enabled,
                "dry_run": req.dry_run,
                "force_full_sync": req.force_full_sync,
                "failover_enabled": failover_enabled,
                "source_matrix_count": len(source_matrix),
                "source_attempts": [],
            },
        )
        self.store.create_run(run)

        errors: list[str] = []
        next_retry_at = now + timedelta(seconds=connector.replay_backoff_seconds)
        fail_payloads: list[dict] = []
        fetched = None
        selected_state: EventConnectorSourceStateRecord | None = None
        selected_candidate: dict | None = None

        try:
            fetch_limit = req.fetch_limit_override or connector.fetch_limit
            run.details["fetch_limit"] = fetch_limit

            picked_states = ordered_states[:max(1, max_candidates)]
            for state in picked_states:
                candidate = candidate_index.get(state.source_key)
                if candidate is None:
                    continue
                source_cfg = dict(candidate["config"]) if isinstance(candidate.get("config"), dict) else {}
                global_cfg = connector.config if isinstance(connector.config, dict) else {}
                budget_raw = source_cfg.get("request_budget_per_hour", global_cfg.get("request_budget_per_hour"))
                try:
                    budget_per_hour = int(budget_raw) if budget_raw is not None else None
                except Exception:  # noqa: BLE001
                    budget_per_hour = None
                budget_allowed, budget_used, budget_limit, budget_window = self.store.try_consume_source_budget(
                    connector_name=connector.connector_name,
                    source_key=state.source_key,
                    budget_per_hour=budget_per_hour,
                    as_of=datetime.now(timezone.utc),
                )
                if not budget_allowed:
                    budget_msg = (
                        f"source={state.source_key}: budget exceeded "
                        f"{budget_used}/{budget_limit} (window={budget_window})"
                    )
                    errors.append(budget_msg)
                    run.details["source_attempts"].append(
                        {
                            "source_key": state.source_key,
                            "connector_type": candidate["connector_type"].value,
                            "status": "SKIPPED_BUDGET",
                            "budget_used": budget_used,
                            "budget_limit": budget_limit,
                            "budget_window": budget_window,
                            "error": budget_msg,
                        }
                    )
                    if not failover_enabled:
                        break
                    continue

                credential_alias = None
                raw_aliases = source_cfg.get("credential_aliases", global_cfg.get("credential_aliases", []))
                if not isinstance(raw_aliases, list):
                    raw_aliases = []
                credential_alias = self.store.next_source_credential_alias(
                    connector_name=connector.connector_name,
                    source_key=state.source_key,
                    aliases=[str(x) for x in raw_aliases],
                )
                raw_credential_map = source_cfg.get("credentials", global_cfg.get("credentials", {}))
                if (
                    credential_alias
                    and isinstance(raw_credential_map, dict)
                    and isinstance(raw_credential_map.get(credential_alias), dict)
                ):
                    source_cfg.update(dict(raw_credential_map[credential_alias]))
                source_cfg.pop("credential_aliases", None)
                source_cfg.pop("credentials", None)
                source_cfg.pop("request_budget_per_hour", None)

                cursor = None if req.force_full_sync else (state.checkpoint_cursor or checkpoint_before)
                fetch_started = datetime.now(timezone.utc)
                try:
                    ann_connector = build_announcement_connector(candidate["connector_type"], source_cfg)
                    candidate_fetched = ann_connector.fetch(cursor=cursor, limit=fetch_limit)
                    latency_ms = int((datetime.now(timezone.utc) - fetch_started).total_seconds() * 1000)
                    updated_state = self.store.mark_source_attempt_success(
                        connector_name=connector.connector_name,
                        source_key=state.source_key,
                        checkpoint_cursor=candidate_fetched.next_cursor or cursor,
                        checkpoint_publish_time=candidate_fetched.checkpoint_publish_time,
                        latency_ms=latency_ms,
                    )
                    selected_state = updated_state or state
                    selected_candidate = candidate
                    fetched = candidate_fetched
                    run.checkpoint_before = cursor
                    run.checkpoint_after = candidate_fetched.next_cursor or cursor
                    run.details["source_attempts"].append(
                        {
                            "source_key": state.source_key,
                            "connector_type": candidate["connector_type"].value,
                            "status": "SUCCESS",
                            "latency_ms": latency_ms,
                            "checkpoint_before": cursor,
                            "checkpoint_after": run.checkpoint_after,
                            "credential_alias": credential_alias,
                            "budget_used": budget_used,
                            "budget_limit": budget_limit,
                            "budget_window": budget_window,
                        }
                    )
                    break
                except Exception as exc:  # noqa: BLE001
                    latency_ms = int((datetime.now(timezone.utc) - fetch_started).total_seconds() * 1000)
                    _ = self.store.mark_source_attempt_failure(
                        connector_name=connector.connector_name,
                        source_key=state.source_key,
                        error_message=str(exc),
                        latency_ms=latency_ms,
                    )
                    err = f"source={state.source_key}: fetch failed: {exc}"
                    errors.append(err)
                    run.details["source_attempts"].append(
                        {
                            "source_key": state.source_key,
                            "connector_type": candidate["connector_type"].value,
                            "status": "FAILED",
                            "latency_ms": latency_ms,
                            "error": str(exc),
                            "credential_alias": credential_alias,
                            "budget_used": budget_used,
                            "budget_limit": budget_limit,
                            "budget_window": budget_window,
                        }
                    )
                    if not failover_enabled:
                        break

            if fetched is None:
                raise RuntimeError("all source matrix candidates failed")

            run.pulled_count = len(fetched.records)
            run.details["fetched"] = len(fetched.records)
            run.details["selected_source_key"] = selected_state.source_key if selected_state else None
            run.details["selected_connector_type"] = (
                selected_candidate["connector_type"].value if selected_candidate else None
            )

            normalized_events = []
            for idx, raw in enumerate(fetched.records):
                try:
                    event, nlp, warning = self.standardizer.normalize_record(
                        row=raw,
                        source_name=connector.source_name,
                        default_symbol=None,
                        default_timezone=source.timezone,
                        source_reliability_score=source.reliability_score,
                    )
                    normalized_events.append((idx, raw, event, nlp))
                    if warning:
                        errors.append(f"idx={idx}: {warning}")
                except Exception as exc:  # noqa: BLE001
                    run.failed_count += 1
                    err = f"idx={idx}: normalize failed: {exc}"
                    errors.append(err)
                    fail_payloads.append(
                        {
                            "phase": "normalize",
                            "source_key": selected_state.source_key if selected_state else None,
                            "error": str(exc),
                            "raw_record": raw.model_dump(mode="json"),
                        }
                    )
            run.normalized_count = len(normalized_events)

            if req.dry_run:
                run.status = EventConnectorRunStatus.DRY_RUN
            else:
                if normalized_events:
                    ingest = self.event_service.ingest(
                        EventBatchIngestRequest(
                            source_name=connector.source_name,
                            events=[item[2] for item in normalized_events],
                        )
                    )
                    run.inserted_count = ingest.inserted
                    run.updated_count = ingest.updated
                    run.failed_count += len(ingest.errors)
                    for ingest_error in ingest.errors:
                        errors.append(ingest_error)
                        idx = self._extract_error_index(ingest_error)
                        payload = {
                            "phase": "ingest",
                            "source_key": selected_state.source_key if selected_state else None,
                            "error": ingest_error,
                        }
                        if idx is not None and 0 <= idx < len(normalized_events):
                            raw = normalized_events[idx][1]
                            event = normalized_events[idx][2]
                            payload["raw_record"] = raw.model_dump(mode="json")
                            payload["event"] = event.model_dump(mode="json")
                        fail_payloads.append(payload)

                if run.failed_count == 0:
                    run.status = EventConnectorRunStatus.SUCCESS
                elif run.inserted_count + run.updated_count == 0:
                    run.status = EventConnectorRunStatus.FAILED
                else:
                    run.status = EventConnectorRunStatus.PARTIAL

            if not req.dry_run:
                self.store.update_checkpoint(
                    connector_name=connector.connector_name,
                    checkpoint_cursor=run.checkpoint_after,
                    checkpoint_publish_time=fetched.checkpoint_publish_time,
                    mark_run_at=now,
                    mark_success_at=now if run.status in {EventConnectorRunStatus.SUCCESS, EventConnectorRunStatus.PARTIAL} else None,
                )
                if fail_payloads:
                    _ = self.store.append_failures(
                        connector_name=connector.connector_name,
                        source_name=connector.source_name,
                        run_id=run.run_id,
                        payloads=fail_payloads,
                        error_message="connector run failure",
                        next_retry_at=next_retry_at,
                    )
        except Exception as exc:  # noqa: BLE001
            run.status = EventConnectorRunStatus.FAILED
            run.error_message = str(exc)
            errors.append(str(exc))
            if not req.dry_run:
                self.store.update_checkpoint(
                    connector_name=connector.connector_name,
                    checkpoint_cursor=checkpoint_before,
                    checkpoint_publish_time=checkpoint.checkpoint_publish_time if checkpoint else None,
                    mark_run_at=now,
                )
        finally:
            run.finished_at = datetime.now(timezone.utc)
            run.details["errors"] = len(errors)
            if run.status == EventConnectorRunStatus.RUNNING:
                run.status = EventConnectorRunStatus.FAILED
                run.error_message = run.error_message or "connector run did not finish correctly"
            self.store.update_run(run)

        return EventConnectorRunResult(run=run, errors=errors)

    def replay_failures(self, req: EventConnectorReplayRequest) -> EventConnectorReplayResult:
        connector = self.store.get_connector(req.connector_name)
        if connector is None:
            raise KeyError(f"connector '{req.connector_name}' not found")
        source = self.event_service.store.get_source(connector.source_name)
        if source is None:
            raise KeyError(f"event source '{connector.source_name}' not found")

        now = datetime.now(timezone.utc)
        checkpoint = self.store.get_checkpoint(connector.connector_name)
        run = EventConnectorRunRecord(
            run_id=uuid4().hex,
            connector_name=connector.connector_name,
            source_name=connector.source_name,
            started_at=now,
            status=EventConnectorRunStatus.RUNNING,
            triggered_by=req.triggered_by,
            checkpoint_before=checkpoint.checkpoint_cursor if checkpoint else None,
            checkpoint_after=checkpoint.checkpoint_cursor if checkpoint else None,
            details={"mode": "replay"},
        )
        self.store.create_run(run)

        failures = self.store.claim_pending_failures(
            connector_name=connector.connector_name,
            limit=req.limit,
            max_retry=connector.max_retry,
            as_of=now,
        )
        replayed, failed, dead, errors, _items = self._replay_failure_rows(
            connector=connector,
            source_timezone=source.timezone,
            source_reliability_score=source.reliability_score,
            failures=failures,
            now=now,
        )

        run.replayed_count = replayed
        run.failed_count = failed
        if failed == 0:
            run.status = EventConnectorRunStatus.SUCCESS
        elif replayed > 0:
            run.status = EventConnectorRunStatus.PARTIAL
        else:
            run.status = EventConnectorRunStatus.FAILED
        run.finished_at = datetime.now(timezone.utc)
        run.details["picked"] = len(failures)
        run.details["dead"] = dead
        run.details["errors"] = len(errors)
        run.error_message = errors[0] if errors else None
        self.store.update_run(run)

        return EventConnectorReplayResult(
            connector_name=req.connector_name,
            picked=len(failures),
            replayed=replayed,
            failed=failed,
            dead=dead,
            errors=errors,
        )

    def repair_failure(self, req: EventConnectorFailureRepairRequest) -> EventConnectorFailureRepairResult:
        connector = self.store.get_connector(req.connector_name)
        if connector is None:
            raise KeyError(f"connector '{req.connector_name}' not found")

        row = self.store.get_failure(req.failure_id)
        if row is None:
            raise KeyError(f"failure id '{req.failure_id}' not found")
        if row.connector_name != req.connector_name:
            raise ValueError(
                f"failure id '{req.failure_id}' belongs to connector '{row.connector_name}', not '{req.connector_name}'"
            )

        payload = dict(row.payload)
        if req.patch_raw_record:
            raw = payload.get("raw_record")
            if not isinstance(raw, dict):
                raw = {}
            raw.update(req.patch_raw_record)
            payload["raw_record"] = raw
        if req.patch_event:
            event = payload.get("event")
            if not isinstance(event, dict):
                event = {}
            event.update(req.patch_event)
            payload["event"] = event
        if req.note:
            payload["manual_note"] = req.note
            payload["manual_repair_by"] = req.triggered_by

        updated = self.store.update_failure_payload(
            req.failure_id,
            payload=payload,
            last_error=req.note or "manual payload repair",
            status=EventConnectorFailureStatus.PENDING,
            next_retry_at=datetime.now(timezone.utc),
            reset_retry_count=req.reset_retry_count,
        )
        refreshed = self.store.get_failure(req.failure_id)
        return EventConnectorFailureRepairResult(
            connector_name=req.connector_name,
            failure_id=req.failure_id,
            updated=updated,
            failure=refreshed,
        )

    def replay_selected_failures(self, req: EventConnectorManualReplayRequest) -> EventConnectorManualReplayResult:
        connector = self.store.get_connector(req.connector_name)
        if connector is None:
            raise KeyError(f"connector '{req.connector_name}' not found")
        source = self.event_service.store.get_source(connector.source_name)
        if source is None:
            raise KeyError(f"event source '{connector.source_name}' not found")

        now = datetime.now(timezone.utc)
        checkpoint = self.store.get_checkpoint(connector.connector_name)
        run = EventConnectorRunRecord(
            run_id=uuid4().hex,
            connector_name=connector.connector_name,
            source_name=connector.source_name,
            started_at=now,
            status=EventConnectorRunStatus.RUNNING,
            triggered_by=req.triggered_by,
            checkpoint_before=checkpoint.checkpoint_cursor if checkpoint else None,
            checkpoint_after=checkpoint.checkpoint_cursor if checkpoint else None,
            details={"mode": "manual_replay", "failure_ids": sorted(set(req.failure_ids))[:500]},
        )
        self.store.create_run(run)

        failures = self.store.claim_failures_by_ids(
            connector_name=connector.connector_name,
            failure_ids=req.failure_ids,
        )
        replayed, failed, dead, errors, items = self._replay_failure_rows(
            connector=connector,
            source_timezone=source.timezone,
            source_reliability_score=source.reliability_score,
            failures=failures,
            now=now,
        )

        run.replayed_count = replayed
        run.failed_count = failed
        if failed == 0:
            run.status = EventConnectorRunStatus.SUCCESS
        elif replayed > 0:
            run.status = EventConnectorRunStatus.PARTIAL
        else:
            run.status = EventConnectorRunStatus.FAILED
        run.finished_at = datetime.now(timezone.utc)
        run.details["picked"] = len(failures)
        run.details["dead"] = dead
        run.details["errors"] = len(errors)
        run.error_message = errors[0] if errors else None
        self.store.update_run(run)

        return EventConnectorManualReplayResult(
            connector_name=req.connector_name,
            picked=len(failures),
            replayed=replayed,
            failed=failed,
            dead=dead,
            items=items,
            errors=errors,
        )

    def repair_and_replay_failures(self, req: EventConnectorRepairReplayRequest) -> EventConnectorRepairReplayResult:
        repaired_ids: list[int] = []
        errors: list[str] = []

        for item in req.items:
            try:
                result = self.repair_failure(
                    EventConnectorFailureRepairRequest(
                        connector_name=req.connector_name,
                        failure_id=item.failure_id,
                        patch_raw_record=item.patch_raw_record,
                        patch_event=item.patch_event,
                        reset_retry_count=item.reset_retry_count,
                        triggered_by=req.triggered_by,
                        note=item.note,
                    )
                )
                if result.updated:
                    repaired_ids.append(item.failure_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"repair failure_id={item.failure_id} failed: {exc}")

        if not repaired_ids:
            return EventConnectorRepairReplayResult(
                connector_name=req.connector_name,
                repaired=0,
                picked=0,
                replayed=0,
                failed=0,
                dead=0,
                repaired_failure_ids=[],
                items=[],
                errors=errors,
            )

        replay = self.replay_selected_failures(
            EventConnectorManualReplayRequest(
                connector_name=req.connector_name,
                failure_ids=repaired_ids,
                triggered_by=req.triggered_by,
            )
        )
        return EventConnectorRepairReplayResult(
            connector_name=req.connector_name,
            repaired=len(repaired_ids),
            picked=replay.picked,
            replayed=replay.replayed,
            failed=replay.failed,
            dead=replay.dead,
            repaired_failure_ids=repaired_ids,
            items=replay.items,
            errors=[*errors, *replay.errors],
        )

    def overview(self, limit: int = 200) -> EventConnectorOverviewResult:
        return EventConnectorOverviewResult(
            generated_at=datetime.now(timezone.utc),
            connectors=self.store.connector_overview(limit=limit),
        )

    def coverage_summary(self, lookback_days: int = 30):
        return self.store.coverage_summary(lookback_days=lookback_days)

    def list_sla_alert_states(
        self,
        *,
        connector_name: str | None = None,
        open_only: bool = False,
        limit: int = 200,
    ) -> list[EventConnectorSLAAlertStateRecord]:
        return self.store.list_sla_alert_states(
            connector_name=connector_name,
            open_only=open_only,
            limit=limit,
        )

    def sla_alert_state_summary(self, *, connector_name: str | None = None) -> EventConnectorSLAAlertStateSummary:
        open_states = self.store.list_sla_alert_states(
            connector_name=connector_name,
            open_only=True,
            limit=5000,
        )
        severity_counter: Counter[str] = Counter()
        breach_counter: Counter[str] = Counter()
        escalation_counter: Counter[str] = Counter()
        for row in open_states:
            severity_counter[row.severity.value] += 1
            breach_counter[row.breach_type.value] += 1
            escalation_counter[str(int(row.escalation_level))] += 1

        escalated_open = sum(1 for x in open_states if x.escalation_level > 0)
        return EventConnectorSLAAlertStateSummary(
            generated_at=datetime.now(timezone.utc),
            connector_name=connector_name,
            open_states=len(open_states),
            escalated_open_states=escalated_open,
            open_by_severity=dict(sorted(severity_counter.items())),
            open_by_breach_type=dict(sorted(breach_counter.items())),
            open_by_escalation_level=dict(sorted(escalation_counter.items(), key=lambda kv: int(kv[0]))),
        )

    def evaluate_sla(self, now: datetime | None = None, *, include_disabled: bool = True) -> EventConnectorSLAReport:
        now_utc = now.astimezone(timezone.utc) if now and now.tzinfo else (now or datetime.now(timezone.utc))
        connectors = self.store.list_connectors(limit=5000, enabled_only=not include_disabled)

        statuses: list[EventConnectorSLAStatus] = []
        breaches: list[EventConnectorSLABreach] = []

        for connector in connectors:
            policy = self._resolve_policy(connector)
            pending = self.store.count_failures(
                connector_name=connector.connector_name,
                status=EventConnectorFailureStatus.PENDING,
            )
            dead = self.store.count_failures(
                connector_name=connector.connector_name,
                status=EventConnectorFailureStatus.DEAD,
            )
            checkpoint = self.store.get_checkpoint(connector.connector_name)
            latest_run = self.store.latest_run(connector.connector_name)

            freshness_ref = None
            if checkpoint is not None:
                freshness_ref = checkpoint.checkpoint_publish_time or checkpoint.last_success_at or checkpoint.last_run_at
            if freshness_ref is None and latest_run is not None:
                freshness_ref = latest_run.finished_at or latest_run.started_at

            freshness_minutes = None
            if freshness_ref is not None:
                freshness_minutes = max(0, int((now_utc - freshness_ref).total_seconds() // 60))

            connector_breach_types: list[EventConnectorSLABreachType] = []
            connector_max_sev = SignalLevel.INFO
            if connector.enabled:
                if freshness_minutes is None:
                    breach = EventConnectorSLABreach(
                        connector_name=connector.connector_name,
                        source_name=connector.source_name,
                        breach_type=EventConnectorSLABreachType.FRESHNESS,
                        severity=SignalLevel.WARNING,
                        stage="warning",
                        message="No freshness checkpoint found yet.",
                        freshness_minutes=None,
                        pending_failures=pending,
                        dead_failures=dead,
                        latest_run_status=latest_run.status if latest_run else None,
                        latest_run_at=latest_run.started_at if latest_run else None,
                    )
                    breaches.append(breach)
                    connector_breach_types.append(breach.breach_type)
                    connector_max_sev = self._max_severity(connector_max_sev, breach.severity)
                else:
                    sev, stage = self._severity_stage(
                        freshness_minutes,
                        warning=policy.freshness_warning_minutes,
                        critical=policy.freshness_critical_minutes,
                        escalation=policy.freshness_escalation_minutes,
                    )
                    if sev is not None and stage is not None:
                        breach = EventConnectorSLABreach(
                            connector_name=connector.connector_name,
                            source_name=connector.source_name,
                            breach_type=EventConnectorSLABreachType.FRESHNESS,
                            severity=sev,
                            stage=stage,
                            message=f"Freshness lag={freshness_minutes}m exceeds {stage} threshold.",
                            freshness_minutes=freshness_minutes,
                            pending_failures=pending,
                            dead_failures=dead,
                            latest_run_status=latest_run.status if latest_run else None,
                            latest_run_at=latest_run.started_at if latest_run else None,
                        )
                        breaches.append(breach)
                        connector_breach_types.append(breach.breach_type)
                        connector_max_sev = self._max_severity(connector_max_sev, breach.severity)

                sev, stage = self._severity_stage(
                    pending,
                    warning=policy.pending_warning,
                    critical=policy.pending_critical,
                    escalation=policy.pending_escalation,
                )
                if sev is not None and stage is not None:
                    breach = EventConnectorSLABreach(
                        connector_name=connector.connector_name,
                        source_name=connector.source_name,
                        breach_type=EventConnectorSLABreachType.PENDING_BACKLOG,
                        severity=sev,
                        stage=stage,
                        message=f"Pending failure backlog={pending} exceeds {stage} threshold.",
                        freshness_minutes=freshness_minutes,
                        pending_failures=pending,
                        dead_failures=dead,
                        latest_run_status=latest_run.status if latest_run else None,
                        latest_run_at=latest_run.started_at if latest_run else None,
                    )
                    breaches.append(breach)
                    connector_breach_types.append(breach.breach_type)
                    connector_max_sev = self._max_severity(connector_max_sev, breach.severity)

                sev, stage = self._severity_stage(
                    dead,
                    warning=policy.dead_warning,
                    critical=policy.dead_critical,
                    escalation=policy.dead_escalation,
                )
                if sev is not None and stage is not None:
                    breach = EventConnectorSLABreach(
                        connector_name=connector.connector_name,
                        source_name=connector.source_name,
                        breach_type=EventConnectorSLABreachType.DEAD_BACKLOG,
                        severity=sev,
                        stage=stage,
                        message=f"Dead-letter backlog={dead} exceeds {stage} threshold.",
                        freshness_minutes=freshness_minutes,
                        pending_failures=pending,
                        dead_failures=dead,
                        latest_run_status=latest_run.status if latest_run else None,
                        latest_run_at=latest_run.started_at if latest_run else None,
                    )
                    breaches.append(breach)
                    connector_breach_types.append(breach.breach_type)
                    connector_max_sev = self._max_severity(connector_max_sev, breach.severity)

            statuses.append(
                EventConnectorSLAStatus(
                    connector_name=connector.connector_name,
                    source_name=connector.source_name,
                    enabled=connector.enabled,
                    freshness_minutes=freshness_minutes,
                    pending_failures=pending,
                    dead_failures=dead,
                    latest_run_status=latest_run.status if latest_run else None,
                    latest_run_at=latest_run.started_at if latest_run else None,
                    severity=connector_max_sev,
                    breach_types=sorted(set(connector_breach_types), key=lambda x: x.value),
                )
            )

        warning_count = sum(1 for b in breaches if b.severity == SignalLevel.WARNING)
        critical_count = sum(1 for b in breaches if b.severity == SignalLevel.CRITICAL)
        escalated_count = sum(1 for b in breaches if b.stage == "escalated")
        return EventConnectorSLAReport(
            generated_at=now_utc,
            policy_defaults=self.default_sla_policy,
            connector_count=len(connectors),
            warning_count=warning_count,
            critical_count=critical_count,
            escalated_count=escalated_count,
            statuses=statuses,
            breaches=breaches,
        )

    def sync_sla_alerts(
        self,
        *,
        audit: AuditService,
        lookback_days: int = 30,
        cooldown_seconds: int = 900,
        warning_repeat_escalate: int = 3,
        critical_repeat_escalate: int = 2,
    ) -> EventConnectorSLAAlertSyncResult:
        now = datetime.now(timezone.utc)
        report = self.evaluate_sla(now=now)
        _ = self.store.append_sla_history(observed_at=now, breaches=report.breaches)
        connector_map = {
            c.connector_name: c
            for c in self.store.list_connectors(limit=5000, enabled_only=False)
        }
        emitted = 0
        skipped = 0
        escalated = 0
        active_keys: set[str] = set()

        for breach in report.breaches:
            dedupe_key = self._sla_state_key(breach)
            active_keys.add(dedupe_key)
            state, should_emit = self.store.upsert_sla_breach_state(
                dedupe_key=dedupe_key,
                breach=breach,
                observed_at=now,
                cooldown_seconds=cooldown_seconds,
            )
            target_level, target_reason = self._target_sla_escalation_level(
                state=state,
                warning_repeat_escalate=warning_repeat_escalate,
                critical_repeat_escalate=critical_repeat_escalate,
            )
            if target_level > state.escalation_level:
                escalated_state = self.store.update_sla_state_escalation(
                    dedupe_key=dedupe_key,
                    escalation_level=target_level,
                    escalation_reason=target_reason,
                    escalated_at=now,
                )
                if escalated_state is not None:
                    state = escalated_state
                    escalated += 1
                    audit.log(
                        event_type="event_connector_sla_escalation",
                        action=f"level_{target_level}",
                        status="ERROR" if target_level >= 2 else "OK",
                        payload={
                            "connector_name": state.connector_name,
                            "source_name": state.source_name,
                            "breach_type": state.breach_type.value,
                            "severity": state.severity.value,
                            "stage": state.stage,
                            "dedupe_key": state.dedupe_key,
                            "repeat_count": state.repeat_count,
                            "escalation_level": state.escalation_level,
                            "escalation_reason": state.escalation_reason,
                            "last_escalated_at": state.last_escalated_at.isoformat() if state.last_escalated_at else None,
                            "runbook_url": self._resolve_connector_runbook_url(
                                connector_map.get(state.connector_name)
                            ),
                        },
                    )
            if not should_emit:
                skipped += 1
                continue
            audit.log(
                event_type="event_connector_sla",
                action=breach.breach_type.value.lower(),
                status="ERROR" if breach.severity == SignalLevel.CRITICAL else "OK",
                payload={
                    "connector_name": breach.connector_name,
                    "source_name": breach.source_name,
                    "breach_type": breach.breach_type.value,
                    "severity": breach.severity.value,
                    "stage": breach.stage,
                    "message": breach.message,
                    "freshness_minutes": breach.freshness_minutes,
                    "pending_failures": breach.pending_failures,
                    "dead_failures": breach.dead_failures,
                    "lookback_days": lookback_days,
                    "dedupe_key": dedupe_key,
                    "repeat_count": state.repeat_count,
                    "escalation_level": state.escalation_level,
                    "escalation_reason": state.escalation_reason,
                    "first_seen_at": state.first_seen_at.isoformat(),
                    "last_seen_at": state.last_seen_at.isoformat(),
                    "last_emitted_at": state.last_emitted_at.isoformat() if state.last_emitted_at else None,
                    "last_escalated_at": state.last_escalated_at.isoformat() if state.last_escalated_at else None,
                    "runbook_url": self._resolve_connector_runbook_url(
                        connector_map.get(breach.connector_name)
                    ),
                },
            )
            emitted += 1

        recovered_states = self.store.resolve_sla_alert_states(
            active_dedupe_keys=active_keys,
            observed_at=now,
        )
        for state in recovered_states:
            audit.log(
                event_type="event_connector_sla_recovery",
                action="resolved",
                status="OK",
                payload={
                    "connector_name": state.connector_name,
                    "source_name": state.source_name,
                    "breach_type": state.breach_type.value,
                    "dedupe_key": state.dedupe_key,
                    "repeat_count": state.repeat_count,
                    "last_stage": state.stage,
                    "last_severity": state.severity.value,
                    "last_escalation_level": state.escalation_level,
                    "last_escalation_reason": state.escalation_reason,
                    "first_seen_at": state.first_seen_at.isoformat(),
                    "last_seen_at": state.last_seen_at.isoformat(),
                    "last_recovered_at": state.last_recovered_at.isoformat() if state.last_recovered_at else None,
                    "runbook_url": self._resolve_connector_runbook_url(
                        connector_map.get(state.connector_name)
                    ),
                },
            )

        return EventConnectorSLAAlertSyncResult(
            generated_at=now,
            emitted=emitted,
            skipped=skipped,
            recovered=len(recovered_states),
            escalated=escalated,
            open_states=self.store.count_open_sla_alert_states(),
            open_escalated=self.store.count_open_sla_alert_states(min_escalation_level=1),
            report=report,
        )

    def ops_event_stats(self, lookback_days: int = 30) -> OpsEventStats:
        coverage = self.coverage_summary(lookback_days=lookback_days)
        now = datetime.now(timezone.utc)
        since = now - timedelta(hours=24)
        runs = self.store.list_runs(limit=5000)
        recent_runs = [r for r in runs if r.started_at >= since]
        failures = self.store.list_failures(limit=5000)
        recent_failures = [f for f in failures if f.created_at >= since]
        sla = self.evaluate_sla(now=now)
        return OpsEventStats(
            lookback_days=coverage.lookback_days,
            total_events=coverage.total_events,
            active_symbols=coverage.symbols_covered,
            active_sources=coverage.sources_covered,
            pending_failures=self.store.count_failures(status=EventConnectorFailureStatus.PENDING),
            dead_failures=self.store.count_failures(status=EventConnectorFailureStatus.DEAD),
            connector_runs_24h=len(recent_runs),
            connector_failures_24h=len(recent_failures),
            connector_sla_warning=sla.warning_count,
            connector_sla_critical=sla.critical_count,
            connector_sla_escalated=sla.escalated_count,
        )

    def slo_history(
        self,
        *,
        connector_name: str | None = None,
        lookback_days: int = 30,
        bucket_hours: int = 24,
        include_disabled: bool = True,
    ) -> EventConnectorSLOHistory:
        days = max(1, min(lookback_days, 3650))
        bucket = max(1, min(bucket_hours, 24 * 14))
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)

        connectors = self.store.list_connectors(limit=5000, enabled_only=not include_disabled)
        allowed_connectors = {x.connector_name for x in connectors}
        if connector_name:
            if connector_name not in allowed_connectors:
                allowed_connectors = set()
            else:
                allowed_connectors = {connector_name}

        runs = self.store.list_runs(
            connector_name=connector_name if connector_name else None,
            limit=500000,
        )
        scoped_runs = [
            r
            for r in runs
            if r.connector_name in allowed_connectors and r.started_at >= start and r.started_at <= now
        ]
        history_rows = self.store.list_sla_history(
            connector_name=connector_name if connector_name else None,
            start_time=start,
            end_time=now,
            limit=500000,
        )
        scoped_history = [
            (
                datetime.fromisoformat(str(row["observed_at"])),
                str(row["severity"]),
                str(row["stage"]),
            )
            for row in history_rows
            if str(row["connector_name"]) in allowed_connectors
        ]

        points: list[EventConnectorSLOPoint] = []
        current = start
        while current < now:
            next_dt = current + timedelta(hours=bucket)
            run_rows = [r for r in scoped_runs if current <= r.started_at < next_dt]
            run_effective = [r for r in run_rows if r.status != EventConnectorRunStatus.DRY_RUN]
            success = sum(1 for r in run_effective if r.status in {EventConnectorRunStatus.SUCCESS, EventConnectorRunStatus.PARTIAL})
            failed = sum(1 for r in run_effective if r.status == EventConnectorRunStatus.FAILED)
            run_count = len(run_effective)

            breach_rows = [row for row in scoped_history if current <= row[0] < next_dt]
            warning_breaches = sum(1 for row in breach_rows if row[1] == SignalLevel.WARNING.value)
            critical_breaches = sum(1 for row in breach_rows if row[1] == SignalLevel.CRITICAL.value)
            escalated_breaches = sum(1 for row in breach_rows if row[2] == "escalated")

            warning_budget = max(1.0, run_count * 0.2)
            critical_budget = max(1.0, run_count * 0.05)
            points.append(
                EventConnectorSLOPoint(
                    window_start=current,
                    window_end=next_dt,
                    runs=run_count,
                    run_success_rate=round(success / max(1, run_count), 6),
                    run_failure_rate=round(failed / max(1, run_count), 6),
                    warning_breaches=warning_breaches,
                    critical_breaches=critical_breaches,
                    escalated_breaches=escalated_breaches,
                    burn_rate_warning=round(warning_breaches / warning_budget, 6),
                    burn_rate_critical=round(critical_breaches / critical_budget, 6),
                )
            )
            current = next_dt

        return EventConnectorSLOHistory(
            generated_at=now,
            connector_name=connector_name,
            lookback_days=days,
            bucket_hours=bucket,
            total_points=len(points),
            points=points,
        )

    def _replay_failure_rows(
        self,
        *,
        connector: EventConnectorRecord,
        source_timezone: str,
        source_reliability_score: float,
        failures,
        now: datetime,
    ) -> tuple[int, int, int, list[str], list[EventConnectorManualReplayItem]]:
        replayed = 0
        failed = 0
        dead = 0
        errors: list[str] = []
        items: list[EventConnectorManualReplayItem] = []

        for item in failures:
            if item.status == EventConnectorFailureStatus.REPLAYED:
                items.append(
                    EventConnectorManualReplayItem(
                        failure_id=item.id,
                        status=EventConnectorFailureStatus.REPLAYED,
                        message="already replayed",
                    )
                )
                continue
            try:
                event = None
                payload = dict(item.payload)
                if payload.get("event"):
                    event = payload["event"]
                elif payload.get("raw_record"):
                    raw = AnnouncementRawRecord.model_validate(payload["raw_record"])
                    event, _, _ = self.standardizer.normalize_record(
                        row=raw,
                        source_name=connector.source_name,
                        default_symbol=None,
                        default_timezone=source_timezone,
                        source_reliability_score=source_reliability_score,
                    )
                if event is None:
                    raise ValueError("failure payload missing both event and raw_record")

                ingest = self.event_service.ingest(
                    EventBatchIngestRequest(
                        source_name=connector.source_name,
                        events=[event if hasattr(event, "model_dump") else event],
                    )
                )
                if ingest.errors:
                    raise ValueError("; ".join(ingest.errors[:3]))
                self.store.mark_failure_replayed(item.id)
                replayed += 1
                items.append(
                    EventConnectorManualReplayItem(
                        failure_id=item.id,
                        status=EventConnectorFailureStatus.REPLAYED,
                        message="replayed",
                    )
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                err = f"id={item.id}: {exc}"
                errors.append(err)
                next_retry = now + timedelta(seconds=connector.replay_backoff_seconds * (2 ** max(0, item.retry_count)))
                if item.retry_count + 1 >= connector.max_retry:
                    self.store.mark_failure_dead(item.id, error_message=str(exc))
                    dead += 1
                    items.append(
                        EventConnectorManualReplayItem(
                            failure_id=item.id,
                            status=EventConnectorFailureStatus.DEAD,
                            message=str(exc),
                        )
                    )
                else:
                    self.store.mark_failure_retry(item.id, next_retry_at=next_retry, error_message=str(exc))
                    items.append(
                        EventConnectorManualReplayItem(
                            failure_id=item.id,
                            status=EventConnectorFailureStatus.PENDING,
                            message=str(exc),
                        )
                    )

        return replayed, failed, dead, errors, items

    @staticmethod
    def _resolve_source_matrix(connector: EventConnectorRecord) -> list[dict]:
        raw = connector.config.get("source_matrix") if isinstance(connector.config, dict) else None
        out: list[dict] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw):
                if not isinstance(item, dict):
                    continue
                source_key = str(item.get("source_key") or f"source_{idx + 1}").strip()
                if not source_key:
                    continue
                raw_type = item.get("connector_type") or connector.connector_type.value
                try:
                    connector_type = EventConnectorType(str(raw_type))
                except Exception:  # noqa: BLE001
                    continue
                priority = max(0, int(item.get("priority", (idx + 1) * 10)))
                enabled = bool(item.get("enabled", True))
                cfg = item.get("config")
                cfg_map = dict(cfg) if isinstance(cfg, dict) else {}
                budget_raw = item.get("request_budget_per_hour")
                if budget_raw is not None:
                    try:
                        budget = int(budget_raw)
                        if budget > 0:
                            cfg_map["request_budget_per_hour"] = budget
                    except Exception:  # noqa: BLE001
                        pass
                aliases_raw = item.get("credential_aliases")
                if isinstance(aliases_raw, list):
                    aliases = [str(x).strip() for x in aliases_raw if str(x).strip()]
                    if aliases:
                        cfg_map["credential_aliases"] = aliases
                out.append(
                    {
                        "source_key": source_key,
                        "connector_type": connector_type,
                        "priority": priority,
                        "enabled": enabled,
                        "config": cfg_map,
                    }
                )
        if not out:
            out.append(
                {
                    "source_key": "primary",
                    "connector_type": connector.connector_type,
                    "priority": 10,
                    "enabled": True,
                    "config": dict(connector.config),
                }
            )
        out.sort(key=lambda x: (x["priority"], x["source_key"]))
        return out

    def _sync_source_state_registry(self, *, connector: EventConnectorRecord, source_matrix: list[dict]) -> None:
        active_keys = {str(item["source_key"]) for item in source_matrix}
        for item in source_matrix:
            self.store.upsert_source_state(
                connector_name=connector.connector_name,
                source_key=str(item["source_key"]),
                connector_type=item["connector_type"],
                priority=int(item["priority"]),
                enabled=bool(item["enabled"]),
            )
        existing = self.store.list_source_states(connector_name=connector.connector_name, limit=5000)
        for state in existing:
            if state.source_key in active_keys:
                continue
            self.store.upsert_source_state(
                connector_name=state.connector_name,
                source_key=state.source_key,
                connector_type=state.connector_type,
                priority=state.priority,
                enabled=False,
                default_health=state.health_score,
            )

    @staticmethod
    def _order_source_states_for_run(
        *,
        connector: EventConnectorRecord,
        states: list[EventConnectorSourceStateRecord],
    ) -> tuple[list[EventConnectorSourceStateRecord], bool, int]:
        if not states:
            return [], True, 1
        failover_cfg = connector.config.get("failover") if isinstance(connector.config, dict) else None
        failover_enabled = True
        health_threshold = 35.0
        max_candidates = len(states)
        if isinstance(failover_cfg, dict):
            failover_enabled = bool(failover_cfg.get("enabled", True))
            try:
                health_threshold = float(failover_cfg.get("health_threshold", 35.0))
            except Exception:  # noqa: BLE001
                health_threshold = 35.0
            try:
                max_candidates = int(failover_cfg.get("max_candidates_per_run", len(states)))
            except Exception:  # noqa: BLE001
                max_candidates = len(states)

        enabled_states = [x for x in states if x.enabled]
        if not enabled_states:
            enabled_states = list(states)

        if not failover_enabled:
            ordered = sorted(
                enabled_states,
                key=lambda x: (0 if x.is_active else 1, x.priority, -x.effective_health_score, x.source_key),
            )
            return ordered[:1], False, 1

        ordered = sorted(
            enabled_states,
            key=lambda x: (
                0 if (x.is_active and x.effective_health_score >= health_threshold) else 1,
                -x.effective_health_score,
                x.priority,
                x.source_key,
            ),
        )
        bounded = max(1, min(max_candidates, len(ordered)))
        return ordered, True, bounded

    def _resolve_policy(self, connector: EventConnectorRecord) -> EventConnectorSLAPolicy:
        raw = connector.config.get("sla") if isinstance(connector.config, dict) else None
        if not isinstance(raw, dict):
            return self.default_sla_policy.model_copy(deep=True)
        merged = self.default_sla_policy.model_dump(mode="python")
        merged.update(raw)
        try:
            return EventConnectorSLAPolicy.model_validate(merged)
        except Exception:  # noqa: BLE001
            return self.default_sla_policy.model_copy(deep=True)

    @staticmethod
    def _resolve_connector_runbook_url(connector: EventConnectorRecord | None) -> str:
        if connector is None:
            return ""
        cfg = connector.config if isinstance(connector.config, dict) else {}
        raw = cfg.get("runbook_url")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        raw = cfg.get("runbook_path")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        return ""

    @staticmethod
    def _severity_stage(value: int, *, warning: int, critical: int, escalation: int) -> tuple[SignalLevel | None, str | None]:
        if value >= escalation:
            return SignalLevel.CRITICAL, "escalated"
        if value >= critical:
            return SignalLevel.CRITICAL, "critical"
        if value >= warning:
            return SignalLevel.WARNING, "warning"
        return None, None

    @staticmethod
    def _target_sla_escalation_level(
        *,
        state: EventConnectorSLAAlertStateRecord,
        warning_repeat_escalate: int,
        critical_repeat_escalate: int,
    ) -> tuple[int, str]:
        warning_repeat = max(1, int(warning_repeat_escalate))
        critical_repeat = max(1, int(critical_repeat_escalate))

        if state.stage == "escalated":
            return 3, "breach stage escalated by SLA threshold"
        if state.severity == SignalLevel.CRITICAL and state.repeat_count >= critical_repeat:
            return 2, f"critical breach repeated >= {critical_repeat}"
        if state.repeat_count >= warning_repeat:
            return 1, f"sustained breach repeated >= {warning_repeat}"
        return 0, ""

    @staticmethod
    def _max_severity(a: SignalLevel, b: SignalLevel) -> SignalLevel:
        rank = {
            SignalLevel.INFO: 1,
            SignalLevel.WARNING: 2,
            SignalLevel.CRITICAL: 3,
        }
        return a if rank[a] >= rank[b] else b

    @staticmethod
    def _extract_error_index(raw: str) -> int | None:
        matched = re.search(r"idx=(\d+)", raw)
        if not matched:
            return None
        try:
            return int(matched.group(1))
        except ValueError:
            return None

    @staticmethod
    def _sla_state_key(breach: EventConnectorSLABreach) -> str:
        return f"{breach.connector_name}|{breach.breach_type.value}"
