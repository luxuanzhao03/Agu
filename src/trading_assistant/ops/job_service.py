from __future__ import annotations

from datetime import datetime, timedelta, timezone
import time
from uuid import uuid4
from zoneinfo import ZoneInfo

from trading_assistant.alerts.service import AlertService
from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.core.models import (
    AutoTuneJobPayload,
    ComplianceEvidenceExportJobPayload,
    CostModelCalibrationRequest,
    CostModelRecalibrationJobPayload,
    ExecutionReviewJobPayload,
    EventConnectorReplayJobPayload,
    EventConnectorReplayRequest,
    EventConnectorRunRequest,
    EventConnectorSyncJobPayload,
    JobDefinitionRecord,
    JobRegisterRequest,
    JobSLABreach,
    JobSLABreachType,
    JobSLAReport,
    JobScheduleTickResult,
    JobRunRecord,
    JobRunStatus,
    JobStatus,
    JobType,
    OncallReconcileJobPayload,
    OncallReconcileRequest,
    PipelineRunRequest,
    ReportGenerateRequest,
    ResearchWorkflowRequest,
    SignalLevel,
)
from trading_assistant.governance.compliance_evidence import ComplianceEvidenceService
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.ops.cron import CronSchedule
from trading_assistant.ops.job_store import JobStore
from trading_assistant.pipeline.runner import DailyPipelineRunner
from trading_assistant.replay.service import ReplayService
from trading_assistant.reporting.service import ReportingService
from trading_assistant.workflow.research import ResearchWorkflowService


class JobService:
    def __init__(
        self,
        store: JobStore,
        pipeline: DailyPipelineRunner,
        research: ResearchWorkflowService,
        reporting: ReportingService,
        event_connector: EventConnectorService | None = None,
        compliance_evidence: ComplianceEvidenceService | None = None,
        alerts: AlertService | None = None,
        autotune: AutoTuneService | None = None,
        replay: ReplayService | None = None,
        scheduler_timezone: str = "Asia/Shanghai",
        running_timeout_minutes: int = 120,
    ) -> None:
        self.store = store
        self.pipeline = pipeline
        self.research = research
        self.reporting = reporting
        self.event_connector = event_connector
        self.compliance_evidence = compliance_evidence
        self.alerts = alerts
        self.autotune = autotune
        self.replay = replay
        self.scheduler_timezone = scheduler_timezone
        self.running_timeout_minutes = max(1, running_timeout_minutes)

    def register(self, req: JobRegisterRequest) -> int:
        if req.schedule_cron:
            _ = CronSchedule.parse(req.schedule_cron)
        return self.store.register(req)

    def list_jobs(self, active_only: bool = False, limit: int = 200) -> list[JobDefinitionRecord]:
        return self.store.list_jobs(active_only=active_only, limit=limit)

    def get_job(self, job_id: int) -> JobDefinitionRecord | None:
        return self.store.get_job(job_id)

    def trigger(self, job_id: int, triggered_by: str) -> JobRunRecord:
        job = self.store.get_job(job_id)
        if job is None:
            raise KeyError(f"job_id '{job_id}' not found")
        if job.status != JobStatus.ACTIVE:
            raise PermissionError(f"job_id '{job_id}' is disabled")

        run_id = uuid4().hex
        self.store.create_run(run_id=run_id, job_id=job_id, triggered_by=triggered_by)

        max_retries, retry_backoff_seconds = self._resolve_retry_config(job.payload)
        attempts = 0
        last_error: str | None = None
        try:
            while True:
                attempts += 1
                try:
                    summary = self._execute(job)
                    break
                except Exception as exc:  # noqa: BLE001
                    last_error = str(exc)
                    if attempts > max_retries:
                        raise
                    if retry_backoff_seconds > 0:
                        time.sleep(min(retry_backoff_seconds, 2))
            if attempts > 1:
                summary = {**summary, "retry_attempts": attempts - 1}
            self.store.finish_run(run_id=run_id, status=JobRunStatus.SUCCESS, result_summary=summary)
        except Exception as exc:  # noqa: BLE001
            self.store.finish_run(
                run_id=run_id,
                status=JobRunStatus.FAILED,
                result_summary={
                    "error": str(exc),
                    "retry_attempts": max(0, attempts - 1),
                    "last_error": last_error or str(exc),
                },
                error_message=last_error or str(exc),
            )
        run = self.store.get_run(run_id)
        if run is None:
            raise RuntimeError(f"run_id '{run_id}' not found after execution")
        return run

    def list_runs(self, job_id: int, limit: int = 200) -> list[JobRunRecord]:
        return self.store.list_runs(job_id=job_id, limit=limit)

    def get_run(self, run_id: str) -> JobRunRecord | None:
        return self.store.get_run(run_id)

    def get_latest_run(self, job_id: int) -> JobRunRecord | None:
        return self.store.get_latest_run(job_id)

    def list_recent_runs(
        self,
        limit: int = 200,
        since: datetime | None = None,
        job_id: int | None = None,
    ) -> list[JobRunRecord]:
        return self.store.list_recent_runs(limit=limit, since=since, job_id=job_id)

    def scheduler_tick(
        self,
        as_of: datetime | None = None,
        triggered_by: str = "scheduler",
    ) -> JobScheduleTickResult:
        now_utc = self._ensure_utc(as_of or datetime.now(timezone.utc))
        tz_name, schedule_zone = self._schedule_zone()
        local_minute = now_utc.astimezone(schedule_zone).replace(second=0, microsecond=0)
        expected_utc = local_minute.astimezone(timezone.utc)
        result = JobScheduleTickResult(tick_time=now_utc, timezone=tz_name)

        jobs = self.list_jobs(active_only=True, limit=1000)
        for job in jobs:
            if not job.schedule_cron:
                continue
            try:
                schedule = CronSchedule.parse(job.schedule_cron)
            except ValueError as exc:
                result.errors.append(f"job_id={job.id} invalid cron '{job.schedule_cron}': {exc}")
                continue
            if not schedule.matches(local_minute):
                continue

            result.matched_jobs.append(job.id)
            latest = self.get_latest_run(job.id)
            if latest is not None:
                latest_minute_utc = self._minute_floor(self._ensure_utc(latest.started_at))
                if latest.triggered_by == triggered_by and latest_minute_utc == expected_utc:
                    result.skipped_jobs.append(job.id)
                    continue

            run = self.trigger(job_id=job.id, triggered_by=triggered_by)
            result.triggered_runs.append(run)
        return result

    def evaluate_sla(
        self,
        as_of: datetime | None = None,
        grace_minutes: int = 15,
        running_timeout_minutes: int | None = None,
    ) -> JobSLAReport:
        now_utc = self._ensure_utc(as_of or datetime.now(timezone.utc))
        tz_name, schedule_zone = self._schedule_zone()
        local_now = now_utc.astimezone(schedule_zone).replace(second=0, microsecond=0)
        active_jobs = self.list_jobs(active_only=True, limit=1000)
        report = JobSLAReport(
            checked_at=now_utc,
            timezone=tz_name,
            total_active_jobs=len(active_jobs),
            total_scheduled_jobs=0,
        )

        grace = timedelta(minutes=max(0, grace_minutes))
        running_timeout = timedelta(minutes=max(1, running_timeout_minutes or self.running_timeout_minutes))

        for job in active_jobs:
            if not job.schedule_cron:
                continue
            report.total_scheduled_jobs += 1
            try:
                schedule = CronSchedule.parse(job.schedule_cron)
            except ValueError as exc:
                report.breaches.append(
                    JobSLABreach(
                        job_id=job.id,
                        job_name=job.name,
                        breach_type=JobSLABreachType.INVALID_CRON,
                        severity=SignalLevel.CRITICAL,
                        message=str(exc),
                        schedule_cron=job.schedule_cron,
                    )
                )
                continue

            latest = self.get_latest_run(job.id)
            latest_started_utc = self._ensure_utc(latest.started_at) if latest is not None else None
            expected_local = schedule.previous_at_or_before(local_now)
            if expected_local is None:
                continue
            expected_utc = expected_local.astimezone(timezone.utc)

            if now_utc >= expected_utc + grace:
                if latest_started_utc is None or self._minute_floor(latest_started_utc) < expected_utc:
                    delay = int((now_utc - expected_utc).total_seconds() // 60)
                    report.breaches.append(
                        JobSLABreach(
                            job_id=job.id,
                            job_name=job.name,
                            breach_type=JobSLABreachType.MISSED_RUN,
                            severity=SignalLevel.WARNING,
                            message="Scheduled run is overdue and has not started.",
                            schedule_cron=job.schedule_cron,
                            expected_run_at=expected_utc,
                            last_run_at=latest_started_utc,
                            delay_minutes=max(delay, 0),
                        )
                    )
                    continue

            if latest is not None and latest_started_utc is not None:
                if latest.status == JobRunStatus.FAILED:
                    report.breaches.append(
                        JobSLABreach(
                            job_id=job.id,
                            job_name=job.name,
                            breach_type=JobSLABreachType.LATEST_RUN_FAILED,
                            severity=SignalLevel.CRITICAL,
                            message=latest.error_message or "Latest scheduled run failed.",
                            schedule_cron=job.schedule_cron,
                            expected_run_at=expected_utc,
                            last_run_at=latest_started_utc,
                        )
                    )
                if latest.status == JobRunStatus.RUNNING and now_utc - latest_started_utc > running_timeout:
                    delay = int((now_utc - latest_started_utc).total_seconds() // 60)
                    report.breaches.append(
                        JobSLABreach(
                            job_id=job.id,
                            job_name=job.name,
                            breach_type=JobSLABreachType.RUNNING_TIMEOUT,
                            severity=SignalLevel.WARNING,
                            message="Latest run remains RUNNING beyond timeout threshold.",
                            schedule_cron=job.schedule_cron,
                            expected_run_at=expected_utc,
                            last_run_at=latest_started_utc,
                            delay_minutes=max(delay, 0),
                        )
                    )

        return report

    def _execute(self, job: JobDefinitionRecord) -> dict:
        if job.job_type == JobType.PIPELINE_DAILY:
            req = PipelineRunRequest.model_validate(job.payload)
            result = self.pipeline.run(req)
            return {
                "pipeline_run_id": result.run_id,
                "strategy_name": result.strategy_name,
                "total_symbols": result.total_symbols,
                "total_signals": result.total_signals,
                "total_blocked": result.total_blocked,
                "total_warnings": result.total_warnings,
            }

        if job.job_type == JobType.RESEARCH_WORKFLOW:
            req = ResearchWorkflowRequest.model_validate(job.payload)
            result = self.research.run(req)
            return {
                "research_run_id": result.run_id,
                "strategy_name": result.strategy_name,
                "signal_count": len(result.signals),
                "optimized": result.optimized_portfolio is not None,
            }

        if job.job_type == JobType.REPORT_GENERATE:
            req = ReportGenerateRequest.model_validate(job.payload)
            result = self.reporting.generate(req)
            return {
                "title": result.title,
                "saved_path": result.saved_path,
                "content_size": len(result.content),
            }

        if job.job_type == JobType.EVENT_CONNECTOR_SYNC:
            if self.event_connector is None:
                raise ValueError("event connector service is not configured")
            payload = EventConnectorSyncJobPayload.model_validate(job.payload)
            result = self.event_connector.run_connector(
                EventConnectorRunRequest(
                    connector_name=payload.connector_name,
                    triggered_by="ops_job_runner",
                    dry_run=payload.dry_run,
                    force_full_sync=payload.force_full_sync,
                    fetch_limit_override=payload.fetch_limit_override,
                )
            )
            return {
                "connector_name": payload.connector_name,
                "run_id": result.run.run_id,
                "status": result.run.status.value,
                "pulled": result.run.pulled_count,
                "normalized": result.run.normalized_count,
                "inserted": result.run.inserted_count,
                "updated": result.run.updated_count,
                "failed": result.run.failed_count,
            }

        if job.job_type == JobType.EVENT_CONNECTOR_REPLAY:
            if self.event_connector is None:
                raise ValueError("event connector service is not configured")
            payload = EventConnectorReplayJobPayload.model_validate(job.payload)
            result = self.event_connector.replay_failures(
                EventConnectorReplayRequest(
                    connector_name=payload.connector_name,
                    limit=payload.limit,
                    triggered_by="ops_job_runner",
                )
            )
            return {
                "connector_name": payload.connector_name,
                "picked": result.picked,
                "replayed": result.replayed,
                "failed": result.failed,
                "dead": result.dead,
            }

        if job.job_type == JobType.COMPLIANCE_EVIDENCE_EXPORT:
            if self.compliance_evidence is None:
                raise ValueError("compliance evidence service is not configured")
            payload = ComplianceEvidenceExportJobPayload.model_validate(job.payload)
            result = self.compliance_evidence.export_bundle(payload.request)
            return {
                "bundle_id": result.bundle_id,
                "package_path": result.package_path,
                "package_sha256": result.package_sha256,
                "signature_enabled": bool(result.signature and result.signature.enabled),
                "vault_copy_path": result.vault_copy_path,
                "file_count": result.file_count,
            }

        if job.job_type == JobType.ALERT_ONCALL_RECONCILE:
            if self.alerts is None:
                raise ValueError("alert service is not configured")
            payload = OncallReconcileJobPayload.model_validate(job.payload)
            result = self.alerts.reconcile_oncall(
                OncallReconcileRequest(
                    provider=payload.provider,
                    endpoint=payload.endpoint,
                    mapping_template=payload.mapping_template,
                    limit=payload.limit,
                    dry_run=payload.dry_run,
                )
            )
            return {
                "provider": result.provider,
                "endpoint": result.endpoint,
                "pulled": result.pulled,
                "matched": result.matched,
                "callbacks": result.callbacks,
                "acked_notifications": result.acked_notifications,
                "dry_run": result.dry_run,
                "errors": len(result.errors),
            }

        if job.job_type == JobType.AUTO_TUNE:
            if self.autotune is None:
                raise ValueError("autotune service is not configured")
            payload = AutoTuneJobPayload.model_validate(job.payload)
            result = self.autotune.run(payload.request)
            return {
                "autotune_run_id": result.run_id,
                "strategy_name": result.strategy_name,
                "symbol": result.symbol,
                "evaluated_count": result.evaluated_count,
                "applied": result.applied,
                "apply_decision": result.apply_decision,
                "best_objective": (result.best.objective_score if result.best is not None else None),
                "improvement_vs_baseline": result.improvement_vs_baseline,
            }

        if job.job_type == JobType.EXECUTION_REVIEW:
            payload = ExecutionReviewJobPayload.model_validate(job.payload)
            result = self.reporting.generate(
                ReportGenerateRequest(
                    report_type="closure",
                    symbol=payload.symbol,
                    strategy_name=payload.strategy_name,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                    limit=payload.limit,
                    save_to_file=payload.save_to_file,
                )
            )
            return {
                "title": result.title,
                "saved_path": result.saved_path,
                "content_size": len(result.content),
            }

        if job.job_type == JobType.COST_MODEL_RECALIBRATION:
            if self.replay is None:
                raise ValueError("replay service is not configured")
            payload = CostModelRecalibrationJobPayload.model_validate(job.payload)
            result = self.replay.calibrate_cost_model(
                CostModelCalibrationRequest(
                    symbol=payload.symbol,
                    strategy_name=payload.strategy_name,
                    start_date=payload.start_date,
                    end_date=payload.end_date,
                    limit=payload.limit,
                    min_samples=payload.min_samples,
                    save_record=payload.save_record,
                )
            )
            report_path = None
            if payload.save_to_file:
                report = self.reporting.generate(
                    ReportGenerateRequest(
                        report_type="cost_model",
                        symbol=payload.symbol,
                        strategy_name=payload.strategy_name,
                        start_date=payload.start_date,
                        end_date=payload.end_date,
                        limit=payload.limit,
                        save_to_file=True,
                    )
                )
                report_path = report.saved_path
            return {
                "symbol": result.symbol,
                "strategy_name": result.strategy_name,
                "sample_size": result.sample_size,
                "executed_samples": result.executed_samples,
                "slippage_coverage": result.slippage_coverage,
                "recommended_slippage_rate": result.recommended_slippage_rate,
                "recommended_impact_cost_coeff": result.recommended_impact_cost_coeff,
                "recommended_fill_probability_floor": result.recommended_fill_probability_floor,
                "confidence": result.confidence,
                "calibration_id": result.calibration_id,
                "report_path": report_path,
            }

        raise ValueError(f"unsupported job_type: {job.job_type.value}")

    @staticmethod
    def _resolve_retry_config(payload: dict) -> tuple[int, int]:
        cfg = payload.get("_retry", {}) if isinstance(payload, dict) else {}
        if not isinstance(cfg, dict):
            return 0, 0
        max_retries = int(cfg.get("max_retries", 0) or 0)
        max_retries = max(0, min(max_retries, 5))
        backoff_seconds = int(cfg.get("backoff_seconds", 0) or 0)
        backoff_seconds = max(0, min(backoff_seconds, 60))
        return max_retries, backoff_seconds

    def _schedule_zone(self) -> tuple[str, ZoneInfo]:
        try:
            return self.scheduler_timezone, ZoneInfo(self.scheduler_timezone)
        except Exception:  # noqa: BLE001
            return "UTC", ZoneInfo("UTC")

    @staticmethod
    def _ensure_utc(dt: datetime) -> datetime:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    @staticmethod
    def _minute_floor(dt: datetime) -> datetime:
        return dt.replace(second=0, microsecond=0)
