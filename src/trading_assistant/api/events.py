from __future__ import annotations

from datetime import date, datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import (
    get_audit_service,
    get_event_connector_service,
    get_event_feature_compare_service,
    get_event_nlp_governance_service,
    get_event_service,
)
from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventBatchIngestResult,
    EventConnectorFailureRecord,
    EventConnectorFailureRepairRequest,
    EventConnectorFailureRepairResult,
    EventConnectorFailureStatus,
    EventConnectorManualReplayRequest,
    EventConnectorManualReplayResult,
    EventConnectorOverviewResult,
    EventConnectorRepairReplayRequest,
    EventConnectorRepairReplayResult,
    EventConnectorRecord,
    EventConnectorRegisterRequest,
    EventConnectorReplayRequest,
    EventConnectorReplayResult,
    EventConnectorSLAAlertSyncResult,
    EventConnectorSLAAlertStateRecord,
    EventConnectorSLAAlertStateSummary,
    EventConnectorSLAReport,
    EventConnectorRunRecord,
    EventConnectorRunRequest,
    EventConnectorRunResult,
    EventFeatureBacktestCompareRequest,
    EventFeatureBacktestCompareResult,
    EventFeaturePreviewRequest,
    EventFeaturePreviewResult,
    EventJoinPITValidationRequest,
    EventJoinPITValidationResult,
    EventNormalizeIngestRequest,
    EventNormalizeIngestResult,
    EventNormalizePreviewRequest,
    EventNormalizePreviewResult,
    EventOpsCoverageSummary,
    EventNLPDriftCheckRequest,
    EventNLPDriftCheckResult,
    EventNLPDriftMonitorSummary,
    EventNLPDriftSnapshotRecord,
    EventNLPFeedbackRecord,
    EventNLPFeedbackSummary,
    EventNLPFeedbackUpsertRequest,
    EventNLPRulesetActivateRequest,
    EventNLPRulesetRecord,
    EventNLPRulesetUpsertRequest,
    EventRecord,
    EventSourceRecord,
    EventSourceRegisterRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.governance.event_feature_compare import EventFeatureBacktestCompareService
from trading_assistant.governance.event_nlp_governance import EventNLPGovernanceService
from trading_assistant.governance.event_service import EventService

router = APIRouter(prefix="/events", tags=["events"])


@router.post("/sources/register", response_model=int)
def register_event_source(
    req: EventSourceRegisterRequest,
    events: EventService = Depends(get_event_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> int:
    row_id = events.register_source(req)
    audit.log(
        event_type="event_source",
        action="register",
        payload={
            "source_name": req.source_name,
            "source_type": req.source_type.value,
            "provider": req.provider,
            "source_id": row_id,
        },
    )
    return row_id


@router.get("/sources", response_model=list[EventSourceRecord])
def list_event_sources(
    limit: int = Query(default=200, ge=1, le=1000),
    events: EventService = Depends(get_event_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventSourceRecord]:
    return events.list_sources(limit=limit)


@router.post("/ingest", response_model=EventBatchIngestResult)
def ingest_events(
    req: EventBatchIngestRequest,
    events: EventService = Depends(get_event_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventBatchIngestResult:
    try:
        result = events.ingest(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    status = "ERROR" if result.errors else "OK"
    audit.log(
        event_type="event_ingest",
        action="batch",
        status=status,
        payload={
            "source_name": req.source_name,
            "total": result.total,
            "inserted": result.inserted,
            "updated": result.updated,
            "errors": len(result.errors),
        },
    )
    return result


@router.get("", response_model=list[EventRecord])
def list_events(
    symbol: str | None = Query(default=None),
    source_name: str | None = Query(default=None),
    event_type: str | None = Query(default=None),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    events: EventService = Depends(get_event_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventRecord]:
    return events.list_events(
        symbol=symbol,
        source_name=source_name,
        event_type=event_type,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )


@router.post("/pit/join-validate", response_model=EventJoinPITValidationResult)
def validate_event_join_pit(
    req: EventJoinPITValidationRequest,
    events: EventService = Depends(get_event_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventJoinPITValidationResult:
    result = events.validate_join(req)
    audit.log(
        event_type="event_pit",
        action="join_validate",
        status="OK" if result.passed else "ERROR",
        payload={
            "checked_rows": result.checked_rows,
            "issues": len(result.issues),
            "passed": result.passed,
        },
    )
    return result


@router.post("/features/preview", response_model=EventFeaturePreviewResult)
def preview_event_features(
    req: EventFeaturePreviewRequest,
    events: EventService = Depends(get_event_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventFeaturePreviewResult:
    return events.preview_features(req)


@router.post("/connectors/register", response_model=int)
def register_connector(
    req: EventConnectorRegisterRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> int:
    try:
        row_id = connectors.register_connector(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    audit.log(
        event_type="event_connector",
        action="register",
        payload={
            "connector_name": req.connector_name,
            "source_name": req.source_name,
            "connector_type": req.connector_type.value,
            "connector_id": row_id,
        },
    )
    return row_id


@router.get("/connectors", response_model=list[EventConnectorRecord])
def list_connectors(
    enabled_only: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=2000),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventConnectorRecord]:
    return connectors.list_connectors(limit=limit, enabled_only=enabled_only)


@router.get("/connectors/overview", response_model=EventConnectorOverviewResult)
def connector_overview(
    limit: int = Query(default=200, ge=1, le=2000),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventConnectorOverviewResult:
    return connectors.overview(limit=limit)


@router.post("/connectors/run", response_model=EventConnectorRunResult)
def run_connector(
    req: EventConnectorRunRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventConnectorRunResult:
    try:
        result = connectors.run_connector(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="event_connector",
        action="run",
        status="ERROR" if result.run.status.value == "FAILED" else "OK",
        payload={
            "connector_name": req.connector_name,
            "run_id": result.run.run_id,
            "status": result.run.status.value,
            "pulled": result.run.pulled_count,
            "inserted": result.run.inserted_count,
            "updated": result.run.updated_count,
            "failed": result.run.failed_count,
            "errors": len(result.errors),
        },
    )
    return result


@router.get("/connectors/runs", response_model=list[EventConnectorRunRecord])
def list_connector_runs(
    connector_name: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventConnectorRunRecord]:
    return connectors.list_runs(connector_name=connector_name, limit=limit)


@router.get("/connectors/failures", response_model=list[EventConnectorFailureRecord])
def list_connector_failures(
    connector_name: str | None = Query(default=None),
    status: EventConnectorFailureStatus | None = Query(default=None),
    error_keyword: str | None = Query(default=None, min_length=1, max_length=200),
    limit: int = Query(default=200, ge=1, le=5000),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventConnectorFailureRecord]:
    return connectors.list_failures(
        connector_name=connector_name,
        status=status,
        error_keyword=error_keyword,
        limit=limit,
    )


@router.post("/connectors/failures/repair", response_model=EventConnectorFailureRepairResult)
def repair_connector_failure(
    req: EventConnectorFailureRepairRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK)),
) -> EventConnectorFailureRepairResult:
    try:
        result = connectors.repair_failure(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="event_connector",
        action="failure_repair",
        payload={
            "connector_name": req.connector_name,
            "failure_id": req.failure_id,
            "updated": result.updated,
            "reset_retry_count": req.reset_retry_count,
            "triggered_by": req.triggered_by,
        },
    )
    return result


@router.post("/connectors/replay", response_model=EventConnectorReplayResult)
def replay_connector_failures(
    req: EventConnectorReplayRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventConnectorReplayResult:
    try:
        result = connectors.replay_failures(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    audit.log(
        event_type="event_connector",
        action="replay",
        status="ERROR" if result.failed > 0 else "OK",
        payload={
            "connector_name": req.connector_name,
            "picked": result.picked,
            "replayed": result.replayed,
            "failed": result.failed,
            "dead": result.dead,
            "errors": len(result.errors),
        },
    )
    return result


@router.post("/connectors/replay/manual", response_model=EventConnectorManualReplayResult)
def replay_connector_failures_manual(
    req: EventConnectorManualReplayRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK)),
) -> EventConnectorManualReplayResult:
    try:
        result = connectors.replay_selected_failures(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="event_connector",
        action="manual_replay",
        status="ERROR" if result.failed > 0 else "OK",
        payload={
            "connector_name": req.connector_name,
            "picked": result.picked,
            "replayed": result.replayed,
            "failed": result.failed,
            "dead": result.dead,
            "errors": len(result.errors),
        },
    )
    return result


@router.post("/connectors/replay/repair", response_model=EventConnectorRepairReplayResult)
def repair_and_replay_connector_failures(
    req: EventConnectorRepairReplayRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK)),
) -> EventConnectorRepairReplayResult:
    try:
        result = connectors.repair_and_replay_failures(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="event_connector",
        action="repair_replay",
        status="ERROR" if result.failed > 0 else "OK",
        payload={
            "connector_name": req.connector_name,
            "repaired": result.repaired,
            "picked": result.picked,
            "replayed": result.replayed,
            "failed": result.failed,
            "dead": result.dead,
            "errors": len(result.errors),
        },
    )
    return result


@router.get("/connectors/sla", response_model=EventConnectorSLAReport)
def connector_sla_report(
    include_disabled: bool = Query(default=True),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK)),
) -> EventConnectorSLAReport:
    return connectors.evaluate_sla(include_disabled=include_disabled)


@router.post("/connectors/sla/sync-alerts", response_model=EventConnectorSLAAlertSyncResult)
def connector_sla_sync_alerts(
    lookback_days: int = Query(default=30, ge=1, le=3650),
    cooldown_seconds: int = Query(default=900, ge=0, le=86400),
    warning_repeat_escalate: int = Query(default=3, ge=1, le=1000),
    critical_repeat_escalate: int = Query(default=2, ge=1, le=1000),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK)),
) -> EventConnectorSLAAlertSyncResult:
    return connectors.sync_sla_alerts(
        audit=audit,
        lookback_days=lookback_days,
        cooldown_seconds=cooldown_seconds,
        warning_repeat_escalate=warning_repeat_escalate,
        critical_repeat_escalate=critical_repeat_escalate,
    )


@router.get("/connectors/sla/states", response_model=list[EventConnectorSLAAlertStateRecord])
def connector_sla_alert_states(
    connector_name: str | None = Query(default=None),
    open_only: bool = Query(default=True),
    limit: int = Query(default=200, ge=1, le=5000),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK)),
) -> list[EventConnectorSLAAlertStateRecord]:
    return connectors.list_sla_alert_states(
        connector_name=connector_name,
        open_only=open_only,
        limit=limit,
    )


@router.get("/connectors/sla/states/summary", response_model=EventConnectorSLAAlertStateSummary)
def connector_sla_alert_states_summary(
    connector_name: str | None = Query(default=None),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK)),
) -> EventConnectorSLAAlertStateSummary:
    return connectors.sla_alert_state_summary(connector_name=connector_name)


@router.get("/ops/coverage", response_model=EventOpsCoverageSummary)
def ops_coverage(
    lookback_days: int = Query(default=30, ge=1, le=3650),
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventOpsCoverageSummary:
    return connectors.coverage_summary(lookback_days=lookback_days)


@router.post("/nlp/normalize/preview", response_model=EventNormalizePreviewResult)
def normalize_preview(
    req: EventNormalizePreviewRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventNormalizePreviewResult:
    return connectors.normalize_preview(req)


@router.post("/nlp/normalize/ingest", response_model=EventNormalizeIngestResult)
def normalize_ingest(
    req: EventNormalizeIngestRequest,
    connectors: EventConnectorService = Depends(get_event_connector_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventNormalizeIngestResult:
    try:
        result = connectors.normalize_and_ingest(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    ingest = result.ingest
    audit.log(
        event_type="event_nlp",
        action="normalize_ingest",
        status="ERROR" if (ingest and ingest.errors) else "OK",
        payload={
            "source_name": req.source_name,
            "normalized": len(result.preview.normalized),
            "dropped": result.preview.dropped,
            "preview_errors": len(result.preview.errors),
            "ingest_inserted": ingest.inserted if ingest else 0,
            "ingest_updated": ingest.updated if ingest else 0,
            "ingest_errors": len(ingest.errors) if ingest else 0,
        },
    )
    return result


@router.post("/nlp/rulesets", response_model=int)
def upsert_nlp_ruleset(
    req: EventNLPRulesetUpsertRequest,
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> int:
    row_id = service.upsert_ruleset(req)
    audit.log(
        event_type="event_nlp",
        action="ruleset_upsert",
        payload={
            "version": req.version,
            "rule_count": len(req.rules),
            "activate": req.activate,
            "created_by": req.created_by,
        },
    )
    return row_id


@router.post("/nlp/rulesets/activate", response_model=bool)
def activate_nlp_ruleset(
    req: EventNLPRulesetActivateRequest,
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK)),
) -> bool:
    activated = service.activate_ruleset(req)
    if not activated:
        raise HTTPException(status_code=404, detail=f"ruleset version '{req.version}' not found")
    audit.log(
        event_type="event_nlp",
        action="ruleset_activate",
        payload={
            "version": req.version,
            "activated_by": req.activated_by,
            "note": req.note,
        },
    )
    return activated


@router.get("/nlp/rulesets", response_model=list[EventNLPRulesetRecord])
def list_nlp_rulesets(
    include_rules: bool = Query(default=False),
    limit: int = Query(default=50, ge=1, le=500),
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventNLPRulesetRecord]:
    return service.list_rulesets(limit=limit, include_rules=include_rules)


@router.get("/nlp/rulesets/active", response_model=EventNLPRulesetRecord | None)
def active_nlp_ruleset(
    include_rules: bool = Query(default=True),
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventNLPRulesetRecord | None:
    return service.get_active_ruleset(include_rules=include_rules)


@router.post("/nlp/drift-check", response_model=EventNLPDriftCheckResult)
def nlp_drift_check(
    req: EventNLPDriftCheckRequest,
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventNLPDriftCheckResult:
    result = service.drift_check(req)
    audit.log(
        event_type="event_nlp",
        action="drift_check",
        status="ERROR" if any(a.severity.value == "CRITICAL" for a in result.alerts) else "OK",
        payload={
            "source_name": req.source_name,
            "ruleset_version": result.ruleset_version,
            "sample_size": result.current.sample_size,
            "hit_rate_delta": result.hit_rate_delta,
            "score_p50_delta": result.score_p50_delta,
            "contribution_delta": result.contribution_delta,
            "alerts": len(result.alerts),
            "snapshot_id": result.snapshot_id,
        },
    )
    return result


@router.post("/nlp/feedback", response_model=int)
def upsert_nlp_feedback(
    req: EventNLPFeedbackUpsertRequest,
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> int:
    try:
        row_id = service.upsert_feedback(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    audit.log(
        event_type="event_nlp",
        action="feedback_upsert",
        payload={
            "source_name": req.source_name,
            "event_id": req.event_id,
            "label_event_type": req.label_event_type,
            "label_polarity": req.label_polarity.value,
            "labeler": req.labeler,
            "feedback_id": row_id,
        },
    )
    return row_id


@router.get("/nlp/feedback", response_model=list[EventNLPFeedbackRecord])
def list_nlp_feedback(
    source_name: str | None = Query(default=None),
    labeler: str | None = Query(default=None),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventNLPFeedbackRecord]:
    return service.list_feedback(
        source_name=source_name,
        labeler=labeler,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )


@router.get("/nlp/feedback/summary", response_model=EventNLPFeedbackSummary)
def nlp_feedback_summary(
    start_date: date = Query(...),
    end_date: date = Query(...),
    source_name: str | None = Query(default=None),
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventNLPFeedbackSummary:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    return service.feedback_summary(
        source_name=source_name,
        start_date=start_date,
        end_date=end_date,
    )


@router.get("/nlp/drift/snapshots", response_model=list[EventNLPDriftSnapshotRecord])
def list_nlp_drift_snapshots(
    source_name: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[EventNLPDriftSnapshotRecord]:
    return service.list_drift_snapshots(source_name=source_name, limit=limit)


@router.get("/nlp/drift/monitor", response_model=EventNLPDriftMonitorSummary)
def nlp_drift_monitor(
    source_name: str | None = Query(default=None),
    limit: int = Query(default=30, ge=3, le=365),
    service: EventNLPGovernanceService = Depends(get_event_nlp_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> EventNLPDriftMonitorSummary:
    return service.drift_monitor(source_name=source_name, limit=limit)


@router.post("/features/backtest-compare", response_model=EventFeatureBacktestCompareResult)
def compare_event_features(
    req: EventFeatureBacktestCompareRequest,
    service: EventFeatureBacktestCompareService = Depends(get_event_feature_compare_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> EventFeatureBacktestCompareResult:
    try:
        result = service.compare(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    audit.log(
        event_type="event_feature_compare",
        action="backtest_compare",
        payload={
            "symbol": req.symbol,
            "strategy": req.strategy_name,
            "provider": result.provider,
            "total_return_delta": result.delta.total_return_delta,
            "events_loaded": result.diagnostics.events_loaded,
            "event_row_ratio": result.diagnostics.event_row_ratio,
            "report_path": result.report_path,
        },
    )
    return result
