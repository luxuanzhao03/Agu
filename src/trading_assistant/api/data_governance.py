from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import (
    get_audit_service,
    get_data_provider,
    get_data_quality_service,
    get_pit_validator,
    get_snapshot_service,
)
from trading_assistant.core.models import (
    DataQualityReport,
    DataQualityRequest,
    EventPITValidationRequest,
    PITValidationResult,
    DataSnapshotRecord,
    DataSnapshotRegisterRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.snapshot_service import DataSnapshotService

router = APIRouter(prefix="/data", tags=["data-governance"])


@router.post("/quality/report", response_model=DataQualityReport)
def data_quality_report(
    req: DataQualityRequest,
    provider: CompositeDataProvider = Depends(get_data_provider),
    quality: DataQualityService = Depends(get_data_quality_service),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT)),
) -> DataQualityReport:
    try:
        used_provider, bars = provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    report = quality.evaluate(req=req, bars=bars, provider=used_provider)
    snapshot_id = snapshots.register(
        DataSnapshotRegisterRequest(
            dataset_name="daily_bars",
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            provider=used_provider,
            row_count=len(bars),
            content_hash=dataframe_content_hash(bars),
        )
    )
    audit.log(
        event_type="data_quality",
        action="report",
        payload={
            "symbol": req.symbol,
            "provider": used_provider,
            "passed": report.passed,
            "issues": len(report.issues),
            "snapshot_id": snapshot_id,
        },
    )
    return report


@router.post("/snapshots/register", response_model=int)
def register_snapshot(
    req: DataSnapshotRegisterRequest,
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK)),
) -> int:
    snapshot_id = snapshots.register(req)
    audit.log(
        event_type="data_snapshot",
        action="register",
        payload={"snapshot_id": snapshot_id, "dataset": req.dataset_name, "symbol": req.symbol},
    )
    return snapshot_id


@router.get("/snapshots", response_model=list[DataSnapshotRecord])
def list_snapshots(
    dataset_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[DataSnapshotRecord]:
    return snapshots.list_snapshots(dataset_name=dataset_name, symbol=symbol, limit=limit)


@router.get("/snapshots/latest", response_model=DataSnapshotRecord | None)
def latest_snapshot(
    dataset_name: str = Query(...),
    symbol: str = Query(...),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> DataSnapshotRecord | None:
    return snapshots.latest(dataset_name=dataset_name, symbol=symbol)


@router.post("/pit/validate", response_model=PITValidationResult)
def validate_pit(
    req: DataQualityRequest,
    provider: CompositeDataProvider = Depends(get_data_provider),
    pit: PITValidator = Depends(get_pit_validator),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> PITValidationResult:
    try:
        used_provider, bars = provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    result = pit.validate_bars(symbol=req.symbol, provider=used_provider, bars=bars, as_of=req.end_date)
    audit.log(
        event_type="pit_validation",
        action="validate",
        payload={"symbol": req.symbol, "provider": used_provider, "passed": result.passed},
    )
    return result


@router.post("/pit/validate-events", response_model=PITValidationResult)
def validate_event_pit(
    req: EventPITValidationRequest,
    pit: PITValidator = Depends(get_pit_validator),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> PITValidationResult:
    result = pit.validate_event_rows(req)
    audit.log(
        event_type="pit_validation",
        action="validate_events",
        payload={"symbol": req.symbol, "passed": result.passed, "rows": len(req.rows)},
    )
    return result
