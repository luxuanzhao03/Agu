from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import get_audit_service, get_data_license_service, get_reporting_service
from trading_assistant.core.models import (
    DataLicenseCheckRequest,
    GoLiveReadinessReport,
    ReportGenerateRequest,
    ReportGenerateResult,
    StrategyAccuracyReport,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.reporting.service import ReportingService

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate", response_model=ReportGenerateResult)
def generate_report(
    req: ReportGenerateRequest,
    reporting: ReportingService = Depends(get_reporting_service),
    license_service: DataLicenseService = Depends(get_data_license_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH, UserRole.ADMIN)),
) -> ReportGenerateResult:
    license_check = license_service.check(
        DataLicenseCheckRequest(
            dataset_name="research_report",
            provider="internal",
            requested_usage="internal_research",
            export_requested=req.save_to_file,
            expected_rows=req.limit,
            as_of=req.end_date,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_report_export",
            payload={"report_type": req.report_type, "reason": license_check.reason},
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    if req.save_to_file and license_check.watermark and req.watermark == "For Research Only":
        req = req.model_copy(update={"watermark": license_check.watermark})

    result = reporting.generate(req)
    audit.log(
        event_type="reporting",
        action="generate",
        payload={
            "report_type": req.report_type,
            "strategy_name": req.strategy_name or "",
            "saved_path": result.saved_path or "",
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return result


@router.get("/strategy-accuracy", response_model=StrategyAccuracyReport)
def get_strategy_accuracy_report(
    lookback_days: int = Query(default=90, ge=1, le=3650),
    end_date: date | None = Query(default=None),
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=4000, ge=1, le=20_000),
    reporting: ReportingService = Depends(get_reporting_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH, UserRole.ADMIN)),
) -> StrategyAccuracyReport:
    try:
        result = reporting.strategy_accuracy(
            lookback_days=lookback_days,
            end_date=end_date,
            strategy_name=strategy_name,
            symbol=symbol,
            min_confidence=min_confidence,
            limit=limit,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    audit.log(
        event_type="reporting",
        action="strategy_accuracy",
        payload={
            "lookback_days": lookback_days,
            "end_date": (end_date.isoformat() if end_date else None),
            "strategy_name": strategy_name,
            "symbol": symbol,
            "min_confidence": min_confidence,
            "limit": limit,
            "sample_size": result.sample_size,
            "executed_samples": result.executed_samples,
            "hit_rate": result.hit_rate,
            "brier_score": result.brier_score,
        },
        status="OK",
    )
    return result


@router.get("/go-live-readiness", response_model=GoLiveReadinessReport)
def get_go_live_readiness_report(
    lookback_days: int = Query(default=90, ge=1, le=3650),
    end_date: date | None = Query(default=None),
    strategy_name: str | None = Query(default=None),
    symbol: str | None = Query(default=None),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0),
    limit: int = Query(default=4000, ge=1, le=20_000),
    reporting: ReportingService = Depends(get_reporting_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH, UserRole.ADMIN)),
) -> GoLiveReadinessReport:
    try:
        result = reporting.go_live_readiness(
            lookback_days=lookback_days,
            end_date=end_date,
            strategy_name=strategy_name,
            symbol=symbol,
            min_confidence=min_confidence,
            limit=limit,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    audit.log(
        event_type="reporting",
        action="go_live_readiness",
        payload={
            "lookback_days": lookback_days,
            "end_date": (end_date.isoformat() if end_date else None),
            "strategy_name": strategy_name,
            "symbol": symbol,
            "min_confidence": min_confidence,
            "limit": limit,
            "overall_passed": result.overall_passed,
            "readiness_level": result.readiness_level,
            "failed_gate_count": result.failed_gate_count,
            "warning_gate_count": result.warning_gate_count,
        },
        status=("OK" if result.overall_passed else "ERROR"),
    )
    return result
