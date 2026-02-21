from __future__ import annotations

from datetime import datetime, time, timezone

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import (
    get_audit_service,
    get_data_provider,
    get_event_service,
    get_data_quality_service,
    get_pit_validator,
    get_strategy_governance_service,
    get_strategy_registry,
)
from trading_assistant.core.models import (
    ComplianceCheckItem,
    CompliancePreflightRequest,
    CompliancePreflightResult,
    DataQualityRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.registry import StrategyRegistry

router = APIRouter(prefix="/compliance", tags=["compliance"])


@router.post("/preflight", response_model=CompliancePreflightResult)
def compliance_preflight(
    req: CompliancePreflightRequest,
    provider: CompositeDataProvider = Depends(get_data_provider),
    events: EventService = Depends(get_event_service),
    quality: DataQualityService = Depends(get_data_quality_service),
    pit: PITValidator = Depends(get_pit_validator),
    registry: StrategyRegistry = Depends(get_strategy_registry),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.AUDIT, UserRole.RESEARCH)),
) -> CompliancePreflightResult:
    checks: list[ComplianceCheckItem] = []
    try:
        _ = registry.get(req.strategy_name)
        checks.append(ComplianceCheckItem(check_name="strategy_exists", passed=True, message="Strategy exists."))
    except KeyError:
        checks.append(ComplianceCheckItem(check_name="strategy_exists", passed=False, message="Strategy not found."))
        result = CompliancePreflightResult(passed=False, checks=checks)
        audit.log("compliance", "preflight", {"symbol": req.symbol, "passed": False}, status="ERROR")
        return result

    if settings.enforce_approved_strategy:
        approved = strategy_gov.is_approved(req.strategy_name)
        checks.append(
            ComplianceCheckItem(
                check_name="strategy_approved",
                passed=approved,
                message="Approved strategy found." if approved else "No approved version found.",
            )
        )

    try:
        used_provider, bars = provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    quality_result = quality.evaluate(
        req=DataQualityRequest(symbol=req.symbol, start_date=req.start_date, end_date=req.end_date),
        bars=bars,
        provider=used_provider,
    )
    checks.append(
        ComplianceCheckItem(
            check_name="data_quality",
            passed=quality_result.passed,
            message=f"Data quality issues: {len(quality_result.issues)}",
        )
    )

    pit_result = pit.validate_bars(symbol=req.symbol, provider=used_provider, bars=bars, as_of=req.end_date)
    checks.append(
        ComplianceCheckItem(
            check_name="pit_validation",
            passed=pit_result.passed,
            message=f"PIT issues: {len(pit_result.issues)}",
        )
    )

    if req.strategy_name == "event_driven":
        event_rows = events.list_events(
            symbol=req.symbol,
            start_time=datetime.combine(req.start_date, time.min, tzinfo=timezone.utc),
            end_time=datetime.combine(req.end_date, time.max, tzinfo=timezone.utc),
            limit=2000,
        )
        checks.append(
            ComplianceCheckItem(
                check_name="event_data_ready",
                passed=len(event_rows) > 0,
                message=f"Event rows found: {len(event_rows)}",
            )
        )

    passed = all(c.passed for c in checks)
    audit.log(
        event_type="compliance",
        action="preflight",
        payload={"symbol": req.symbol, "strategy": req.strategy_name, "passed": passed, "checks": len(checks)},
    )
    return CompliancePreflightResult(passed=passed, checks=checks)
