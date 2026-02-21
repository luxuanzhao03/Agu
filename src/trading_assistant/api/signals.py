from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import (
    get_audit_service,
    get_data_license_service,
    get_data_provider,
    get_event_service,
    get_factor_engine,
    get_pit_validator,
    get_risk_engine,
    get_signal_service,
    get_snapshot_service,
    get_strategy_governance_service,
    get_strategy_registry,
    get_replay_service,
)
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.models import (
    DataLicenseCheckRequest,
    DataSnapshotRegisterRequest,
    GenerateSignalRequest,
    RiskCheckRequest,
    SignalDecisionRecord,
    TradePrepSheet,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.replay.service import ReplayService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.signal.service import SignalService
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.registry import StrategyRegistry

router = APIRouter(prefix="/signals", tags=["signals"])


def _infer_limit_flags(close_today: float, close_yesterday: float, is_st: bool) -> tuple[bool, bool]:
    if close_yesterday <= 0:
        return False, False
    pct = close_today / close_yesterday - 1
    limit = 0.05 if is_st else 0.10
    at_limit_up = pct >= (limit - 0.0005)
    at_limit_down = pct <= (-limit + 0.0005)
    return at_limit_up, at_limit_down


@router.post("/generate", response_model=list[TradePrepSheet])
def generate_signals(
    req: GenerateSignalRequest,
    provider: CompositeDataProvider = Depends(get_data_provider),
    license_service: DataLicenseService = Depends(get_data_license_service),
    factor_engine: FactorEngine = Depends(get_factor_engine),
    pit: PITValidator = Depends(get_pit_validator),
    events: EventService = Depends(get_event_service),
    registry: StrategyRegistry = Depends(get_strategy_registry),
    risk_engine: RiskEngine = Depends(get_risk_engine),
    signal_service: SignalService = Depends(get_signal_service),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    replay: ReplayService = Depends(get_replay_service),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK)),
) -> list[TradePrepSheet]:
    try:
        strategy = registry.get(req.strategy_name)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if settings.enforce_approved_strategy and not strategy_gov.is_approved(req.strategy_name):
        raise HTTPException(
            status_code=403,
            detail=f"Strategy '{req.strategy_name}' has no approved version.",
        )

    try:
        used_provider, bars = provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
    except DataProviderError as exc:
        audit.log(
            event_type="signal_generation",
            action="generate",
            payload={"symbol": req.symbol, "strategy": req.strategy_name, "error": str(exc)},
            status="ERROR",
        )
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    if bars.empty:
        raise HTTPException(status_code=404, detail="No market data available for requested range.")
    license_check = license_service.check(
        DataLicenseCheckRequest(
            dataset_name="daily_bars",
            provider=used_provider,
            requested_usage="internal_research",
            export_requested=False,
            expected_rows=len(bars),
            as_of=req.end_date,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_signal_generation",
            payload={"symbol": req.symbol, "provider": used_provider, "reason": license_check.reason},
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    pit_result = pit.validate_bars(symbol=req.symbol, provider=used_provider, bars=bars, as_of=req.end_date)
    if not pit_result.passed:
        raise HTTPException(status_code=422, detail={"message": "PIT validation failed", "issues": pit_result.model_dump()})

    status = provider.get_security_status(req.symbol)
    bars["is_st"] = bool(status.get("is_st", False))
    bars["is_suspended"] = bool(status.get("is_suspended", False))
    use_event_enrichment = req.enable_event_enrichment or req.strategy_name == "event_driven"
    event_stats = {"events_loaded": 0}
    if use_event_enrichment:
        bars, event_stats = events.enrich_bars(
            symbol=req.symbol,
            bars=bars,
            lookback_days=req.event_lookback_days,
            decay_half_life_days=req.event_decay_half_life_days,
        )
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

    features = factor_engine.compute(bars)
    context = StrategyContext(params=req.strategy_params)
    candidates = strategy.generate(features, context=context)
    if not candidates:
        return []

    latest = features.sort_values("trade_date").iloc[-1]
    previous = features.sort_values("trade_date").iloc[-2] if len(features) >= 2 else latest
    at_limit_up, at_limit_down = _infer_limit_flags(
        close_today=float(latest["close"]),
        close_yesterday=float(previous["close"]),
        is_st=bool(status.get("is_st", False)),
    )
    avg_turnover_20d = float(latest.get("turnover20", 0.0))

    results: list[TradePrepSheet] = []
    for signal in candidates:
        signal_id = uuid4().hex
        signal.metadata["signal_id"] = signal_id
        if req.industry:
            signal.metadata["industry"] = req.industry

        risk_req = RiskCheckRequest(
            signal=signal,
            position=req.current_position,
            portfolio=req.portfolio_snapshot,
            is_st=bool(status.get("is_st", False)),
            is_suspended=bool(status.get("is_suspended", False)),
            at_limit_up=at_limit_up,
            at_limit_down=at_limit_down,
            avg_turnover_20d=avg_turnover_20d,
            symbol_industry=req.industry,
        )
        risk_result = risk_engine.evaluate(risk_req)
        results.append(signal_service.to_trade_prep_sheet(signal, risk_result))
        replay.record_signal(
            SignalDecisionRecord(
                signal_id=signal_id,
                symbol=signal.symbol,
                strategy_name=signal.strategy_name,
                trade_date=signal.trade_date,
                action=signal.action,
                confidence=signal.confidence,
                reason=signal.reason,
                suggested_position=signal.suggested_position,
            )
        )

    audit.log(
        event_type="signal_generation",
        action="generate",
        payload={
            "symbol": req.symbol,
            "strategy": req.strategy_name,
            "provider": used_provider,
            "signals": len(results),
            "snapshot_id": snapshot_id,
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
            "event_enriched": use_event_enrichment,
            "event_rows_used": int(event_stats.get("events_loaded", 0)),
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return results
