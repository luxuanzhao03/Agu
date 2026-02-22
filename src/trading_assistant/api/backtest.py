from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.backtest.engine import BacktestEngine
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import (
    get_audit_service,
    get_data_license_service,
    get_data_provider,
    get_event_service,
    get_factor_engine,
    get_fundamental_service,
    get_pit_validator,
    get_snapshot_service,
    get_strategy_governance_service,
    get_strategy_registry,
)
from trading_assistant.core.models import BacktestRequest, BacktestResult, DataLicenseCheckRequest, DataSnapshotRegisterRequest
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.registry import StrategyRegistry

router = APIRouter(prefix="/backtest", tags=["backtest"])


@router.post("/run", response_model=BacktestResult)
def run_backtest(
    req: BacktestRequest,
    provider: CompositeDataProvider = Depends(get_data_provider),
    license_service: DataLicenseService = Depends(get_data_license_service),
    factor_engine: FactorEngine = Depends(get_factor_engine),
    fundamentals: FundamentalService = Depends(get_fundamental_service),
    pit: PITValidator = Depends(get_pit_validator),
    events: EventService = Depends(get_event_service),
    registry: StrategyRegistry = Depends(get_strategy_registry),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK)),
) -> BacktestResult:
    effective_req = req.model_copy(
        update={
            "enable_small_capital_mode": bool(settings.small_capital_mode_enabled or req.enable_small_capital_mode),
            "small_capital_principal": float(req.small_capital_principal or settings.small_capital_principal_cny),
            "min_commission_cny": float(req.min_commission_cny),
            "stamp_duty_sell_rate": float(req.stamp_duty_sell_rate),
            "transfer_fee_rate": float(req.transfer_fee_rate),
            "commission_rate": float(req.commission_rate),
            "slippage_rate": float(req.slippage_rate),
            "small_capital_min_expected_edge_bps": float(req.small_capital_min_expected_edge_bps),
        }
    )
    if effective_req.enable_small_capital_mode:
        effective_req = effective_req.model_copy(
            update={
                "initial_cash": min(float(effective_req.initial_cash), float(effective_req.small_capital_principal or effective_req.initial_cash)),
                "lot_size": max(int(effective_req.lot_size), int(settings.small_capital_lot_size)),
            }
        )

    try:
        strategy = registry.get(effective_req.strategy_name)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if settings.enforce_approved_strategy and not strategy_gov.is_approved(effective_req.strategy_name):
        raise HTTPException(
            status_code=403,
            detail=f"Strategy '{effective_req.strategy_name}' has no approved version.",
        )

    try:
        used_provider, bars = provider.get_daily_bars_with_source(effective_req.symbol, effective_req.start_date, effective_req.end_date)
    except DataProviderError as exc:
        audit.log(
            event_type="backtest",
            action="run",
            payload={"symbol": effective_req.symbol, "strategy": effective_req.strategy_name, "error": str(exc)},
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
            as_of=effective_req.end_date,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_backtest",
            payload={"symbol": effective_req.symbol, "provider": used_provider, "reason": license_check.reason},
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    pit_result = pit.validate_bars(symbol=effective_req.symbol, provider=used_provider, bars=bars, as_of=effective_req.end_date)
    if not pit_result.passed:
        raise HTTPException(status_code=422, detail={"message": "PIT validation failed", "issues": pit_result.model_dump()})

    status = provider.get_security_status(effective_req.symbol)
    bars["is_st"] = bool(status.get("is_st", False))
    bars["is_suspended"] = bool(status.get("is_suspended", False))
    use_event_enrichment = effective_req.enable_event_enrichment or effective_req.strategy_name == "event_driven"
    event_stats = {"events_loaded": 0}
    if use_event_enrichment:
        bars, event_stats = events.enrich_bars(
            symbol=effective_req.symbol,
            bars=bars,
            lookback_days=effective_req.event_lookback_days,
            decay_half_life_days=effective_req.event_decay_half_life_days,
        )
    use_fundamental_enrichment = settings.enable_fundamental_enrichment and effective_req.enable_fundamental_enrichment
    fundamental_stats: dict[str, object] = {"available": False, "source": None}
    if use_fundamental_enrichment:
        bars, fundamental_stats = fundamentals.enrich_bars(
            symbol=effective_req.symbol,
            bars=bars,
            as_of=effective_req.end_date,
            max_staleness_days=effective_req.fundamental_max_staleness_days,
        )
    snapshot_id = snapshots.register(
        DataSnapshotRegisterRequest(
            dataset_name="daily_bars",
            symbol=effective_req.symbol,
            start_date=effective_req.start_date,
            end_date=effective_req.end_date,
            provider=used_provider,
            row_count=len(bars),
            content_hash=dataframe_content_hash(bars),
        )
    )

    risk_engine = RiskEngine(
        max_single_position=effective_req.max_single_position,
        max_drawdown=settings.max_drawdown,
        max_industry_exposure=settings.max_industry_exposure,
        min_turnover_20d=settings.min_turnover_20d,
        fundamental_buy_warning_score=settings.fundamental_buy_warning_score,
        fundamental_buy_critical_score=settings.fundamental_buy_critical_score,
        fundamental_require_data_for_buy=settings.fundamental_require_data_for_buy,
    )
    engine = BacktestEngine(factor_engine=factor_engine, risk_engine=risk_engine)
    result = engine.run(bars=bars, req=effective_req, strategy=strategy)

    audit.log(
        event_type="backtest",
        action="run",
        payload={
            "symbol": effective_req.symbol,
            "strategy": effective_req.strategy_name,
            "provider": used_provider,
            "total_return": result.metrics.total_return,
            "trade_count": result.metrics.trade_count,
            "snapshot_id": snapshot_id,
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
            "event_enriched": use_event_enrichment,
            "event_rows_used": int(event_stats.get("events_loaded", 0)),
            "fundamental_enriched": use_fundamental_enrichment,
            "fundamental_available": bool(fundamental_stats.get("available", False)),
            "fundamental_source": fundamental_stats.get("source"),
            "fundamental_pit_ok": fundamental_stats.get("pit_ok"),
            "small_capital_mode": effective_req.enable_small_capital_mode,
            "small_capital_principal": round(float(effective_req.small_capital_principal or 0.0), 2),
            "fee_min_commission_cny": effective_req.min_commission_cny,
            "fee_stamp_duty_sell_rate": effective_req.stamp_duty_sell_rate,
            "fee_transfer_rate": effective_req.transfer_fee_rate,
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return result
