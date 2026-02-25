from __future__ import annotations

import logging
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.core.container import (
    get_audit_service,
    get_autotune_service,
    get_data_license_service,
    get_data_provider,
    get_event_service,
    get_factor_engine,
    get_fundamental_service,
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
from trading_assistant.fundamentals.service import FundamentalService
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
from trading_assistant.trading.costs import (
    estimate_roundtrip_cost_bps,
    infer_expected_edge_bps,
    required_cash_for_min_lot,
)
from trading_assistant.trading.small_capital import apply_small_capital_overrides

router = APIRouter(prefix="/signals", tags=["signals"])
logger = logging.getLogger(__name__)


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
    fundamentals: FundamentalService = Depends(get_fundamental_service),
    pit: PITValidator = Depends(get_pit_validator),
    events: EventService = Depends(get_event_service),
    registry: StrategyRegistry = Depends(get_strategy_registry),
    autotune: AutoTuneService = Depends(get_autotune_service),
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
    strategy_params, autotune_profile = autotune.resolve_runtime_params(
        strategy_name=req.strategy_name,
        symbol=req.symbol,
        explicit_params=req.strategy_params,
        use_profile=req.use_autotune_profile,
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

    status = {
        "is_st": bool(bars.iloc[-1].get("is_st", False)),
        "is_suspended": bool(bars.iloc[-1].get("is_suspended", False)),
    }
    try:
        fetched_status = provider.get_security_status(req.symbol)
        status["is_st"] = bool(fetched_status.get("is_st", status["is_st"]))
        status["is_suspended"] = bool(fetched_status.get("is_suspended", status["is_suspended"]))
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Security status lookup failed for %s in signal generation; fallback to bars/default status: %s",
            req.symbol,
            exc,
        )
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
    use_fundamental_enrichment = settings.enable_fundamental_enrichment and req.enable_fundamental_enrichment
    fundamental_stats: dict[str, object] = {"available": False, "source": None}
    if use_fundamental_enrichment:
        bars, fundamental_stats = fundamentals.enrich_bars(
            symbol=req.symbol,
            bars=bars,
            as_of=req.end_date,
            max_staleness_days=req.fundamental_max_staleness_days,
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
    small_capital_enabled = bool(settings.small_capital_mode_enabled or req.enable_small_capital_mode)
    small_capital_principal = float(req.small_capital_principal or settings.small_capital_principal_cny)
    small_lot_size = max(1, int(settings.small_capital_lot_size))
    context = StrategyContext(
        params=strategy_params,
        market_state={
            "enable_small_capital_mode": small_capital_enabled,
            "small_capital_principal": small_capital_principal,
            "small_capital_lot_size": small_lot_size,
            "small_capital_cash_buffer_ratio": settings.small_capital_cash_buffer_ratio,
            "commission_rate": settings.default_commission_rate,
            "min_commission_cny": settings.fee_min_commission_cny,
            "transfer_fee_rate": settings.fee_transfer_rate,
            "stamp_duty_sell_rate": settings.fee_stamp_duty_sell_rate,
            "slippage_rate": settings.default_slippage_rate,
        },
    )
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
    latest_close = float(latest.get("close", 0.0))
    roundtrip_cost_bps = estimate_roundtrip_cost_bps(
        price=latest_close,
        lot_size=small_lot_size,
        commission_rate=settings.default_commission_rate,
        min_commission=settings.fee_min_commission_cny,
        transfer_fee_rate=settings.fee_transfer_rate,
        stamp_duty_sell_rate=settings.fee_stamp_duty_sell_rate,
        slippage_rate=settings.default_slippage_rate,
    )
    min_lot_cash = required_cash_for_min_lot(
        price=latest_close,
        lot_size=small_lot_size,
        commission_rate=settings.default_commission_rate,
        min_commission=settings.fee_min_commission_cny,
        transfer_fee_rate=settings.fee_transfer_rate,
    )

    results: list[TradePrepSheet] = []
    def _opt_float(value):
        return float(value) if (value is not None and value == value) else None

    latest_tushare_disclosure_risk = _opt_float(latest.get("tushare_disclosure_risk_score"))
    latest_tushare_audit_risk = _opt_float(latest.get("tushare_audit_opinion_risk"))
    latest_tushare_forecast_mid = _opt_float(latest.get("tushare_forecast_pchg_mid"))
    latest_tushare_pledge_ratio = _opt_float(latest.get("tushare_pledge_ratio"))
    latest_tushare_unlock_ratio = _opt_float(latest.get("tushare_share_float_unlock_ratio"))
    latest_tushare_holder_crowding = _opt_float(latest.get("tushare_holder_crowding_ratio"))
    latest_tushare_overhang_risk = _opt_float(latest.get("tushare_overhang_risk_score"))

    for signal in candidates:
        signal_id = uuid4().hex
        signal.metadata["signal_id"] = signal_id
        if req.industry:
            signal.metadata["industry"] = req.industry
        _ = apply_small_capital_overrides(
            signal=signal,
            enable_small_capital_mode=small_capital_enabled,
            principal=small_capital_principal,
            latest_price=latest_close,
            lot_size=small_lot_size,
            commission_rate=settings.default_commission_rate,
            min_commission=settings.fee_min_commission_cny,
            transfer_fee_rate=settings.fee_transfer_rate,
            cash_buffer_ratio=settings.small_capital_cash_buffer_ratio,
            max_single_position=settings.max_single_position,
            max_positions=max(1, int(float(strategy_params.get("max_positions", 3)))),
        )
        available_cash = (
            float(req.portfolio_snapshot.cash)
            if (req.portfolio_snapshot is not None and req.portfolio_snapshot.cash is not None)
            else small_capital_principal
        )
        expected_edge_bps = infer_expected_edge_bps(
            confidence=float(signal.confidence),
            momentum20=float(latest.get("momentum20", 0.0)),
            event_score=float(latest.get("event_score", 0.0)) if "event_score" in latest else None,
            fundamental_score=float(latest.get("fundamental_score", 0.5))
            if bool(latest.get("fundamental_available", False))
            else None,
        )
        signal.metadata["small_capital_mode"] = small_capital_enabled
        signal.metadata["small_capital_principal"] = round(small_capital_principal, 2)
        signal.metadata["small_capital_lot_size"] = small_lot_size
        signal.metadata["required_cash_for_min_lot"] = round(min_lot_cash, 4)
        signal.metadata["estimated_roundtrip_cost_bps"] = round(roundtrip_cost_bps, 3)
        signal.metadata["expected_edge_bps"] = round(expected_edge_bps, 3)

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
            fundamental_score=float(latest.get("fundamental_score", 0.5))
            if bool(latest.get("fundamental_available", False))
            else None,
            fundamental_available=bool(latest.get("fundamental_available", False)),
            fundamental_pit_ok=bool(latest.get("fundamental_pit_ok", True)),
            fundamental_stale_days=(
                int(latest.get("fundamental_stale_days", -1))
                if int(latest.get("fundamental_stale_days", -1)) >= 0
                else None
            ),
            tushare_disclosure_risk_score=latest_tushare_disclosure_risk,
            tushare_audit_opinion_risk=latest_tushare_audit_risk,
            tushare_forecast_pchg_mid=latest_tushare_forecast_mid,
            tushare_pledge_ratio=latest_tushare_pledge_ratio,
            tushare_share_float_unlock_ratio=latest_tushare_unlock_ratio,
            tushare_holder_crowding_ratio=latest_tushare_holder_crowding,
            tushare_overhang_risk_score=latest_tushare_overhang_risk,
            enable_small_capital_mode=small_capital_enabled,
            small_capital_principal=small_capital_principal,
            available_cash=available_cash,
            latest_price=latest_close if latest_close > 0 else None,
            lot_size=small_lot_size,
            required_cash_for_min_lot=min_lot_cash,
            estimated_roundtrip_cost_bps=roundtrip_cost_bps,
            expected_edge_bps=expected_edge_bps,
            min_expected_edge_bps=float(req.small_capital_min_expected_edge_bps),
            small_capital_cash_buffer_ratio=settings.small_capital_cash_buffer_ratio,
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
            "fundamental_enriched": use_fundamental_enrichment,
            "fundamental_available": bool(fundamental_stats.get("available", False)),
            "fundamental_source": fundamental_stats.get("source"),
            "fundamental_pit_ok": fundamental_stats.get("pit_ok"),
            "fundamental_score": round(float(latest.get("fundamental_score", 0.5)), 6)
            if bool(latest.get("fundamental_available", False))
            else None,
            "tushare_advanced_score": round(float(latest.get("tushare_advanced_score", 0.5)), 6)
            if bool(latest.get("tushare_advanced_available", False))
            else None,
            "tushare_disclosure_risk_score": round(float(latest_tushare_disclosure_risk), 6)
            if latest_tushare_disclosure_risk is not None
            else None,
            "tushare_overhang_risk_score": round(float(latest_tushare_overhang_risk), 6)
            if latest_tushare_overhang_risk is not None
            else None,
            "small_capital_mode": small_capital_enabled,
            "small_capital_principal": round(small_capital_principal, 2),
            "small_capital_roundtrip_cost_bps": round(roundtrip_cost_bps, 3),
            "autotune_profile_id": (autotune_profile.id if autotune_profile is not None else None),
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return results
