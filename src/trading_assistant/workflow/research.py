from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from trading_assistant.core.models import (
    DataLicenseCheckRequest,
    OptimizeCandidate,
    PortfolioOptimizeRequest,
    ResearchWorkflowRequest,
    ResearchWorkflowResult,
    RiskCheckRequest,
    SignalAction,
    SignalLevel,
    SignalDecisionRecord,
    WorkflowSignalItem,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.replay.service import ReplayService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.registry import StrategyRegistry
from trading_assistant.trading.costs import (
    estimate_roundtrip_cost_bps,
    infer_expected_edge_bps,
    required_cash_for_min_lot,
)
from trading_assistant.trading.small_capital import apply_small_capital_overrides


class ResearchWorkflowService:
    def __init__(
        self,
        provider: CompositeDataProvider,
        factor_engine: FactorEngine,
        registry: StrategyRegistry,
        risk_engine: RiskEngine,
        optimizer: PortfolioOptimizer,
        replay: ReplayService,
        pit_validator: PITValidator,
        event_service: EventService | None = None,
        license_service: DataLicenseService | None = None,
        enforce_data_license: bool = False,
        fundamental_service: FundamentalService | None = None,
        default_commission_rate: float = 0.0003,
        default_slippage_rate: float = 0.0005,
        fee_min_commission_cny: float = 5.0,
        fee_stamp_duty_sell_rate: float = 0.0005,
        fee_transfer_rate: float = 0.00001,
        small_capital_mode_enabled: bool = False,
        small_capital_principal_cny: float = 2000.0,
        small_capital_cash_buffer_ratio: float = 0.10,
        small_capital_min_expected_edge_bps: float = 80.0,
        small_capital_lot_size: int = 100,
    ) -> None:
        self.provider = provider
        self.fundamental_service = fundamental_service or FundamentalService(provider=provider)
        self.factor_engine = factor_engine
        self.registry = registry
        self.risk_engine = risk_engine
        self.optimizer = optimizer
        self.replay = replay
        self.pit_validator = pit_validator
        self.event_service = event_service
        self.license_service = license_service
        self.enforce_data_license = enforce_data_license
        self.default_commission_rate = float(default_commission_rate)
        self.default_slippage_rate = float(default_slippage_rate)
        self.fee_min_commission_cny = float(fee_min_commission_cny)
        self.fee_stamp_duty_sell_rate = float(fee_stamp_duty_sell_rate)
        self.fee_transfer_rate = float(fee_transfer_rate)
        self.small_capital_mode_enabled = bool(small_capital_mode_enabled)
        self.small_capital_principal_cny = float(small_capital_principal_cny)
        self.small_capital_cash_buffer_ratio = float(small_capital_cash_buffer_ratio)
        self.small_capital_min_expected_edge_bps = float(small_capital_min_expected_edge_bps)
        self.small_capital_lot_size = max(1, int(small_capital_lot_size))

    def run(self, req: ResearchWorkflowRequest) -> ResearchWorkflowResult:
        strategy = self.registry.get(req.strategy_name)
        signals: list[WorkflowSignalItem] = []
        optimize_candidates: list[OptimizeCandidate] = []
        use_event_enrichment = req.enable_event_enrichment or req.strategy_name == "event_driven"

        for symbol in req.symbols:
            try:
                used_provider, bars = self.provider.get_daily_bars_with_source(symbol, req.start_date, req.end_date)
            except Exception:  # noqa: BLE001
                continue
            if bars.empty:
                continue
            if self.license_service is not None:
                check = self.license_service.check(
                    DataLicenseCheckRequest(
                        dataset_name="daily_bars",
                        provider=used_provider,
                        requested_usage="internal_research",
                        export_requested=False,
                        expected_rows=len(bars),
                        as_of=req.end_date,
                    )
                )
                if self.enforce_data_license and not check.allowed:
                    continue
            pit = self.pit_validator.validate_bars(symbol=symbol, provider=used_provider, bars=bars, as_of=req.end_date)
            if not pit.passed:
                continue

            status = self.provider.get_security_status(symbol)
            bars["is_st"] = bool(status.get("is_st", False))
            bars["is_suspended"] = bool(status.get("is_suspended", False))
            event_stats = {"events_loaded": 0}
            bars_for_features = bars
            if use_event_enrichment and self.event_service is not None:
                bars_for_features, event_stats = self.event_service.enrich_bars(
                    symbol=symbol,
                    bars=bars,
                    lookback_days=req.event_lookback_days,
                    decay_half_life_days=req.event_decay_half_life_days,
                )
            fundamental_stats: dict[str, object] = {"available": False, "source": None}
            if req.enable_fundamental_enrichment and self.fundamental_service is not None:
                bars_for_features, fundamental_stats = self.fundamental_service.enrich_bars(
                    symbol=symbol,
                    bars=bars_for_features,
                    as_of=req.end_date,
                    max_staleness_days=req.fundamental_max_staleness_days,
                )
            features = self.factor_engine.compute(bars_for_features)
            small_capital_mode = bool(self.small_capital_mode_enabled or req.enable_small_capital_mode)
            small_capital_principal = float(req.small_capital_principal or self.small_capital_principal_cny)
            small_lot = max(1, self.small_capital_lot_size)
            strategy_signals = strategy.generate(
                features,
                StrategyContext(
                    params=req.strategy_params,
                    market_state={
                        "enable_small_capital_mode": small_capital_mode,
                        "small_capital_principal": small_capital_principal,
                        "small_capital_lot_size": small_lot,
                        "small_capital_cash_buffer_ratio": self.small_capital_cash_buffer_ratio,
                        "commission_rate": self.default_commission_rate,
                        "min_commission_cny": self.fee_min_commission_cny,
                        "transfer_fee_rate": self.fee_transfer_rate,
                        "stamp_duty_sell_rate": self.fee_stamp_duty_sell_rate,
                        "slippage_rate": self.default_slippage_rate,
                    },
                ),
            )
            if not strategy_signals:
                continue
            latest = features.sort_values("trade_date").iloc[-1]
            signal = strategy_signals[-1]
            industry = req.industry_map.get(symbol)
            if industry:
                signal.metadata["industry"] = industry
            latest_close = float(latest.get("close", 0.0))
            _ = apply_small_capital_overrides(
                signal=signal,
                enable_small_capital_mode=small_capital_mode,
                principal=small_capital_principal,
                latest_price=latest_close,
                lot_size=small_lot,
                commission_rate=self.default_commission_rate,
                min_commission=self.fee_min_commission_cny,
                transfer_fee_rate=self.fee_transfer_rate,
                cash_buffer_ratio=self.small_capital_cash_buffer_ratio,
                max_single_position=float(req.max_single_position),
                max_positions=max(1, int(float(req.strategy_params.get("max_positions", 3)))),
            )
            required_cash = required_cash_for_min_lot(
                price=latest_close,
                lot_size=small_lot,
                commission_rate=self.default_commission_rate,
                min_commission=self.fee_min_commission_cny,
                transfer_fee_rate=self.fee_transfer_rate,
            )
            roundtrip_cost_bps = estimate_roundtrip_cost_bps(
                price=latest_close,
                lot_size=small_lot,
                commission_rate=self.default_commission_rate,
                min_commission=self.fee_min_commission_cny,
                transfer_fee_rate=self.fee_transfer_rate,
                stamp_duty_sell_rate=self.fee_stamp_duty_sell_rate,
                slippage_rate=self.default_slippage_rate,
            )
            expected_edge_bps = infer_expected_edge_bps(
                confidence=float(signal.confidence),
                momentum20=float(latest.get("momentum20", 0.0)),
                event_score=float(latest.get("event_score", 0.0)) if "event_score" in latest else None,
                fundamental_score=float(latest.get("fundamental_score", 0.5))
                if bool(latest.get("fundamental_available", False))
                else None,
            )

            risk = self.risk_engine.evaluate(
                RiskCheckRequest(
                    signal=signal,
                    is_st=bool(status.get("is_st", False)),
                    is_suspended=bool(status.get("is_suspended", False)),
                    avg_turnover_20d=float(latest.get("turnover20", 0.0)),
                    symbol_industry=industry,
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
                    enable_small_capital_mode=small_capital_mode,
                    small_capital_principal=small_capital_principal,
                    available_cash=small_capital_principal,
                    latest_price=latest_close if latest_close > 0 else None,
                    lot_size=small_lot,
                    required_cash_for_min_lot=required_cash,
                    estimated_roundtrip_cost_bps=roundtrip_cost_bps,
                    expected_edge_bps=expected_edge_bps,
                    min_expected_edge_bps=float(req.small_capital_min_expected_edge_bps),
                    small_capital_cash_buffer_ratio=self.small_capital_cash_buffer_ratio,
                )
            )
            small_hits_all = [x for x in risk.hits if x.rule_name == "small_capital_tradability"]
            small_hits_failed = [x for x in small_hits_all if not x.passed]
            small_note = (small_hits_failed[0].message if small_hits_failed else (small_hits_all[0].message if small_hits_all else None))
            small_blocked = any(x.level == SignalLevel.CRITICAL for x in small_hits_failed)
            signal_id = uuid4().hex
            self.replay.record_signal(
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

            signals.append(
                WorkflowSignalItem(
                    symbol=signal.symbol,
                    provider=used_provider,
                    action=signal.action,
                    confidence=signal.confidence,
                    blocked=risk.blocked,
                    level=risk.level,
                    reason=signal.reason,
                    suggested_position=signal.suggested_position,
                    signal_id=signal_id,
                    event_rows_used=int(event_stats.get("events_loaded", 0)),
                    fundamental_available=bool(latest.get("fundamental_available", False)),
                    fundamental_score=float(latest.get("fundamental_score", 0.5))
                    if bool(latest.get("fundamental_available", False))
                    else None,
                    fundamental_source=str(fundamental_stats.get("source")) if fundamental_stats.get("source") else None,
                    small_capital_blocked=small_blocked,
                    small_capital_note=small_note,
                )
            )
            if signal.action == SignalAction.BUY and (not risk.blocked):
                fundamental_available = bool(latest.get("fundamental_available", False))
                fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
                momentum = float(latest.get("momentum20", 0.0))
                blended_expected_return = 0.7 * momentum + 0.3 * (fundamental_score - 0.5)
                optimize_candidates.append(
                    OptimizeCandidate(
                        symbol=signal.symbol,
                        expected_return=blended_expected_return,
                        volatility=max(0.001, float(latest.get("volatility20", 0.01))),
                        industry=industry or "UNKNOWN",
                        liquidity_score=min(1.0, float(latest.get("turnover20", 0.0)) / 30_000_000),
                    )
                )

        optimized = None
        if req.optimize_portfolio and optimize_candidates:
            optimized = self.optimizer.optimize(
                PortfolioOptimizeRequest(
                    candidates=optimize_candidates,
                    max_single_position=req.max_single_position,
                    max_industry_exposure=req.max_industry_exposure,
                    target_gross_exposure=req.target_gross_exposure,
                )
            )

        return ResearchWorkflowResult(
            run_id=uuid4().hex,
            generated_at=datetime.now(timezone.utc),
            strategy_name=req.strategy_name,
            signals=signals,
            optimized_portfolio=optimized,
        )
