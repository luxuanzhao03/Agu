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
    SignalDecisionRecord,
    WorkflowSignalItem,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.replay.service import ReplayService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.registry import StrategyRegistry


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
    ) -> None:
        self.provider = provider
        self.factor_engine = factor_engine
        self.registry = registry
        self.risk_engine = risk_engine
        self.optimizer = optimizer
        self.replay = replay
        self.pit_validator = pit_validator
        self.event_service = event_service
        self.license_service = license_service
        self.enforce_data_license = enforce_data_license

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
            features = self.factor_engine.compute(bars_for_features)
            strategy_signals = strategy.generate(features, StrategyContext(params=req.strategy_params))
            if not strategy_signals:
                continue
            signal = strategy_signals[-1]
            industry = req.industry_map.get(symbol)
            if industry:
                signal.metadata["industry"] = industry

            risk = self.risk_engine.evaluate(
                RiskCheckRequest(
                    signal=signal,
                    is_st=bool(status.get("is_st", False)),
                    is_suspended=bool(status.get("is_suspended", False)),
                    avg_turnover_20d=float(features.iloc[-1].get("turnover20", 0.0)),
                    symbol_industry=industry,
                )
            )
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
                )
            )
            if signal.action == SignalAction.BUY and (not risk.blocked):
                optimize_candidates.append(
                    OptimizeCandidate(
                        symbol=signal.symbol,
                        expected_return=float(features.iloc[-1].get("momentum20", 0.0)),
                        volatility=max(0.001, float(features.iloc[-1].get("volatility20", 0.01))),
                        industry=industry or "UNKNOWN",
                        liquidity_score=min(1.0, float(features.iloc[-1].get("turnover20", 0.0)) / 30_000_000),
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
