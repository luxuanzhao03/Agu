from __future__ import annotations

from datetime import datetime, timezone
import math
from pathlib import Path

import pandas as pd

from trading_assistant.backtest.engine import BacktestEngine
from trading_assistant.core.config import Settings
from trading_assistant.core.models import (
    BacktestMetrics,
    BacktestRequest,
    EventFeatureBacktestCompareRequest,
    EventFeatureBacktestCompareResult,
    EventFeatureBacktestDelta,
    EventFeatureSignalDiagnostics,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.registry import StrategyRegistry


class EventFeatureBacktestCompareService:
    def __init__(
        self,
        *,
        provider: CompositeDataProvider,
        factor_engine: FactorEngine,
        pit: PITValidator,
        event_service: EventService,
        registry: StrategyRegistry,
        settings: Settings,
        output_dir: str = "reports",
    ) -> None:
        self.provider = provider
        self.factor_engine = factor_engine
        self.pit = pit
        self.event_service = event_service
        self.registry = registry
        self.settings = settings
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def compare(self, req: EventFeatureBacktestCompareRequest) -> EventFeatureBacktestCompareResult:
        strategy = self.registry.get(req.strategy_name)
        provider_name, bars = self.provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
        if bars.empty:
            raise ValueError("no bars available for compare request")

        pit_result = self.pit.validate_bars(symbol=req.symbol, provider=provider_name, bars=bars, as_of=req.end_date)
        if not pit_result.passed:
            issue = pit_result.issues[0].message if pit_result.issues else "pit validation failed"
            raise ValueError(issue)

        status = self.provider.get_security_status(req.symbol)
        bars = bars.copy()
        bars["is_st"] = bool(status.get("is_st", False))
        bars["is_suspended"] = bool(status.get("is_suspended", False))

        baseline_req = BacktestRequest(
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            strategy_name=req.strategy_name,
            strategy_params=req.strategy_params,
            enable_event_enrichment=False,
            initial_cash=req.initial_cash,
            commission_rate=req.commission_rate,
            slippage_rate=req.slippage_rate,
            lot_size=req.lot_size,
            max_single_position=req.max_single_position,
        )
        enriched_req = baseline_req.model_copy(update={"enable_event_enrichment": True})

        baseline_engine = self._build_engine()
        baseline_result = baseline_engine.run(bars=bars, req=baseline_req, strategy=strategy)

        enriched_bars, event_stats = self.event_service.enrich_bars(
            symbol=req.symbol,
            bars=bars,
            lookback_days=req.event_lookback_days,
            decay_half_life_days=req.event_decay_half_life_days,
        )
        enriched_engine = self._build_engine()
        enriched_result = enriched_engine.run(bars=enriched_bars, req=enriched_req, strategy=strategy)

        diagnostics = self._diagnostics(enriched_bars=enriched_bars, events_loaded=int(event_stats.get("events_loaded", 0)))
        delta = self._delta(baseline=baseline_result.metrics, enriched=enriched_result.metrics)
        report_content = self._build_report(
            req=req,
            provider_name=provider_name,
            baseline=baseline_result.metrics,
            enriched=enriched_result.metrics,
            delta=delta,
            diagnostics=diagnostics,
        )
        report_path = self._save_report(req, report_content)

        return EventFeatureBacktestCompareResult(
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            provider=provider_name,
            baseline=baseline_result.metrics,
            enriched=enriched_result.metrics,
            delta=delta,
            diagnostics=diagnostics,
            report_content=report_content,
            report_path=report_path,
        )

    def _build_engine(self) -> BacktestEngine:
        return BacktestEngine(
            factor_engine=self.factor_engine,
            risk_engine=RiskEngine(
                max_single_position=self.settings.max_single_position,
                max_drawdown=self.settings.max_drawdown,
                max_industry_exposure=self.settings.max_industry_exposure,
                min_turnover_20d=self.settings.min_turnover_20d,
            ),
        )

    @staticmethod
    def _delta(*, baseline: BacktestMetrics, enriched: BacktestMetrics) -> EventFeatureBacktestDelta:
        return EventFeatureBacktestDelta(
            total_return_delta=round(enriched.total_return - baseline.total_return, 6),
            max_drawdown_delta=round(enriched.max_drawdown - baseline.max_drawdown, 6),
            trade_count_delta=int(enriched.trade_count - baseline.trade_count),
            win_rate_delta=round(enriched.win_rate - baseline.win_rate, 6),
            annualized_return_delta=round(enriched.annualized_return - baseline.annualized_return, 6),
            sharpe_delta=round(enriched.sharpe - baseline.sharpe, 6),
        )

    @staticmethod
    def _diagnostics(*, enriched_bars: pd.DataFrame, events_loaded: int) -> EventFeatureSignalDiagnostics:
        if enriched_bars.empty:
            return EventFeatureSignalDiagnostics(
                events_loaded=events_loaded,
                event_rows_covered=0,
                event_row_ratio=0.0,
                avg_event_score=0.0,
                avg_negative_event_score=0.0,
                score_return_corr_1d=None,
            )

        frame = enriched_bars.copy()
        if "event_score" not in frame.columns:
            frame["event_score"] = 0.0
        if "negative_event_score" not in frame.columns:
            frame["negative_event_score"] = 0.0
        if "event_count" not in frame.columns:
            frame["event_count"] = 0
        event_rows = int(frame["event_count"].fillna(0).astype(int).gt(0).sum())
        row_count = len(frame)
        ratio = 0.0 if row_count == 0 else event_rows / row_count

        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["fwd_ret_1d"] = frame["close"].shift(-1) / frame["close"] - 1
        corr = frame[["event_score", "fwd_ret_1d"]].dropna()
        corr_value = None
        if len(corr) >= 3:
            corr_value = corr["event_score"].corr(corr["fwd_ret_1d"])
            if corr_value is not None:
                corr_value = float(corr_value)
                if not math.isfinite(corr_value):
                    corr_value = None

        return EventFeatureSignalDiagnostics(
            events_loaded=events_loaded,
            event_rows_covered=event_rows,
            event_row_ratio=round(ratio, 6),
            avg_event_score=round(float(frame["event_score"].fillna(0.0).mean()), 6),
            avg_negative_event_score=round(float(frame["negative_event_score"].fillna(0.0).mean()), 6),
            score_return_corr_1d=round(corr_value, 6) if corr_value is not None else None,
        )

    @staticmethod
    def _build_report(
        *,
        req: EventFeatureBacktestCompareRequest,
        provider_name: str,
        baseline: BacktestMetrics,
        enriched: BacktestMetrics,
        delta: EventFeatureBacktestDelta,
        diagnostics: EventFeatureSignalDiagnostics,
    ) -> str:
        generated_at = datetime.now(timezone.utc).isoformat()
        lines = [
            "# Event Feature Backtest Comparison",
            "",
            f"- Generated at: {generated_at}",
            f"- Watermark: {req.watermark}",
            f"- Symbol: {req.symbol}",
            f"- Strategy: {req.strategy_name}",
            f"- Provider: {provider_name}",
            f"- Range: {req.start_date.isoformat()} -> {req.end_date.isoformat()}",
            "",
            "## Baseline (No Event Enrichment)",
            f"- Total Return: {baseline.total_return:.4f}",
            f"- Max Drawdown: {baseline.max_drawdown:.4f}",
            f"- Trade Count: {baseline.trade_count}",
            f"- Win Rate: {baseline.win_rate:.4f}",
            f"- Annualized Return: {baseline.annualized_return:.4f}",
            f"- Sharpe: {baseline.sharpe:.4f}",
            "",
            "## Enriched (Event Features Enabled)",
            f"- Total Return: {enriched.total_return:.4f}",
            f"- Max Drawdown: {enriched.max_drawdown:.4f}",
            f"- Trade Count: {enriched.trade_count}",
            f"- Win Rate: {enriched.win_rate:.4f}",
            f"- Annualized Return: {enriched.annualized_return:.4f}",
            f"- Sharpe: {enriched.sharpe:.4f}",
            "",
            "## Delta (Enriched - Baseline)",
            f"- total_return_delta: {delta.total_return_delta:.4f}",
            f"- max_drawdown_delta: {delta.max_drawdown_delta:.4f}",
            f"- trade_count_delta: {delta.trade_count_delta}",
            f"- win_rate_delta: {delta.win_rate_delta:.4f}",
            f"- annualized_return_delta: {delta.annualized_return_delta:.4f}",
            f"- sharpe_delta: {delta.sharpe_delta:.4f}",
            "",
            "## Event Diagnostics",
            f"- events_loaded: {diagnostics.events_loaded}",
            f"- event_rows_covered: {diagnostics.event_rows_covered}",
            f"- event_row_ratio: {diagnostics.event_row_ratio:.4f}",
            f"- avg_event_score: {diagnostics.avg_event_score:.4f}",
            f"- avg_negative_event_score: {diagnostics.avg_negative_event_score:.4f}",
            (
                f"- score_return_corr_1d: {diagnostics.score_return_corr_1d:.4f}"
                if diagnostics.score_return_corr_1d is not None
                else "- score_return_corr_1d: N/A"
            ),
        ]
        return "\n".join(lines)

    def _save_report(self, req: EventFeatureBacktestCompareRequest, content: str) -> str | None:
        if not req.save_report:
            return None
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.output_dir / f"event_feature_compare_{req.symbol}_{ts}.md"
        path.write_text(content, encoding="utf-8")
        return str(path)
