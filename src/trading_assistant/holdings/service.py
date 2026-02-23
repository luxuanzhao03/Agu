from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
import math
import statistics

import pandas as pd

from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.core.models import (
    HoldingRecommendationAction,
    ManualHoldingAnalysisPosition,
    ManualHoldingAnalysisRequest,
    ManualHoldingAnalysisResult,
    ManualHoldingPortfolioSummary,
    ManualHoldingPositionItem,
    ManualHoldingPositionsResult,
    ManualHoldingRecommendationItem,
    ManualHoldingSide,
    ManualHoldingTradeCreate,
    ManualHoldingTradeRecord,
    SignalAction,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.event_service import EventService
from trading_assistant.holdings.store import HoldingStore
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.registry import StrategyRegistry


@dataclass
class _PositionState:
    symbol: str
    symbol_name: str
    quantity: int
    avg_cost: float
    lot_size: int


@dataclass
class _IntradayAdvice:
    data_date: date | None
    execution_window: str
    avoid_windows: list[str]
    risk_level: str
    intraday_volatility: float | None
    stop_loss_hint_pct: float | None
    take_profit_hint_pct: float | None
    note: str


@dataclass
class _ForecastEstimate:
    expected_return: float
    up_probability: float
    raw_expected_return: float
    raw_up_probability: float
    calibration_samples: int
    return_calibration_weight: float
    probability_calibration_weight: float
    note: str


class HoldingService:
    def __init__(
        self,
        *,
        store: HoldingStore,
        provider: CompositeDataProvider,
        factor_engine: FactorEngine,
        registry: StrategyRegistry,
        autotune: AutoTuneService,
        fundamental_service: FundamentalService | None = None,
        event_service: EventService | None = None,
    ) -> None:
        self.store = store
        self.provider = provider
        self.factor_engine = factor_engine
        self.registry = registry
        self.autotune = autotune
        self.fundamental_service = fundamental_service
        self.event_service = event_service

    def record_trade(self, req: ManualHoldingTradeCreate) -> ManualHoldingTradeRecord:
        return self.store.insert_trade(req)

    def list_trades(
        self,
        *,
        symbol: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 2000,
    ) -> list[ManualHoldingTradeRecord]:
        return self.store.list_trades(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)

    def delete_trade(self, trade_id: int) -> bool:
        return self.store.delete_trade(trade_id)

    def positions(self, *, as_of_date: date) -> ManualHoldingPositionsResult:
        result, _ = self._build_positions_and_snapshots(as_of_date=as_of_date)
        return result

    def analyze(self, req: ManualHoldingAnalysisRequest) -> ManualHoldingAnalysisResult:
        strategy = self.registry.get(req.strategy_name)
        style_snapshot = self._market_style_snapshot(as_of_date=req.as_of_date)
        positions_result, snapshots = self._build_positions_and_snapshots(
            as_of_date=req.as_of_date,
            style_snapshot=style_snapshot,
        )
        next_trade_date = self._next_trade_date(req.as_of_date)

        analyzed_positions: list[ManualHoldingAnalysisPosition] = []
        recommendations: list[ManualHoldingRecommendationItem] = []

        for item in positions_result.positions:
            snapshot = snapshots.get(item.symbol)
            runtime_params, _ = self._resolve_runtime_params(
                strategy_name=req.strategy_name,
                symbol=item.symbol,
                explicit_params=req.strategy_params,
                use_profile=req.use_autotune_profile,
            )
            signal = SignalAction.WATCH
            expected_ret = 0.0
            up_prob = 0.5
            data_note = ""
            event_score = None
            negative_event_score = None
            style_risk_on_score = None
            style_regime = str(style_snapshot.get("regime", "NEUTRAL"))
            if snapshot is not None:
                signal = self._latest_strategy_signal(strategy, snapshot["features"], runtime_params)
                forecast = self._forecast_next_day(latest_row=snapshot["latest"], features=snapshot["features"])
                expected_ret = float(forecast.expected_return)
                up_prob = float(forecast.up_probability)
                event_score = self._to_float(snapshot["latest"].get("event_score"))
                negative_event_score = self._to_float(snapshot["latest"].get("negative_event_score"))
                style_risk_on_score = self._to_float(snapshot["latest"].get("style_risk_on_score"))
                style_regime = str(snapshot["latest"].get("style_regime") or style_regime or "NEUTRAL").strip().upper()
                if forecast.note:
                    data_note = f"（{forecast.note}）"
            else:
                # Missing bars fallback: still output conservative suggestion.
                expected_ret = self._bounded(float(item.day_change_pct or 0.0) * 0.35, -0.04, 0.04)
                up_prob = self._bounded(0.5 + expected_ret * 3.0, 0.10, 0.90)
                data_note = "（行情不足，按当日变化给出保守估计）"

            suggested_action, delta_lots, risk_flags = self._position_action(
                signal=signal,
                expected_return=expected_ret,
                up_probability=up_prob,
                current_lots=item.lots,
                volatility20=item.volatility20,
                fundamental_score=item.fundamental_score,
                style_risk_on_score=style_risk_on_score,
                style_regime=style_regime,
            )
            if snapshot is None:
                risk_flags.append("NO_MARKET_DATA")

            intraday_advice = self._intraday_execution_advice(
                symbol=item.symbol,
                as_of_date=req.as_of_date,
                side=("SELL" if suggested_action in {HoldingRecommendationAction.REDUCE, HoldingRecommendationAction.EXIT} else "BUY"),
                expected_return=expected_ret,
                volatility20=item.volatility20,
                interval=req.intraday_interval,
                lookback_days=req.intraday_lookback_days,
            )
            if intraday_advice.risk_level.upper() == "HIGH":
                risk_flags.append("INTRADAY_RISK_HIGH")
            if intraday_advice.data_date is None:
                risk_flags.append("NO_INTRADAY_DATA")

            analyzed_positions.append(
                ManualHoldingAnalysisPosition(
                    symbol=item.symbol,
                    symbol_name=item.symbol_name,
                    quantity=item.quantity,
                    lots=item.lots,
                    latest_price=item.latest_price,
                    day_change_pct=item.day_change_pct,
                    market_value=item.market_value,
                    weight=item.weight,
                    momentum20=item.momentum20,
                    volatility20=item.volatility20,
                    fundamental_score=item.fundamental_score,
                    event_score=(round(float(event_score), 6) if event_score is not None else None),
                    negative_event_score=(
                        round(float(negative_event_score), 6) if negative_event_score is not None else None
                    ),
                    style_risk_on_score=(
                        round(float(style_risk_on_score), 6) if style_risk_on_score is not None else None
                    ),
                    style_regime=style_regime,
                    expected_next_day_return=round(expected_ret, 6),
                    up_probability=round(up_prob, 6),
                    strategy_signal=signal,
                    suggested_action=suggested_action,
                    suggested_delta_lots=int(delta_lots),
                    recommended_execution_window=intraday_advice.execution_window,
                    avoid_execution_windows=list(intraday_advice.avoid_windows),
                    intraday_risk_level=intraday_advice.risk_level,
                    intraday_volatility=(
                        round(float(intraday_advice.intraday_volatility), 6)
                        if intraday_advice.intraday_volatility is not None
                        else None
                    ),
                    analysis_note=(
                        self._analysis_note(signal=signal, expected_return=expected_ret, up_probability=up_prob)
                        + data_note
                        + (f" 盘中建议：{intraday_advice.note}" if intraday_advice.note else "")
                    ),
                )
            )
            recommendations.append(
                ManualHoldingRecommendationItem(
                    symbol=item.symbol,
                    symbol_name=item.symbol_name,
                    action=suggested_action,
                    target_lots=max(0, int(item.lots) + int(delta_lots)),
                    delta_lots=int(delta_lots),
                    confidence=round(self._confidence(expected_return=expected_ret, up_probability=up_prob), 6),
                    expected_next_day_return=round(expected_ret, 6),
                    up_probability=round(up_prob, 6),
                    next_trade_date=next_trade_date,
                    style_regime=style_regime,
                    execution_window=intraday_advice.execution_window,
                    avoid_execution_windows=list(intraday_advice.avoid_windows),
                    intraday_risk_level=intraday_advice.risk_level,
                    stop_loss_hint_pct=(
                        round(float(intraday_advice.stop_loss_hint_pct), 6)
                        if intraday_advice.stop_loss_hint_pct is not None
                        else None
                    ),
                    take_profit_hint_pct=(
                        round(float(intraday_advice.take_profit_hint_pct), 6)
                        if intraday_advice.take_profit_hint_pct is not None
                        else None
                    ),
                    rationale=self._rationale(signal=signal, expected_return=expected_ret, up_probability=up_prob),
                    risk_flags=risk_flags,
                )
            )

        recommendations.extend(
            self._new_buy_recommendations(
                req=req,
                held_symbols={x.symbol for x in positions_result.positions},
                strategy=strategy,
                next_trade_date=next_trade_date,
                style_snapshot=style_snapshot,
            )
        )

        action_priority = {
            HoldingRecommendationAction.EXIT: 6,
            HoldingRecommendationAction.REDUCE: 5,
            HoldingRecommendationAction.ADD: 4,
            HoldingRecommendationAction.BUY_NEW: 3,
            HoldingRecommendationAction.HOLD: 2,
            HoldingRecommendationAction.WATCH: 1,
        }
        recommendations.sort(
            key=lambda x: (
                action_priority.get(x.action, 0),
                abs(float(x.expected_next_day_return)),
                float(x.confidence),
            ),
            reverse=True,
        )
        analyzed_positions.sort(key=lambda x: float(x.market_value), reverse=True)

        market_overview = self._market_overview(
            analyzed_positions,
            next_trade_date=next_trade_date,
            recommendations=recommendations,
            style_snapshot=style_snapshot,
        )
        result = ManualHoldingAnalysisResult(
            generated_at=datetime.now(timezone.utc),
            as_of_date=req.as_of_date,
            next_trade_date=next_trade_date,
            strategy_name=req.strategy_name,
            provider=positions_result.provider,
            market_overview=market_overview,
            summary=positions_result.summary,
            positions=analyzed_positions,
            recommendations=recommendations,
        )
        run_id = self.store.save_analysis_snapshot(result)
        return result.model_copy(update={"analysis_run_id": run_id})

    def _build_positions_and_snapshots(
        self,
        *,
        as_of_date: date,
        style_snapshot: dict[str, object] | None = None,
    ) -> tuple[ManualHoldingPositionsResult, dict[str, dict[str, object]]]:
        states = self._build_positions_state(as_of_date=as_of_date)
        if not states:
            empty_summary = ManualHoldingPortfolioSummary(as_of_date=as_of_date)
            return ManualHoldingPositionsResult(as_of_date=as_of_date, provider="", summary=empty_summary, positions=[]), {}

        items: list[ManualHoldingPositionItem] = []
        snapshots: dict[str, dict[str, object]] = {}
        provider_used = ""

        for state in states:
            load_error = ""
            snapshot = None
            try:
                snapshot = self._load_symbol_snapshot(
                    symbol=state.symbol,
                    as_of_date=as_of_date,
                    style_snapshot=style_snapshot,
                )
            except Exception as exc:  # noqa: BLE001
                load_error = str(exc)

            latest_price = float(state.avg_cost)
            latest_close_date = None
            day_change_pct = None
            momentum20 = None
            volatility20 = None
            fundamental_score = None
            advanced_score = None
            event_score = None
            negative_event_score = None
            style_risk_on_score = None
            style_regime = ""
            market_comment = ""

            if snapshot is not None:
                snapshots[state.symbol] = snapshot
                provider_used = provider_used or str(snapshot["provider"] or "")
                latest_price = max(0.0, float(snapshot["latest"].get("close", state.avg_cost) or state.avg_cost))
                latest_close_date = snapshot["latest_date"]
                prev_close = float(snapshot["prev_close"] or latest_price)
                if prev_close > 0:
                    day_change_pct = latest_price / prev_close - 1.0
                momentum20 = self._to_float(snapshot["latest"].get("momentum20"))
                volatility20 = self._to_float(snapshot["latest"].get("volatility20"))
                fundamental_score = self._to_float(snapshot["latest"].get("fundamental_score"))
                advanced_score = self._to_float(snapshot["latest"].get("tushare_advanced_score"))
                event_score = self._to_float(snapshot["latest"].get("event_score"))
                negative_event_score = self._to_float(snapshot["latest"].get("negative_event_score"))
                style_risk_on_score = self._to_float(snapshot["latest"].get("style_risk_on_score"))
                style_regime = str(snapshot["latest"].get("style_regime") or "").strip().upper()
                market_comment = self._market_comment(
                    day_change_pct=day_change_pct,
                    momentum20=momentum20,
                    volatility20=volatility20,
                    fundamental_score=fundamental_score,
                )
                if style_regime:
                    market_comment = f"{market_comment} 风格={style_regime}。"
            else:
                market_comment = f"行情拉取失败，使用成本价估算市值：{load_error[:120] or 'unknown error'}"

            market_value = float(state.quantity) * latest_price
            cost_value = float(state.quantity) * float(state.avg_cost)
            unrealized = market_value - cost_value
            unrealized_pct = (unrealized / cost_value) if cost_value > 0 else 0.0
            items.append(
                ManualHoldingPositionItem(
                    symbol=state.symbol,
                    symbol_name=state.symbol_name,
                    quantity=int(state.quantity),
                    lots=int(state.quantity // max(1, state.lot_size)),
                    avg_cost=round(float(state.avg_cost), 4),
                    latest_price=round(latest_price, 4),
                    latest_close_date=latest_close_date,
                    day_change_pct=(round(float(day_change_pct), 6) if day_change_pct is not None else None),
                    cost_value=round(cost_value, 2),
                    market_value=round(market_value, 2),
                    unrealized_pnl=round(unrealized, 2),
                    unrealized_pnl_pct=round(unrealized_pct, 6),
                    weight=0.0,
                    momentum20=(round(float(momentum20), 6) if momentum20 is not None else None),
                    volatility20=(round(float(volatility20), 6) if volatility20 is not None else None),
                    fundamental_score=(round(float(fundamental_score), 6) if fundamental_score is not None else None),
                    tushare_advanced_score=(round(float(advanced_score), 6) if advanced_score is not None else None),
                    event_score=(round(float(event_score), 6) if event_score is not None else None),
                    negative_event_score=(
                        round(float(negative_event_score), 6) if negative_event_score is not None else None
                    ),
                    style_risk_on_score=(
                        round(float(style_risk_on_score), 6) if style_risk_on_score is not None else None
                    ),
                    style_regime=style_regime,
                    market_comment=market_comment,
                )
            )

        total_market_value = sum(float(x.market_value) for x in items)
        total_cost_value = sum(float(x.cost_value) for x in items)
        total_unrealized = sum(float(x.unrealized_pnl) for x in items)
        for item in items:
            item.weight = round(float(item.market_value) / total_market_value, 6) if total_market_value > 0 else 0.0
        items.sort(key=lambda x: float(x.market_value), reverse=True)

        summary = ManualHoldingPortfolioSummary(
            as_of_date=as_of_date,
            position_count=len(items),
            total_quantity=sum(int(x.quantity) for x in items),
            total_cost_value=round(total_cost_value, 2),
            total_market_value=round(total_market_value, 2),
            total_unrealized_pnl=round(total_unrealized, 2),
            total_unrealized_pnl_pct=round(total_unrealized / total_cost_value, 6) if total_cost_value > 0 else 0.0,
        )
        return (
            ManualHoldingPositionsResult(
                as_of_date=as_of_date,
                provider=provider_used,
                summary=summary,
                positions=items,
            ),
            snapshots,
        )

    def _build_positions_state(self, *, as_of_date: date) -> list[_PositionState]:
        trades = self.store.list_trades(end_date=as_of_date, limit=50000)
        ordered = sorted(trades, key=lambda x: (x.trade_date, x.id))
        states: dict[str, _PositionState] = {}

        for trade in ordered:
            symbol = str(trade.symbol).strip().upper()
            if not symbol:
                continue
            cur = states.get(symbol)
            if cur is None:
                cur = _PositionState(
                    symbol=symbol,
                    symbol_name=str(trade.symbol_name or symbol),
                    quantity=0,
                    avg_cost=0.0,
                    lot_size=max(1, int(trade.lot_size)),
                )
                states[symbol] = cur

            if trade.symbol_name:
                cur.symbol_name = str(trade.symbol_name)
            cur.lot_size = max(1, int(trade.lot_size or cur.lot_size))

            qty = max(0, int(trade.quantity))
            if qty <= 0:
                continue

            if trade.side == ManualHoldingSide.BUY:
                new_qty = cur.quantity + qty
                if new_qty > 0:
                    gross_cost = (cur.avg_cost * cur.quantity) + float(trade.price) * qty + float(trade.fee)
                    cur.avg_cost = max(0.0, gross_cost / new_qty)
                cur.quantity = new_qty
                continue

            sell_qty = min(cur.quantity, qty)
            cur.quantity -= sell_qty
            if cur.quantity <= 0:
                cur.quantity = 0
                cur.avg_cost = 0.0

        out = [item for item in states.values() if int(item.quantity) > 0]
        out.sort(key=lambda x: x.symbol)
        return out

    def _load_symbol_snapshot(
        self,
        *,
        symbol: str,
        as_of_date: date,
        style_snapshot: dict[str, object] | None = None,
    ) -> dict[str, object]:
        lookback_start = as_of_date - timedelta(days=280)
        provider_name, bars = self.provider.get_daily_bars_with_source(symbol, lookback_start, as_of_date)
        if bars is None or bars.empty:
            raise ValueError(f"{symbol}: no market bars available")

        frame = bars.sort_values("trade_date").reset_index(drop=True).copy()
        status: dict[str, bool] = {}
        try:
            status = self.provider.get_security_status(symbol)
        except Exception:  # noqa: BLE001
            status = {}
        frame["is_st"] = bool(status.get("is_st", False))
        frame["is_suspended"] = bool(status.get("is_suspended", False))

        if self.fundamental_service is not None:
            try:
                frame, _ = self.fundamental_service.enrich_bars(
                    symbol=symbol,
                    bars=frame,
                    as_of=as_of_date,
                    max_staleness_days=540,
                )
            except Exception:  # noqa: BLE001
                pass

        if self.event_service is not None:
            try:
                frame, _ = self.event_service.enrich_bars(
                    symbol=symbol,
                    bars=frame,
                    lookback_days=120,
                    decay_half_life_days=12.0,
                )
            except Exception:  # noqa: BLE001
                pass

        provider_event: dict[str, object] = {}
        provider_event_source = ""
        try:
            if hasattr(self.provider, "get_corporate_event_snapshot_with_source"):
                provider_event_source, provider_event = self.provider.get_corporate_event_snapshot_with_source(
                    symbol=symbol,
                    as_of=as_of_date,
                    lookback_days=120,
                )
            else:
                provider_event = self.provider.get_corporate_event_snapshot(
                    symbol,
                    as_of_date,
                    lookback_days=120,
                )
        except Exception:  # noqa: BLE001
            provider_event = {}
            provider_event_source = ""

        if provider_event:
            frame = self._apply_provider_event_snapshot(frame=frame, event_snapshot=provider_event)

        style = dict(style_snapshot or {})
        frame["style_risk_on_score"] = float(self._bounded(float(style.get("risk_on_score", 0.5) or 0.5), 0.0, 1.0))
        frame["style_flow_score"] = float(self._bounded(float(style.get("flow_score", 0.5) or 0.5), 0.0, 1.0))
        frame["style_leverage_score"] = float(self._bounded(float(style.get("leverage_score", 0.5) or 0.5), 0.0, 1.0))
        frame["style_theme_heat_score"] = float(self._bounded(float(style.get("theme_heat_score", 0.5) or 0.5), 0.0, 1.0))
        frame["style_regime"] = str(style.get("regime", "NEUTRAL")).strip().upper() or "NEUTRAL"

        features = self.factor_engine.compute(frame)
        if features is None or features.empty:
            raise ValueError(f"{symbol}: factor feature frame is empty")
        features = self._inject_disclosure_event_proxy(features)
        latest = features.iloc[-1]
        latest_date = pd.to_datetime(latest.get("trade_date"), errors="coerce")
        prev_close = float(latest.get("close", 0.0) or 0.0)
        if len(features) >= 2:
            prev_close = float(features.iloc[-2].get("close", prev_close) or prev_close)

        return {
            "provider": provider_name,
            "features": features,
            "latest": latest,
            "latest_date": (latest_date.date() if not pd.isna(latest_date) else None),
            "prev_close": prev_close,
            "provider_event_source": provider_event_source,
            "provider_event_snapshot": provider_event,
            "style_snapshot": style,
        }

    def _market_style_snapshot(self, *, as_of_date: date) -> dict[str, object]:
        style: dict[str, object] = {
            "risk_on_score": 0.5,
            "flow_score": 0.5,
            "leverage_score": 0.5,
            "theme_heat_score": 0.5,
            "regime": "NEUTRAL",
            "source": "",
        }
        try:
            if hasattr(self.provider, "get_market_style_snapshot_with_source"):
                source, payload = self.provider.get_market_style_snapshot_with_source(
                    as_of=as_of_date,
                    lookback_days=30,
                )
                style["source"] = str(source)
                if isinstance(payload, dict):
                    style.update(payload)
            elif hasattr(self.provider, "get_market_style_snapshot"):
                payload = self.provider.get_market_style_snapshot(as_of_date, lookback_days=30)
                if isinstance(payload, dict):
                    style.update(payload)
        except Exception:  # noqa: BLE001
            pass

        style["risk_on_score"] = float(self._bounded(float(style.get("risk_on_score", 0.5) or 0.5), 0.0, 1.0))
        style["flow_score"] = float(self._bounded(float(style.get("flow_score", 0.5) or 0.5), 0.0, 1.0))
        style["leverage_score"] = float(self._bounded(float(style.get("leverage_score", 0.5) or 0.5), 0.0, 1.0))
        style["theme_heat_score"] = float(self._bounded(float(style.get("theme_heat_score", 0.5) or 0.5), 0.0, 1.0))
        style["regime"] = str(style.get("regime", "NEUTRAL")).strip().upper() or "NEUTRAL"
        if style["regime"] not in {"RISK_ON", "RISK_OFF", "NEUTRAL"}:
            style["regime"] = "NEUTRAL"
        return style

    @staticmethod
    def _apply_provider_event_snapshot(frame: pd.DataFrame, *, event_snapshot: dict[str, object]) -> pd.DataFrame:
        out = frame.copy()
        if out.empty:
            return out
        event_score = HoldingService._bounded(float(event_snapshot.get("event_score", 0.0) or 0.0), 0.0, 1.0)
        negative_event_score = HoldingService._bounded(float(event_snapshot.get("negative_event_score", 0.0) or 0.0), 0.0, 1.0)
        earnings_revision_score = HoldingService._bounded(
            float(event_snapshot.get("earnings_revision_score", 0.5) or 0.5),
            0.0,
            1.0,
        )
        disclosure_timing_score = HoldingService._bounded(
            float(event_snapshot.get("disclosure_timing_score", 0.5) or 0.5),
            0.0,
            1.0,
        )
        if "event_score" not in out.columns:
            out["event_score"] = event_score * disclosure_timing_score
        else:
            existing = pd.to_numeric(out["event_score"], errors="coerce").fillna(0.0)
            if float(existing.abs().sum()) <= 1e-9:
                out["event_score"] = event_score * disclosure_timing_score
            else:
                out["event_score"] = existing
        if "negative_event_score" not in out.columns:
            out["negative_event_score"] = negative_event_score
        else:
            existing = pd.to_numeric(out["negative_event_score"], errors="coerce").fillna(0.0)
            if float(existing.abs().sum()) <= 1e-9:
                out["negative_event_score"] = negative_event_score
            else:
                out["negative_event_score"] = existing

        out["provider_event_score"] = event_score
        out["provider_negative_event_score"] = negative_event_score
        out["provider_earnings_revision_score"] = earnings_revision_score
        out["provider_disclosure_timing_score"] = disclosure_timing_score
        freshness = event_snapshot.get("freshness_days")
        out["provider_event_freshness_days"] = (
            float(freshness) if freshness is not None and str(freshness).strip() != "" else None
        )
        return out

    @staticmethod
    def _inject_disclosure_event_proxy(features: pd.DataFrame) -> pd.DataFrame:
        out = features.copy()
        if out.empty:
            return out

        if "event_score" not in out.columns:
            out["event_score"] = 0.0
        if "negative_event_score" not in out.columns:
            out["negative_event_score"] = 0.0
        event_series = pd.to_numeric(out["event_score"], errors="coerce").fillna(0.0)
        negative_series = pd.to_numeric(out["negative_event_score"], errors="coerce").fillna(0.0)
        if float(event_series.abs().sum() + negative_series.abs().sum()) > 1e-9:
            return out

        forecast_raw = out.get("tushare_forecast_pchg_mid")
        if forecast_raw is None:
            forecast_raw = out.get("ts_forecast_pchg_mid")
        forecast = pd.to_numeric(forecast_raw, errors="coerce")
        if not isinstance(forecast, pd.Series):
            forecast = pd.Series([0.0] * len(out), index=out.index, dtype=float)
        else:
            forecast = forecast.fillna(0.0)

        disclosure_risk_raw = out.get("tushare_disclosure_risk_score")
        disclosure_risk = pd.to_numeric(disclosure_risk_raw, errors="coerce")
        if not isinstance(disclosure_risk, pd.Series):
            disclosure_risk = pd.Series([0.5] * len(out), index=out.index, dtype=float)
        else:
            disclosure_risk = disclosure_risk.fillna(0.5).clip(lower=0.0, upper=1.0)

        pos = (forecast / 40.0).clip(lower=0.0, upper=1.0) * (1.0 - disclosure_risk * 0.35)
        neg = ((-forecast) / 35.0).clip(lower=0.0, upper=1.0) * 0.7 + disclosure_risk * 0.3
        out["event_score"] = pos.clip(lower=0.0, upper=1.0)
        out["negative_event_score"] = neg.clip(lower=0.0, upper=1.0)
        return out

    def _resolve_runtime_params(
        self,
        *,
        strategy_name: str,
        symbol: str,
        explicit_params: dict[str, float | int | str | bool],
        use_profile: bool,
    ) -> tuple[dict[str, float | int | str | bool], object | None]:
        try:
            return self.autotune.resolve_runtime_params(
                strategy_name=strategy_name,
                symbol=symbol,
                explicit_params=explicit_params,
                use_profile=use_profile,
            )
        except Exception:  # noqa: BLE001
            return dict(explicit_params or {}), None

    def _latest_strategy_signal(
        self,
        strategy,
        features: pd.DataFrame,
        params: dict[str, float | int | str | bool],
    ) -> SignalAction:
        try:
            signals = strategy.generate(features, StrategyContext(params=params, market_state={}))
        except Exception:  # noqa: BLE001
            return SignalAction.WATCH
        if not signals:
            return SignalAction.WATCH
        raw = signals[-1].action
        if isinstance(raw, SignalAction):
            return raw
        try:
            return SignalAction(str(raw))
        except Exception:  # noqa: BLE001
            return SignalAction.WATCH

    @staticmethod
    def _raw_forecast_next_day(latest_row: pd.Series) -> tuple[float, float]:
        close = float(latest_row.get("close", 0.0) or 0.0)
        ma20 = float(latest_row.get("ma20", 0.0) or 0.0)
        momentum5 = float(latest_row.get("momentum5", 0.0) or 0.0)
        momentum20 = float(latest_row.get("momentum20", 0.0) or 0.0)
        volatility20 = max(0.0, float(latest_row.get("volatility20", 0.0) or 0.0))
        fundamental_score = float(latest_row.get("fundamental_score", 0.5) or 0.5)
        advanced_score = float(latest_row.get("tushare_advanced_score", 0.5) or 0.5)
        event_score = float(latest_row.get("event_score", 0.0) or 0.0)
        negative_event = float(latest_row.get("negative_event_score", 0.0) or 0.0)
        provider_event_score = float(latest_row.get("provider_event_score", 0.0) or 0.0)
        provider_negative_event_score = float(latest_row.get("provider_negative_event_score", 0.0) or 0.0)
        earnings_revision = float(latest_row.get("provider_earnings_revision_score", 0.5) or 0.5)
        disclosure_timing = float(latest_row.get("provider_disclosure_timing_score", 0.5) or 0.5)
        style_risk_on = float(latest_row.get("style_risk_on_score", 0.5) or 0.5)
        style_flow = float(latest_row.get("style_flow_score", 0.5) or 0.5)
        style_leverage = float(latest_row.get("style_leverage_score", 0.5) or 0.5)
        style_regime = str(latest_row.get("style_regime", "") or "").strip().upper()

        if abs(event_score) + abs(negative_event) <= 1e-9:
            forecast_mid = float(
                latest_row.get("tushare_forecast_pchg_mid", latest_row.get("ts_forecast_pchg_mid", 0.0)) or 0.0
            )
            disclosure_risk = float(latest_row.get("tushare_disclosure_risk_score", 0.5) or 0.5)
            event_score = HoldingService._bounded(max(0.0, forecast_mid / 40.0) * (1.0 - disclosure_risk * 0.35), 0.0, 1.0)
            negative_event = HoldingService._bounded(
                max(0.0, -forecast_mid / 35.0) * 0.7 + disclosure_risk * 0.3,
                0.0,
                1.0,
            )

        event_score = HoldingService._bounded(0.65 * event_score + 0.35 * provider_event_score, 0.0, 1.0)
        negative_event = HoldingService._bounded(
            0.65 * negative_event + 0.35 * provider_negative_event_score,
            0.0,
            1.0,
        )

        ma_bias = ((close - ma20) / ma20) if ma20 > 1e-12 else 0.0
        score = 0.0
        score += HoldingService._bounded(momentum5, -0.30, 0.30) * 2.3
        score += HoldingService._bounded(momentum20, -0.45, 0.45) * 1.7
        score += HoldingService._bounded(ma_bias, -0.20, 0.20) * 1.5
        score -= HoldingService._bounded(volatility20, 0.0, 0.25) * 2.1
        score += (HoldingService._bounded(fundamental_score, 0.0, 1.0) - 0.5) * 0.8
        score += (HoldingService._bounded(advanced_score, 0.0, 1.0) - 0.5) * 0.6
        score += HoldingService._bounded(event_score - negative_event, -1.0, 1.0) * 0.15
        score += (HoldingService._bounded(earnings_revision, 0.0, 1.0) - 0.5) * 0.18
        score += (HoldingService._bounded(disclosure_timing, 0.0, 1.0) - 0.5) * 0.14
        score += (HoldingService._bounded(style_risk_on, 0.0, 1.0) - 0.5) * 0.70
        score += (HoldingService._bounded(style_flow, 0.0, 1.0) - 0.5) * 0.45
        score += (HoldingService._bounded(style_leverage, 0.0, 1.0) - 0.5) * 0.35
        if style_regime == "RISK_OFF":
            score -= 0.10
        elif style_regime == "RISK_ON":
            score += 0.06

        expected_return = HoldingService._bounded(score * 0.018, -0.08, 0.08)
        up_probability = HoldingService._bounded(0.5 + score * 0.20, 0.05, 0.95)
        return float(expected_return), float(up_probability)

    def _forecast_next_day(
        self,
        *,
        latest_row: pd.Series,
        features: pd.DataFrame | None = None,
    ) -> _ForecastEstimate:
        raw_expected, raw_prob = self._raw_forecast_next_day(latest_row)

        if features is None or features.empty:
            return _ForecastEstimate(
                expected_return=float(raw_expected),
                up_probability=float(raw_prob),
                raw_expected_return=float(raw_expected),
                raw_up_probability=float(raw_prob),
                calibration_samples=0,
                return_calibration_weight=0.0,
                probability_calibration_weight=0.0,
                note="无历史样本校准，使用基础预测。",
            )

        history = self._build_forecast_calibration_history(features=features, max_samples=180)
        if len(history) < 24:
            return _ForecastEstimate(
                expected_return=float(raw_expected),
                up_probability=float(raw_prob),
                raw_expected_return=float(raw_expected),
                raw_up_probability=float(raw_prob),
                calibration_samples=len(history),
                return_calibration_weight=0.0,
                probability_calibration_weight=0.0,
                note="历史样本不足，使用基础预测。",
            )

        pred_returns = [float(x[0]) for x in history]
        pred_probs = [float(x[1]) for x in history]
        realized_returns = [float(x[2]) for x in history]
        realized_ups = [float(x[3]) for x in history]

        ret_adjusted, ret_weight = self._calibrate_expected_return(
            raw_expected_return=raw_expected,
            predicted_returns=pred_returns,
            realized_returns=realized_returns,
        )
        prob_adjusted, prob_weight = self._calibrate_up_probability(
            raw_up_probability=raw_prob,
            predicted_probs=pred_probs,
            realized_ups=realized_ups,
        )
        note = f"滚动{len(history)}样本校准：ret_w={ret_weight:.2f}, prob_w={prob_weight:.2f}。"
        return _ForecastEstimate(
            expected_return=float(ret_adjusted),
            up_probability=float(prob_adjusted),
            raw_expected_return=float(raw_expected),
            raw_up_probability=float(raw_prob),
            calibration_samples=len(history),
            return_calibration_weight=float(ret_weight),
            probability_calibration_weight=float(prob_weight),
            note=note,
        )

    @staticmethod
    def _build_forecast_calibration_history(
        *,
        features: pd.DataFrame,
        max_samples: int,
    ) -> list[tuple[float, float, float, float]]:
        ordered = features.sort_values("trade_date").reset_index(drop=True)
        if len(ordered) < 3:
            return []

        start_idx = max(0, len(ordered) - 1 - max(30, int(max_samples)))
        rows: list[tuple[float, float, float, float]] = []
        for idx in range(start_idx, len(ordered) - 1):
            current = ordered.iloc[idx]
            nxt = ordered.iloc[idx + 1]
            close_t = float(current.get("close", 0.0) or 0.0)
            close_next = float(nxt.get("close", 0.0) or 0.0)
            if close_t <= 1e-9 or close_next <= 1e-9:
                continue
            pred_ret, pred_prob = HoldingService._raw_forecast_next_day(current)
            realized_ret = (close_next / close_t) - 1.0
            realized_up = 1.0 if realized_ret > 0 else 0.0
            rows.append((float(pred_ret), float(pred_prob), float(realized_ret), float(realized_up)))
        return rows

    @staticmethod
    def _calibrate_expected_return(
        *,
        raw_expected_return: float,
        predicted_returns: list[float],
        realized_returns: list[float],
    ) -> tuple[float, float]:
        n = min(len(predicted_returns), len(realized_returns))
        if n < 12:
            return float(HoldingService._bounded(raw_expected_return, -0.08, 0.08)), 0.0

        preds = [float(x) for x in predicted_returns[:n]]
        reals = [float(x) for x in realized_returns[:n]]
        mean_pred = float(statistics.fmean(preds))
        mean_real = float(statistics.fmean(reals))
        var_pred = float(statistics.fmean([(x - mean_pred) ** 2 for x in preds]))
        var_real = float(statistics.fmean([(y - mean_real) ** 2 for y in reals]))
        if var_pred <= 1e-12:
            return float(HoldingService._bounded(raw_expected_return, -0.08, 0.08)), 0.0

        cov = float(statistics.fmean([(preds[i] - mean_pred) * (reals[i] - mean_real) for i in range(n)]))
        beta = HoldingService._bounded(cov / var_pred, -2.5, 2.5)
        alpha = mean_real - beta * mean_pred
        mapped = alpha + beta * float(raw_expected_return)
        if var_real <= 1e-12:
            corr_abs = 0.0
        else:
            corr_abs = abs(cov / max(1e-12, math.sqrt(var_pred * var_real)))
        corr_abs = HoldingService._bounded(corr_abs, 0.0, 1.0)
        sample_weight = HoldingService._bounded(n / 120.0, 0.0, 1.0)
        weight = float(corr_abs * sample_weight)
        adjusted = float(raw_expected_return) * (1.0 - weight) + float(mapped) * weight
        adjusted = float(HoldingService._bounded(adjusted, -0.08, 0.08))
        return adjusted, weight

    @staticmethod
    def _calibrate_up_probability(
        *,
        raw_up_probability: float,
        predicted_probs: list[float],
        realized_ups: list[float],
    ) -> tuple[float, float]:
        n = min(len(predicted_probs), len(realized_ups))
        if n < 20:
            return float(HoldingService._bounded(raw_up_probability, 0.05, 0.95)), 0.0

        probs = [HoldingService._bounded(float(x), 0.001, 0.999) for x in predicted_probs[:n]]
        outcomes = [HoldingService._bounded(float(x), 0.0, 1.0) for x in realized_ups[:n]]
        brier = float(statistics.fmean([(probs[i] - outcomes[i]) ** 2 for i in range(n)]))
        baseline = 0.25
        quality = HoldingService._bounded(1.0 - (brier / baseline), 0.0, 1.0)
        sample_weight = HoldingService._bounded(n / 150.0, 0.0, 1.0)
        weight = float(quality * sample_weight)

        bucket_count = 10
        stats: dict[int, list[float]] = {}
        for i in range(n):
            p = probs[i]
            bucket = int(min(bucket_count - 1, max(0, math.floor(p * bucket_count))))
            if bucket not in stats:
                stats[bucket] = [0.0, 0.0, 0.0]
            stats[bucket][0] += p
            stats[bucket][1] += outcomes[i]
            stats[bucket][2] += 1.0

        points: list[tuple[float, float, float]] = []
        for bucket in sorted(stats.keys()):
            sum_p, sum_y, count = stats[bucket]
            if count <= 0:
                continue
            points.append((sum_p / count, sum_y / count, count))
        if len(points) < 2:
            adjusted_prob = HoldingService._bounded(raw_up_probability, 0.05, 0.95)
            return float(adjusted_prob), 0.0

        smooth_points: list[tuple[float, float]] = []
        for idx, (p_center, hit_rate, count) in enumerate(points):
            weighted_sum = hit_rate * count
            weighted_cnt = count
            if idx > 0:
                left = points[idx - 1]
                weighted_sum += left[1] * left[2] * 0.35
                weighted_cnt += left[2] * 0.35
            if idx + 1 < len(points):
                right = points[idx + 1]
                weighted_sum += right[1] * right[2] * 0.35
                weighted_cnt += right[2] * 0.35
            smooth_points.append((float(p_center), float(weighted_sum / max(1e-9, weighted_cnt))))

        x = HoldingService._bounded(float(raw_up_probability), 0.001, 0.999)
        mapped = smooth_points[0][1]
        if x <= smooth_points[0][0]:
            mapped = smooth_points[0][1]
        elif x >= smooth_points[-1][0]:
            mapped = smooth_points[-1][1]
        else:
            for idx in range(1, len(smooth_points)):
                left_x, left_y = smooth_points[idx - 1]
                right_x, right_y = smooth_points[idx]
                if x <= right_x:
                    span = max(1e-9, right_x - left_x)
                    ratio = (x - left_x) / span
                    mapped = left_y * (1.0 - ratio) + right_y * ratio
                    break

        blended = x * (1.0 - weight) + mapped * weight
        blended = HoldingService._bounded(blended, 0.05, 0.95)
        return float(blended), weight

    @staticmethod
    def _position_action(
        *,
        signal: SignalAction,
        expected_return: float,
        up_probability: float,
        current_lots: int,
        volatility20: float | None,
        fundamental_score: float | None,
        style_risk_on_score: float | None = None,
        style_regime: str = "NEUTRAL",
    ) -> tuple[HoldingRecommendationAction, int, list[str]]:
        lots = max(0, int(current_lots))
        risk_flags: list[str] = []
        if volatility20 is not None and volatility20 >= 0.05:
            risk_flags.append("HIGH_VOLATILITY")
        if fundamental_score is not None and fundamental_score < 0.42:
            risk_flags.append("WEAK_FUNDAMENTAL")
        regime = str(style_regime or "NEUTRAL").strip().upper()
        risk_on = 0.5 if style_risk_on_score is None else HoldingService._bounded(float(style_risk_on_score), 0.0, 1.0)
        if regime == "RISK_OFF" or risk_on <= 0.42:
            risk_flags.append("RISK_OFF_REGIME")

        if lots <= 0:
            return HoldingRecommendationAction.WATCH, 0, risk_flags

        if signal == SignalAction.SELL:
            if expected_return <= -0.025 or up_probability <= 0.28:
                return HoldingRecommendationAction.EXIT, -lots, risk_flags
            reduce_ratio = 0.40 if (regime != "RISK_OFF" and risk_on > 0.42) else 0.55
            reduce_lots = min(lots, max(1, int(round(lots * reduce_ratio))))
            return HoldingRecommendationAction.REDUCE, -reduce_lots, risk_flags

        risk_off_guard = regime == "RISK_OFF" or risk_on <= 0.42
        if expected_return <= -0.020 or up_probability <= 0.35 or (risk_off_guard and up_probability < 0.50):
            reduce_ratio = 0.30 if not risk_off_guard else 0.45
            reduce_lots = min(lots, max(1, int(round(lots * reduce_ratio))))
            return HoldingRecommendationAction.REDUCE, -reduce_lots, risk_flags

        add_expected_threshold = 0.010 if not risk_off_guard else 0.016
        add_prob_threshold = 0.60 if not risk_off_guard else 0.66
        if signal == SignalAction.BUY and expected_return >= add_expected_threshold and up_probability >= add_prob_threshold:
            add_lots = max(1, int(round(max(1.0, lots * 0.25))))
            return HoldingRecommendationAction.ADD, add_lots, risk_flags

        return HoldingRecommendationAction.HOLD, 0, risk_flags

    def _new_buy_recommendations(
        self,
        *,
        req: ManualHoldingAnalysisRequest,
        held_symbols: set[str],
        strategy,
        next_trade_date: date | None,
        style_snapshot: dict[str, object] | None = None,
    ) -> list[ManualHoldingRecommendationItem]:
        candidates = [str(x).strip().upper() for x in req.candidate_symbols if str(x).strip()]
        candidates = [x for x in dict.fromkeys(candidates) if x and x not in held_symbols]
        if not candidates or req.max_new_buys <= 0:
            return []

        available_cash = max(0.0, float(req.available_cash))
        if available_cash <= 0:
            return []

        ranked: list[tuple[float, str, str, float, float, float, float | None, float | None]] = []
        for symbol in candidates:
            try:
                snapshot = self._load_symbol_snapshot(
                    symbol=symbol,
                    as_of_date=req.as_of_date,
                    style_snapshot=style_snapshot,
                )
            except Exception:  # noqa: BLE001
                continue
            params, _ = self._resolve_runtime_params(
                strategy_name=req.strategy_name,
                symbol=symbol,
                explicit_params=req.strategy_params,
                use_profile=req.use_autotune_profile,
            )
            signal = self._latest_strategy_signal(strategy, snapshot["features"], params)
            if signal != SignalAction.BUY:
                continue

            latest = snapshot["latest"]
            latest_price = float(latest.get("close", 0.0) or 0.0)
            if latest_price <= 0:
                continue
            forecast = self._forecast_next_day(latest_row=latest, features=snapshot["features"])
            expected_ret = float(forecast.expected_return)
            up_prob = float(forecast.up_probability)
            style_regime = str(latest.get("style_regime", "NEUTRAL") or "NEUTRAL").strip().upper()
            style_risk_on = self._to_float(latest.get("style_risk_on_score"))
            risk_off_guard = style_regime == "RISK_OFF" or ((style_risk_on or 0.5) <= 0.42)
            if expected_ret < (0.004 if not risk_off_guard else 0.008) or up_prob < (0.55 if not risk_off_guard else 0.60):
                continue

            momentum20 = self._to_float(latest.get("momentum20"))
            volatility20 = self._to_float(latest.get("volatility20"))
            score = expected_ret * 0.55 + (up_prob - 0.5) * 0.35 + (((style_risk_on or 0.5) - 0.5) * 0.10)
            symbol_name = str(latest.get("name") or symbol).strip() or symbol
            ranked.append((float(score), symbol, symbol_name, expected_ret, up_prob, latest_price, momentum20, volatility20))

        if not ranked:
            return []

        ranked.sort(key=lambda x: x[0], reverse=True)
        selected = ranked[: int(req.max_new_buys)]

        lot_size = max(1, int(req.lot_size))
        max_single_ratio = self._bounded(float(req.max_single_position_ratio), 0.05, 1.0)
        per_symbol_cash_cap = available_cash * max_single_ratio
        remaining_cash = available_cash

        out: list[ManualHoldingRecommendationItem] = []
        for score, symbol, symbol_name, expected_ret, up_prob, latest_price, momentum20, volatility20 in selected:
            lot_cost = latest_price * lot_size
            if lot_cost <= 0:
                continue

            budget_for_symbol = min(per_symbol_cash_cap, remaining_cash)
            lots = int(budget_for_symbol / lot_cost) if budget_for_symbol > 0 else 0
            if lots <= 0 and remaining_cash >= lot_cost:
                lots = 1
            required_cash = lots * lot_cost
            if required_cash > remaining_cash and lot_cost > 0:
                lots = int(remaining_cash / lot_cost)
                required_cash = lots * lot_cost
            if lots <= 0:
                continue

            remaining_cash = max(0.0, remaining_cash - required_cash)
            confidence = self._bounded(0.55 + score * 3.0, 0.35, 0.95)
            risk_flags: list[str] = []
            if volatility20 is not None and volatility20 >= 0.05:
                risk_flags.append("HIGH_VOLATILITY")
            if momentum20 is not None and momentum20 < -0.05:
                risk_flags.append("WEAK_MOMENTUM")
            style_regime = str(snapshot["latest"].get("style_regime", "NEUTRAL") or "NEUTRAL").strip().upper()
            style_risk_on = self._to_float(snapshot["latest"].get("style_risk_on_score"))
            if style_regime == "RISK_OFF" or ((style_risk_on or 0.5) <= 0.42):
                risk_flags.append("RISK_OFF_REGIME")
            intraday_advice = self._intraday_execution_advice(
                symbol=symbol,
                as_of_date=req.as_of_date,
                side="BUY",
                expected_return=expected_ret,
                volatility20=volatility20,
                interval=req.intraday_interval,
                lookback_days=req.intraday_lookback_days,
            )
            if intraday_advice.risk_level.upper() == "HIGH":
                risk_flags.append("INTRADAY_RISK_HIGH")
            if intraday_advice.data_date is None:
                risk_flags.append("NO_INTRADAY_DATA")

            out.append(
                ManualHoldingRecommendationItem(
                    symbol=symbol,
                    symbol_name=symbol_name,
                    action=HoldingRecommendationAction.BUY_NEW,
                    target_lots=lots,
                    delta_lots=lots,
                    confidence=round(confidence, 6),
                    expected_next_day_return=round(expected_ret, 6),
                    up_probability=round(up_prob, 6),
                    next_trade_date=next_trade_date,
                    style_regime=style_regime,
                    execution_window=intraday_advice.execution_window,
                    avoid_execution_windows=list(intraday_advice.avoid_windows),
                    intraday_risk_level=intraday_advice.risk_level,
                    stop_loss_hint_pct=(
                        round(float(intraday_advice.stop_loss_hint_pct), 6)
                        if intraday_advice.stop_loss_hint_pct is not None
                        else None
                    ),
                    take_profit_hint_pct=(
                        round(float(intraday_advice.take_profit_hint_pct), 6)
                        if intraday_advice.take_profit_hint_pct is not None
                        else None
                    ),
                    rationale=(
                        "候选池中信号为 BUY，且趋势/概率评分靠前，建议按资金上限试探建仓。"
                        + (f" 盘中建议：{intraday_advice.note}" if intraday_advice.note else "")
                    ),
                    risk_flags=risk_flags,
                )
            )
            if remaining_cash < lot_cost:
                break
        return out

    def _intraday_execution_advice(
        self,
        *,
        symbol: str,
        as_of_date: date,
        side: str,
        expected_return: float,
        volatility20: float | None,
        interval: str,
        lookback_days: int,
    ) -> _IntradayAdvice:
        bars, bar_date = self._load_intraday_reference_bars(
            symbol=symbol,
            as_of_date=as_of_date,
            interval=interval,
            lookback_days=lookback_days,
        )
        if bars is None or bars.empty or bar_date is None:
            fallback_stop = self._bounded(0.015 + max(0.0, float(volatility20 or 0.0)) * 0.65, 0.01, 0.05)
            fallback_take = self._bounded(fallback_stop * (1.8 if expected_return >= 0 else 1.2), 0.012, 0.09)
            return _IntradayAdvice(
                data_date=None,
                execution_window="10:20-10:50",
                avoid_windows=["09:30-09:45", "14:50-15:00"],
                risk_level="UNKNOWN",
                intraday_volatility=None,
                stop_loss_hint_pct=fallback_stop,
                take_profit_hint_pct=fallback_take,
                note="无分钟级行情，采用保守执行时段。",
            )

        frame = bars.sort_values("bar_time").reset_index(drop=True).copy()
        frame["bar_time"] = pd.to_datetime(frame["bar_time"], errors="coerce")
        frame = frame[frame["bar_time"].notna()].copy()
        if frame.empty:
            return _IntradayAdvice(
                data_date=None,
                execution_window="10:20-10:50",
                avoid_windows=["09:30-09:45", "14:50-15:00"],
                risk_level="UNKNOWN",
                intraday_volatility=None,
                stop_loss_hint_pct=None,
                take_profit_hint_pct=None,
                note="分钟级行情为空，采用默认执行时段。",
            )

        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["volume"] = pd.to_numeric(frame.get("volume"), errors="coerce").fillna(0.0)
        frame = frame[frame["close"].notna()].copy()
        if frame.empty:
            return _IntradayAdvice(
                data_date=None,
                execution_window="10:20-10:50",
                avoid_windows=["09:30-09:45", "14:50-15:00"],
                risk_level="UNKNOWN",
                intraday_volatility=None,
                stop_loss_hint_pct=None,
                take_profit_hint_pct=None,
                note="分钟级价格不可用，采用默认执行时段。",
            )

        frame["ret"] = frame["close"].pct_change().fillna(0.0)
        intraday_vol = float(frame["ret"].std(ddof=0) or 0.0)
        day_ret = float(frame["close"].iloc[-1] / frame["close"].iloc[0] - 1.0) if len(frame) >= 2 else 0.0
        vwap_den = float(frame["volume"].sum() or 0.0)
        vwap = (
            float((frame["close"] * frame["volume"]).sum() / vwap_den)
            if vwap_den > 0
            else float(frame["close"].mean())
        )

        windows = [
            ("09:35-10:15", time(9, 35), time(10, 15)),
            ("10:30-11:15", time(10, 30), time(11, 15)),
            ("13:05-14:00", time(13, 5), time(14, 0)),
            ("14:00-14:45", time(14, 0), time(14, 45)),
        ]
        side_key = str(side or "BUY").strip().upper()
        scores: list[tuple[str, float]] = []
        for label, st, et in windows:
            part = frame[
                (frame["bar_time"].dt.time >= st)
                & (frame["bar_time"].dt.time <= et)
            ].copy()
            if part.empty:
                continue
            price_mean = float(part["close"].mean())
            vol = float(part["ret"].std(ddof=0) or 0.0)
            liquidity = float(part["volume"].mean() or 0.0)
            price_edge = (price_mean / max(1e-9, vwap)) - 1.0
            liquidity_bonus = math.log1p(max(0.0, liquidity)) / 30.0
            if side_key == "SELL":
                score = (price_edge * 80.0) - (vol * 230.0) + liquidity_bonus
            else:
                score = (-price_edge * 80.0) - (vol * 230.0) + liquidity_bonus
            scores.append((label, float(score)))

        if not scores:
            scores = [("10:20-10:50", 0.0), ("14:20-14:45", -0.1)]

        scores_sorted = sorted(scores, key=lambda x: x[1], reverse=True)
        execution_window = scores_sorted[0][0]
        avoid_windows = [x[0] for x in sorted(scores, key=lambda x: x[1])[:2]]

        risk_level = "LOW"
        if intraday_vol >= 0.012 or abs(day_ret) >= 0.025:
            risk_level = "HIGH"
        elif intraday_vol >= 0.007 or abs(day_ret) >= 0.015:
            risk_level = "MEDIUM"

        stop_loss_hint = self._bounded(
            0.012 + max(0.0, float(volatility20 or 0.0)) * 0.55 + intraday_vol * 2.2,
            0.01,
            0.06,
        )
        take_profit_hint = self._bounded(
            stop_loss_hint * (1.8 if expected_return >= 0 else 1.2),
            0.012,
            0.10,
        )

        note = (
            f"{bar_date.isoformat()}盘中波动={intraday_vol:.2%}，日内偏移={day_ret:.2%}，"
            f"{'卖出' if side_key == 'SELL' else '买入'}优先时段 {execution_window}。"
        )
        return _IntradayAdvice(
            data_date=bar_date,
            execution_window=execution_window,
            avoid_windows=avoid_windows,
            risk_level=risk_level,
            intraday_volatility=intraday_vol,
            stop_loss_hint_pct=stop_loss_hint,
            take_profit_hint_pct=take_profit_hint,
            note=note,
        )

    def _load_intraday_reference_bars(
        self,
        *,
        symbol: str,
        as_of_date: date,
        interval: str,
        lookback_days: int,
    ) -> tuple[pd.DataFrame | None, date | None]:
        lookback = max(1, int(lookback_days))
        for shift in range(lookback):
            target_day = as_of_date - timedelta(days=shift)
            start_dt = datetime.combine(target_day, time(9, 30))
            end_dt = datetime.combine(target_day, time(15, 0))
            try:
                if hasattr(self.provider, "get_intraday_bars_with_source"):
                    _, bars = self.provider.get_intraday_bars_with_source(
                        symbol=symbol,
                        start_datetime=start_dt,
                        end_datetime=end_dt,
                        interval=interval,
                    )
                else:
                    bars = self.provider.get_intraday_bars(
                        symbol,
                        start_dt,
                        end_dt,
                        interval=interval,
                    )
            except Exception:  # noqa: BLE001
                continue
            if bars is None or bars.empty:
                continue
            frame = bars.copy()
            frame["bar_time"] = pd.to_datetime(frame["bar_time"], errors="coerce")
            frame = frame[frame["bar_time"].notna()].copy()
            if frame.empty:
                continue
            return frame, target_day
        return None, None

    def _next_trade_date(self, as_of_date: date) -> date | None:
        start = as_of_date + timedelta(days=1)
        end = as_of_date + timedelta(days=15)
        try:
            calendar = self.provider.get_trade_calendar(start, end)
        except Exception:  # noqa: BLE001
            return start
        if calendar is None or calendar.empty:
            return start

        frame = calendar.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        frame = frame[frame["trade_date"].notna()]
        if "is_open" in frame.columns:
            frame = frame[frame["is_open"].apply(bool)]
        if frame.empty:
            return start
        return sorted(frame["trade_date"].tolist())[0]

    @staticmethod
    def _market_comment(
        *,
        day_change_pct: float | None,
        momentum20: float | None,
        volatility20: float | None,
        fundamental_score: float | None,
    ) -> str:
        day_text = "平稳"
        if day_change_pct is not None:
            if day_change_pct > 0.01:
                day_text = "偏强"
            elif day_change_pct < -0.01:
                day_text = "偏弱"
            else:
                day_text = "震荡"

        trend_text = "中性"
        if momentum20 is not None:
            if momentum20 > 0.06:
                trend_text = "中期上行"
            elif momentum20 < -0.06:
                trend_text = "中期下行"

        vol_text = "波动正常"
        if volatility20 is not None and volatility20 >= 0.05:
            vol_text = "波动偏高"

        quality_text = "质量中性"
        if fundamental_score is not None:
            if fundamental_score >= 0.62:
                quality_text = "质量较优"
            elif fundamental_score <= 0.40:
                quality_text = "质量偏弱"

        return f"当日{day_text}，{trend_text}，{vol_text}，{quality_text}。"

    @staticmethod
    def _analysis_note(*, signal: SignalAction, expected_return: float, up_probability: float) -> str:
        return f"策略信号={signal.value}，预计次日收益={expected_return:.2%}，上涨概率={up_probability:.1%}。"

    @staticmethod
    def _rationale(*, signal: SignalAction, expected_return: float, up_probability: float) -> str:
        if signal == SignalAction.SELL:
            return "策略信号为 SELL，且短期胜率/收益前景偏弱。"
        if signal == SignalAction.BUY and expected_return > 0:
            return "策略信号为 BUY，趋势与收益预期同向，可考虑加仓。"
        if expected_return < 0:
            return "预期收益转负，建议控制仓位与单票风险敞口。"
        if up_probability < 0.5:
            return "上涨概率不占优，建议以持有观察为主。"
        return "信号与预测中性，维持仓位纪律，等待更强触发条件。"

    @staticmethod
    def _confidence(*, expected_return: float, up_probability: float) -> float:
        edge = min(1.0, abs(expected_return) / 0.05)
        prob_edge = min(1.0, abs(up_probability - 0.5) / 0.5)
        return HoldingService._bounded(0.35 + 0.35 * edge + 0.30 * prob_edge, 0.20, 0.95)

    @staticmethod
    def _market_overview(
        positions: list[ManualHoldingAnalysisPosition],
        *,
        next_trade_date: date | None,
        recommendations: list[ManualHoldingRecommendationItem],
        style_snapshot: dict[str, object] | None = None,
    ) -> str:
        if not positions:
            return "当前无持仓，请先在持仓页录入手工成交，再生成组合分析与次日建议。"

        avg_day = sum(float(x.day_change_pct or 0.0) for x in positions) / len(positions)
        avg_expected = sum(float(x.expected_next_day_return) for x in positions) / len(positions)
        risk_high = sum(1 for x in positions if (x.volatility20 or 0.0) >= 0.05)
        add_count = sum(1 for x in positions if x.suggested_action == HoldingRecommendationAction.ADD)
        reduce_count = sum(1 for x in positions if x.suggested_action == HoldingRecommendationAction.REDUCE)
        exit_count = sum(1 for x in positions if x.suggested_action == HoldingRecommendationAction.EXIT)
        intraday_high_risk = sum(1 for x in recommendations if str(x.intraday_risk_level).upper() == "HIGH")
        key_windows = [x.execution_window for x in recommendations if str(x.execution_window).strip()]
        preferred_window = key_windows[0] if key_windows else "10:20-10:50"
        style = dict(style_snapshot or {})
        regime = str(style.get("regime", "NEUTRAL")).strip().upper() or "NEUTRAL"
        risk_on_score = HoldingService._bounded(float(style.get("risk_on_score", 0.5) or 0.5), 0.0, 1.0)
        flow_score = HoldingService._bounded(float(style.get("flow_score", 0.5) or 0.5), 0.0, 1.0)

        bias = "中性"
        if avg_expected >= 0.006:
            bias = "偏多"
        elif avg_expected <= -0.006:
            bias = "偏空"
        if regime == "RISK_OFF" and bias == "偏多":
            bias = "谨慎偏多"
        elif regime == "RISK_ON" and bias == "偏空":
            bias = "震荡偏空"

        next_trade_text = f"下一交易日：{next_trade_date.isoformat()}。" if next_trade_date else "下一交易日：待确认。"
        return (
            f"组合当日平均涨跌 {avg_day:.2%}，预计次日平均收益 {avg_expected:.2%}，整体倾向{bias}。"
            f"高波动持仓 {risk_high}/{len(positions)}；建议加仓 {add_count} 只、减仓 {reduce_count} 只、退出 {exit_count} 只。"
            f"资金风格判定 {regime}（risk_on={risk_on_score:.2f}, flow={flow_score:.2f}）。"
            f"盘中高风险建议 {intraday_high_risk} 条，优先执行时段参考 {preferred_window}。"
            f"{next_trade_text}"
        )

    @staticmethod
    def _to_float(value: object) -> float | None:
        if value is None:
            return None
        try:
            out = float(value)
        except Exception:  # noqa: BLE001
            return None
        if math.isnan(out) or math.isinf(out):
            return None
        return out

    @staticmethod
    def _bounded(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(value)))
