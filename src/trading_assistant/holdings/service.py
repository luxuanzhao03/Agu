from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import math

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


class HoldingService:
    def __init__(
        self,
        *,
        store: HoldingStore,
        provider: CompositeDataProvider,
        factor_engine: FactorEngine,
        registry: StrategyRegistry,
        autotune: AutoTuneService,
    ) -> None:
        self.store = store
        self.provider = provider
        self.factor_engine = factor_engine
        self.registry = registry
        self.autotune = autotune

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
        positions_result, snapshots = self._build_positions_and_snapshots(as_of_date=req.as_of_date)
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
            if snapshot is not None:
                signal = self._latest_strategy_signal(strategy, snapshot["features"], runtime_params)
                expected_ret, up_prob = self._forecast_next_day(snapshot["latest"])
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
            )
            if snapshot is None:
                risk_flags.append("NO_MARKET_DATA")

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
                    expected_next_day_return=round(expected_ret, 6),
                    up_probability=round(up_prob, 6),
                    strategy_signal=signal,
                    suggested_action=suggested_action,
                    suggested_delta_lots=int(delta_lots),
                    analysis_note=(
                        self._analysis_note(signal=signal, expected_return=expected_ret, up_probability=up_prob) + data_note
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

        market_overview = self._market_overview(analyzed_positions, next_trade_date=next_trade_date)
        return ManualHoldingAnalysisResult(
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

    def _build_positions_and_snapshots(
        self,
        *,
        as_of_date: date,
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
                snapshot = self._load_symbol_snapshot(symbol=state.symbol, as_of_date=as_of_date)
            except Exception as exc:  # noqa: BLE001
                load_error = str(exc)

            latest_price = float(state.avg_cost)
            latest_close_date = None
            day_change_pct = None
            momentum20 = None
            volatility20 = None
            fundamental_score = None
            advanced_score = None
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
                market_comment = self._market_comment(
                    day_change_pct=day_change_pct,
                    momentum20=momentum20,
                    volatility20=volatility20,
                    fundamental_score=fundamental_score,
                )
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

    def _load_symbol_snapshot(self, *, symbol: str, as_of_date: date) -> dict[str, object]:
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

        features = self.factor_engine.compute(frame)
        if features is None or features.empty:
            raise ValueError(f"{symbol}: factor feature frame is empty")
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
        }

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
    def _forecast_next_day(latest_row: pd.Series) -> tuple[float, float]:
        close = float(latest_row.get("close", 0.0) or 0.0)
        ma20 = float(latest_row.get("ma20", 0.0) or 0.0)
        momentum5 = float(latest_row.get("momentum5", 0.0) or 0.0)
        momentum20 = float(latest_row.get("momentum20", 0.0) or 0.0)
        volatility20 = max(0.0, float(latest_row.get("volatility20", 0.0) or 0.0))
        fundamental_score = float(latest_row.get("fundamental_score", 0.5) or 0.5)
        advanced_score = float(latest_row.get("tushare_advanced_score", 0.5) or 0.5)
        event_score = float(latest_row.get("event_score", 0.0) or 0.0)
        negative_event = float(latest_row.get("negative_event_score", 0.0) or 0.0)

        ma_bias = ((close - ma20) / ma20) if ma20 > 1e-12 else 0.0
        score = 0.0
        score += HoldingService._bounded(momentum5, -0.30, 0.30) * 2.3
        score += HoldingService._bounded(momentum20, -0.45, 0.45) * 1.7
        score += HoldingService._bounded(ma_bias, -0.20, 0.20) * 1.5
        score -= HoldingService._bounded(volatility20, 0.0, 0.25) * 2.1
        score += (HoldingService._bounded(fundamental_score, 0.0, 1.0) - 0.5) * 0.8
        score += (HoldingService._bounded(advanced_score, 0.0, 1.0) - 0.5) * 0.6
        score += HoldingService._bounded(event_score - negative_event, -1.0, 1.0) * 0.15

        expected_return = HoldingService._bounded(score * 0.018, -0.08, 0.08)
        up_probability = HoldingService._bounded(0.5 + score * 0.20, 0.05, 0.95)
        return float(expected_return), float(up_probability)

    @staticmethod
    def _position_action(
        *,
        signal: SignalAction,
        expected_return: float,
        up_probability: float,
        current_lots: int,
        volatility20: float | None,
        fundamental_score: float | None,
    ) -> tuple[HoldingRecommendationAction, int, list[str]]:
        lots = max(0, int(current_lots))
        risk_flags: list[str] = []
        if volatility20 is not None and volatility20 >= 0.05:
            risk_flags.append("HIGH_VOLATILITY")
        if fundamental_score is not None and fundamental_score < 0.42:
            risk_flags.append("WEAK_FUNDAMENTAL")

        if lots <= 0:
            return HoldingRecommendationAction.WATCH, 0, risk_flags

        if signal == SignalAction.SELL:
            if expected_return <= -0.025 or up_probability <= 0.28:
                return HoldingRecommendationAction.EXIT, -lots, risk_flags
            reduce_lots = min(lots, max(1, int(round(lots * 0.40))))
            return HoldingRecommendationAction.REDUCE, -reduce_lots, risk_flags

        if expected_return <= -0.020 or up_probability <= 0.35:
            reduce_lots = min(lots, max(1, int(round(lots * 0.30))))
            return HoldingRecommendationAction.REDUCE, -reduce_lots, risk_flags

        if signal == SignalAction.BUY and expected_return >= 0.010 and up_probability >= 0.60:
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
                snapshot = self._load_symbol_snapshot(symbol=symbol, as_of_date=req.as_of_date)
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
            expected_ret, up_prob = self._forecast_next_day(latest)
            if expected_ret < 0.004 or up_prob < 0.55:
                continue

            momentum20 = self._to_float(latest.get("momentum20"))
            volatility20 = self._to_float(latest.get("volatility20"))
            score = expected_ret * 0.62 + (up_prob - 0.5) * 0.38
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
                    rationale="候选池中信号为 BUY，且趋势/概率评分靠前，建议按资金上限试探建仓。",
                    risk_flags=risk_flags,
                )
            )
            if remaining_cash < lot_cost:
                break
        return out

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
    def _market_overview(positions: list[ManualHoldingAnalysisPosition], *, next_trade_date: date | None) -> str:
        if not positions:
            return "当前无持仓，请先在持仓页录入手工成交，再生成组合分析与次日建议。"

        avg_day = sum(float(x.day_change_pct or 0.0) for x in positions) / len(positions)
        avg_expected = sum(float(x.expected_next_day_return) for x in positions) / len(positions)
        risk_high = sum(1 for x in positions if (x.volatility20 or 0.0) >= 0.05)
        add_count = sum(1 for x in positions if x.suggested_action == HoldingRecommendationAction.ADD)
        reduce_count = sum(1 for x in positions if x.suggested_action == HoldingRecommendationAction.REDUCE)
        exit_count = sum(1 for x in positions if x.suggested_action == HoldingRecommendationAction.EXIT)

        bias = "中性"
        if avg_expected >= 0.006:
            bias = "偏多"
        elif avg_expected <= -0.006:
            bias = "偏空"

        next_trade_text = f"下一交易日：{next_trade_date.isoformat()}。" if next_trade_date else "下一交易日：待确认。"
        return (
            f"组合当日平均涨跌 {avg_day:.2%}，预计次日平均收益 {avg_expected:.2%}，整体倾向{bias}。"
            f"高波动持仓 {risk_high}/{len(positions)}；建议加仓 {add_count} 只、减仓 {reduce_count} 只、退出 {exit_count} 只。"
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
