from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class TrendFollowingStrategy(BaseStrategy):
    info = StrategyInfo(
        name="trend_following",
        title="Trend Following",
        description="MA crossover with ATR exit filter, suitable for daily bars.",
        frequency="D",
        params_schema={"entry_ma_fast": "int", "entry_ma_slow": "int", "atr_multiplier": "float"},
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        atr_mult = float(context.params.get("atr_multiplier", 2.0))
        df = features.sort_values("trade_date")
        latest = df.iloc[-1]

        if len(df) < 2:
            action = SignalAction.WATCH
            reason = "Insufficient history for trend confirmation."
        else:
            prev = df.iloc[-2]
            long_signal = bool(
                latest["ma20"] > latest["ma60"] and prev["close"] <= prev["ma20"] and latest["close"] > latest["ma20"]
            )
            exit_signal = bool(latest["close"] < latest["ma20"] - atr_mult * latest["atr14"])

            if long_signal:
                action = SignalAction.BUY
                reason = "MA20 above MA60 and price confirms breakout."
            elif exit_signal:
                action = SignalAction.SELL
                reason = "Price breaks below dynamic ATR exit band."
            else:
                action = SignalAction.WATCH
                reason = "No clear trend entry or exit."

        strength = abs(float(latest.get("momentum20", 0.0)))
        confidence = min(0.95, max(0.25, strength * 2 + 0.45))
        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=confidence,
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=0.05 if action == SignalAction.BUY else None,
                metadata={
                    "ma20": round(float(latest.get("ma20", 0.0)), 4),
                    "ma60": round(float(latest.get("ma60", 0.0)), 4),
                    "atr14": round(float(latest.get("atr14", 0.0)), 4),
                },
            )
        ]

