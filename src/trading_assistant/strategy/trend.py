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
        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_advanced_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced_score = float(latest.get("tushare_advanced_score", 0.5)) if tushare_advanced_available else 0.5
        disclosure_risk = (
            float(latest.get("tushare_disclosure_risk_score", 0.5)) if tushare_advanced_available else 0.5
        )
        overhang_risk = float(latest.get("tushare_overhang_risk_score", 0.5)) if tushare_advanced_available else 0.5

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

        if action == SignalAction.BUY and fundamental_available and fundamental_score < 0.35:
            action = SignalAction.WATCH
            reason = (
                f"Trend entry detected, but fundamental score {fundamental_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_advanced_available and tushare_advanced_score < 0.32:
            action = SignalAction.WATCH
            reason = (
                f"Trend entry detected, but tushare advanced score {tushare_advanced_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and disclosure_risk >= 0.82:
            action = SignalAction.WATCH
            reason = f"Trend entry blocked by disclosure risk ({disclosure_risk:.2f})."

        strength = abs(float(latest.get("momentum20", 0.0)))
        base_confidence = min(0.95, max(0.25, strength * 2 + 0.45))
        if not fundamental_available and not tushare_advanced_available:
            confidence = base_confidence
        else:
            confidence = min(
                0.95,
                max(
                    0.2,
                    0.65 * base_confidence + 0.20 * fundamental_score + 0.15 * (1.0 - max(disclosure_risk, overhang_risk)),
                ),
            )
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
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_disclosure_risk_score": round(disclosure_risk, 4),
                    "tushare_overhang_risk_score": round(overhang_risk, 4),
                },
            )
        ]
