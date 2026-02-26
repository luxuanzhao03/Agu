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
        entry_ma_fast = max(2, int(context.params.get("entry_ma_fast", 12)))
        entry_ma_slow = max(entry_ma_fast + 1, int(context.params.get("entry_ma_slow", 34)))
        atr_mult = float(context.params.get("atr_multiplier", 1.6))
        df = features.copy()
        close_series = pd.to_numeric(df["close"], errors="coerce")

        def _resolve_ma(window: int) -> pd.Series:
            col = f"ma{window}"
            if col in df.columns:
                return pd.to_numeric(df[col], errors="coerce").fillna(close_series)
            return close_series.rolling(window=window, min_periods=1).mean()

        fast_ma = _resolve_ma(entry_ma_fast)
        slow_ma = _resolve_ma(entry_ma_slow)
        latest = df.iloc[-1]

        def _opt_float(value: object) -> float | None:
            try:
                out = float(value)
            except Exception:  # noqa: BLE001
                return None
            if out != out:  # NaN
                return None
            return out

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
            momentum20 = _opt_float(latest.get("momentum20"))
            atr14 = _opt_float(latest.get("atr14"))
        else:
            prev_fast = float(fast_ma.iloc[-2])
            prev_slow = float(slow_ma.iloc[-2])
            latest_fast = float(fast_ma.iloc[-1])
            latest_slow = float(slow_ma.iloc[-1])
            latest_close = float(close_series.iloc[-1])
            momentum20 = _opt_float(latest.get("momentum20"))
            atr14 = _opt_float(latest.get("atr14"))
            if momentum20 is None:
                action = SignalAction.WATCH
                reason = "Insufficient factor history: momentum20 unavailable."
            else:
                long_signal = bool(
                    latest_fast >= latest_slow * 0.998
                    and latest_close >= latest_fast * 0.997
                )
                exit_signal = bool(
                    (latest_fast < latest_slow * 0.998 and prev_fast >= prev_slow * 0.995)
                    or (latest_close < latest_fast - atr_mult * float(atr14 or 0.0))
                    or (momentum20 < -0.015 and latest_close < latest_fast)
                )

                if long_signal:
                    action = SignalAction.BUY
                    reason = f"MA{entry_ma_fast} is above MA{entry_ma_slow} and price confirms breakout."
                elif exit_signal:
                    action = SignalAction.SELL
                    reason = "Price breaks below dynamic ATR exit band."
                else:
                    action = SignalAction.WATCH
                    reason = "No clear trend entry or exit."

        if action == SignalAction.BUY and fundamental_available and fundamental_score < 0.25:
            action = SignalAction.WATCH
            reason = (
                f"Trend entry detected, but fundamental score {fundamental_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_advanced_available and tushare_advanced_score < 0.20:
            action = SignalAction.WATCH
            reason = (
                f"Trend entry detected, but tushare advanced score {tushare_advanced_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and disclosure_risk >= 0.90:
            action = SignalAction.WATCH
            reason = f"Trend entry blocked by disclosure risk ({disclosure_risk:.2f})."

        strength = abs(float(momentum20)) if momentum20 is not None else 0.0
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
                suggested_position=0.08 if action == SignalAction.BUY else None,
                metadata={
                    "entry_ma_fast": entry_ma_fast,
                    "entry_ma_slow": entry_ma_slow,
                    "ma_fast": round(float(fast_ma.iloc[-1]), 4),
                    "ma_slow": round(float(slow_ma.iloc[-1]), 4),
                    "atr14": (round(float(atr14), 4) if atr14 is not None else None),
                    "momentum20": (round(float(momentum20), 4) if momentum20 is not None else None),
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_disclosure_risk_score": round(disclosure_risk, 4),
                    "tushare_overhang_risk_score": round(overhang_risk, 4),
                },
            )
        ]
