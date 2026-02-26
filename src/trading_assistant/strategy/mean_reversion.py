from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class MeanReversionStrategy(BaseStrategy):
    info = StrategyInfo(
        name="mean_reversion",
        title="Mean Reversion",
        description="Z-score based short swing strategy with liquidity filter.",
        frequency="D",
        params_schema={"z_enter": "float", "z_exit": "float", "min_turnover": "float"},
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        z_enter = float(context.params.get("z_enter", 1.5))
        z_exit = float(context.params.get("z_exit", -0.1))
        min_turnover = float(context.params.get("min_turnover", 2_000_000.0))

        latest = features.iloc[-1]

        def _opt_float(value: object) -> float | None:
            try:
                out = float(value)
            except Exception:  # noqa: BLE001
                return None
            if out != out:  # NaN
                return None
            return out

        z = _opt_float(latest.get("zscore20"))
        turnover = _opt_float(latest.get("turnover20"))
        momentum20 = _opt_float(latest.get("momentum20"))
        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_advanced_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced_score = float(latest.get("tushare_advanced_score", 0.5)) if tushare_advanced_available else 0.5
        disclosure_risk = (
            float(latest.get("tushare_disclosure_risk_score", 0.5)) if tushare_advanced_available else 0.5
        )

        if z is None or turnover is None:
            action = SignalAction.WATCH
            reason = "Insufficient factor history for mean-reversion signals."
        elif turnover < min_turnover:
            action = SignalAction.WATCH
            reason = "Liquidity too low for mean-reversion execution."
        elif z <= -abs(z_enter):
            action = SignalAction.BUY
            reason = f"Price deviates below mean (z={z:.2f}), reversion entry candidate."
        elif z >= abs(z_exit) or (z > -0.15 and momentum20 is not None and momentum20 < -0.02):
            action = SignalAction.SELL
            reason = f"Mean reversion completed (z={z:.2f}), consider exit."
        else:
            action = SignalAction.WATCH
            reason = "Z-score in neutral zone."

        if action == SignalAction.BUY and fundamental_available and fundamental_score < 0.25:
            action = SignalAction.WATCH
            reason = (
                f"Mean-reversion setup exists, but fundamental score {fundamental_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_advanced_available and tushare_advanced_score < 0.18:
            action = SignalAction.WATCH
            reason = (
                f"Mean-reversion setup exists, but tushare advanced score {tushare_advanced_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and disclosure_risk >= 0.90:
            action = SignalAction.WATCH
            reason = f"Mean-reversion setup blocked by disclosure risk ({disclosure_risk:.2f})."

        z_for_confidence = float(z) if z is not None else 0.0
        base_confidence = min(0.95, max(0.2, abs(z_for_confidence) / 3))
        if not fundamental_available and not tushare_advanced_available:
            confidence = base_confidence
        else:
            confidence = min(
                0.95,
                max(0.2, 0.60 * base_confidence + 0.25 * fundamental_score + 0.15 * (1.0 - disclosure_risk)),
            )
        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=confidence,
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=0.07 if action == SignalAction.BUY else None,
                metadata={
                    "zscore20": (round(float(z), 4) if z is not None else None),
                    "turnover20": (round(float(turnover), 2) if turnover is not None else None),
                    "momentum20": (round(float(momentum20), 4) if momentum20 is not None else None),
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_disclosure_risk_score": round(disclosure_risk, 4),
                },
            )
        ]
