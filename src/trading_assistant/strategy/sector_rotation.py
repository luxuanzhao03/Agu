from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class SectorRotationStrategy(BaseStrategy):
    info = StrategyInfo(
        name="sector_rotation",
        title="Sector Rotation",
        description="Sector-strength guided timing model for A-share rotation cycles.",
        frequency="D/W",
        params_schema={"sector_strength": "float", "risk_off_strength": "float"},
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        sector_strength = float(context.params.get("sector_strength", context.market_state.get("sector_strength", 0.5)))
        risk_off_strength = float(
            context.params.get("risk_off_strength", context.market_state.get("risk_off_strength", 0.5))
        )

        latest = features.iloc[-1]
        momentum20 = float(latest.get("momentum20", 0.0))
        momentum60 = float(latest.get("momentum60", 0.0))
        vol20 = float(latest.get("volatility20", 0.0))
        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_advanced_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced_score = float(latest.get("tushare_advanced_score", 0.5)) if tushare_advanced_available else 0.5
        disclosure_risk = (
            float(latest.get("tushare_disclosure_risk_score", 0.5)) if tushare_advanced_available else 0.5
        )

        if (
            sector_strength >= 0.52
            and momentum20 >= -0.01
            and momentum20 >= (momentum60 - 0.02)
            and vol20 < 0.075
        ):
            action = SignalAction.BUY
            reason = "Sector strength and symbol momentum align."
        elif risk_off_strength >= 0.58 or (momentum20 < -0.01 and latest["close"] < latest["ma20"] * 1.01):
            action = SignalAction.SELL
            reason = "Risk-off regime or sector momentum deterioration."
        else:
            action = SignalAction.WATCH
            reason = "Rotation signal not confirmed."

        if action == SignalAction.BUY and fundamental_available and fundamental_score < 0.25:
            action = SignalAction.WATCH
            reason = (
                f"Rotation buy setup exists, but fundamental score {fundamental_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_advanced_available and tushare_advanced_score < 0.20:
            action = SignalAction.WATCH
            reason = (
                f"Rotation buy setup exists, but tushare advanced score {tushare_advanced_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and disclosure_risk >= 0.90:
            action = SignalAction.WATCH
            reason = f"Rotation buy setup blocked by disclosure risk ({disclosure_risk:.2f})."

        base_confidence = min(0.95, max(0.2, 0.5 * sector_strength + 0.5 * max(0.0, momentum20 + 0.5)))
        if not fundamental_available and not tushare_advanced_available:
            confidence = base_confidence
        else:
            confidence = min(
                0.95,
                max(0.2, 0.65 * base_confidence + 0.20 * fundamental_score + 0.15 * (1.0 - disclosure_risk)),
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
                    "sector_strength": round(sector_strength, 4),
                    "risk_off_strength": round(risk_off_strength, 4),
                    "momentum20": round(momentum20, 4),
                    "momentum60": round(momentum60, 4),
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_disclosure_risk_score": round(disclosure_risk, 4),
                },
            )
        ]
