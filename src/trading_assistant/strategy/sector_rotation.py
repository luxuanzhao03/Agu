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

        latest = features.sort_values("trade_date").iloc[-1]
        momentum20 = float(latest.get("momentum20", 0.0))
        momentum60 = float(latest.get("momentum60", 0.0))
        vol20 = float(latest.get("volatility20", 0.0))

        if sector_strength >= 0.6 and momentum20 > 0 and momentum20 >= momentum60 and vol20 < 0.05:
            action = SignalAction.BUY
            reason = "Sector strength and symbol momentum align."
        elif risk_off_strength >= 0.65 or (momentum20 < 0 and latest["close"] < latest["ma20"]):
            action = SignalAction.SELL
            reason = "Risk-off regime or sector momentum deterioration."
        else:
            action = SignalAction.WATCH
            reason = "Rotation signal not confirmed."

        confidence = min(0.95, max(0.2, 0.5 * sector_strength + 0.5 * max(0.0, momentum20 + 0.5)))
        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=confidence,
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=0.04 if action == SignalAction.BUY else None,
                metadata={
                    "sector_strength": round(sector_strength, 4),
                    "risk_off_strength": round(risk_off_strength, 4),
                    "momentum20": round(momentum20, 4),
                    "momentum60": round(momentum60, 4),
                },
            )
        ]

