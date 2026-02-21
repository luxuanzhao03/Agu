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
        z_enter = float(context.params.get("z_enter", 2.0))
        z_exit = float(context.params.get("z_exit", 0.0))
        min_turnover = float(context.params.get("min_turnover", 5_000_000.0))

        latest = features.sort_values("trade_date").iloc[-1]
        z = float(latest.get("zscore20", 0.0))
        turnover = float(latest.get("turnover20", 0.0))

        if turnover < min_turnover:
            action = SignalAction.WATCH
            reason = "Liquidity too low for mean-reversion execution."
        elif z <= -abs(z_enter):
            action = SignalAction.BUY
            reason = f"Price deviates below mean (z={z:.2f}), reversion entry candidate."
        elif z >= abs(z_exit):
            action = SignalAction.SELL
            reason = f"Mean reversion completed (z={z:.2f}), consider exit."
        else:
            action = SignalAction.WATCH
            reason = "Z-score in neutral zone."

        confidence = min(0.95, max(0.2, abs(z) / 3))
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
                    "zscore20": round(z, 4),
                    "turnover20": round(turnover, 2),
                },
            )
        ]

