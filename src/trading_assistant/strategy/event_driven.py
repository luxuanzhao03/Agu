from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class EventDrivenStrategy(BaseStrategy):
    info = StrategyInfo(
        name="event_driven",
        title="Event Driven",
        description="Event score based trigger for announcement-driven opportunities.",
        frequency="D/Intraday",
        params_schema={"event_score": "float", "negative_event_score": "float"},
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        latest = features.sort_values("trade_date").iloc[-1]

        event_score = float(context.params.get("event_score", latest.get("event_score", 0.0)))
        negative_event = float(context.params.get("negative_event_score", latest.get("negative_event_score", 0.0)))
        momentum20 = float(latest.get("momentum20", 0.0))

        if event_score >= 0.7 and momentum20 >= -0.03:
            action = SignalAction.BUY
            reason = "Positive event score and acceptable momentum."
        elif negative_event >= 0.6:
            action = SignalAction.SELL
            reason = "Negative event risk is elevated."
        else:
            action = SignalAction.WATCH
            reason = "No dominant event signal."

        confidence = min(0.95, max(0.2, max(event_score, negative_event)))
        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=confidence,
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=0.03 if action == SignalAction.BUY else None,
                metadata={
                    "event_score": round(event_score, 4),
                    "negative_event_score": round(negative_event, 4),
                },
            )
        ]

