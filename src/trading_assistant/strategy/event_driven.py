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
        latest = features.iloc[-1]

        def _opt_float(value: object) -> float | None:
            try:
                out = float(value)
            except Exception:  # noqa: BLE001
                return None
            if out != out:  # NaN
                return None
            return out

        buy_event_threshold = float(context.params.get("event_score", 0.58))
        sell_event_threshold = float(context.params.get("negative_event_score", 0.45))
        event_score = _opt_float(latest.get("event_score")) or 0.0
        negative_event = _opt_float(latest.get("negative_event_score")) or 0.0
        momentum20 = _opt_float(latest.get("momentum20"))
        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_advanced_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced_score = float(latest.get("tushare_advanced_score", 0.5)) if tushare_advanced_available else 0.5
        disclosure_risk = (
            float(latest.get("tushare_disclosure_risk_score", 0.5)) if tushare_advanced_available else 0.5
        )

        if event_score >= buy_event_threshold and (momentum20 is None or momentum20 >= -0.06):
            action = SignalAction.BUY
            if momentum20 is None:
                reason = (
                    f"Positive event score ({event_score:.2f}) >= threshold ({buy_event_threshold:.2f}); "
                    "momentum20 unavailable, skipped momentum filter."
                )
            else:
                reason = f"Positive event score ({event_score:.2f}) >= threshold ({buy_event_threshold:.2f})."
        elif event_score >= buy_event_threshold and momentum20 < -0.06:
            action = SignalAction.WATCH
            reason = (
                f"Positive event score reached threshold, but momentum20 ({momentum20:.3f}) "
                "is below risk guardrail (-0.060)."
            )
        elif negative_event >= sell_event_threshold:
            action = SignalAction.SELL
            reason = f"Negative event score ({negative_event:.2f}) >= threshold ({sell_event_threshold:.2f})."
        else:
            action = SignalAction.WATCH
            reason = "No dominant event signal."

        if action == SignalAction.BUY and fundamental_available and fundamental_score < 0.25:
            action = SignalAction.WATCH
            reason = (
                f"Event trigger is positive, but fundamental score {fundamental_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_advanced_available and tushare_advanced_score < 0.20:
            action = SignalAction.WATCH
            reason = (
                f"Event trigger is positive, but tushare advanced score {tushare_advanced_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and disclosure_risk >= 0.90:
            action = SignalAction.WATCH
            reason = f"Event trigger blocked by disclosure risk ({disclosure_risk:.2f})."

        base_confidence = min(0.95, max(0.2, max(event_score, negative_event)))
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
                    "event_score": round(event_score, 4),
                    "negative_event_score": round(negative_event, 4),
                    "momentum20": (round(float(momentum20), 4) if momentum20 is not None else None),
                    "buy_event_threshold": round(buy_event_threshold, 4),
                    "sell_event_threshold": round(sell_event_threshold, 4),
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_disclosure_risk_score": round(disclosure_risk, 4),
                },
            )
        ]
