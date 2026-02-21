from __future__ import annotations

from trading_assistant.core.models import RiskCheckResult, SignalAction, SignalCandidate, TradePrepSheet


class SignalService:
    def to_trade_prep_sheet(self, signal: SignalCandidate, risk: RiskCheckResult) -> TradePrepSheet:
        recs: list[str] = []
        if signal.action == SignalAction.BUY:
            recs.extend(
                [
                    "Use staged entries to reduce execution impact.",
                    "Recheck limit-up, suspension, and ST status before order entry.",
                    "Set predefined stop and max-loss boundaries.",
                ]
            )
        elif signal.action == SignalAction.SELL:
            recs.extend(
                [
                    "Verify available quantity to satisfy T+1 constraint.",
                    "If close to limit-down, estimate fill probability first.",
                    "Use partial exits and record execution slippage.",
                ]
            )
        else:
            recs.append("Keep monitoring and wait for higher-confidence confirmation.")

        if risk.blocked:
            recs.insert(0, "Signal is blocked by hard risk rules. No execution.")
        elif risk.level.value == "WARNING":
            recs.insert(0, "Signal carries execution risk warnings; require manual review.")

        recs.extend(risk.recommendations)
        return TradePrepSheet(signal=signal, risk=risk, recommendations=recs)

