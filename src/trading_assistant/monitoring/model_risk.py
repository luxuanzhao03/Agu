from __future__ import annotations

from statistics import mean

from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import ModelDriftRequest, ModelDriftResult, SignalLevel
from trading_assistant.replay.service import ReplayService


class ModelRiskService:
    def __init__(self, audit: AuditService, replay: ReplayService) -> None:
        self.audit = audit
        self.replay = replay

    def detect_drift(self, req: ModelDriftRequest) -> ModelDriftResult:
        events = self.audit.query(event_type="backtest", limit=req.lookback_events)
        strategy_events = [e for e in events if str(e.payload.get("strategy")) == req.strategy_name]
        returns: list[float] = []
        for e in strategy_events:
            value = e.payload.get("total_return")
            try:
                returns.append(float(value))
            except Exception:  # noqa: BLE001
                continue

        baseline = None
        recent = None
        drift = None
        warnings: list[str] = []
        status = SignalLevel.INFO
        if returns:
            recent = returns[0]
            if len(returns) > 1:
                baseline = mean(returns[1:]) if len(returns) > 2 else returns[-1]
            else:
                baseline = returns[0]
            drift = abs(recent - baseline)
            if drift > req.return_drift_threshold:
                status = SignalLevel.WARNING
                warnings.append(
                    f"Return drift {drift:.4f} exceeds threshold {req.return_drift_threshold:.4f}."
                )

        replay = self.replay.report(symbol=req.symbol, limit=req.lookback_events)
        follow_rate = replay.follow_rate
        if follow_rate < req.follow_rate_threshold:
            status = SignalLevel.WARNING if status == SignalLevel.INFO else status
            warnings.append(
                f"Follow rate {follow_rate:.4f} below threshold {req.follow_rate_threshold:.4f}."
            )

        if not returns:
            warnings.append("No backtest history found for requested strategy.")

        if warnings and status == SignalLevel.INFO:
            status = SignalLevel.WARNING

        return ModelDriftResult(
            strategy_name=req.strategy_name,
            symbol=req.symbol,
            baseline_return=baseline,
            recent_return=recent,
            return_drift=drift,
            follow_rate=follow_rate,
            status=status,
            warnings=warnings,
        )

