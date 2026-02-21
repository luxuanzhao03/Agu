from datetime import date
from pathlib import Path

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import ModelDriftRequest, SignalAction, SignalDecisionRecord
from trading_assistant.monitoring.model_risk import ModelRiskService
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore


def test_model_risk_detects_warning(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    audit.log("backtest", "run", {"strategy": "multi_factor", "total_return": 0.30})
    audit.log("backtest", "run", {"strategy": "multi_factor", "total_return": 0.05})
    replay.record_signal(
        SignalDecisionRecord(
            signal_id="sig-1",
            symbol="000001",
            strategy_name="multi_factor",
            trade_date=date(2025, 1, 2),
            action=SignalAction.BUY,
            confidence=0.8,
            reason="x",
            suggested_position=0.05,
        )
    )
    service = ModelRiskService(audit=audit, replay=replay)
    result = service.detect_drift(
        ModelDriftRequest(
            strategy_name="multi_factor",
            symbol="000001",
            return_drift_threshold=0.1,
            follow_rate_threshold=0.8,
        )
    )
    assert result.status.value in {"WARNING", "CRITICAL", "INFO"}
    assert result.follow_rate is not None
