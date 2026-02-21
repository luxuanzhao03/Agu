from datetime import date
from pathlib import Path

from trading_assistant.core.models import ExecutionRecordCreate, SignalAction, SignalDecisionRecord
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore


def test_replay_signal_and_execution(tmp_path: Path) -> None:
    service = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    signal_id = "sig-1"
    service.record_signal(
        SignalDecisionRecord(
            signal_id=signal_id,
            symbol="000001",
            strategy_name="trend_following",
            trade_date=date(2025, 1, 2),
            action=SignalAction.BUY,
            confidence=0.8,
            reason="test",
            suggested_position=0.05,
        )
    )
    row_id = service.record_execution(
        ExecutionRecordCreate(
            signal_id=signal_id,
            symbol="000001",
            execution_date=date(2025, 1, 2),
            side=SignalAction.BUY,
            quantity=100,
            price=10.0,
            fee=1.0,
            note="manual",
        )
    )
    assert row_id > 0

    report = service.report(symbol="000001")
    assert len(report.items) == 1
    assert report.follow_rate >= 0
