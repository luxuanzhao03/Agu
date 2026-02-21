from datetime import date
from pathlib import Path

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import ReportGenerateRequest, SignalAction, SignalDecisionRecord
from trading_assistant.reporting.service import ReportingService
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore


def test_reporting_service_generates_signal_report(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    audit.log("signal_generation", "generate", {"symbol": "000001", "strategy": "trend_following", "signals": 1})
    service = ReportingService(replay=replay, audit=audit, output_dir=str(tmp_path / "reports"))
    result = service.generate(ReportGenerateRequest(report_type="signal", save_to_file=True))
    assert "Signal Generation Report" in result.content
    assert result.saved_path is not None


def test_reporting_service_generates_replay_report(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    replay.record_signal(
        SignalDecisionRecord(
            signal_id="sig-1",
            symbol="000001",
            strategy_name="trend_following",
            trade_date=date(2025, 1, 2),
            action=SignalAction.BUY,
            confidence=0.8,
            reason="x",
            suggested_position=0.05,
        )
    )
    service = ReportingService(replay=replay, audit=audit, output_dir=str(tmp_path / "reports"))
    result = service.generate(ReportGenerateRequest(report_type="replay", save_to_file=False))
    assert "Execution Replay Report" in result.content
