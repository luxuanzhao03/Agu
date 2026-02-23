from datetime import date
from pathlib import Path

from trading_assistant.core.models import (
    CostModelCalibrationRequest,
    ExecutionRecordCreate,
    SignalAction,
    SignalDecisionRecord,
)
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
    assert report.items[0].strategy_name == "trend_following"
    assert report.items[0].slippage_available is False

    attr = service.attribution(symbol="000001")
    assert attr.sample_size == 1
    assert isinstance(attr.suggestions, list)


def test_replay_slippage_from_reference_price(tmp_path: Path) -> None:
    service = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    signal_id = "sig-2"
    service.record_signal(
        SignalDecisionRecord(
            signal_id=signal_id,
            symbol="000001",
            strategy_name="trend_following",
            trade_date=date(2025, 1, 3),
            action=SignalAction.BUY,
            confidence=0.82,
            reason="test",
            suggested_position=0.05,
        )
    )
    _ = service.record_execution(
        ExecutionRecordCreate(
            signal_id=signal_id,
            symbol="000001",
            execution_date=date(2025, 1, 3),
            side=SignalAction.BUY,
            quantity=200,
            price=10.20,
            reference_price=10.00,
            fee=1.2,
            note="manual",
        )
    )

    report = service.report(symbol="000001")
    assert len(report.items) == 1
    assert report.items[0].slippage_available is True
    assert report.items[0].slippage_bps > 0
    assert report.avg_slippage_bps > 0


def test_replay_attribution_contains_bucket_and_drag_metrics(tmp_path: Path) -> None:
    service = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    service.record_signal(
        SignalDecisionRecord(
            signal_id="sig-a",
            symbol="000001",
            strategy_name="trend_following",
            trade_date=date(2025, 1, 4),
            action=SignalAction.BUY,
            confidence=0.90,
            reason="x",
            suggested_position=0.05,
        )
    )
    service.record_signal(
        SignalDecisionRecord(
            signal_id="sig-b",
            symbol="000002",
            strategy_name="mean_reversion",
            trade_date=date(2025, 1, 4),
            action=SignalAction.SELL,
            confidence=0.78,
            reason="x",
            suggested_position=0.03,
        )
    )
    _ = service.record_execution(
        ExecutionRecordCreate(
            signal_id="sig-a",
            symbol="000001",
            execution_date=date(2025, 1, 6),
            side=SignalAction.BUY,
            quantity=100,
            price=10.6,
            reference_price=10.0,
            fee=1.0,
            note="delayed and expensive",
        )
    )

    report = service.attribution()
    assert report.sample_size == 2
    assert "NO_EXECUTION" in report.reason_counts
    assert "EXECUTION_DELAY" in report.reason_counts
    assert report.estimated_total_drag_bps > 0
    assert report.top_symbols
    assert report.top_strategies


def test_replay_cost_model_calibration_and_history(tmp_path: Path) -> None:
    service = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    for idx in range(8):
        signal_id = f"sig-{idx}"
        service.record_signal(
            SignalDecisionRecord(
                signal_id=signal_id,
                symbol="000001",
                strategy_name="trend_following",
                trade_date=date(2025, 1, 1 + idx),
                action=SignalAction.BUY,
                confidence=0.7,
                reason="x",
                suggested_position=0.04,
            )
        )
        _ = service.record_execution(
            ExecutionRecordCreate(
                signal_id=signal_id,
                symbol="000001",
                execution_date=date(2025, 1, 1 + idx),
                side=SignalAction.BUY,
                quantity=100,
                price=10.0 + idx * 0.05,
                reference_price=10.0,
                fee=1.0,
                note="batch",
            )
        )

    result = service.calibrate_cost_model(
        CostModelCalibrationRequest(
            symbol="000001",
            strategy_name="trend_following",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            limit=100,
            min_samples=5,
            save_record=True,
        )
    )
    assert result.sample_size == 8
    assert result.calibration_id is not None
    assert result.recommended_slippage_rate > 0
    history = service.list_cost_calibrations(symbol="000001", limit=10)
    assert len(history) >= 1
    assert history[0].result.calibration_id == result.calibration_id
    assert history[0].result.sample_size == 8


def test_replay_aggregates_multiple_executions_per_signal(tmp_path: Path) -> None:
    service = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    signal_id = "sig-multi"
    service.record_signal(
        SignalDecisionRecord(
            signal_id=signal_id,
            symbol="000001",
            strategy_name="trend_following",
            trade_date=date(2025, 1, 10),
            action=SignalAction.BUY,
            confidence=0.8,
            reason="x",
            suggested_position=0.05,
        )
    )
    _ = service.record_execution(
        ExecutionRecordCreate(
            signal_id=signal_id,
            symbol="000001",
            execution_date=date(2025, 1, 10),
            side=SignalAction.BUY,
            quantity=100,
            price=10.0,
            reference_price=9.9,
            fee=1.0,
            note="part1",
        )
    )
    _ = service.record_execution(
        ExecutionRecordCreate(
            signal_id=signal_id,
            symbol="000001",
            execution_date=date(2025, 1, 10),
            side=SignalAction.BUY,
            quantity=200,
            price=10.2,
            reference_price=10.0,
            fee=1.5,
            note="part2",
        )
    )

    report = service.report(symbol="000001")
    assert len(report.items) == 1
    item = report.items[0]
    assert item.executed_quantity == 300
    assert abs(item.executed_price - ((100 * 10.0 + 200 * 10.2) / 300.0)) < 1e-9
    assert item.slippage_available is True
