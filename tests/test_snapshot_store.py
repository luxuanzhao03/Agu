from datetime import date
from pathlib import Path

from trading_assistant.core.models import DataSnapshotRegisterRequest
from trading_assistant.governance.snapshot_store import DataSnapshotStore


def test_snapshot_store_register_and_latest(tmp_path: Path) -> None:
    store = DataSnapshotStore(str(tmp_path / "snapshot.db"))
    snapshot_id = store.register(
        DataSnapshotRegisterRequest(
            dataset_name="daily_bars",
            symbol="000001",
            start_date=date(2025, 1, 1),
            end_date=date(2025, 1, 31),
            provider="akshare",
            row_count=20,
            content_hash="hash123",
        )
    )
    assert snapshot_id > 0
    latest = store.latest_snapshot("daily_bars", "000001")
    assert latest is not None
    assert latest.content_hash == "hash123"
