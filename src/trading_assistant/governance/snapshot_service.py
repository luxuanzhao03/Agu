from __future__ import annotations

from trading_assistant.core.models import DataSnapshotRecord, DataSnapshotRegisterRequest
from trading_assistant.governance.snapshot_store import DataSnapshotStore


class DataSnapshotService:
    def __init__(self, store: DataSnapshotStore) -> None:
        self.store = store

    def register(self, req: DataSnapshotRegisterRequest) -> int:
        return self.store.register(req)

    def list_snapshots(
        self,
        dataset_name: str | None = None,
        symbol: str | None = None,
        limit: int = 200,
    ) -> list[DataSnapshotRecord]:
        return self.store.list_snapshots(dataset_name=dataset_name, symbol=symbol, limit=limit)

    def latest(self, dataset_name: str, symbol: str) -> DataSnapshotRecord | None:
        return self.store.latest_snapshot(dataset_name=dataset_name, symbol=symbol)

