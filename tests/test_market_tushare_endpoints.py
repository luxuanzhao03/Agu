from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_audit_service, get_data_provider, get_snapshot_service
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.governance.snapshot_store import DataSnapshotStore
from trading_assistant.main import app


class FakeTushareProvider(MarketDataProvider):
    name = "tushare"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        _ = (symbol, start_date, end_date)
        return pd.DataFrame(
            [
                {
                    "trade_date": date(2025, 1, 2),
                    "symbol": "000001",
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.1,
                    "volume": 100_000.0,
                    "amount": 1_010_000.0,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        _ = end_date
        return pd.DataFrame([{"trade_date": start_date, "is_open": True}])

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}

    def get_intraday_bars(
        self,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        *,
        interval: str = "15m",
    ) -> pd.DataFrame:
        _ = end_datetime
        return pd.DataFrame(
            [
                {
                    "bar_time": start_datetime,
                    "symbol": symbol,
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.05,
                    "volume": 1000.0,
                    "amount": 10050.0,
                    "interval": interval,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )

    def list_advanced_capabilities(self, user_points: int = 0) -> list[dict[str, object]]:
        _ = user_points
        return [
            {
                "dataset_name": "daily_basic",
                "api_name": "daily_basic",
                "category": "market_microstructure",
                "min_points_hint": 2000,
                "eligible": True,
                "api_available": True,
                "ready_to_call": True,
                "integrated_in_system": True,
                "integrated_targets": ["factor_engine", "strategy"],
                "notes": "fake",
            }
        ]

    def prefetch_advanced_datasets(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        user_points: int = 0,
        include_ineligible: bool = False,
    ) -> dict[str, object]:
        _ = (user_points, include_ineligible)
        return {
            "symbol": symbol,
            "ts_code": "000001.SZ",
            "start_date": start_date,
            "end_date": end_date,
            "user_points": user_points,
            "include_ineligible": include_ineligible,
            "summary": {"total": 1, "success": 1, "failed": 0, "skipped": 0},
            "results": [
                {
                    "dataset_name": "daily_basic",
                    "api_name": "daily_basic",
                    "category": "market_microstructure",
                    "min_points_hint": 2000,
                    "eligible": True,
                    "api_available": True,
                    "ready_to_call": True,
                    "integrated_in_system": True,
                    "integrated_targets": ["factor_engine", "strategy"],
                    "notes": "fake",
                    "status": "success",
                    "row_count": 10,
                    "column_count": 8,
                    "used_params": {"ts_code": "000001.SZ"},
                    "error": "",
                }
            ],
        }


def _setup_overrides(tmp_path: Path) -> None:
    provider = CompositeDataProvider([FakeTushareProvider()])
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    snapshots = DataSnapshotService(DataSnapshotStore(str(tmp_path / "snapshot.db")))
    app.dependency_overrides[get_data_provider] = lambda: provider
    app.dependency_overrides[get_audit_service] = lambda: audit
    app.dependency_overrides[get_snapshot_service] = lambda: snapshots


def test_market_tushare_capabilities_endpoint(tmp_path: Path) -> None:
    _setup_overrides(tmp_path)
    client = TestClient(app)
    try:
        resp = client.get("/market/tushare/capabilities?user_points=2120")
        assert resp.status_code == 200
        data = resp.json()
        assert data["provider"] == "tushare"
        assert data["dataset_total"] == 1
        assert data["ready_total"] == 1
    finally:
        app.dependency_overrides.clear()


def test_market_tushare_prefetch_endpoint(tmp_path: Path) -> None:
    _setup_overrides(tmp_path)
    client = TestClient(app)
    try:
        resp = client.post(
            "/market/tushare/prefetch",
            json={
                "symbol": "000001",
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "user_points": 2120,
                "include_ineligible": False,
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["provider"] == "tushare"
        assert data["summary"]["success"] == 1
        assert data["results"][0]["dataset_name"] == "daily_basic"
    finally:
        app.dependency_overrides.clear()


def test_market_intraday_endpoint(tmp_path: Path) -> None:
    _setup_overrides(tmp_path)
    client = TestClient(app)
    try:
        resp = client.get(
            "/market/intraday?symbol=000001&start_datetime=2025-01-02T09:30:00&end_datetime=2025-01-02T10:00:00&interval=15m"
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["provider"] == "tushare"
        assert payload["symbol"] == "000001"
        assert payload["interval"] == "15m"
        assert payload["row_count"] == 1
    finally:
        app.dependency_overrides.clear()
