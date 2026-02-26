from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from trading_assistant.applied_stats.service import AppliedStatisticsService
from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_applied_statistics_service, get_audit_service
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.main import app


class FakeStatsProvider(MarketDataProvider):
    name = "fake_stats_provider"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        cursor = start_date
        idx = 0
        while cursor <= end_date:
            if cursor.weekday() < 5:
                close = 9.6 + 0.003 * idx + ((idx % 5) - 2) * 0.005
                rows.append(
                    {
                        "trade_date": cursor,
                        "symbol": symbol,
                        "open": close * 0.997,
                        "high": close * 1.004,
                        "low": close * 0.994,
                        "close": close,
                        "volume": 80_000 + idx * 1_000,
                        "amount": close * (80_000 + idx * 1_000),
                        "is_suspended": False,
                        "is_st": False,
                    }
                )
                idx += 1
            cursor += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        cursor = start_date
        while cursor <= end_date:
            rows.append({"trade_date": cursor, "is_open": cursor.weekday() < 5})
            cursor += timedelta(days=1)
        return pd.DataFrame(rows)

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        _ = (symbol, as_of)
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 3, 31),
            "roe": 11.2,
            "revenue_yoy": 9.3,
            "net_profit_yoy": 10.7,
            "gross_margin": 28.6,
            "debt_to_asset": 47.1,
            "ocf_to_profit": 0.93,
            "eps": 0.75,
        }


def _setup_overrides(tmp_path: Path) -> None:
    provider = CompositeDataProvider([FakeStatsProvider()])
    service = AppliedStatisticsService(
        provider=provider,
        factor_engine=FactorEngine(),
        fundamental_service=FundamentalService(provider=provider),
    )
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_applied_statistics_service] = lambda: service
    app.dependency_overrides[get_audit_service] = lambda: audit


def test_applied_stats_descriptive_endpoint(tmp_path: Path) -> None:
    _setup_overrides(tmp_path)
    client = TestClient(app)
    try:
        resp = client.post(
            "/applied-stats/descriptive",
            json={
                "dataset_name": "exam_scores",
                "rows": [
                    {"x": 1.0, "y": 2.0},
                    {"x": 2.0, "y": 4.1},
                    {"x": 3.0, "y": 5.9},
                    {"x": 4.0, "y": 8.0},
                ],
                "columns": ["x", "y"],
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["dataset_name"] == "exam_scores"
        assert payload["row_count"] == 4
        assert "x" in payload["descriptive_statistics"]
    finally:
        app.dependency_overrides.clear()


def test_applied_stats_ols_endpoint(tmp_path: Path) -> None:
    _setup_overrides(tmp_path)
    client = TestClient(app)
    try:
        resp = client.post(
            "/applied-stats/model/ols",
            json={
                "target": [15, 18, 24, 29, 34, 37, 42, 48],
                "features": {
                    "x1": [1, 2, 3, 4, 5, 6, 7, 8],
                    "x2": [3, 3, 4, 4, 5, 5, 6, 6],
                },
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["n"] == 8
        assert payload["r2"] > 0
        assert len(payload["coefficients"]) == 3
    finally:
        app.dependency_overrides.clear()


def test_applied_stats_market_factor_case_endpoint(tmp_path: Path) -> None:
    _setup_overrides(tmp_path)
    client = TestClient(app)
    try:
        resp = client.post(
            "/applied-stats/cases/market-factor-study",
            json={
                "symbol": "000001",
                "start_date": "2025-01-01",
                "end_date": "2025-06-30",
                "include_fundamentals": True,
                "permutations": 250,
                "bootstrap_samples": 250,
                "random_seed": 11,
                "export_markdown": False,
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["symbol"] == "000001"
        assert payload["sample_size"] >= 24
        assert "ols" in payload
        assert "interpretation" in payload
    finally:
        app.dependency_overrides.clear()

