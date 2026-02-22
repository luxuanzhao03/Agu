from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_audit_service, get_holding_service
from trading_assistant.core.models import ManualHoldingAnalysisRequest, ManualHoldingTradeCreate
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.holdings.service import HoldingService
from trading_assistant.holdings.store import HoldingStore
from trading_assistant.main import app
from trading_assistant.strategy.registry import StrategyRegistry


class FakeProvider:
    def get_daily_bars_with_source(self, symbol: str, start_date: date, end_date: date):
        dates = pd.date_range(start=start_date, end=end_date, freq="B")
        base = 10.0 if symbol == "000001" else 8.0
        step = 0.03 if symbol == "000001" else 0.045
        rows = []
        price = base
        for idx, d in enumerate(dates):
            price = price + step
            volume = 100_000.0 + idx * 300
            rows.append(
                {
                    "trade_date": d.date(),
                    "symbol": symbol,
                    "open": price - 0.04,
                    "high": price + 0.08,
                    "low": price - 0.07,
                    "close": price,
                    "volume": volume,
                    "amount": volume * price,
                }
            )
        return "fake_provider", pd.DataFrame(rows)

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        dates = pd.date_range(start=start_date, end=end_date, freq="B")
        return pd.DataFrame({"trade_date": [d.date() for d in dates], "is_open": [True for _ in dates]})

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}


class DummyAutotune:
    def resolve_runtime_params(
        self,
        *,
        strategy_name: str,
        symbol: str,
        explicit_params: dict[str, float | int | str | bool],
        use_profile: bool,
    ) -> tuple[dict[str, float | int | str | bool], None]:
        _ = (strategy_name, symbol, use_profile)
        return dict(explicit_params or {}), None


def _build_service(tmp_path: Path) -> HoldingService:
    return HoldingService(
        store=HoldingStore(str(tmp_path / "holdings.db")),
        provider=FakeProvider(),  # type: ignore[arg-type]
        factor_engine=FactorEngine(),
        registry=StrategyRegistry(),
        autotune=DummyAutotune(),  # type: ignore[arg-type]
    )


def test_holding_service_positions_and_analysis(tmp_path: Path) -> None:
    service = _build_service(tmp_path)

    service.record_trade(
        req=ManualHoldingTradeCreate(
            trade_date=date(2025, 1, 2),
            symbol="000001",
            symbol_name="平安银行",
            side="BUY",
            price=10.0,
            lots=2,
            lot_size=100,
            fee=1.0,
            note="",
        )
    )
    service.record_trade(
        req=ManualHoldingTradeCreate(
            trade_date=date(2025, 1, 3),
            symbol="000001",
            symbol_name="平安银行",
            side="BUY",
            price=12.0,
            lots=1,
            lot_size=100,
            fee=1.0,
            note="",
        )
    )
    service.record_trade(
        req=ManualHoldingTradeCreate(
            trade_date=date(2025, 1, 6),
            symbol="000001",
            symbol_name="平安银行",
            side="SELL",
            price=11.6,
            lots=1,
            lot_size=100,
            fee=1.0,
            note="",
        )
    )

    positions = service.positions(as_of_date=date(2025, 1, 10))
    assert positions.summary.position_count == 1
    assert positions.summary.total_quantity == 200
    assert positions.positions[0].symbol == "000001"
    assert positions.positions[0].avg_cost > 10.0

    result = service.analyze(
        req=ManualHoldingAnalysisRequest(
            as_of_date=date(2025, 1, 10),
            strategy_name="trend_following",
            strategy_params={"entry_ma_fast": 10, "entry_ma_slow": 40, "atr_multiplier": 2.0},
            use_autotune_profile=False,
            available_cash=20_000,
            candidate_symbols=["600000", "000001"],
            max_new_buys=2,
            max_single_position_ratio=0.35,
            lot_size=100,
        )
    )
    assert result.summary.position_count == 1
    assert result.positions
    assert result.recommendations


def test_holdings_api_flow(tmp_path: Path) -> None:
    service = _build_service(tmp_path)
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_holding_service] = lambda: service
    app.dependency_overrides[get_audit_service] = lambda: audit
    client = TestClient(app)

    try:
        create_resp = client.post(
            "/holdings/trades",
            json={
                "trade_date": "2025-01-08",
                "symbol": "000001",
                "symbol_name": "平安银行",
                "side": "BUY",
                "price": 10.12,
                "lots": 2,
                "lot_size": 100,
                "fee": 1.2,
                "note": "manual trade",
            },
        )
        assert create_resp.status_code == 200
        trade_id = int(create_resp.json()["id"])

        list_resp = client.get("/holdings/trades?limit=200")
        assert list_resp.status_code == 200
        assert len(list_resp.json()) == 1

        positions_resp = client.get("/holdings/positions?as_of_date=2025-01-10")
        assert positions_resp.status_code == 200
        assert positions_resp.json()["summary"]["position_count"] == 1

        analyze_resp = client.post(
            "/holdings/analyze",
            json={
                "as_of_date": "2025-01-10",
                "strategy_name": "trend_following",
                "strategy_params": {"entry_ma_fast": 10, "entry_ma_slow": 40, "atr_multiplier": 2.0},
                "use_autotune_profile": False,
                "available_cash": 10000,
                "candidate_symbols": ["600000"],
                "max_new_buys": 1,
                "max_single_position_ratio": 0.4,
                "lot_size": 100,
            },
        )
        assert analyze_resp.status_code == 200
        payload = analyze_resp.json()
        assert payload["summary"]["position_count"] == 1
        assert len(payload["positions"]) >= 1
        assert len(payload["recommendations"]) >= 1

        delete_resp = client.delete(f"/holdings/trades/{trade_id}")
        assert delete_resp.status_code == 200
        assert delete_resp.json() is True
    finally:
        app.dependency_overrides.clear()
