from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import (
    HoldingRecommendationAction,
    ManualHoldingAnalysisResult,
    ManualHoldingPortfolioSummary,
    ManualHoldingRecommendationItem,
)
from trading_assistant.holdings.store import HoldingStore
from trading_assistant.reporting.service import ReportingService
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore


class FakeProvider:
    def get_daily_bars_with_source(self, symbol: str, start_date: date, end_date: date):
        days = pd.date_range(start=start_date, end=end_date, freq="B")
        rows: list[dict[str, object]] = []
        close = 10.0
        for idx, dt in enumerate(days):
            close *= 1.001 + ((idx % 4) - 1.5) * 0.0004
            rows.append(
                {
                    "trade_date": dt.date(),
                    "symbol": symbol,
                    "open": close * 0.998,
                    "high": close * 1.004,
                    "low": close * 0.996,
                    "close": close,
                    "volume": 150_000.0,
                    "amount": close * 150_000.0,
                }
            )
        return "fake_provider", pd.DataFrame(rows)


def test_go_live_readiness_report_runs(tmp_path: Path) -> None:
    holding_store = HoldingStore(str(tmp_path / "holdings.db"))
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))

    as_of = date(2025, 1, 10)
    for i in range(25):
        day = as_of - timedelta(days=24 - i)
        analysis = ManualHoldingAnalysisResult(
            generated_at=datetime(day.year, day.month, day.day, tzinfo=timezone.utc),
            as_of_date=day,
            next_trade_date=day + timedelta(days=1),
            strategy_name="trend_following",
            provider="fake_provider",
            market_overview="ok",
            summary=ManualHoldingPortfolioSummary(as_of_date=day),
            recommendations=[
                ManualHoldingRecommendationItem(
                    symbol="000001",
                    symbol_name="平安银行",
                    action=HoldingRecommendationAction.ADD,
                    target_lots=1,
                    delta_lots=1,
                    confidence=0.66,
                    expected_next_day_return=0.006,
                    up_probability=0.62,
                    next_trade_date=day + timedelta(days=1),
                )
            ],
        )
        _ = holding_store.save_analysis_snapshot(analysis)

    audit.log(
        "strategy_challenge",
        "run",
        {
            "run_id": "run-1",
            "symbol": "000001",
            "run_status": "SUCCESS",
            "champion_strategy": "trend_following",
        },
    )

    service = ReportingService(
        replay=replay,
        audit=audit,
        output_dir=str(tmp_path / "reports"),
        provider=FakeProvider(),  # type: ignore[arg-type]
        holding_store=holding_store,
    )
    report = service.go_live_readiness(
        lookback_days=90,
        end_date=as_of,
        strategy_name="trend_following",
        symbol="000001",
    )

    assert report.lookback_days == 90
    assert report.gate_checks
    assert report.rollback_rules
    assert report.daily_checklist
    assert report.latest_accuracy is not None
