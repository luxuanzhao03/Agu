from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import (
    HoldingRecommendationAction,
    ManualHoldingAnalysisResult,
    ManualHoldingPortfolioSummary,
    ManualHoldingRecommendationItem,
    ManualHoldingSide,
    ManualHoldingTradeCreate,
)
from trading_assistant.holdings.store import HoldingStore
from trading_assistant.reporting.service import ReportingService
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore


class FakeProvider:
    def get_daily_bars_with_source(self, symbol: str, start_date: date, end_date: date):
        dates = pd.date_range(start=start_date, end=end_date, freq="B")
        rows: list[dict[str, object]] = []
        close = 10.0
        for dt in dates:
            close *= 1.003
            rows.append(
                {
                    "trade_date": dt.date(),
                    "symbol": symbol,
                    "open": close * 0.998,
                    "high": close * 1.005,
                    "low": close * 0.995,
                    "close": close,
                    "volume": 200_000.0,
                    "amount": close * 200_000.0,
                }
            )
        return "fake_provider", pd.DataFrame(rows)


def test_strategy_accuracy_report_computes_metrics(tmp_path: Path) -> None:
    store = HoldingStore(str(tmp_path / "holdings.db"))
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))

    analysis = ManualHoldingAnalysisResult(
        generated_at=datetime(2025, 1, 8, 15, 1, tzinfo=timezone.utc),
        as_of_date=date(2025, 1, 8),
        next_trade_date=date(2025, 1, 9),
        strategy_name="trend_following",
        provider="fake_provider",
        market_overview="test",
        summary=ManualHoldingPortfolioSummary(as_of_date=date(2025, 1, 8)),
        recommendations=[
            ManualHoldingRecommendationItem(
                symbol="000001",
                symbol_name="平安银行",
                action=HoldingRecommendationAction.ADD,
                target_lots=2,
                delta_lots=1,
                confidence=0.72,
                expected_next_day_return=0.011,
                up_probability=0.68,
                next_trade_date=date(2025, 1, 9),
            )
        ],
    )
    run_id = store.save_analysis_snapshot(analysis)

    _ = store.insert_trade(
        ManualHoldingTradeCreate(
            trade_date=date(2025, 1, 9),
            symbol="000001",
            symbol_name="平安银行",
            side=ManualHoldingSide.BUY,
            price=10.25,
            lots=1,
            lot_size=100,
            fee=1.2,
            reference_price=10.20,
            executed_at=datetime(2025, 1, 9, 10, 10, tzinfo=timezone.utc),
            is_partial_fill=False,
            unfilled_reason="",
            note="manual",
        )
    )

    service = ReportingService(
        replay=replay,
        audit=audit,
        output_dir=str(tmp_path / "reports"),
        provider=FakeProvider(),  # type: ignore[arg-type]
        holding_store=store,
    )
    report = service.strategy_accuracy(lookback_days=30, end_date=date(2025, 1, 20))

    assert report.sample_size == 1
    assert report.actionable_samples == 1
    assert report.executed_samples == 1
    assert report.by_strategy
    assert report.by_symbol
    assert report.by_strategy[0].bucket_key == "trend_following"
    assert report.details
    assert report.details[0].run_id == run_id
    assert report.details[0].executed is True
    assert report.details[0].execution_cost_bps is not None
