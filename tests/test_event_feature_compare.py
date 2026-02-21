from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from trading_assistant.core.config import Settings
from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventFeatureBacktestCompareRequest,
    EventPolarity,
    EventRecordCreate,
    EventSourceRegisterRequest,
)
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.event_feature_compare import EventFeatureBacktestCompareService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.strategy.registry import StrategyRegistry


class FakeProvider:
    def __init__(self, bars: pd.DataFrame) -> None:
        self._bars = bars

    def get_daily_bars_with_source(self, symbol: str, start_date: date, end_date: date):
        _ = (symbol, start_date, end_date)
        return "fake_provider", self._bars.copy()

    def get_security_status(self, symbol: str):
        _ = symbol
        return {"is_st": False, "is_suspended": False}


def _bars(symbol: str, start: date, days: int) -> pd.DataFrame:
    rows = []
    for i in range(days):
        d = start + timedelta(days=i)
        close = 10.0 + i * 0.2
        rows.append(
            {
                "trade_date": d,
                "symbol": symbol,
                "open": close - 0.1,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": 1_000_000 + i * 5000,
                "amount": 12_000_000 + i * 20000,
            }
        )
    return pd.DataFrame(rows)


def test_event_feature_compare_generates_report(tmp_path: Path) -> None:
    symbol = "000001"
    start = date(2025, 1, 2)
    bars = _bars(symbol=symbol, start=start, days=60)

    event_service = EventService(store=EventStore(str(tmp_path / "event.db")))
    _ = event_service.register_source(
        EventSourceRegisterRequest(source_name="ann_feed", source_type="ANNOUNCEMENT", provider="mock", created_by="qa")
    )
    _ = event_service.ingest(
        EventBatchIngestRequest(
            source_name="ann_feed",
            events=[
                EventRecordCreate(
                    event_id="evt-1",
                    symbol=symbol,
                    event_type="earnings_preannounce_positive",
                    publish_time=datetime(2025, 1, 20, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.9,
                    confidence=0.9,
                    title="earnings beat",
                    summary="guidance up with profit growth",
                )
            ],
        )
    )

    service = EventFeatureBacktestCompareService(
        provider=FakeProvider(bars),
        factor_engine=FactorEngine(),
        pit=PITValidator(),
        event_service=event_service,
        registry=StrategyRegistry(),
        settings=Settings(),
        output_dir=str(tmp_path / "reports"),
    )

    req = EventFeatureBacktestCompareRequest(
        symbol=symbol,
        start_date=start,
        end_date=start + timedelta(days=59),
        strategy_name="event_driven",
        save_report=True,
    )
    result = service.compare(req)
    assert result.provider == "fake_provider"
    assert result.diagnostics.events_loaded >= 1
    assert "Event Feature Backtest Comparison" in result.report_content
    assert result.report_path is not None
