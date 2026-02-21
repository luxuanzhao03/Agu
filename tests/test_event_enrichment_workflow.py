from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventPolarity,
    EventRecordCreate,
    EventSourceRegisterRequest,
    ResearchWorkflowRequest,
    SignalAction,
)
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.registry import StrategyRegistry
from trading_assistant.workflow.research import ResearchWorkflowService


class FlatProvider(MarketDataProvider):
    name = "flat"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": start_date,
                    "symbol": symbol,
                    "open": 10.0,
                    "high": 10.1,
                    "low": 9.9,
                    "close": 10.0,
                    "volume": 100000,
                    "amount": 1000000,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame([{"trade_date": start_date, "is_open": True}])

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        return {"is_st": False, "is_suspended": False}


def test_event_driven_research_auto_enrichment(tmp_path: Path) -> None:
    event_service = EventService(EventStore(str(tmp_path / "event.db")))
    _ = event_service.register_source(
        EventSourceRegisterRequest(
            source_name="event_feed",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = event_service.ingest(
        EventBatchIngestRequest(
            source_name="event_feed",
            events=[
                EventRecordCreate(
                    event_id="evt-1",
                    symbol="000001",
                    event_type="earnings_preannounce",
                    publish_time=datetime(2025, 1, 2, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=1.0,
                    confidence=1.0,
                )
            ],
        )
    )

    workflow = ResearchWorkflowService(
        provider=CompositeDataProvider([FlatProvider()]),
        factor_engine=FactorEngine(),
        registry=StrategyRegistry(),
        risk_engine=RiskEngine(
            max_single_position=0.05,
            max_drawdown=0.12,
            max_industry_exposure=0.2,
            min_turnover_20d=1000,
        ),
        optimizer=PortfolioOptimizer(),
        replay=ReplayService(ReplayStore(str(tmp_path / "replay.db"))),
        pit_validator=PITValidator(),
        event_service=event_service,
    )
    result = workflow.run(
        ResearchWorkflowRequest(
            symbols=["000001"],
            start_date=date(2025, 1, 2),
            end_date=date(2025, 1, 2),
            strategy_name="event_driven",
            optimize_portfolio=False,
        )
    )
    assert len(result.signals) == 1
    assert result.signals[0].action == SignalAction.BUY
    assert result.signals[0].event_rows_used >= 1
