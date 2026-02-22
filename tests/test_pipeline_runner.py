from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from trading_assistant.core.models import PipelineRunRequest
from trading_assistant.core.models import EventBatchIngestRequest, EventPolarity, EventRecordCreate, EventSourceRegisterRequest
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.license_store import DataLicenseStore
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.governance.snapshot_store import DataSnapshotStore
from trading_assistant.pipeline.runner import DailyPipelineRunner
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.signal.service import SignalService
from trading_assistant.strategy.registry import StrategyRegistry


class OkProvider(MarketDataProvider):
    name = "ok"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": start_date,
                    "symbol": symbol,
                    "open": 10.0,
                    "high": 10.2,
                    "low": 9.8,
                    "close": 10.1,
                    "volume": 100000,
                    "amount": 1010000,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame([{"trade_date": start_date, "is_open": True}])

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        return {"is_st": False, "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 1, 1),
            "roe": 11.0,
            "revenue_yoy": 12.0,
            "net_profit_yoy": 16.0,
            "gross_margin": 32.0,
            "debt_to_asset": 45.0,
            "ocf_to_profit": 1.0,
            "eps": 0.7,
        }


class ExpensiveProvider(OkProvider):
    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "trade_date": start_date,
                    "symbol": symbol,
                    "open": 40.0,
                    "high": 40.2,
                    "low": 39.8,
                    "close": 40.1,
                    "volume": 100000,
                    "amount": 4010000,
                    "is_suspended": False,
                    "is_st": False,
                }
            ]
        )


def test_pipeline_runs_for_symbols(tmp_path: Path) -> None:
    provider = CompositeDataProvider([OkProvider()])
    runner = DailyPipelineRunner(
        provider=provider,
        factor_engine=FactorEngine(),
        registry=StrategyRegistry(),
        risk_engine=RiskEngine(
            max_single_position=0.05,
            max_drawdown=0.12,
            max_industry_exposure=0.2,
            min_turnover_20d=1000,
        ),
        signal_service=SignalService(),
        quality_service=DataQualityService(),
        pit_validator=PITValidator(),
        snapshot_service=DataSnapshotService(DataSnapshotStore(str(tmp_path / "snapshot.db"))),
    )
    req = PipelineRunRequest(
        symbols=["000001", "000002"],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 2),
        strategy_name="trend_following",
    )
    result = runner.run(req)
    assert len(result.results) == 2
    assert all(item.snapshot_id is not None for item in result.results)
    assert all(item.fundamental_available for item in result.results)
    assert all(item.fundamental_source == "ok" for item in result.results)


def test_pipeline_blocks_when_license_enforced(tmp_path: Path) -> None:
    provider = CompositeDataProvider([OkProvider()])
    runner = DailyPipelineRunner(
        provider=provider,
        factor_engine=FactorEngine(),
        registry=StrategyRegistry(),
        risk_engine=RiskEngine(
            max_single_position=0.05,
            max_drawdown=0.12,
            max_industry_exposure=0.2,
            min_turnover_20d=1000,
        ),
        signal_service=SignalService(),
        quality_service=DataQualityService(),
        pit_validator=PITValidator(),
        snapshot_service=DataSnapshotService(DataSnapshotStore(str(tmp_path / "snapshot.db"))),
        license_service=DataLicenseService(DataLicenseStore(str(tmp_path / "license.db"))),
        enforce_data_license=True,
    )
    req = PipelineRunRequest(
        symbols=["000001"],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 2),
        strategy_name="trend_following",
    )
    result = runner.run(req)
    assert len(result.results) == 1
    assert result.results[0].quality_passed is False
    assert result.results[0].snapshot_id is None


def test_pipeline_event_driven_auto_event_enrichment(tmp_path: Path) -> None:
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

    provider = CompositeDataProvider([OkProvider()])
    runner = DailyPipelineRunner(
        provider=provider,
        factor_engine=FactorEngine(),
        registry=StrategyRegistry(),
        risk_engine=RiskEngine(
            max_single_position=0.05,
            max_drawdown=0.12,
            max_industry_exposure=0.2,
            min_turnover_20d=1000,
        ),
        signal_service=SignalService(),
        quality_service=DataQualityService(),
        pit_validator=PITValidator(),
        snapshot_service=DataSnapshotService(DataSnapshotStore(str(tmp_path / "snapshot.db"))),
        event_service=event_service,
    )
    req = PipelineRunRequest(
        symbols=["000001"],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 2),
        strategy_name="event_driven",
    )
    result = runner.run(req)
    assert len(result.results) == 1
    assert result.results[0].event_rows_used >= 1


def test_pipeline_small_capital_mode_flags_blocked_symbol(tmp_path: Path) -> None:
    provider = CompositeDataProvider([ExpensiveProvider()])
    runner = DailyPipelineRunner(
        provider=provider,
        factor_engine=FactorEngine(),
        registry=StrategyRegistry(),
        risk_engine=RiskEngine(
            max_single_position=0.05,
            max_drawdown=0.12,
            max_industry_exposure=0.2,
            min_turnover_20d=1000,
        ),
        signal_service=SignalService(),
        quality_service=DataQualityService(),
        pit_validator=PITValidator(),
        snapshot_service=DataSnapshotService(DataSnapshotStore(str(tmp_path / "snapshot.db"))),
        small_capital_mode_enabled=True,
        small_capital_principal_cny=2000,
    )
    req = PipelineRunRequest(
        symbols=["000001"],
        start_date=date(2025, 1, 2),
        end_date=date(2025, 1, 2),
        strategy_name="multi_factor",
        enable_small_capital_mode=True,
        small_capital_principal=2000,
    )
    result = runner.run(req)
    assert len(result.results) == 1
    assert result.results[0].small_capital_note is not None
