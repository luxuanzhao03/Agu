from datetime import date
from pathlib import Path

import pandas as pd

from trading_assistant.core.models import ResearchWorkflowRequest
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.license_store import DataLicenseStore
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.registry import StrategyRegistry
from trading_assistant.workflow.research import ResearchWorkflowService


class GrowingProvider(MarketDataProvider):
    name = "ok"

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        rows = []
        prices = [10, 10.2, 10.5, 10.8, 11.0]
        for i, p in enumerate(prices):
            d = start_date.replace(day=min(28, start_date.day + i))
            rows.append(
                {
                    "trade_date": d,
                    "symbol": symbol,
                    "open": p * 0.99,
                    "high": p * 1.01,
                    "low": p * 0.98,
                    "close": p,
                    "volume": 100000 + i * 1000,
                    "amount": p * (100000 + i * 1000),
                    "is_suspended": False,
                    "is_st": False,
                }
            )
        return pd.DataFrame(rows)

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        return pd.DataFrame([{"trade_date": start_date, "is_open": True}])

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        return {"is_st": False, "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        return {
            "report_date": date(2024, 12, 31),
            "publish_date": date(2025, 1, 1),
            "roe": 13.0,
            "revenue_yoy": 10.0,
            "net_profit_yoy": 14.0,
            "gross_margin": 31.0,
            "debt_to_asset": 43.0,
            "ocf_to_profit": 0.95,
            "eps": 0.88,
        }


def test_research_workflow_runs(tmp_path: Path) -> None:
    workflow = ResearchWorkflowService(
        provider=CompositeDataProvider([GrowingProvider()]),
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
    )
    result = workflow.run(
        ResearchWorkflowRequest(
            symbols=["000001", "000002"],
            start_date=date(2025, 1, 2),
            end_date=date(2025, 1, 10),
            strategy_name="trend_following",
            optimize_portfolio=True,
        )
    )
    assert len(result.signals) >= 1
    assert any(item.fundamental_available for item in result.signals)


def test_research_workflow_blocks_when_license_enforced(tmp_path: Path) -> None:
    workflow = ResearchWorkflowService(
        provider=CompositeDataProvider([GrowingProvider()]),
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
        license_service=DataLicenseService(DataLicenseStore(str(tmp_path / "license.db"))),
        enforce_data_license=True,
    )
    result = workflow.run(
        ResearchWorkflowRequest(
            symbols=["000001"],
            start_date=date(2025, 1, 2),
            end_date=date(2025, 1, 10),
            strategy_name="trend_following",
            optimize_portfolio=True,
        )
    )
    assert result.signals == []
