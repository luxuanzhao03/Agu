from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

import pandas as pd


class MarketDataProvider(ABC):
    name: str

    @abstractmethod
    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Return standardized bar DataFrame columns:
        trade_date, symbol, open, high, low, close, volume, amount, is_suspended, is_st
        """

    @abstractmethod
    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Return DataFrame columns: trade_date, is_open
        """

    @abstractmethod
    def get_security_status(self, symbol: str) -> dict[str, bool]:
        """
        Return dict with keys: is_st, is_suspended
        """

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        """
        Optional method.
        Return dict with normalized fields:
        report_date, publish_date, roe, revenue_yoy, net_profit_yoy, gross_margin,
        debt_to_asset, ocf_to_profit, eps.
        """
        raise NotImplementedError("fundamental snapshot is not implemented by this provider")

    def list_advanced_capabilities(self, user_points: int = 0) -> list[dict[str, Any]]:
        """
        Optional method.
        Return advanced dataset capability metadata.
        """
        _ = user_points
        return []

    def prefetch_advanced_datasets(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        user_points: int = 0,
        include_ineligible: bool = False,
    ) -> dict[str, Any]:
        """
        Optional method.
        Trigger advanced dataset prefetch/warm-up with per-dataset status.
        """
        _ = (symbol, start_date, end_date, user_points, include_ineligible)
        raise NotImplementedError("advanced dataset prefetch is not implemented by this provider")
