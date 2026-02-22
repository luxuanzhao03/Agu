from __future__ import annotations

from trading_assistant.core.models import StrategyInfo
from trading_assistant.strategy.base import BaseStrategy
from trading_assistant.strategy.event_driven import EventDrivenStrategy
from trading_assistant.strategy.mean_reversion import MeanReversionStrategy
from trading_assistant.strategy.multi_factor import MultiFactorStrategy
from trading_assistant.strategy.sector_rotation import SectorRotationStrategy
from trading_assistant.strategy.small_capital_adaptive import SmallCapitalAdaptiveStrategy
from trading_assistant.strategy.trend import TrendFollowingStrategy


class StrategyRegistry:
    def __init__(self) -> None:
        strategies: list[BaseStrategy] = [
            TrendFollowingStrategy(),
            MeanReversionStrategy(),
            MultiFactorStrategy(),
            SectorRotationStrategy(),
            EventDrivenStrategy(),
            SmallCapitalAdaptiveStrategy(),
        ]
        self._mapping = {strategy.info.name: strategy for strategy in strategies}

    def get(self, name: str) -> BaseStrategy:
        key = name.strip().lower()
        if key not in self._mapping:
            available = ", ".join(self._mapping.keys())
            raise KeyError(f"Strategy '{name}' not found. Available: {available}")
        return self._mapping[key]

    def list_info(self) -> list[StrategyInfo]:
        return [strategy.info for strategy in self._mapping.values()]
