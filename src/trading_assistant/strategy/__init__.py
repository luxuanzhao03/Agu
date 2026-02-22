"""Strategy templates and registry."""

from trading_assistant.strategy.registry import StrategyRegistry
from trading_assistant.strategy.small_capital_adaptive import SmallCapitalAdaptiveStrategy

__all__ = ["StrategyRegistry", "SmallCapitalAdaptiveStrategy"]
