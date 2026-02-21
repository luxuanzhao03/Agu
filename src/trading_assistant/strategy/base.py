from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

import pandas as pd

from trading_assistant.core.models import SignalCandidate, StrategyInfo


@dataclass
class StrategyContext:
    params: dict[str, float | int | str | bool] = field(default_factory=dict)
    market_state: dict[str, str | float | int | bool] = field(default_factory=dict)


class BaseStrategy(ABC):
    info: StrategyInfo

    @abstractmethod
    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        """Generate candidate signals from factor-enriched bars."""

