from trading_assistant.strategy.registry import StrategyRegistry


def test_registry_contains_required_strategies() -> None:
    registry = StrategyRegistry()
    names = {s.name for s in registry.list_info()}
    assert {
        "trend_following",
        "mean_reversion",
        "multi_factor",
        "sector_rotation",
        "event_driven",
        "small_capital_adaptive",
    } <= names


def test_registry_get_strategy() -> None:
    registry = StrategyRegistry()
    strategy = registry.get("trend_following")
    assert strategy.info.name == "trend_following"
