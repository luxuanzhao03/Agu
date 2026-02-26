from trading_assistant.strategy.registry import StrategyRegistry


def test_registry_contains_required_strategies() -> None:
    registry = StrategyRegistry()
    names = {s.name for s in registry.list_info()}
    assert {
        "trend_following",
        "trend_pullback",
        "mean_reversion",
        "multi_factor",
        "sector_rotation",
        "event_driven",
    } <= names
    assert len(names) == 6


def test_registry_get_strategy() -> None:
    registry = StrategyRegistry()
    strategy = registry.get("trend_following")
    assert strategy.info.name == "trend_following"
