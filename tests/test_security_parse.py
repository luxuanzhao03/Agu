from trading_assistant.core.security import UserRole, _parse_api_keys


def test_parse_api_keys() -> None:
    mapping = _parse_api_keys("k1:research,k2:risk,k3:admin")
    assert mapping["k1"] == UserRole.RESEARCH
    assert mapping["k2"] == UserRole.RISK
    assert mapping["k3"] == UserRole.ADMIN
