from trading_assistant.core.config import Settings


def test_settings_strip_string_values_for_bool_fields() -> None:
    settings = Settings(
        ops_scheduler_enabled=" true ",
        auth_enabled=" false ",
        enforce_data_license=" true ",
    )
    assert settings.ops_scheduler_enabled is True
    assert settings.auth_enabled is False
    assert settings.enforce_data_license is True

