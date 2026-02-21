from datetime import date

import pandas as pd

from trading_assistant.governance.pit_validator import PITValidator


def test_pit_validator_detects_non_monotonic() -> None:
    bars = pd.DataFrame(
        [
            {"trade_date": date(2025, 1, 3), "open": 1, "high": 2, "low": 1, "close": 2},
            {"trade_date": date(2025, 1, 2), "open": 1, "high": 2, "low": 1, "close": 2},
        ]
    )
    result = PITValidator().validate_bars("000001", "ok", bars, as_of=date(2025, 1, 3))
    assert result.passed is False
    assert any(i.issue_type == "non_monotonic_trade_date" for i in result.issues)


def test_pit_validator_passes_clean_data() -> None:
    bars = pd.DataFrame(
        [
            {"trade_date": date(2025, 1, 2), "open": 1, "high": 2, "low": 1, "close": 2},
            {"trade_date": date(2025, 1, 3), "open": 1, "high": 2, "low": 1, "close": 2},
        ]
    )
    result = PITValidator().validate_bars("000001", "ok", bars, as_of=date(2025, 1, 3))
    assert result.passed is True
