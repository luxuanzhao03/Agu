from datetime import datetime, timezone

from trading_assistant.core.models import EventPITRow, EventPITValidationRequest
from trading_assistant.governance.pit_validator import PITValidator


def test_event_pit_validation_detects_issue() -> None:
    req = EventPITValidationRequest(
        symbol="000001",
        rows=[
            EventPITRow(
                event_id="e1",
                event_time=datetime(2025, 1, 2, 10, 0, tzinfo=timezone.utc),
                effective_time=datetime(2025, 1, 2, 9, 50, tzinfo=timezone.utc),
                used_in_trade_time=datetime(2025, 1, 2, 10, 5, tzinfo=timezone.utc),
            )
        ],
    )
    result = PITValidator().validate_event_rows(req)
    assert result.passed is False
    assert any(i.issue_type == "effective_before_event" for i in result.issues)
