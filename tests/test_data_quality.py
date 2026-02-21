from datetime import date

import pandas as pd

from trading_assistant.core.models import DataQualityRequest
from trading_assistant.governance.data_quality import DataQualityService


def test_data_quality_detects_critical_issue() -> None:
    service = DataQualityService()
    req = DataQualityRequest(symbol="000001", start_date=date(2025, 1, 1), end_date=date(2025, 1, 2))
    bars = pd.DataFrame(
        [
            {
                "trade_date": date(2025, 1, 1),
                "open": 10,
                "high": 9,
                "low": 10,
                "close": 10,
                "volume": 100,
                "amount": 1000,
            }
        ]
    )
    report = service.evaluate(req=req, bars=bars, provider="ok")
    assert report.passed is False
    assert any(i.issue_type == "invalid_high_low" for i in report.issues)
