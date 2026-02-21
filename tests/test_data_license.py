from datetime import date
from pathlib import Path

from trading_assistant.core.models import DataLicenseCheckRequest, DataLicenseRegisterRequest
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.license_store import DataLicenseStore


def _service(tmp_path: Path) -> DataLicenseService:
    return DataLicenseService(DataLicenseStore(str(tmp_path / "license.db")))


def test_data_license_check_ok_and_export_block(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.register(
        DataLicenseRegisterRequest(
            dataset_name="daily_bars",
            provider="akshare",
            licensor="akshare-community",
            usage_scopes=["internal_research"],
            allow_export=False,
            enforce_watermark="Internal Research",
            valid_from=date(2025, 1, 1),
            valid_to=date(2026, 12, 31),
            created_by="tester",
        )
    )

    ok = service.check(
        DataLicenseCheckRequest(
            dataset_name="daily_bars",
            provider="akshare",
            requested_usage="internal_research",
            export_requested=False,
            expected_rows=100,
            as_of=date(2025, 2, 1),
        )
    )
    assert ok.allowed is True
    assert ok.reason == "ok"
    assert ok.watermark == "Internal Research"

    blocked = service.check(
        DataLicenseCheckRequest(
            dataset_name="daily_bars",
            provider="akshare",
            requested_usage="internal_research",
            export_requested=True,
            expected_rows=100,
            as_of=date(2025, 2, 1),
        )
    )
    assert blocked.allowed is False
    assert blocked.reason == "export_not_allowed"


def test_data_license_scope_and_rows_limit(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.register(
        DataLicenseRegisterRequest(
            dataset_name="audit_events",
            provider="internal",
            licensor="internal",
            usage_scopes=["audit"],
            allow_export=True,
            max_export_rows=50,
            valid_from=date(2025, 1, 1),
            created_by="tester",
        )
    )

    wrong_scope = service.check(
        DataLicenseCheckRequest(
            dataset_name="audit_events",
            provider="internal",
            requested_usage="internal_research",
            export_requested=False,
            as_of=date(2025, 3, 1),
        )
    )
    assert wrong_scope.allowed is False
    assert wrong_scope.reason.startswith("usage_scope_not_allowed")

    exceeded = service.check(
        DataLicenseCheckRequest(
            dataset_name="audit_events",
            provider="internal",
            requested_usage="audit",
            export_requested=True,
            expected_rows=120,
            as_of=date(2025, 3, 1),
        )
    )
    assert exceeded.allowed is False
    assert exceeded.reason == "export_rows_exceeded"
