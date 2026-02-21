from __future__ import annotations

from trading_assistant.core.models import (
    DataLicenseCheckRequest,
    DataLicenseCheckResult,
    DataLicenseRecord,
    DataLicenseRegisterRequest,
)
from trading_assistant.governance.license_store import DataLicenseStore


class DataLicenseService:
    def __init__(self, store: DataLicenseStore) -> None:
        self.store = store

    def register(self, req: DataLicenseRegisterRequest) -> int:
        return self.store.register(req)

    def list_licenses(
        self,
        dataset_name: str | None = None,
        provider: str | None = None,
        active_only: bool = False,
        limit: int = 200,
    ) -> list[DataLicenseRecord]:
        return self.store.list_licenses(
            dataset_name=dataset_name,
            provider=provider,
            active_only=active_only,
            limit=limit,
        )

    def check(self, req: DataLicenseCheckRequest) -> DataLicenseCheckResult:
        lic = self.store.latest_active(dataset_name=req.dataset_name, provider=req.provider, as_of=req.as_of)
        if lic is None:
            return DataLicenseCheckResult(
                allowed=False,
                reason="no_active_license",
                watermark="For Research Only",
                allow_export=False,
                matched_license_id=None,
                expires_on=None,
            )

        scopes = {s.strip().lower() for s in lic.usage_scopes if s.strip()}
        requested = req.requested_usage.strip().lower()
        if scopes and requested not in scopes:
            return DataLicenseCheckResult(
                allowed=False,
                reason=f"usage_scope_not_allowed:{requested}",
                watermark=lic.enforce_watermark,
                allow_export=lic.allow_export,
                max_export_rows=lic.max_export_rows,
                matched_license_id=lic.id,
                expires_on=lic.valid_to,
            )

        if req.export_requested and not lic.allow_export:
            return DataLicenseCheckResult(
                allowed=False,
                reason="export_not_allowed",
                watermark=lic.enforce_watermark,
                allow_export=lic.allow_export,
                max_export_rows=lic.max_export_rows,
                matched_license_id=lic.id,
                expires_on=lic.valid_to,
            )

        if req.export_requested and lic.max_export_rows is not None and req.expected_rows > lic.max_export_rows:
            return DataLicenseCheckResult(
                allowed=False,
                reason="export_rows_exceeded",
                watermark=lic.enforce_watermark,
                allow_export=lic.allow_export,
                max_export_rows=lic.max_export_rows,
                matched_license_id=lic.id,
                expires_on=lic.valid_to,
            )

        return DataLicenseCheckResult(
            allowed=True,
            reason="ok",
            watermark=lic.enforce_watermark,
            allow_export=lic.allow_export,
            max_export_rows=lic.max_export_rows,
            matched_license_id=lic.id,
            expires_on=lic.valid_to,
        )
