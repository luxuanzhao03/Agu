from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_data_license_service
from trading_assistant.core.models import (
    DataLicenseCheckRequest,
    DataLicenseCheckResult,
    DataLicenseRecord,
    DataLicenseRegisterRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.governance.license_service import DataLicenseService

router = APIRouter(prefix="/data/licenses", tags=["data-license"])


@router.post("/register", response_model=int)
def register_data_license(
    req: DataLicenseRegisterRequest,
    service: DataLicenseService = Depends(get_data_license_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> int:
    row_id = service.register(req)
    audit.log(
        event_type="data_license",
        action="register",
        payload={
            "license_id": row_id,
            "dataset_name": req.dataset_name,
            "provider": req.provider,
            "allow_export": req.allow_export,
        },
    )
    return row_id


@router.get("", response_model=list[DataLicenseRecord])
def list_data_licenses(
    dataset_name: str | None = Query(default=None),
    provider: str | None = Query(default=None),
    active_only: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
    service: DataLicenseService = Depends(get_data_license_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH, UserRole.ADMIN)),
) -> list[DataLicenseRecord]:
    return service.list_licenses(
        dataset_name=dataset_name,
        provider=provider,
        active_only=active_only,
        limit=limit,
    )


@router.post("/check", response_model=DataLicenseCheckResult)
def check_data_license(
    req: DataLicenseCheckRequest,
    service: DataLicenseService = Depends(get_data_license_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH, UserRole.ADMIN)),
) -> DataLicenseCheckResult:
    result = service.check(req)
    audit.log(
        event_type="data_license",
        action="check",
        payload={
            "dataset_name": req.dataset_name,
            "provider": req.provider,
            "allowed": result.allowed,
            "reason": result.reason,
            "license_id": result.matched_license_id or -1,
        },
        status="OK" if result.allowed else "ERROR",
    )
    return result
