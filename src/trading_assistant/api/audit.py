from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import get_audit_service, get_data_license_service
from trading_assistant.core.models import (
    AuditChainVerifyResult,
    AuditEventRecord,
    AuditExportResult,
    DataLicenseCheckRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.governance.license_service import DataLicenseService

router = APIRouter(prefix="/audit", tags=["audit"])


@router.get("/events", response_model=list[AuditEventRecord])
def list_audit_events(
    event_type: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.ADMIN)),
) -> list[AuditEventRecord]:
    return audit.query(event_type=event_type, limit=limit)


@router.get("/export", response_model=AuditExportResult)
def export_audit_events(
    format: str = Query(default="csv", pattern="^(csv|jsonl)$"),
    event_type: str | None = Query(default=None),
    limit: int = Query(default=1000, ge=1, le=5000),
    audit: AuditService = Depends(get_audit_service),
    license_service: DataLicenseService = Depends(get_data_license_service),
    settings: Settings = Depends(get_settings),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.ADMIN)),
) -> AuditExportResult:
    license_check = license_service.check(
        DataLicenseCheckRequest(
            dataset_name="audit_events",
            provider="internal",
            requested_usage="internal_research",
            export_requested=True,
            expected_rows=limit,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_audit_export",
            payload={"reason": license_check.reason, "format": format, "event_type": event_type or ""},
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    if format == "csv":
        content = audit.export_csv(event_type=event_type, limit=limit)
        content = f"# watermark: {license_check.watermark}\n" + content
    else:
        content = audit.export_jsonl(event_type=event_type, limit=limit)
        header = json.dumps({"watermark": license_check.watermark}, ensure_ascii=False)
        content = f"{header}\n{content}" if content else header
    row_count = len(audit.query(event_type=event_type, limit=limit))
    audit.log(
        event_type="audit",
        action="export",
        payload={
            "format": format,
            "row_count": row_count,
            "event_type": event_type or "",
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return AuditExportResult(format=format, row_count=row_count, content=content)


@router.get("/verify-chain", response_model=AuditChainVerifyResult)
def verify_audit_chain(
    limit: int = Query(default=5000, ge=1, le=50000),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.ADMIN)),
) -> AuditChainVerifyResult:
    return audit.verify_chain(limit=limit)
