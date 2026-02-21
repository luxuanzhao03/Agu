from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import get_audit_service, get_data_license_service, get_reporting_service
from trading_assistant.core.models import DataLicenseCheckRequest, ReportGenerateRequest, ReportGenerateResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.reporting.service import ReportingService

router = APIRouter(prefix="/reports", tags=["reports"])


@router.post("/generate", response_model=ReportGenerateResult)
def generate_report(
    req: ReportGenerateRequest,
    reporting: ReportingService = Depends(get_reporting_service),
    license_service: DataLicenseService = Depends(get_data_license_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH, UserRole.ADMIN)),
) -> ReportGenerateResult:
    license_check = license_service.check(
        DataLicenseCheckRequest(
            dataset_name="research_report",
            provider="internal",
            requested_usage="internal_research",
            export_requested=req.save_to_file,
            expected_rows=req.limit,
            as_of=req.end_date,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_report_export",
            payload={"report_type": req.report_type, "reason": license_check.reason},
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    if req.save_to_file and license_check.watermark and req.watermark == "For Research Only":
        req = req.model_copy(update={"watermark": license_check.watermark})

    result = reporting.generate(req)
    audit.log(
        event_type="reporting",
        action="generate",
        payload={
            "report_type": req.report_type,
            "saved_path": result.saved_path or "",
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return result
