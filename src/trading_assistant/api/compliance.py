from __future__ import annotations

from datetime import datetime, time, timezone

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import (
    get_audit_service,
    get_compliance_evidence_service,
    get_data_provider,
    get_event_service,
    get_data_quality_service,
    get_pit_validator,
    get_strategy_governance_service,
    get_strategy_registry,
)
from trading_assistant.core.models import (
    ComplianceCheckItem,
    ComplianceEvidenceCounterSignRequest,
    ComplianceEvidenceCounterSignResult,
    ComplianceEvidenceExportRequest,
    ComplianceEvidenceExportResult,
    ComplianceEvidenceVerifyRequest,
    ComplianceEvidenceVerifyResult,
    CompliancePreflightRequest,
    CompliancePreflightResult,
    DataQualityRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.compliance_evidence import ComplianceEvidenceService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.registry import StrategyRegistry

router = APIRouter(prefix="/compliance", tags=["compliance"])


@router.post("/preflight", response_model=CompliancePreflightResult)
def compliance_preflight(
    req: CompliancePreflightRequest,
    provider: CompositeDataProvider = Depends(get_data_provider),
    events: EventService = Depends(get_event_service),
    quality: DataQualityService = Depends(get_data_quality_service),
    pit: PITValidator = Depends(get_pit_validator),
    registry: StrategyRegistry = Depends(get_strategy_registry),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.AUDIT, UserRole.RESEARCH)),
) -> CompliancePreflightResult:
    checks: list[ComplianceCheckItem] = []
    try:
        _ = registry.get(req.strategy_name)
        checks.append(ComplianceCheckItem(check_name="strategy_exists", passed=True, message="Strategy exists."))
    except KeyError:
        checks.append(ComplianceCheckItem(check_name="strategy_exists", passed=False, message="Strategy not found."))
        result = CompliancePreflightResult(passed=False, checks=checks)
        audit.log("compliance", "preflight", {"symbol": req.symbol, "passed": False}, status="ERROR")
        return result

    if settings.enforce_approved_strategy:
        approved = strategy_gov.is_approved(req.strategy_name)
        checks.append(
            ComplianceCheckItem(
                check_name="strategy_approved",
                passed=approved,
                message="Approved strategy found." if approved else "No approved version found.",
            )
        )

    try:
        used_provider, bars = provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    quality_result = quality.evaluate(
        req=DataQualityRequest(symbol=req.symbol, start_date=req.start_date, end_date=req.end_date),
        bars=bars,
        provider=used_provider,
    )
    checks.append(
        ComplianceCheckItem(
            check_name="data_quality",
            passed=quality_result.passed,
            message=f"Data quality issues: {len(quality_result.issues)}",
        )
    )

    pit_result = pit.validate_bars(symbol=req.symbol, provider=used_provider, bars=bars, as_of=req.end_date)
    checks.append(
        ComplianceCheckItem(
            check_name="pit_validation",
            passed=pit_result.passed,
            message=f"PIT issues: {len(pit_result.issues)}",
        )
    )

    if req.strategy_name == "event_driven":
        event_rows = events.list_events(
            symbol=req.symbol,
            start_time=datetime.combine(req.start_date, time.min, tzinfo=timezone.utc),
            end_time=datetime.combine(req.end_date, time.max, tzinfo=timezone.utc),
            limit=2000,
        )
        checks.append(
            ComplianceCheckItem(
                check_name="event_data_ready",
                passed=len(event_rows) > 0,
                message=f"Event rows found: {len(event_rows)}",
            )
        )

    passed = all(c.passed for c in checks)
    audit.log(
        event_type="compliance",
        action="preflight",
        payload={"symbol": req.symbol, "strategy": req.strategy_name, "passed": passed, "checks": len(checks)},
    )
    return CompliancePreflightResult(passed=passed, checks=checks)


@router.post("/evidence/export", response_model=ComplianceEvidenceExportResult)
def compliance_evidence_export(
    req: ComplianceEvidenceExportRequest,
    service: ComplianceEvidenceService = Depends(get_compliance_evidence_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.ADMIN, UserRole.RISK)),
) -> ComplianceEvidenceExportResult:
    result = service.export_bundle(req)
    audit.log(
        event_type="compliance",
        action="evidence_export",
        payload={
            "bundle_id": result.bundle_id,
            "file_count": result.file_count,
            "package_path": result.package_path,
            "package_sha256": result.package_sha256,
            "triggered_by": req.triggered_by,
            "strategy_name": req.strategy_name or "",
            "connector_name": req.connector_name or "",
            "source_name": req.source_name or "",
        },
    )
    return result


@router.post("/evidence/verify", response_model=ComplianceEvidenceVerifyResult)
def compliance_evidence_verify(
    req: ComplianceEvidenceVerifyRequest,
    service: ComplianceEvidenceService = Depends(get_compliance_evidence_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.ADMIN, UserRole.RISK)),
) -> ComplianceEvidenceVerifyResult:
    result = service.verify_package(req)
    audit.log(
        event_type="compliance",
        action="evidence_verify",
        status="OK" if result.manifest_valid and (not result.signature_checked or result.signature_valid) else "ERROR",
        payload={
            "package_path": req.package_path,
            "package_exists": result.package_exists,
            "manifest_valid": result.manifest_valid,
            "signature_checked": result.signature_checked,
            "signature_valid": result.signature_valid,
            "message": result.message,
        },
    )
    return result


@router.post("/evidence/countersign", response_model=ComplianceEvidenceCounterSignResult)
def compliance_evidence_countersign(
    req: ComplianceEvidenceCounterSignRequest,
    service: ComplianceEvidenceService = Depends(get_compliance_evidence_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.ADMIN, UserRole.RISK)),
) -> ComplianceEvidenceCounterSignResult:
    try:
        result = service.countersign_package(req)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="compliance",
        action="evidence_countersign",
        payload={
            "package_path": req.package_path,
            "countersign_path": result.countersign_path,
            "entry_count": result.entry_count,
            "signer": req.signer,
            "signing_key_id": req.signing_key_id,
        },
    )
    return result
