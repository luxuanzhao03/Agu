from __future__ import annotations

from fastapi import APIRouter, Depends

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_model_risk_service
from trading_assistant.core.models import ModelDriftRequest, ModelDriftResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.monitoring.model_risk import ModelRiskService

router = APIRouter(prefix="/model-risk", tags=["model-risk"])


@router.post("/drift-check", response_model=ModelDriftResult)
def drift_check(
    req: ModelDriftRequest,
    service: ModelRiskService = Depends(get_model_risk_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.AUDIT, UserRole.RESEARCH)),
) -> ModelDriftResult:
    result = service.detect_drift(req)
    audit.log(
        event_type="model_risk",
        action="drift_check",
        payload={
            "strategy": req.strategy_name,
            "status": result.status.value,
            "warning_count": len(result.warnings),
        },
    )
    return result

