from fastapi import APIRouter, Depends

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_risk_engine
from trading_assistant.core.models import RiskCheckRequest, RiskCheckResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.risk.engine import RiskEngine

router = APIRouter(prefix="/risk", tags=["risk"])


@router.post("/check", response_model=RiskCheckResult)
def check_risk(
    req: RiskCheckRequest,
    risk_engine: RiskEngine = Depends(get_risk_engine),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.RESEARCH)),
) -> RiskCheckResult:
    result = risk_engine.evaluate(req)
    audit.log(
        event_type="risk_check",
        action="evaluate",
        payload={"symbol": req.signal.symbol, "action": req.signal.action.value, "blocked": result.blocked},
    )
    return result
