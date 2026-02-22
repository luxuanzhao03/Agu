from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import (
    get_audit_service,
    get_research_workflow_service,
    get_strategy_governance_service,
)
from trading_assistant.core.models import ResearchWorkflowRequest, ResearchWorkflowResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.workflow.research import ResearchWorkflowService

router = APIRouter(prefix="/research", tags=["research"])


@router.post("/run", response_model=ResearchWorkflowResult)
def run_research_workflow(
    req: ResearchWorkflowRequest,
    workflow: ResearchWorkflowService = Depends(get_research_workflow_service),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO)),
) -> ResearchWorkflowResult:
    if settings.enforce_approved_strategy and not strategy_gov.is_approved(req.strategy_name):
        raise HTTPException(
            status_code=403,
            detail=f"Strategy '{req.strategy_name}' has no approved version.",
        )
    try:
        result = workflow.run(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="research_workflow",
        action="run",
        payload={
            "run_id": result.run_id,
            "strategy": req.strategy_name,
            "signals": len(result.signals),
            "optimized": result.optimized_portfolio is not None,
            "event_enriched": req.enable_event_enrichment or req.strategy_name == "event_driven",
            "event_rows_used": sum(item.event_rows_used for item in result.signals),
            "fundamental_enriched": req.enable_fundamental_enrichment,
            "fundamental_available_signals": sum(1 for item in result.signals if item.fundamental_available),
        },
    )
    return result
