from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import get_audit_service, get_pipeline_runner, get_strategy_governance_service
from trading_assistant.core.models import PipelineRunRequest, PipelineRunResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.pipeline.runner import DailyPipelineRunner
from trading_assistant.strategy.governance_service import StrategyGovernanceService

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


@router.post("/daily-run", response_model=PipelineRunResult)
def run_daily_pipeline(
    req: PipelineRunRequest,
    runner: DailyPipelineRunner = Depends(get_pipeline_runner),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK)),
) -> PipelineRunResult:
    if settings.enforce_approved_strategy and not strategy_gov.is_approved(req.strategy_name):
        raise HTTPException(
            status_code=403,
            detail=f"Strategy '{req.strategy_name}' has no approved version.",
        )
    try:
        result = runner.run(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit.log(
        event_type="pipeline",
        action="daily_run",
        payload={
            "run_id": result.run_id,
            "strategy": req.strategy_name,
            "symbols": len(req.symbols),
            "event_enriched": req.enable_event_enrichment or req.strategy_name == "event_driven",
            "event_rows_used": sum(r.event_rows_used for r in result.results),
            "fundamental_enriched": req.enable_fundamental_enrichment,
            "fundamental_available_symbols": sum(1 for r in result.results if r.fundamental_available),
            "small_capital_mode": req.enable_small_capital_mode,
            "small_capital_blocked_symbols": sum(1 for r in result.results if r.small_capital_blocked),
        },
    )
    return result
