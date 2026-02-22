from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.challenge.service import StrategyChallengeService
from trading_assistant.core.container import get_audit_service, get_strategy_challenge_service
from trading_assistant.core.models import StrategyChallengeRequest, StrategyChallengeResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles

router = APIRouter(prefix="/challenge", tags=["challenge"])


@router.post("/run", response_model=StrategyChallengeResult)
def run_strategy_challenge(
    req: StrategyChallengeRequest,
    service: StrategyChallengeService = Depends(get_strategy_challenge_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> StrategyChallengeResult:
    try:
        result = service.run(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    audit.log(
        event_type="strategy_challenge",
        action="run",
        payload={
            "run_id": result.run_id,
            "symbol": result.symbol,
            "start_date": str(result.start_date),
            "end_date": str(result.end_date),
            "strategy_names": result.strategy_names,
            "evaluated_count": result.evaluated_count,
            "qualified_count": result.qualified_count,
            "champion_strategy": result.champion_strategy,
            "runner_up_strategy": result.runner_up_strategy,
            "rollout_plan": (result.rollout_plan.model_dump(mode="json") if result.rollout_plan is not None else None),
        },
    )
    return result

