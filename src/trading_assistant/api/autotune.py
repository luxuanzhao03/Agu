from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.audit.service import AuditService
from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.core.container import get_audit_service, get_autotune_service
from trading_assistant.core.models import AutoTuneProfileRecord, AutoTuneRunRequest, AutoTuneRunResult
from trading_assistant.core.models import (
    AutoTuneRollbackRequest,
    AutoTuneRolloutRuleRecord,
    AutoTuneRolloutRuleUpsertRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles

router = APIRouter(prefix="/autotune", tags=["autotune"])


@router.post("/run", response_model=AutoTuneRunResult)
def run_autotune(
    req: AutoTuneRunRequest,
    service: AutoTuneService = Depends(get_autotune_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> AutoTuneRunResult:
    try:
        result = service.run(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    audit.log(
        event_type="autotune",
        action="run",
        payload={
            "run_id": result.run_id,
            "strategy_name": req.strategy_name,
            "symbol": req.symbol,
            "evaluated_count": result.evaluated_count,
            "applied": result.applied,
            "apply_decision": result.apply_decision,
            "best_objective": (result.best.objective_score if result.best is not None else None),
            "improvement_vs_baseline": result.improvement_vs_baseline,
            "governance_draft_id": result.governance_draft_id,
            "governance_version": result.governance_version,
        },
    )
    return result


@router.get("/profiles", response_model=list[AutoTuneProfileRecord])
def list_profiles(
    strategy_name: str | None = None,
    symbol: str | None = None,
    active_only: bool = False,
    limit: int = 200,
    service: AutoTuneService = Depends(get_autotune_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> list[AutoTuneProfileRecord]:
    return service.list_profiles(
        strategy_name=strategy_name,
        symbol=symbol,
        active_only=active_only,
        limit=limit,
    )


@router.get("/profiles/active", response_model=AutoTuneProfileRecord | None)
def get_active_profile(
    strategy_name: str,
    symbol: str | None = None,
    service: AutoTuneService = Depends(get_autotune_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> AutoTuneProfileRecord | None:
    return service.get_active_profile(strategy_name=strategy_name, symbol=symbol)


@router.post("/profiles/{profile_id}/activate", response_model=AutoTuneProfileRecord)
def activate_profile(
    profile_id: int,
    service: AutoTuneService = Depends(get_autotune_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> AutoTuneProfileRecord:
    record = service.activate_profile(profile_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"profile_id '{profile_id}' not found")
    audit.log(
        event_type="autotune",
        action="activate_profile",
        payload={
            "profile_id": record.id,
            "strategy_name": record.strategy_name,
            "scope": record.scope.value,
            "symbol": record.symbol,
        },
    )
    return record


@router.post("/profiles/rollback", response_model=AutoTuneProfileRecord)
def rollback_profile(
    req: AutoTuneRollbackRequest,
    service: AutoTuneService = Depends(get_autotune_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> AutoTuneProfileRecord:
    record = service.rollback_active_profile(
        strategy_name=req.strategy_name,
        symbol=req.symbol,
        scope=req.scope,
    )
    if record is None:
        raise HTTPException(status_code=404, detail="no previous profile to rollback")
    audit.log(
        event_type="autotune",
        action="rollback_profile",
        payload={
            "profile_id": record.id,
            "strategy_name": record.strategy_name,
            "scope": record.scope.value,
            "symbol": record.symbol,
        },
    )
    return record


@router.post("/rollout/rules/upsert", response_model=AutoTuneRolloutRuleRecord)
def upsert_rollout_rule(
    req: AutoTuneRolloutRuleUpsertRequest,
    service: AutoTuneService = Depends(get_autotune_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> AutoTuneRolloutRuleRecord:
    record = service.upsert_rollout_rule(
        strategy_name=req.strategy_name,
        symbol=req.symbol,
        enabled=req.enabled,
        note=req.note,
    )
    audit.log(
        event_type="autotune",
        action="upsert_rollout_rule",
        payload={
            "rule_id": record.id,
            "strategy_name": record.strategy_name,
            "symbol": record.symbol,
            "enabled": record.enabled,
        },
    )
    return record


@router.get("/rollout/rules", response_model=list[AutoTuneRolloutRuleRecord])
def list_rollout_rules(
    strategy_name: str | None = None,
    symbol: str | None = None,
    limit: int = 500,
    service: AutoTuneService = Depends(get_autotune_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> list[AutoTuneRolloutRuleRecord]:
    return service.list_rollout_rules(
        strategy_name=strategy_name,
        symbol=symbol,
        limit=limit,
    )


@router.delete("/rollout/rules/{rule_id}", response_model=bool)
def delete_rollout_rule(
    rule_id: int,
    service: AutoTuneService = Depends(get_autotune_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> bool:
    deleted = service.delete_rollout_rule(rule_id)
    if deleted:
        audit.log(
            event_type="autotune",
            action="delete_rollout_rule",
            payload={"rule_id": rule_id},
        )
    return deleted
