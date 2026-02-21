from __future__ import annotations

from fastapi import APIRouter, Depends

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import get_audit_service, get_strategy_governance_service
from trading_assistant.core.models import (
    StrategyApprovalPolicy,
    StrategyDecisionRecord,
    StrategyDecisionRequest,
    StrategySubmitReviewRequest,
    StrategyVersionApproveRequest,
    StrategyVersionRecord,
    StrategyVersionRegisterRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.strategy.governance_service import StrategyGovernanceService

router = APIRouter(prefix="/strategy-governance", tags=["strategy-governance"])


@router.post("/register", response_model=int)
def register_strategy_version(
    req: StrategyVersionRegisterRequest,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.ADMIN)),
) -> int:
    row_id = gov.register_draft(req)
    audit.log(
        event_type="strategy_governance",
        action="register",
        payload={"strategy_name": req.strategy_name, "version": req.version, "id": row_id},
    )
    return row_id


@router.post("/approve", response_model=int)
def approve_strategy_version(
    req: StrategyVersionApproveRequest,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> int:
    row_id = gov.approve(req)
    audit.log(
        event_type="strategy_governance",
        action="approve",
        payload={"strategy_name": req.strategy_name, "version": req.version, "id": row_id},
    )
    return row_id


@router.post("/submit-review", response_model=int)
def submit_strategy_review(
    req: StrategySubmitReviewRequest,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.ADMIN)),
) -> int:
    row_id = gov.submit_review(req)
    audit.log(
        event_type="strategy_governance",
        action="submit_review",
        payload={"strategy_name": req.strategy_name, "version": req.version, "id": row_id},
    )
    return row_id


@router.post("/decide", response_model=int)
def decide_strategy_review(
    req: StrategyDecisionRequest,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    audit: AuditService = Depends(get_audit_service),
    ctx: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> int:
    # Force reviewer role to authenticated role unless admin explicitly sets role.
    reviewer_role = req.reviewer_role.lower()
    if ctx.role != UserRole.ADMIN:
        reviewer_role = ctx.role.value
    row_id = gov.decide(
        StrategyDecisionRequest(
            strategy_name=req.strategy_name,
            version=req.version,
            reviewer=req.reviewer,
            reviewer_role=reviewer_role,
            decision=req.decision,
            note=req.note,
        )
    )
    audit.log(
        event_type="strategy_governance",
        action="decide",
        payload={
            "strategy_name": req.strategy_name,
            "version": req.version,
            "id": row_id,
            "decision": req.decision.value,
            "reviewer_role": reviewer_role,
        },
    )
    return row_id


@router.get("/versions", response_model=list[StrategyVersionRecord])
def list_strategy_versions(
    strategy_name: str | None = None,
    limit: int = 200,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> list[StrategyVersionRecord]:
    return gov.list_versions(strategy_name=strategy_name, limit=limit)


@router.get("/latest-approved", response_model=StrategyVersionRecord | None)
def latest_approved_version(
    strategy_name: str,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> StrategyVersionRecord | None:
    return gov.latest_approved(strategy_name=strategy_name)


@router.get("/decisions", response_model=list[StrategyDecisionRecord])
def list_strategy_decisions(
    strategy_name: str,
    version: str,
    limit: int = 200,
    gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> list[StrategyDecisionRecord]:
    return gov.list_decisions(strategy_name=strategy_name, version=version, limit=limit)


@router.get("/policy", response_model=StrategyApprovalPolicy)
def strategy_approval_policy(
    settings: Settings = Depends(get_settings),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT, UserRole.ADMIN)),
) -> StrategyApprovalPolicy:
    return StrategyApprovalPolicy(
        required_roles=settings.required_approval_roles_list,
        min_approval_count=settings.strategy_min_approval_count,
        enforce_runtime_approved_only=settings.enforce_approved_strategy,
    )
