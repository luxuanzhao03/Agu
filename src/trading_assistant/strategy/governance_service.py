from __future__ import annotations

from trading_assistant.core.models import (
    StrategyDecisionRecord,
    StrategyDecisionRequest,
    StrategyDecisionType,
    StrategySubmitReviewRequest,
    StrategyVersionApproveRequest,
    StrategyVersionRecord,
    StrategyVersionRegisterRequest,
    StrategyVersionStatus,
)
from trading_assistant.strategy.governance_store import StrategyGovernanceStore


class StrategyGovernanceService:
    def __init__(self, store: StrategyGovernanceStore, required_approval_roles: list[str], min_approval_count: int = 2) -> None:
        self.store = store
        self.required_approval_roles = [r.lower() for r in required_approval_roles if r.strip()]
        self.min_approval_count = max(1, min_approval_count)

    def register_draft(self, req: StrategyVersionRegisterRequest) -> int:
        return self.store.register_draft(
            strategy_name=req.strategy_name,
            version=req.version,
            description=req.description,
            params_hash=req.params_hash,
            created_by=req.created_by,
        )

    def submit_review(self, req: StrategySubmitReviewRequest) -> int:
        current = self.store.get_version(req.strategy_name, req.version)
        if current is None:
            return -1
        if current.status not in {StrategyVersionStatus.DRAFT, StrategyVersionStatus.REJECTED}:
            return current.id
        return self.store.update_status(
            strategy_name=req.strategy_name,
            version=req.version,
            status=StrategyVersionStatus.IN_REVIEW,
            note=req.note or f"submitted by {req.submitted_by}",
        )

    def decide(self, req: StrategyDecisionRequest) -> int:
        current = self.store.get_version(req.strategy_name, req.version)
        if current is None:
            return -1
        if current.status in {StrategyVersionStatus.APPROVED, StrategyVersionStatus.RETIRED}:
            return current.id
        if current.status == StrategyVersionStatus.DRAFT:
            self.store.update_status(
                strategy_name=req.strategy_name,
                version=req.version,
                status=StrategyVersionStatus.IN_REVIEW,
                note="auto-moved to IN_REVIEW by decision",
            )

        row_id = self.store.record_decision(
            strategy_name=req.strategy_name,
            version=req.version,
            reviewer=req.reviewer,
            reviewer_role=req.reviewer_role.lower(),
            decision=req.decision,
            note=req.note,
        )
        latest = self.store.latest_decision_by_role(req.strategy_name, req.version)

        if any(dec.decision == StrategyDecisionType.REJECT for dec in latest.values()):
            self.store.update_status(
                strategy_name=req.strategy_name,
                version=req.version,
                status=StrategyVersionStatus.REJECTED,
                note="Rejected by review decision.",
            )
            return row_id

        approved_roles = {
            role for role, dec in latest.items() if dec.decision == StrategyDecisionType.APPROVE
        }
        required_ok = all(role in approved_roles for role in self.required_approval_roles)
        min_count_ok = len(approved_roles) >= self.min_approval_count
        if required_ok and min_count_ok:
            approved_by = ",".join(sorted(approved_roles))
            self.store.update_status(
                strategy_name=req.strategy_name,
                version=req.version,
                status=StrategyVersionStatus.APPROVED,
                note="Approved by governance policy.",
                approved_by=approved_by,
            )
        else:
            self.store.update_status(
                strategy_name=req.strategy_name,
                version=req.version,
                status=StrategyVersionStatus.IN_REVIEW,
                note="Waiting for more approval decisions.",
            )
        return row_id

    def approve(self, req: StrategyVersionApproveRequest) -> int:
        # Backward-compatible API: direct approve from legacy endpoint.
        return self.decide(
            StrategyDecisionRequest(
                strategy_name=req.strategy_name,
                version=req.version,
                reviewer=req.approved_by,
                reviewer_role="risk",
                decision=StrategyDecisionType.APPROVE,
                note=req.note,
            )
        )

    def list_versions(self, strategy_name: str | None = None, limit: int = 200) -> list[StrategyVersionRecord]:
        return self.store.list_versions(strategy_name=strategy_name, limit=limit)

    def list_decisions(self, strategy_name: str, version: str, limit: int = 200) -> list[StrategyDecisionRecord]:
        return self.store.list_decisions(strategy_name=strategy_name, version=version, limit=limit)

    def latest_approved(self, strategy_name: str) -> StrategyVersionRecord | None:
        return self.store.latest_approved(strategy_name)

    def is_approved(self, strategy_name: str) -> bool:
        return self.latest_approved(strategy_name) is not None

