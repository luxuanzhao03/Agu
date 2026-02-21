from pathlib import Path

from trading_assistant.core.models import (
    StrategyDecisionRequest,
    StrategyDecisionType,
    StrategySubmitReviewRequest,
    StrategyVersionRegisterRequest,
)
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.governance_store import StrategyGovernanceStore


def test_strategy_governance_register_and_approve(tmp_path: Path) -> None:
    service = StrategyGovernanceService(
        StrategyGovernanceStore(str(tmp_path / "strategy_gov.db")),
        required_approval_roles=["risk", "audit"],
        min_approval_count=2,
    )
    row_id = service.register_draft(
        StrategyVersionRegisterRequest(
            strategy_name="trend_following",
            version="v1.0.0",
            description="initial",
            params_hash="abc123",
            created_by="alice",
        )
    )
    assert row_id > 0
    review_id = service.submit_review(
        StrategySubmitReviewRequest(
            strategy_name="trend_following",
            version="v1.0.0",
            submitted_by="alice",
            note="submit",
        )
    )
    assert review_id == row_id
    service.decide(
        StrategyDecisionRequest(
            strategy_name="trend_following",
            version="v1.0.0",
            reviewer="bob",
            reviewer_role="risk",
            decision=StrategyDecisionType.APPROVE,
            note="risk ok",
        )
    )
    service.decide(
        StrategyDecisionRequest(
            strategy_name="trend_following",
            version="v1.0.0",
            reviewer="carol",
            reviewer_role="audit",
            decision=StrategyDecisionType.APPROVE,
            note="audit ok",
        )
    )
    latest = service.latest_approved("trend_following")
    assert latest is not None
    assert latest.version == "v1.0.0"


def test_strategy_governance_reject_flow(tmp_path: Path) -> None:
    service = StrategyGovernanceService(
        StrategyGovernanceStore(str(tmp_path / "strategy_gov.db")),
        required_approval_roles=["risk", "audit"],
        min_approval_count=2,
    )
    service.register_draft(
        StrategyVersionRegisterRequest(
            strategy_name="multi_factor",
            version="v2.0.0",
            description="initial",
            params_hash="hash",
            created_by="alice",
        )
    )
    service.submit_review(
        StrategySubmitReviewRequest(
            strategy_name="multi_factor",
            version="v2.0.0",
            submitted_by="alice",
        )
    )
    service.decide(
        StrategyDecisionRequest(
            strategy_name="multi_factor",
            version="v2.0.0",
            reviewer="risker",
            reviewer_role="risk",
            decision=StrategyDecisionType.REJECT,
            note="reject",
        )
    )
    versions = service.list_versions("multi_factor", limit=1)
    assert versions[0].status.value == "REJECTED"
