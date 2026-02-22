from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_audit_service, get_strategy_challenge_service
from trading_assistant.core.models import (
    AutoTuneApplyScope,
    StrategyChallengeRequest,
    StrategyChallengeResult,
    StrategyChallengeRolloutPlan,
    StrategyChallengeStrategyResult,
)
from trading_assistant.main import app


class FakeChallengeService:
    def run(self, req: StrategyChallengeRequest) -> StrategyChallengeResult:
        return StrategyChallengeResult(
            run_id="fake-run-id",
            generated_at=datetime.now(timezone.utc),
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            strategy_names=req.strategy_names or ["trend_following", "mean_reversion"],
            evaluated_count=66,
            qualified_count=2,
            champion_strategy="trend_following",
            runner_up_strategy="mean_reversion",
            market_fit_summary="2/2 strategies qualified.",
            rollout_plan=StrategyChallengeRolloutPlan(
                enabled=True,
                strategy_name="trend_following",
                symbol=req.symbol,
                gray_days=10,
                activation_scope=AutoTuneApplyScope.SYMBOL,
                rollback_triggers=["drawdown_breach"],
            ),
            results=[
                StrategyChallengeStrategyResult(
                    strategy_name="trend_following",
                    provider="fake_provider",
                    qualified=True,
                    ranking_score=0.84,
                ),
                StrategyChallengeStrategyResult(
                    strategy_name="mean_reversion",
                    provider="fake_provider",
                    qualified=True,
                    ranking_score=0.71,
                ),
            ],
        )


def test_strategy_challenge_api_run_success(tmp_path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_strategy_challenge_service] = lambda: FakeChallengeService()
    app.dependency_overrides[get_audit_service] = lambda: audit
    client = TestClient(app)
    try:
        resp = client.post(
            "/challenge/run",
            json={
                "symbol": "000001",
                "start_date": "2024-01-01",
                "end_date": "2025-12-31",
                "strategy_names": ["trend_following", "mean_reversion"],
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["run_id"] == "fake-run-id"
        assert payload["champion_strategy"] == "trend_following"
        assert payload["qualified_count"] == 2
        assert payload["rollout_plan"]["enabled"] is True
    finally:
        app.dependency_overrides.clear()


class ErrorChallengeService:
    def run(self, req: StrategyChallengeRequest) -> StrategyChallengeResult:
        _ = req
        raise KeyError("invalid strategy")


def test_strategy_challenge_api_invalid_strategy_returns_400(tmp_path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_strategy_challenge_service] = lambda: ErrorChallengeService()
    app.dependency_overrides[get_audit_service] = lambda: audit
    client = TestClient(app)
    try:
        resp = client.post(
            "/challenge/run",
            json={
                "symbol": "000001",
                "start_date": str(date(2024, 1, 1)),
                "end_date": str(date(2025, 12, 31)),
                "strategy_names": ["bad_name"],
            },
        )
        assert resp.status_code == 400
    finally:
        app.dependency_overrides.clear()
