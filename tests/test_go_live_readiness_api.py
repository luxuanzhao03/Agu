from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_audit_service, get_reporting_service
from trading_assistant.core.models import (
    GoLiveChecklistItem,
    GoLiveGateCheck,
    GoLiveReadinessReport,
    GoLiveRollbackRule,
    SignalLevel,
    StrategyAccuracyReport,
)
from trading_assistant.main import app


class FakeReportingService:
    def go_live_readiness(
        self,
        *,
        lookback_days: int = 90,
        end_date: date | None = None,
        strategy_name: str | None = None,
        symbol: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 4000,
    ) -> GoLiveReadinessReport:
        _ = (strategy_name, symbol, min_confidence, limit)
        end = end_date or date(2025, 1, 31)
        start = end.replace(day=1)
        return GoLiveReadinessReport(
            generated_at=datetime.now(timezone.utc),
            lookback_days=lookback_days,
            start_date=start,
            end_date=end,
            overall_passed=True,
            readiness_level="GRAY_READY",
            failed_gate_count=0,
            warning_gate_count=1,
            gate_checks=[
                GoLiveGateCheck(
                    gate_key="oos_hit_rate",
                    gate_name="样本外命中率",
                    passed=True,
                    severity=SignalLevel.CRITICAL,
                    actual_value=0.61,
                    threshold_value=0.55,
                    comparator=">=",
                    detail="ok",
                )
            ],
            rollback_rules=[
                GoLiveRollbackRule(
                    trigger_key="daily_loss_limit",
                    trigger_name="单日最大亏损",
                    condition="daily_portfolio_return <= -0.025",
                )
            ],
            daily_checklist=[
                GoLiveChecklistItem(
                    item_key="accuracy_refresh",
                    item_name="刷新策略准确性看板",
                    status="PASS",
                    detail="ok",
                    evidence="/reports/strategy-accuracy",
                )
            ],
            latest_accuracy=StrategyAccuracyReport(
                generated_at=datetime.now(timezone.utc),
                start_date=start,
                end_date=end,
                lookback_days=lookback_days,
                sample_size=120,
                hit_rate=0.61,
            ),
            notes=["ok"],
        )


def test_go_live_readiness_api_endpoint(tmp_path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_reporting_service] = lambda: FakeReportingService()
    app.dependency_overrides[get_audit_service] = lambda: audit
    client = TestClient(app)
    try:
        resp = client.get("/reports/go-live-readiness?lookback_days=45&strategy_name=trend_following")
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["readiness_level"] == "GRAY_READY"
        assert payload["overall_passed"] is True
        assert payload["failed_gate_count"] == 0
        assert payload["gate_checks"]
        assert payload["rollback_rules"]
        assert payload["daily_checklist"]
    finally:
        app.dependency_overrides.clear()
