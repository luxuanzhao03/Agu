from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_audit_service, get_reporting_service
from trading_assistant.core.models import StrategyAccuracyReport
from trading_assistant.main import app


class FakeReportingService:
    def strategy_accuracy(
        self,
        *,
        lookback_days: int = 90,
        end_date: date | None = None,
        strategy_name: str | None = None,
        symbol: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 4000,
    ) -> StrategyAccuracyReport:
        _ = (strategy_name, symbol, min_confidence, limit)
        end = end_date or date(2025, 1, 31)
        start = end.replace(day=1)
        return StrategyAccuracyReport(
            generated_at=datetime.now(timezone.utc),
            start_date=start,
            end_date=end,
            lookback_days=lookback_days,
            sample_size=12,
            actionable_samples=10,
            executed_samples=7,
            execution_coverage=0.7,
            hit_rate=0.6667,
            brier_score=0.1823,
            expected_return_mean=0.008,
            realized_return_mean=0.006,
            return_bias=-0.002,
            return_mae=0.012,
            cost_bps_mean=11.2,
            cost_adjusted_return_mean=0.0041,
            notes=["ok"],
        )


def test_strategy_accuracy_api_endpoint(tmp_path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_reporting_service] = lambda: FakeReportingService()
    app.dependency_overrides[get_audit_service] = lambda: audit
    client = TestClient(app)
    try:
        resp = client.get(
            "/reports/strategy-accuracy?lookback_days=30&strategy_name=trend_following&symbol=000001&min_confidence=0.2&limit=2000"
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["sample_size"] == 12
        assert payload["execution_coverage"] == 0.7
        assert payload["hit_rate"] > 0.6
    finally:
        app.dependency_overrides.clear()
