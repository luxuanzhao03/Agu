from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi.testclient import TestClient

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.container import get_audit_service, get_replay_service
from trading_assistant.core.models import (
    CostModelCalibrationRecord,
    CostModelCalibrationRequest,
    CostModelCalibrationResult,
)
from trading_assistant.main import app


class FakeReplayService:
    def calibrate_cost_model(self, req: CostModelCalibrationRequest) -> CostModelCalibrationResult:
        return CostModelCalibrationResult(
            generated_at=datetime.now(timezone.utc),
            calibration_id=12,
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            sample_size=88,
            executed_samples=72,
            slippage_coverage=0.82,
            follow_rate=0.78,
            avg_delay_days=0.45,
            avg_slippage_bps=18.3,
            p90_abs_slippage_bps=42.1,
            recommended_slippage_rate=0.0018,
            recommended_impact_cost_coeff=0.24,
            recommended_fill_probability_floor=0.08,
            confidence=0.74,
            notes=["ok"],
        )

    def list_cost_calibrations(self, symbol: str | None = None, limit: int = 30) -> list[CostModelCalibrationRecord]:
        _ = symbol, limit
        return [
            CostModelCalibrationRecord(
                id=12,
                created_at=datetime.now(timezone.utc),
                result=self.calibrate_cost_model(
                    CostModelCalibrationRequest(
                        symbol="000001",
                        strategy_name="trend_following",
                        start_date=date(2025, 1, 1),
                        end_date=date(2025, 12, 31),
                        limit=1000,
                        min_samples=30,
                        save_record=True,
                    )
                ),
            )
        ]


def test_replay_api_cost_calibration_endpoints(tmp_path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    app.dependency_overrides[get_replay_service] = lambda: FakeReplayService()
    app.dependency_overrides[get_audit_service] = lambda: audit
    client = TestClient(app)
    try:
        resp = client.post(
            "/replay/cost-model/calibrate",
            json={
                "symbol": "000001",
                "strategy_name": "trend_following",
                "start_date": "2025-01-01",
                "end_date": "2025-12-31",
                "limit": 1000,
                "min_samples": 30,
                "save_record": True,
            },
        )
        assert resp.status_code == 200
        payload = resp.json()
        assert payload["calibration_id"] == 12
        assert payload["sample_size"] == 88
        assert payload["recommended_slippage_rate"] > 0

        resp2 = client.get("/replay/cost-model/calibrations?symbol=000001&limit=10")
        assert resp2.status_code == 200
        rows = resp2.json()
        assert len(rows) == 1
        assert rows[0]["result"]["symbol"] == "000001"
    finally:
        app.dependency_overrides.clear()
