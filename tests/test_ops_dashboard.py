from datetime import date
from pathlib import Path
from types import SimpleNamespace

from trading_assistant.alerts.service import AlertService
from trading_assistant.alerts.store import AlertStore
from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import (
    AlertSubscriptionCreateRequest,
    ExecutionRecordCreate,
    JobRegisterRequest,
    JobType,
    OpsEventStats,
    SignalAction,
    SignalDecisionRecord,
    SignalLevel,
)
from trading_assistant.ops.dashboard import OpsDashboardService
from trading_assistant.ops.job_service import JobService
from trading_assistant.ops.job_store import JobStore
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore


class FakePipelineRunner:
    def run(self, req):
        return SimpleNamespace(
            run_id="pipe-1",
            strategy_name=req.strategy_name,
            total_symbols=len(req.symbols),
            total_signals=1,
            total_blocked=0,
            total_warnings=0,
        )


class FakeResearchWorkflowService:
    def run(self, req):
        return SimpleNamespace(
            run_id="research-1",
            strategy_name=req.strategy_name,
            signals=[{"symbol": s} for s in req.symbols],
            optimized_portfolio=None,
        )


class FakeReportingService:
    def generate(self, req):
        return SimpleNamespace(
            title=f"{req.report_type} report",
            saved_path=None,
            content="# demo",
        )


class FakeEventConnectorService:
    def ops_event_stats(self, lookback_days: int = 30) -> OpsEventStats:
        _ = lookback_days
        return OpsEventStats(
            lookback_days=30,
            total_events=12,
            active_symbols=3,
            active_sources=2,
            pending_failures=1,
            dead_failures=0,
            connector_runs_24h=4,
            connector_failures_24h=1,
        )


def _job_service(tmp_path: Path) -> JobService:
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
    )


def test_ops_dashboard_summary(tmp_path: Path) -> None:
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    alerts = AlertService(store=AlertStore(str(tmp_path / "alert.db")), audit=audit)
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    jobs = _job_service(tmp_path)

    job_id = jobs.register(
        JobRegisterRequest(
            name="dashboard-report",
            job_type=JobType.REPORT_GENERATE,
            owner="ops",
            schedule_cron="0 17 * * 1-5",
            payload={"report_type": "risk", "save_to_file": False},
        )
    )
    _ = jobs.trigger(job_id=job_id, triggered_by="ops_user")

    _ = alerts.create_subscription(
        AlertSubscriptionCreateRequest(
            name="ops-alerts",
            owner="ops",
            event_types=[],
            min_severity=SignalLevel.WARNING,
            dedupe_window_sec=60,
            enabled=True,
        )
    )
    audit.log("risk_check", "evaluate", {"symbol": "000001", "blocked": True})
    _ = alerts.sync_from_audit(limit=200)

    replay.record_signal(
        SignalDecisionRecord(
            signal_id="sig-1",
            symbol="000001",
            strategy_name="trend_following",
            trade_date=date(2025, 1, 2),
            action=SignalAction.BUY,
            confidence=0.8,
            reason="demo",
            suggested_position=0.05,
        )
    )
    _ = replay.record_execution(
        ExecutionRecordCreate(
            signal_id="sig-1",
            symbol="000001",
            execution_date=date(2025, 1, 2),
            side=SignalAction.BUY,
            quantity=100,
            price=10.0,
            fee=0.0,
            note="demo",
        )
    )

    dashboard = OpsDashboardService(
        jobs=jobs,
        alerts=alerts,
        replay=replay,
        event_connector=FakeEventConnectorService(),
    )
    summary = dashboard.summary(sync_alerts_from_audit=False)
    assert summary.jobs.total_jobs == 1
    assert summary.jobs.runs_last_24h >= 1
    assert summary.alerts.unacked_total >= 1
    assert summary.execution.sample_size == 1
    assert summary.event is not None
    assert summary.event.total_events == 12
    assert summary.sla.total_scheduled_jobs == 1
    assert len(summary.recent_runs) >= 1
