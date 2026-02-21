from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from trading_assistant.core.models import (
    JobRegisterRequest,
    JobSLABreachType,
    JobType,
)
from trading_assistant.ops.cron import CronSchedule
from trading_assistant.ops.job_service import JobService
from trading_assistant.ops.job_store import JobStore


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


def _service(tmp_path: Path) -> JobService:
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        scheduler_timezone="Asia/Shanghai",
        running_timeout_minutes=60,
    )


def test_cron_schedule_matches_weekday_rule() -> None:
    schedule = CronSchedule.parse("0 17 * * 1-5")
    assert schedule.matches(datetime(2026, 1, 5, 17, 0, 0)) is True  # Monday
    assert schedule.matches(datetime(2026, 1, 4, 17, 0, 0)) is False  # Sunday


def test_scheduler_tick_triggers_once_per_minute(tmp_path: Path) -> None:
    service = _service(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="minute-report",
            job_type=JobType.REPORT_GENERATE,
            owner="ops",
            schedule_cron="* * * * *",
            payload={"report_type": "risk", "save_to_file": False},
        )
    )
    tick_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    first = service.scheduler_tick(as_of=tick_time, triggered_by="scheduler")
    assert first.matched_jobs == [job_id]
    assert len(first.triggered_runs) == 1

    second = service.scheduler_tick(as_of=tick_time, triggered_by="scheduler")
    assert second.matched_jobs == [job_id]
    assert len(second.triggered_runs) == 0
    assert second.skipped_jobs == [job_id]


def test_scheduler_register_invalid_cron_rejected(tmp_path: Path) -> None:
    service = _service(tmp_path)
    failed = False
    try:
        _ = service.register(
            JobRegisterRequest(
                name="bad-cron",
                job_type=JobType.REPORT_GENERATE,
                owner="ops",
                schedule_cron="invalid cron text",
                payload={"report_type": "risk", "save_to_file": False},
            )
        )
    except ValueError:
        failed = True
    assert failed is True


def test_scheduler_sla_detects_missed_run(tmp_path: Path) -> None:
    service = _service(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="missed-minute-report",
            job_type=JobType.REPORT_GENERATE,
            owner="ops",
            schedule_cron="* * * * *",
            payload={"report_type": "risk", "save_to_file": False},
        )
    )

    report = service.evaluate_sla(
        as_of=datetime(2026, 1, 5, 9, 31, tzinfo=timezone.utc),
        grace_minutes=0,
    )
    breaches = [b for b in report.breaches if b.job_id == job_id]
    assert len(breaches) == 1
    assert breaches[0].breach_type == JobSLABreachType.MISSED_RUN
