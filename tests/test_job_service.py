from pathlib import Path
from types import SimpleNamespace

from trading_assistant.core.models import JobRegisterRequest, JobType
from trading_assistant.ops.job_service import JobService
from trading_assistant.ops.job_store import JobStore


class FakePipelineRunner:
    def run(self, req):
        return SimpleNamespace(
            run_id="pipe-1",
            strategy_name=req.strategy_name,
            total_symbols=len(req.symbols),
            total_signals=2,
            total_blocked=1,
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
            saved_path="reports/demo.md" if req.save_to_file else None,
            content="# demo",
        )


class FakeEventConnectorService:
    def run_connector(self, req):
        _ = req
        return SimpleNamespace(
            run=SimpleNamespace(
                run_id="connector-run-1",
                status=SimpleNamespace(value="SUCCESS"),
                pulled_count=3,
                normalized_count=3,
                inserted_count=2,
                updated_count=1,
                failed_count=0,
            )
        )

    def replay_failures(self, req):
        _ = req
        return SimpleNamespace(
            connector_name="c1",
            picked=2,
            replayed=2,
            failed=0,
            dead=0,
        )


def _service(tmp_path: Path) -> JobService:
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
    )


def _service_with_connector(tmp_path: Path) -> JobService:
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        event_connector=FakeEventConnectorService(),
    )


def test_job_trigger_pipeline_success(tmp_path: Path) -> None:
    service = _service(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="daily pipeline",
            job_type=JobType.PIPELINE_DAILY,
            owner="qa",
            payload={
                "symbols": ["000001"],
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "strategy_name": "trend_following",
                "strategy_params": {},
                "industry_map": {},
            },
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["total_symbols"] == 1
    assert run.result_summary["total_signals"] == 2


def test_job_trigger_disabled_job_fails(tmp_path: Path) -> None:
    service = _service(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="disabled report",
            job_type=JobType.REPORT_GENERATE,
            owner="qa",
            enabled=False,
            payload={
                "report_type": "risk",
                "save_to_file": False,
            },
        )
    )
    failed = False
    try:
        service.trigger(job_id=job_id, triggered_by="qa_user")
    except PermissionError:
        failed = True
    assert failed is True


def test_job_trigger_event_connector_sync_success(tmp_path: Path) -> None:
    service = _service_with_connector(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="connector sync",
            job_type=JobType.EVENT_CONNECTOR_SYNC,
            owner="qa",
            payload={"connector_name": "c1", "dry_run": False},
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["connector_name"] == "c1"
    assert run.result_summary["inserted"] == 2
