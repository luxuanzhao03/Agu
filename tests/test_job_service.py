from datetime import date
from pathlib import Path
from types import SimpleNamespace

from trading_assistant.core.models import ExecutionRecordCreate, JobRegisterRequest, JobType, SignalAction, SignalDecisionRecord
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


class FakeComplianceEvidenceService:
    def export_bundle(self, req):
        _ = req
        return SimpleNamespace(
            bundle_id="bundle-1",
            package_path="reports/compliance/bundle-1.zip",
            package_sha256="sha256-demo",
            signature=SimpleNamespace(enabled=True),
            vault_copy_path="reports/compliance_vault/bundle-1.zip",
            file_count=12,
        )


class FakeAlertService:
    def reconcile_oncall(self, req):
        _ = req
        return SimpleNamespace(
            provider="pagerduty",
            endpoint="local://data/oncall.json",
            pulled=3,
            matched=2,
            callbacks=2,
            acked_notifications=2,
            dry_run=False,
            errors=[],
        )


class FakeAutoTuneService:
    def run(self, req):
        _ = req
        return SimpleNamespace(
            run_id="autotune-1",
            strategy_name="trend_following",
            symbol="000001",
            evaluated_count=8,
            applied=True,
            apply_decision="applied",
            best=SimpleNamespace(objective_score=0.88),
            improvement_vs_baseline=0.12,
        )


def _service(tmp_path: Path) -> JobService:
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        replay=replay,
    )


def _service_with_connector(tmp_path: Path) -> JobService:
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        event_connector=FakeEventConnectorService(),
        replay=replay,
    )


def _service_with_compliance(tmp_path: Path) -> JobService:
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        compliance_evidence=FakeComplianceEvidenceService(),
        replay=replay,
    )


def _service_with_alerts(tmp_path: Path) -> JobService:
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        alerts=FakeAlertService(),
        replay=replay,
    )


def _service_with_autotune(tmp_path: Path) -> JobService:
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    return JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        autotune=FakeAutoTuneService(),
        replay=replay,
    )


def _prepare_replay_samples(tmp_path: Path) -> ReplayService:
    replay = ReplayService(ReplayStore(str(tmp_path / "replay.db")))
    for idx in range(6):
        signal_id = f"sig-{idx}"
        trade_date = date(2025, 1, idx + 1)
        replay.record_signal(
            SignalDecisionRecord(
                signal_id=signal_id,
                symbol="000001",
                strategy_name="trend_following",
                trade_date=trade_date,
                action=SignalAction.BUY,
                confidence=0.8,
                reason="x",
                suggested_position=0.05,
            )
        )
        replay.record_execution(
            ExecutionRecordCreate(
                signal_id=signal_id,
                symbol="000001",
                execution_date=trade_date,
                side=SignalAction.BUY,
                quantity=100,
                price=10.0 + idx * 0.1,
                reference_price=10.0,
                fee=1.0,
                note="sample",
            )
        )
    return replay


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


def test_job_trigger_compliance_evidence_export_success(tmp_path: Path) -> None:
    service = _service_with_compliance(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="compliance evidence export",
            job_type=JobType.COMPLIANCE_EVIDENCE_EXPORT,
            owner="qa",
            payload={
                "request": {
                    "triggered_by": "ops_job_runner",
                    "output_dir": "reports/compliance",
                    "package_prefix": "evidence_job",
                    "sign_bundle": True,
                }
            },
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["bundle_id"] == "bundle-1"
    assert run.result_summary["signature_enabled"] is True


def test_job_trigger_oncall_reconcile_success(tmp_path: Path) -> None:
    service = _service_with_alerts(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="oncall reconcile",
            job_type=JobType.ALERT_ONCALL_RECONCILE,
            owner="qa",
            payload={
                "provider": "pagerduty",
                "endpoint": "local://data/oncall.json",
                "mapping_template": "pagerduty",
                "limit": 200,
                "dry_run": False,
            },
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["provider"] == "pagerduty"
    assert run.result_summary["callbacks"] == 2
    assert run.result_summary["acked_notifications"] == 2


def test_job_trigger_autotune_success(tmp_path: Path) -> None:
    service = _service_with_autotune(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="autotune trend",
            job_type=JobType.AUTO_TUNE,
            owner="qa",
            payload={
                "request": {
                    "symbol": "000001",
                    "start_date": "2024-01-01",
                    "end_date": "2025-01-31",
                    "strategy_name": "trend_following",
                    "base_strategy_params": {},
                    "search_space": {"entry_ma_fast": [15, 20], "entry_ma_slow": [40, 60]},
                }
            },
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["autotune_run_id"] == "autotune-1"
    assert run.result_summary["applied"] is True
    assert run.result_summary["apply_decision"] == "applied"
    assert run.result_summary["evaluated_count"] == 8


def test_job_trigger_execution_review_success(tmp_path: Path) -> None:
    service = _service(tmp_path)
    job_id = service.register(
        JobRegisterRequest(
            name="execution review",
            job_type=JobType.EXECUTION_REVIEW,
            owner="qa",
            payload={
                "symbol": "000001",
                "limit": 200,
                "save_to_file": False,
            },
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["title"] == "closure report"
    assert run.result_summary["content_size"] > 0


def test_job_trigger_cost_model_recalibration_success(tmp_path: Path) -> None:
    replay = _prepare_replay_samples(tmp_path)
    service = JobService(
        store=JobStore(str(tmp_path / "job.db")),
        pipeline=FakePipelineRunner(),
        research=FakeResearchWorkflowService(),
        reporting=FakeReportingService(),
        replay=replay,
    )
    job_id = service.register(
        JobRegisterRequest(
            name="cost recalibration",
            job_type=JobType.COST_MODEL_RECALIBRATION,
            owner="qa",
            payload={
                "symbol": "000001",
                "strategy_name": "trend_following",
                "start_date": "2025-01-01",
                "end_date": "2025-01-31",
                "limit": 200,
                "min_samples": 5,
                "save_record": True,
                "save_to_file": False,
            },
        )
    )
    run = service.trigger(job_id=job_id, triggered_by="qa_user")
    assert run.status.value == "SUCCESS"
    assert run.result_summary["symbol"] == "000001"
    assert run.result_summary["sample_size"] == 6
    assert run.result_summary["recommended_slippage_rate"] > 0
