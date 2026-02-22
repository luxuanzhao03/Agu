from datetime import datetime, timezone
import json
from pathlib import Path
import zipfile

from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.core.models import (
    ComplianceEvidenceCounterSignRequest,
    ComplianceEvidenceExportRequest,
    ComplianceEvidenceVerifyRequest,
    EventBatchIngestRequest,
    EventNLPRule,
    EventNLPRulesetUpsertRequest,
    EventPolarity,
    EventRecordCreate,
    EventSourceRegisterRequest,
    StrategyDecisionRequest,
    StrategyDecisionType,
    StrategySubmitReviewRequest,
    StrategyVersionRegisterRequest,
)
from trading_assistant.governance.compliance_evidence import ComplianceEvidenceService
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.governance.event_connector_store import EventConnectorStore
from trading_assistant.governance.event_nlp import EventStandardizer
from trading_assistant.governance.event_nlp_governance import EventNLPGovernanceService
from trading_assistant.governance.event_nlp_store import EventNLPStore
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.governance_store import StrategyGovernanceStore


def test_compliance_evidence_bundle_export(tmp_path: Path) -> None:
    db_path = str(tmp_path / "event.db")
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))

    event_service = EventService(store=EventStore(db_path))
    _ = event_service.register_source(
        EventSourceRegisterRequest(
            source_name="evidence_source",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = event_service.ingest(
        EventBatchIngestRequest(
            source_name="evidence_source",
            events=[
                EventRecordCreate(
                    event_id="evi-1",
                    symbol="000001",
                    event_type="share_buyback",
                    publish_time=datetime(2025, 2, 1, 8, 30, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.86,
                    confidence=0.9,
                    title="Buyback",
                    summary="Evidence sample event",
                    metadata={"nlp_ruleset_version": "ruleset-evidence"},
                )
            ],
        )
    )

    connector_service = EventConnectorService(
        event_service=event_service,
        connector_store=EventConnectorStore(db_path),
        standardizer=EventStandardizer(),
    )
    nlp_service = EventNLPGovernanceService(store=EventNLPStore(db_path))
    _ = nlp_service.upsert_ruleset(
        EventNLPRulesetUpsertRequest(
            version="ruleset-evidence",
            rules=[
                EventNLPRule(
                    rule_id="buyback_rule",
                    event_type="share_buyback",
                    polarity=EventPolarity.POSITIVE,
                    weight=0.9,
                    tag="buyback",
                    patterns=["buyback"],
                )
            ],
            created_by="qa",
            activate=True,
        )
    )

    strategy_service = StrategyGovernanceService(
        store=StrategyGovernanceStore(str(tmp_path / "strategy_gov.db")),
        required_approval_roles=["risk", "audit"],
        min_approval_count=2,
    )
    _ = strategy_service.register_draft(
        StrategyVersionRegisterRequest(
            strategy_name="event_driven",
            version="v1.0.0",
            description="evidence strategy",
            params_hash="sha256:demo",
            created_by="qa",
        )
    )
    _ = strategy_service.submit_review(
        StrategySubmitReviewRequest(
            strategy_name="event_driven",
            version="v1.0.0",
            submitted_by="qa",
            note="submit for evidence test",
        )
    )
    _ = strategy_service.decide(
        StrategyDecisionRequest(
            strategy_name="event_driven",
            version="v1.0.0",
            reviewer="risk_user",
            reviewer_role="risk",
            decision=StrategyDecisionType.APPROVE,
            note="risk ok",
        )
    )
    _ = strategy_service.decide(
        StrategyDecisionRequest(
            strategy_name="event_driven",
            version="v1.0.0",
            reviewer="audit_user",
            reviewer_role="audit",
            decision=StrategyDecisionType.APPROVE,
            note="audit ok",
        )
    )

    audit.log("event_connector_sla", "freshness", {"connector_name": "demo_connector", "severity": "WARNING"})
    audit.log("strategy_governance", "approve", {"strategy_name": "event_driven", "version": "v1.0.0"})

    service = ComplianceEvidenceService(
        audit=audit,
        strategy_gov=strategy_service,
        event_connector=connector_service,
        event_nlp=nlp_service,
    )
    result = service.export_bundle(
        ComplianceEvidenceExportRequest(
            triggered_by="audit_user",
            output_dir=str(tmp_path / "reports"),
            package_prefix="evidence_test",
            include_ruleset_body=True,
            include_feedback_summary=True,
        )
    )

    package_path = Path(result.package_path)
    assert package_path.exists()
    assert result.file_count >= 8
    assert result.package_sha256

    with zipfile.ZipFile(package_path, "r") as zf:
        names = set(zf.namelist())
        assert "manifest.json" in names
        assert "audit_chain_verify.json" in names
        assert "strategy_versions.json" in names
        assert "event_connector_sla_report.json" in names
        assert "event_nlp_drift_monitor.json" in names
        manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
    assert manifest["bundle_id"] == result.bundle_id
    assert manifest["summary"]["audit_events"] >= 1


def test_compliance_evidence_signature_verify_and_vault_copy(tmp_path: Path) -> None:
    db_path = str(tmp_path / "event.db")
    audit = AuditService(AuditStore(str(tmp_path / "audit.db")))
    event_service = EventService(store=EventStore(db_path))
    _ = event_service.register_source(
        EventSourceRegisterRequest(
            source_name="evidence_source_sign",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = event_service.ingest(
        EventBatchIngestRequest(
            source_name="evidence_source_sign",
            events=[
                EventRecordCreate(
                    event_id="evi-sign-1",
                    symbol="000001",
                    event_type="share_buyback",
                    publish_time=datetime(2025, 2, 2, 8, 30, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.8,
                    confidence=0.8,
                    title="Buyback",
                    summary="Evidence signed sample",
                    metadata={"nlp_ruleset_version": "ruleset-sign"},
                )
            ],
        )
    )
    connector_service = EventConnectorService(
        event_service=event_service,
        connector_store=EventConnectorStore(db_path),
        standardizer=EventStandardizer(),
    )
    nlp_service = EventNLPGovernanceService(store=EventNLPStore(db_path))
    _ = nlp_service.upsert_ruleset(
        EventNLPRulesetUpsertRequest(
            version="ruleset-sign",
            rules=[
                EventNLPRule(
                    rule_id="buyback_rule",
                    event_type="share_buyback",
                    polarity=EventPolarity.POSITIVE,
                    weight=0.9,
                    tag="buyback",
                    patterns=["buyback"],
                )
            ],
            created_by="qa",
            activate=True,
        )
    )
    strategy_service = StrategyGovernanceService(
        store=StrategyGovernanceStore(str(tmp_path / "strategy_gov.db")),
        required_approval_roles=["risk", "audit"],
        min_approval_count=2,
    )

    service = ComplianceEvidenceService(
        audit=audit,
        strategy_gov=strategy_service,
        event_connector=connector_service,
        event_nlp=nlp_service,
        default_signing_secret="demo-secret",
        default_vault_dir=str(tmp_path / "vault"),
    )
    external_worm_file = tmp_path / "external" / "worm_request.json"
    external_kms_file = tmp_path / "external" / "kms_request.json"
    result = service.export_bundle(
        ComplianceEvidenceExportRequest(
            triggered_by="audit_user",
            output_dir=str(tmp_path / "reports"),
            package_prefix="evidence_signed",
            sign_bundle=True,
            signer="compliance_ops",
            signing_key_id="ops-key",
            retention_policy="regulatory_7y",
            vault_mode="SIMULATED_WORM",
            kms_key_id="kms-key-demo",
            external_worm_endpoint=f"local://{external_worm_file}",
            external_kms_wrap_endpoint=f"local://{external_kms_file}",
            external_require_success=True,
            write_vault_copy=True,
            cleanup_bundle_dir=True,
        )
    )
    assert result.signature is not None
    assert result.signature.enabled is True
    assert result.signature_path is not None
    assert Path(result.signature_path).exists()
    assert result.vault_copy_path is not None
    assert Path(result.vault_copy_path).exists()
    assert result.summary.get("vault_worm_lock_path")
    assert Path(str(result.summary.get("vault_worm_lock_path"))).exists()
    assert result.summary.get("vault_envelope_path")
    assert Path(str(result.summary.get("vault_envelope_path"))).exists()
    assert result.summary.get("external_worm_status") == "OK"
    assert isinstance(result.summary.get("external_worm_receipt"), dict)
    assert result.summary.get("external_kms_status") == "OK"
    assert external_worm_file.exists()
    assert external_kms_file.exists()
    assert not Path(result.bundle_dir).exists()

    countersign = service.countersign_package(
        ComplianceEvidenceCounterSignRequest(
            package_path=result.package_path,
            signer="audit_manager",
            signing_key_id="audit-key-1",
            signing_secret="demo-secret",
            note="dual control",
        )
    )
    assert countersign.entry_count == 1
    assert Path(countersign.countersign_path).exists()

    verify = service.verify_package(
        ComplianceEvidenceVerifyRequest(
            package_path=result.package_path,
            signature_path=result.signature_path,
            countersign_path=countersign.countersign_path,
            require_countersign=True,
            signing_secret="demo-secret",
        )
    )
    assert verify.package_exists is True
    assert verify.manifest_valid is True
    assert verify.signature_checked is True
    assert verify.signature_valid is True
    assert verify.countersign_checked is True
    assert verify.countersign_valid is True
    assert verify.countersign_count == 1
