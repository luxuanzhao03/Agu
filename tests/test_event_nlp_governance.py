from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventNLPAdjudicationRequest,
    EventNLPDriftCheckRequest,
    EventNLPFeedbackUpsertRequest,
    EventNLPLabelEntryUpsertRequest,
    EventNLPLabelSnapshotRequest,
    EventNLPRule,
    EventNLPRulesetUpsertRequest,
    EventPolarity,
    EventRecordCreate,
    EventSourceRegisterRequest,
)
from trading_assistant.governance.event_nlp_governance import EventNLPGovernanceService
from trading_assistant.governance.event_nlp_store import EventNLPStore
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore


class FakeFeatureCompareService:
    def compare(self, req):
        # Current window intentionally has weaker contribution than baseline.
        if req.start_date >= date(2025, 2, 1):
            ret_delta = 0.01
            sharpe_delta = 0.02
        else:
            ret_delta = 0.09
            sharpe_delta = 0.12
        return SimpleNamespace(
            delta=SimpleNamespace(total_return_delta=ret_delta, sharpe_delta=sharpe_delta),
            diagnostics=SimpleNamespace(event_row_ratio=0.25, events_loaded=12),
        )


def test_ruleset_versioning_activate_and_list(tmp_path: Path) -> None:
    db_path = str(tmp_path / "event.db")
    service = EventNLPGovernanceService(store=EventNLPStore(db_path))

    row_id = service.upsert_ruleset(
        EventNLPRulesetUpsertRequest(
            version="ruleset-v2",
            rules=[
                EventNLPRule(
                    rule_id="buyback_plus",
                    event_type="share_buyback",
                    polarity=EventPolarity.POSITIVE,
                    weight=0.92,
                    tag="buyback",
                    patterns=["buyback", "回购"],
                )
            ],
            created_by="qa",
            note="test ruleset",
            activate=True,
        )
    )
    assert row_id > 0
    active = service.get_active_ruleset(include_rules=True)
    assert active is not None
    assert active.version == "ruleset-v2"
    assert active.is_active is True
    assert active.rule_count == 1

    listed = service.list_rulesets(limit=10, include_rules=False)
    assert listed
    assert listed[0].version == "ruleset-v2"
    assert listed[0].rules == []


def test_nlp_drift_monitoring_tracks_hitrate_distribution_and_contribution(tmp_path: Path) -> None:
    db_path = str(tmp_path / "event.db")
    event_service = EventService(store=EventStore(db_path))
    _ = event_service.register_source(
        EventSourceRegisterRequest(
            source_name="ann_feed",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )

    baseline_events = [
        EventRecordCreate(
            event_id=f"b-{i}",
            symbol="000001",
            event_type="share_buyback",
            publish_time=datetime(2025, 1, 3 + i, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.POSITIVE,
            score=0.9,
            confidence=0.9,
            title="buyback",
            summary="buyback progress",
            metadata={"matched_rules": "share_buyback", "nlp_ruleset_version": "ruleset-v2"},
        )
        for i in range(4)
    ]
    current_events = [
        EventRecordCreate(
            event_id="c-1",
            symbol="000001",
            event_type="generic_announcement",
            publish_time=datetime(2025, 2, 2, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.NEUTRAL,
            score=0.2,
            confidence=0.6,
            title="notice",
            summary="generic notice",
            metadata={"matched_rules": "", "nlp_ruleset_version": "ruleset-v2"},
        ),
        EventRecordCreate(
            event_id="c-2",
            symbol="000001",
            event_type="generic_announcement",
            publish_time=datetime(2025, 2, 3, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.NEUTRAL,
            score=0.25,
            confidence=0.6,
            title="notice",
            summary="generic notice",
            metadata={"matched_rules": "", "nlp_ruleset_version": "ruleset-v2"},
        ),
        EventRecordCreate(
            event_id="c-3",
            symbol="000001",
            event_type="share_buyback",
            publish_time=datetime(2025, 2, 4, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.POSITIVE,
            score=0.55,
            confidence=0.8,
            title="buyback",
            summary="buyback update",
            metadata={"matched_rules": "share_buyback", "nlp_ruleset_version": "ruleset-v2"},
        ),
        EventRecordCreate(
            event_id="c-4",
            symbol="000001",
            event_type="policy_positive",
            publish_time=datetime(2025, 2, 5, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.POSITIVE,
            score=0.58,
            confidence=0.8,
            title="policy support",
            summary="policy support note",
            metadata={"matched_rules": "policy_positive", "nlp_ruleset_version": "ruleset-v2"},
        ),
    ]
    _ = event_service.ingest(EventBatchIngestRequest(source_name="ann_feed", events=baseline_events + current_events))

    service = EventNLPGovernanceService(
        store=EventNLPStore(db_path),
        feature_compare=FakeFeatureCompareService(),
    )
    _ = service.upsert_ruleset(
        EventNLPRulesetUpsertRequest(
            version="ruleset-v2",
            rules=[
                EventNLPRule(
                    rule_id="share_buyback",
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

    result = service.drift_check(
        EventNLPDriftCheckRequest(
            source_name="ann_feed",
            current_start=date(2025, 2, 1),
            current_end=date(2025, 2, 10),
            baseline_start=date(2025, 1, 1),
            baseline_end=date(2025, 1, 10),
            include_contribution=True,
            contribution_symbol="000001",
            contribution_strategy_name="event_driven",
            save_snapshot=True,
        )
    )
    assert result.current.sample_size == 4
    assert result.baseline.sample_size == 4
    assert result.hit_rate_delta < 0
    assert result.contribution_delta is not None
    assert result.contribution_delta < 0
    assert result.snapshot_id is not None
    assert result.alerts

    snapshots = service.list_drift_snapshots(source_name="ann_feed", limit=10)
    assert snapshots
    assert snapshots[0].id == result.snapshot_id

    second = service.drift_check(
        EventNLPDriftCheckRequest(
            source_name="ann_feed",
            current_start=date(2025, 2, 2),
            current_end=date(2025, 2, 10),
            baseline_start=date(2025, 1, 1),
            baseline_end=date(2025, 1, 10),
            include_contribution=True,
            contribution_symbol="000001",
            contribution_strategy_name="event_driven",
            save_snapshot=True,
        )
    )
    monitor = service.drift_monitor(source_name="ann_feed", limit=10)
    assert monitor.window_size >= 2
    assert monitor.latest_snapshot_id == second.snapshot_id
    assert monitor.latest_ruleset_version == "ruleset-v2"
    assert monitor.warning_alert_snapshots + monitor.critical_alert_snapshots >= 1
    assert monitor.latest_risk_level.value in {"WARNING", "CRITICAL"}


def test_nlp_feedback_loop_and_drift_feedback_accuracy_alerts(tmp_path: Path) -> None:
    db_path = str(tmp_path / "event.db")
    event_service = EventService(store=EventStore(db_path))
    _ = event_service.register_source(
        EventSourceRegisterRequest(
            source_name="ann_feedback",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )

    # Baseline window: prediction aligns with labels.
    baseline = [
        EventRecordCreate(
            event_id=f"fb-b-{i}",
            symbol="000001",
            event_type="share_buyback",
            publish_time=datetime(2025, 1, 2 + i, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.POSITIVE,
            score=0.85,
            confidence=0.9,
            title="buyback",
            summary="buyback notice",
            metadata={"matched_rules": "share_buyback", "nlp_ruleset_version": "ruleset-v3"},
        )
        for i in range(3)
    ]
    # Current window: labels disagree with prediction.
    current = [
        EventRecordCreate(
            event_id=f"fb-c-{i}",
            symbol="000001",
            event_type="share_buyback",
            publish_time=datetime(2025, 2, 2 + i, 8, 0, tzinfo=timezone.utc),
            polarity=EventPolarity.POSITIVE,
            score=0.82,
            confidence=0.85,
            title="buyback",
            summary="buyback notice",
            metadata={"matched_rules": "share_buyback", "nlp_ruleset_version": "ruleset-v3"},
        )
        for i in range(3)
    ]
    _ = event_service.ingest(EventBatchIngestRequest(source_name="ann_feedback", events=baseline + current))

    service = EventNLPGovernanceService(store=EventNLPStore(db_path))
    _ = service.upsert_ruleset(
        EventNLPRulesetUpsertRequest(
            version="ruleset-v3",
            rules=[
                EventNLPRule(
                    rule_id="share_buyback",
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

    # Baseline labels match predictions.
    for i in range(3):
        _ = service.upsert_feedback(
            EventNLPFeedbackUpsertRequest(
                source_name="ann_feedback",
                event_id=f"fb-b-{i}",
                label_event_type="share_buyback",
                label_polarity=EventPolarity.POSITIVE,
                label_score=0.85,
                labeler="reviewer_a",
            )
        )

    # Current labels conflict with predictions.
    for i in range(3):
        _ = service.upsert_feedback(
            EventNLPFeedbackUpsertRequest(
                source_name="ann_feedback",
                event_id=f"fb-c-{i}",
                label_event_type="earnings_warning",
                label_polarity=EventPolarity.NEGATIVE,
                label_score=0.2,
                labeler="reviewer_b",
            )
        )

    listed = service.list_feedback(source_name="ann_feedback", limit=20)
    assert len(listed) == 6
    summary_current = service.feedback_summary(
        source_name="ann_feedback",
        start_date=date(2025, 2, 1),
        end_date=date(2025, 2, 20),
    )
    assert summary_current.sample_size == 3
    assert summary_current.polarity_accuracy == 0.0
    assert summary_current.event_type_accuracy == 0.0

    drift = service.drift_check(
        EventNLPDriftCheckRequest(
            source_name="ann_feedback",
            current_start=date(2025, 2, 1),
            current_end=date(2025, 2, 20),
            baseline_start=date(2025, 1, 1),
            baseline_end=date(2025, 1, 20),
            include_contribution=False,
            include_feedback_quality=True,
            feedback_min_samples=2,
            save_snapshot=True,
        )
    )
    assert drift.feedback_current is not None
    assert drift.feedback_baseline is not None
    assert drift.feedback_polarity_accuracy_delta is not None
    assert drift.feedback_event_type_accuracy_delta is not None
    assert drift.feedback_polarity_accuracy_delta < 0
    assert drift.feedback_event_type_accuracy_delta < 0
    assert any(a.metric == "feedback_polarity_accuracy" for a in drift.alerts)


def test_nlp_label_adjudication_consistency_and_snapshot(tmp_path: Path) -> None:
    db_path = str(tmp_path / "event.db")
    event_service = EventService(store=EventStore(db_path))
    _ = event_service.register_source(
        EventSourceRegisterRequest(
            source_name="ann_qc",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    _ = event_service.ingest(
        EventBatchIngestRequest(
            source_name="ann_qc",
            events=[
                EventRecordCreate(
                    event_id="qc-1",
                    symbol="000001",
                    event_type="share_buyback",
                    publish_time=datetime(2025, 2, 10, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.9,
                    confidence=0.9,
                    title="buyback",
                    summary="buyback",
                    metadata={"matched_rules": "share_buyback", "nlp_ruleset_version": "ruleset-v4"},
                ),
                EventRecordCreate(
                    event_id="qc-2",
                    symbol="000001",
                    event_type="earnings_warning",
                    publish_time=datetime(2025, 2, 11, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.NEGATIVE,
                    score=0.8,
                    confidence=0.9,
                    title="warning",
                    summary="warning",
                    metadata={"matched_rules": "earnings_warning", "nlp_ruleset_version": "ruleset-v4"},
                ),
            ],
        )
    )
    service = EventNLPGovernanceService(store=EventNLPStore(db_path))

    # Event qc-1 has agreement, qc-2 has disagreement.
    for labeler in ["reviewer_a", "reviewer_b"]:
        _ = service.upsert_label_entry(
            EventNLPLabelEntryUpsertRequest(
                source_name="ann_qc",
                event_id="qc-1",
                label_event_type="share_buyback",
                label_polarity=EventPolarity.POSITIVE,
                label_score=0.88,
                labeler=labeler,
                label_version="v1",
            )
        )
    _ = service.upsert_label_entry(
        EventNLPLabelEntryUpsertRequest(
            source_name="ann_qc",
            event_id="qc-2",
            label_event_type="earnings_warning",
            label_polarity=EventPolarity.NEGATIVE,
            label_score=0.7,
            labeler="reviewer_a",
            label_version="v1",
        )
    )
    _ = service.upsert_label_entry(
        EventNLPLabelEntryUpsertRequest(
            source_name="ann_qc",
            event_id="qc-2",
            label_event_type="policy_positive",
            label_polarity=EventPolarity.POSITIVE,
            label_score=0.4,
            labeler="reviewer_b",
            label_version="v1",
        )
    )

    adjudication = service.adjudicate_labels(
        EventNLPAdjudicationRequest(
            source_name="ann_qc",
            start_date=date(2025, 2, 1),
            end_date=date(2025, 2, 20),
            min_labelers=2,
            save_consensus=True,
            adjudicated_by="qa_reviewer",
        )
    )
    assert adjudication.total_events == 2
    assert adjudication.adjudicated == 2
    assert adjudication.conflicts == 1

    consensus = service.list_consensus_labels(source_name="ann_qc", limit=20)
    assert len(consensus) == 2
    assert any(x.conflict for x in consensus)

    consistency = service.label_consistency_summary(
        source_name="ann_qc",
        start_date=date(2025, 2, 1),
        end_date=date(2025, 2, 20),
        min_labelers=2,
    )
    assert consistency.events_with_labels == 2
    assert consistency.total_label_rows == 4
    assert consistency.majority_conflict_rate > 0
    assert consistency.pair_agreements

    snapshot = service.create_label_snapshot(
        EventNLPLabelSnapshotRequest(
            source_name="ann_qc",
            start_date=date(2025, 2, 1),
            end_date=date(2025, 2, 20),
            min_labelers=2,
            include_conflicts=False,
            created_by="qa_reviewer",
            note="exclude conflicts",
        )
    )
    assert snapshot.sample_size == 1
    assert snapshot.consensus_size == 1
    assert snapshot.conflict_size == 0
    assert snapshot.hash_sha256
