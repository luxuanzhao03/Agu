from datetime import date, datetime, timezone
from pathlib import Path

from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventJoinPITRow,
    EventJoinPITValidationRequest,
    EventPolarity,
    EventRecordCreate,
    EventSourceRegisterRequest,
)
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore


def _service(tmp_path: Path) -> EventService:
    return EventService(store=EventStore(str(tmp_path / "event.db")))


def test_event_source_and_ingest_upsert(tmp_path: Path) -> None:
    service = _service(tmp_path)
    source_id = service.register_source(
        EventSourceRegisterRequest(
            source_name="ann_feed",
            source_type="ANNOUNCEMENT",
            provider="mock",
            created_by="qa",
        )
    )
    assert source_id > 0

    batch = EventBatchIngestRequest(
        source_name="ann_feed",
        events=[
            EventRecordCreate(
                event_id="e1",
                symbol="000001",
                event_type="earnings_preannounce",
                publish_time=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                polarity=EventPolarity.POSITIVE,
                score=0.9,
                confidence=0.8,
            ),
            EventRecordCreate(
                event_id="e2",
                symbol="000001",
                event_type="risk_notice",
                publish_time=datetime(2025, 1, 9, 8, 0, tzinfo=timezone.utc),
                polarity=EventPolarity.NEGATIVE,
                score=0.7,
                confidence=0.7,
            ),
        ],
    )
    first = service.ingest(batch)
    assert first.inserted == 2
    assert first.updated == 0
    assert first.errors == []

    second = service.ingest(
        EventBatchIngestRequest(
            source_name="ann_feed",
            events=[
                EventRecordCreate(
                    event_id="e1",
                    symbol="000001",
                    event_type="earnings_preannounce",
                    publish_time=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.95,
                    confidence=0.85,
                )
            ],
        )
    )
    assert second.inserted == 0
    assert second.updated == 1

    events = service.list_events(symbol="000001", source_name="ann_feed", limit=10)
    assert len(events) == 2
    latest_e1 = next(e for e in events if e.event_id == "e1")
    assert latest_e1.score == 0.95


def test_event_join_pit_detects_used_before_publish(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _ = service.register_source(EventSourceRegisterRequest(source_name="news_feed", provider="mock", created_by="qa"))
    _ = service.ingest(
        EventBatchIngestRequest(
            source_name="news_feed",
            events=[
                EventRecordCreate(
                    event_id="evt-a",
                    symbol="000001",
                    event_type="news",
                    publish_time=datetime(2025, 1, 10, 10, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.8,
                    confidence=0.8,
                )
            ],
        )
    )
    result = service.validate_join(
        EventJoinPITValidationRequest(
            rows=[
                EventJoinPITRow(
                    event_id="evt-a",
                    source_name="news_feed",
                    symbol="000001",
                    used_in_trade_time=datetime(2025, 1, 10, 9, 30, tzinfo=timezone.utc),
                )
            ]
        )
    )
    assert result.passed is False
    assert any(i.issue_type == "used_before_publish" for i in result.issues)


def test_event_feature_points_contains_positive_and_negative(tmp_path: Path) -> None:
    service = _service(tmp_path)
    _ = service.register_source(EventSourceRegisterRequest(source_name="mix_feed", provider="mock", created_by="qa"))
    _ = service.ingest(
        EventBatchIngestRequest(
            source_name="mix_feed",
            events=[
                EventRecordCreate(
                    event_id="p1",
                    symbol="000001",
                    event_type="buyback",
                    publish_time=datetime(2025, 1, 10, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.POSITIVE,
                    score=0.9,
                    confidence=0.9,
                ),
                EventRecordCreate(
                    event_id="n1",
                    symbol="000001",
                    event_type="investigation",
                    publish_time=datetime(2025, 1, 9, 8, 0, tzinfo=timezone.utc),
                    polarity=EventPolarity.NEGATIVE,
                    score=0.6,
                    confidence=0.8,
                ),
            ],
        )
    )
    points = service.build_feature_points(
        symbol="000001",
        trade_dates=[date(2025, 1, 10), date(2025, 1, 11)],
        lookback_days=5,
        decay_half_life_days=5,
    )
    assert len(points) == 2
    assert points[0].event_score > 0
    assert points[0].negative_event_score > 0
    assert points[0].positive_event_count >= 1
    assert points[0].negative_event_count >= 1
