from __future__ import annotations

import math
from datetime import date, datetime, time, timedelta, timezone

import pandas as pd

from trading_assistant.core.models import (
    EventBatchIngestRequest,
    EventBatchIngestResult,
    EventFeaturePoint,
    EventFeaturePreviewRequest,
    EventFeaturePreviewResult,
    EventJoinPITIssue,
    EventJoinPITValidationRequest,
    EventJoinPITValidationResult,
    EventPolarity,
    EventRecord,
    EventSourceRecord,
    EventSourceRegisterRequest,
    SignalLevel,
)
from trading_assistant.governance.event_store import EventStore


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class EventService:
    def __init__(self, store: EventStore) -> None:
        self.store = store

    def register_source(self, req: EventSourceRegisterRequest) -> int:
        return self.store.register_source(req)

    def list_sources(self, limit: int = 200) -> list[EventSourceRecord]:
        return self.store.list_sources(limit=limit)

    def ingest(self, req: EventBatchIngestRequest) -> EventBatchIngestResult:
        inserted, updated, errors = self.store.ingest_batch(req)
        return EventBatchIngestResult(
            source_name=req.source_name,
            inserted=inserted,
            updated=updated,
            total=len(req.events),
            errors=errors,
        )

    def list_events(
        self,
        symbol: str | None = None,
        source_name: str | None = None,
        event_type: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 500,
    ) -> list[EventRecord]:
        return self.store.list_events(
            symbol=symbol,
            source_name=source_name,
            event_type=event_type,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def validate_join(self, req: EventJoinPITValidationRequest) -> EventJoinPITValidationResult:
        issues: list[EventJoinPITIssue] = []
        for idx, row in enumerate(req.rows):
            event: EventRecord | None = None
            if row.source_name:
                event = self.store.get_event(source_name=row.source_name, event_id=row.event_id)
                if event is None:
                    issues.append(
                        EventJoinPITIssue(
                            row_index=idx,
                            event_id=row.event_id,
                            issue_type="event_not_found",
                            severity=SignalLevel.CRITICAL,
                            message=f"source={row.source_name}, event_id={row.event_id} not found.",
                        )
                    )
                    continue
            else:
                candidates = self.store.find_events_by_event_id(event_id=row.event_id, limit=20)
                if not candidates:
                    issues.append(
                        EventJoinPITIssue(
                            row_index=idx,
                            event_id=row.event_id,
                            issue_type="event_not_found",
                            severity=SignalLevel.CRITICAL,
                            message=f"event_id={row.event_id} not found.",
                        )
                    )
                    continue
                if len(candidates) == 1:
                    event = candidates[0]
                else:
                    symbol_matched = [e for e in candidates if e.symbol == row.symbol]
                    if len(symbol_matched) == 1:
                        event = symbol_matched[0]
                        issues.append(
                            EventJoinPITIssue(
                                row_index=idx,
                                event_id=row.event_id,
                                issue_type="event_id_ambiguous_resolved",
                                severity=SignalLevel.WARNING,
                                message="event_id resolved by symbol match across multiple sources.",
                            )
                        )
                    else:
                        issues.append(
                            EventJoinPITIssue(
                                row_index=idx,
                                event_id=row.event_id,
                                issue_type="event_id_ambiguous",
                                severity=SignalLevel.CRITICAL,
                                message="event_id matched multiple records; provide source_name.",
                            )
                        )
                        continue

            if event is None:
                continue

            if req.strict_symbol_match and event.symbol != row.symbol:
                issues.append(
                    EventJoinPITIssue(
                        row_index=idx,
                        event_id=row.event_id,
                        issue_type="symbol_mismatch",
                        severity=SignalLevel.CRITICAL,
                        message=f"row symbol={row.symbol}, event symbol={event.symbol}.",
                    )
                )
                continue

            used_time = _ensure_utc(row.used_in_trade_time)
            publish_time = _ensure_utc(event.publish_time)
            if publish_time > used_time:
                issues.append(
                    EventJoinPITIssue(
                        row_index=idx,
                        event_id=row.event_id,
                        issue_type="used_before_publish",
                        severity=SignalLevel.CRITICAL,
                        message=(
                            f"used_in_trade_time={used_time.isoformat()} before "
                            f"publish_time={publish_time.isoformat()}."
                        ),
                    )
                )

            if event.effective_time is not None:
                effective_time = _ensure_utc(event.effective_time)
                if effective_time > used_time:
                    issues.append(
                        EventJoinPITIssue(
                            row_index=idx,
                            event_id=row.event_id,
                            issue_type="used_before_effective",
                            severity=SignalLevel.CRITICAL,
                            message=(
                                f"used_in_trade_time={used_time.isoformat()} before "
                                f"effective_time={effective_time.isoformat()}."
                            ),
                        )
                    )
                if effective_time < publish_time:
                    issues.append(
                        EventJoinPITIssue(
                            row_index=idx,
                            event_id=row.event_id,
                            issue_type="effective_before_publish",
                            severity=SignalLevel.WARNING,
                            message=(
                                f"effective_time={effective_time.isoformat()} earlier than "
                                f"publish_time={publish_time.isoformat()}."
                            ),
                        )
                    )

        passed = not any(i.severity == SignalLevel.CRITICAL for i in issues)
        return EventJoinPITValidationResult(
            passed=passed,
            checked_rows=len(req.rows),
            issues=issues,
        )

    def build_feature_points(
        self,
        symbol: str,
        trade_dates: list[date],
        lookback_days: int = 30,
        decay_half_life_days: float = 7.0,
    ) -> list[EventFeaturePoint]:
        if not trade_dates:
            return []
        unique_dates = sorted(set(trade_dates))
        events = self._load_symbol_events(symbol=symbol, trade_dates=unique_dates, lookback_days=lookback_days)
        return self._build_points_from_events(
            trade_dates=unique_dates,
            events=events,
            lookback_days=lookback_days,
            decay_half_life_days=decay_half_life_days,
        )

    def enrich_bars(
        self,
        symbol: str,
        bars: pd.DataFrame,
        lookback_days: int = 30,
        decay_half_life_days: float = 7.0,
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        if bars.empty:
            return bars, {"events_loaded": 0, "trade_rows": 0}
        out = bars.copy()
        if "trade_date" not in out.columns:
            out["event_score"] = 0.0
            out["negative_event_score"] = 0.0
            out["event_count"] = 0
            return out, {"events_loaded": 0, "trade_rows": len(out)}

        date_keys = pd.to_datetime(out["trade_date"], errors="coerce").dt.date
        trade_dates = sorted(set(date_keys.dropna().tolist()))
        events = self._load_symbol_events(
            symbol=symbol,
            trade_dates=trade_dates,
            lookback_days=lookback_days,
        )
        points = self._build_points_from_events(
            trade_dates=trade_dates,
            events=events,
            lookback_days=lookback_days,
            decay_half_life_days=decay_half_life_days,
        )
        point_df = pd.DataFrame(
            [
                {
                    "_trade_date_key": p.trade_date,
                    "event_score": p.event_score,
                    "negative_event_score": p.negative_event_score,
                    "event_count": p.event_count,
                }
                for p in points
            ]
        )

        out["_trade_date_key"] = date_keys
        if not point_df.empty:
            out = out.merge(point_df, how="left", on="_trade_date_key")
        if "event_score" not in out.columns:
            out["event_score"] = 0.0
        if "negative_event_score" not in out.columns:
            out["negative_event_score"] = 0.0
        if "event_count" not in out.columns:
            out["event_count"] = 0
        out["event_score"] = out["event_score"].fillna(0.0).astype(float)
        out["negative_event_score"] = out["negative_event_score"].fillna(0.0).astype(float)
        out["event_count"] = out["event_count"].fillna(0).astype(int)
        out = out.drop(columns=["_trade_date_key"])
        return out, {"events_loaded": len(events), "trade_rows": len(out)}

    def preview_features(self, req: EventFeaturePreviewRequest) -> EventFeaturePreviewResult:
        trade_dates: list[date] = []
        cursor = req.start_date
        while cursor <= req.end_date:
            trade_dates.append(cursor)
            cursor += timedelta(days=1)
        points = self.build_feature_points(
            symbol=req.symbol,
            trade_dates=trade_dates,
            lookback_days=req.lookback_days,
            decay_half_life_days=req.decay_half_life_days,
        )
        return EventFeaturePreviewResult(symbol=req.symbol, points=points)

    def _load_symbol_events(self, symbol: str, trade_dates: list[date], lookback_days: int) -> list[EventRecord]:
        if not trade_dates:
            return []
        min_date = trade_dates[0]
        max_date = trade_dates[-1]
        start_time = datetime.combine(min_date - timedelta(days=lookback_days), time.min, tzinfo=timezone.utc)
        end_time = datetime.combine(max_date, time.max, tzinfo=timezone.utc)
        return self.store.list_symbol_events_between(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
            limit=50000,
        )

    @staticmethod
    def _build_points_from_events(
        trade_dates: list[date],
        events: list[EventRecord],
        lookback_days: int,
        decay_half_life_days: float,
    ) -> list[EventFeaturePoint]:
        points: list[EventFeaturePoint] = []
        if decay_half_life_days <= 0:
            decay_half_life_days = 1.0
        decay_lambda = math.log(2) / decay_half_life_days

        for trade_day in trade_dates:
            as_of = datetime.combine(trade_day, time.max, tzinfo=timezone.utc)
            window_start = as_of - timedelta(days=lookback_days)
            positive = 0.0
            negative = 0.0
            event_count = 0
            positive_count = 0
            negative_count = 0
            for event in events:
                publish_time = _ensure_utc(event.publish_time)
                if publish_time < window_start or publish_time > as_of:
                    continue
                event_count += 1
                age_days = max(0.0, (as_of - publish_time).total_seconds() / 86400.0)
                decay = math.exp(-decay_lambda * age_days)
                base = max(0.0, min(1.0, event.score)) * max(0.0, min(1.0, event.confidence)) * decay
                if event.polarity == EventPolarity.POSITIVE:
                    positive += base
                    positive_count += 1
                elif event.polarity == EventPolarity.NEGATIVE:
                    negative += base
                    negative_count += 1
            points.append(
                EventFeaturePoint(
                    trade_date=trade_day,
                    event_score=round(min(1.0, positive), 6),
                    negative_event_score=round(min(1.0, negative), 6),
                    event_count=event_count,
                    positive_event_count=positive_count,
                    negative_event_count=negative_count,
                )
            )
        return points
