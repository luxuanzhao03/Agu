from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timedelta, timezone
import json
import math

from trading_assistant.core.models import (
    EventFeatureBacktestCompareRequest,
    EventNLPContributionWindow,
    EventNLPDriftAlert,
    EventNLPDriftCheckRequest,
    EventNLPDriftCheckResult,
    EventNLPDriftMonitorPoint,
    EventNLPDriftMonitorSummary,
    EventNLPDriftSnapshotRecord,
    EventNLPFeedbackRecord,
    EventNLPFeedbackSummary,
    EventNLPFeedbackUpsertRequest,
    EventNLPRulesetActivateRequest,
    EventNLPRulesetRecord,
    EventNLPRulesetUpsertRequest,
    EventNLPWindowMetrics,
    SignalLevel,
)
from trading_assistant.governance.event_feature_compare import EventFeatureBacktestCompareService
from trading_assistant.governance.event_nlp_store import EventNLPStore


class EventNLPGovernanceService:
    def __init__(
        self,
        *,
        store: EventNLPStore,
        feature_compare: EventFeatureBacktestCompareService | None = None,
    ) -> None:
        self.store = store
        self.feature_compare = feature_compare

    def upsert_ruleset(self, req: EventNLPRulesetUpsertRequest) -> int:
        return self.store.upsert_ruleset(req)

    def activate_ruleset(self, req: EventNLPRulesetActivateRequest) -> bool:
        return self.store.activate_ruleset(req)

    def list_rulesets(self, limit: int = 50, include_rules: bool = False) -> list[EventNLPRulesetRecord]:
        return self.store.list_rulesets(limit=limit, include_rules=include_rules)

    def get_active_ruleset(self, include_rules: bool = True) -> EventNLPRulesetRecord | None:
        return self.store.get_active_ruleset(include_rules=include_rules)

    def list_drift_snapshots(self, source_name: str | None = None, limit: int = 200) -> list[EventNLPDriftSnapshotRecord]:
        return self.store.list_drift_snapshots(source_name=source_name, limit=limit)

    def drift_monitor(self, *, source_name: str | None = None, limit: int = 30) -> EventNLPDriftMonitorSummary:
        snapshots = self.list_drift_snapshots(source_name=source_name, limit=max(3, min(limit, 365)))
        if not snapshots:
            return EventNLPDriftMonitorSummary(
                generated_at=datetime.now(timezone.utc),
                source_name=source_name,
                window_size=0,
                points=[],
            )

        points: list[EventNLPDriftMonitorPoint] = []
        warning_snapshots = 0
        critical_snapshots = 0
        # Snapshot listing is latest-first; monitoring points should be oldest->latest.
        for snapshot in reversed(snapshots):
            warning_alerts = sum(1 for a in snapshot.alerts if a.severity == SignalLevel.WARNING)
            critical_alerts = sum(1 for a in snapshot.alerts if a.severity == SignalLevel.CRITICAL)
            if warning_alerts > 0:
                warning_snapshots += 1
            if critical_alerts > 0:
                critical_snapshots += 1
            points.append(
                EventNLPDriftMonitorPoint(
                    snapshot_id=snapshot.id,
                    created_at=snapshot.created_at,
                    ruleset_version=snapshot.ruleset_version,
                    hit_rate_delta=snapshot.hit_rate_delta,
                    score_p50_delta=snapshot.score_p50_delta,
                    contribution_delta=snapshot.contribution_delta,
                    feedback_polarity_accuracy_delta=snapshot.feedback_polarity_accuracy_delta,
                    feedback_event_type_accuracy_delta=snapshot.feedback_event_type_accuracy_delta,
                    warning_alerts=warning_alerts,
                    critical_alerts=critical_alerts,
                )
            )

        latest = points[-1]
        latest_level = SignalLevel.INFO
        if latest.critical_alerts > 0 or critical_snapshots >= 2:
            latest_level = SignalLevel.CRITICAL
        elif latest.warning_alerts > 0 or warning_snapshots >= 3:
            latest_level = SignalLevel.WARNING

        first = points[0]
        return EventNLPDriftMonitorSummary(
            generated_at=datetime.now(timezone.utc),
            source_name=source_name,
            window_size=len(points),
            latest_snapshot_id=latest.snapshot_id,
            latest_ruleset_version=latest.ruleset_version,
            latest_risk_level=latest_level,
            warning_alert_snapshots=warning_snapshots,
            critical_alert_snapshots=critical_snapshots,
            hit_rate_delta_trend=round(latest.hit_rate_delta - first.hit_rate_delta, 6),
            score_p50_delta_trend=round(latest.score_p50_delta - first.score_p50_delta, 6),
            contribution_delta_trend=self._optional_delta_trend(latest.contribution_delta, first.contribution_delta),
            feedback_polarity_accuracy_delta_trend=self._optional_delta_trend(
                latest.feedback_polarity_accuracy_delta,
                first.feedback_polarity_accuracy_delta,
            ),
            feedback_event_type_accuracy_delta_trend=self._optional_delta_trend(
                latest.feedback_event_type_accuracy_delta,
                first.feedback_event_type_accuracy_delta,
            ),
            points=points,
        )

    def upsert_feedback(self, req: EventNLPFeedbackUpsertRequest) -> int:
        return self.store.upsert_feedback(req)

    def list_feedback(
        self,
        *,
        source_name: str | None = None,
        labeler: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 200,
    ) -> list[EventNLPFeedbackRecord]:
        return self.store.list_feedback(
            source_name=source_name,
            labeler=labeler,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )

    def feedback_summary(
        self,
        *,
        source_name: str | None,
        start_date: date,
        end_date: date,
    ) -> EventNLPFeedbackSummary:
        rows = self.store.load_feedback_rows_for_metrics(
            source_name=source_name,
            start_date=start_date,
            end_date=end_date,
        )
        if not rows:
            return EventNLPFeedbackSummary(
                source_name=source_name,
                start_date=start_date,
                end_date=end_date,
                sample_size=0,
            )

        sample_size = len(rows)
        polarity_hit = 0
        event_type_hit = 0
        score_abs_errors: list[float] = []
        mismatch_counter: Counter[str] = Counter()

        for row in rows:
            pred_pol = str(row["predicted_polarity"])
            label_pol = str(row["label_polarity"])
            if pred_pol == label_pol:
                polarity_hit += 1

            pred_type = str(row["predicted_event_type"])
            label_type = str(row["label_event_type"])
            if pred_type == label_type:
                event_type_hit += 1
            else:
                mismatch_counter[f"{pred_type}->{label_type}"] += 1

            label_score = row["label_score"]
            if label_score is not None:
                score_abs_errors.append(abs(float(row["predicted_score"]) - float(label_score)))

        return EventNLPFeedbackSummary(
            source_name=source_name,
            start_date=start_date,
            end_date=end_date,
            sample_size=sample_size,
            polarity_accuracy=round(polarity_hit / sample_size, 6),
            event_type_accuracy=round(event_type_hit / sample_size, 6),
            score_mae=round(sum(score_abs_errors) / len(score_abs_errors), 6) if score_abs_errors else None,
            top_mismatches=dict(mismatch_counter.most_common(8)),
        )

    def drift_check(self, req: EventNLPDriftCheckRequest) -> EventNLPDriftCheckResult:
        baseline_start, baseline_end = self._resolve_baseline(req)
        active_ruleset = self.get_active_ruleset(include_rules=False)
        fallback_version = active_ruleset.version if active_ruleset else "builtin-v1"

        current = self._window_metrics(
            source_name=req.source_name,
            start_date=req.current_start,
            end_date=req.current_end,
            fallback_ruleset_version=fallback_version,
        )
        baseline = self._window_metrics(
            source_name=req.source_name,
            start_date=baseline_start,
            end_date=baseline_end,
            fallback_ruleset_version=fallback_version,
        )

        ruleset_version = current.ruleset_version if current.ruleset_version != "unknown" else fallback_version
        hit_rate_delta = round(current.hit_rate - baseline.hit_rate, 6)
        score_p50_delta = round(current.score_p50 - baseline.score_p50, 6)

        alerts: list[EventNLPDriftAlert] = []
        if hit_rate_delta <= -req.thresholds.hit_rate_drop_critical:
            alerts.append(
                EventNLPDriftAlert(
                    severity=SignalLevel.CRITICAL,
                    metric="hit_rate",
                    message="NLP hit rate dropped beyond critical threshold.",
                    current=current.hit_rate,
                    baseline=baseline.hit_rate,
                    delta=hit_rate_delta,
                )
            )
        elif hit_rate_delta <= -req.thresholds.hit_rate_drop_warning:
            alerts.append(
                EventNLPDriftAlert(
                    severity=SignalLevel.WARNING,
                    metric="hit_rate",
                    message="NLP hit rate dropped beyond warning threshold.",
                    current=current.hit_rate,
                    baseline=baseline.hit_rate,
                    delta=hit_rate_delta,
                )
            )

        score_shift = abs(score_p50_delta)
        if score_shift >= req.thresholds.score_p50_shift_critical:
            alerts.append(
                EventNLPDriftAlert(
                    severity=SignalLevel.CRITICAL,
                    metric="score_p50_shift",
                    message="NLP score p50 shifted beyond critical threshold.",
                    current=current.score_p50,
                    baseline=baseline.score_p50,
                    delta=score_p50_delta,
                )
            )
        elif score_shift >= req.thresholds.score_p50_shift_warning:
            alerts.append(
                EventNLPDriftAlert(
                    severity=SignalLevel.WARNING,
                    metric="score_p50_shift",
                    message="NLP score p50 shifted beyond warning threshold.",
                    current=current.score_p50,
                    baseline=baseline.score_p50,
                    delta=score_p50_delta,
                )
            )

        contribution_current = None
        contribution_baseline = None
        contribution_delta = None
        feedback_current = None
        feedback_baseline = None
        feedback_polarity_accuracy_delta = None
        feedback_event_type_accuracy_delta = None
        if req.include_contribution and self.feature_compare is not None:
            try:
                contribution_current = self._contribution_window(
                    symbol=req.contribution_symbol,
                    strategy_name=req.contribution_strategy_name,
                    start_date=req.current_start,
                    end_date=req.current_end,
                    req=req,
                )
                contribution_baseline = self._contribution_window(
                    symbol=req.contribution_symbol,
                    strategy_name=req.contribution_strategy_name,
                    start_date=baseline_start,
                    end_date=baseline_end,
                    req=req,
                )
                contribution_delta = round(
                    contribution_current.total_return_delta - contribution_baseline.total_return_delta,
                    6,
                )
                if contribution_delta <= -req.thresholds.contribution_drop_critical:
                    alerts.append(
                        EventNLPDriftAlert(
                            severity=SignalLevel.CRITICAL,
                            metric="contribution_total_return_delta",
                            message="Event feature backtest contribution dropped beyond critical threshold.",
                            current=contribution_current.total_return_delta,
                            baseline=contribution_baseline.total_return_delta,
                            delta=contribution_delta,
                        )
                    )
                elif contribution_delta <= -req.thresholds.contribution_drop_warning:
                    alerts.append(
                        EventNLPDriftAlert(
                            severity=SignalLevel.WARNING,
                            metric="contribution_total_return_delta",
                            message="Event feature backtest contribution dropped beyond warning threshold.",
                            current=contribution_current.total_return_delta,
                            baseline=contribution_baseline.total_return_delta,
                            delta=contribution_delta,
                        )
                    )
            except Exception as exc:  # noqa: BLE001
                alerts.append(
                    EventNLPDriftAlert(
                        severity=SignalLevel.WARNING,
                        metric="contribution_total_return_delta",
                        message=f"Contribution compare skipped: {exc}",
                    )
                )

        if req.include_feedback_quality:
            feedback_current = self.feedback_summary(
                source_name=req.source_name,
                start_date=req.current_start,
                end_date=req.current_end,
            )
            feedback_baseline = self.feedback_summary(
                source_name=req.source_name,
                start_date=baseline_start,
                end_date=baseline_end,
            )
            enough_feedback = (
                feedback_current.sample_size >= req.feedback_min_samples
                and feedback_baseline.sample_size >= req.feedback_min_samples
            )
            if enough_feedback:
                feedback_polarity_accuracy_delta = round(
                    feedback_current.polarity_accuracy - feedback_baseline.polarity_accuracy,
                    6,
                )
                feedback_event_type_accuracy_delta = round(
                    feedback_current.event_type_accuracy - feedback_baseline.event_type_accuracy,
                    6,
                )
                if feedback_polarity_accuracy_delta <= -req.thresholds.feedback_polarity_accuracy_drop_critical:
                    alerts.append(
                        EventNLPDriftAlert(
                            severity=SignalLevel.CRITICAL,
                            metric="feedback_polarity_accuracy",
                            message="Labeled polarity accuracy dropped beyond critical threshold.",
                            current=feedback_current.polarity_accuracy,
                            baseline=feedback_baseline.polarity_accuracy,
                            delta=feedback_polarity_accuracy_delta,
                        )
                    )
                elif feedback_polarity_accuracy_delta <= -req.thresholds.feedback_polarity_accuracy_drop_warning:
                    alerts.append(
                        EventNLPDriftAlert(
                            severity=SignalLevel.WARNING,
                            metric="feedback_polarity_accuracy",
                            message="Labeled polarity accuracy dropped beyond warning threshold.",
                            current=feedback_current.polarity_accuracy,
                            baseline=feedback_baseline.polarity_accuracy,
                            delta=feedback_polarity_accuracy_delta,
                        )
                    )

                if feedback_event_type_accuracy_delta <= -req.thresholds.feedback_event_type_accuracy_drop_critical:
                    alerts.append(
                        EventNLPDriftAlert(
                            severity=SignalLevel.CRITICAL,
                            metric="feedback_event_type_accuracy",
                            message="Labeled event-type accuracy dropped beyond critical threshold.",
                            current=feedback_current.event_type_accuracy,
                            baseline=feedback_baseline.event_type_accuracy,
                            delta=feedback_event_type_accuracy_delta,
                        )
                    )
                elif feedback_event_type_accuracy_delta <= -req.thresholds.feedback_event_type_accuracy_drop_warning:
                    alerts.append(
                        EventNLPDriftAlert(
                            severity=SignalLevel.WARNING,
                            metric="feedback_event_type_accuracy",
                            message="Labeled event-type accuracy dropped beyond warning threshold.",
                            current=feedback_current.event_type_accuracy,
                            baseline=feedback_baseline.event_type_accuracy,
                            delta=feedback_event_type_accuracy_delta,
                        )
                    )

        result = EventNLPDriftCheckResult(
            generated_at=datetime.now(timezone.utc),
            source_name=req.source_name,
            ruleset_version=ruleset_version,
            current=current,
            baseline=baseline,
            hit_rate_delta=hit_rate_delta,
            score_p50_delta=score_p50_delta,
            contribution_current=contribution_current,
            contribution_baseline=contribution_baseline,
            contribution_delta=contribution_delta,
            feedback_current=feedback_current,
            feedback_baseline=feedback_baseline,
            feedback_polarity_accuracy_delta=feedback_polarity_accuracy_delta,
            feedback_event_type_accuracy_delta=feedback_event_type_accuracy_delta,
            alerts=alerts,
        )

        if req.save_snapshot:
            snapshot_id = self.store.insert_drift_snapshot(
                source_name=req.source_name,
                ruleset_version=ruleset_version,
                current_start=req.current_start,
                current_end=req.current_end,
                baseline_start=baseline_start,
                baseline_end=baseline_end,
                current=current,
                baseline=baseline,
                hit_rate_delta=hit_rate_delta,
                score_p50_delta=score_p50_delta,
                contribution_delta=contribution_delta,
                feedback_polarity_accuracy_delta=feedback_polarity_accuracy_delta,
                feedback_event_type_accuracy_delta=feedback_event_type_accuracy_delta,
                alerts=alerts,
                payload=result.model_dump(mode="json"),
            )
            result.snapshot_id = snapshot_id

        return result

    def _window_metrics(
        self,
        *,
        source_name: str | None,
        start_date: date,
        end_date: date,
        fallback_ruleset_version: str,
    ) -> EventNLPWindowMetrics:
        rows = self.store.load_event_rows_for_metrics(
            source_name=source_name,
            start_date=start_date,
            end_date=end_date,
        )
        sample_size = len(rows)
        if sample_size == 0:
            return EventNLPWindowMetrics(
                source_name=source_name,
                ruleset_version=fallback_ruleset_version,
                sample_size=0,
            )

        event_type_counter: Counter[str] = Counter()
        ruleset_counter: Counter[str] = Counter()
        scores: list[float] = []
        hit_count = 0
        positive = 0
        negative = 0
        neutral = 0

        for row in rows:
            event_type = str(row["event_type"])
            event_type_counter[event_type] += 1
            polarity = str(row["polarity"])
            if polarity == "POSITIVE":
                positive += 1
            elif polarity == "NEGATIVE":
                negative += 1
            else:
                neutral += 1

            score = float(row["score"])
            if math.isfinite(score):
                scores.append(score)

            metadata = self._safe_json_dict(str(row["metadata"]))
            version = str(metadata.get("nlp_ruleset_version") or "").strip()
            if version:
                ruleset_counter[version] += 1
            matched_raw = str(metadata.get("matched_rules") or "").strip()
            matched_rules = [x.strip() for x in matched_raw.split(",") if x.strip()]
            if matched_rules and event_type != "generic_announcement":
                hit_count += 1

        if not scores:
            scores = [0.0]
        scores_sorted = sorted(scores)

        ruleset_version = "unknown"
        if ruleset_counter:
            ruleset_version = ruleset_counter.most_common(1)[0][0]
        if ruleset_version == "unknown":
            ruleset_version = fallback_ruleset_version

        top_event_types = dict(event_type_counter.most_common(8))
        return EventNLPWindowMetrics(
            source_name=source_name,
            ruleset_version=ruleset_version,
            sample_size=sample_size,
            hit_count=hit_count,
            hit_rate=round(hit_count / sample_size, 6),
            score_mean=round(sum(scores_sorted) / len(scores_sorted), 6),
            score_p10=round(self._quantile(scores_sorted, 0.1), 6),
            score_p50=round(self._quantile(scores_sorted, 0.5), 6),
            score_p90=round(self._quantile(scores_sorted, 0.9), 6),
            positive_ratio=round(positive / sample_size, 6),
            negative_ratio=round(negative / sample_size, 6),
            neutral_ratio=round(neutral / sample_size, 6),
            top_event_types=top_event_types,
        )

    def _contribution_window(
        self,
        *,
        symbol: str,
        strategy_name: str,
        start_date: date,
        end_date: date,
        req: EventNLPDriftCheckRequest,
    ) -> EventNLPContributionWindow:
        if self.feature_compare is None:
            raise RuntimeError("event feature compare service is not configured")
        compare = self.feature_compare.compare(
            EventFeatureBacktestCompareRequest(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                strategy_name=strategy_name,
                strategy_params=req.contribution_strategy_params,
                event_lookback_days=req.contribution_event_lookback_days,
                event_decay_half_life_days=req.contribution_event_decay_half_life_days,
                initial_cash=req.contribution_initial_cash,
                commission_rate=req.contribution_commission_rate,
                slippage_rate=req.contribution_slippage_rate,
                lot_size=req.contribution_lot_size,
                max_single_position=req.contribution_max_single_position,
                save_report=False,
                watermark="NLP drift monitor",
            )
        )
        return EventNLPContributionWindow(
            symbol=symbol,
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            total_return_delta=compare.delta.total_return_delta,
            sharpe_delta=compare.delta.sharpe_delta,
            event_row_ratio=compare.diagnostics.event_row_ratio,
            events_loaded=compare.diagnostics.events_loaded,
        )

    @staticmethod
    def _resolve_baseline(req: EventNLPDriftCheckRequest) -> tuple[date, date]:
        if req.baseline_start is not None and req.baseline_end is not None:
            return req.baseline_start, req.baseline_end
        days = max(1, (req.current_end - req.current_start).days + 1)
        baseline_end = req.current_start - timedelta(days=1)
        baseline_start = baseline_end - timedelta(days=days - 1)
        return baseline_start, baseline_end

    @staticmethod
    def _quantile(values_sorted: list[float], q: float) -> float:
        if not values_sorted:
            return 0.0
        if len(values_sorted) == 1:
            return float(values_sorted[0])
        q = max(0.0, min(1.0, q))
        pos = (len(values_sorted) - 1) * q
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            return float(values_sorted[lo])
        ratio = pos - lo
        return float(values_sorted[lo] + (values_sorted[hi] - values_sorted[lo]) * ratio)

    @staticmethod
    def _optional_delta_trend(latest: float | None, first: float | None) -> float | None:
        if latest is None or first is None:
            return None
        return round(latest - first, 6)

    @staticmethod
    def _safe_json_dict(raw: str) -> dict:
        try:
            value = json.loads(raw)
            if isinstance(value, dict):
                return value
        except Exception:  # noqa: BLE001
            pass
        return {}
