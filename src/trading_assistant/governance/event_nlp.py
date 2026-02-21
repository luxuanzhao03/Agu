from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone
from typing import Callable
from zoneinfo import ZoneInfo

from trading_assistant.core.models import (
    AnnouncementRawRecord,
    EventNLPScoreResult,
    EventNLPTagScore,
    EventNLPRule,
    EventNormalizedRecord,
    EventNormalizePreviewRequest,
    EventNormalizePreviewResult,
    EventPolarity,
    EventRecordCreate,
)


def default_event_nlp_ruleset() -> tuple[str, list[EventNLPRule]]:
    return (
        "builtin-v1",
        [
            EventNLPRule(
                rule_id="share_buyback",
                event_type="share_buyback",
                polarity=EventPolarity.POSITIVE,
                weight=0.9,
                tag="buyback",
                patterns=[r"buyback", r"回购", r"增持计划", r"员工持股"],
            ),
            EventNLPRule(
                rule_id="earnings_preannounce_positive",
                event_type="earnings_preannounce_positive",
                polarity=EventPolarity.POSITIVE,
                weight=0.85,
                tag="earnings_up",
                patterns=[
                    r"earnings beat",
                    r"profit growth",
                    r"guidance up",
                    r"业绩预增",
                    r"利润增长",
                    r"超预期",
                ],
            ),
            EventNLPRule(
                rule_id="major_contract",
                event_type="major_contract",
                polarity=EventPolarity.POSITIVE,
                weight=0.78,
                tag="contract",
                patterns=[r"major contract", r"winning bid", r"new order", r"重大合同", r"中标", r"订单"],
            ),
            EventNLPRule(
                rule_id="policy_positive",
                event_type="policy_positive",
                polarity=EventPolarity.POSITIVE,
                weight=0.7,
                tag="policy_tailwind",
                patterns=[r"policy support", r"subsidy", r"tax incentive", r"政策支持", r"补贴", r"税收优惠"],
            ),
            EventNLPRule(
                rule_id="regulatory_investigation",
                event_type="regulatory_investigation",
                polarity=EventPolarity.NEGATIVE,
                weight=0.92,
                tag="investigation",
                patterns=[
                    r"investigation",
                    r"regulatory inquiry",
                    r"penalty",
                    r"立案调查",
                    r"处罚",
                    r"问询函",
                ],
            ),
            EventNLPRule(
                rule_id="earnings_warning",
                event_type="earnings_warning",
                polarity=EventPolarity.NEGATIVE,
                weight=0.88,
                tag="earnings_down",
                patterns=[r"earnings warning", r"profit drop", r"guidance down", r"业绩预亏", r"利润下滑", r"减值"],
            ),
            EventNLPRule(
                rule_id="shareholder_reduction",
                event_type="shareholder_reduction",
                polarity=EventPolarity.NEGATIVE,
                weight=0.8,
                tag="reduction",
                patterns=[r"share reduction", r"stake sale", r"margin call", r"减持", r"平仓风险", r"清仓"],
            ),
            EventNLPRule(
                rule_id="delist_or_st_risk",
                event_type="delist_or_st_risk",
                polarity=EventPolarity.NEGATIVE,
                weight=0.95,
                tag="delist_risk",
                patterns=[r"delist", r"special treatment", r"listing termination", r"退市", r"风险警示", r"\*st"],
            ),
            EventNLPRule(
                rule_id="litigation_or_default",
                event_type="litigation_or_default",
                polarity=EventPolarity.NEGATIVE,
                weight=0.75,
                tag="litigation",
                patterns=[r"lawsuit", r"arbitration", r"default", r"诉讼", r"仲裁", r"违约"],
            ),
        ],
    )


class EventNLPScorer:
    def __init__(
        self,
        *,
        rules: list[EventNLPRule] | None = None,
        version: str | None = None,
    ) -> None:
        default_version, default_rules = default_event_nlp_ruleset()
        self.version = version or default_version
        self._rules = [r.model_copy(deep=True) for r in (rules or default_rules)]

    @property
    def rules(self) -> list[EventNLPRule]:
        return [r.model_copy(deep=True) for r in self._rules]

    def set_ruleset(self, *, version: str, rules: list[EventNLPRule]) -> None:
        if not rules:
            raise ValueError("rules must not be empty")
        self.version = version
        self._rules = [r.model_copy(deep=True) for r in rules]

    def score(
        self,
        *,
        title: str,
        summary: str,
        content: str,
        source_reliability_score: float = 0.7,
    ) -> EventNLPScoreResult:
        merged = " ".join([title or "", summary or "", content or ""]).strip().lower()
        if not merged:
            merged = (title or summary or content or "").strip().lower()

        matched_rules: list[str] = []
        per_tag: dict[str, float] = {}
        per_tag_terms: dict[str, set[str]] = {}
        type_scores: dict[str, float] = {}
        positive_score = 0.0
        negative_score = 0.0

        for rule in self._rules:
            local_hits: list[str] = []
            for pattern in rule.patterns:
                if re.search(pattern, merged, flags=re.IGNORECASE):
                    local_hits.append(pattern)
            if not local_hits:
                continue

            hit_bonus = min(0.2, 0.05 * max(0, len(local_hits) - 1))
            weighted = min(1.0, rule.weight + hit_bonus)
            type_scores[rule.event_type] = max(type_scores.get(rule.event_type, 0.0), weighted)
            matched_rules.append(rule.rule_id)
            per_tag[rule.tag] = max(per_tag.get(rule.tag, 0.0), weighted)
            per_tag_terms.setdefault(rule.tag, set()).update(local_hits)

            if rule.polarity == EventPolarity.POSITIVE:
                positive_score += weighted
            elif rule.polarity == EventPolarity.NEGATIVE:
                negative_score += weighted

        dominance = positive_score - negative_score
        if dominance > 0.08:
            polarity = EventPolarity.POSITIVE
        elif dominance < -0.08:
            polarity = EventPolarity.NEGATIVE
        else:
            polarity = EventPolarity.NEUTRAL

        event_type = max(type_scores.items(), key=lambda x: x[1])[0] if type_scores else "generic_announcement"

        source_rel = max(0.0, min(1.0, source_reliability_score))
        text_strength = min(1.0, len(merged) / 160.0)
        base = 0.5 + 0.32 * max(-1.0, min(1.0, dominance))
        if polarity == EventPolarity.NEGATIVE:
            # Keep score as intensity rather than directional expected return.
            base = 0.5 + 0.32 * max(-1.0, min(1.0, -dominance))
        score = max(0.05, min(1.0, base))
        confidence = min(0.99, 0.35 + 0.25 * source_rel + 0.15 * text_strength + 0.1 * len(type_scores))

        tag_scores = [
            EventNLPTagScore(
                tag=tag,
                weight=round(weight, 6),
                matched_terms=sorted(per_tag_terms.get(tag, set())),
            )
            for tag, weight in sorted(per_tag.items(), key=lambda x: x[1], reverse=True)
        ]
        tags = [t.tag for t in tag_scores]
        rationale = (
            f"rules={len(matched_rules)}, pos={positive_score:.3f}, neg={negative_score:.3f}, "
            f"source_rel={source_rel:.2f}, text_strength={text_strength:.2f}"
        )

        return EventNLPScoreResult(
            event_type=event_type,
            polarity=polarity,
            score=round(score, 6),
            confidence=round(confidence, 6),
            ruleset_version=self.version,
            tags=tags,
            matched_rules=sorted(set(matched_rules)),
            tag_scores=tag_scores,
            rationale=rationale,
        )


class EventStandardizer:
    def __init__(
        self,
        scorer: EventNLPScorer | None = None,
        *,
        active_ruleset_loader: Callable[[], tuple[str, list[EventNLPRule]] | None] | None = None,
        refresh_interval_seconds: int = 15,
    ) -> None:
        self.scorer = scorer or EventNLPScorer()
        self._active_ruleset_loader = active_ruleset_loader
        self._refresh_interval_seconds = max(1, int(refresh_interval_seconds))
        self._last_ruleset_refresh_ts = 0.0

    def normalize_preview(self, req: EventNormalizePreviewRequest) -> EventNormalizePreviewResult:
        self._refresh_ruleset_if_needed(force=False)
        normalized: list[EventNormalizedRecord] = []
        errors: list[str] = []
        dropped = 0
        for idx, row in enumerate(req.records):
            try:
                event, nlp, warning = self.normalize_record(
                    row=row,
                    source_name=req.source_name,
                    default_symbol=req.default_symbol,
                    default_timezone=req.default_timezone,
                    source_reliability_score=req.source_reliability_score,
                )
                normalized.append(
                    EventNormalizedRecord(
                        row_index=idx,
                        event=event,
                        nlp=nlp,
                        warning=warning,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                dropped += 1
                errors.append(f"idx={idx}: {exc}")
        return EventNormalizePreviewResult(
            source_name=req.source_name,
            normalized=normalized,
            dropped=dropped,
            errors=errors,
        )

    def normalize_record(
        self,
        *,
        row: AnnouncementRawRecord,
        source_name: str,
        default_symbol: str | None,
        default_timezone: str,
        source_reliability_score: float,
    ) -> tuple[EventRecordCreate, EventNLPScoreResult, str | None]:
        self._refresh_ruleset_if_needed(force=False)

        symbol = self._normalize_symbol(row.symbol or row.ts_code or default_symbol)
        if not symbol:
            raise ValueError("symbol is missing and cannot be inferred")

        publish_time = self._resolve_publish_time(
            publish_time=row.publish_time,
            publish_time_text=row.publish_time_text,
            default_timezone=default_timezone,
        )
        nlp = self.scorer.score(
            title=row.title,
            summary=row.summary,
            content=row.content,
            source_reliability_score=source_reliability_score,
        )
        event_id = row.source_event_id or self._build_event_id(
            source_name=source_name,
            symbol=symbol,
            publish_time=publish_time,
            title=row.title,
            summary=row.summary,
        )

        warning: str | None = None
        if not row.source_event_id:
            warning = "source_event_id missing, generated deterministic event_id hash."

        summary = row.summary.strip()
        if not summary:
            summary = row.content.strip()[:220]

        event = EventRecordCreate(
            event_id=event_id,
            symbol=symbol,
            event_type=nlp.event_type,
            publish_time=publish_time,
            polarity=nlp.polarity,
            score=nlp.score,
            confidence=nlp.confidence,
            title=row.title.strip()[:120],
            summary=summary,
            raw_ref=row.url,
            tags=nlp.tags,
            metadata={
                "source_event_id": row.source_event_id,
                "publish_time_text": row.publish_time_text,
                "nlp_ruleset_version": nlp.ruleset_version,
                "nlp_rationale": nlp.rationale,
                "matched_rules": ",".join(nlp.matched_rules[:12]),
                "tag_count": len(nlp.tags),
            }
            | row.metadata,
        )
        return event, nlp, warning

    def _refresh_ruleset_if_needed(self, *, force: bool = False) -> None:
        if self._active_ruleset_loader is None:
            return
        now_ts = time.time()
        if not force and (now_ts - self._last_ruleset_refresh_ts) < self._refresh_interval_seconds:
            return
        self._last_ruleset_refresh_ts = now_ts

        try:
            loaded = self._active_ruleset_loader()
        except Exception:  # noqa: BLE001
            return
        if not loaded:
            return
        version, rules = loaded
        if not version or not rules:
            return
        if version != self.scorer.version:
            self.scorer.set_ruleset(version=version, rules=rules)

    @staticmethod
    def _normalize_symbol(raw: str | None) -> str | None:
        if not raw:
            return None
        text = raw.strip().upper()
        if "." in text:
            text = text.split(".", 1)[0]
        digits = "".join(ch for ch in text if ch.isdigit())
        if len(digits) >= 6:
            return digits[:6]
        return digits if digits else None

    @staticmethod
    def _resolve_publish_time(
        *,
        publish_time: datetime | None,
        publish_time_text: str | None,
        default_timezone: str,
    ) -> datetime:
        try:
            tz = ZoneInfo(default_timezone)
        except Exception:  # noqa: BLE001
            tz = timezone.utc
        if publish_time is not None:
            if publish_time.tzinfo is None:
                return publish_time.replace(tzinfo=timezone.utc)
            return publish_time.astimezone(timezone.utc)
        if publish_time_text:
            raw = publish_time_text.strip()
            known_formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%Y/%m/%d %H:%M:%S",
                "%Y/%m/%d %H:%M",
                "%Y/%m/%d",
            ]
            for fmt in known_formats:
                try:
                    parsed = datetime.strptime(raw, fmt)
                    return parsed.replace(tzinfo=tz).astimezone(timezone.utc)
                except ValueError:
                    continue
            try:
                parsed = datetime.fromisoformat(raw)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=tz)
                return parsed.astimezone(timezone.utc)
            except ValueError as exc:  # noqa: PERF203
                raise ValueError(f"publish_time_text parse failed: {raw}") from exc
        raise ValueError("publish_time or publish_time_text must be provided")

    @staticmethod
    def _build_event_id(
        *,
        source_name: str,
        symbol: str,
        publish_time: datetime,
        title: str,
        summary: str,
    ) -> str:
        raw = f"{source_name}|{symbol}|{publish_time.isoformat()}|{title}|{summary}".encode("utf-8")
        digest = hashlib.sha1(raw).hexdigest()[:20]
        return f"{source_name}-{digest}"
