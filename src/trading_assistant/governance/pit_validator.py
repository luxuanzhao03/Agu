from __future__ import annotations

from datetime import date

import pandas as pd

from trading_assistant.core.models import EventPITValidationRequest, PITValidationIssue, PITValidationResult, SignalLevel


class PITValidator:
    """
    Point-in-time guard rails.
    This validator focuses on generic anti-lookahead checks for tabular market/event datasets.
    """

    def validate_bars(self, symbol: str, provider: str, bars: pd.DataFrame, as_of: date | None = None) -> PITValidationResult:
        issues: list[PITValidationIssue] = []
        if bars.empty:
            issues.append(
                PITValidationIssue(
                    issue_type="empty_dataset",
                    severity=SignalLevel.CRITICAL,
                    message="Dataset is empty.",
                )
            )
            return PITValidationResult(symbol=symbol, provider=provider, passed=False, issues=issues)

        if "trade_date" not in bars.columns:
            issues.append(
                PITValidationIssue(
                    issue_type="missing_trade_date",
                    severity=SignalLevel.CRITICAL,
                    message="trade_date column is required for PIT validation.",
                )
            )
            return PITValidationResult(symbol=symbol, provider=provider, passed=False, issues=issues)

        series = pd.to_datetime(bars["trade_date"], errors="coerce")
        if series.isna().any():
            issues.append(
                PITValidationIssue(
                    issue_type="invalid_trade_date",
                    severity=SignalLevel.CRITICAL,
                    message="trade_date contains unparsable values.",
                )
            )

        if series.duplicated().any():
            dup = int(series.duplicated().sum())
            issues.append(
                PITValidationIssue(
                    issue_type="duplicate_trade_date",
                    severity=SignalLevel.WARNING,
                    message=f"Found {dup} duplicated trade_date rows.",
                )
            )

        if len(series) >= 2 and not series.is_monotonic_increasing:
            issues.append(
                PITValidationIssue(
                    issue_type="non_monotonic_trade_date",
                    severity=SignalLevel.CRITICAL,
                    message="trade_date is not monotonic increasing.",
                )
            )

        if as_of is not None:
            future_count = int((series.dt.date > as_of).sum())
            if future_count > 0:
                issues.append(
                    PITValidationIssue(
                        issue_type="future_row_detected",
                        severity=SignalLevel.CRITICAL,
                        message=f"Found {future_count} rows after as_of date {as_of.isoformat()}.",
                    )
                )

        # Optional event-style check if columns exist.
        if {"announce_date", "trade_date"}.issubset(set(bars.columns)):
            announce = pd.to_datetime(bars["announce_date"], errors="coerce")
            invalid = int((announce > series).sum())
            if invalid > 0:
                issues.append(
                    PITValidationIssue(
                        issue_type="announce_after_trade_date",
                        severity=SignalLevel.CRITICAL,
                        message=f"Found {invalid} rows where announce_date > trade_date.",
                    )
                )

        passed = not any(i.severity == SignalLevel.CRITICAL for i in issues)
        return PITValidationResult(symbol=symbol, provider=provider, passed=passed, issues=issues)

    def validate_event_rows(self, req: EventPITValidationRequest) -> PITValidationResult:
        issues: list[PITValidationIssue] = []
        for row in req.rows:
            if row.effective_time is not None and row.effective_time < row.event_time:
                issues.append(
                    PITValidationIssue(
                        issue_type="effective_before_event",
                        severity=SignalLevel.CRITICAL,
                        message=f"event_id={row.event_id}: effective_time earlier than event_time.",
                    )
                )
            if row.used_in_trade_time is not None and row.used_in_trade_time < row.event_time:
                issues.append(
                    PITValidationIssue(
                        issue_type="used_before_event",
                        severity=SignalLevel.CRITICAL,
                        message=f"event_id={row.event_id}: used_in_trade_time earlier than event_time.",
                    )
                )
            if row.effective_time and row.used_in_trade_time and row.used_in_trade_time < row.effective_time:
                issues.append(
                    PITValidationIssue(
                        issue_type="used_before_effective",
                        severity=SignalLevel.WARNING,
                        message=f"event_id={row.event_id}: used_in_trade_time earlier than effective_time.",
                    )
                )
        passed = not any(i.severity == SignalLevel.CRITICAL for i in issues)
        return PITValidationResult(symbol=req.symbol, provider="event_rows", passed=passed, issues=issues)
