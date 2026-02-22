from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import DataQualityIssue, DataQualityReport, DataQualityRequest, SignalLevel


class DataQualityService:
    def evaluate(self, req: DataQualityRequest, bars: pd.DataFrame, provider: str) -> DataQualityReport:
        issues: list[DataQualityIssue] = []
        field_scores: dict[str, float] = {}

        if bars.empty:
            issues.append(
                DataQualityIssue(
                    issue_type="empty_dataset",
                    severity=SignalLevel.CRITICAL,
                    message="No rows returned for requested date range.",
                )
            )
            field_scores = {field: 0.0 for field in req.required_fields}
            return DataQualityReport(
                symbol=req.symbol,
                provider=provider,
                row_count=0,
                issues=issues,
                passed=False,
                field_scores=field_scores,
                overall_score=0.0,
            )

        missing_cols = [col for col in req.required_fields if col not in bars.columns]
        if missing_cols:
            issues.append(
                DataQualityIssue(
                    issue_type="missing_columns",
                    severity=SignalLevel.CRITICAL,
                    message=f"Missing required columns: {', '.join(missing_cols)}",
                )
            )
        row_count = max(1, len(bars))
        for field in req.required_fields:
            if field not in bars.columns:
                field_scores[field] = 0.0
                continue
            score = 1.0
            null_ratio = float(bars[field].isna().sum()) / float(row_count)
            score -= min(1.0, null_ratio)
            if field in {"open", "high", "low", "close", "volume", "amount"}:
                numeric = pd.to_numeric(bars[field], errors="coerce")
                invalid_ratio = float(numeric.isna().sum()) / float(row_count)
                score -= 0.5 * min(1.0, invalid_ratio)
                if field in {"open", "high", "low", "close", "volume", "amount"}:
                    non_positive_ratio = float((numeric <= 0).sum()) / float(row_count)
                    score -= 0.3 * min(1.0, non_positive_ratio)
            field_scores[field] = round(max(0.0, min(1.0, score)), 6)

        if "trade_date" in bars.columns:
            dup_count = int(bars["trade_date"].duplicated().sum())
            if dup_count > 0:
                issues.append(
                    DataQualityIssue(
                        issue_type="duplicate_trade_date",
                        severity=SignalLevel.WARNING,
                        message=f"Found {dup_count} duplicated trade_date rows.",
                    )
                )

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col not in bars.columns:
                continue
            null_count = int(bars[col].isna().sum())
            if null_count > 0:
                issues.append(
                    DataQualityIssue(
                        issue_type=f"null_{col}",
                        severity=SignalLevel.WARNING,
                        message=f"Column {col} has {null_count} null values.",
                    )
                )

        if {"high", "low"}.issubset(set(bars.columns)):
            invalid_hl = int((bars["high"] < bars["low"]).sum())
            if invalid_hl > 0:
                issues.append(
                    DataQualityIssue(
                        issue_type="invalid_high_low",
                        severity=SignalLevel.CRITICAL,
                        message=f"Found {invalid_hl} rows with high < low.",
                    )
                )

        passed = not any(issue.severity == SignalLevel.CRITICAL for issue in issues)
        overall = 0.0 if not field_scores else float(sum(field_scores.values()) / len(field_scores))
        return DataQualityReport(
            symbol=req.symbol,
            provider=provider,
            row_count=len(bars),
            issues=issues,
            passed=passed,
            field_scores=field_scores,
            overall_score=round(overall, 6),
        )
