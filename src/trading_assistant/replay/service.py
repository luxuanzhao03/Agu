from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, timezone
from statistics import fmean, pstdev

from trading_assistant.core.models import (
    CostModelCalibrationRecord,
    CostModelCalibrationRequest,
    CostModelCalibrationResult,
    ExecutionAttributionBucket,
    ExecutionAttributionItem,
    ExecutionAttributionReport,
    ExecutionRecordCreate,
    ExecutionReplayItem,
    ExecutionReplayReport,
    SignalAction,
    SignalDecisionRecord,
    SignalLevel,
)
from trading_assistant.replay.store import ReplayStore


class ReplayService:
    def __init__(self, store: ReplayStore) -> None:
        self.store = store

    def record_signal(self, record: SignalDecisionRecord) -> str:
        return self.store.record_signal(record)

    def record_execution(self, record: ExecutionRecordCreate) -> int:
        if not self.store.signal_exists(record.signal_id):
            raise KeyError(f"signal_id '{record.signal_id}' not found")
        return self.store.record_execution(record)

    def list_signals(self, symbol: str | None = None, limit: int = 200) -> list[SignalDecisionRecord]:
        return self.store.list_signals(symbol=symbol, limit=limit)

    def report(
        self,
        symbol: str | None = None,
        strategy_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> ExecutionReplayReport:
        rows = self.store.load_pairs(
            symbol=symbol,
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        items: list[ExecutionReplayItem] = []
        followed_count = 0
        slippage_sum = 0.0
        slippage_count = 0
        delay_sum = 0
        delay_count = 0

        for row in rows:
            signal_action = self.store.parse_action(str(row["signal_action"]))
            if signal_action is None:
                continue
            executed_action = self.store.parse_action(str(row["executed_action"])) if row["executed_action"] else None
            confidence = float(row["confidence"])
            quantity = int(row["quantity"] or 0)
            executed_price = float(row["price"] or 0.0)
            reference_price = float(row["reference_price"] or 0.0)
            signal_date = datetime.fromisoformat(str(row["trade_date"])).date()
            execution_date = (
                datetime.fromisoformat(str(row["execution_date"])).date() if row["execution_date"] else signal_date
            )

            followed = signal_action == executed_action and quantity > 0
            if followed:
                followed_count += 1

            slippage_bps = 0.0
            slippage_available = False
            if quantity > 0 and reference_price > 0 and executed_action is not None:
                slippage_available = True
                if executed_action == SignalAction.BUY:
                    slippage_bps = (executed_price - reference_price) / reference_price * 10000.0
                elif executed_action == SignalAction.SELL:
                    slippage_bps = (reference_price - executed_price) / reference_price * 10000.0
                else:
                    slippage_bps = 0.0
                slippage_sum += slippage_bps
                slippage_count += 1

            delay_days = max(0, (execution_date - signal_date).days) if quantity > 0 else 0
            if quantity > 0:
                delay_sum += delay_days
                delay_count += 1

            items.append(
                ExecutionReplayItem(
                    signal_id=str(row["signal_id"]),
                    symbol=str(row["symbol"]),
                    strategy_name=str(row["strategy_name"]),
                    signal_date=signal_date,
                    execution_date=execution_date,
                    signal_action=signal_action,
                    executed_action=executed_action,
                    signal_confidence=confidence,
                    executed_quantity=quantity,
                    executed_price=executed_price,
                    slippage_bps=round(slippage_bps, 6),
                    slippage_available=slippage_available,
                    followed=followed,
                    delay_days=delay_days,
                )
            )

        total = len(items)
        follow_rate = 0.0 if total == 0 else followed_count / total
        avg_slippage_bps = 0.0 if slippage_count == 0 else slippage_sum / slippage_count
        avg_delay_days = 0.0 if delay_count == 0 else delay_sum / delay_count
        return ExecutionReplayReport(
            items=items,
            follow_rate=round(follow_rate, 6),
            avg_slippage_bps=round(avg_slippage_bps, 6),
            avg_delay_days=round(avg_delay_days, 6),
        )

    def attribution(
        self,
        symbol: str | None = None,
        strategy_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> ExecutionAttributionReport:
        replay = self.report(
            symbol=symbol,
            strategy_name=strategy_name,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        reason_counts: dict[str, int] = {}
        reason_cost_bps: dict[str, float] = {}
        items: list[ExecutionAttributionItem] = []

        def _add(
            reason_code: str,
            item: ExecutionReplayItem,
            detail: str,
            suggestion: str,
            severity: SignalLevel,
            drag_bps: float,
        ) -> None:
            reason_counts[reason_code] = reason_counts.get(reason_code, 0) + 1
            reason_cost_bps[reason_code] = reason_cost_bps.get(reason_code, 0.0) + max(0.0, float(drag_bps))
            items.append(
                ExecutionAttributionItem(
                    signal_id=item.signal_id,
                    symbol=item.symbol,
                    strategy_name=item.strategy_name,
                    reason_code=reason_code,
                    severity=severity,
                    detail=detail,
                    estimated_drag_bps=round(max(0.0, float(drag_bps)), 4),
                    suggestion=suggestion,
                )
            )

        for item in replay.items:
            if item.executed_quantity <= 0:
                no_exec_drag = max(8.0, (item.signal_confidence - 0.5) * 180.0)
                _add(
                    "NO_EXECUTION",
                    item,
                    "Signal has no manual execution record.",
                    "Increase execution coverage and prioritize high-confidence signals first.",
                    SignalLevel.WARNING,
                    no_exec_drag,
                )
                continue
            if not item.followed:
                mismatch_drag = 15.0 + max(0.0, (item.signal_confidence - 0.5) * 60.0)
                _add(
                    "ACTION_MISMATCH",
                    item,
                    f"Signal={item.signal_action.value}, execution={item.executed_action.value if item.executed_action else 'NONE'}.",
                    "Review execution SOP and the action mapping table to avoid BUY/SELL reversals.",
                    SignalLevel.WARNING,
                    mismatch_drag,
                )
            if item.delay_days >= 2:
                delay_drag = float(item.delay_days) * 6.0
                _add(
                    "EXECUTION_DELAY",
                    item,
                    f"Execution delayed by {item.delay_days} days.",
                    "Shorten manual approval latency or tune strategy holding period for delay tolerance.",
                    SignalLevel.WARNING,
                    delay_drag,
                )
            if item.slippage_available and abs(float(item.slippage_bps)) >= 35:
                slip_drag = max(0.0, float(item.slippage_bps))
                _add(
                    "HIGH_SLIPPAGE",
                    item,
                    f"Observed slippage={item.slippage_bps:.1f} bps.",
                    "Increase liquidity threshold and raise slippage/impact settings in backtest assumptions.",
                    SignalLevel.WARNING,
                    slip_drag,
                )

        suggestions: list[str] = []
        total = max(1, len(replay.items))
        no_exec_ratio = reason_counts.get("NO_EXECUTION", 0) / total
        mismatch_ratio = reason_counts.get("ACTION_MISMATCH", 0) / total
        delay_ratio = reason_counts.get("EXECUTION_DELAY", 0) / total
        if no_exec_ratio >= 0.30:
            suggestions.append(
                "Execution coverage is low: reduce signal density and follow high-confidence orders first."
            )
        if mismatch_ratio >= 0.15:
            suggestions.append(
                "Action mismatch is elevated: tighten decision thresholds and avoid borderline action flips."
            )
        if delay_ratio >= 0.20 or replay.avg_delay_days >= 1.0:
            suggestions.append(
                "Execution delay is elevated: increase holding horizon tolerance and cut short-cycle turnover."
            )
        if replay.follow_rate <= 0.60:
            suggestions.append(
                "Follow rate is weak: lower concurrent positions or raise per-symbol confidence threshold."
            )
        if not suggestions:
            suggestions.append(
                "Execution deviation is controlled: keep parameters stable and review with weekly rolling checks."
            )

        reason_rates = {
            key: round(value / max(1, len(replay.items)), 6) for key, value in sorted(reason_counts.items())
        }
        reason_cost_bps = {key: round(value, 6) for key, value in sorted(reason_cost_bps.items())}
        total_drag_bps = round(sum(reason_cost_bps.values()), 6)
        avg_drag_bps = round(total_drag_bps / max(1, len(replay.items)), 6)
        top_symbols = self._top_buckets(replay.items, key_fn=lambda x: x.symbol)
        top_strategies = self._top_buckets(replay.items, key_fn=lambda x: x.strategy_name)

        return ExecutionAttributionReport(
            sample_size=len(replay.items),
            follow_rate=replay.follow_rate,
            avg_delay_days=replay.avg_delay_days,
            avg_slippage_bps=replay.avg_slippage_bps,
            reason_counts=reason_counts,
            reason_rates=reason_rates,
            reason_cost_bps=reason_cost_bps,
            estimated_total_drag_bps=total_drag_bps,
            estimated_avg_drag_bps=avg_drag_bps,
            suggestions=suggestions,
            top_symbols=top_symbols,
            top_strategies=top_strategies,
            items=items,
        )

    def calibrate_cost_model(self, req: CostModelCalibrationRequest) -> CostModelCalibrationResult:
        replay = self.report(
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
        )
        executed = [item for item in replay.items if item.executed_quantity > 0]
        slippage_values = [abs(item.slippage_bps) for item in executed if item.slippage_available]
        signed_slippage = [item.slippage_bps for item in executed if item.slippage_available]

        sample_size = len(replay.items)
        executed_samples = len(executed)
        slippage_coverage = (len(slippage_values) / executed_samples) if executed_samples > 0 else 0.0
        median_abs_slippage_bps = self._quantile(slippage_values, 0.50)
        p90_abs_slippage_bps = self._quantile(slippage_values, 0.90)
        avg_slippage_bps = fmean(signed_slippage) if signed_slippage else 0.0
        no_exec_ratio = sum(1 for item in replay.items if item.executed_quantity <= 0) / max(1, sample_size)

        sample_weight = min(1.0, sample_size / max(1, req.min_samples))
        data_weight = sample_weight * slippage_coverage

        data_slippage_rate = max(
            0.0001,
            min(
                0.02,
                (p90_abs_slippage_bps / 10000.0) * 0.65 + (median_abs_slippage_bps / 10000.0) * 0.35,
            ),
        )
        recommended_slippage_rate = self._blend(0.0005, data_slippage_rate, data_weight)

        tail_ratio = p90_abs_slippage_bps / max(1e-6, median_abs_slippage_bps) if slippage_values else 1.0
        data_impact_coeff = max(0.05, min(1.5, 0.10 + max(0.0, tail_ratio - 1.0) * 0.12))
        recommended_impact_coeff = self._blend(0.18, data_impact_coeff, data_weight)

        data_fill_probability_floor = max(
            0.02,
            min(
                0.35,
                0.02 + 0.55 * no_exec_ratio + 0.08 * max(0.0, 1.0 - replay.follow_rate),
            ),
        )
        recommended_fill_probability_floor = self._blend(0.02, data_fill_probability_floor, sample_weight)

        stability = 0.25
        if slippage_values:
            dispersion = pstdev(slippage_values) if len(slippage_values) > 1 else 0.0
            stability = max(0.0, 1.0 - min(1.0, dispersion / max(1.0, p90_abs_slippage_bps)))
        confidence = max(
            0.0,
            min(
                1.0,
                0.50 * sample_weight + 0.25 * slippage_coverage + 0.25 * stability,
            ),
        )

        notes: list[str] = []
        if sample_size < req.min_samples:
            notes.append(f"Sample size {sample_size} is below min_samples={req.min_samples}; keep conservative blending.")
        if slippage_coverage < 0.40:
            notes.append("Slippage coverage is low; fill execution reference_price to improve calibration quality.")
        if no_exec_ratio >= 0.30:
            notes.append("No-execution ratio is high; prioritize execution coverage before tightening alpha thresholds.")
        if p90_abs_slippage_bps >= 60:
            notes.append("High tail slippage detected; increase impact coefficient and reduce participation per order.")
        if not notes:
            notes.append("Calibration quality is acceptable; monitor drift with weekly rolling recalibration.")

        result = CostModelCalibrationResult(
            generated_at=datetime.now(timezone.utc),
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            sample_size=sample_size,
            executed_samples=executed_samples,
            slippage_coverage=round(slippage_coverage, 6),
            follow_rate=replay.follow_rate,
            avg_delay_days=replay.avg_delay_days,
            avg_slippage_bps=round(avg_slippage_bps, 6),
            p90_abs_slippage_bps=round(p90_abs_slippage_bps, 6),
            recommended_slippage_rate=round(recommended_slippage_rate, 6),
            recommended_impact_cost_coeff=round(recommended_impact_coeff, 6),
            recommended_fill_probability_floor=round(recommended_fill_probability_floor, 6),
            confidence=round(confidence, 6),
            notes=notes,
        )
        if req.save_record:
            calibration_id = self.store.save_cost_calibration(result)
            result = result.model_copy(update={"calibration_id": calibration_id})
        return result

    def list_cost_calibrations(self, symbol: str | None = None, limit: int = 30) -> list[CostModelCalibrationRecord]:
        return self.store.list_cost_calibrations(symbol=symbol, limit=limit)

    def _top_buckets(
        self,
        items: list[ExecutionReplayItem],
        key_fn: Callable[[ExecutionReplayItem], str],
        top_n: int = 8,
    ) -> list[ExecutionAttributionBucket]:
        groups: dict[str, list[ExecutionReplayItem]] = {}
        for item in items:
            key = str(key_fn(item) or "UNKNOWN")
            groups.setdefault(key, []).append(item)

        buckets: list[ExecutionAttributionBucket] = []
        for key, group in groups.items():
            total = len(group)
            follow_rate = sum(1 for x in group if x.followed) / max(1, total)
            avg_delay_days = sum(float(x.delay_days) for x in group) / max(1, total)
            slippage_vals = [float(x.slippage_bps) for x in group if x.slippage_available]
            avg_slippage_bps = fmean(slippage_vals) if slippage_vals else 0.0
            deviation_score = (1.0 - follow_rate) * 55.0 + avg_delay_days * 8.0 + max(0.0, avg_slippage_bps) / 12.0
            buckets.append(
                ExecutionAttributionBucket(
                    key=key,
                    sample_size=total,
                    follow_rate=round(follow_rate, 6),
                    avg_delay_days=round(avg_delay_days, 6),
                    avg_slippage_bps=round(avg_slippage_bps, 6),
                    deviation_score=round(deviation_score, 6),
                )
            )
        buckets.sort(key=lambda x: (x.deviation_score, x.sample_size), reverse=True)
        return buckets[: max(1, min(top_n, 20))]

    @staticmethod
    def _quantile(values: list[float], q: float) -> float:
        if not values:
            return 0.0
        sorted_vals = sorted(float(v) for v in values)
        if len(sorted_vals) == 1:
            return sorted_vals[0]
        qq = max(0.0, min(1.0, float(q)))
        pos = qq * (len(sorted_vals) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(sorted_vals) - 1)
        frac = pos - lo
        return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac

    @staticmethod
    def _blend(base: float, data: float, weight: float) -> float:
        w = max(0.0, min(1.0, float(weight)))
        return max(0.0, base * (1.0 - w) + data * w)
