from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from statistics import fmean

from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import (
    CostModelCalibrationRequest,
    GoLiveChecklistItem,
    GoLiveGateCheck,
    GoLiveReadinessReport,
    GoLiveRollbackRule,
    HoldingRecommendationAction,
    ManualHoldingRecommendationSnapshot,
    ManualHoldingSide,
    ReportGenerateRequest,
    ReportGenerateResult,
    SignalLevel,
    StrategyAccuracyBucket,
    StrategyAccuracyPoint,
    StrategyAccuracyReport,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.holdings.store import HoldingStore
from trading_assistant.replay.service import ReplayService


class ReportingService:
    _EXEC_REQUIRED_ACTIONS: set[HoldingRecommendationAction] = {
        HoldingRecommendationAction.ADD,
        HoldingRecommendationAction.BUY_NEW,
        HoldingRecommendationAction.REDUCE,
        HoldingRecommendationAction.EXIT,
    }

    def __init__(
        self,
        replay: ReplayService,
        audit: AuditService,
        output_dir: str = "reports",
        provider: CompositeDataProvider | None = None,
        holding_store: HoldingStore | None = None,
    ) -> None:
        self.replay = replay
        self.audit = audit
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.provider = provider
        self.holding_store = holding_store

    def generate(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        if req.report_type == "replay":
            return self._build_replay_report(req)
        if req.report_type == "risk":
            return self._build_risk_report(req)
        if req.report_type == "closure":
            return self._build_closure_report(req)
        if req.report_type == "cost_model":
            return self._build_cost_model_report(req)
        return self._build_signal_report(req)

    def _build_signal_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        events = self.audit.query(event_type="signal_generation", limit=req.limit)
        lines = [
            "# Signal Generation Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            "",
            "## Recent Runs",
        ]
        for e in events:
            if req.symbol and str(e.payload.get("symbol")) != req.symbol:
                continue
            lines.append(
                f"- {e.event_time.isoformat()} | symbol={e.payload.get('symbol')} "
                f"strategy={e.payload.get('strategy')} signals={e.payload.get('signals')}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("signal", content, req.save_to_file)
        return ReportGenerateResult(title="Signal Generation Report", content=content, saved_path=path)

    def _build_replay_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        report = self.replay.report(
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
        )
        lines = [
            "# Execution Replay Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            f"- Strategy: {req.strategy_name or 'ALL'}",
            f"- Follow Rate: {report.follow_rate:.2%}",
            f"- Avg Slippage (bps): {report.avg_slippage_bps:.2f}",
            f"- Avg Delay (days): {report.avg_delay_days:.2f}",
            "",
            "## Items",
        ]
        for item in report.items:
            lines.append(
                f"- {item.signal_id} | {item.symbol} | signal={item.signal_action.value} "
                f"exec={item.executed_action.value if item.executed_action else 'NONE'} "
                f"qty={item.executed_quantity} delay={item.delay_days}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("replay", content, req.save_to_file)
        return ReportGenerateResult(title="Execution Replay Report", content=content, saved_path=path)

    def _build_risk_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        events = self.audit.query(event_type="risk_check", limit=req.limit)
        lines = [
            "# Risk Check Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            "",
            "## Recent Risk Checks",
        ]
        for e in events:
            if req.symbol and str(e.payload.get("symbol")) != req.symbol:
                continue
            lines.append(
                f"- {e.event_time.isoformat()} | symbol={e.payload.get('symbol')} "
                f"action={e.payload.get('action')} blocked={e.payload.get('blocked')}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("risk", content, req.save_to_file)
        return ReportGenerateResult(title="Risk Check Report", content=content, saved_path=path)

    def _build_closure_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        attribution = self.replay.attribution(
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            limit=req.limit,
        )
        lines = [
            "# Execution Closure Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            f"- Strategy: {req.strategy_name or 'ALL'}",
            f"- Samples: {attribution.sample_size}",
            f"- Follow Rate: {attribution.follow_rate:.2%}",
            f"- Avg Delay (days): {attribution.avg_delay_days:.2f}",
            f"- Avg Slippage (bps): {attribution.avg_slippage_bps:.2f}",
            f"- Estimated Total Drag (bps): {attribution.estimated_total_drag_bps:.2f}",
            f"- Estimated Avg Drag / Signal (bps): {attribution.estimated_avg_drag_bps:.2f}",
            "",
            "## Deviation Reasons",
        ]
        if attribution.reason_counts:
            for key, value in sorted(attribution.reason_counts.items(), key=lambda x: x[1], reverse=True):
                ratio = attribution.reason_rates.get(key, 0.0)
                drag = attribution.reason_cost_bps.get(key, 0.0)
                lines.append(f"- {key}: count={value}, ratio={ratio:.2%}, drag_bps={drag:.2f}")
        else:
            lines.append("- No major execution deviations.")
        lines.extend(["", "## Top Symbol Deviation"])
        if attribution.top_symbols:
            for bucket in attribution.top_symbols:
                lines.append(
                    f"- {bucket.key}: samples={bucket.sample_size}, follow={bucket.follow_rate:.2%}, "
                    f"delay={bucket.avg_delay_days:.2f}, slip={bucket.avg_slippage_bps:.2f}, "
                    f"deviation_score={bucket.deviation_score:.2f}"
                )
        else:
            lines.append("- No symbol buckets.")
        lines.extend(["", "## Top Strategy Deviation"])
        if attribution.top_strategies:
            for bucket in attribution.top_strategies:
                lines.append(
                    f"- {bucket.key}: samples={bucket.sample_size}, follow={bucket.follow_rate:.2%}, "
                    f"delay={bucket.avg_delay_days:.2f}, slip={bucket.avg_slippage_bps:.2f}, "
                    f"deviation_score={bucket.deviation_score:.2f}"
                )
        else:
            lines.append("- No strategy buckets.")
        lines.extend(["", "## Parameter Suggestions"])
        for suggestion in attribution.suggestions:
            lines.append(f"- {suggestion}")
        lines.extend(["", "## Sample Items"])
        for item in attribution.items[: min(100, len(attribution.items))]:
            lines.append(
                f"- {item.signal_id} | {item.symbol} | {item.reason_code} | "
                f"{item.severity.value} | drag={item.estimated_drag_bps:.2f}bps | {item.detail}"
            )
        content = "\n".join(lines)
        path = self._save_if_needed("closure", content, req.save_to_file)
        return ReportGenerateResult(title="Execution Closure Report", content=content, saved_path=path)

    def _build_cost_model_report(self, req: ReportGenerateRequest) -> ReportGenerateResult:
        calibration = self.replay.calibrate_cost_model(
            CostModelCalibrationRequest(
                symbol=req.symbol,
                strategy_name=req.strategy_name,
                start_date=req.start_date,
                end_date=req.end_date,
                limit=req.limit,
                save_record=False,
            )
        )
        lines = [
            "# Cost Model Calibration Report",
            "",
            f"- Generated at: {datetime.now(timezone.utc).isoformat()}",
            f"- Watermark: {req.watermark}",
            f"- Symbol: {req.symbol or 'ALL'}",
            f"- Sample Size: {calibration.sample_size}",
            f"- Executed Samples: {calibration.executed_samples}",
            f"- Slippage Coverage: {calibration.slippage_coverage:.2%}",
            f"- Follow Rate: {calibration.follow_rate:.2%}",
            f"- Avg Delay (days): {calibration.avg_delay_days:.2f}",
            f"- Avg Slippage (bps): {calibration.avg_slippage_bps:.2f}",
            f"- P90 Abs Slippage (bps): {calibration.p90_abs_slippage_bps:.2f}",
            "",
            "## Recommended Parameters",
            f"- slippage_rate: {calibration.recommended_slippage_rate:.6f}",
            f"- impact_cost_coeff: {calibration.recommended_impact_cost_coeff:.6f}",
            f"- fill_probability_floor: {calibration.recommended_fill_probability_floor:.6f}",
            f"- confidence: {calibration.confidence:.2%}",
            f"- calibration_id: {calibration.calibration_id}",
            "",
            "## Notes",
        ]
        for note in calibration.notes:
            lines.append(f"- {note}")
        content = "\n".join(lines)
        path = self._save_if_needed("cost_model", content, req.save_to_file)
        return ReportGenerateResult(title="Cost Model Calibration Report", content=content, saved_path=path)

    def strategy_accuracy(
        self,
        *,
        lookback_days: int = 90,
        end_date: date | None = None,
        strategy_name: str | None = None,
        symbol: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 4000,
    ) -> StrategyAccuracyReport:
        if self.holding_store is None or self.provider is None:
            raise RuntimeError("strategy accuracy report dependencies are not configured")

        lb_days = max(1, min(int(lookback_days), 3650))
        report_end = end_date or datetime.now(timezone.utc).date()
        report_start = report_end - timedelta(days=lb_days - 1)
        strategy_filter = str(strategy_name or "").strip().lower() or None
        symbol_filter = str(symbol or "").strip().upper() or None
        conf_filter = self._bounded(float(min_confidence), 0.0, 1.0)

        snapshots = self.holding_store.list_analysis_recommendations(
            symbol=symbol_filter,
            strategy_name=strategy_filter,
            start_date=report_start,
            end_date=report_end,
            min_confidence=conf_filter,
            limit=max(1, min(int(limit), 20_000)),
        )
        if not snapshots:
            return StrategyAccuracyReport(
                generated_at=datetime.now(timezone.utc),
                start_date=report_start,
                end_date=report_end,
                lookback_days=lb_days,
                strategy_name=strategy_filter,
                symbol=symbol_filter,
                min_confidence=conf_filter,
                notes=[
                    "No holding analysis snapshot found in this window.",
                    "Run /holdings/analyze first, then refresh the strategy accuracy report.",
                ],
            )

        close_maps: dict[str, dict[date, float]] = {}
        grouped_by_symbol: dict[str, list[ManualHoldingRecommendationSnapshot]] = defaultdict(list)
        for item in snapshots:
            grouped_by_symbol[item.symbol].append(item)
        for sym, rows in grouped_by_symbol.items():
            min_date = min((x.as_of_date for x in rows), default=report_start) - timedelta(days=10)
            max_ref = max((x.next_trade_date or x.as_of_date for x in rows), default=report_end) + timedelta(days=10)
            close_maps[sym] = self._load_close_map(symbol=sym, start_date=min_date, end_date=max_ref)

        trade_rows = self.holding_store.list_trades(
            symbol=symbol_filter,
            start_date=report_start,
            end_date=report_end + timedelta(days=10),
            limit=max(2_000, min(int(limit) * 8, 20_000)),
        )
        trade_index: dict[tuple[str, date], list] = defaultdict(list)
        for row in trade_rows:
            trade_index[(row.symbol, row.trade_date)].append(row)

        details: list[StrategyAccuracyPoint] = []
        missing_market_rows = 0
        for item in snapshots:
            close_map = close_maps.get(item.symbol, {})
            as_of_close = close_map.get(item.as_of_date)
            eval_date = self._resolve_eval_date(
                as_of_date=item.as_of_date,
                preferred_date=item.next_trade_date,
                close_map=close_map,
            )
            next_close = close_map.get(eval_date) if eval_date is not None else None
            realized_return = None
            realized_up = None
            direction_hit = None
            brier = None
            return_error = None
            if as_of_close is not None and next_close is not None and as_of_close > 0:
                realized_return = float(next_close / as_of_close - 1.0)
                realized_up = bool(realized_return > 0.0)
                prob = self._bounded(float(item.up_probability), 0.0, 1.0)
                direction_hit = bool((prob >= 0.5) == realized_up)
                outcome = 1.0 if realized_up else 0.0
                brier = float((prob - outcome) ** 2)
                return_error = float(realized_return - float(item.expected_next_day_return))
            else:
                missing_market_rows += 1

            expected_side = self._expected_execution_side(item.action)
            executed = False
            execution_side = None
            execution_price = None
            execution_reference_price = None
            execution_cost_bps = None
            cost_adjusted_action_return = None
            if (
                eval_date is not None
                and item.action in self._EXEC_REQUIRED_ACTIONS
                and expected_side is not None
            ):
                matched = [
                    x
                    for x in trade_index.get((item.symbol, eval_date), [])
                    if x.side == expected_side
                ]
                if matched:
                    total_qty = sum(int(x.quantity or 0) for x in matched)
                    total_notional = sum(float(x.price or 0.0) * int(x.quantity or 0) for x in matched)
                    total_fee = sum(float(x.fee or 0.0) for x in matched)
                    if total_qty > 0 and total_notional > 0:
                        executed = True
                        execution_side = matched[0].side if expected_side is None else expected_side
                        execution_price = float(total_notional / total_qty)
                        ref_notional = 0.0
                        ref_qty = 0
                        for tr in matched:
                            if tr.reference_price is not None and tr.reference_price > 0:
                                qq = int(tr.quantity or 0)
                                ref_notional += float(tr.reference_price) * qq
                                ref_qty += qq
                        if ref_qty > 0:
                            execution_reference_price = float(ref_notional / ref_qty)
                        elif as_of_close is not None and as_of_close > 0:
                            execution_reference_price = float(as_of_close)

                        if execution_reference_price is not None and execution_reference_price > 0:
                            if execution_side == ManualHoldingSide.SELL:
                                slip_bps = (execution_reference_price - execution_price) / execution_reference_price * 10000.0
                            else:
                                slip_bps = (execution_price - execution_reference_price) / execution_reference_price * 10000.0
                            fee_bps = total_fee / max(1e-9, total_notional) * 10000.0
                            execution_cost_bps = float(slip_bps + fee_bps)
                            action_sign = self._action_sign(item.action)
                            if realized_return is not None and action_sign != 0:
                                cost_adjusted_action_return = float(action_sign * realized_return - execution_cost_bps / 10000.0)

            details.append(
                StrategyAccuracyPoint(
                    run_id=item.run_id,
                    generated_at=item.generated_at,
                    as_of_date=item.as_of_date,
                    next_trade_date=eval_date,
                    strategy_name=item.strategy_name,
                    symbol=item.symbol,
                    action=item.action,
                    confidence=float(item.confidence),
                    expected_next_day_return=float(item.expected_next_day_return),
                    up_probability=float(item.up_probability),
                    realized_next_day_return=(
                        round(float(realized_return), 8) if realized_return is not None else None
                    ),
                    realized_up=realized_up,
                    direction_hit=direction_hit,
                    brier_score=(round(float(brier), 8) if brier is not None else None),
                    return_error=(round(float(return_error), 8) if return_error is not None else None),
                    executed=executed,
                    execution_side=execution_side,
                    execution_price=(round(float(execution_price), 6) if execution_price is not None else None),
                    execution_reference_price=(
                        round(float(execution_reference_price), 6)
                        if execution_reference_price is not None
                        else None
                    ),
                    execution_cost_bps=(round(float(execution_cost_bps), 6) if execution_cost_bps is not None else None),
                    cost_adjusted_action_return=(
                        round(float(cost_adjusted_action_return), 8)
                        if cost_adjusted_action_return is not None
                        else None
                    ),
                )
            )

        valid_points = [x for x in details if x.realized_next_day_return is not None]
        overall = self._bucket_from_points(bucket_key="ALL", points=valid_points)
        strategy_buckets = self._group_bucket(points=valid_points, key_fn=lambda x: x.strategy_name)
        symbol_buckets = self._group_bucket(points=valid_points, key_fn=lambda x: x.symbol)
        sorted_details = sorted(
            valid_points,
            key=lambda x: (x.as_of_date, x.generated_at, x.confidence),
            reverse=True,
        )[:300]

        notes: list[str] = []
        if missing_market_rows > 0:
            notes.append(
                f"{missing_market_rows} snapshot rows miss market close mapping and were excluded from realized metrics."
            )
        if overall.actionable_samples > 0 and overall.execution_coverage < 0.6:
            notes.append(
                "Execution coverage is below 60%; fill more manual trades to improve cost-adjusted accuracy diagnostics."
            )
        if overall.sample_size < 30:
            notes.append(
                f"Sample size {overall.sample_size} is small; treat score changes as directional reference only."
            )
        if not notes:
            notes.append("Accuracy metrics are stable; continue weekly monitoring for drift and execution bias.")

        return StrategyAccuracyReport(
            generated_at=datetime.now(timezone.utc),
            start_date=report_start,
            end_date=report_end,
            lookback_days=lb_days,
            strategy_name=strategy_filter,
            symbol=symbol_filter,
            min_confidence=conf_filter,
            sample_size=overall.sample_size,
            actionable_samples=overall.actionable_samples,
            executed_samples=overall.executed_samples,
            execution_coverage=overall.execution_coverage,
            hit_rate=overall.hit_rate,
            brier_score=overall.brier_score,
            expected_return_mean=overall.expected_return_mean,
            realized_return_mean=overall.realized_return_mean,
            return_bias=overall.return_bias,
            return_mae=overall.return_mae,
            cost_bps_mean=overall.cost_bps_mean,
            cost_adjusted_return_mean=overall.cost_adjusted_return_mean,
            by_strategy=strategy_buckets,
            by_symbol=symbol_buckets,
            details=sorted_details,
            notes=notes,
        )

    def go_live_readiness(
        self,
        *,
        lookback_days: int = 90,
        end_date: date | None = None,
        strategy_name: str | None = None,
        symbol: str | None = None,
        min_confidence: float = 0.0,
        limit: int = 4000,
    ) -> GoLiveReadinessReport:
        lb_days = max(1, min(int(lookback_days), 3650))
        report_end = end_date or datetime.now(timezone.utc).date()
        strategy_filter = str(strategy_name or "").strip().lower() or None
        symbol_filter = str(symbol or "").strip().upper() or None
        conf_filter = self._bounded(float(min_confidence), 0.0, 1.0)

        accuracy = self.strategy_accuracy(
            lookback_days=lb_days,
            end_date=report_end,
            strategy_name=strategy_filter,
            symbol=symbol_filter,
            min_confidence=conf_filter,
            limit=limit,
        )

        replay_report = self.replay.report(
            symbol=symbol_filter,
            strategy_name=strategy_filter,
            start_date=accuracy.start_date,
            end_date=accuracy.end_date,
            limit=max(300, min(2000, int(limit))),
        )
        calibrations = self.replay.list_cost_calibrations(symbol=symbol_filter, limit=80)
        matched_calibration = None
        for item in calibrations:
            result = item.result
            if strategy_filter:
                if str(result.strategy_name or "").strip().lower() != strategy_filter:
                    continue
            matched_calibration = item
            break

        challenge_events = self.audit.query(event_type="strategy_challenge", limit=120)
        challenge_cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        latest_challenge = None
        for ev in challenge_events:
            if ev.event_time < challenge_cutoff:
                continue
            payload = dict(ev.payload or {})
            if symbol_filter:
                ev_symbol = str(payload.get("symbol") or "").strip().upper()
                if ev_symbol and ev_symbol != symbol_filter:
                    continue
            latest_challenge = ev
            break

        gate_checks: list[GoLiveGateCheck] = []

        def _add_gate(
            *,
            gate_key: str,
            gate_name: str,
            passed: bool,
            actual_value: float | str | None,
            threshold_value: float | str | None,
            comparator: str,
            detail: str,
            severity: SignalLevel = SignalLevel.WARNING,
        ) -> None:
            gate_checks.append(
                GoLiveGateCheck(
                    gate_key=gate_key,
                    gate_name=gate_name,
                    passed=bool(passed),
                    severity=severity,
                    actual_value=actual_value,
                    threshold_value=threshold_value,
                    comparator=comparator,
                    detail=detail,
                )
            )

        min_samples = 40
        min_hit_rate = 0.55
        max_brier = 0.23
        min_exec_coverage = 0.70
        min_cost_adj = 0.0
        min_follow_rate = 0.65
        max_slippage_bps = 35.0
        max_delay_days = 1.2
        min_calibration_confidence = 0.60
        min_calibration_samples = 60

        _add_gate(
            gate_key="oos_sample_size",
            gate_name="样本外样本数",
            passed=accuracy.sample_size >= min_samples,
            actual_value=accuracy.sample_size,
            threshold_value=min_samples,
            comparator=">=",
            detail="样本不足时，命中率和偏差指标不稳定。",
            severity=SignalLevel.CRITICAL,
        )
        _add_gate(
            gate_key="oos_hit_rate",
            gate_name="样本外命中率",
            passed=accuracy.hit_rate >= min_hit_rate,
            actual_value=round(float(accuracy.hit_rate), 6),
            threshold_value=min_hit_rate,
            comparator=">=",
            detail="命中率低于门槛时不应继续加仓灰度。",
            severity=SignalLevel.CRITICAL,
        )
        brier_val = float(accuracy.brier_score or 1.0)
        _add_gate(
            gate_key="oos_brier_score",
            gate_name="概率校准(Brier)",
            passed=brier_val <= max_brier,
            actual_value=round(brier_val, 6),
            threshold_value=max_brier,
            comparator="<=",
            detail="Brier 越低越好，过高说明概率输出失真。",
            severity=SignalLevel.CRITICAL,
        )
        _add_gate(
            gate_key="execution_coverage",
            gate_name="建议执行覆盖率",
            passed=accuracy.execution_coverage >= min_exec_coverage,
            actual_value=round(float(accuracy.execution_coverage), 6),
            threshold_value=min_exec_coverage,
            comparator=">=",
            detail="回写覆盖率不足会导致评估偏差。",
            severity=SignalLevel.WARNING,
        )
        _add_gate(
            gate_key="cost_adjusted_return",
            gate_name="成本后动作收益",
            passed=accuracy.cost_adjusted_return_mean >= min_cost_adj,
            actual_value=round(float(accuracy.cost_adjusted_return_mean), 8),
            threshold_value=min_cost_adj,
            comparator=">=",
            detail="成本后收益为负时应优先回滚画像。",
            severity=SignalLevel.CRITICAL,
        )
        _add_gate(
            gate_key="replay_follow_rate",
            gate_name="执行跟随率",
            passed=float(replay_report.follow_rate) >= min_follow_rate,
            actual_value=round(float(replay_report.follow_rate), 6),
            threshold_value=min_follow_rate,
            comparator=">=",
            detail="建议与执行偏离过大时，模型评价失效。",
            severity=SignalLevel.WARNING,
        )
        _add_gate(
            gate_key="replay_avg_slippage",
            gate_name="平均滑点(bps)",
            passed=float(replay_report.avg_slippage_bps) <= max_slippage_bps,
            actual_value=round(float(replay_report.avg_slippage_bps), 6),
            threshold_value=max_slippage_bps,
            comparator="<=",
            detail="滑点失真会吞噬全部 alpha。",
            severity=SignalLevel.CRITICAL,
        )
        _add_gate(
            gate_key="replay_avg_delay",
            gate_name="平均执行延迟(日)",
            passed=float(replay_report.avg_delay_days) <= max_delay_days,
            actual_value=round(float(replay_report.avg_delay_days), 6),
            threshold_value=max_delay_days,
            comparator="<=",
            detail="执行延迟偏高时建议降低换手频率。",
            severity=SignalLevel.WARNING,
        )

        if matched_calibration is None:
            _add_gate(
                gate_key="cost_calibration_exists",
                gate_name="成本重估记录",
                passed=False,
                actual_value="missing",
                threshold_value="exists",
                comparator="==",
                detail="缺少近期成本模型重估，成本假设风险较高。",
                severity=SignalLevel.WARNING,
            )
        else:
            cc = matched_calibration.result
            _add_gate(
                gate_key="cost_calibration_confidence",
                gate_name="成本重估置信度",
                passed=float(cc.confidence) >= min_calibration_confidence,
                actual_value=round(float(cc.confidence), 6),
                threshold_value=min_calibration_confidence,
                comparator=">=",
                detail="置信度不足时不应放大仓位。",
                severity=SignalLevel.WARNING,
            )
            _add_gate(
                gate_key="cost_calibration_samples",
                gate_name="成本重估样本数",
                passed=int(cc.sample_size) >= min_calibration_samples,
                actual_value=int(cc.sample_size),
                threshold_value=min_calibration_samples,
                comparator=">=",
                detail="样本太少会导致成本参数抖动。",
                severity=SignalLevel.WARNING,
            )

        if latest_challenge is None:
            _add_gate(
                gate_key="challenge_recent",
                gate_name="30日内挑战赛验证",
                passed=False,
                actual_value="missing",
                threshold_value="recent_pass",
                comparator="==",
                detail="缺少最近挑战赛验证记录。",
                severity=SignalLevel.CRITICAL,
            )
        else:
            payload = dict(latest_challenge.payload or {})
            run_status = str(payload.get("run_status") or "UNKNOWN").strip().upper()
            champion = str(payload.get("champion_strategy") or "").strip().lower()
            strategy_ok = True
            if strategy_filter:
                strategy_ok = champion == strategy_filter
            passed = run_status in {"SUCCESS", "PARTIAL_FAILED"} and bool(champion) and strategy_ok
            _add_gate(
                gate_key="challenge_recent",
                gate_name="30日内挑战赛验证",
                passed=passed,
                actual_value=f"run_status={run_status};champion={champion or '-'}",
                threshold_value=(strategy_filter or "any_champion"),
                comparator="==",
                detail="上线前需要最近窗口挑战赛验证并具备冠军策略。",
                severity=SignalLevel.CRITICAL,
            )

        failed = [x for x in gate_checks if not x.passed]
        failed_critical = [x for x in failed if x.severity == SignalLevel.CRITICAL]
        failed_warning = [x for x in failed if x.severity != SignalLevel.CRITICAL]
        if failed_critical:
            readiness_level = "BLOCKED"
            overall_passed = False
        elif failed_warning:
            readiness_level = "GRAY_READY_WITH_WARNINGS"
            overall_passed = True
        else:
            readiness_level = "GRAY_READY"
            overall_passed = True

        checklist: list[GoLiveChecklistItem] = []
        checklist.append(
            GoLiveChecklistItem(
                item_key="accuracy_refresh",
                item_name="刷新策略准确性看板",
                status=("PASS" if accuracy.sample_size > 0 else "FAIL"),
                detail=f"sample={accuracy.sample_size}, hit_rate={accuracy.hit_rate:.2%}, brier={accuracy.brier_score}",
                evidence=f"/reports/strategy-accuracy?lookback_days={lb_days}",
            )
        )
        today_rows = []
        if self.holding_store is not None:
            today_rows = self.holding_store.list_analysis_recommendations(
                symbol=symbol_filter,
                strategy_name=strategy_filter,
                start_date=report_end,
                end_date=report_end,
                min_confidence=0.0,
                limit=1,
            )
        checklist.append(
            GoLiveChecklistItem(
                item_key="analysis_snapshot_today",
                item_name="当日持仓分析快照",
                status=("PASS" if today_rows else "WARN"),
                detail=("存在当日分析快照" if today_rows else "未检测到当日分析快照"),
                evidence=(today_rows[0].run_id if today_rows else ""),
            )
        )
        checklist.append(
            GoLiveChecklistItem(
                item_key="execution_writeback",
                item_name="执行回写完整性",
                status=("PASS" if accuracy.execution_coverage >= min_exec_coverage else "WARN"),
                detail=f"execution_coverage={accuracy.execution_coverage:.2%}",
                evidence="/holdings/trades + /replay/executions/record",
            )
        )
        if matched_calibration is not None:
            age_days = (datetime.now(timezone.utc) - matched_calibration.created_at).days
            checklist.append(
                GoLiveChecklistItem(
                    item_key="cost_calibration_freshness",
                    item_name="成本模型重估新鲜度",
                    status=("PASS" if age_days <= 14 else "WARN"),
                    detail=f"last_calibration_id={matched_calibration.id}, age_days={age_days}",
                    evidence="/replay/cost-model/calibrations",
                )
            )
        else:
            checklist.append(
                GoLiveChecklistItem(
                    item_key="cost_calibration_freshness",
                    item_name="成本模型重估新鲜度",
                    status="FAIL",
                    detail="未找到成本重估记录",
                    evidence="/replay/cost-model/calibrate",
                )
            )

        rollback_rules = [
            GoLiveRollbackRule(
                trigger_key="loss_streak_3d",
                trigger_name="连续亏损熔断",
                condition="rolling_3d_pnl_days <= -3",
            ),
            GoLiveRollbackRule(
                trigger_key="daily_loss_limit",
                trigger_name="单日最大亏损",
                condition="daily_portfolio_return <= -0.025",
            ),
            GoLiveRollbackRule(
                trigger_key="gray_window_drawdown",
                trigger_name="灰度窗口回撤",
                condition="gray_window_max_drawdown > 0.06",
            ),
            GoLiveRollbackRule(
                trigger_key="execution_quality_drop",
                trigger_name="执行质量恶化",
                condition="execution_coverage_5d < 0.60 OR avg_slippage_bps_5d > 45",
            ),
        ]

        notes: list[str] = [
            "上线准入强调样本外稳定性，不以单段高收益作为唯一标准。",
            "达到 GRAY_READY 后仍需执行 7~20 个交易日灰度窗口，不达标立即回滚。",
        ]
        if latest_challenge is not None:
            payload = dict(latest_challenge.payload or {})
            notes.append(
                f"Latest challenge: run_id={payload.get('run_id')}, champion={payload.get('champion_strategy')}."
            )
        else:
            notes.append("No recent strategy_challenge event in last 30 days.")
        if matched_calibration is not None:
            notes.append(
                f"Latest cost calibration: id={matched_calibration.id}, confidence={matched_calibration.result.confidence:.2%}."
            )

        return GoLiveReadinessReport(
            generated_at=datetime.now(timezone.utc),
            lookback_days=lb_days,
            start_date=accuracy.start_date,
            end_date=accuracy.end_date,
            strategy_name=strategy_filter,
            symbol=symbol_filter,
            overall_passed=overall_passed,
            readiness_level=readiness_level,
            failed_gate_count=len(failed_critical),
            warning_gate_count=len(failed_warning),
            gate_checks=gate_checks,
            rollback_rules=rollback_rules,
            daily_checklist=checklist,
            latest_accuracy=accuracy,
            notes=notes,
        )

    def _load_close_map(self, *, symbol: str, start_date: date, end_date: date) -> dict[date, float]:
        try:
            if hasattr(self.provider, "get_daily_bars_with_source"):
                _, bars = self.provider.get_daily_bars_with_source(symbol, start_date, end_date)
            else:
                bars = self.provider.get_daily_bars(symbol, start_date, end_date)
        except Exception:  # noqa: BLE001
            return {}
        if bars is None or bars.empty:
            return {}
        frame = bars.copy()
        if "trade_date" not in frame.columns or "close" not in frame.columns:
            return {}
        frame["trade_date"] = frame["trade_date"].apply(
            lambda x: x.date() if hasattr(x, "date") else x
        )
        out: dict[date, float] = {}
        for _, row in frame.iterrows():
            try:
                dd = row["trade_date"]
                if isinstance(dd, datetime):
                    key = dd.date()
                elif isinstance(dd, date):
                    key = dd
                else:
                    key = date.fromisoformat(str(dd))
                close = float(row["close"])
            except Exception:  # noqa: BLE001
                continue
            if close > 0:
                out[key] = close
        return out

    @staticmethod
    def _resolve_eval_date(
        *,
        as_of_date: date,
        preferred_date: date | None,
        close_map: dict[date, float],
    ) -> date | None:
        if preferred_date is not None and preferred_date in close_map:
            return preferred_date
        if preferred_date is not None:
            later = sorted([d for d in close_map if d >= preferred_date])
            if later:
                return later[0]
        later = sorted([d for d in close_map if d > as_of_date])
        return later[0] if later else None

    @staticmethod
    def _expected_execution_side(action: HoldingRecommendationAction) -> ManualHoldingSide | None:
        if action in {HoldingRecommendationAction.ADD, HoldingRecommendationAction.BUY_NEW}:
            return ManualHoldingSide.BUY
        if action in {HoldingRecommendationAction.REDUCE, HoldingRecommendationAction.EXIT}:
            return ManualHoldingSide.SELL
        return None

    @staticmethod
    def _action_sign(action: HoldingRecommendationAction) -> int:
        if action in {HoldingRecommendationAction.ADD, HoldingRecommendationAction.BUY_NEW}:
            return 1
        if action in {HoldingRecommendationAction.REDUCE, HoldingRecommendationAction.EXIT}:
            return -1
        return 0

    @staticmethod
    def _bounded(value: float, lo: float, hi: float) -> float:
        return max(lo, min(hi, float(value)))

    def _group_bucket(
        self,
        *,
        points: list[StrategyAccuracyPoint],
        key_fn,
    ) -> list[StrategyAccuracyBucket]:
        groups: dict[str, list[StrategyAccuracyPoint]] = defaultdict(list)
        for item in points:
            key = str(key_fn(item) or "UNKNOWN")
            groups[key].append(item)
        buckets = [self._bucket_from_points(bucket_key=key, points=rows) for key, rows in groups.items()]
        buckets.sort(key=lambda x: (x.sample_size, x.hit_rate, -x.return_mae), reverse=True)
        return buckets

    def _bucket_from_points(
        self,
        *,
        bucket_key: str,
        points: list[StrategyAccuracyPoint],
    ) -> StrategyAccuracyBucket:
        samples = len(points)
        actionable = sum(1 for x in points if x.action in self._EXEC_REQUIRED_ACTIONS)
        executed = sum(1 for x in points if x.executed and x.action in self._EXEC_REQUIRED_ACTIONS)
        hit_vals = [1.0 if bool(x.direction_hit) else 0.0 for x in points if x.direction_hit is not None]
        brier_vals = [float(x.brier_score) for x in points if x.brier_score is not None]
        expected_vals = [float(x.expected_next_day_return) for x in points]
        realized_vals = [float(x.realized_next_day_return) for x in points if x.realized_next_day_return is not None]
        error_vals = [float(x.return_error) for x in points if x.return_error is not None]
        abs_error_vals = [abs(x) for x in error_vals]
        cost_vals = [float(x.execution_cost_bps) for x in points if x.execution_cost_bps is not None]
        cost_adj_vals = [
            float(x.cost_adjusted_action_return)
            for x in points
            if x.cost_adjusted_action_return is not None
        ]

        return StrategyAccuracyBucket(
            bucket_key=bucket_key,
            sample_size=samples,
            actionable_samples=actionable,
            executed_samples=executed,
            execution_coverage=round(executed / actionable, 6) if actionable > 0 else 0.0,
            hit_rate=round(fmean(hit_vals), 6) if hit_vals else 0.0,
            brier_score=round(fmean(brier_vals), 6) if brier_vals else None,
            expected_return_mean=round(fmean(expected_vals), 8) if expected_vals else 0.0,
            realized_return_mean=round(fmean(realized_vals), 8) if realized_vals else 0.0,
            return_bias=round(fmean(error_vals), 8) if error_vals else 0.0,
            return_mae=round(fmean(abs_error_vals), 8) if abs_error_vals else 0.0,
            cost_bps_mean=round(fmean(cost_vals), 6) if cost_vals else 0.0,
            cost_adjusted_return_mean=round(fmean(cost_adj_vals), 8) if cost_adj_vals else 0.0,
        )

    def _save_if_needed(self, prefix: str, content: str, save: bool) -> str | None:
        if not save:
            return None
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        path = self.output_dir / f"{prefix}_report_{ts}.md"
        path.write_text(content, encoding="utf-8")
        return str(path)
