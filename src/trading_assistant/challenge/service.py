from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
import logging
import multiprocessing as mp
from uuid import uuid4

import pandas as pd

from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.backtest.engine import BacktestEngine
from trading_assistant.core.models import (
    AutoTuneApplyScope,
    AutoTuneRunRequest,
    BacktestMetrics,
    BacktestRequest,
    StrategyChallengeRequest,
    StrategyChallengeResult,
    StrategyChallengeRolloutPlan,
    StrategyChallengeRunStatus,
    StrategyChallengeStrategyResult,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.event_service import EventService
from trading_assistant.strategy.registry import StrategyRegistry

logger = logging.getLogger(__name__)


def _run_single_strategy_subprocess(req_payload: dict[str, object], strategy_name: str) -> dict[str, object]:
    from trading_assistant.core.container import get_strategy_challenge_service
    from trading_assistant.core.models import StrategyChallengeRequest

    service = get_strategy_challenge_service()
    req = StrategyChallengeRequest.model_validate(req_payload)
    single_req = req.model_copy(update={"strategy_names": [strategy_name]})
    single_result = service.run(single_req)
    selected = next((item for item in single_result.results if item.strategy_name == strategy_name), None)
    if selected is None and single_result.results:
        selected = single_result.results[0]
    if selected is None:
        selected = StrategyChallengeStrategyResult(
            strategy_name=strategy_name,
            qualified=False,
            qualification_reasons=["runtime_error"],
            ranking_score=None,
            error="no strategy result generated in subprocess",
        )
    return {
        "strategy_name": strategy_name,
        "evaluated_count": int(single_result.evaluated_count),
        "result": selected.model_dump(mode="json"),
    }


class StrategyChallengeService:
    def __init__(
        self,
        *,
        autotune: AutoTuneService,
        provider: CompositeDataProvider,
        backtest_engine: BacktestEngine,
        registry: StrategyRegistry,
        event_service: EventService | None = None,
        fundamental_service: FundamentalService | None = None,
        max_parallel_workers: int = 1,
    ) -> None:
        self.autotune = autotune
        self.provider = provider
        self.backtest_engine = backtest_engine
        self.registry = registry
        self.event_service = event_service
        self.fundamental_service = fundamental_service
        self.max_parallel_workers = max(1, int(max_parallel_workers))

    def run(self, req: StrategyChallengeRequest) -> StrategyChallengeResult:
        strategy_names = self._resolve_strategy_names(req.strategy_names)
        run_id = uuid4().hex

        results, total_evaluated = self._evaluate_strategies(req=req, strategy_names=strategy_names)

        ordered = sorted(results, key=self._result_sort_key, reverse=True)
        qualified = [item for item in ordered if item.qualified]
        champion = qualified[0] if qualified else None
        runner_up = qualified[1] if len(qualified) > 1 else None
        failed_strategies = [item.strategy_name for item in ordered if bool(item.error)]
        error_count = len(failed_strategies)
        if error_count <= 0:
            run_status = StrategyChallengeRunStatus.SUCCESS
        elif error_count >= len(ordered):
            run_status = StrategyChallengeRunStatus.FAILED
        else:
            run_status = StrategyChallengeRunStatus.PARTIAL_FAILED

        return StrategyChallengeResult(
            run_id=run_id,
            generated_at=datetime.now(timezone.utc),
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            strategy_names=strategy_names,
            evaluated_count=total_evaluated,
            qualified_count=len(qualified),
            run_status=run_status,
            error_count=error_count,
            failed_strategies=failed_strategies,
            champion_strategy=(champion.strategy_name if champion is not None else None),
            runner_up_strategy=(runner_up.strategy_name if runner_up is not None else None),
            market_fit_summary=self._build_summary(
                req=req,
                ordered=ordered,
                champion=champion,
                run_status=run_status,
                error_count=error_count,
            ),
            rollout_plan=self._build_rollout_plan(req=req, champion=champion),
            results=ordered,
        )

    def _evaluate_strategies(
        self,
        *,
        req: StrategyChallengeRequest,
        strategy_names: list[str],
    ) -> tuple[list[StrategyChallengeStrategyResult], int]:
        if self.max_parallel_workers <= 1 or len(strategy_names) <= 1:
            return self._evaluate_strategies_sequential(req=req, strategy_names=strategy_names)
        return self._evaluate_strategies_parallel(req=req, strategy_names=strategy_names)

    def _evaluate_strategies_sequential(
        self,
        *,
        req: StrategyChallengeRequest,
        strategy_names: list[str],
    ) -> tuple[list[StrategyChallengeStrategyResult], int]:
        results: list[StrategyChallengeStrategyResult] = []
        total_evaluated = 0
        for strategy_name in strategy_names:
            item, evaluated_count = self._evaluate_single_strategy(req=req, strategy_name=strategy_name)
            results.append(item)
            total_evaluated += int(evaluated_count)
        return results, total_evaluated

    def _evaluate_strategies_parallel(
        self,
        *,
        req: StrategyChallengeRequest,
        strategy_names: list[str],
    ) -> tuple[list[StrategyChallengeStrategyResult], int]:
        worker_count = max(1, min(int(self.max_parallel_workers), len(strategy_names)))
        if worker_count <= 1:
            return self._evaluate_strategies_sequential(req=req, strategy_names=strategy_names)

        payload = req.model_dump(mode="json")
        results: list[StrategyChallengeStrategyResult] = []
        total_evaluated = 0
        try:
            mp_ctx = mp.get_context("spawn")
            with ProcessPoolExecutor(max_workers=worker_count, mp_context=mp_ctx) as executor:
                future_map = {
                    executor.submit(_run_single_strategy_subprocess, payload, strategy_name): strategy_name
                    for strategy_name in strategy_names
                }
                for future in as_completed(future_map):
                    strategy_name = future_map[future]
                    try:
                        result_payload = future.result()
                        raw_result = result_payload.get("result")
                        item = StrategyChallengeStrategyResult.model_validate(raw_result)
                        if not item.strategy_name:
                            item = item.model_copy(update={"strategy_name": strategy_name})
                        results.append(item)
                        total_evaluated += int(result_payload.get("evaluated_count", 0))
                    except Exception as exc:  # noqa: BLE001
                        logger.warning("Challenge subprocess failed for %s: %s", strategy_name, exc)
                        results.append(
                            StrategyChallengeStrategyResult(
                                strategy_name=strategy_name,
                                qualified=False,
                                qualification_reasons=["runtime_error"],
                                ranking_score=None,
                                error=str(exc),
                            )
                        )
            return results, total_evaluated
        except Exception as exc:  # noqa: BLE001
            logger.warning("Challenge parallel execution failed; fallback to sequential: %s", exc)
            return self._evaluate_strategies_sequential(req=req, strategy_names=strategy_names)

    def _evaluate_single_strategy(
        self,
        *,
        req: StrategyChallengeRequest,
        strategy_name: str,
    ) -> tuple[StrategyChallengeStrategyResult, int]:
        try:
            autotune_req = self._build_autotune_request(req=req, strategy_name=strategy_name)
            autotune_result = self.autotune.run(autotune_req)
            evaluated_count = int(autotune_result.evaluated_count)

            best = autotune_result.best
            if best is None:
                return (
                    StrategyChallengeStrategyResult(
                        strategy_name=strategy_name,
                        provider=autotune_result.provider,
                        autotune_run_id=autotune_result.run_id,
                        evaluated_count=autotune_result.evaluated_count,
                        qualified=False,
                        qualification_reasons=["no_best_candidate"],
                        ranking_score=None,
                        error="autotune returned no candidate",
                    ),
                    evaluated_count,
                )

            full_metrics, provider_name = self._run_full_backtest(
                req=req,
                strategy_name=strategy_name,
                strategy_params=best.strategy_params,
            )
            base_result = StrategyChallengeStrategyResult(
                strategy_name=strategy_name,
                provider=provider_name or autotune_result.provider,
                autotune_run_id=autotune_result.run_id,
                evaluated_count=autotune_result.evaluated_count,
                best_params=dict(best.strategy_params),
                best_objective_score=float(best.objective_score),
                validation_metrics=best.validation_metrics,
                full_backtest_metrics=full_metrics,
                walk_forward_samples=int(best.walk_forward_samples),
                walk_forward_return_std=best.walk_forward_return_std,
                stability_penalty=float(best.stability_penalty),
                return_variance_penalty=float(best.return_variance_penalty),
                param_drift_penalty=float(best.param_drift_penalty),
            )
            qualified, reasons = self._evaluate_gate(req=req, result=base_result)
            ranking_score = self._ranking_score(req=req, result=base_result, qualified=qualified)
            return (
                base_result.model_copy(
                    update={
                        "qualified": qualified,
                        "qualification_reasons": reasons,
                        "ranking_score": ranking_score,
                        "error": None,
                    }
                ),
                evaluated_count,
            )
        except Exception as exc:  # noqa: BLE001
            return (
                StrategyChallengeStrategyResult(
                    strategy_name=strategy_name,
                    qualified=False,
                    qualification_reasons=["runtime_error"],
                    ranking_score=None,
                    error=str(exc),
                ),
                0,
            )

    def _resolve_strategy_names(self, names: list[str]) -> list[str]:
        if names:
            raw = names
        else:
            raw = [info.name for info in self.registry.list_info()]

        out: list[str] = []
        seen: set[str] = set()
        for item in raw:
            normalized = str(item).strip().lower()
            if not normalized or normalized in seen:
                continue
            _ = self.registry.get(normalized)
            seen.add(normalized)
            out.append(normalized)

        if not out:
            raise ValueError("strategy_names resolved empty")
        return out

    def _build_autotune_request(self, *, req: StrategyChallengeRequest, strategy_name: str) -> AutoTuneRunRequest:
        return AutoTuneRunRequest(
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            strategy_name=strategy_name,
            base_strategy_params=dict(req.base_strategy_params_map.get(strategy_name, {})),
            search_space=dict(req.search_space_map.get(strategy_name, {})),
            max_combinations=req.per_strategy_max_combinations,
            validation_ratio=req.validation_ratio,
            validation_weight=req.validation_weight,
            min_train_bars=req.min_train_bars,
            min_validation_bars=req.min_validation_bars,
            min_trade_count=req.min_trade_count,
            low_trade_penalty=req.low_trade_penalty,
            objective_weight_total_return=req.objective_weight_total_return,
            objective_weight_annualized_return=req.objective_weight_annualized_return,
            objective_weight_sharpe=req.objective_weight_sharpe,
            objective_weight_win_rate=req.objective_weight_win_rate,
            objective_weight_trade_count=req.objective_weight_trade_count,
            objective_weight_max_drawdown=req.objective_weight_max_drawdown,
            objective_weight_blocked_ratio=req.objective_weight_blocked_ratio,
            objective_weight_overfit_gap=req.objective_weight_overfit_gap,
            objective_weight_stability=req.objective_weight_stability,
            objective_weight_param_drift=req.objective_weight_param_drift,
            objective_weight_return_variance=req.objective_weight_return_variance,
            stability_eval_top_n=req.stability_eval_top_n,
            walk_forward_slices=req.walk_forward_slices,
            low_sample_penalty=req.low_sample_penalty,
            enable_event_enrichment=req.enable_event_enrichment,
            enable_fundamental_enrichment=req.enable_fundamental_enrichment,
            enable_small_capital_mode=req.enable_small_capital_mode,
            small_capital_principal=req.small_capital_principal,
            small_capital_min_expected_edge_bps=req.small_capital_min_expected_edge_bps,
            fundamental_max_staleness_days=req.fundamental_max_staleness_days,
            event_lookback_days=req.event_lookback_days,
            event_decay_half_life_days=req.event_decay_half_life_days,
            initial_cash=req.initial_cash,
            commission_rate=req.commission_rate,
            slippage_rate=req.slippage_rate,
            min_commission_cny=req.min_commission_cny,
            stamp_duty_sell_rate=req.stamp_duty_sell_rate,
            transfer_fee_rate=req.transfer_fee_rate,
            lot_size=req.lot_size,
            max_single_position=req.max_single_position,
            enable_realistic_cost_model=req.enable_realistic_cost_model,
            impact_cost_coeff=req.impact_cost_coeff,
            impact_cost_exponent=req.impact_cost_exponent,
            fill_probability_floor=req.fill_probability_floor,
            auto_apply=False,
            apply_scope=AutoTuneApplyScope.SYMBOL,
            apply_require_validation=req.gate_require_validation,
            apply_min_validation_total_return=req.gate_min_validation_total_return,
            apply_min_walk_forward_samples=req.gate_min_walk_forward_samples,
            create_governance_draft=False,
            governance_submit_review=False,
            run_by=f"{req.run_by}:{strategy_name}",
        )

    def _run_full_backtest(
        self,
        *,
        req: StrategyChallengeRequest,
        strategy_name: str,
        strategy_params: dict[str, float | int | str | bool],
    ) -> tuple[BacktestMetrics, str]:
        strategy = self.registry.get(strategy_name)
        provider_name, bars = self.provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
        if bars.empty:
            raise ValueError("No market data available for challenge full-window backtest.")

        bars = bars.sort_values("trade_date").reset_index(drop=True).copy()
        status = self.provider.get_security_status(req.symbol)
        bars["is_st"] = bool(status.get("is_st", False))
        bars["is_suspended"] = bool(status.get("is_suspended", False))

        if (req.enable_event_enrichment or strategy_name == "event_driven") and self.event_service is not None:
            bars, _ = self.event_service.enrich_bars(
                symbol=req.symbol,
                bars=bars,
                lookback_days=req.event_lookback_days,
                decay_half_life_days=req.event_decay_half_life_days,
            )
        if req.enable_fundamental_enrichment and self.fundamental_service is not None:
            bars, _ = self.fundamental_service.enrich_bars(
                symbol=req.symbol,
                bars=bars,
                as_of=req.end_date,
                max_staleness_days=req.fundamental_max_staleness_days,
            )

        run_req = BacktestRequest(
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            strategy_name=strategy_name,
            strategy_params=dict(strategy_params),
            enable_event_enrichment=False,
            enable_fundamental_enrichment=False,
            use_autotune_profile=False,
            enable_small_capital_mode=req.enable_small_capital_mode,
            small_capital_principal=req.small_capital_principal,
            small_capital_min_expected_edge_bps=req.small_capital_min_expected_edge_bps,
            event_lookback_days=req.event_lookback_days,
            event_decay_half_life_days=req.event_decay_half_life_days,
            initial_cash=req.initial_cash,
            commission_rate=req.commission_rate,
            slippage_rate=req.slippage_rate,
            min_commission_cny=req.min_commission_cny,
            stamp_duty_sell_rate=req.stamp_duty_sell_rate,
            transfer_fee_rate=req.transfer_fee_rate,
            lot_size=req.lot_size,
            max_single_position=req.max_single_position,
            enable_realistic_cost_model=req.enable_realistic_cost_model,
            impact_cost_coeff=req.impact_cost_coeff,
            impact_cost_exponent=req.impact_cost_exponent,
            fill_probability_floor=req.fill_probability_floor,
        )
        result = self.backtest_engine.run(bars, run_req, strategy)
        return result.metrics, provider_name

    def _evaluate_gate(
        self,
        *,
        req: StrategyChallengeRequest,
        result: StrategyChallengeStrategyResult,
    ) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        vm = result.validation_metrics

        if req.gate_require_validation and vm is None:
            reasons.append("validation_missing")

        if vm is not None:
            if float(vm.total_return) < req.gate_min_validation_total_return:
                reasons.append(
                    f"validation_total_return<{req.gate_min_validation_total_return:.4f}"
                )
            if float(vm.max_drawdown) > req.gate_max_validation_drawdown:
                reasons.append(
                    f"validation_max_drawdown>{req.gate_max_validation_drawdown:.4f}"
                )
            if float(vm.sharpe) < req.gate_min_validation_sharpe:
                reasons.append(f"validation_sharpe<{req.gate_min_validation_sharpe:.4f}")
            if int(vm.trade_count) < req.gate_min_validation_trade_count:
                reasons.append(
                    f"validation_trade_count<{req.gate_min_validation_trade_count}"
                )

        if int(result.walk_forward_samples) < req.gate_min_walk_forward_samples:
            reasons.append(f"walk_forward_samples<{req.gate_min_walk_forward_samples}")
        if result.walk_forward_return_std is not None and float(result.walk_forward_return_std) > req.gate_max_walk_forward_return_std:
            reasons.append(
                f"walk_forward_return_std>{req.gate_max_walk_forward_return_std:.4f}"
            )

        return len(reasons) == 0, reasons

    @staticmethod
    def _metric_for_ranking(result: StrategyChallengeStrategyResult) -> BacktestMetrics | None:
        if result.validation_metrics is not None:
            return result.validation_metrics
        if result.full_backtest_metrics is not None:
            return result.full_backtest_metrics
        return None

    def _ranking_score(
        self,
        *,
        req: StrategyChallengeRequest,
        result: StrategyChallengeStrategyResult,
        qualified: bool,
    ) -> float | None:
        metric = self._metric_for_ranking(result)
        if metric is None:
            return None

        stability_component = 1.0 - min(
            1.0,
            float(result.stability_penalty) + float(result.param_drift_penalty),
        )
        variance_penalty_base = float(result.return_variance_penalty) + float(result.walk_forward_return_std or 0.0)
        score = 0.0
        score += req.rank_weight_validation_return * float(metric.total_return)
        score += req.rank_weight_validation_sharpe * float(metric.sharpe)
        score += req.rank_weight_stability * float(stability_component)
        score -= req.rank_weight_drawdown_penalty * float(metric.max_drawdown)
        score -= req.rank_weight_variance_penalty * float(variance_penalty_base)
        if not qualified:
            score -= 10.0
        return float(score)

    @staticmethod
    def _result_sort_key(result: StrategyChallengeStrategyResult) -> tuple[int, float, float]:
        qualified_score = 1 if result.qualified else 0
        ranking_score = float(result.ranking_score) if result.ranking_score is not None else float("-inf")
        objective_score = (
            float(result.best_objective_score)
            if result.best_objective_score is not None
            else float("-inf")
        )
        return qualified_score, ranking_score, objective_score

    def _build_summary(
        self,
        *,
        req: StrategyChallengeRequest,
        ordered: list[StrategyChallengeStrategyResult],
        champion: StrategyChallengeStrategyResult | None,
        run_status: StrategyChallengeRunStatus,
        error_count: int,
    ) -> str:
        if not ordered:
            return f"run_status={run_status.value}; no strategy result generated."
        qualified_count = len([item for item in ordered if item.qualified])
        if champion is None:
            return (
                f"run_status={run_status.value}; errors={error_count}; "
                f"0/{len(ordered)} strategies passed hard gates. "
                "No strategy is safe to promote under current constraints."
            )
        metric = champion.validation_metrics or champion.full_backtest_metrics
        if metric is None:
            return (
                f"run_status={run_status.value}; errors={error_count}; "
                f"{qualified_count}/{len(ordered)} strategies qualified. "
                f"Champion={champion.strategy_name}."
            )
        return (
            f"run_status={run_status.value}; errors={error_count}; "
            f"{qualified_count}/{len(ordered)} strategies qualified. "
            f"Champion={champion.strategy_name}; "
            f"validation_return={float(metric.total_return):.4f}, "
            f"validation_drawdown={float(metric.max_drawdown):.4f}, "
            f"validation_sharpe={float(metric.sharpe):.4f}; "
            f"gray_days={req.rollout_gray_days}."
        )

    def _build_rollout_plan(
        self,
        *,
        req: StrategyChallengeRequest,
        champion: StrategyChallengeStrategyResult | None,
    ) -> StrategyChallengeRolloutPlan:
        if champion is None:
            return StrategyChallengeRolloutPlan(
                enabled=False,
                strategy_name=None,
                symbol=req.symbol,
                gray_days=req.rollout_gray_days,
                activation_scope=AutoTuneApplyScope.SYMBOL,
                rollback_triggers=[
                    "no_qualified_strategy",
                ],
            )
        return StrategyChallengeRolloutPlan(
            enabled=True,
            strategy_name=champion.strategy_name,
            symbol=req.symbol,
            gray_days=req.rollout_gray_days,
            activation_scope=AutoTuneApplyScope.SYMBOL,
            rollback_triggers=[
                (
                    f"gray_window_cumulative_return<{req.gate_min_validation_total_return:.2%}"
                ),
                (
                    f"gray_window_max_drawdown>{req.gate_max_validation_drawdown:.2%}"
                ),
                (
                    f"gray_window_sharpe<{req.gate_min_validation_sharpe:.2f}"
                ),
                "three_consecutive_loss_days",
            ],
        )
