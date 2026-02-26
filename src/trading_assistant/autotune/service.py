from __future__ import annotations

import hashlib
import inspect
import itertools
import json
import logging
import statistics
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

import pandas as pd

from trading_assistant.autotune.store import AutoTuneStore
from trading_assistant.backtest.engine import BacktestEngine
from trading_assistant.core.models import (
    AutoTuneApplyScope,
    AutoTuneCandidateResult,
    AutoTuneProfileRecord,
    AutoTuneRolloutRuleRecord,
    AutoTuneRunRequest,
    AutoTuneRunResult,
    BacktestMetrics,
    BacktestRequest,
    StrategySubmitReviewRequest,
    StrategyVersionRegisterRequest,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.event_service import EventService
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.registry import StrategyRegistry

logger = logging.getLogger(__name__)


_DEFAULT_SPACE: dict[str, dict[str, list[float | int | str | bool]]] = {
    "trend_following": {
        "entry_ma_fast": [6, 10, 14, 18, 24],
        "entry_ma_slow": [20, 30, 40, 55, 60],
        "atr_multiplier": [1.1, 1.4, 1.8, 2.2],
    },
    "mean_reversion": {
        "z_enter": [1.0, 1.3, 1.6, 2.0],
        "z_exit": [-0.4, -0.2, 0.0, 0.2],
        "min_turnover": [1_500_000, 2_500_000, 4_000_000, 5_000_000, 7_000_000],
    },
    "multi_factor": {
        "buy_threshold": [0.45, 0.49, 0.53, 0.55, 0.58],
        "sell_threshold": [0.35, 0.36, 0.40, 0.44, 0.48],
        "w_momentum": [0.30, 0.40, 0.50],
        "w_quality": [0.15, 0.20, 0.28],
        "w_low_vol": [0.05, 0.10, 0.15],
        "w_liquidity": [0.15, 0.22, 0.30],
        "liquidity_direction": [-1.0, 1.0],
        "w_fundamental": [0.03, 0.07, 0.12],
        "w_tushare_advanced": [0.02, 0.05, 0.10],
        "min_fundamental_score_buy": [0.20, 0.25, 0.32],
        "min_tushare_score_buy": [0.16, 0.20, 0.26],
    },
    "sector_rotation": {
        "sector_strength": [0.40, 0.48, 0.56, 0.65],
        "risk_off_strength": [0.30, 0.40, 0.50, 0.60, 0.65],
    },
    "event_driven": {
        "event_score": [0.45, 0.55, 0.65, 0.70, 0.75],
        "negative_event_score": [0.35, 0.45, 0.55, 0.60, 0.65],
    },
}


class AutoTuneService:
    def __init__(
        self,
        *,
        store: AutoTuneStore,
        provider: CompositeDataProvider,
        backtest_engine: BacktestEngine,
        registry: StrategyRegistry,
        event_service: EventService | None = None,
        fundamental_service: FundamentalService | None = None,
        strategy_gov: StrategyGovernanceService | None = None,
        runtime_override_enabled: bool = True,
    ) -> None:
        self.store = store
        self.provider = provider
        self.backtest_engine = backtest_engine
        self.registry = registry
        self.event_service = event_service
        self.fundamental_service = fundamental_service
        self.strategy_gov = strategy_gov
        self.runtime_override_enabled = bool(runtime_override_enabled)

    def resolve_runtime_params(
        self,
        *,
        strategy_name: str,
        symbol: str | None,
        explicit_params: dict[str, float | int | str | bool] | None,
        use_profile: bool,
    ) -> tuple[dict[str, float | int | str | bool], AutoTuneProfileRecord | None]:
        explicit = dict(explicit_params or {})
        if not self.runtime_override_enabled or (not use_profile):
            return explicit, None
        rollout_rule = self.store.get_rollout_rule(strategy_name=strategy_name, symbol=symbol)
        if rollout_rule is not None and (not rollout_rule.enabled):
            return explicit, None
        profile = self.store.get_active_profile(strategy_name=strategy_name, symbol=symbol)
        if profile is None:
            return explicit, None
        merged = dict(profile.strategy_params)
        merged.update(explicit)
        return merged, profile

    def list_profiles(
        self,
        *,
        strategy_name: str | None = None,
        symbol: str | None = None,
        active_only: bool = False,
        limit: int = 200,
    ) -> list[AutoTuneProfileRecord]:
        return self.store.list_profiles(
            strategy_name=strategy_name,
            symbol=symbol,
            active_only=active_only,
            limit=limit,
        )

    def get_active_profile(self, *, strategy_name: str, symbol: str | None = None) -> AutoTuneProfileRecord | None:
        return self.store.get_active_profile(strategy_name=strategy_name, symbol=symbol)

    def activate_profile(self, profile_id: int) -> AutoTuneProfileRecord | None:
        return self.store.activate_profile(profile_id=profile_id)

    def rollback_active_profile(
        self,
        *,
        strategy_name: str,
        symbol: str | None = None,
        scope: AutoTuneApplyScope | None = None,
    ) -> AutoTuneProfileRecord | None:
        effective_scope = scope or (AutoTuneApplyScope.SYMBOL if symbol else AutoTuneApplyScope.GLOBAL)
        effective_symbol = symbol if effective_scope == AutoTuneApplyScope.SYMBOL else None
        return self.store.rollback_active_profile(
            strategy_name=strategy_name,
            scope=effective_scope,
            symbol=effective_symbol,
        )

    def upsert_rollout_rule(
        self,
        *,
        strategy_name: str,
        symbol: str | None,
        enabled: bool,
        note: str = "",
    ) -> AutoTuneRolloutRuleRecord:
        return self.store.upsert_rollout_rule(
            strategy_name=strategy_name,
            symbol=symbol,
            enabled=enabled,
            note=note,
        )

    def list_rollout_rules(
        self,
        *,
        strategy_name: str | None = None,
        symbol: str | None = None,
        limit: int = 500,
    ) -> list[AutoTuneRolloutRuleRecord]:
        return self.store.list_rollout_rules(
            strategy_name=strategy_name,
            symbol=symbol,
            limit=limit,
        )

    def delete_rollout_rule(self, rule_id: int) -> bool:
        return self.store.delete_rollout_rule(rule_id)

    def run(self, req: AutoTuneRunRequest) -> AutoTuneRunResult:
        strategy = self.registry.get(req.strategy_name)
        run_id = uuid4().hex

        used_provider, bars = self.provider.get_daily_bars_with_source(req.symbol, req.start_date, req.end_date)
        if bars.empty:
            raise ValueError("No market data available for requested range.")

        bars = bars.sort_values("trade_date").reset_index(drop=True).copy()
        status = self._resolve_security_status(provider=self.provider, symbol=req.symbol, bars=bars)
        bars["is_st"] = bool(status.get("is_st", False))
        bars["is_suspended"] = bool(status.get("is_suspended", False))

        if (req.enable_event_enrichment or req.strategy_name == "event_driven") and self.event_service is not None:
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

        train_bars, validation_bars = self._split_bars(
            bars=bars,
            validation_ratio=req.validation_ratio,
            min_train_bars=req.min_train_bars,
            min_validation_bars=req.min_validation_bars,
        )
        has_validation = validation_bars is not None and (not validation_bars.empty)
        supports_precomputed = self._backtest_supports_precomputed_features()
        train_features = (
            self.backtest_engine.factor_engine.compute(train_bars)
            if supports_precomputed
            else None
        )
        validation_features = (
            self.backtest_engine.factor_engine.compute(validation_bars)
            if (supports_precomputed and has_validation and validation_bars is not None and (not validation_bars.empty))
            else None
        )

        param_candidates = self._build_candidate_params(req=req, params_schema=strategy.info.params_schema)
        if not param_candidates:
            param_candidates = [dict(req.base_strategy_params)]

        baseline_eval = self._evaluate_candidate(
            req=req,
            strategy=strategy,
            train_bars=train_bars,
            validation_bars=validation_bars,
            train_features=train_features,
            validation_features=validation_features,
            params=dict(req.base_strategy_params),
            has_validation=has_validation,
            supports_precomputed=supports_precomputed,
        )

        candidate_results: list[AutoTuneCandidateResult] = []
        for params in param_candidates:
            item = self._evaluate_candidate(
                req=req,
                strategy=strategy,
                train_bars=train_bars,
                validation_bars=validation_bars,
                train_features=train_features,
                validation_features=validation_features,
                params=params,
                has_validation=has_validation,
                supports_precomputed=supports_precomputed,
            )
            candidate_results.append(item)

        self._apply_stability_penalties(
            req=req,
            strategy=strategy,
            bars=train_bars,
            candidate_results=candidate_results,
            supports_precomputed=supports_precomputed,
        )
        candidate_results.sort(key=lambda x: (x.objective_score, x.train_score), reverse=True)
        ranked: list[AutoTuneCandidateResult] = []
        for idx, item in enumerate(candidate_results, start=1):
            ranked.append(item.model_copy(update={"rank": idx}))

        best = ranked[0] if ranked else None
        baseline = baseline_eval.model_copy(update={"rank": 0})
        improvement = (best.objective_score - baseline.objective_score) if best is not None else None

        applied = False
        applied_profile: AutoTuneProfileRecord | None = None
        governance_draft_id: int | None = None
        governance_version: str | None = None
        apply_decision = "auto_apply_disabled"

        should_apply = False
        if req.auto_apply:
            if best is None:
                apply_decision = "no_best_candidate"
            else:
                should_apply, apply_decision = self._evaluate_apply_decision(
                    req=req,
                    best=best,
                    has_validation=has_validation,
                    improvement=improvement,
                )
                best = best.model_copy(
                    update={
                        "apply_eligible": should_apply,
                        "apply_guard_reason": (None if should_apply else apply_decision),
                    }
                )
                ranked[0] = best

        if should_apply:
            scope = req.apply_scope
            symbol_value = req.symbol if scope == AutoTuneApplyScope.SYMBOL else None
            applied_profile = self.store.upsert_active_profile(
                strategy_name=req.strategy_name,
                scope=scope,
                symbol=symbol_value,
                strategy_params=best.strategy_params,
                objective_score=best.objective_score,
                validation_total_return=(
                    float(best.validation_metrics.total_return) if best.validation_metrics is not None else None
                ),
                source_run_id=run_id,
                note=f"autotune run by {req.run_by}",
            )
            applied = True
            apply_decision = "applied"

            if req.create_governance_draft and self.strategy_gov is not None:
                governance_version = self._build_governance_version(strategy_name=req.strategy_name, run_id=run_id)
                params_hash = self._params_hash(best.strategy_params)
                governance_draft_id = self.strategy_gov.register_draft(
                    StrategyVersionRegisterRequest(
                        strategy_name=req.strategy_name,
                        version=governance_version,
                        description=(
                            f"AutoTune run={run_id} symbol={req.symbol} "
                            f"objective={best.objective_score:.6f} baseline={baseline.objective_score:.6f}"
                        ),
                        params_hash=params_hash,
                        created_by=req.run_by,
                    )
                )
                if req.governance_submit_review:
                    _ = self.strategy_gov.submit_review(
                        StrategySubmitReviewRequest(
                            strategy_name=req.strategy_name,
                            version=governance_version,
                            submitted_by=req.run_by,
                            note=f"autotune auto submit, run={run_id}",
                        )
                    )

        message = self._build_message(
            best=best,
            baseline=baseline,
            improvement=improvement,
            applied=applied,
            has_validation=has_validation,
            apply_decision=apply_decision,
        )
        return AutoTuneRunResult(
            run_id=run_id,
            strategy_name=req.strategy_name,
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            provider=used_provider,
            evaluated_count=len(ranked),
            candidates=ranked,
            baseline=baseline,
            best=best,
            improvement_vs_baseline=improvement,
            applied=applied,
            applied_profile=applied_profile,
            governance_draft_id=governance_draft_id,
            governance_version=governance_version,
            apply_decision=apply_decision,
            message=message,
        )

    def _evaluate_candidate(
        self,
        *,
        req: AutoTuneRunRequest,
        strategy,
        train_bars: pd.DataFrame,
        validation_bars: pd.DataFrame | None,
        train_features: pd.DataFrame | None,
        validation_features: pd.DataFrame | None,
        params: dict[str, float | int | str | bool],
        has_validation: bool,
        supports_precomputed: bool,
    ) -> AutoTuneCandidateResult:
        train_req = BacktestRequest(
            symbol=req.symbol,
            start_date=self._bars_start(train_bars, req.start_date),
            end_date=self._bars_end(train_bars, req.end_date),
            strategy_name=req.strategy_name,
            strategy_params=params,
            enable_event_enrichment=False,
            enable_fundamental_enrichment=False,
            use_autotune_profile=False,
            enable_small_capital_mode=req.enable_small_capital_mode,
            small_capital_principal=req.small_capital_principal,
            small_capital_min_expected_edge_bps=req.small_capital_min_expected_edge_bps,
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
        train_result = self._run_backtest(
            bars=train_bars,
            req=train_req,
            strategy=strategy,
            precomputed_features=train_features,
            supports_precomputed=supports_precomputed,
        )
        train_score = self._objective_score(metrics=train_result.metrics, req=req)
        if train_result.metrics.trade_count < req.min_trade_count:
            train_score -= req.low_trade_penalty

        validation_result = None
        validation_score: float | None = None
        if has_validation and validation_bars is not None and (not validation_bars.empty):
            validation_req = train_req.model_copy(
                update={
                    "start_date": self._bars_start(validation_bars, req.start_date),
                    "end_date": self._bars_end(validation_bars, req.end_date),
                }
            )
            validation_result = self._run_backtest(
                bars=validation_bars,
                req=validation_req,
                strategy=strategy,
                precomputed_features=validation_features,
                supports_precomputed=supports_precomputed,
            )
            validation_score = self._objective_score(metrics=validation_result.metrics, req=req)
            if validation_result.metrics.trade_count < req.min_trade_count:
                validation_score -= req.low_trade_penalty

        objective_score = train_score
        if validation_score is not None:
            objective_score = (1.0 - req.validation_weight) * train_score + req.validation_weight * validation_score
        overfit_gap = max(0.0, float(train_score) - float(validation_score or train_score))
        overfit_penalty = req.objective_weight_overfit_gap * overfit_gap
        objective_score = float(objective_score) - float(overfit_penalty)

        return AutoTuneCandidateResult(
            rank=0,
            strategy_params=dict(params),
            objective_score=float(objective_score),
            train_metrics=train_result.metrics,
            validation_metrics=(validation_result.metrics if validation_result is not None else None),
            train_score=float(train_score),
            validation_score=validation_score,
            overfit_gap=float(overfit_gap),
            overfit_penalty=float(overfit_penalty),
            stability_score_std=None,
            stability_penalty=0.0,
            walk_forward_return_std=None,
            return_variance_penalty=0.0,
            param_drift_score=None,
            param_drift_penalty=0.0,
            low_sample_penalty=0.0,
            walk_forward_samples=0,
            apply_eligible=True,
            apply_guard_reason=None,
        )

    def _apply_stability_penalties(
        self,
        *,
        req: AutoTuneRunRequest,
        strategy,
        bars: pd.DataFrame,
        candidate_results: list[AutoTuneCandidateResult],
        supports_precomputed: bool,
    ) -> None:
        if not candidate_results:
            return
        if req.stability_eval_top_n <= 0 or req.walk_forward_slices <= 0:
            return

        ordered = sorted(candidate_results, key=lambda x: (x.objective_score, x.train_score), reverse=True)
        top_n = max(1, min(len(ordered), int(req.stability_eval_top_n)))
        top_tokens = {self._params_hash(item.strategy_params) for item in ordered[:top_n]}
        top_items: dict[str, AutoTuneCandidateResult] = {
            self._params_hash(item.strategy_params): item for item in ordered[:top_n]
        }
        fold_scores_by_token: dict[str, list[float]] = {}
        windows = self._walk_forward_validation_slices(
            bars=bars,
            slices=req.walk_forward_slices,
            min_train_bars=max(20, min(req.min_train_bars, 240)),
            min_validation_bars=max(10, min(req.min_validation_bars, 120)),
            validation_ratio=req.validation_ratio,
        )
        if not windows:
            return
        window_features = (
            [self.backtest_engine.factor_engine.compute(item) for item in windows]
            if supports_precomputed
            else None
        )

        for idx, item in enumerate(candidate_results):
            token = self._params_hash(item.strategy_params)
            if token not in top_tokens:
                continue

            fold_scores, fold_returns = self._evaluate_walk_forward_scores(
                req=req,
                strategy=strategy,
                bars=bars,
                params=item.strategy_params,
                windows=windows,
                window_features=window_features,
                supports_precomputed=supports_precomputed,
            )
            fold_scores_by_token[token] = fold_scores
            sample_count = len(fold_scores)
            std_value = statistics.pstdev(fold_scores) if sample_count >= 2 else 0.0
            stability_penalty = req.objective_weight_stability * float(std_value)
            return_std = statistics.pstdev(fold_returns) if len(fold_returns) >= 2 else 0.0
            return_variance_penalty = req.objective_weight_return_variance * float(return_std)
            low_sample_penalty = (
                req.low_sample_penalty
                if (req.apply_min_walk_forward_samples > 0 and sample_count < req.apply_min_walk_forward_samples)
                else 0.0
            )
            candidate_results[idx] = item.model_copy(
                update={
                    "objective_score": float(
                        item.objective_score - stability_penalty - return_variance_penalty - low_sample_penalty
                    ),
                    "stability_score_std": (float(std_value) if sample_count > 0 else None),
                    "stability_penalty": float(stability_penalty),
                    "walk_forward_return_std": (float(return_std) if sample_count > 0 else None),
                    "return_variance_penalty": float(return_variance_penalty),
                    "param_drift_score": None,
                    "param_drift_penalty": 0.0,
                    "low_sample_penalty": float(low_sample_penalty),
                    "walk_forward_samples": int(sample_count),
                }
            )

        if req.objective_weight_param_drift <= 0:
            return
        if not fold_scores_by_token:
            return

        fold_best_params = self._fold_best_params(
            fold_scores_by_token=fold_scores_by_token,
            params_by_token={token: top_items[token].strategy_params for token in top_items},
        )
        if not fold_best_params:
            return

        for idx, item in enumerate(candidate_results):
            token = self._params_hash(item.strategy_params)
            if token not in top_tokens:
                continue
            drift_score = self._param_drift_score(
                candidate=item.strategy_params,
                fold_best_params=fold_best_params,
            )
            drift_penalty = req.objective_weight_param_drift * drift_score
            candidate_results[idx] = candidate_results[idx].model_copy(
                update={
                    "objective_score": float(candidate_results[idx].objective_score - drift_penalty),
                    "param_drift_score": float(drift_score),
                    "param_drift_penalty": float(drift_penalty),
                }
            )

    def _evaluate_walk_forward_scores(
        self,
        *,
        req: AutoTuneRunRequest,
        strategy,
        bars: pd.DataFrame,
        params: dict[str, float | int | str | bool],
        windows: list[pd.DataFrame] | None = None,
        window_features: list[pd.DataFrame] | None = None,
        supports_precomputed: bool = False,
    ) -> tuple[list[float], list[float]]:
        windows = windows or self._walk_forward_validation_slices(
            bars=bars,
            slices=req.walk_forward_slices,
            min_train_bars=max(20, min(req.min_train_bars, 240)),
            min_validation_bars=max(10, min(req.min_validation_bars, 120)),
            validation_ratio=req.validation_ratio,
        )
        if not windows:
            return [], []

        scores: list[float] = []
        returns: list[float] = []
        for idx, validation_bars in enumerate(windows):
            fold_req = BacktestRequest(
                symbol=req.symbol,
                start_date=self._bars_start(validation_bars, req.start_date),
                end_date=self._bars_end(validation_bars, req.end_date),
                strategy_name=req.strategy_name,
                strategy_params=dict(params),
                enable_event_enrichment=False,
                enable_fundamental_enrichment=False,
                use_autotune_profile=False,
                enable_small_capital_mode=req.enable_small_capital_mode,
                small_capital_principal=req.small_capital_principal,
                small_capital_min_expected_edge_bps=req.small_capital_min_expected_edge_bps,
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
            precomputed = (
                window_features[idx]
                if (window_features is not None and idx < len(window_features))
                else None
            )
            fold_result = self._run_backtest(
                bars=validation_bars,
                req=fold_req,
                strategy=strategy,
                precomputed_features=precomputed,
                supports_precomputed=supports_precomputed,
            )
            fold_score = self._objective_score(metrics=fold_result.metrics, req=req)
            if fold_result.metrics.trade_count < req.min_trade_count:
                fold_score -= req.low_trade_penalty
            scores.append(float(fold_score))
            returns.append(float(fold_result.metrics.total_return))
        return scores, returns

    @staticmethod
    def _walk_forward_validation_slices(
        *,
        bars: pd.DataFrame,
        slices: int,
        min_train_bars: int,
        min_validation_bars: int,
        validation_ratio: float,
    ) -> list[pd.DataFrame]:
        ordered = bars.sort_values("trade_date").reset_index(drop=True)
        if slices <= 0:
            return []
        if len(ordered) < (min_train_bars + min_validation_bars):
            return []

        val_len = int(round(len(ordered) * max(0.05, min(validation_ratio, 0.60))))
        val_len = max(min_validation_bars, val_len)
        val_len = min(val_len, len(ordered) - min_train_bars)
        if val_len < min_validation_bars:
            return []

        first_split = min_train_bars
        last_split = len(ordered) - val_len
        if last_split < first_split:
            return []

        if slices == 1:
            split_points = [last_split]
        else:
            split_points = []
            step = (last_split - first_split) / max(1, slices - 1)
            for i in range(slices):
                split = int(round(first_split + step * i))
                split = max(first_split, min(last_split, split))
                if split not in split_points:
                    split_points.append(split)

        windows: list[pd.DataFrame] = []
        for split in split_points:
            validation = ordered.iloc[split : split + val_len].copy()
            if len(validation) < min_validation_bars:
                continue
            windows.append(validation)
        return windows

    @staticmethod
    def _evaluate_apply_decision(
        *,
        req: AutoTuneRunRequest,
        best: AutoTuneCandidateResult,
        has_validation: bool,
        improvement: float | None,
    ) -> tuple[bool, str]:
        reasons: list[str] = []

        if improvement is not None and improvement < req.min_improvement_to_apply:
            reasons.append(
                f"improvement {improvement:.6f} < min_improvement_to_apply {req.min_improvement_to_apply:.6f}"
            )

        if req.apply_require_validation and (not has_validation or best.validation_metrics is None):
            reasons.append("validation_required_but_missing")

        if best.validation_metrics is not None:
            validation_return = float(best.validation_metrics.total_return)
            if validation_return < req.apply_min_validation_total_return:
                reasons.append(
                    "validation_total_return "
                    f"{validation_return:.4f} < apply_min_validation_total_return {req.apply_min_validation_total_return:.4f}"
                )

        if best.validation_score is not None:
            gap = float(best.train_score) - float(best.validation_score)
            if gap > req.apply_max_train_validation_gap:
                reasons.append(
                    f"train_validation_gap {gap:.6f} > apply_max_train_validation_gap {req.apply_max_train_validation_gap:.6f}"
                )

        if req.apply_min_walk_forward_samples > 0 and int(best.walk_forward_samples) < req.apply_min_walk_forward_samples:
            reasons.append(
                f"walk_forward_samples {best.walk_forward_samples} < apply_min_walk_forward_samples {req.apply_min_walk_forward_samples}"
            )

        if reasons:
            return False, "guard_blocked: " + "; ".join(reasons)
        return True, "eligible"

    def _backtest_supports_precomputed_features(self) -> bool:
        run_fn = self.backtest_engine.run
        try:
            params = inspect.signature(run_fn).parameters
        except (TypeError, ValueError):
            return False
        return "precomputed_features" in params

    def _run_backtest(
        self,
        *,
        bars: pd.DataFrame,
        req: BacktestRequest,
        strategy,
        precomputed_features: pd.DataFrame | None = None,
        supports_precomputed: bool | None = None,
    ):
        if supports_precomputed is None:
            supports_precomputed = self._backtest_supports_precomputed_features()
        if precomputed_features is not None and supports_precomputed:
            return self.backtest_engine.run(
                bars,
                req,
                strategy,
                precomputed_features=precomputed_features,
            )
        return self.backtest_engine.run(bars, req, strategy)

    @staticmethod
    def _bars_start(bars: pd.DataFrame, fallback: date) -> date:
        if bars.empty:
            return fallback
        parsed = pd.to_datetime(bars.iloc[0]["trade_date"], errors="coerce")
        if pd.isna(parsed):
            return fallback
        return parsed.date()

    @staticmethod
    def _bars_end(bars: pd.DataFrame, fallback: date) -> date:
        if bars.empty:
            return fallback
        parsed = pd.to_datetime(bars.iloc[-1]["trade_date"], errors="coerce")
        if pd.isna(parsed):
            return fallback
        return parsed.date()

    @staticmethod
    def _resolve_security_status(
        *,
        provider: CompositeDataProvider,
        symbol: str,
        bars: pd.DataFrame,
    ) -> dict[str, bool]:
        fallback = {
            "is_st": bool(bars.iloc[-1].get("is_st", False)) if (bars is not None and not bars.empty) else False,
            "is_suspended": bool(bars.iloc[-1].get("is_suspended", False)) if (bars is not None and not bars.empty) else False,
        }
        try:
            status = provider.get_security_status(symbol)
            return {
                "is_st": bool(status.get("is_st", fallback["is_st"])),
                "is_suspended": bool(status.get("is_suspended", fallback["is_suspended"])),
            }
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Security status lookup failed for %s in autotune; fallback to bars/default status: %s",
                symbol,
                exc,
            )
            return fallback

    @staticmethod
    def _split_bars(
        *,
        bars: pd.DataFrame,
        validation_ratio: float,
        min_train_bars: int,
        min_validation_bars: int,
    ) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        ordered = bars.sort_values("trade_date").reset_index(drop=True)
        if validation_ratio <= 0.0 or len(ordered) < (min_train_bars + min_validation_bars):
            return ordered, None
        raw_split = int(round(len(ordered) * (1.0 - validation_ratio)))
        split = max(min_train_bars, min(len(ordered) - min_validation_bars, raw_split))
        if split <= 0 or split >= len(ordered):
            return ordered, None
        return ordered.iloc[:split].copy(), ordered.iloc[split:].copy()

    def _build_candidate_params(
        self,
        *,
        req: AutoTuneRunRequest,
        params_schema: dict[str, str],
    ) -> list[dict[str, float | int | str | bool]]:
        base = self._normalize_params(req.base_strategy_params)
        source = req.search_space if req.search_space else _DEFAULT_SPACE.get(req.strategy_name, {})
        if not source:
            return [base]

        normalized_grid: dict[str, list[float | int | str | bool]] = {}
        for key, values in source.items():
            if not values:
                continue
            kind = str(params_schema.get(key, "")).strip().lower()
            packed: list[float | int | str | bool] = []
            for raw in values:
                packed.append(self._coerce_value(raw=raw, kind=kind))
            dedup: list[float | int | str | bool] = []
            seen: set[str] = set()
            for item in packed:
                token = self._param_key_fragment(item)
                if token in seen:
                    continue
                seen.add(token)
                dedup.append(item)
            if dedup:
                normalized_grid[key] = dedup

        if not normalized_grid:
            return [base]

        keys = sorted(normalized_grid.keys())
        combos: list[dict[str, float | int | str | bool]] = [base]
        for values in itertools.product(*[normalized_grid[k] for k in keys]):
            item = dict(base)
            for idx, key in enumerate(keys):
                item[key] = values[idx]
            combos.append(item)

        uniq: list[dict[str, float | int | str | bool]] = []
        seen_combo: set[str] = set()
        for combo in combos:
            token = self._params_hash(combo)
            if token in seen_combo:
                continue
            seen_combo.add(token)
            uniq.append(combo)

        limit = max(1, min(req.max_combinations, 5000))
        if len(uniq) <= limit:
            return uniq
        if limit == 1:
            return [uniq[0]]

        indices = self._evenly_spaced_indices(total=len(uniq), keep=limit)
        return [uniq[idx] for idx in indices]

    @staticmethod
    def _evenly_spaced_indices(*, total: int, keep: int) -> list[int]:
        if keep >= total:
            return list(range(total))
        if keep <= 1:
            return [0]
        out: list[int] = []
        for i in range(keep):
            idx = int(round(i * (total - 1) / (keep - 1)))
            if idx not in out:
                out.append(idx)
        while len(out) < keep:
            candidate = len(out)
            if candidate >= total:
                break
            if candidate not in out:
                out.append(candidate)
        return sorted(out)[:keep]

    @staticmethod
    def _coerce_value(*, raw: float | int | str | bool, kind: str) -> float | int | str | bool:
        if kind == "int":
            try:
                return int(round(float(str(raw).strip())))
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"invalid int candidate value: {raw!r}") from exc
        if kind == "float":
            try:
                return float(str(raw).strip())
            except Exception as exc:  # noqa: BLE001
                raise ValueError(f"invalid float candidate value: {raw!r}") from exc
        if kind == "bool":
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            return text in {"1", "true", "yes", "y", "on"}
        if isinstance(raw, (bool, int, float, str)):
            return raw
        return str(raw)

    def _objective_score(self, *, metrics: BacktestMetrics, req: AutoTuneRunRequest) -> float:
        trade_score = min(float(metrics.trade_count), 30.0) / 30.0
        blocked_base = float(metrics.trade_count + metrics.blocked_signal_count)
        blocked_ratio = 0.0 if blocked_base <= 0 else float(metrics.blocked_signal_count) / blocked_base
        score = 0.0
        score += req.objective_weight_total_return * float(metrics.total_return)
        score += req.objective_weight_annualized_return * float(metrics.annualized_return)
        score += req.objective_weight_sharpe * float(metrics.sharpe)
        score += req.objective_weight_win_rate * float(metrics.win_rate)
        score += req.objective_weight_trade_count * trade_score
        score -= req.objective_weight_max_drawdown * float(metrics.max_drawdown)
        score -= req.objective_weight_blocked_ratio * blocked_ratio
        return float(score)

    @staticmethod
    def _normalize_params(params: dict[str, Any]) -> dict[str, float | int | str | bool]:
        out: dict[str, float | int | str | bool] = {}
        for key, value in params.items():
            if isinstance(value, bool):
                out[str(key)] = bool(value)
            elif isinstance(value, int):
                out[str(key)] = int(value)
            elif isinstance(value, float):
                out[str(key)] = float(value)
            elif isinstance(value, str):
                out[str(key)] = value
            else:
                out[str(key)] = str(value)
        return out

    @staticmethod
    def _param_key_fragment(value: Any) -> str:
        if isinstance(value, bool):
            return f"b:{int(value)}"
        if isinstance(value, int):
            return f"i:{value}"
        if isinstance(value, float):
            return f"f:{value:.12g}"
        return f"s:{value}"

    @staticmethod
    def _params_hash(params: dict[str, float | int | str | bool]) -> str:
        blob = json.dumps(params, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    @staticmethod
    def _build_governance_version(*, strategy_name: str, run_id: str) -> str:
        now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        prefix = strategy_name.replace(" ", "_").lower()
        return f"{prefix}-autotune-{now}-{run_id[:8]}"

    @staticmethod
    def _build_message(
        *,
        best: AutoTuneCandidateResult | None,
        baseline: AutoTuneCandidateResult | None,
        improvement: float | None,
        applied: bool,
        has_validation: bool,
        apply_decision: str,
    ) -> str:
        if best is None:
            return "No candidate evaluated."
        parts = [
            f"best objective={best.objective_score:.6f}",
            f"train_return={best.train_metrics.total_return:.4f}",
        ]
        if has_validation and best.validation_metrics is not None:
            parts.append(f"validation_return={best.validation_metrics.total_return:.4f}")
        if best.walk_forward_samples > 0:
            parts.append(
                f"wf_samples={best.walk_forward_samples}"
                f" wf_std={(best.stability_score_std or 0.0):.6f}"
            )
            parts.append(f"wf_return_std={(best.walk_forward_return_std or 0.0):.6f}")
        if best.param_drift_score is not None:
            parts.append(f"param_drift={best.param_drift_score:.6f}")
        if baseline is not None and improvement is not None:
            parts.append(f"improvement={improvement:.6f}")
        parts.append(f"apply_decision={apply_decision}")
        parts.append("applied" if applied else "not_applied")
        return "; ".join(parts)

    @staticmethod
    def _fold_best_params(
        *,
        fold_scores_by_token: dict[str, list[float]],
        params_by_token: dict[str, dict[str, float | int | str | bool]],
    ) -> list[dict[str, float | int | str | bool]]:
        if not fold_scores_by_token:
            return []
        max_samples = max((len(x) for x in fold_scores_by_token.values()), default=0)
        if max_samples <= 0:
            return []
        out: list[dict[str, float | int | str | bool]] = []
        for fold_idx in range(max_samples):
            best_token = None
            best_score = None
            for token, scores in fold_scores_by_token.items():
                if fold_idx >= len(scores):
                    continue
                score = float(scores[fold_idx])
                if best_score is None or score > best_score:
                    best_score = score
                    best_token = token
            if best_token is None:
                continue
            params = params_by_token.get(best_token)
            if params is None:
                continue
            out.append(dict(params))
        return out

    @classmethod
    def _param_drift_score(
        cls,
        *,
        candidate: dict[str, float | int | str | bool],
        fold_best_params: list[dict[str, float | int | str | bool]],
    ) -> float:
        if not fold_best_params:
            return 0.0
        distances = [cls._param_distance(candidate, anchor) for anchor in fold_best_params]
        return float(sum(distances) / len(distances))

    @classmethod
    def _param_distance(
        cls,
        left: dict[str, float | int | str | bool],
        right: dict[str, float | int | str | bool],
    ) -> float:
        keys = set(left.keys()) | set(right.keys())
        if not keys:
            return 0.0
        score = 0.0
        for key in keys:
            a = left.get(key)
            b = right.get(key)
            if isinstance(a, bool) or isinstance(b, bool):
                score += 0.0 if bool(a) == bool(b) else 1.0
                continue
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                af = float(a)
                bf = float(b)
                denom = abs(af) + abs(bf) + 1e-6
                score += min(1.0, abs(af - bf) / denom)
                continue
            score += 0.0 if str(a) == str(b) else 1.0
        return score / float(len(keys))
