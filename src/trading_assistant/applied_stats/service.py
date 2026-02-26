from __future__ import annotations

import itertools

from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_assistant.applied_stats.statistics import (
    bootstrap_confidence_interval,
    correlation_matrix_with_p_values,
    information_coefficient,
    jarque_bera_test,
    ols_regression,
    ridge_regression,
    ridge_select_alpha_cv,
    rolling_information_coefficient,
    summarize_series,
    two_sample_mean_test,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService


class AppliedStatisticsService:
    """Applied statistics workflows on top of existing project datasets."""

    def __init__(
        self,
        *,
        provider: CompositeDataProvider,
        factor_engine: FactorEngine,
        fundamental_service: FundamentalService | None = None,
    ) -> None:
        self.provider = provider
        self.factor_engine = factor_engine
        self.fundamental_service = fundamental_service

    def descriptive_analysis(
        self,
        *,
        rows: list[dict[str, Any]],
        columns: list[str] | None = None,
        dataset_name: str = "custom_dataset",
    ) -> dict[str, Any]:
        if not rows:
            raise ValueError("rows must not be empty.")
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError("rows produced an empty dataframe.")

        numeric = frame.apply(pd.to_numeric, errors="coerce")
        if columns:
            selected = [col for col in columns if col in numeric.columns]
            if not selected:
                raise ValueError("None of the requested columns exist in dataset.")
        else:
            selected = [col for col in numeric.columns if numeric[col].notna().any()]
            if not selected:
                raise ValueError("No numeric columns available in dataset.")

        descriptive = {col: summarize_series(numeric[col].to_numpy(dtype=float)) for col in selected}
        corr = correlation_matrix_with_p_values(numeric, selected) if len(selected) > 1 else None
        return {
            "dataset_name": dataset_name,
            "row_count": int(len(frame)),
            "numeric_columns": selected,
            "descriptive_statistics": descriptive,
            "correlation": corr,
        }

    def two_sample_test(
        self,
        *,
        sample_a: list[float | int | None],
        sample_b: list[float | int | None],
        equal_var: bool = False,
        permutations: int = 2000,
        random_seed: int = 42,
        group_a_name: str = "group_a",
        group_b_name: str = "group_b",
    ) -> dict[str, Any]:
        result = two_sample_mean_test(
            sample_a,
            sample_b,
            equal_var=equal_var,
            permutations=permutations,
            random_seed=random_seed,
        )
        result["group_a_name"] = group_a_name
        result["group_b_name"] = group_b_name
        return result

    def ols_analysis(
        self,
        *,
        target: list[float | int | None],
        features: dict[str, list[float | int | None]],
    ) -> dict[str, Any]:
        if not features:
            raise ValueError("features must not be empty.")
        feature_names = list(features.keys())
        lengths = {name: len(values) for name, values in features.items()}
        expected = len(target)
        if any(length != expected for length in lengths.values()):
            raise ValueError("target and all feature vectors must have identical lengths.")
        frame = pd.DataFrame(features).apply(pd.to_numeric, errors="coerce")
        y = pd.to_numeric(pd.Series(target), errors="coerce")
        return ols_regression(
            features=frame[feature_names],
            target=y,
            feature_names=feature_names,
        )

    def ridge_analysis(
        self,
        *,
        target: list[float | int | None],
        features: dict[str, list[float | int | None]],
        alpha: float | None = None,
        alpha_grid: list[float] | None = None,
        cv_folds: int = 5,
        standardize: bool = True,
        random_seed: int = 42,
    ) -> dict[str, Any]:
        if not features:
            raise ValueError("features must not be empty.")
        feature_names = list(features.keys())
        expected = len(target)
        if expected <= 0:
            raise ValueError("target must not be empty.")
        for name, values in features.items():
            if len(values) != expected:
                raise ValueError("target and all feature vectors must have identical lengths.")

        frame = pd.DataFrame(features).apply(pd.to_numeric, errors="coerce")
        y = pd.to_numeric(pd.Series(target), errors="coerce")

        cv = None
        if alpha is None:
            cv = ridge_select_alpha_cv(
                features=frame[feature_names],
                target=y,
                alphas=alpha_grid,
                folds=int(cv_folds),
                standardize=bool(standardize),
                random_seed=int(random_seed),
            )
            alpha = float(cv["best_alpha"])

        result = ridge_regression(
            features=frame[feature_names],
            target=y,
            feature_names=feature_names,
            alpha=float(alpha),
            standardize=bool(standardize),
        )
        if cv is not None:
            result["cv"] = cv
        return result

    def market_factor_study(
        self,
        *,
        symbol: str,
        start_date: date,
        end_date: date,
        include_fundamentals: bool = True,
        permutations: int = 2000,
        bootstrap_samples: int = 2000,
        random_seed: int = 42,
        export_markdown: bool = False,
    ) -> dict[str, Any]:
        if start_date > end_date:
            raise ValueError("start_date must be <= end_date.")

        provider_name, bars = self.provider.get_daily_bars_with_source(symbol, start_date, end_date)
        if bars.empty:
            raise ValueError("No market data available for requested range.")

        bars_for_features = bars
        fundamental_enrichment: dict[str, object] | None = None
        if include_fundamentals and self.fundamental_service is not None:
            bars_for_features, fundamental_enrichment = self.fundamental_service.enrich_bars_point_in_time(
                symbol=symbol,
                bars=bars,
                max_staleness_days=540,
                anchor_frequency="month",
            )

        factors = self.factor_engine.compute(bars_for_features).sort_values("trade_date").copy()
        target_horizons = [5, 10, 20]
        fixed_target_horizon = 10
        if fixed_target_horizon not in target_horizons:
            target_horizons.append(fixed_target_horizon)
            target_horizons = sorted(set(target_horizons))
        for horizon in target_horizons:
            factors[f"ret_next_{horizon}d"] = factors["close"].shift(-int(horizon)) / factors["close"] - 1.0
        # Use strict 20-day realized volatility for research:
        # first 20 rows remain NaN and will be dropped later instead of being filled with 0.
        ret_for_vol = pd.to_numeric(factors["close"], errors="coerce").pct_change()
        factors["volatility20"] = ret_for_vol.rolling(window=20, min_periods=20).std()
        factors["log_turnover20"] = np.log(pd.to_numeric(factors["turnover20"], errors="coerce").clip(lower=1.0))

        # (1) Improve momentum: do NOT use 0 for insufficient history.
        #     e.g. momentum20 is only valid from the 21st observation onward.
        momentum_horizons = [5, 20, 60, 120]
        for horizon in momentum_horizons:
            factors[f"momentum{horizon}"] = factors["close"].pct_change(int(horizon))

        momentum_cols = [f"momentum{h}" for h in momentum_horizons]
        target_cols = [f"ret_next_{h}d" for h in target_horizons]
        research_columns = [
            "trade_date",
            *target_cols,
            "ret_1d",
            *momentum_cols,
            "volatility20",
            "zscore20",
            "log_turnover20",
        ]
        dataset = factors[research_columns].copy()
        numeric = dataset.drop(columns=["trade_date"]).apply(pd.to_numeric, errors="coerce")

        def _two_tail_mean_test(
            *,
            factor_series: pd.Series,
            target_series: pd.Series,
            low_pct: float = 0.30,
            high_pct: float = 0.70,
        ) -> dict[str, object]:
            tmp = pd.DataFrame(
                {
                    "factor": pd.to_numeric(factor_series, errors="coerce"),
                    "target": pd.to_numeric(target_series, errors="coerce"),
                }
            ).dropna()
            n_pairs = int(len(tmp))
            if n_pairs < 8:
                return {"available": False, "reason": "insufficient_pairs", "n_pairs": n_pairs}

            ranks = tmp["factor"].rank(method="average", pct=True)
            high = tmp.loc[ranks >= float(high_pct), "target"].to_numpy(dtype=float)
            low = tmp.loc[ranks <= float(low_pct), "target"].to_numpy(dtype=float)
            if int(high.size) < 2 or int(low.size) < 2:
                return {
                    "available": False,
                    "reason": "insufficient_group_sizes",
                    "n_pairs": n_pairs,
                    "n_high": int(high.size),
                    "n_low": int(low.size),
                }

            out = two_sample_mean_test(
                high,
                low,
                equal_var=False,
                permutations=permutations,
                random_seed=random_seed,
            )
            out["available"] = True
            out["group_a_name"] = f"top_{int(round((1.0 - float(high_pct)) * 100.0))}pct"
            out["group_b_name"] = f"bottom_{int(round(float(low_pct) * 100.0))}pct"
            out["split_low_pct"] = float(low_pct)
            out["split_high_pct"] = float(high_pct)
            out["threshold_low"] = float(tmp["factor"].quantile(float(low_pct)))
            out["threshold_high"] = float(tmp["factor"].quantile(float(high_pct)))
            out["n_pairs"] = n_pairs
            out["dropped_middle_pct"] = float(max(0.0, float(high_pct) - float(low_pct)))
            return out

        def _evaluate_factor(
            *,
            name: str,
            factor_series: pd.Series,
            target_col: str,
            include_rolling_series: bool = False,
            rolling_window: int = 60,
        ) -> dict[str, object]:
            y = numeric[target_col]
            x = pd.to_numeric(factor_series, errors="coerce")
            pairs = pd.DataFrame({"x": x, "y": y}).dropna()
            n_pairs = int(len(pairs))

            ic_sp = information_coefficient(x, y, method="spearman")
            ic_pe = information_coefficient(x, y, method="pearson")
            rolling = rolling_information_coefficient(
                trade_dates=dataset["trade_date"],
                factor=x,
                target=y,
                window=int(rolling_window),
                method="spearman",
                min_obs=min(20, max(8, int(rolling_window // 3))),
            )
            series_tail = list(rolling.get("series") or [])[-20:]
            rolling_payload: dict[str, object] = {
                "window": int(rolling.get("window", rolling_window)),
                "method": str(rolling.get("method", "spearman")),
                "min_obs": int(rolling.get("min_obs", 0)),
                "summary": rolling.get("summary"),
                "series_tail": series_tail,
            }
            if include_rolling_series:
                rolling_payload["series"] = rolling.get("series")

            group_test = _two_tail_mean_test(factor_series=x, target_series=y, low_pct=0.30, high_pct=0.70)

            return {
                "name": str(name),
                "target_col": str(target_col),
                "available": bool(n_pairs >= 30),
                "n_pairs": n_pairs,
                "ic": {"spearman": ic_sp, "pearson": ic_pe},
                "rolling_ic": rolling_payload,
                "group_mean_test": group_test,
            }

        horizon_evaluations: list[dict[str, object]] = []
        for target_horizon in target_horizons:
            target_col = f"ret_next_{target_horizon}d"
            for momentum_horizon in momentum_horizons:
                col = f"momentum{momentum_horizon}"
                ev = _evaluate_factor(
                    name=col,
                    factor_series=numeric[col],
                    target_col=target_col,
                    include_rolling_series=False,
                )
                ev["horizon"] = int(momentum_horizon)
                ev["target_horizon"] = int(target_horizon)
                horizon_evaluations.append(ev)

        def _eval_score(item: dict[str, object]) -> float:
            rolling_summary = (
                ((item.get("rolling_ic") or {}).get("summary") or {}) if isinstance(item.get("rolling_ic"), dict) else {}
            )
            mean_ic = rolling_summary.get("mean")
            if isinstance(mean_ic, (int, float)) and np.isfinite(mean_ic):
                return float(abs(float(mean_ic)))
            ic = (item.get("ic") or {}).get("spearman") if isinstance(item.get("ic"), dict) else None
            if isinstance(ic, (int, float)) and np.isfinite(ic):
                return float(abs(float(ic)))
            return -1.0

        selected_target_horizon = int(fixed_target_horizon)
        available_single = [
            ev
            for ev in horizon_evaluations
            if bool(ev.get("available")) and int(ev.get("target_horizon", -1)) == selected_target_horizon
        ]
        best_single = max(available_single, key=_eval_score) if available_single else None
        selected_horizon = int(best_single["horizon"]) if best_single is not None else 20
        selected_momentum_col = f"momentum{selected_horizon}"
        selected_target_col = f"ret_next_{selected_target_horizon}d"

        selected_horizon_evaluation = _evaluate_factor(
            name=selected_momentum_col,
            factor_series=numeric[selected_momentum_col],
            target_col=selected_target_col,
            include_rolling_series=True,
        )
        selected_horizon_evaluation["horizon"] = int(selected_horizon)
        selected_horizon_evaluation["target_horizon"] = int(selected_target_horizon)

        combo_candidates: list[dict[str, object]] = []

        def _zscore(series: pd.Series) -> pd.Series:
            s = pd.to_numeric(series, errors="coerce")
            mu = float(s.mean(skipna=True))
            sd = float(s.std(skipna=True, ddof=0))
            if (not np.isfinite(sd)) or abs(sd) <= 1e-12:
                return s * np.nan
            return (s - mu) / sd

        for r in range(2, len(momentum_horizons) + 1):
            for subset in itertools.combinations(momentum_horizons, r):
                subset_cols = [f"momentum{h}" for h in subset]
                z = pd.concat([_zscore(numeric[c]) for c in subset_cols], axis=1)
                # Require all components to be present to keep a stable factor definition over time.
                combo = z.mean(axis=1, skipna=False)
                name = "momentum_combo_" + "_".join(str(h) for h in subset)
                ev = _evaluate_factor(
                    name=name,
                    factor_series=combo,
                    target_col=selected_target_col,
                    include_rolling_series=False,
                )
                ev["horizons"] = [int(h) for h in subset]
                ev["target_horizon"] = int(selected_target_horizon)
                combo_candidates.append(ev)

        available_combos = [ev for ev in combo_candidates if bool(ev.get("available"))]
        combo_sorted = sorted(available_combos, key=_eval_score, reverse=True)
        top_combos = combo_sorted[:5]
        best_combo = top_combos[0] if top_combos else None

        mean_tests_by_target: dict[str, dict[str, object]] = {}
        for target_horizon in target_horizons:
            target_col = f"ret_next_{target_horizon}d"
            mt = _two_tail_mean_test(
                factor_series=numeric[selected_momentum_col],
                target_series=numeric[target_col],
                low_pct=0.30,
                high_pct=0.70,
            )
            mt["target_horizon"] = int(target_horizon)
            mt["target_column"] = str(target_col)
            mean_tests_by_target[f"T+{target_horizon}"] = mt

        # Primary regression uses the selected horizon momentum + controls.
        model_cols = [
            selected_target_col,
            selected_momentum_col,
            "volatility20",
            "zscore20",
            "log_turnover20",
        ]
        model_ready = numeric[model_cols].dropna().copy()
        if len(model_ready) < 24:
            raise ValueError(
                f"Insufficient observations for applied statistics study: {len(model_ready)} rows. "
                "Try extending date range."
            )

        target = model_ready[selected_target_col]
        predictors_all = [selected_momentum_col, "volatility20", "zscore20", "log_turnover20"]

        dropped_predictors: list[str] = []
        for name in predictors_all:
            series = pd.to_numeric(model_ready[name], errors="coerce")
            std_val = float(series.std(ddof=1)) if len(series) > 1 else 0.0
            nunique = int(series.nunique(dropna=True))
            if (not np.isfinite(std_val)) or abs(std_val) <= 1e-12 or nunique <= 1:
                dropped_predictors.append(name)

        predictors = [name for name in predictors_all if name not in dropped_predictors]
        if not predictors:
            raise ValueError(
                "All predictors have zero variance in the analysis window. "
                "Try extending date range or disable fundamental enrichment."
            )

        descriptive = {col: summarize_series(numeric[col].to_numpy(dtype=float)) for col in numeric.columns}
        target_normality = jarque_bera_test(target.to_numpy(dtype=float))
        target_normality["target_column"] = str(selected_target_col)
        target_normality["target_horizon"] = int(selected_target_horizon)

        correlation_columns = [*target_cols, *momentum_cols, "volatility20", "zscore20", "log_turnover20"]
        correlation = correlation_matrix_with_p_values(numeric, correlation_columns)

        # (2) New grouping: top 30% vs bottom 30%, drop middle 40%.
        selected_group_key = f"T+{selected_target_horizon}"
        mean_test = dict(mean_tests_by_target.get(selected_group_key) or {})
        mean_test["selected"] = True
        mean_test["selected_target_horizon"] = int(selected_target_horizon)
        mean_test["by_horizon"] = mean_tests_by_target

        bootstrap_ci = bootstrap_confidence_interval(
            target.to_numpy(dtype=float),
            statistic="mean",
            bootstrap_samples=bootstrap_samples,
            random_seed=random_seed,
        )
        bootstrap_ci["target_column"] = str(selected_target_col)
        bootstrap_ci["target_horizon"] = int(selected_target_horizon)

        ols = ols_regression(
            features=model_ready[predictors],
            target=target,
            feature_names=predictors,
        )

        alpha_grid = [float(v) for v in np.logspace(-4, 4, num=25)]
        ridge_cv = ridge_select_alpha_cv(
            features=model_ready[predictors],
            target=target,
            alphas=alpha_grid,
            folds=5,
            standardize=True,
            random_seed=random_seed,
        )
        ridge = ridge_regression(
            features=model_ready[predictors],
            target=target,
            feature_names=predictors,
            alpha=float(ridge_cv["best_alpha"]),
            standardize=True,
        )
        ridge["cv"] = ridge_cv

        max_abs_corr = None
        cond_standardized = None
        if len(predictors) >= 2:
            corr_abs = model_ready[predictors].corr().abs()
            np.fill_diagonal(corr_abs.values, 0.0)
            value = float(np.nanmax(corr_abs.values))
            max_abs_corr = value if np.isfinite(value) else None

        x = model_ready[predictors].to_numpy(dtype=float)
        x_mean = x.mean(axis=0)
        x_std = x.std(axis=0, ddof=0)
        x_std = np.where(np.abs(x_std) <= 1e-12, 1.0, x_std)
        z = (x - x_mean) / x_std
        try:
            cond_standardized = float(np.linalg.cond(np.column_stack([np.ones(len(z)), z])))
        except Exception:  # noqa: BLE001
            cond_standardized = None

        significant_terms = [
            item["term"]
            for item in ols["coefficients"]
            if item["term"] != "intercept" and float(item["p_value_normal_approx"]) < 0.05
        ]

        selected_rolling_summary = None
        if isinstance(selected_horizon_evaluation.get("rolling_ic"), dict):
            selected_rolling_summary = selected_horizon_evaluation["rolling_ic"].get("summary")

        best_momentum_sentence = (
            f"目标收益期固定为 `T+{selected_target_horizon}`：在该目标下选择 `{selected_momentum_col}` "
            "作为主动量（按 rolling IC 均值绝对值筛选）。"
        )
        if isinstance(selected_rolling_summary, dict) and selected_rolling_summary.get("mean") is not None:
            mean_ic = selected_rolling_summary.get("mean")
            t_stat = selected_rolling_summary.get("t_stat")
            pos_ratio = selected_rolling_summary.get("positive_ratio")
            if isinstance(mean_ic, (int, float)) and np.isfinite(mean_ic):
                detail = f"rolling IC(mean)={float(mean_ic):.4f}"
                if isinstance(t_stat, (int, float)) and np.isfinite(t_stat):
                    detail += f", t={float(t_stat):.2f}"
                if isinstance(pos_ratio, (int, float)) and np.isfinite(pos_ratio):
                    detail += f", positive_ratio={float(pos_ratio):.2%}"
                best_momentum_sentence = f"{best_momentum_sentence}（{detail}, target=T+{selected_target_horizon}）"

        combo_sentence = None
        if isinstance(best_combo, dict):
            combo_name = str(best_combo.get("name") or "")
            combo_summary = None
            if isinstance(best_combo.get("rolling_ic"), dict):
                combo_summary = best_combo["rolling_ic"].get("summary")
            if isinstance(combo_summary, dict) and combo_summary.get("mean") is not None:
                mean_ic = combo_summary.get("mean")
                if isinstance(mean_ic, (int, float)) and np.isfinite(mean_ic):
                    combo_sentence = (
                        f"动量组合候选中最优：`{combo_name}`（rolling IC(mean)={float(mean_ic):.4f}, "
                        f"target=T+{selected_target_horizon}）。"
                    )
            else:
                combo_sentence = f"动量组合候选中最优：`{combo_name}`（target=T+{selected_target_horizon}）。"

        group_sentence = (
            f"动量分组检验（Top 30% vs Bottom 30%，中间 40% 丢弃，目标 T+{selected_target_horizon}）"
            "两组均值差异：-"
        )
        if isinstance(mean_test, dict) and bool(mean_test.get("available", True)) and mean_test.get("p_value_permutation") is not None:
            p_perm = float(mean_test["p_value_permutation"])
            group_sentence = (
                f"动量分组检验（Top 30% vs Bottom 30%，中间 40% 丢弃，目标 T+{selected_target_horizon}）两组均值差异"
                + ("显著" if p_perm < 0.05 else "不显著")
                + f"，置换检验 p={p_perm:.4f}。"
            )
        elif isinstance(mean_test, dict) and mean_test.get("available") is False:
            reason = str(mean_test.get("reason") or "unknown")
            group_sentence = f"动量分组检验无法进行（{reason}）。"

        horizon_group_p = []
        for h in target_horizons:
            key = f"T+{h}"
            mt = mean_tests_by_target.get(key)
            p = mt.get("p_value_permutation") if isinstance(mt, dict) else None
            if isinstance(p, (int, float)) and np.isfinite(p):
                horizon_group_p.append(f"T+{h}: p={float(p):.4f}")
            else:
                horizon_group_p.append(f"T+{h}: NA")
        group_multi_sentence = "动量分组检验跨收益期结果：" + "；".join(horizon_group_p) + "。"

        interpretation = [
            (
                f"目标变量（T+{selected_target_horizon} 收益率）正态性检验"
                + ("通过" if float(target_normality["p_value"]) >= 0.05 else "未通过")
                + f"，p={float(target_normality['p_value']):.4f}。"
            ),
            best_momentum_sentence,
            *( [combo_sentence] if combo_sentence else [] ),
            group_sentence,
            group_multi_sentence,
            (
                f"OLS 回归 R²={float(ols['r2']):.4f}，调整后 R²={float(ols['adjusted_r2']):.4f}。"
                + (f"（5% 显著变量：{', '.join(significant_terms)}）" if significant_terms else "（5% 显著变量：无）")
            ),
            (
                f"岭回归用于缓解多重共线性（保留全部变量），alpha={float(ridge['alpha']):.4g}，R²={float(ridge['r2']):.4f}。"
            ),
        ]
        if max_abs_corr is not None:
            interpretation.append(f"自变量最大绝对相关系数约 {max_abs_corr:.3f}，存在多重共线性风险。")

        report: dict[str, Any] = {
            "study_name": "market_factor_applied_statistics_case",
            "symbol": symbol,
            "provider": provider_name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "fundamental_enrichment": fundamental_enrichment,
            "sample_size": int(len(model_ready)),
            "target_horizons": target_horizons,
            "target_selected": selected_target_col,
            "target_selected_horizon": int(selected_target_horizon),
            "target_selection_mode": "fixed_horizon",
            "momentum_candidates": momentum_cols,
            "momentum_selected": selected_momentum_col,
            "momentum_horizon_analysis": {
                "horizons": momentum_horizons,
                "target_horizons": target_horizons,
                "evaluations": horizon_evaluations,
                "selected": selected_horizon_evaluation,
            },
            "momentum_combo_analysis": {
                "top": top_combos,
                "best": best_combo,
                "scoring": "abs(rolling_ic.mean) fallback abs(IC_spearman)",
            },
            "predictors": predictors,
            "predictors_all": predictors_all,
            "dropped_predictors": dropped_predictors,
            "descriptive_statistics": descriptive,
            "target_normality": target_normality,
            "group_test_target_horizons": mean_tests_by_target,
            "momentum_group_mean_test": mean_test,
            "target_bootstrap_ci": bootstrap_ci,
            "correlation": correlation,
            "collinearity_diagnostics": {
                "max_abs_corr": max_abs_corr,
                "condition_number_standardized": cond_standardized,
                "vif": ols.get("vif"),
            },
            "ols": ols,
            "ridge": ridge,
            "interpretation": interpretation,
            "notes": [
                "主目标函数固定为 T+10 前瞻收益（ret_next_10d）；T+5/T+20 仅作为对照检验，不再使用 T+1。",
                "OLS 在多重共线性下系数和显著性可能不稳定；岭回归通过 L2 正则化降低方差以获得更稳定估计。",
                (
                    "Zero-variance predictors are excluded from correlation/regression: "
                    + (", ".join(dropped_predictors) if dropped_predictors else "none")
                    + "."
                ),
                "p 值采用正态近似，仅用于工程内快速研究；正式论文建议用 scipy/statsmodels 复核。",
            ],
        }

        if export_markdown:
            report["markdown_report_path"] = self._export_market_study_markdown(report)
        return report

    def _export_market_study_markdown(self, report: dict[str, Any]) -> str:
        output_dir = Path("reports")
        output_dir.mkdir(parents=True, exist_ok=True)
        safe_symbol = "".join(ch for ch in str(report.get("symbol", "unknown")) if ch.isalnum() or ch in ("_", "-"))
        filename = f"applied_stats_{safe_symbol}_{report.get('start_date', 'start')}_{report.get('end_date', 'end')}.md"
        target_path = output_dir / filename

        lines: list[str] = []
        lines.append("# 应用统计案例研究报告")
        lines.append("")
        lines.append(f"- 研究对象: `{report.get('symbol')}`")
        lines.append(f"- 数据来源: `{report.get('provider')}`")
        lines.append(f"- 时间区间: `{report.get('start_date')} ~ {report.get('end_date')}`")
        lines.append(f"- 样本量: `{report.get('sample_size')}`")
        lines.append(f"- 主检验收益期: `T+{report.get('target_selected_horizon')}`")
        lines.append("")

        lines.append("## 研究流程")
        lines.append("1. 描述统计与分布检验")
        lines.append("2. 动量因子有效性检验（IC / Rolling IC / 分组检验）")
        lines.append("3. 回归建模：OLS 与 Ridge（缓解多重共线性）")
        lines.append("4. 诊断与结论解释")
        lines.append("")

        lines.append("## 关键结论")
        for sentence in report.get("interpretation", []):
            lines.append(f"- {sentence}")
        lines.append("")

        ols = report.get("ols", {})
        lines.append("## OLS 主要指标")
        lines.append(f"- R²: `{float(ols.get('r2', 0.0)):.6f}`")
        lines.append(f"- Adjusted R²: `{float(ols.get('adjusted_r2', 0.0)):.6f}`")
        lines.append(f"- RMSE: `{float(ols.get('rmse', 0.0)):.6f}`")
        lines.append(f"- Durbin-Watson: `{float(ols.get('durbin_watson', 0.0)):.6f}`")
        lines.append("")

        momentum_selected = report.get("momentum_selected")
        momentum_horizon = (report.get("momentum_horizon_analysis") or {}) if isinstance(report.get("momentum_horizon_analysis"), dict) else {}
        momentum_selected_detail = (momentum_horizon.get("selected") or {}) if isinstance(momentum_horizon.get("selected"), dict) else {}
        selected_ic = (momentum_selected_detail.get("ic") or {}) if isinstance(momentum_selected_detail.get("ic"), dict) else {}
        selected_roll = (momentum_selected_detail.get("rolling_ic") or {}) if isinstance(momentum_selected_detail.get("rolling_ic"), dict) else {}
        selected_roll_summary = (selected_roll.get("summary") or {}) if isinstance(selected_roll.get("summary"), dict) else {}

        lines.append("## 动量因子有效性")
        lines.append(f"- 候选周期: `{report.get('momentum_candidates')}`")
        lines.append(f"- 选择周期: `{momentum_selected}`")
        lines.append(f"- 目标收益期: `T+{report.get('target_selected_horizon')}`")
        lines.append(f"- IC(Spearman): `{selected_ic.get('spearman')}`")
        lines.append(f"- Rolling IC(mean): `{selected_roll_summary.get('mean')}`")
        lines.append(f"- Rolling IC(t): `{selected_roll_summary.get('t_stat')}`")
        lines.append(f"- Rolling IC(positive_ratio): `{selected_roll_summary.get('positive_ratio')}`")

        group_test = report.get("momentum_group_mean_test") or {}
        if isinstance(group_test, dict) and group_test.get("p_value_permutation") is not None:
            lines.append(
                f"- Top30 vs Bottom30 mean_diff (T+{group_test.get('target_horizon')}): `{group_test.get('mean_diff')}`"
            )
            lines.append(
                f"- Top30 vs Bottom30 p(permutation) (T+{group_test.get('target_horizon')}): `{group_test.get('p_value_permutation')}`"
            )
        elif isinstance(group_test, dict) and group_test.get("available") is False:
            lines.append(f"- 分组检验未执行: `{group_test.get('reason')}`")
        by_target = report.get("group_test_target_horizons")
        if isinstance(by_target, dict):
            lines.append("- 分组检验跨收益期:")
            for key in ("T+5", "T+10", "T+20"):
                item = by_target.get(key)
                if isinstance(item, dict):
                    p = item.get("p_value_permutation")
                    lines.append(f"  - {key}: p(permutation)={p}")
        lines.append("")

        evals = momentum_horizon.get("evaluations") if isinstance(momentum_horizon.get("evaluations"), list) else []
        if evals:
            lines.append("### 单周期对比（摘要）")
            lines.append("| horizon | target_horizon | n_pairs | ic_spearman | rolling_ic_mean | rolling_ic_t | group_p_perm |")
            lines.append("|---:|---:|---:|---:|---:|---:|---:|")
            for item in evals:
                if not isinstance(item, dict):
                    continue
                h = item.get("horizon")
                th = item.get("target_horizon")
                n_pairs = item.get("n_pairs")
                ic_sp = None
                if isinstance(item.get("ic"), dict):
                    ic_sp = item["ic"].get("spearman")
                roll_mean = None
                roll_t = None
                if isinstance(item.get("rolling_ic"), dict) and isinstance(item["rolling_ic"].get("summary"), dict):
                    roll_mean = item["rolling_ic"]["summary"].get("mean")
                    roll_t = item["rolling_ic"]["summary"].get("t_stat")
                gp = None
                if isinstance(item.get("group_mean_test"), dict):
                    gp = item["group_mean_test"].get("p_value_permutation")
                lines.append(f"| {h} | {th} | {n_pairs} | {ic_sp} | {roll_mean} | {roll_t} | {gp} |")
            lines.append("")

        combo = report.get("momentum_combo_analysis") or {}
        if isinstance(combo, dict) and isinstance(combo.get("top"), list) and combo.get("top"):
            lines.append("### 动量组合 Top（摘要）")
            lines.append("| name | horizons | target_horizon | n_pairs | rolling_ic_mean | group_p_perm |")
            lines.append("|---|---|---:|---:|---:|---:|")
            for item in combo.get("top") or []:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                horizons = item.get("horizons")
                target_horizon = item.get("target_horizon")
                n_pairs = item.get("n_pairs")
                roll_mean = None
                if isinstance(item.get("rolling_ic"), dict) and isinstance(item["rolling_ic"].get("summary"), dict):
                    roll_mean = item["rolling_ic"]["summary"].get("mean")
                gp = None
                if isinstance(item.get("group_mean_test"), dict):
                    gp = item["group_mean_test"].get("p_value_permutation")
                lines.append(f"| {name} | {horizons} | {target_horizon} | {n_pairs} | {roll_mean} | {gp} |")
            lines.append("")

        ridge = report.get("ridge", {})
        if ridge:
            lines.append("## Ridge 主要指标")
            lines.append(f"- alpha: `{float(ridge.get('alpha', 0.0)):.6g}`")
            lines.append(f"- R²: `{float(ridge.get('r2', 0.0)):.6f}`")
            lines.append(f"- RMSE: `{float(ridge.get('rmse', 0.0)):.6f}`")
            cv = ridge.get("cv") or {}
            if cv:
                lines.append(f"- CV best_rmse: `{float(cv.get('best_rmse', 0.0)):.6f}`")
            lines.append("")

        lines.append("## OLS 回归系数（节选）")
        lines.append("| term | coef | p-value | ci95_low | ci95_high |")
        lines.append("|---|---:|---:|---:|---:|")
        for row in (ols.get("coefficients") or [])[:10]:
            lines.append(
                "| {term} | {coefficient:.6f} | {p_value_normal_approx:.6f} | {ci95_low:.6f} | {ci95_high:.6f} |".format(
                    **row
                )
            )
        lines.append("")

        if ridge:
            lines.append("## Ridge 系数")
            lines.append("| term | coef | standardized_coef |")
            lines.append("|---|---:|---:|")
            for row in ridge.get("coefficients") or []:
                std_coef = row.get("standardized_coefficient")
                std_text = "" if std_coef is None else f"{float(std_coef):.6f}"
                lines.append(f"| {row.get('term')} | {float(row.get('coefficient', 0.0)):.6f} | {std_text} |")
            lines.append("")

        lines.append("## 备注")
        for note in report.get("notes", []):
            lines.append(f"- {note}")
        lines.append("")

        target_path.write_text("\n".join(lines), encoding="utf-8")
        return str(target_path)
