from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from trading_assistant.applied_stats.statistics import (
    bootstrap_confidence_interval,
    correlation_matrix_with_p_values,
    jarque_bera_test,
    ols_regression,
    ridge_regression,
    ridge_select_alpha_cv,
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
        if include_fundamentals and self.fundamental_service is not None:
            bars_for_features, _ = self.fundamental_service.enrich_bars(
                symbol=symbol,
                bars=bars,
                as_of=end_date,
            )

        factors = self.factor_engine.compute(bars_for_features).sort_values("trade_date").copy()
        factors["ret_next_1d"] = factors["close"].shift(-1) / factors["close"] - 1.0
        factors["log_turnover20"] = np.log(pd.to_numeric(factors["turnover20"], errors="coerce").clip(lower=1.0))

        research_columns = [
            "trade_date",
            "ret_next_1d",
            "ret_1d",
            "momentum20",
            "volatility20",
            "zscore20",
            "log_turnover20",
            "fundamental_score",
        ]
        dataset = factors[research_columns].copy()

        model_cols = [
            "ret_next_1d",
            "momentum20",
            "volatility20",
            "zscore20",
            "log_turnover20",
            "fundamental_score",
        ]
        model_ready = dataset[model_cols].apply(pd.to_numeric, errors="coerce").dropna().copy()
        if len(model_ready) < 24:
            raise ValueError(
                f"Insufficient observations for applied statistics study: {len(model_ready)} rows. "
                "Try extending date range."
            )

        target = model_ready["ret_next_1d"]
        predictors_all = ["momentum20", "volatility20", "zscore20", "log_turnover20", "fundamental_score"]

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

        descriptive = {
            col: summarize_series(model_ready[col].to_numpy(dtype=float))
            for col in ["ret_next_1d", *predictors_all]
        }
        target_normality = jarque_bera_test(target.to_numpy(dtype=float))
        correlation = correlation_matrix_with_p_values(model_ready, ["ret_next_1d", *predictors])

        momentum_median = float(np.median(model_ready["momentum20"].to_numpy(dtype=float)))
        high_momentum = model_ready.loc[model_ready["momentum20"] >= momentum_median, "ret_next_1d"].to_numpy(dtype=float)
        low_momentum = model_ready.loc[model_ready["momentum20"] < momentum_median, "ret_next_1d"].to_numpy(dtype=float)
        mean_test = two_sample_mean_test(
            high_momentum,
            low_momentum,
            equal_var=False,
            permutations=permutations,
            random_seed=random_seed,
        )
        mean_test["group_a_name"] = "high_momentum"
        mean_test["group_b_name"] = "low_momentum"
        mean_test["split_threshold"] = momentum_median

        bootstrap_ci = bootstrap_confidence_interval(
            target.to_numpy(dtype=float),
            statistic="mean",
            bootstrap_samples=bootstrap_samples,
            random_seed=random_seed,
        )

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

        interpretation = [
            (
                "目标变量（次日收益率）正态性检验"
                + ("通过" if float(target_normality["p_value"]) >= 0.05 else "未通过")
                + f"，p={float(target_normality['p_value']):.4f}。"
            ),
            (
                "高动量与低动量两组均值差异"
                + ("显著" if float(mean_test["p_value_permutation"]) < 0.05 else "不显著")
                + f"，置换检验 p={float(mean_test['p_value_permutation']):.4f}。"
            ),
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
            "sample_size": int(len(model_ready)),
            "predictors": predictors,
            "predictors_all": predictors_all,
            "dropped_predictors": dropped_predictors,
            "descriptive_statistics": descriptive,
            "target_normality": target_normality,
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
        lines.append("")

        lines.append("## 研究流程")
        lines.append("1. 描述统计与分布检验")
        lines.append("2. 组间均值差异检验（高动量 vs 低动量）")
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
