from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

_EPS = 1e-12
_NORMAL_95 = 1.959963984540054


def _normal_cdf(value: float) -> float:
    return 0.5 * (1.0 + math.erf(float(value) / math.sqrt(2.0)))


def _to_clean_array(values: list[float | int | None] | np.ndarray | pd.Series) -> np.ndarray:
    arr = np.asarray(values, dtype=float).reshape(-1)
    if arr.size == 0:
        return np.array([], dtype=float)
    return arr[np.isfinite(arr)]


def summarize_series(values: list[float | int | None] | np.ndarray | pd.Series) -> dict[str, float | int | None]:
    arr = _to_clean_array(values)
    if arr.size == 0:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "std": None,
            "min": None,
            "q1": None,
            "q3": None,
            "max": None,
            "skewness": None,
            "kurtosis_excess": None,
        }

    mean_val = float(np.mean(arr))
    centered = arr - mean_val
    m2 = float(np.mean(centered**2))
    if m2 <= _EPS:
        skewness = 0.0
        kurt_excess = 0.0
    else:
        m3 = float(np.mean(centered**3))
        m4 = float(np.mean(centered**4))
        skewness = m3 / (m2 ** 1.5)
        kurt_excess = m4 / (m2**2) - 3.0

    return {
        "count": int(arr.size),
        "mean": mean_val,
        "median": float(np.median(arr)),
        "std": float(np.std(arr, ddof=1)) if arr.size > 1 else 0.0,
        "min": float(np.min(arr)),
        "q1": float(np.quantile(arr, 0.25)),
        "q3": float(np.quantile(arr, 0.75)),
        "max": float(np.max(arr)),
        "skewness": float(skewness),
        "kurtosis_excess": float(kurt_excess),
    }


def jarque_bera_test(values: list[float | int | None] | np.ndarray | pd.Series) -> dict[str, float | int]:
    arr = _to_clean_array(values)
    n = int(arr.size)
    if n < 3:
        return {"n": n, "jb_stat": 0.0, "p_value": 1.0}
    stats = summarize_series(arr)
    skewness = float(stats["skewness"] or 0.0)
    kurt_excess = float(stats["kurtosis_excess"] or 0.0)
    jb = n / 6.0 * (skewness**2 + 0.25 * kurt_excess**2)
    # Chi-square(df=2) upper-tail probability: exp(-x/2).
    p_value = float(math.exp(-max(0.0, jb) / 2.0))
    return {"n": n, "jb_stat": float(jb), "p_value": p_value}


def two_sample_mean_test(
    sample_a: list[float | int | None] | np.ndarray | pd.Series,
    sample_b: list[float | int | None] | np.ndarray | pd.Series,
    *,
    equal_var: bool = False,
    permutations: int = 2000,
    random_seed: int = 42,
) -> dict[str, float | int]:
    a = _to_clean_array(sample_a)
    b = _to_clean_array(sample_b)
    n_a = int(a.size)
    n_b = int(b.size)
    if n_a < 2 or n_b < 2:
        raise ValueError("Both samples must contain at least 2 valid numeric observations.")

    mean_a = float(np.mean(a))
    mean_b = float(np.mean(b))
    diff = mean_a - mean_b
    var_a = float(np.var(a, ddof=1))
    var_b = float(np.var(b, ddof=1))

    if equal_var:
        pooled = (((n_a - 1) * var_a) + ((n_b - 1) * var_b)) / max(1, n_a + n_b - 2)
        se = math.sqrt(max(_EPS, pooled) * (1.0 / n_a + 1.0 / n_b))
        dof = float(max(1, n_a + n_b - 2))
    else:
        se_sq = var_a / n_a + var_b / n_b
        se = math.sqrt(max(_EPS, se_sq))
        dof_num = se_sq**2
        dof_den = ((var_a / n_a) ** 2) / max(1, n_a - 1) + ((var_b / n_b) ** 2) / max(1, n_b - 1)
        dof = float(dof_num / max(_EPS, dof_den))

    t_stat = float(diff / max(_EPS, se))
    p_value_normal = float(2.0 * (1.0 - _normal_cdf(abs(t_stat))))
    ci_low = float(diff - _NORMAL_95 * se)
    ci_high = float(diff + _NORMAL_95 * se)

    pooled_var_for_d = (((n_a - 1) * var_a) + ((n_b - 1) * var_b)) / max(1, n_a + n_b - 2)
    pooled_std_for_d = math.sqrt(max(_EPS, pooled_var_for_d))
    cohen_d = float(diff / pooled_std_for_d)

    rng = np.random.default_rng(random_seed)
    pooled_values = np.concatenate([a, b])
    more_extreme = 0
    perm_n = max(100, int(permutations))
    for _ in range(perm_n):
        shuffled = rng.permutation(pooled_values)
        perm_diff = float(np.mean(shuffled[:n_a]) - np.mean(shuffled[n_a:]))
        if abs(perm_diff) >= abs(diff):
            more_extreme += 1
    p_value_permutation = float((more_extreme + 1) / (perm_n + 1))

    return {
        "n_a": n_a,
        "n_b": n_b,
        "mean_a": mean_a,
        "mean_b": mean_b,
        "mean_diff": diff,
        "se": float(se),
        "t_stat": t_stat,
        "dof": dof,
        "p_value_normal_approx": p_value_normal,
        "p_value_permutation": p_value_permutation,
        "ci95_low": ci_low,
        "ci95_high": ci_high,
        "cohen_d": cohen_d,
    }


def bootstrap_confidence_interval(
    values: list[float | int | None] | np.ndarray | pd.Series,
    *,
    statistic: str = "mean",
    bootstrap_samples: int = 2000,
    random_seed: int = 42,
) -> dict[str, float | int | str]:
    arr = _to_clean_array(values)
    n = int(arr.size)
    if n < 2:
        raise ValueError("At least 2 valid numeric observations are required for bootstrap confidence interval.")

    stat_name = statistic.strip().lower()
    if stat_name == "mean":
        stat_fn = np.mean
    elif stat_name == "median":
        stat_fn = np.median
    else:
        raise ValueError("Unsupported statistic. Expected one of: mean, median.")

    rng = np.random.default_rng(random_seed)
    b = max(200, int(bootstrap_samples))
    estimates = np.empty(b, dtype=float)
    for i in range(b):
        sample = rng.choice(arr, size=n, replace=True)
        estimates[i] = float(stat_fn(sample))

    estimate = float(stat_fn(arr))
    ci_low = float(np.quantile(estimates, 0.025))
    ci_high = float(np.quantile(estimates, 0.975))
    return {
        "statistic": stat_name,
        "estimate": estimate,
        "ci95_low": ci_low,
        "ci95_high": ci_high,
        "bootstrap_samples": b,
    }


def correlation_matrix_with_p_values(
    frame: pd.DataFrame,
    columns: list[str],
) -> dict[str, Any]:
    if not columns:
        raise ValueError("columns must not be empty.")
    sub = frame[columns].apply(pd.to_numeric, errors="coerce")
    corr_matrix: dict[str, dict[str, float | None]] = {}
    p_matrix: dict[str, dict[str, float | None]] = {}

    for col_i in columns:
        corr_matrix[col_i] = {}
        p_matrix[col_i] = {}
        for col_j in columns:
            x = sub[col_i].to_numpy(dtype=float)
            y = sub[col_j].to_numpy(dtype=float)
            mask = np.isfinite(x) & np.isfinite(y)
            x_valid = x[mask]
            y_valid = y[mask]
            n = int(x_valid.size)
            if n < 3:
                corr_matrix[col_i][col_j] = None
                p_matrix[col_i][col_j] = None
                continue
            r = float(np.corrcoef(x_valid, y_valid)[0, 1])
            if not np.isfinite(r):
                corr_matrix[col_i][col_j] = None
                p_matrix[col_i][col_j] = None
                continue
            if abs(r) >= 0.999999:
                p_value = 0.0
            else:
                t = abs(r) * math.sqrt((n - 2) / max(_EPS, 1.0 - r * r))
                p_value = float(2.0 * (1.0 - _normal_cdf(t)))
            corr_matrix[col_i][col_j] = r
            p_matrix[col_i][col_j] = p_value

    return {"correlation": corr_matrix, "p_value_normal_approx": p_matrix}


def ols_regression(
    *,
    features: pd.DataFrame | np.ndarray,
    target: pd.Series | np.ndarray,
    feature_names: list[str] | None = None,
) -> dict[str, Any]:
    x = np.asarray(features, dtype=float)
    y = np.asarray(target, dtype=float).reshape(-1)

    if x.ndim == 1:
        x = x.reshape(-1, 1)
    if x.ndim != 2:
        raise ValueError("features must be a 2D array.")
    if x.shape[0] != y.shape[0]:
        raise ValueError("features and target must have the same row count.")

    finite_mask = np.isfinite(y)
    finite_mask &= np.isfinite(x).all(axis=1)
    x = x[finite_mask]
    y = y[finite_mask]

    n, p = x.shape
    if n <= p + 1:
        raise ValueError(f"Insufficient observations for OLS: n={n}, p={p}. Need n > p + 1.")

    if feature_names is None:
        feature_names = [f"x{i + 1}" for i in range(p)]
    if len(feature_names) != p:
        raise ValueError("feature_names length must equal number of feature columns.")

    design = np.column_stack([np.ones(n), x])
    names = ["intercept"] + feature_names

    xtx = design.T @ design
    xtx_inv = np.linalg.pinv(xtx)
    beta = xtx_inv @ design.T @ y
    y_hat = design @ beta
    residuals = y - y_hat

    rss = float(np.sum(residuals**2))
    tss = float(np.sum((y - np.mean(y)) ** 2))
    dof = max(1, n - (p + 1))
    sigma2 = rss / dof
    cov = sigma2 * xtx_inv
    std_err = np.sqrt(np.clip(np.diag(cov), a_min=0.0, a_max=None))
    t_stats = np.divide(beta, std_err, out=np.zeros_like(beta), where=std_err > _EPS)
    p_values = 2.0 * (1.0 - np.vectorize(_normal_cdf)(np.abs(t_stats)))
    ci_low = beta - _NORMAL_95 * std_err
    ci_high = beta + _NORMAL_95 * std_err

    r2 = 1.0 - rss / max(_EPS, tss)
    adjusted_r2 = 1.0 - (1.0 - r2) * (n - 1) / max(1, n - p - 1)
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(math.sqrt(max(0.0, np.mean(residuals**2))))
    denom = np.where(np.abs(y) > _EPS, np.abs(y), np.nan)
    mape = float(np.nanmean(np.abs(residuals) / denom)) if np.isfinite(denom).any() else float("nan")
    durbin_watson = float(np.sum(np.diff(residuals) ** 2) / max(_EPS, rss))

    coef_table: list[dict[str, float | str]] = []
    for idx, name in enumerate(names):
        coef_table.append(
            {
                "term": name,
                "coefficient": float(beta[idx]),
                "std_error": float(std_err[idx]),
                "t_stat": float(t_stats[idx]),
                "p_value_normal_approx": float(p_values[idx]),
                "ci95_low": float(ci_low[idx]),
                "ci95_high": float(ci_high[idx]),
            }
        )

    vif: dict[str, float | None] = {}
    for idx, name in enumerate(feature_names):
        y_j = x[:, idx]
        x_others = np.delete(x, idx, axis=1)
        if x_others.shape[1] == 0:
            vif[name] = 1.0
            continue
        design_j = np.column_stack([np.ones(n), x_others])
        beta_j = np.linalg.pinv(design_j.T @ design_j) @ design_j.T @ y_j
        pred_j = design_j @ beta_j
        rss_j = float(np.sum((y_j - pred_j) ** 2))
        tss_j = float(np.sum((y_j - np.mean(y_j)) ** 2))
        if tss_j <= _EPS:
            vif[name] = None
            continue
        r2_j = 1.0 - rss_j / tss_j
        vif[name] = float(1.0 / max(_EPS, 1.0 - r2_j))

    residual_jb = jarque_bera_test(residuals)
    return {
        "n": int(n),
        "p": int(p),
        "dof": int(dof),
        "r2": float(r2),
        "adjusted_r2": float(adjusted_r2),
        "mae": mae,
        "rmse": rmse,
        "mape": None if not np.isfinite(mape) else float(mape),
        "durbin_watson": durbin_watson,
        "condition_number": float(np.linalg.cond(design)),
        "coefficients": coef_table,
        "vif": vif,
        "residual_normality": residual_jb,
        "p_value_method": "normal_approximation",
    }


def ridge_select_alpha_cv(
    *,
    features: pd.DataFrame | np.ndarray,
    target: pd.Series | np.ndarray,
    alphas: list[float] | None = None,
    folds: int = 5,
    standardize: bool = True,
    random_seed: int = 42,
) -> dict[str, Any]:
    x = np.asarray(features, dtype=float)
    y = np.asarray(target, dtype=float).reshape(-1)
    if x.ndim == 1:
        x = x.reshape(-1, 1)
    if x.ndim != 2:
        raise ValueError("features must be a 2D array.")
    if x.shape[0] != y.shape[0]:
        raise ValueError("features and target must have the same row count.")

    finite_mask = np.isfinite(y)
    finite_mask &= np.isfinite(x).all(axis=1)
    x = x[finite_mask]
    y = y[finite_mask]
    n, p = x.shape
    if n < 8:
        raise ValueError("Insufficient observations for ridge CV (need at least 8 rows).")
    if p < 1:
        raise ValueError("features must contain at least 1 column.")

    k = int(folds)
    k = max(2, min(k, n))

    if alphas is None:
        alphas = [float(v) for v in np.logspace(-4, 4, num=25)]
    alpha_list = [float(a) for a in alphas if float(a) >= 0.0]
    if not alpha_list:
        raise ValueError("alphas must contain at least one non-negative value.")

    rng = np.random.default_rng(int(random_seed))
    indices = rng.permutation(n)
    fold_sizes = [(n // k) + (1 if i < (n % k) else 0) for i in range(k)]
    folds_idx: list[np.ndarray] = []
    cursor = 0
    for size in fold_sizes:
        folds_idx.append(indices[cursor : cursor + size])
        cursor += size

    def _fit_ridge(train_x: np.ndarray, train_y: np.ndarray, alpha: float) -> tuple[np.ndarray, float]:
        x_mean = train_x.mean(axis=0)
        y_mean = float(np.mean(train_y))
        x_centered = train_x - x_mean
        y_centered = train_y - y_mean

        if standardize:
            x_std = train_x.std(axis=0, ddof=0)
            x_std = np.where(np.abs(x_std) <= _EPS, 1.0, x_std)
            z = x_centered / x_std
        else:
            x_std = np.ones(train_x.shape[1], dtype=float)
            z = x_centered

        a = z.T @ z + float(alpha) * np.eye(train_x.shape[1], dtype=float)
        try:
            beta_std = np.linalg.solve(a, z.T @ y_centered)
        except Exception:  # noqa: BLE001
            beta_std = np.linalg.pinv(a) @ (z.T @ y_centered)

        beta = beta_std / x_std
        intercept = y_mean - float(x_mean @ beta)
        return beta, intercept

    rows: list[dict[str, float]] = []
    for alpha in alpha_list:
        fold_mse: list[float] = []
        for fold in folds_idx:
            val_idx = fold
            train_idx = np.setdiff1d(indices, val_idx, assume_unique=False)
            beta, intercept = _fit_ridge(x[train_idx], y[train_idx], alpha)
            preds = intercept + x[val_idx] @ beta
            resid = y[val_idx] - preds
            fold_mse.append(float(np.mean(resid**2)))

        mean_mse = float(np.mean(fold_mse))
        rows.append({"alpha": float(alpha), "mean_mse": mean_mse, "mean_rmse": float(math.sqrt(max(0.0, mean_mse)))})

    best = min(rows, key=lambda item: item["mean_mse"])
    return {
        "folds": int(k),
        "standardize": bool(standardize),
        "alpha_grid": [item["alpha"] for item in rows],
        "mean_mse": [item["mean_mse"] for item in rows],
        "mean_rmse": [item["mean_rmse"] for item in rows],
        "best_alpha": float(best["alpha"]),
        "best_rmse": float(best["mean_rmse"]),
    }


def ridge_regression(
    *,
    features: pd.DataFrame | np.ndarray,
    target: pd.Series | np.ndarray,
    feature_names: list[str] | None = None,
    alpha: float = 1.0,
    standardize: bool = True,
) -> dict[str, Any]:
    x = np.asarray(features, dtype=float)
    y = np.asarray(target, dtype=float).reshape(-1)
    if x.ndim == 1:
        x = x.reshape(-1, 1)
    if x.ndim != 2:
        raise ValueError("features must be a 2D array.")
    if x.shape[0] != y.shape[0]:
        raise ValueError("features and target must have the same row count.")

    finite_mask = np.isfinite(y)
    finite_mask &= np.isfinite(x).all(axis=1)
    x = x[finite_mask]
    y = y[finite_mask]
    n, p = x.shape
    if n < 3 or p < 1:
        raise ValueError("Insufficient data for ridge regression.")

    if feature_names is None:
        feature_names = [f"x{i + 1}" for i in range(p)]
    if len(feature_names) != p:
        raise ValueError("feature_names length must equal number of feature columns.")

    alpha_val = float(alpha)
    if alpha_val < 0.0:
        raise ValueError("alpha must be >= 0.")

    x_mean = x.mean(axis=0)
    y_mean = float(np.mean(y))
    x_centered = x - x_mean
    y_centered = y - y_mean

    if standardize:
        x_std = x.std(axis=0, ddof=0)
        x_std = np.where(np.abs(x_std) <= _EPS, 1.0, x_std)
        z = x_centered / x_std
    else:
        x_std = np.ones(p, dtype=float)
        z = x_centered

    a = z.T @ z + alpha_val * np.eye(p, dtype=float)
    try:
        beta_std = np.linalg.solve(a, z.T @ y_centered)
    except Exception:  # noqa: BLE001
        beta_std = np.linalg.pinv(a) @ (z.T @ y_centered)

    beta = beta_std / x_std
    intercept = y_mean - float(x_mean @ beta)
    y_hat = intercept + x @ beta
    residuals = y - y_hat

    rss = float(np.sum(residuals**2))
    tss = float(np.sum((y - y_mean) ** 2))
    r2 = 1.0 - rss / max(_EPS, tss)
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(math.sqrt(max(0.0, np.mean(residuals**2))))

    coef_table: list[dict[str, float | str | None]] = [
        {"term": "intercept", "coefficient": float(intercept), "standardized_coefficient": None}
    ]
    for idx, name in enumerate(feature_names):
        coef_table.append(
            {
                "term": name,
                "coefficient": float(beta[idx]),
                "standardized_coefficient": float(beta_std[idx]) if standardize else None,
            }
        )

    return {
        "n": int(n),
        "p": int(p),
        "alpha": float(alpha_val),
        "standardize": bool(standardize),
        "r2": float(r2),
        "mae": mae,
        "rmse": rmse,
        "coefficients": coef_table,
        "method": "ridge",
    }
