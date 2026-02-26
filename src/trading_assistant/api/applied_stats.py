from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator

from trading_assistant.applied_stats.service import AppliedStatisticsService
from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_applied_statistics_service, get_audit_service
from trading_assistant.core.security import AuthContext, UserRole, require_roles

router = APIRouter(prefix="/applied-stats", tags=["applied-stats"])


class DescriptiveAnalysisRequest(BaseModel):
    dataset_name: str = Field(default="custom_dataset")
    rows: list[dict[str, Any]] = Field(default_factory=list)
    columns: list[str] | None = None

    @model_validator(mode="after")
    def _validate_rows(self) -> "DescriptiveAnalysisRequest":
        if not self.rows:
            raise ValueError("rows must not be empty.")
        return self


class TwoSampleMeanTestRequest(BaseModel):
    sample_a: list[float | int | None] = Field(default_factory=list)
    sample_b: list[float | int | None] = Field(default_factory=list)
    group_a_name: str = Field(default="group_a")
    group_b_name: str = Field(default="group_b")
    equal_var: bool = Field(default=False)
    permutations: int = Field(default=2000, ge=200, le=20000)
    random_seed: int = Field(default=42)


class OLSAnalysisRequest(BaseModel):
    target: list[float | int | None] = Field(default_factory=list)
    features: dict[str, list[float | int | None]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_lengths(self) -> "OLSAnalysisRequest":
        if not self.target:
            raise ValueError("target must not be empty.")
        if not self.features:
            raise ValueError("features must not be empty.")
        target_len = len(self.target)
        for name, values in self.features.items():
            if len(values) != target_len:
                raise ValueError(f"feature '{name}' length mismatch with target.")
        return self


class RidgeAnalysisRequest(BaseModel):
    target: list[float | int | None] = Field(default_factory=list)
    features: dict[str, list[float | int | None]] = Field(default_factory=dict)
    alpha: float | None = Field(default=None, ge=0.0)
    alpha_grid: list[float] | None = None
    cv_folds: int = Field(default=5, ge=2, le=20)
    standardize: bool = True
    random_seed: int = 42

    @model_validator(mode="after")
    def _validate_lengths(self) -> "RidgeAnalysisRequest":
        if not self.target:
            raise ValueError("target must not be empty.")
        if not self.features:
            raise ValueError("features must not be empty.")
        target_len = len(self.target)
        for name, values in self.features.items():
            if len(values) != target_len:
                raise ValueError(f"feature '{name}' length mismatch with target.")
        if self.alpha_grid is not None:
            if not self.alpha_grid:
                raise ValueError("alpha_grid must not be empty when provided.")
            bad = [a for a in self.alpha_grid if a is None or float(a) < 0.0]
            if bad:
                raise ValueError("alpha_grid must contain non-negative numeric values only.")
        return self


class MarketFactorStudyRequest(BaseModel):
    symbol: str
    start_date: date
    end_date: date
    include_fundamentals: bool = True
    permutations: int = Field(default=2000, ge=200, le=20000)
    bootstrap_samples: int = Field(default=2000, ge=200, le=20000)
    random_seed: int = 42
    export_markdown: bool = False

    @model_validator(mode="after")
    def _validate_dates(self) -> "MarketFactorStudyRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


@router.post("/descriptive")
def descriptive_analysis(
    req: DescriptiveAnalysisRequest,
    service: AppliedStatisticsService = Depends(get_applied_statistics_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.READONLY)),
) -> dict[str, Any]:
    try:
        result = service.descriptive_analysis(
            rows=req.rows,
            columns=req.columns,
            dataset_name=req.dataset_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit.log(
        event_type="applied_stats",
        action="descriptive_analysis",
        payload={
            "dataset_name": req.dataset_name,
            "rows": len(req.rows),
            "columns": req.columns or [],
            "resolved_columns": result.get("numeric_columns", []),
        },
    )
    return result


@router.post("/tests/two-sample-mean")
def two_sample_mean(
    req: TwoSampleMeanTestRequest,
    service: AppliedStatisticsService = Depends(get_applied_statistics_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.READONLY)),
) -> dict[str, Any]:
    try:
        result = service.two_sample_test(
            sample_a=req.sample_a,
            sample_b=req.sample_b,
            equal_var=req.equal_var,
            permutations=req.permutations,
            random_seed=req.random_seed,
            group_a_name=req.group_a_name,
            group_b_name=req.group_b_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit.log(
        event_type="applied_stats",
        action="two_sample_mean_test",
        payload={
            "group_a_name": req.group_a_name,
            "group_b_name": req.group_b_name,
            "n_a": result.get("n_a"),
            "n_b": result.get("n_b"),
            "p_value_permutation": result.get("p_value_permutation"),
        },
    )
    return result


@router.post("/model/ols")
def ols_analysis(
    req: OLSAnalysisRequest,
    service: AppliedStatisticsService = Depends(get_applied_statistics_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.READONLY)),
) -> dict[str, Any]:
    try:
        result = service.ols_analysis(target=req.target, features=req.features)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit.log(
        event_type="applied_stats",
        action="ols_analysis",
        payload={
            "n_target": len(req.target),
            "feature_count": len(req.features),
            "r2": result.get("r2"),
            "adjusted_r2": result.get("adjusted_r2"),
        },
    )
    return result


@router.post("/model/ridge")
def ridge_analysis(
    req: RidgeAnalysisRequest,
    service: AppliedStatisticsService = Depends(get_applied_statistics_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.READONLY)),
) -> dict[str, Any]:
    try:
        result = service.ridge_analysis(
            target=req.target,
            features=req.features,
            alpha=req.alpha,
            alpha_grid=req.alpha_grid,
            cv_folds=req.cv_folds,
            standardize=req.standardize,
            random_seed=req.random_seed,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    audit.log(
        event_type="applied_stats",
        action="ridge_analysis",
        payload={
            "n_target": len(req.target),
            "feature_count": len(req.features),
            "alpha": result.get("alpha"),
            "standardize": result.get("standardize"),
            "r2": result.get("r2"),
        },
    )
    return result


@router.post("/cases/market-factor-study")
def market_factor_study(
    req: MarketFactorStudyRequest,
    service: AppliedStatisticsService = Depends(get_applied_statistics_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.RISK, UserRole.READONLY)),
) -> dict[str, Any]:
    try:
        result = service.market_factor_study(
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            include_fundamentals=req.include_fundamentals,
            permutations=req.permutations,
            bootstrap_samples=req.bootstrap_samples,
            random_seed=req.random_seed,
            export_markdown=req.export_markdown,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    audit.log(
        event_type="applied_stats",
        action="market_factor_study",
        payload={
            "symbol": req.symbol,
            "start_date": req.start_date.isoformat(),
            "end_date": req.end_date.isoformat(),
            "sample_size": result.get("sample_size"),
            "provider": result.get("provider"),
            "export_markdown": req.export_markdown,
            "markdown_report_path": result.get("markdown_report_path"),
        },
    )
    return result
