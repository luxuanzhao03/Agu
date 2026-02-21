from __future__ import annotations

import importlib.util

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.security import AuthContext, UserRole, get_auth_context, permission_matrix, require_roles

router = APIRouter(prefix="/system", tags=["system"])


class SystemConfigResponse(BaseModel):
    app_name: str
    env: str
    data_provider_priority: list[str]
    risk_limits: dict[str, float]
    audit_db_path: str
    snapshot_db_path: str
    replay_db_path: str
    strategy_gov_db_path: str
    license_db_path: str
    job_db_path: str
    alert_db_path: str
    event_db_path: str
    provider_status: dict[str, bool]
    auth_enabled: bool
    auth_header_name: str
    enforce_approved_strategy: bool
    enforce_data_license: bool
    strategy_required_approval_roles: list[str]
    strategy_min_approval_count: int
    ops_scheduler_enabled: bool
    ops_scheduler_tick_seconds: int
    ops_scheduler_timezone: str
    ops_job_sla_grace_minutes: int
    ops_job_running_timeout_minutes: int


class AuthMeResponse(BaseModel):
    role: str
    api_key_present: bool


class PermissionMatrixResponse(BaseModel):
    permissions: dict[str, list[str]]


@router.get("/config", response_model=SystemConfigResponse)
def system_config(
    settings: Settings = Depends(get_settings),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.READONLY)),
) -> SystemConfigResponse:
    return SystemConfigResponse(
        app_name=settings.app_name,
        env=settings.env,
        data_provider_priority=settings.provider_priority_list,
        risk_limits={
            "max_single_position": settings.max_single_position,
            "max_drawdown": settings.max_drawdown,
            "max_industry_exposure": settings.max_industry_exposure,
            "min_turnover_20d": settings.min_turnover_20d,
        },
        audit_db_path=settings.audit_db_path,
        snapshot_db_path=settings.snapshot_db_path,
        replay_db_path=settings.replay_db_path,
        strategy_gov_db_path=settings.strategy_gov_db_path,
        license_db_path=settings.license_db_path,
        job_db_path=settings.job_db_path,
        alert_db_path=settings.alert_db_path,
        event_db_path=settings.event_db_path,
        provider_status={
            "akshare": bool(importlib.util.find_spec("akshare")),
            "tushare": bool(importlib.util.find_spec("tushare")),
        },
        auth_enabled=settings.auth_enabled,
        auth_header_name=settings.auth_header_name,
        enforce_approved_strategy=settings.enforce_approved_strategy,
        enforce_data_license=settings.enforce_data_license,
        strategy_required_approval_roles=settings.required_approval_roles_list,
        strategy_min_approval_count=settings.strategy_min_approval_count,
        ops_scheduler_enabled=settings.ops_scheduler_enabled,
        ops_scheduler_tick_seconds=settings.ops_scheduler_tick_seconds,
        ops_scheduler_timezone=settings.ops_scheduler_timezone,
        ops_job_sla_grace_minutes=settings.ops_job_sla_grace_minutes,
        ops_job_running_timeout_minutes=settings.ops_job_running_timeout_minutes,
    )


@router.get("/auth/me", response_model=AuthMeResponse)
def auth_me(ctx: AuthContext = Depends(get_auth_context)) -> AuthMeResponse:
    return AuthMeResponse(role=ctx.role.value, api_key_present=ctx.api_key != "")


@router.get("/auth/permissions", response_model=PermissionMatrixResponse)
def auth_permissions(
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.READONLY)),
) -> PermissionMatrixResponse:
    return PermissionMatrixResponse(permissions=permission_matrix())
