from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable

from fastapi import Depends, HTTPException, Request, status

from trading_assistant.core.config import Settings, get_settings


class UserRole(str, Enum):
    RESEARCH = "research"
    RISK = "risk"
    PORTFOLIO = "portfolio"
    AUDIT = "audit"
    ADMIN = "admin"
    READONLY = "readonly"


@dataclass
class AuthContext:
    api_key: str
    role: UserRole


def _parse_api_keys(raw: str) -> dict[str, UserRole]:
    mapping: dict[str, UserRole] = {}
    if not raw.strip():
        return mapping
    pairs = [p.strip() for p in raw.split(",") if p.strip()]
    for pair in pairs:
        if ":" not in pair:
            continue
        key, role = pair.split(":", 1)
        key = key.strip()
        role = role.strip().lower()
        if not key:
            continue
        try:
            mapping[key] = UserRole(role)
        except ValueError:
            continue
    return mapping


def get_auth_context(
    request: Request,
    settings: Settings = Depends(get_settings),
) -> AuthContext:
    if not settings.auth_enabled:
        return AuthContext(api_key="local-dev", role=UserRole.ADMIN)

    header_name = settings.auth_header_name
    api_key = request.headers.get(header_name, "").strip()
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Missing API key in header '{header_name}'.",
        )

    mapping = _parse_api_keys(settings.auth_api_keys)
    role = mapping.get(api_key)
    if role is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )
    return AuthContext(api_key=api_key, role=role)


def require_roles(*roles: UserRole) -> Callable:
    allowed = set(roles)

    def _dep(ctx: AuthContext = Depends(get_auth_context)) -> AuthContext:
        if ctx.role == UserRole.ADMIN:
            return ctx
        if ctx.role not in allowed:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role '{ctx.role.value}' is not allowed for this operation.",
            )
        return ctx

    return _dep


def permission_matrix() -> dict[str, list[str]]:
    return {
        "market_data_read": ["readonly", "research", "risk", "audit", "admin"],
        "signal_generation": ["research", "risk", "admin"],
        "backtest": ["research", "risk", "admin"],
        "portfolio_optimize": ["portfolio", "research", "admin"],
        "portfolio_rebalance": ["portfolio", "admin"],
        "risk_checks": ["risk", "research", "admin"],
        "audit_read_export": ["audit", "admin"],
        "strategy_governance_review": ["research", "risk", "audit", "admin"],
        "reporting": ["audit", "risk", "research", "admin"],
        "data_license_governance": ["audit", "risk", "admin"],
        "event_governance": ["research", "risk", "audit", "admin"],
        "alerts_subscription": ["audit", "risk", "admin"],
        "ops_jobs": ["research", "risk", "audit", "admin"],
        "ops_scheduler": ["risk", "audit", "admin"],
        "ops_dashboard": ["risk", "audit", "admin"],
    }
