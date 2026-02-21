from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from trading_assistant.core.container import get_strategy_registry
from trading_assistant.core.models import StrategyInfo
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.strategy.registry import StrategyRegistry

router = APIRouter(prefix="/strategies", tags=["strategies"])


@router.get("", response_model=list[StrategyInfo])
def list_strategies(
    registry: StrategyRegistry = Depends(get_strategy_registry),
    _auth: AuthContext = Depends(require_roles(UserRole.READONLY, UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT)),
) -> list[StrategyInfo]:
    return registry.list_info()


@router.get("/{strategy_name}", response_model=StrategyInfo)
def get_strategy(
    strategy_name: str,
    registry: StrategyRegistry = Depends(get_strategy_registry),
    _auth: AuthContext = Depends(require_roles(UserRole.READONLY, UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT)),
) -> StrategyInfo:
    try:
        return registry.get(strategy_name).info
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
