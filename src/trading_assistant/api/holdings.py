from __future__ import annotations

from datetime import date, datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_holding_service
from trading_assistant.core.models import (
    ManualHoldingAnalysisRequest,
    ManualHoldingAnalysisResult,
    ManualHoldingPositionsResult,
    ManualHoldingTradeCreate,
    ManualHoldingTradeRecord,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.holdings.service import HoldingService

router = APIRouter(prefix="/holdings", tags=["holdings"])


def _today() -> date:
    return datetime.now(timezone.utc).date()


@router.post("/trades", response_model=ManualHoldingTradeRecord)
def create_holding_trade(
    req: ManualHoldingTradeCreate,
    service: HoldingService = Depends(get_holding_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO)),
) -> ManualHoldingTradeRecord:
    try:
        row = service.record_trade(req)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    audit.log(
        event_type="manual_holding",
        action="record_trade",
        payload={
            "trade_id": row.id,
            "symbol": row.symbol,
            "side": row.side.value,
            "lots": row.lots,
            "lot_size": row.lot_size,
            "quantity": row.quantity,
            "price": row.price,
            "trade_date": row.trade_date.isoformat(),
        },
    )
    return row


@router.get("/trades", response_model=list[ManualHoldingTradeRecord])
def list_holding_trades(
    symbol: str | None = Query(default=None),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=5000),
    service: HoldingService = Depends(get_holding_service),
    _auth: AuthContext = Depends(
        require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK, UserRole.AUDIT)
    ),
) -> list[ManualHoldingTradeRecord]:
    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=422, detail="start_date must be <= end_date")
    return service.list_trades(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)


@router.delete("/trades/{trade_id}", response_model=bool)
def delete_holding_trade(
    trade_id: int,
    service: HoldingService = Depends(get_holding_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO)),
) -> bool:
    deleted = service.delete_trade(trade_id)
    if not deleted:
        raise HTTPException(status_code=404, detail=f"trade_id '{trade_id}' not found")

    audit.log(
        event_type="manual_holding",
        action="delete_trade",
        payload={"trade_id": int(trade_id)},
    )
    return True


@router.get("/positions", response_model=ManualHoldingPositionsResult)
def get_holding_positions(
    as_of_date: date | None = Query(default=None),
    service: HoldingService = Depends(get_holding_service),
    _auth: AuthContext = Depends(
        require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK, UserRole.AUDIT)
    ),
) -> ManualHoldingPositionsResult:
    try:
        return service.positions(as_of_date=as_of_date or _today())
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/analyze", response_model=ManualHoldingAnalysisResult)
def analyze_holding_portfolio(
    req: ManualHoldingAnalysisRequest,
    service: HoldingService = Depends(get_holding_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK)),
) -> ManualHoldingAnalysisResult:
    try:
        result = service.analyze(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    audit.log(
        event_type="manual_holding",
        action="analyze",
        payload={
            "as_of_date": req.as_of_date.isoformat(),
            "strategy_name": req.strategy_name,
            "position_count": result.summary.position_count,
            "candidate_symbols": len(req.candidate_symbols),
            "recommendation_count": len(result.recommendations),
            "next_trade_date": (result.next_trade_date.isoformat() if result.next_trade_date else None),
        },
    )
    return result
