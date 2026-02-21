from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import (
    get_audit_service,
    get_data_license_service,
    get_data_provider,
    get_snapshot_service,
)
from trading_assistant.core.models import DataLicenseCheckRequest, DataSnapshotRegisterRequest
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.snapshot_service import DataSnapshotService

router = APIRouter(prefix="/market", tags=["market"])


class BarItem(BaseModel):
    trade_date: date
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    is_suspended: bool
    is_st: bool


class MarketBarsResponse(BaseModel):
    provider: str
    row_count: int
    symbol: str
    start_date: date
    end_date: date
    bars: list[BarItem]


class CalendarItem(BaseModel):
    trade_date: date
    is_open: bool


class MarketCalendarResponse(BaseModel):
    provider: str
    start_date: date
    end_date: date
    days: list[CalendarItem]


@router.get("/bars", response_model=MarketBarsResponse)
def get_market_bars(
    symbol: str = Query(..., description="Stock symbol, e.g. 000001"),
    start_date: date = Query(..., description="Start date, e.g. 2025-01-01"),
    end_date: date = Query(..., description="End date, e.g. 2025-12-31"),
    limit: int = Query(5, ge=1, le=200, description="Max rows returned in response"),
    provider: CompositeDataProvider = Depends(get_data_provider),
    license_service: DataLicenseService = Depends(get_data_license_service),
    settings: Settings = Depends(get_settings),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.READONLY, UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT)),
) -> MarketBarsResponse:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        used_provider, bars = provider.get_daily_bars_with_source(symbol, start_date, end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    license_check = license_service.check(
        DataLicenseCheckRequest(
            dataset_name="daily_bars",
            provider=used_provider,
            requested_usage="internal_research",
            export_requested=False,
            expected_rows=len(bars),
            as_of=end_date,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_market_bars",
            payload={
                "symbol": symbol,
                "provider": used_provider,
                "allowed": False,
                "reason": license_check.reason,
            },
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    if bars.empty:
        raise HTTPException(status_code=404, detail="No market data available for requested range.")
    snapshot_id = snapshots.register(
        DataSnapshotRegisterRequest(
            dataset_name="daily_bars",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            provider=used_provider,
            row_count=len(bars),
            content_hash=dataframe_content_hash(bars),
        )
    )

    records = bars.sort_values("trade_date").tail(limit).to_dict(orient="records")
    resp = MarketBarsResponse(
        provider=used_provider,
        row_count=len(bars),
        symbol=symbol,
        start_date=start_date,
        end_date=end_date,
        bars=[BarItem(**item) for item in records],
    )
    audit.log(
        event_type="market_data",
        action="bars",
        payload={
            "symbol": symbol,
            "provider": used_provider,
            "row_count": resp.row_count,
            "snapshot_id": snapshot_id,
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return resp


@router.get("/calendar", response_model=MarketCalendarResponse)
def get_trade_calendar(
    start_date: date = Query(..., description="Start date, e.g. 2025-01-01"),
    end_date: date = Query(..., description="End date, e.g. 2025-12-31"),
    provider: CompositeDataProvider = Depends(get_data_provider),
    license_service: DataLicenseService = Depends(get_data_license_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.READONLY, UserRole.RESEARCH, UserRole.RISK, UserRole.AUDIT)),
) -> MarketCalendarResponse:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    try:
        used_provider, cal = provider.get_trade_calendar_with_source(start_date, end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    license_check = license_service.check(
        DataLicenseCheckRequest(
            dataset_name="trade_calendar",
            provider=used_provider,
            requested_usage="internal_research",
            export_requested=False,
            expected_rows=len(cal),
            as_of=end_date,
        )
    )
    if settings.enforce_data_license and not license_check.allowed:
        audit.log(
            event_type="data_license",
            action="enforce_market_calendar",
            payload={"provider": used_provider, "allowed": False, "reason": license_check.reason},
            status="ERROR",
        )
        raise HTTPException(status_code=403, detail=f"data license check failed: {license_check.reason}")

    days = [
        CalendarItem(trade_date=row["trade_date"], is_open=bool(row["is_open"]))
        for row in cal.sort_values("trade_date").to_dict(orient="records")
    ]
    audit.log(
        event_type="market_data",
        action="calendar",
        payload={
            "provider": used_provider,
            "days": len(days),
            "license_ok": license_check.allowed,
            "license_reason": license_check.reason,
            "license_enforced": settings.enforce_data_license,
        },
        status="OK" if (license_check.allowed or not settings.enforce_data_license) else "ERROR",
    )
    return MarketCalendarResponse(
        provider=used_provider,
        start_date=start_date,
        end_date=end_date,
        days=days,
    )
