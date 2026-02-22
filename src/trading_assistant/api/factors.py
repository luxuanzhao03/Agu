from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import (
    get_audit_service,
    get_data_provider,
    get_factor_engine,
    get_fundamental_service,
    get_snapshot_service,
)
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.models import DataSnapshotRegisterRequest
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.exceptions import DataProviderError
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.snapshot_service import DataSnapshotService

router = APIRouter(prefix="/factors", tags=["factors"])


class FactorSnapshotResponse(BaseModel):
    provider: str
    symbol: str
    trade_date: date
    values: dict[str, float]


@router.get("/snapshot", response_model=FactorSnapshotResponse)
def factor_snapshot(
    symbol: str = Query(...),
    start_date: date = Query(...),
    end_date: date = Query(...),
    provider: CompositeDataProvider = Depends(get_data_provider),
    factor_engine: FactorEngine = Depends(get_factor_engine),
    fundamentals: FundamentalService = Depends(get_fundamental_service),
    snapshots: DataSnapshotService = Depends(get_snapshot_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.READONLY, UserRole.RESEARCH, UserRole.RISK)),
) -> FactorSnapshotResponse:
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")
    try:
        used_provider, bars = provider.get_daily_bars_with_source(symbol, start_date, end_date)
    except DataProviderError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

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

    bars_for_features = bars
    if settings.enable_fundamental_enrichment:
        bars_for_features, _ = fundamentals.enrich_bars(
            symbol=symbol,
            bars=bars,
            as_of=end_date,
            max_staleness_days=settings.fundamental_max_staleness_days,
        )

    features = factor_engine.compute(bars_for_features)
    latest = features.sort_values("trade_date").iloc[-1]
    keys = [
        "ma5",
        "ma20",
        "ma60",
        "atr14",
        "ret_1d",
        "momentum20",
        "momentum60",
        "volatility20",
        "zscore20",
        "turnover20",
        "fundamental_score",
        "fundamental_profitability_score",
        "fundamental_growth_score",
        "fundamental_quality_score",
        "fundamental_leverage_score",
        "fundamental_completeness",
    ]
    values = {k: round(float(latest.get(k, 0.0)), 6) for k in keys}
    audit.log(
        event_type="factor",
        action="snapshot",
        payload={"symbol": symbol, "provider": used_provider, "snapshot_id": snapshot_id},
    )
    return FactorSnapshotResponse(
        provider=used_provider,
        symbol=symbol,
        trade_date=latest["trade_date"],
        values=values,
    )
