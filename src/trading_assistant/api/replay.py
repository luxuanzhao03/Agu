from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_replay_service
from trading_assistant.core.models import (
    CostModelCalibrationRecord,
    CostModelCalibrationRequest,
    CostModelCalibrationResult,
    ExecutionAttributionReport,
    ExecutionRecordCreate,
    ExecutionReplayReport,
    SignalDecisionRecord,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.replay.service import ReplayService

router = APIRouter(prefix="/replay", tags=["replay"])


@router.post("/signals/record", response_model=str)
def record_signal(
    req: SignalDecisionRecord,
    replay: ReplayService = Depends(get_replay_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO)),
) -> str:
    signal_id = replay.record_signal(req)
    audit.log(
        event_type="replay_signal",
        action="record",
        payload={"signal_id": signal_id, "symbol": req.symbol, "action": req.action.value},
    )
    return signal_id


@router.get("/signals", response_model=list[SignalDecisionRecord])
def list_signals(
    symbol: str | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
    replay: ReplayService = Depends(get_replay_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RESEARCH, UserRole.RISK)),
) -> list[SignalDecisionRecord]:
    return replay.list_signals(symbol=symbol, limit=limit)


@router.post("/executions/record", response_model=int)
def record_execution(
    req: ExecutionRecordCreate,
    replay: ReplayService = Depends(get_replay_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO)),
) -> int:
    try:
        row_id = replay.record_execution(req)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    audit.log(
        event_type="replay_execution",
        action="record",
        payload={
            "signal_id": req.signal_id,
            "symbol": req.symbol,
            "quantity": req.quantity,
            "price": req.price,
            "reference_price": req.reference_price,
            "fee": req.fee,
        },
    )
    return row_id


@router.get("/report", response_model=ExecutionReplayReport)
def replay_report(
    symbol: str | None = Query(default=None),
    strategy_name: str | None = Query(default=None),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    replay: ReplayService = Depends(get_replay_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RESEARCH, UserRole.RISK)),
) -> ExecutionReplayReport:
    return replay.report(
        symbol=symbol,
        strategy_name=strategy_name,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )


@router.get("/attribution", response_model=ExecutionAttributionReport)
def replay_attribution(
    symbol: str | None = Query(default=None),
    strategy_name: str | None = Query(default=None),
    start_date: date | None = Query(default=None),
    end_date: date | None = Query(default=None),
    limit: int = Query(default=500, ge=1, le=2000),
    replay: ReplayService = Depends(get_replay_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RESEARCH, UserRole.RISK)),
) -> ExecutionAttributionReport:
    return replay.attribution(
        symbol=symbol,
        strategy_name=strategy_name,
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )


@router.post("/cost-model/calibrate", response_model=CostModelCalibrationResult)
def calibrate_cost_model(
    req: CostModelCalibrationRequest,
    replay: ReplayService = Depends(get_replay_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> CostModelCalibrationResult:
    result = replay.calibrate_cost_model(req)
    audit.log(
        event_type="cost_model",
        action="calibrate",
        payload={
            "symbol": req.symbol,
            "strategy_name": req.strategy_name,
            "sample_size": result.sample_size,
            "executed_samples": result.executed_samples,
            "slippage_coverage": result.slippage_coverage,
            "recommended_slippage_rate": result.recommended_slippage_rate,
            "recommended_impact_cost_coeff": result.recommended_impact_cost_coeff,
            "recommended_fill_probability_floor": result.recommended_fill_probability_floor,
            "confidence": result.confidence,
            "calibration_id": result.calibration_id,
        },
        status="OK",
    )
    return result


@router.get("/cost-model/calibrations", response_model=list[CostModelCalibrationRecord])
def list_cost_model_calibrations(
    symbol: str | None = Query(default=None),
    limit: int = Query(default=30, ge=1, le=200),
    replay: ReplayService = Depends(get_replay_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RESEARCH, UserRole.RISK, UserRole.ADMIN)),
) -> list[CostModelCalibrationRecord]:
    return replay.list_cost_calibrations(symbol=symbol, limit=limit)
