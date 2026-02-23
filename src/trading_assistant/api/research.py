from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, model_validator

from trading_assistant.audit.service import AuditService
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.core.container import (
    get_audit_service,
    get_research_workflow_service,
    get_strategy_governance_service,
)
from trading_assistant.core.models import ResearchWorkflowRequest, ResearchWorkflowResult
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.workflow.research import ResearchWorkflowService

router = APIRouter(prefix="/research", tags=["research"])


class FullMarket2000ScanRequest(BaseModel):
    start_date: date
    end_date: date
    principal: float = Field(default=2000.0, gt=0)
    lot_size: int = Field(default=100, ge=1, le=10_000)
    cash_buffer_ratio: float = Field(default=0.10, ge=0.0, le=0.90)
    max_single_position: float = Field(default=0.60, gt=0.0, le=1.0)
    min_edge_bps: float = Field(default=140.0, ge=0.0, le=5_000.0)
    max_symbols: int = Field(default=0, ge=0, le=20_000)
    sleep_ms: int = Field(default=0, ge=0, le=5_000)
    top_n: int = Field(default=30, ge=1, le=500)
    timeout_minutes: int = Field(default=120, ge=1, le=24 * 60)

    @model_validator(mode="after")
    def _validate_dates(self) -> "FullMarket2000ScanRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class FullMarket2000Candidate(BaseModel):
    symbol: str
    provider: str
    confidence: float
    close: float | None = None
    suggested_lots: int = 0
    max_buy_price: float | None = None
    buy_price_low: float | None = None
    buy_price_high: float | None = None
    risk_level: str
    small_capital_note: str | None = None
    reason: str


class FullMarket2000ScanResponse(BaseModel):
    run_id: str
    started_at: datetime
    finished_at: datetime
    granularity: str = "daily"
    start_date: date
    end_date: date
    total_symbols: int
    buy_pass_symbols: int
    error_symbols: int
    summary_path: str
    csv_path: str
    jsonl_path: str
    top_candidates: list[FullMarket2000Candidate] = Field(default_factory=list)


def _resolve_project_root() -> Path:
    # .../src/trading_assistant/api/research.py -> project root
    return Path(__file__).resolve().parents[3]


def _tail_text(text: str, max_lines: int = 12) -> str:
    lines = [line for line in (text or "").splitlines() if line.strip()]
    if not lines:
        return ""
    return "\n".join(lines[-max_lines:])


@router.post("/run", response_model=ResearchWorkflowResult)
def run_research_workflow(
    req: ResearchWorkflowRequest,
    workflow: ResearchWorkflowService = Depends(get_research_workflow_service),
    strategy_gov: StrategyGovernanceService = Depends(get_strategy_governance_service),
    settings: Settings = Depends(get_settings),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO)),
) -> ResearchWorkflowResult:
    if settings.enforce_approved_strategy and not strategy_gov.is_approved(req.strategy_name):
        raise HTTPException(
            status_code=403,
            detail=f"Strategy '{req.strategy_name}' has no approved version.",
        )
    try:
        result = workflow.run(req)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="research_workflow",
        action="run",
        payload={
            "run_id": result.run_id,
            "strategy": req.strategy_name,
            "signals": len(result.signals),
            "optimized": result.optimized_portfolio is not None,
            "event_enriched": req.enable_event_enrichment or req.strategy_name == "event_driven",
            "event_rows_used": sum(item.event_rows_used for item in result.signals),
            "fundamental_enriched": req.enable_fundamental_enrichment,
            "fundamental_available_signals": sum(1 for item in result.signals if item.fundamental_available),
            "small_capital_mode": req.enable_small_capital_mode,
            "small_capital_blocked_signals": sum(1 for item in result.signals if item.small_capital_blocked),
        },
    )
    return result


@router.post("/full-market-2000-scan", response_model=FullMarket2000ScanResponse)
def run_full_market_2000_scan(
    req: FullMarket2000ScanRequest,
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK, UserRole.ADMIN)),
) -> FullMarket2000ScanResponse:
    started_at = datetime.now(timezone.utc)
    run_id = uuid4().hex

    root = _resolve_project_root()
    script_path = root / "scripts" / "full_market_pick_2000.py"
    if not script_path.exists():
        raise HTTPException(status_code=500, detail=f"scan script not found: {script_path}")

    output_summary = root / "reports" / f"full_market_summary_2000_{run_id}.json"
    output_jsonl = root / "reports" / f"full_market_signals_2000_{run_id}.jsonl"
    output_csv = root / "reports" / f"buy_candidates_2000_{run_id}.csv"

    cmd = [
        sys.executable,
        str(script_path),
        "--start-date",
        req.start_date.isoformat(),
        "--end-date",
        req.end_date.isoformat(),
        "--principal",
        str(req.principal),
        "--lot-size",
        str(req.lot_size),
        "--cash-buffer-ratio",
        str(req.cash_buffer_ratio),
        "--max-single-position",
        str(req.max_single_position),
        "--min-edge-bps",
        str(req.min_edge_bps),
        "--max-symbols",
        str(req.max_symbols),
        "--sleep-ms",
        str(req.sleep_ms),
        "--top-n",
        str(req.top_n),
        "--output-summary",
        str(output_summary.relative_to(root)),
        "--output-jsonl",
        str(output_jsonl.relative_to(root)),
        "--output-csv",
        str(output_csv.relative_to(root)),
    ]

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(root),
            capture_output=True,
            text=True,
            timeout=max(60, int(req.timeout_minutes) * 60),
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(status_code=504, detail=f"scan timed out after {req.timeout_minutes} minutes") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to launch scan: {exc}") from exc

    stdout_tail = _tail_text(proc.stdout)
    stderr_tail = _tail_text(proc.stderr)
    if proc.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail=(
                "full-market scan failed.\n"
                f"return_code={proc.returncode}\n"
                f"stdout_tail:\n{stdout_tail}\n"
                f"stderr_tail:\n{stderr_tail}"
            ),
        )

    if not output_summary.exists():
        raise HTTPException(status_code=500, detail=f"scan finished but summary missing: {output_summary}")

    try:
        summary = json.loads(output_summary.read_text(encoding="utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"failed to parse scan summary: {exc}") from exc

    finished_at = datetime.now(timezone.utc)
    top_candidates = list(summary.get("top_candidates") or [])

    response = FullMarket2000ScanResponse(
        run_id=run_id,
        started_at=started_at,
        finished_at=finished_at,
        start_date=req.start_date,
        end_date=req.end_date,
        granularity="daily",
        total_symbols=int(summary.get("total_symbols", 0)),
        buy_pass_symbols=int(summary.get("buy_pass_symbols", 0)),
        error_symbols=int(summary.get("error_symbols", 0)),
        summary_path=str(output_summary),
        csv_path=str(output_csv),
        jsonl_path=str(output_jsonl),
        top_candidates=[FullMarket2000Candidate(**item) for item in top_candidates],
    )

    audit.log(
        event_type="research_workflow",
        action="full_market_2000_scan",
        payload={
            "run_id": run_id,
            "start_date": req.start_date.isoformat(),
            "end_date": req.end_date.isoformat(),
            "principal": req.principal,
            "lot_size": req.lot_size,
            "max_symbols": req.max_symbols,
            "top_n": req.top_n,
            "total_symbols": response.total_symbols,
            "buy_pass_symbols": response.buy_pass_symbols,
            "error_symbols": response.error_symbols,
            "summary_path": response.summary_path,
            "csv_path": response.csv_path,
            "jsonl_path": response.jsonl_path,
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
        },
        status="OK",
    )
    return response
