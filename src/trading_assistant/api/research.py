from __future__ import annotations

import json
import importlib.util
import subprocess
import sys
import time
from datetime import date, datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any
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
    symbol_timeout_sec: float = Field(default=45.0, ge=5.0, le=600.0)
    network_timeout_sec: float = Field(default=12.0, ge=1.0, le=120.0)
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
    timeout_symbols: int = 0
    summary_path: str
    csv_path: str
    jsonl_path: str
    top_candidates: list[FullMarket2000Candidate] = Field(default_factory=list)


class FullMarket2000ScanProgressResponse(BaseModel):
    run_id: str | None = None
    status: str = "IDLE"
    scanned_symbols: int = 0
    total_symbols: int = 0
    buy_pass_symbols: int = 0
    error_symbols: int = 0
    timeout_symbols: int = 0
    progress_pct: float = 0.0
    started_at: datetime | None = None
    updated_at: datetime | None = None
    finished_at: datetime | None = None
    message: str | None = None


class FullMarket2000ScanCancelRequest(BaseModel):
    run_id: str | None = None


class FullMarket2000ScanCancelResponse(BaseModel):
    run_id: str | None = None
    status: str
    message: str


_FULL_MARKET_PROGRESS_LOCK = Lock()
_FULL_MARKET_PROGRESS_STATE: dict[str, Any] = {
    "run_id": None,
    "status": "IDLE",
    "scanned_symbols": 0,
    "total_symbols": 0,
    "buy_pass_symbols": 0,
    "error_symbols": 0,
    "timeout_symbols": 0,
    "progress_pct": 0.0,
    "started_at": None,
    "updated_at": None,
    "finished_at": None,
    "message": "idle",
    "progress_path": None,
}
_FULL_MARKET_RUNTIME_LOCK = Lock()
_FULL_MARKET_RUNTIME_STATE: dict[str, Any] = {
    "run_id": None,
    "process": None,
    "cancel_requested": False,
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            out = datetime.fromisoformat(raw)
        except Exception:
            return None
        return out if out.tzinfo is not None else out.replace(tzinfo=timezone.utc)
    return None


def _missing_scan_runtime_deps() -> list[str]:
    required = ("tushare", "akshare", "pandas")
    return [name for name in required if importlib.util.find_spec(name) is None]


def _set_full_market_progress(**updates: Any) -> None:
    with _FULL_MARKET_PROGRESS_LOCK:
        _FULL_MARKET_PROGRESS_STATE.update(updates)
        if "updated_at" not in updates:
            _FULL_MARKET_PROGRESS_STATE["updated_at"] = _utc_now()


def _get_full_market_progress() -> dict[str, Any]:
    with _FULL_MARKET_PROGRESS_LOCK:
        return dict(_FULL_MARKET_PROGRESS_STATE)


def _set_full_market_runtime(**updates: Any) -> None:
    with _FULL_MARKET_RUNTIME_LOCK:
        _FULL_MARKET_RUNTIME_STATE.update(updates)


def _get_full_market_runtime() -> dict[str, Any]:
    with _FULL_MARKET_RUNTIME_LOCK:
        return dict(_FULL_MARKET_RUNTIME_STATE)


def _clear_full_market_runtime(*, run_id: str | None = None) -> None:
    with _FULL_MARKET_RUNTIME_LOCK:
        if run_id is not None and _FULL_MARKET_RUNTIME_STATE.get("run_id") != run_id:
            return
        _FULL_MARKET_RUNTIME_STATE["run_id"] = None
        _FULL_MARKET_RUNTIME_STATE["process"] = None
        _FULL_MARKET_RUNTIME_STATE["cancel_requested"] = False


def _terminate_process(proc: subprocess.Popen[str], timeout_sec: int = 8) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout_sec)


def _read_full_market_progress_file(progress_path: Path) -> dict[str, Any]:
    if not progress_path.exists():
        return {}
    try:
        payload = json.loads(progress_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _merge_full_market_progress(base: dict[str, Any], from_file: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key in (
        "run_id",
        "status",
        "scanned_symbols",
        "total_symbols",
        "buy_pass_symbols",
        "error_symbols",
        "timeout_symbols",
        "progress_pct",
        "started_at",
        "updated_at",
        "finished_at",
        "message",
    ):
        if key in from_file and from_file.get(key) is not None:
            out[key] = from_file.get(key)

    base_status = str(base.get("status") or "").upper()
    if base_status in {"CANCELLING", "CANCELED", "FAILED", "TIMEOUT", "COMPLETED"}:
        out["status"] = base_status
        if base.get("message") is not None:
            out["message"] = base.get("message")
        if base.get("finished_at") is not None:
            out["finished_at"] = base.get("finished_at")
        if base.get("updated_at") is not None:
            out["updated_at"] = base.get("updated_at")

    total = max(0, _safe_int(out.get("total_symbols"), 0))
    scanned = max(0, _safe_int(out.get("scanned_symbols"), 0))
    buy_pass = max(0, _safe_int(out.get("buy_pass_symbols"), 0))
    errors = max(0, _safe_int(out.get("error_symbols"), 0))
    timeouts = max(0, _safe_int(out.get("timeout_symbols"), 0))
    pct = _safe_float(out.get("progress_pct"), -1.0)
    if pct < 0:
        pct = round((float(scanned) / float(total) * 100.0), 2) if total > 0 else 0.0
    pct = min(100.0, max(0.0, pct))

    status = str(out.get("status") or "IDLE").upper()
    if status == "COMPLETED":
        pct = 100.0
    out["status"] = status
    out["total_symbols"] = total
    out["scanned_symbols"] = scanned
    out["buy_pass_symbols"] = buy_pass
    out["error_symbols"] = errors
    out["timeout_symbols"] = timeouts
    out["progress_pct"] = pct
    return out


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


@router.get("/full-market-2000-scan/progress", response_model=FullMarket2000ScanProgressResponse)
def get_full_market_2000_scan_progress(
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK, UserRole.ADMIN)),
) -> FullMarket2000ScanProgressResponse:
    state = _get_full_market_progress()
    progress_file_payload: dict[str, Any] = {}
    progress_path_raw = state.get("progress_path")
    if isinstance(progress_path_raw, str) and progress_path_raw:
        progress_file_payload = _read_full_market_progress_file(Path(progress_path_raw))
    merged = _merge_full_market_progress(state, progress_file_payload)

    status = str(merged.get("status") or "IDLE").upper()
    if status in {"RUNNING", "CANCELLING"}:
        runtime = _get_full_market_runtime()
        runtime_run_id = runtime.get("run_id")
        runtime_proc = runtime.get("process")
        runtime_alive = runtime_proc is not None and runtime_proc.poll() is None
        same_run = bool(merged.get("run_id")) and merged.get("run_id") == runtime_run_id

        # If no live process exists for this run and heartbeat is stale, mark it failed
        # so the UI does not stay in an infinite "running" state after abnormal exits.
        if not (same_run and runtime_alive):
            updated_at = _parse_datetime(merged.get("updated_at"))
            now = _utc_now()
            stale_sec = (now - updated_at).total_seconds() if updated_at is not None else 0.0
            if stale_sec >= 30.0:
                merged["status"] = "FAILED"
                merged["finished_at"] = merged.get("finished_at") or now
                merged["updated_at"] = now
                merged["message"] = (
                    f"scan heartbeat stale for {int(stale_sec)}s with no live process; "
                    "scan likely exited unexpectedly"
                )
                _set_full_market_progress(
                    run_id=merged.get("run_id"),
                    status="FAILED",
                    scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
                    total_symbols=_safe_int(merged.get("total_symbols"), 0),
                    buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
                    error_symbols=_safe_int(merged.get("error_symbols"), 0),
                    timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
                    progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
                    started_at=merged.get("started_at"),
                    updated_at=merged.get("updated_at"),
                    finished_at=merged.get("finished_at"),
                    message=str(merged.get("message") or ""),
                    progress_path=state.get("progress_path"),
                )

    return FullMarket2000ScanProgressResponse(
        run_id=merged.get("run_id"),
        status=str(merged.get("status") or "IDLE"),
        scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
        total_symbols=_safe_int(merged.get("total_symbols"), 0),
        buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
        error_symbols=_safe_int(merged.get("error_symbols"), 0),
        timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
        progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
        started_at=merged.get("started_at"),
        updated_at=merged.get("updated_at"),
        finished_at=merged.get("finished_at"),
        message=(str(merged.get("message")) if merged.get("message") is not None else None),
    )


@router.post("/full-market-2000-scan/cancel", response_model=FullMarket2000ScanCancelResponse)
def cancel_full_market_2000_scan(
    req: FullMarket2000ScanCancelRequest,
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK, UserRole.ADMIN)),
) -> FullMarket2000ScanCancelResponse:
    runtime = _get_full_market_runtime()
    run_id = runtime.get("run_id")
    proc = runtime.get("process")
    if req.run_id and run_id and req.run_id != run_id:
        raise HTTPException(
            status_code=409,
            detail=f"run_id mismatch: active_run_id={run_id}, requested_run_id={req.run_id}",
        )

    if run_id is None or proc is None or proc.poll() is not None:
        progress_state = _get_full_market_progress()
        status = str(progress_state.get("status") or "IDLE")
        return FullMarket2000ScanCancelResponse(
            run_id=progress_state.get("run_id"),
            status=status,
            message="no running full-market scan to cancel",
        )

    _set_full_market_runtime(cancel_requested=True)
    now = _utc_now()
    _set_full_market_progress(
        run_id=run_id,
        status="CANCELLING",
        updated_at=now,
        message="cancel requested by user",
    )
    audit.log(
        event_type="research_workflow",
        action="full_market_2000_scan_cancel",
        payload={"run_id": run_id},
        status="OK",
    )
    return FullMarket2000ScanCancelResponse(
        run_id=run_id,
        status="CANCELLING",
        message="cancel request accepted",
    )


@router.post("/full-market-2000-scan", response_model=FullMarket2000ScanResponse)
def run_full_market_2000_scan(
    req: FullMarket2000ScanRequest,
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RESEARCH, UserRole.PORTFOLIO, UserRole.RISK, UserRole.ADMIN)),
) -> FullMarket2000ScanResponse:
    started_at = _utc_now()
    run_id = uuid4().hex

    root = _resolve_project_root()
    script_path = root / "scripts" / "full_market_pick_2000.py"
    if not script_path.exists():
        raise HTTPException(status_code=500, detail=f"scan script not found: {script_path}")
    missing_deps = _missing_scan_runtime_deps()
    if missing_deps:
        missing = ", ".join(missing_deps)
        raise HTTPException(
            status_code=500,
            detail=(
                "scan runtime self-check failed: "
                f"python={sys.executable}, missing_deps=[{missing}]"
            ),
        )

    output_summary = root / "reports" / f"full_market_summary_2000_{run_id}.json"
    output_jsonl = root / "reports" / f"full_market_signals_2000_{run_id}.jsonl"
    output_csv = root / "reports" / f"buy_candidates_2000_{run_id}.csv"
    progress_path = root / "reports" / f"full_market_progress_2000_{run_id}.json"

    active_runtime = _get_full_market_runtime()
    active_run_id = active_runtime.get("run_id")
    active_proc = active_runtime.get("process")
    if active_run_id and active_proc is not None and active_proc.poll() is None:
        raise HTTPException(
            status_code=409,
            detail=f"another full-market scan is running (run_id={active_run_id}), cancel it first",
        )

    _set_full_market_progress(
        run_id=run_id,
        status="RUNNING",
        scanned_symbols=0,
        total_symbols=0,
        buy_pass_symbols=0,
        error_symbols=0,
        timeout_symbols=0,
        progress_pct=0.0,
        started_at=started_at,
        updated_at=started_at,
        finished_at=None,
        message="scan started",
        progress_path=str(progress_path),
    )

    cmd = [
        sys.executable,
        str(script_path),
        "--run-id",
        run_id,
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
        "--symbol-timeout-sec",
        str(req.symbol_timeout_sec),
        "--network-timeout-sec",
        str(req.network_timeout_sec),
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
        "--progress-file",
        str(progress_path.relative_to(root)),
    ]

    proc: subprocess.Popen[str] | None = None
    stdout_text = ""
    stderr_text = ""
    timeout_sec = max(60, int(req.timeout_minutes) * 60)
    deadline = time.monotonic() + float(timeout_sec)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        _set_full_market_runtime(run_id=run_id, process=proc, cancel_requested=False)
        while proc.poll() is None:
            runtime = _get_full_market_runtime()
            cancel_requested = bool(runtime.get("cancel_requested")) and runtime.get("run_id") == run_id
            if cancel_requested:
                _terminate_process(proc)
                stdout_text, stderr_text = proc.communicate()
                latest_progress = _read_full_market_progress_file(progress_path)
                merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
                finished_at = _utc_now()
                _set_full_market_progress(
                    run_id=run_id,
                    status="CANCELED",
                    scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
                    total_symbols=_safe_int(merged.get("total_symbols"), 0),
                    buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
                    error_symbols=_safe_int(merged.get("error_symbols"), 0),
                    timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
                    progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
                    finished_at=finished_at,
                    updated_at=finished_at,
                    message="scan cancelled by user",
                    progress_path=str(progress_path),
                )
                raise HTTPException(status_code=409, detail=f"scan cancelled by user (run_id={run_id})")

            if time.monotonic() >= deadline:
                _terminate_process(proc)
                stdout_text, stderr_text = proc.communicate()
                latest_progress = _read_full_market_progress_file(progress_path)
                merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
                finished_at = _utc_now()
                _set_full_market_progress(
                    run_id=run_id,
                    status="TIMEOUT",
                    scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
                    total_symbols=_safe_int(merged.get("total_symbols"), 0),
                    buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
                    error_symbols=_safe_int(merged.get("error_symbols"), 0),
                    timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
                    progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
                    finished_at=finished_at,
                    updated_at=finished_at,
                    message=f"scan timed out after {req.timeout_minutes} minutes",
                    progress_path=str(progress_path),
                )
                raise HTTPException(status_code=504, detail=f"scan timed out after {req.timeout_minutes} minutes")

            time.sleep(1.0)

        stdout_text, stderr_text = proc.communicate()
    except HTTPException:
        raise
    except subprocess.TimeoutExpired as exc:
        latest_progress = _read_full_market_progress_file(progress_path)
        merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
        finished_at = _utc_now()
        _set_full_market_progress(
            run_id=run_id,
            status="TIMEOUT",
            scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
            total_symbols=_safe_int(merged.get("total_symbols"), 0),
            buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
            error_symbols=_safe_int(merged.get("error_symbols"), 0),
            timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
            progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
            finished_at=finished_at,
            updated_at=finished_at,
            message=f"scan timed out after {req.timeout_minutes} minutes",
            progress_path=str(progress_path),
        )
        raise HTTPException(status_code=504, detail=f"scan timed out after {req.timeout_minutes} minutes") from exc
    except Exception as exc:
        if proc is not None and proc.poll() is None:
            try:
                _terminate_process(proc)
                stdout_text, stderr_text = proc.communicate()
            except Exception:
                pass
        latest_progress = _read_full_market_progress_file(progress_path)
        merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
        finished_at = _utc_now()
        _set_full_market_progress(
            run_id=run_id,
            status="FAILED",
            scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
            total_symbols=_safe_int(merged.get("total_symbols"), 0),
            buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
            error_symbols=_safe_int(merged.get("error_symbols"), 0),
            timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
            progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
            finished_at=finished_at,
            updated_at=finished_at,
            message=f"failed to launch scan: {exc}",
            progress_path=str(progress_path),
        )
        raise HTTPException(status_code=500, detail=f"failed to launch scan: {exc}") from exc
    finally:
        _clear_full_market_runtime(run_id=run_id)

    stdout_tail = _tail_text(stdout_text)
    stderr_tail = _tail_text(stderr_text)
    proc_returncode = proc.returncode if proc is not None else -1
    if proc_returncode != 0:
        latest_progress = _read_full_market_progress_file(progress_path)
        merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
        finished_at = _utc_now()
        _set_full_market_progress(
            run_id=run_id,
            status="FAILED",
            scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
            total_symbols=_safe_int(merged.get("total_symbols"), 0),
            buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
            error_symbols=_safe_int(merged.get("error_symbols"), 0),
            timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
            progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
            finished_at=finished_at,
            updated_at=finished_at,
            message=f"scan failed (return_code={proc_returncode})",
            progress_path=str(progress_path),
        )
        raise HTTPException(
            status_code=500,
            detail=(
                "full-market scan failed.\n"
                f"return_code={proc_returncode}\n"
                f"stdout_tail:\n{stdout_tail}\n"
                f"stderr_tail:\n{stderr_tail}"
            ),
        )

    if not output_summary.exists():
        latest_progress = _read_full_market_progress_file(progress_path)
        merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
        finished_at = _utc_now()
        _set_full_market_progress(
            run_id=run_id,
            status="FAILED",
            scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
            total_symbols=_safe_int(merged.get("total_symbols"), 0),
            buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
            error_symbols=_safe_int(merged.get("error_symbols"), 0),
            timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
            progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
            finished_at=finished_at,
            updated_at=finished_at,
            message="scan finished but summary missing",
            progress_path=str(progress_path),
        )
        raise HTTPException(status_code=500, detail=f"scan finished but summary missing: {output_summary}")

    try:
        summary = json.loads(output_summary.read_text(encoding="utf-8"))
    except Exception as exc:
        latest_progress = _read_full_market_progress_file(progress_path)
        merged = _merge_full_market_progress(_get_full_market_progress(), latest_progress)
        finished_at = _utc_now()
        _set_full_market_progress(
            run_id=run_id,
            status="FAILED",
            scanned_symbols=_safe_int(merged.get("scanned_symbols"), 0),
            total_symbols=_safe_int(merged.get("total_symbols"), 0),
            buy_pass_symbols=_safe_int(merged.get("buy_pass_symbols"), 0),
            error_symbols=_safe_int(merged.get("error_symbols"), 0),
            timeout_symbols=_safe_int(merged.get("timeout_symbols"), 0),
            progress_pct=_safe_float(merged.get("progress_pct"), 0.0),
            finished_at=finished_at,
            updated_at=finished_at,
            message=f"failed to parse scan summary: {exc}",
            progress_path=str(progress_path),
        )
        raise HTTPException(status_code=500, detail=f"failed to parse scan summary: {exc}") from exc

    finished_at = _utc_now()
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
        timeout_symbols=int(summary.get("timeout_symbols", 0)),
        summary_path=str(output_summary),
        csv_path=str(output_csv),
        jsonl_path=str(output_jsonl),
        top_candidates=[FullMarket2000Candidate(**item) for item in top_candidates],
    )
    _set_full_market_progress(
        run_id=run_id,
        status="COMPLETED",
        scanned_symbols=response.total_symbols,
        total_symbols=response.total_symbols,
        buy_pass_symbols=response.buy_pass_symbols,
        error_symbols=response.error_symbols,
        timeout_symbols=response.timeout_symbols,
        progress_pct=100.0,
        started_at=started_at,
        updated_at=finished_at,
        finished_at=finished_at,
        message="scan completed",
        progress_path=str(progress_path),
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
            "timeout_symbols": response.timeout_symbols,
            "summary_path": response.summary_path,
            "csv_path": response.csv_path,
            "jsonl_path": response.jsonl_path,
            "progress_path": str(progress_path),
            "stdout_tail": stdout_tail,
            "stderr_tail": stderr_tail,
        },
        status="OK",
    )
    return response
