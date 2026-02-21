from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_job_service
from trading_assistant.core.models import (
    JobDefinitionRecord,
    JobRegisterRequest,
    JobRunRecord,
    JobSLAReport,
    JobScheduleTickRequest,
    JobScheduleTickResult,
    JobTriggerRequest,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.ops.job_service import JobService

router = APIRouter(prefix="/ops/jobs", tags=["ops-jobs"])


@router.post("/register", response_model=int)
def register_job(
    req: JobRegisterRequest,
    service: JobService = Depends(get_job_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> int:
    try:
        row_id = service.register(req)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    audit.log(
        event_type="ops_job",
        action="register",
        payload={"job_id": row_id, "job_type": req.job_type.value, "name": req.name},
    )
    return row_id


@router.get("", response_model=list[JobDefinitionRecord])
def list_jobs(
    active_only: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
    service: JobService = Depends(get_job_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[JobDefinitionRecord]:
    return service.list_jobs(active_only=active_only, limit=limit)


@router.post("/{job_id}/run", response_model=JobRunRecord)
def trigger_job(
    job_id: int,
    req: JobTriggerRequest,
    service: JobService = Depends(get_job_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.RESEARCH)),
) -> JobRunRecord:
    try:
        run = service.trigger(job_id=job_id, triggered_by=req.triggered_by)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc

    audit.log(
        event_type="ops_job",
        action="run",
        payload={
            "job_id": job_id,
            "run_id": run.run_id,
            "status": run.status.value,
            "triggered_by": req.triggered_by,
        },
        status="OK" if run.status.value == "SUCCESS" else "ERROR",
    )
    return run


@router.get("/{job_id}/runs", response_model=list[JobRunRecord])
def list_job_runs(
    job_id: int,
    limit: int = Query(default=200, ge=1, le=1000),
    service: JobService = Depends(get_job_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> list[JobRunRecord]:
    return service.list_runs(job_id=job_id, limit=limit)


@router.get("/runs/{run_id}", response_model=JobRunRecord | None)
def get_job_run(
    run_id: str,
    service: JobService = Depends(get_job_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK, UserRole.RESEARCH)),
) -> JobRunRecord | None:
    return service.get_run(run_id)


@router.post("/scheduler/tick", response_model=JobScheduleTickResult)
def scheduler_tick(
    req: JobScheduleTickRequest,
    service: JobService = Depends(get_job_service),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK)),
) -> JobScheduleTickResult:
    result = service.scheduler_tick(as_of=req.as_of, triggered_by=req.triggered_by)
    audit.log(
        event_type="ops_scheduler",
        action="manual_tick",
        status="ERROR" if result.errors else "OK",
        payload={
            "tick_time": result.tick_time.isoformat(),
            "timezone": result.timezone,
            "matched_jobs": len(result.matched_jobs),
            "triggered_runs": len(result.triggered_runs),
            "skipped_jobs": len(result.skipped_jobs),
            "errors": "; ".join(result.errors[:5]),
        },
    )
    return result


@router.get("/scheduler/sla", response_model=JobSLAReport)
def scheduler_sla(
    grace_minutes: int = Query(default=15, ge=0, le=1440),
    running_timeout_minutes: int | None = Query(default=None, ge=1, le=10080),
    service: JobService = Depends(get_job_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.AUDIT, UserRole.RISK)),
) -> JobSLAReport:
    return service.evaluate_sla(
        grace_minutes=grace_minutes,
        running_timeout_minutes=running_timeout_minutes,
    )
