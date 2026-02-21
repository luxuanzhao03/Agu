from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from trading_assistant.alerts.service import AlertService
from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import JobSLAReport
from trading_assistant.ops.job_service import JobService


class JobSchedulerWorker:
    def __init__(
        self,
        jobs: JobService,
        audit: AuditService,
        alerts: AlertService,
        *,
        tick_seconds: int = 30,
        sla_grace_minutes: int = 15,
        sla_log_cooldown_seconds: int = 1800,
        sync_alerts_from_audit: bool = True,
    ) -> None:
        self.jobs = jobs
        self.audit = audit
        self.alerts = alerts
        self.tick_seconds = max(5, tick_seconds)
        self.sla_grace_minutes = max(0, sla_grace_minutes)
        self.sla_log_cooldown_seconds = max(60, sla_log_cooldown_seconds)
        self.sync_alerts_from_audit = sync_alerts_from_audit
        self._running = False
        self._last_sla_log_by_key: dict[str, datetime] = {}

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            self.run_once()
            await asyncio.sleep(self.tick_seconds)

    async def stop(self) -> None:
        self._running = False

    def run_once(self) -> None:
        now = datetime.now(timezone.utc)
        tick = self.jobs.scheduler_tick(as_of=now, triggered_by="scheduler")
        if tick.matched_jobs or tick.errors:
            status = "ERROR" if tick.errors else "OK"
            self.audit.log(
                event_type="ops_scheduler",
                action="tick",
                status=status,
                payload={
                    "tick_time": tick.tick_time.isoformat(),
                    "timezone": tick.timezone,
                    "matched_jobs": len(tick.matched_jobs),
                    "triggered_runs": len(tick.triggered_runs),
                    "skipped_jobs": len(tick.skipped_jobs),
                    "errors": "; ".join(tick.errors[:5]),
                },
            )
        for run in tick.triggered_runs:
            self.audit.log(
                event_type="ops_job",
                action="scheduled_run",
                status="OK" if run.status.value == "SUCCESS" else "ERROR",
                payload={
                    "job_id": run.job_id,
                    "run_id": run.run_id,
                    "status": run.status.value,
                    "triggered_by": run.triggered_by,
                },
            )

        sla = self.jobs.evaluate_sla(as_of=now, grace_minutes=self.sla_grace_minutes)
        self._audit_sla_breaches(sla)
        if self.jobs.event_connector is not None:
            _ = self.jobs.event_connector.sync_sla_alerts(
                audit=self.audit,
                lookback_days=30,
                cooldown_seconds=self.sla_log_cooldown_seconds,
            )

        if self.sync_alerts_from_audit:
            _ = self.alerts.sync_from_audit(limit=1000)

    def _audit_sla_breaches(self, report: JobSLAReport) -> None:
        if not report.breaches:
            return
        now = datetime.now(timezone.utc)
        for breach in report.breaches:
            expected = breach.expected_run_at.isoformat() if breach.expected_run_at else ""
            key = f"{breach.job_id}|{breach.breach_type.value}|{expected}"
            last = self._last_sla_log_by_key.get(key)
            if last is not None and (now - last).total_seconds() < self.sla_log_cooldown_seconds:
                continue
            self._last_sla_log_by_key[key] = now
            self.audit.log(
                event_type="ops_sla",
                action=breach.breach_type.value.lower(),
                status="ERROR" if breach.severity.value == "CRITICAL" else "OK",
                payload={
                    "job_id": breach.job_id,
                    "job_name": breach.job_name,
                    "schedule_cron": breach.schedule_cron,
                    "breach_type": breach.breach_type.value,
                    "severity": breach.severity.value,
                    "message": breach.message,
                    "expected_run_at": expected,
                    "last_run_at": breach.last_run_at.isoformat() if breach.last_run_at else None,
                    "delay_minutes": breach.delay_minutes,
                },
            )
