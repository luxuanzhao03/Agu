from __future__ import annotations

from datetime import datetime, timedelta, timezone

from trading_assistant.alerts.service import AlertService
from trading_assistant.core.models import (
    JobRunStatus,
    JobStatus,
    OpsAlertStats,
    OpsDashboardSummary,
    OpsExecutionStats,
    OpsEventStats,
    OpsJobStats,
    SignalLevel,
)
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.ops.job_service import JobService
from trading_assistant.replay.service import ReplayService


class OpsDashboardService:
    def __init__(
        self,
        jobs: JobService,
        alerts: AlertService,
        replay: ReplayService,
        event_connector: EventConnectorService | None = None,
    ) -> None:
        self.jobs = jobs
        self.alerts = alerts
        self.replay = replay
        self.event_connector = event_connector

    def summary(
        self,
        lookback_hours: int = 24,
        recent_run_limit: int = 20,
        replay_limit: int = 300,
        sla_grace_minutes: int = 15,
        event_lookback_days: int = 30,
        sync_alerts_from_audit: bool = True,
    ) -> OpsDashboardSummary:
        now = datetime.now(timezone.utc)
        lookback = now - timedelta(hours=max(1, min(lookback_hours, 24 * 30)))
        recent_runs_window = self.jobs.list_recent_runs(limit=5000, since=lookback)
        recent_runs = self.jobs.list_recent_runs(limit=max(1, min(recent_run_limit, 200)))

        jobs = self.jobs.list_jobs(limit=1000)
        active_jobs = [j for j in jobs if j.status == JobStatus.ACTIVE]
        scheduled_jobs = [j for j in active_jobs if bool(j.schedule_cron)]

        if sync_alerts_from_audit:
            _ = self.alerts.sync_from_audit(limit=1000)
        unacked_total = self.alerts.count_notifications(only_unacked=True)
        unacked_warning = self.alerts.count_notifications(only_unacked=True, severity=SignalLevel.WARNING)
        unacked_critical = self.alerts.count_notifications(only_unacked=True, severity=SignalLevel.CRITICAL)

        replay_report = self.replay.report(limit=max(1, min(replay_limit, 2000)))
        sla_report = self.jobs.evaluate_sla(as_of=now, grace_minutes=sla_grace_minutes)
        event_stats: OpsEventStats | None = None
        if self.event_connector is not None:
            event_stats = self.event_connector.ops_event_stats(
                lookback_days=max(1, min(event_lookback_days, 3650))
            )

        return OpsDashboardSummary(
            generated_at=now,
            jobs=OpsJobStats(
                total_jobs=len(jobs),
                active_jobs=len(active_jobs),
                scheduled_jobs=len(scheduled_jobs),
                runs_last_24h=len(recent_runs_window),
                success_last_24h=sum(1 for r in recent_runs_window if r.status == JobRunStatus.SUCCESS),
                failed_last_24h=sum(1 for r in recent_runs_window if r.status == JobRunStatus.FAILED),
                running_last_24h=sum(1 for r in recent_runs_window if r.status == JobRunStatus.RUNNING),
            ),
            alerts=OpsAlertStats(
                unacked_total=unacked_total,
                unacked_warning=unacked_warning,
                unacked_critical=unacked_critical,
            ),
            execution=OpsExecutionStats(
                sample_size=len(replay_report.items),
                follow_rate=replay_report.follow_rate,
                avg_delay_days=replay_report.avg_delay_days,
                avg_slippage_bps=replay_report.avg_slippage_bps,
            ),
            event=event_stats,
            sla=sla_report,
            recent_runs=recent_runs,
        )
