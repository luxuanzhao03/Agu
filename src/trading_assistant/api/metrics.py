from __future__ import annotations

from collections import Counter

from fastapi import APIRouter, Depends, Query

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import get_audit_service, get_ops_dashboard_service
from trading_assistant.core.models import OpsDashboardSummary, ServiceMetricsSummary
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.ops.dashboard import OpsDashboardService

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/summary", response_model=ServiceMetricsSummary)
def metrics_summary(
    limit: int = Query(default=1000, ge=1, le=5000),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> ServiceMetricsSummary:
    events = audit.query(limit=limit)
    counter = Counter(e.event_type for e in events)
    error_events = sum(1 for e in events if e.status.upper() == "ERROR")
    warning_events = sum(1 for e in events if bool(e.payload.get("blocked")))
    return ServiceMetricsSummary(
        total_events=len(events),
        event_type_counts=dict(counter),
        error_events=error_events,
        warning_events=warning_events,
    )


@router.get("/ops-dashboard", response_model=OpsDashboardSummary)
def ops_dashboard(
    lookback_hours: int = Query(default=24, ge=1, le=24 * 30),
    recent_run_limit: int = Query(default=20, ge=1, le=200),
    replay_limit: int = Query(default=300, ge=1, le=2000),
    sla_grace_minutes: int = Query(default=15, ge=0, le=1440),
    event_lookback_days: int = Query(default=30, ge=1, le=3650),
    sync_alerts_from_audit: bool = Query(default=True),
    dashboard: OpsDashboardService = Depends(get_ops_dashboard_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> OpsDashboardSummary:
    return dashboard.summary(
        lookback_hours=lookback_hours,
        recent_run_limit=recent_run_limit,
        replay_limit=replay_limit,
        sla_grace_minutes=sla_grace_minutes,
        event_lookback_days=event_lookback_days,
        sync_alerts_from_audit=sync_alerts_from_audit,
    )
