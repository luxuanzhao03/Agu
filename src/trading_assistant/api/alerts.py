from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from trading_assistant.alerts.service import AlertService
from trading_assistant.core.container import get_alert_service
from trading_assistant.core.models import (
    AlertDeliveryRecord,
    AlertDeliveryStatus,
    AlertItem,
    AlertNotificationRecord,
    OncallCallbackRequest,
    OncallCallbackResult,
    OncallEventRecord,
    OncallReconcileRequest,
    OncallReconcileResult,
    AlertSubscriptionCreateRequest,
    AlertSubscriptionRecord,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles

router = APIRouter(prefix="/alerts", tags=["alerts"])


@router.get("/recent", response_model=list[AlertItem])
def recent_alerts(
    limit: int = Query(default=100, ge=1, le=1000),
    sync_limit: int = Query(default=500, ge=1, le=2000),
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> list[AlertItem]:
    _ = alerts.sync_from_audit(limit=sync_limit)
    notifications = alerts.list_notifications(only_unacked=False, limit=limit)
    return [
        AlertItem(
            event_id=n.event_id,
            event_time=n.created_at,
            severity=n.severity,
            source=n.source,
            message=n.message,
            payload=n.payload,
        )
        for n in notifications
    ]


@router.post("/subscriptions", response_model=int)
def create_subscription(
    req: AlertSubscriptionCreateRequest,
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> int:
    return alerts.create_subscription(req)


@router.get("/subscriptions", response_model=list[AlertSubscriptionRecord])
def list_subscriptions(
    owner: str | None = Query(default=None),
    enabled_only: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> list[AlertSubscriptionRecord]:
    return alerts.list_subscriptions(owner=owner, enabled_only=enabled_only, limit=limit)


@router.get("/notifications", response_model=list[AlertNotificationRecord])
def list_notifications(
    subscription_id: int | None = Query(default=None),
    only_unacked: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=2000),
    sync_limit: int = Query(default=500, ge=1, le=2000),
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> list[AlertNotificationRecord]:
    _ = alerts.sync_from_audit(limit=sync_limit)
    return alerts.list_notifications(subscription_id=subscription_id, only_unacked=only_unacked, limit=limit)


@router.post("/notifications/{notification_id}/ack", response_model=bool)
def ack_notification(
    notification_id: int,
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> bool:
    return alerts.ack(notification_id=notification_id)


@router.post("/oncall/callback", response_model=OncallCallbackResult)
def oncall_callback(
    req: OncallCallbackRequest,
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.AUDIT)),
) -> OncallCallbackResult:
    try:
        return alerts.process_oncall_callback(req)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/oncall/events", response_model=list[OncallEventRecord])
def list_oncall_events(
    provider: str | None = Query(default=None),
    incident_id: str | None = Query(default=None),
    acked: bool | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.AUDIT)),
) -> list[OncallEventRecord]:
    return alerts.list_oncall_events(
        provider=provider,
        incident_id=incident_id,
        acked=acked,
        limit=limit,
    )


@router.post("/oncall/reconcile", response_model=OncallReconcileResult)
def reconcile_oncall(
    req: OncallReconcileRequest,
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.ADMIN, UserRole.RISK, UserRole.AUDIT)),
) -> OncallReconcileResult:
    try:
        return alerts.reconcile_oncall(req)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/deliveries", response_model=list[AlertDeliveryRecord])
def list_deliveries(
    notification_id: int | None = Query(default=None),
    subscription_id: int | None = Query(default=None),
    status: AlertDeliveryStatus | None = Query(default=None),
    limit: int = Query(default=200, ge=1, le=5000),
    alerts: AlertService = Depends(get_alert_service),
    _auth: AuthContext = Depends(require_roles(UserRole.AUDIT, UserRole.RISK, UserRole.ADMIN)),
) -> list[AlertDeliveryRecord]:
    return alerts.list_deliveries(
        notification_id=notification_id,
        subscription_id=subscription_id,
        status=status,
        limit=limit,
    )
