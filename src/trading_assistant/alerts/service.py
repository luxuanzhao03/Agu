from __future__ import annotations

from typing import Any, Protocol

from trading_assistant.alerts.dispatcher import AlertSendResult
from trading_assistant.alerts.store import AlertStore
from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import (
    AlertDeliveryRecord,
    AlertDeliveryStatus,
    AlertEscalationStage,
    AlertItem,
    AlertNotificationRecord,
    AlertSubscriptionCreateRequest,
    AlertSubscriptionRecord,
    AuditEventRecord,
    SignalLevel,
)


def _severity_rank(level: SignalLevel) -> int:
    if level == SignalLevel.CRITICAL:
        return 3
    if level == SignalLevel.WARNING:
        return 2
    return 1


class AlertDispatcherProtocol(Protocol):
    def send(
        self,
        *,
        channel: str,
        target: str,
        subject: str,
        message: str,
        payload: dict[str, Any],
    ) -> AlertSendResult:
        ...


class _NoopDispatcher:
    def send(
        self,
        *,
        channel: str,
        target: str,
        subject: str,
        message: str,
        payload: dict[str, Any],
    ) -> AlertSendResult:
        _ = (channel, target, subject, message, payload)
        return AlertSendResult(success=False, error_message="dispatcher not configured")


class AlertService:
    def __init__(
        self,
        *,
        store: AlertStore,
        audit: AuditService,
        dispatcher: AlertDispatcherProtocol | None = None,
        default_runbook_base_url: str = "",
    ) -> None:
        self.store = store
        self.audit = audit
        self.dispatcher = dispatcher or _NoopDispatcher()
        self.default_runbook_base_url = default_runbook_base_url.rstrip("/")

    def create_subscription(self, req: AlertSubscriptionCreateRequest) -> int:
        return self.store.create_subscription(req)

    def list_subscriptions(
        self,
        owner: str | None = None,
        enabled_only: bool = False,
        limit: int = 200,
    ) -> list[AlertSubscriptionRecord]:
        return self.store.list_subscriptions(owner=owner, enabled_only=enabled_only, limit=limit)

    def sync_from_audit(self, limit: int = 500) -> int:
        events = self.audit.query(limit=limit)
        subscriptions = self.store.list_subscriptions(enabled_only=True, limit=1000)
        inserted = 0
        for event in reversed(events):
            alert = self._event_to_alert(event)
            if alert is None:
                continue
            for sub in subscriptions:
                if not self._subscription_match(sub, event, alert):
                    continue
                dedupe_key = f"{alert.source}|{alert.message}"
                if self.store.exists_recent_notification(sub.id, dedupe_key, sub.dedupe_window_sec):
                    continue
                row_id = self.store.create_notification(
                    subscription_id=sub.id,
                    event_id=event.id,
                    severity=alert.severity,
                    source=alert.source,
                    message=alert.message,
                    payload=alert.payload,
                    dedupe_key=dedupe_key,
                )
                if row_id is None:
                    continue
                inserted += 1
                self._dispatch_notification(
                    subscription=sub,
                    notification_id=row_id,
                    alert=alert,
                    event=event,
                )
        return inserted

    def list_notifications(
        self,
        subscription_id: int | None = None,
        only_unacked: bool = False,
        limit: int = 200,
    ) -> list[AlertNotificationRecord]:
        return self.store.list_notifications(
            subscription_id=subscription_id,
            only_unacked=only_unacked,
            limit=limit,
        )

    def list_deliveries(
        self,
        *,
        notification_id: int | None = None,
        subscription_id: int | None = None,
        status: AlertDeliveryStatus | None = None,
        limit: int = 200,
    ) -> list[AlertDeliveryRecord]:
        return self.store.list_deliveries(
            notification_id=notification_id,
            subscription_id=subscription_id,
            status=status,
            limit=limit,
        )

    def ack(self, notification_id: int) -> bool:
        return self.store.ack_notification(notification_id)

    def count_notifications(
        self,
        *,
        only_unacked: bool = False,
        severity: SignalLevel | None = None,
    ) -> int:
        return self.store.count_notifications(only_unacked=only_unacked, severity=severity)

    def _dispatch_notification(
        self,
        *,
        subscription: AlertSubscriptionRecord,
        notification_id: int,
        alert: AlertItem,
        event: AuditEventRecord,
    ) -> None:
        channel = subscription.channel.strip().lower()
        if channel == "inbox":
            _ = self.store.create_delivery(
                notification_id=notification_id,
                subscription_id=subscription.id,
                channel="inbox",
                target="inbox",
                status=AlertDeliveryStatus.SKIPPED,
                payload={"reason": "inbox_only"},
            )
            return

        runbook = self._resolve_runbook_url(subscription=subscription, alert=alert)
        escalation_level = self._resolve_escalation_level(payload=alert.payload, severity=alert.severity)
        subject = f"[{alert.severity.value}] {alert.source}"
        message = self._render_message(
            alert=alert,
            event=event,
            runbook_url=runbook,
            escalation_level=escalation_level,
        )
        base_payload: dict[str, Any] = {
            "event_id": alert.event_id,
            "source": alert.source,
            "severity": alert.severity.value,
            "escalation_level": escalation_level,
            "runbook_url": runbook,
        }

        if channel == "oncall":
            self._dispatch_oncall(
                subscription=subscription,
                notification_id=notification_id,
                subject=subject,
                message=message,
                base_payload=base_payload,
                escalation_level=escalation_level,
            )
            return

        targets = self._resolve_targets(channel=channel, config=subscription.channel_config)
        if not targets:
            _ = self.store.create_delivery(
                notification_id=notification_id,
                subscription_id=subscription.id,
                channel=channel,
                target="",
                status=AlertDeliveryStatus.FAILED,
                error_message="channel target is empty",
                payload=base_payload,
            )
            return

        for target in targets:
            result = self.dispatcher.send(
                channel=channel,
                target=target,
                subject=subject,
                message=message,
                payload=base_payload,
            )
            _ = self.store.create_delivery(
                notification_id=notification_id,
                subscription_id=subscription.id,
                channel=channel,
                target=target,
                status=AlertDeliveryStatus.SENT if result.success else AlertDeliveryStatus.FAILED,
                error_message=result.error_message,
                payload={**base_payload, "provider_status": result.provider_status},
            )

    def _dispatch_oncall(
        self,
        *,
        subscription: AlertSubscriptionRecord,
        notification_id: int,
        subject: str,
        message: str,
        base_payload: dict[str, Any],
        escalation_level: int,
    ) -> None:
        stages = self._resolve_escalation_chain(subscription)
        triggered = False
        for stage in stages:
            if escalation_level < stage.level_threshold:
                continue
            channel = stage.channel.strip().lower() if stage.channel else "im"
            targets = [x.strip() for x in stage.targets if x and x.strip()]
            if not targets:
                targets = self._resolve_targets(channel=channel, config=subscription.channel_config)
            if not targets:
                _ = self.store.create_delivery(
                    notification_id=notification_id,
                    subscription_id=subscription.id,
                    channel=channel,
                    target="",
                    status=AlertDeliveryStatus.FAILED,
                    error_message=f"no targets for escalation stage >= L{stage.level_threshold}",
                    payload={**base_payload, "stage_note": stage.note},
                )
                continue
            triggered = True
            for target in targets:
                result = self.dispatcher.send(
                    channel=channel,
                    target=target,
                    subject=subject,
                    message=message,
                    payload={**base_payload, "stage_note": stage.note, "stage_level_threshold": stage.level_threshold},
                )
                _ = self.store.create_delivery(
                    notification_id=notification_id,
                    subscription_id=subscription.id,
                    channel=channel,
                    target=target,
                    status=AlertDeliveryStatus.SENT if result.success else AlertDeliveryStatus.FAILED,
                    error_message=result.error_message,
                    payload={
                        **base_payload,
                        "stage_note": stage.note,
                        "stage_level_threshold": stage.level_threshold,
                        "provider_status": result.provider_status,
                    },
                )
        if not triggered:
            _ = self.store.create_delivery(
                notification_id=notification_id,
                subscription_id=subscription.id,
                channel="oncall",
                target="",
                status=AlertDeliveryStatus.SKIPPED,
                payload={
                    **base_payload,
                    "reason": f"escalation level={escalation_level} did not match any escalation stage",
                },
            )

    @staticmethod
    def _resolve_targets(*, channel: str, config: dict[str, Any]) -> list[str]:
        if not isinstance(config, dict):
            return []
        out: list[str] = []
        if channel == "email":
            raw = config.get("email_to") or config.get("to") or config.get("targets")
            out.extend(AlertService._to_targets(raw))
        elif channel == "im":
            raw = config.get("im_to") or config.get("webhooks") or config.get("targets")
            out.extend(AlertService._to_targets(raw))
        else:
            out.extend(AlertService._to_targets(config.get("targets")))
        deduped: list[str] = []
        seen: set[str] = set()
        for item in out:
            if item in seen:
                continue
            seen.add(item)
            deduped.append(item)
        return deduped

    @staticmethod
    def _to_targets(raw: Any) -> list[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            text = raw.strip()
            if not text:
                return []
            if "," in text:
                return [x.strip() for x in text.split(",") if x.strip()]
            return [text]
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        return []

    @staticmethod
    def _resolve_escalation_level(*, payload: dict[str, Any], severity: SignalLevel) -> int:
        raw = payload.get("escalation_level")
        try:
            value = int(raw)
            return max(0, min(value, 10))
        except Exception:  # noqa: BLE001
            if severity == SignalLevel.CRITICAL:
                return 2
            if severity == SignalLevel.WARNING:
                return 1
            return 0

    def _resolve_runbook_url(self, *, subscription: AlertSubscriptionRecord, alert: AlertItem) -> str:
        if subscription.runbook_url:
            return subscription.runbook_url
        raw = alert.payload.get("runbook_url")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        connector_name = alert.payload.get("connector_name")
        if isinstance(connector_name, str) and connector_name.strip() and self.default_runbook_base_url:
            return f"{self.default_runbook_base_url}/{connector_name.strip()}"
        return ""

    @staticmethod
    def _resolve_escalation_chain(subscription: AlertSubscriptionRecord) -> list[AlertEscalationStage]:
        stages = [x for x in subscription.escalation_chain if x.targets or x.channel]
        if not stages:
            return [AlertEscalationStage(level_threshold=1, channel="im", targets=[])]
        return sorted(stages, key=lambda x: x.level_threshold)

    @staticmethod
    def _render_message(
        *,
        alert: AlertItem,
        event: AuditEventRecord,
        runbook_url: str,
        escalation_level: int,
    ) -> str:
        lines = [
            f"Alert Source: {alert.source}",
            f"Severity: {alert.severity.value}",
            f"Message: {alert.message}",
            f"Event Time: {alert.event_time.isoformat()}",
            f"Audit Event ID: {event.id}",
            f"Escalation Level: {escalation_level}",
        ]
        if runbook_url:
            lines.append(f"Runbook: {runbook_url}")
        return "\n".join(lines)

    def _subscription_match(
        self,
        sub: AlertSubscriptionRecord,
        event: AuditEventRecord,
        alert: AlertItem,
    ) -> bool:
        if sub.event_types and event.event_type not in set(sub.event_types):
            return False
        return _severity_rank(alert.severity) >= _severity_rank(sub.min_severity)

    def _event_to_alert(self, event: AuditEventRecord) -> AlertItem | None:
        payload = event.payload
        severity = SignalLevel.INFO
        message = f"{event.event_type}:{event.action}"

        if event.event_type in {"ops_sla", "event_connector_sla", "event_connector_sla_escalation"}:
            raw = str(payload.get("severity", "WARNING")).upper()
            severity = SignalLevel.CRITICAL if raw == "CRITICAL" else SignalLevel.WARNING
            raw_message = payload.get("message")
            if isinstance(raw_message, str) and raw_message.strip():
                message = raw_message
            elif event.event_type == "event_connector_sla_escalation":
                reason = payload.get("escalation_reason")
                connector = payload.get("connector_name")
                if isinstance(reason, str) and reason.strip():
                    message = f"{connector or 'connector'} escalation: {reason}"
        elif event.status.upper() == "ERROR":
            severity = SignalLevel.CRITICAL
            raw_error = payload.get("error")
            if isinstance(raw_error, str) and raw_error.strip():
                message = raw_error
        elif payload.get("blocked") is True:
            severity = SignalLevel.WARNING
            message = "Blocked signal or risk event."
        elif event.event_type in {"portfolio_risk", "risk_check"}:
            severity = SignalLevel.WARNING
            message = "Risk event generated."
        elif event.event_type == "compliance" and payload.get("passed") is False:
            severity = SignalLevel.WARNING
            message = "Compliance preflight failed."

        if severity == SignalLevel.INFO:
            return None
        return AlertItem(
            event_id=event.id,
            event_time=event.event_time,
            severity=severity,
            source=event.event_type,
            message=message,
            payload=payload,
        )
