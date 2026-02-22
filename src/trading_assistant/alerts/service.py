from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import hmac
import json
from pathlib import Path
from typing import Any, Protocol
from urllib import parse, request

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
    OncallCallbackRequest,
    OncallCallbackResult,
    OncallEventRecord,
    OncallReconcileRequest,
    OncallReconcileResult,
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
    DEFAULT_ONCALL_MAPPING_TEMPLATES: dict[str, dict[str, Any]] = {
        "pagerduty": {
            "incident_id_keys": ["incident.id", "incident_id", "id", "dedup_key"],
            "status_keys": ["event_action", "status", "incident.status"],
            "notification_id_keys": [
                "payload.custom_details.notification_id",
                "custom_details.notification_id",
                "notification_id",
            ],
            "delivery_id_keys": [
                "payload.custom_details.delivery_id",
                "custom_details.delivery_id",
                "delivery_id",
            ],
            "external_ticket_id_keys": ["incident_number", "ticket_id", "issue_key"],
            "ack_statuses": ["acknowledged", "resolve", "resolved", "closed"],
        },
        "opsgenie": {
            "incident_id_keys": ["alertId", "alert_id", "incident_id"],
            "status_keys": ["action", "status"],
            "notification_id_keys": ["details.notification_id", "notification_id"],
            "delivery_id_keys": ["details.delivery_id", "delivery_id"],
            "external_ticket_id_keys": ["ticket_id", "details.ticket_id"],
            "ack_statuses": ["acknowledged", "closed"],
        },
        "wecom": {
            "incident_id_keys": ["incident_id", "id"],
            "status_keys": ["status", "action"],
            "notification_id_keys": ["notification_id"],
            "delivery_id_keys": ["delivery_id"],
            "external_ticket_id_keys": ["ticket_id"],
            "ack_statuses": ["ack", "resolved", "closed"],
        },
        "dingtalk": {
            "incident_id_keys": ["incident_id", "id"],
            "status_keys": ["status", "action"],
            "notification_id_keys": ["notification_id"],
            "delivery_id_keys": ["delivery_id"],
            "external_ticket_id_keys": ["ticket_id"],
            "ack_statuses": ["ack", "resolved", "closed"],
        },
    }

    def __init__(
        self,
        *,
        store: AlertStore,
        audit: AuditService,
        dispatcher: AlertDispatcherProtocol | None = None,
        default_runbook_base_url: str = "",
        oncall_callback_signing_secret: str = "",
        oncall_callback_require_signature: bool = False,
        oncall_callback_signature_ttl_seconds: int = 600,
        oncall_mapping_templates: dict[str, dict[str, Any]] | None = None,
        oncall_reconcile_default_endpoint: str = "",
        oncall_reconcile_timeout_seconds: int = 10,
    ) -> None:
        self.store = store
        self.audit = audit
        self.dispatcher = dispatcher or _NoopDispatcher()
        self.default_runbook_base_url = default_runbook_base_url.rstrip("/")
        self.oncall_callback_signing_secret = (oncall_callback_signing_secret or "").strip()
        self.oncall_callback_require_signature = bool(oncall_callback_require_signature)
        self.oncall_callback_signature_ttl_seconds = max(0, int(oncall_callback_signature_ttl_seconds))
        merged_templates = dict(self.DEFAULT_ONCALL_MAPPING_TEMPLATES)
        if isinstance(oncall_mapping_templates, dict):
            for key, value in oncall_mapping_templates.items():
                name = str(key).strip().lower()
                if not name or not isinstance(value, dict):
                    continue
                merged_templates[name] = dict(value)
        self.oncall_mapping_templates = merged_templates
        self.oncall_reconcile_default_endpoint = (oncall_reconcile_default_endpoint or "").strip()
        self.oncall_reconcile_timeout_seconds = max(1, int(oncall_reconcile_timeout_seconds))

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
                if self._should_suppress_noise(subscription=sub, alert=alert):
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

    def list_oncall_events(
        self,
        *,
        provider: str | None = None,
        incident_id: str | None = None,
        acked: bool | None = None,
        limit: int = 200,
    ) -> list[OncallEventRecord]:
        return self.store.list_oncall_events(
            provider=provider,
            incident_id=incident_id,
            acked=acked,
            limit=limit,
        )

    def process_oncall_callback(self, req: OncallCallbackRequest) -> OncallCallbackResult:
        provider = req.provider.strip().lower() or "generic_oncall"
        payload = dict(req.raw_payload or {})
        mapping_template = self._resolve_mapping_template(provider=provider, requested=req.mapping_template)
        mapped = self._extract_mapped_fields(payload=payload, mapping_template=mapping_template)

        incident_id = (req.incident_id or mapped.get("incident_id") or "").strip()
        status = (req.status or mapped.get("status") or "").strip().lower()
        if not incident_id:
            raise ValueError("incident_id must not be empty")
        if not status:
            raise ValueError("status must not be empty")

        signature_checked, signature_valid = self._verify_callback_signature(
            provider=provider,
            timestamp=req.timestamp,
            signature=req.signature,
            payload=payload,
        )
        if signature_checked and not signature_valid:
            raise ValueError("oncall callback signature invalid")

        linked_ids: set[int] = set()
        if req.notification_id is not None and req.notification_id > 0:
            linked_ids.add(int(req.notification_id))
        mapped_notification_id = self._safe_int(mapped.get("notification_id"))
        if mapped_notification_id is not None and mapped_notification_id > 0:
            linked_ids.add(mapped_notification_id)

        delivery_ids: list[int] = []
        if req.delivery_id is not None and req.delivery_id > 0:
            delivery_ids.append(int(req.delivery_id))
        mapped_delivery_id = self._safe_int(mapped.get("delivery_id"))
        if mapped_delivery_id is not None and mapped_delivery_id > 0:
            delivery_ids.append(mapped_delivery_id)

        payload_notification_id = self._extract_payload_int(
            payload,
            keys=["notification_id", "alert_notification_id"],
        )
        if payload_notification_id is not None:
            linked_ids.add(payload_notification_id)

        payload_delivery_id = self._extract_payload_int(
            payload,
            keys=["delivery_id", "alert_delivery_id"],
        )
        if payload_delivery_id is not None:
            delivery_ids.append(payload_delivery_id)

        for delivery_id in delivery_ids:
            linked_ids.update(self.store.find_notification_ids_by_delivery(int(delivery_id)))

        if not linked_ids:
            linked_ids.update(
                self.store.find_notification_ids_by_incident(
                    provider=provider,
                    incident_id=incident_id,
                    limit=500,
                )
            )

        external_ticket_id = req.external_ticket_id
        if not external_ticket_id:
            external_ticket_id = (
                mapped.get("external_ticket_id")
                or self._extract_payload_str(
                    payload,
                    keys=["external_ticket_id", "ticket_id", "issue_key", "case_id"],
                )
            )

        acked = self._status_implies_ack(
            status,
            provider=provider,
            mapping_template=mapping_template,
        )
        acked_count = 0
        if acked:
            for notification_id in sorted(linked_ids):
                if self.store.ack_notification(notification_id):
                    acked_count += 1

        target_notification_ids = sorted(linked_ids) if linked_ids else [None]
        created = 0
        for notification_id in target_notification_ids:
            _, was_created = self.store.upsert_oncall_event(
                provider=provider,
                incident_id=incident_id,
                status=status,
                notification_id=notification_id,
                delivery_id=delivery_ids[0] if delivery_ids else None,
                external_ticket_id=external_ticket_id,
                acked=acked,
                ack_by=req.ack_by,
                note=req.note,
                payload=payload,
            )
            if was_created:
                created += 1

        message = (
            f"callback stored={len(target_notification_ids)} created={created} "
            f"linked_notifications={len(linked_ids)} acked={acked_count} "
            f"signature_checked={signature_checked}"
        )
        return OncallCallbackResult(
            provider=provider,
            incident_id=incident_id,
            status=status,
            mapping_template=mapping_template,
            signature_checked=signature_checked,
            signature_valid=signature_valid,
            linked_notification_ids=sorted(linked_ids),
            acked_notifications=acked_count,
            stored_events=len(target_notification_ids),
            message=message,
        )

    def reconcile_oncall(self, req: OncallReconcileRequest) -> OncallReconcileResult:
        provider = req.provider.strip().lower() or "generic_oncall"
        endpoint = (req.endpoint or self.oncall_reconcile_default_endpoint).strip()
        if not endpoint:
            raise ValueError("reconcile endpoint is empty")

        errors: list[str] = []
        remote_items = self._load_reconcile_items(endpoint=endpoint, limit=req.limit)
        open_events = self.store.list_oncall_events(provider=provider, acked=False, limit=5000)
        open_incidents = {x.incident_id for x in open_events if x.incident_id}
        matched = 0
        callbacks = 0
        acked_notifications = 0

        mapping_template = self._resolve_mapping_template(provider=provider, requested=req.mapping_template)
        for idx, item in enumerate(remote_items):
            mapped = self._extract_mapped_fields(payload=item, mapping_template=mapping_template)
            incident_id = str(mapped.get("incident_id") or "").strip()
            status = str(mapped.get("status") or "").strip()
            if not incident_id or not status:
                errors.append(f"idx={idx}: missing incident_id/status")
                continue
            if incident_id not in open_incidents:
                continue
            matched += 1
            if req.dry_run:
                continue
            try:
                result = self.process_oncall_callback(
                    OncallCallbackRequest(
                        provider=provider,
                        incident_id=incident_id,
                        status=status,
                        mapping_template=mapping_template,
                        external_ticket_id=str(mapped.get("external_ticket_id") or "").strip() or None,
                        ack_by="oncall_reconcile",
                        note="reconciled_from_provider",
                        raw_payload=item,
                    )
                )
                callbacks += 1
                acked_notifications += result.acked_notifications
            except Exception as exc:  # noqa: BLE001
                errors.append(f"idx={idx}: {exc}")

        return OncallReconcileResult(
            provider=provider,
            endpoint=endpoint,
            mapping_template=mapping_template,
            pulled=len(remote_items),
            matched=matched,
            callbacks=callbacks,
            acked_notifications=acked_notifications,
            dry_run=req.dry_run,
            errors=errors,
        )

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
            "notification_id": notification_id,
            "subscription_id": subscription.id,
            "source": alert.source,
            "severity": alert.severity.value,
            "escalation_level": escalation_level,
            "runbook_url": runbook,
        }
        if channel == "pagerduty" and isinstance(subscription.channel_config, dict):
            routing_key = subscription.channel_config.get("pagerduty_routing_key")
            if isinstance(routing_key, str) and routing_key.strip():
                base_payload["pagerduty_routing_key"] = routing_key.strip()

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
                stage_payload = {
                    **base_payload,
                    "stage_note": stage.note,
                    "stage_level_threshold": stage.level_threshold,
                    "oncall_ref": f"n{notification_id}-l{stage.level_threshold}",
                }
                if channel == "pagerduty" and isinstance(subscription.channel_config, dict):
                    routing_key = subscription.channel_config.get("pagerduty_routing_key")
                    if isinstance(routing_key, str) and routing_key.strip():
                        stage_payload["pagerduty_routing_key"] = routing_key.strip()
                result = self.dispatcher.send(
                    channel=channel,
                    target=target,
                    subject=subject,
                    message=message,
                    payload=stage_payload,
                )
                _ = self.store.create_delivery(
                    notification_id=notification_id,
                    subscription_id=subscription.id,
                    channel=channel,
                    target=target,
                    status=AlertDeliveryStatus.SENT if result.success else AlertDeliveryStatus.FAILED,
                    error_message=result.error_message,
                    payload={
                        **stage_payload,
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
        elif channel in {"im", "wecom", "dingtalk"}:
            raw = config.get("im_to") or config.get("webhooks") or config.get("targets")
            out.extend(AlertService._to_targets(raw))
        elif channel == "pagerduty":
            raw = (
                config.get("pagerduty_events_api")
                or config.get("pagerduty_url")
                or config.get("webhooks")
                or config.get("targets")
            )
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
    def _extract_payload_int(payload: dict[str, Any], *, keys: list[str]) -> int | None:
        for key in keys:
            value = AlertService._extract_path(payload, key)
            if value is None:
                continue
            try:
                parsed = int(value)
            except Exception:  # noqa: BLE001
                continue
            if parsed > 0:
                return parsed
        return None

    @staticmethod
    def _extract_payload_str(payload: dict[str, Any], *, keys: list[str]) -> str | None:
        for key in keys:
            value = AlertService._extract_path(payload, key)
            if value is None:
                continue
            text = str(value).strip()
            if text:
                return text
        return None

    def _resolve_mapping_template(self, *, provider: str, requested: str | None) -> str | None:
        if requested and requested.strip():
            name = requested.strip().lower()
            return name if name in self.oncall_mapping_templates else None
        provider_name = provider.strip().lower()
        if provider_name in self.oncall_mapping_templates:
            return provider_name
        return None

    def _extract_mapped_fields(self, *, payload: dict[str, Any], mapping_template: str | None) -> dict[str, Any]:
        if not mapping_template:
            return {}
        mapping = self.oncall_mapping_templates.get(mapping_template, {})
        if not isinstance(mapping, dict):
            return {}
        out: dict[str, Any] = {}
        field_key_map = {
            "incident_id": "incident_id_keys",
            "status": "status_keys",
            "notification_id": "notification_id_keys",
            "delivery_id": "delivery_id_keys",
            "external_ticket_id": "external_ticket_id_keys",
        }
        for field_name, key_name in field_key_map.items():
            raw_keys = mapping.get(key_name, [])
            keys = [str(x).strip() for x in raw_keys] if isinstance(raw_keys, list) else []
            if not keys:
                continue
            value = self._extract_payload_str(payload, keys=keys)
            if value is not None:
                out[field_name] = value
        return out

    def _verify_callback_signature(
        self,
        *,
        provider: str,
        timestamp: str | None,
        signature: str | None,
        payload: dict[str, Any],
    ) -> tuple[bool, bool]:
        has_signature = bool(signature and signature.strip())
        if not has_signature:
            if self.oncall_callback_require_signature:
                return True, False
            return False, False
        secret = self.oncall_callback_signing_secret
        if not secret:
            return True, False
        signed_timestamp = (timestamp or "").strip()
        if self.oncall_callback_signature_ttl_seconds > 0:
            parsed = self._parse_callback_timestamp(signed_timestamp)
            if parsed is None:
                return True, False
            now = datetime.now(timezone.utc)
            ttl = self.oncall_callback_signature_ttl_seconds
            if abs((now - parsed).total_seconds()) > ttl:
                return True, False

        canonical_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        base = f"{signed_timestamp}|{provider}|{canonical_payload}"
        expected_hex = hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
        provided = str(signature or "").strip()
        if "=" in provided:
            provided = provided.split("=", 1)[1].strip()
        return True, hmac.compare_digest(provided.lower(), expected_hex.lower())

    def _load_reconcile_items(self, *, endpoint: str, limit: int) -> list[dict[str, Any]]:
        payload = self._load_reconcile_payload(endpoint=endpoint)
        rows: list[Any]
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            candidate = (
                payload.get("incidents")
                or payload.get("items")
                or payload.get("results")
                or payload.get("data")
                or []
            )
            rows = candidate if isinstance(candidate, list) else []
        else:
            rows = []
        out: list[dict[str, Any]] = []
        for item in rows[: max(1, min(limit, 5000))]:
            if not isinstance(item, dict):
                continue
            out.append(dict(item))
        return out

    def _load_reconcile_payload(self, *, endpoint: str) -> Any:
        parsed = parse.urlparse(endpoint)
        if parsed.scheme == "file":
            raw_path = parse.unquote(parsed.path)
            if raw_path.startswith("/") and len(raw_path) >= 3 and raw_path[2] == ":":
                raw_path = raw_path[1:]
            return json.loads(Path(raw_path).read_text(encoding="utf-8"))
        if parsed.scheme in {"", "local"}:
            local_path = Path(endpoint.replace("local://", ""))
            if local_path.exists():
                return json.loads(local_path.read_text(encoding="utf-8"))

        req = request.Request(endpoint, method="GET", headers={"Accept": "application/json"})
        with request.urlopen(req, timeout=self.oncall_reconcile_timeout_seconds) as resp:  # noqa: S310
            raw = resp.read().decode("utf-8")
        return json.loads(raw)

    @staticmethod
    def _extract_path(payload: dict[str, Any], key: str) -> Any:
        if key in payload:
            return payload.get(key)
        parts = [x for x in key.split(".") if x.strip()]
        node: Any = payload
        for part in parts:
            if isinstance(node, dict):
                node = node.get(part)
                continue
            if isinstance(node, list):
                try:
                    idx = int(part)
                except ValueError:
                    return None
                if idx < 0 or idx >= len(node):
                    return None
                node = node[idx]
                continue
            return None
        return node

    @staticmethod
    def _parse_callback_timestamp(raw: str) -> datetime | None:
        text = (raw or "").strip()
        if not text:
            return None
        if text.isdigit():
            try:
                value = int(text)
                if value > 10_000_000_000:
                    value = int(value / 1000)
                return datetime.fromtimestamp(value, tz=timezone.utc)
            except Exception:  # noqa: BLE001
                return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:  # noqa: BLE001
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:  # noqa: BLE001
            return None

    def _status_implies_ack(
        self,
        status: str,
        *,
        provider: str,
        mapping_template: str | None,
    ) -> bool:
        normalized = status.strip().lower()
        if not normalized:
            return False
        mapping_name = mapping_template or self._resolve_mapping_template(provider=provider, requested=None)
        if mapping_name:
            mapping = self.oncall_mapping_templates.get(mapping_name, {})
            raw_tokens = mapping.get("ack_statuses", []) if isinstance(mapping, dict) else []
            ack_tokens = {str(x).strip().lower() for x in raw_tokens if str(x).strip()}
            if ack_tokens and (normalized in ack_tokens or any(token in normalized for token in ack_tokens)):
                return True
        ack_tokens = {"ack", "acknowledged", "accepted", "resolved", "closed", "mitigated"}
        if normalized in ack_tokens:
            return True
        return any(token in normalized for token in {"ack", "resolve", "close"})

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

    @staticmethod
    def _should_suppress_noise(*, subscription: AlertSubscriptionRecord, alert: AlertItem) -> bool:
        cfg = subscription.channel_config if isinstance(subscription.channel_config, dict) else {}
        noise = cfg.get("noise_reduction")
        if not isinstance(noise, dict):
            return False
        if alert.severity == SignalLevel.CRITICAL and not bool(noise.get("allow_critical_suppression", False)):
            return False

        min_repeat = int(noise.get("min_repeat_count", 0) or 0)
        repeat_count = int(alert.payload.get("repeat_count", 0) or 0)
        if min_repeat > 0 and repeat_count < min_repeat:
            return True

        max_escalation = int(noise.get("max_escalation_level", 0) or 0)
        escalation_level = int(alert.payload.get("escalation_level", 0) or 0)
        if max_escalation > 0 and escalation_level > max_escalation:
            return False
        if max_escalation > 0 and escalation_level <= max_escalation and alert.severity == SignalLevel.WARNING:
            return True

        keywords = noise.get("message_keywords")
        if isinstance(keywords, list):
            message = alert.message.lower()
            for keyword in keywords:
                token = str(keyword).strip().lower()
                if token and token in message:
                    return True
        return False
