from __future__ import annotations

from dataclasses import dataclass
from email.message import EmailMessage
import json
import smtplib
from typing import Any
from urllib import request

from trading_assistant.core.config import Settings


@dataclass
class AlertSendResult:
    success: bool
    error_message: str = ""
    provider_status: str = ""


class RealAlertDispatcher:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def send(
        self,
        *,
        channel: str,
        target: str,
        subject: str,
        message: str,
        payload: dict[str, Any],
    ) -> AlertSendResult:
        channel_normalized = channel.strip().lower()
        if channel_normalized == "email":
            return self._send_email(target=target, subject=subject, message=message)
        if channel_normalized in {"im", "dingtalk", "wecom", "pagerduty"}:
            return self._send_webhook(
                channel=channel_normalized,
                target=target,
                subject=subject,
                message=message,
                payload=payload,
            )
        return AlertSendResult(success=False, error_message=f"unsupported dispatch channel: {channel}")

    def _send_email(self, *, target: str, subject: str, message: str) -> AlertSendResult:
        if not self.settings.alert_email_enabled:
            return AlertSendResult(success=False, error_message="email channel disabled by settings")
        if not self.settings.alert_smtp_host:
            return AlertSendResult(success=False, error_message="smtp host is empty")
        sender = self.settings.alert_email_from
        if not sender:
            return AlertSendResult(success=False, error_message="alert_email_from is empty")

        email = EmailMessage()
        email["Subject"] = subject
        email["From"] = sender
        email["To"] = target
        email.set_content(message)

        try:
            if self.settings.alert_smtp_use_ssl:
                smtp = smtplib.SMTP_SSL(
                    host=self.settings.alert_smtp_host,
                    port=self.settings.alert_smtp_port,
                    timeout=self.settings.alert_notify_timeout_seconds,
                )
            else:
                smtp = smtplib.SMTP(
                    host=self.settings.alert_smtp_host,
                    port=self.settings.alert_smtp_port,
                    timeout=self.settings.alert_notify_timeout_seconds,
                )
            with smtp:
                smtp.ehlo()
                if self.settings.alert_smtp_use_tls and not self.settings.alert_smtp_use_ssl:
                    smtp.starttls()
                    smtp.ehlo()
                username = (self.settings.alert_smtp_username or "").strip()
                if username:
                    smtp.login(username, self.settings.alert_smtp_password or "")
                smtp.send_message(email)
            return AlertSendResult(success=True, provider_status="250")
        except Exception as exc:  # noqa: BLE001
            return AlertSendResult(success=False, error_message=str(exc))

    def _send_webhook(
        self,
        *,
        channel: str,
        target: str,
        subject: str,
        message: str,
        payload: dict[str, Any],
    ) -> AlertSendResult:
        if not self.settings.alert_im_enabled:
            return AlertSendResult(success=False, error_message="im channel disabled by settings")
        url = target.strip()
        if not url:
            if self.settings.alert_im_default_webhook:
                url = self.settings.alert_im_default_webhook
            else:
                return AlertSendResult(success=False, error_message="webhook target is empty")

        body: dict[str, Any]
        if channel == "dingtalk":
            body = {
                "msgtype": "markdown",
                "markdown": {
                    "title": subject,
                    "text": f"### {subject}\n\n{message}",
                },
                "at": {"isAtAll": False},
            }
        elif channel == "wecom":
            body = {
                "msgtype": "markdown",
                "markdown": {
                    "content": f"**{subject}**\n{message}",
                },
            }
        elif channel == "pagerduty":
            routing_key = str(payload.get("pagerduty_routing_key") or "").strip()
            if not routing_key:
                return AlertSendResult(success=False, error_message="pagerduty_routing_key is required in payload")
            body = {
                "routing_key": routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": subject,
                    "source": str(payload.get("source") or "trading-assistant"),
                    "severity": str(payload.get("severity") or "warning").lower(),
                    "custom_details": payload | {"message": message},
                },
            }
        else:
            body = {
                "title": subject,
                "text": message,
                "payload": payload,
            }
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        req = request.Request(
            url=url,
            data=raw,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
        )
        try:
            with request.urlopen(req, timeout=self.settings.alert_notify_timeout_seconds) as resp:  # noqa: S310
                status = getattr(resp, "status", 200)
            if status >= 400:
                return AlertSendResult(success=False, error_message=f"webhook status={status}")
            return AlertSendResult(success=True, provider_status=str(status))
        except Exception as exc:  # noqa: BLE001
            return AlertSendResult(success=False, error_message=str(exc))
