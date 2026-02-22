from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
from pathlib import Path
import shutil
from urllib import parse, request
import zipfile
from uuid import uuid4

from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.audit.service import AuditService
from trading_assistant.core.models import (
    ComplianceEvidenceBundleSignature,
    ComplianceEvidenceCounterSignEntry,
    ComplianceEvidenceCounterSignRequest,
    ComplianceEvidenceCounterSignResult,
    ComplianceEvidenceExportRequest,
    ComplianceEvidenceExportResult,
    ComplianceEvidenceFileItem,
    ComplianceEvidenceVerifyRequest,
    ComplianceEvidenceVerifyResult,
)
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.governance.event_nlp_governance import EventNLPGovernanceService
from trading_assistant.strategy.governance_service import StrategyGovernanceService


class ComplianceEvidenceService:
    def __init__(
        self,
        *,
        audit: AuditService,
        strategy_gov: StrategyGovernanceService,
        event_connector: EventConnectorService,
        event_nlp: EventNLPGovernanceService,
        autotune: AutoTuneService | None = None,
        default_signing_secret: str = "",
        default_vault_dir: str = "reports/compliance_vault",
        default_external_worm_endpoint: str = "",
        default_external_kms_wrap_endpoint: str = "",
        default_external_auth_token: str = "",
        default_external_timeout_seconds: int = 10,
        default_external_require_success: bool = False,
    ) -> None:
        self.audit = audit
        self.strategy_gov = strategy_gov
        self.event_connector = event_connector
        self.event_nlp = event_nlp
        self.autotune = autotune
        self.default_signing_secret = (default_signing_secret or "").strip()
        self.default_vault_dir = default_vault_dir
        self.default_external_worm_endpoint = (default_external_worm_endpoint or "").strip()
        self.default_external_kms_wrap_endpoint = (default_external_kms_wrap_endpoint or "").strip()
        self.default_external_auth_token = (default_external_auth_token or "").strip()
        self.default_external_timeout_seconds = max(1, int(default_external_timeout_seconds))
        self.default_external_require_success = bool(default_external_require_success)

    def export_bundle(self, req: ComplianceEvidenceExportRequest) -> ComplianceEvidenceExportResult:
        now = datetime.now(timezone.utc)
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        bundle_id = f"{req.package_prefix}_{stamp}_{uuid4().hex[:8]}"
        output_root = Path(req.output_dir)
        bundle_dir = output_root / bundle_id
        bundle_dir.mkdir(parents=True, exist_ok=True)

        files: list[ComplianceEvidenceFileItem] = []
        summary: dict[str, object] = {
            "retention_policy": req.retention_policy,
            "vault_mode": req.vault_mode,
            "kms_key_id": req.kms_key_id or "",
            "archive_policy_version": "v1",
            "external_worm_endpoint": req.external_worm_endpoint or self.default_external_worm_endpoint,
            "external_kms_wrap_endpoint": req.external_kms_wrap_endpoint or self.default_external_kms_wrap_endpoint,
        }
        archive_policy = {
            "policy_version": "v1",
            "retention_policy": req.retention_policy,
            "vault_mode": req.vault_mode,
            "kms_key_id": req.kms_key_id or "",
            "triggered_by": req.triggered_by,
            "generated_at": now.isoformat(),
        }
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="archive_policy.json",
                payload=archive_policy,
            )
        )

        chain = self.audit.verify_chain(limit=req.audit_verify_limit)
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="audit_chain_verify.json",
                payload=chain.model_dump(mode="json"),
            )
        )

        audit_events = self.audit.query(event_type=req.audit_event_type, limit=req.audit_event_limit)
        files.append(
            self._write_jsonl(
                bundle_dir=bundle_dir,
                relative_path="audit_events.jsonl",
                rows=[row.model_dump(mode="json") for row in audit_events],
            )
        )
        summary["audit_events"] = len(audit_events)
        summary["audit_chain_valid"] = chain.valid

        autotune_events = self.audit.query(event_type="autotune", limit=min(req.audit_event_limit, 5000))
        files.append(
            self._write_jsonl(
                bundle_dir=bundle_dir,
                relative_path="autotune_events.jsonl",
                rows=[row.model_dump(mode="json") for row in autotune_events],
            )
        )
        summary["autotune_events"] = len(autotune_events)
        if self.autotune is not None:
            profiles = self.autotune.list_profiles(limit=2000)
            rollout_rules = self.autotune.list_rollout_rules(limit=2000)
            files.append(
                self._write_json(
                    bundle_dir=bundle_dir,
                    relative_path="autotune_profiles.json",
                    payload=[x.model_dump(mode="json") for x in profiles],
                )
            )
            files.append(
                self._write_json(
                    bundle_dir=bundle_dir,
                    relative_path="autotune_rollout_rules.json",
                    payload=[x.model_dump(mode="json") for x in rollout_rules],
                )
            )
            summary["autotune_profiles"] = len(profiles)
            summary["autotune_rollout_rules"] = len(rollout_rules)

        versions = self.strategy_gov.list_versions(
            strategy_name=req.strategy_name,
            limit=req.strategy_version_limit,
        )
        decisions_payload: list[dict] = []
        for version in versions:
            decisions = self.strategy_gov.list_decisions(
                strategy_name=version.strategy_name,
                version=version.version,
                limit=500,
            )
            decisions_payload.append(
                {
                    "strategy_name": version.strategy_name,
                    "version": version.version,
                    "decision_count": len(decisions),
                    "decisions": [x.model_dump(mode="json") for x in decisions],
                }
            )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="strategy_versions.json",
                payload=[x.model_dump(mode="json") for x in versions],
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="strategy_decisions.json",
                payload=decisions_payload,
            )
        )
        summary["strategy_versions"] = len(versions)

        connector_overview = self.event_connector.overview(limit=1000)
        source_states = self.event_connector.list_source_states(
            connector_name=req.connector_name,
            limit=5000,
        )
        sla_report = self.event_connector.evaluate_sla(include_disabled=True)
        sla_states = self.event_connector.list_sla_alert_states(
            connector_name=req.connector_name,
            open_only=False,
            limit=req.connector_state_limit,
        )
        runs = self.event_connector.list_runs(
            connector_name=req.connector_name,
            limit=req.connector_run_limit,
        )
        failures = self.event_connector.list_failures(
            connector_name=req.connector_name,
            status=None,
            error_keyword=None,
            limit=req.connector_failure_limit,
        )
        coverage = self.event_connector.coverage_summary(lookback_days=req.event_lookback_days)

        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_connector_overview.json",
                payload=connector_overview.model_dump(mode="json"),
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_connector_source_states.json",
                payload=[x.model_dump(mode="json") for x in source_states],
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_connector_sla_report.json",
                payload=sla_report.model_dump(mode="json"),
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_connector_sla_states.json",
                payload=[x.model_dump(mode="json") for x in sla_states],
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_connector_runs.json",
                payload=[x.model_dump(mode="json") for x in runs],
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_connector_failures.json",
                payload=[x.model_dump(mode="json") for x in failures],
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_coverage_summary.json",
                payload=coverage.model_dump(mode="json"),
            )
        )
        summary["connector_runs"] = len(runs)
        summary["connector_failures"] = len(failures)
        summary["connector_open_sla_states"] = sum(1 for x in sla_states if x.is_open)

        active_ruleset = self.event_nlp.get_active_ruleset(include_rules=req.include_ruleset_body)
        drift_monitor = self.event_nlp.drift_monitor(
            source_name=req.source_name,
            limit=req.nlp_monitor_limit,
        )
        drift_snapshots = self.event_nlp.list_drift_snapshots(
            source_name=req.source_name,
            limit=req.nlp_snapshot_limit,
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_nlp_active_ruleset.json",
                payload=active_ruleset.model_dump(mode="json") if active_ruleset else None,
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_nlp_drift_monitor.json",
                payload=drift_monitor.model_dump(mode="json"),
            )
        )
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="event_nlp_drift_snapshots.json",
                payload=[x.model_dump(mode="json") for x in drift_snapshots],
            )
        )
        summary["nlp_drift_snapshots"] = len(drift_snapshots)
        summary["nlp_latest_risk_level"] = drift_monitor.latest_risk_level.value

        if req.include_feedback_summary:
            end_date = now.date()
            start_date = end_date - timedelta(days=max(1, req.event_lookback_days) - 1)
            feedback_summary = self.event_nlp.feedback_summary(
                source_name=req.source_name,
                start_date=start_date,
                end_date=end_date,
            )
            files.append(
                self._write_json(
                    bundle_dir=bundle_dir,
                    relative_path="event_nlp_feedback_summary.json",
                    payload=feedback_summary.model_dump(mode="json"),
                )
            )
            summary["nlp_feedback_samples"] = feedback_summary.sample_size

        manifest_payload = {
            "bundle_id": bundle_id,
            "generated_at": now.isoformat(),
            "request": req.model_dump(mode="json"),
            "summary": summary,
            "files": [x.model_dump(mode="json") for x in files],
        }
        files.append(
            self._write_json(
                bundle_dir=bundle_dir,
                relative_path="manifest.json",
                payload=manifest_payload,
            )
        )

        package_path = output_root / f"{bundle_id}.zip"
        package_size = self._zip_bundle(bundle_dir=bundle_dir, package_path=package_path)
        package_sha256 = hashlib.sha256(package_path.read_bytes()).hexdigest()
        summary["package_sha256"] = package_sha256
        summary["package_size_bytes"] = package_size

        signature_payload: ComplianceEvidenceBundleSignature | None = None
        signature_path: Path | None = None
        if req.sign_bundle:
            signature_payload = self._sign_package(
                bundle_id=bundle_id,
                package_sha256=package_sha256,
                signer=req.signer,
                signing_key_id=req.signing_key_id,
                secret=self.default_signing_secret,
                signed_at=now,
            )
            if signature_payload.enabled:
                signature_path = output_root / f"{bundle_id}.signature.json"
                signature_path.write_text(
                    json.dumps(signature_payload.model_dump(mode="json"), ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            summary["signature_enabled"] = signature_payload.enabled
            summary["signature_algorithm"] = signature_payload.algorithm
            summary["signature_signer"] = signature_payload.signer

        vault_copy_path: Path | None = None
        vault_worm_lock_path: Path | None = None
        vault_envelope_path: Path | None = None
        if req.write_vault_copy:
            vault_root = Path(req.vault_dir or self.default_vault_dir)
            day_path = now.strftime("%Y/%m/%d")
            vault_dir = vault_root / day_path / bundle_id
            vault_dir.mkdir(parents=True, exist_ok=True)
            copied_package = vault_dir / package_path.name
            shutil.copy2(package_path, copied_package)
            if signature_path and signature_path.exists():
                shutil.copy2(signature_path, vault_dir / signature_path.name)

            if req.kms_key_id:
                kms_endpoint = (req.external_kms_wrap_endpoint or self.default_external_kms_wrap_endpoint).strip()
                external_token = (req.external_auth_token or self.default_external_auth_token).strip()
                timeout_seconds = max(1, int(req.external_timeout_seconds or self.default_external_timeout_seconds))
                require_success = bool(req.external_require_success or self.default_external_require_success)
                envelope_payload: dict[str, object]
                if kms_endpoint:
                    try:
                        envelope_payload = self._invoke_external_kms_wrap(
                            endpoint=kms_endpoint,
                            bundle_id=bundle_id,
                            digest=package_sha256,
                            kms_key_id=req.kms_key_id,
                            retention_policy=req.retention_policy,
                            timeout_seconds=timeout_seconds,
                            auth_token=external_token or None,
                        )
                        envelope_payload["mode"] = "external_kms"
                        summary["external_kms_status"] = "OK"
                        summary["external_kms_provider"] = str(envelope_payload.get("provider") or "")
                    except Exception as exc:  # noqa: BLE001
                        if require_success:
                            raise
                        summary["external_kms_status"] = f"ERROR: {exc}"
                        wrapped_digest = hashlib.sha256(
                            f"{req.kms_key_id}|{package_sha256}|{bundle_id}".encode("utf-8")
                        ).hexdigest()
                        envelope_payload = {
                            "bundle_id": bundle_id,
                            "created_at": now.isoformat(),
                            "kms_key_id": req.kms_key_id,
                            "digest": package_sha256,
                            "wrapped_digest": wrapped_digest,
                            "algorithm": "sha256-simulated-kms-wrap",
                            "mode": "fallback_simulated",
                            "error": str(exc),
                        }
                else:
                    wrapped_digest = hashlib.sha256(
                        f"{req.kms_key_id}|{package_sha256}|{bundle_id}".encode("utf-8")
                    ).hexdigest()
                    envelope_payload = {
                        "bundle_id": bundle_id,
                        "created_at": now.isoformat(),
                        "kms_key_id": req.kms_key_id,
                        "digest": package_sha256,
                        "wrapped_digest": wrapped_digest,
                        "algorithm": "sha256-simulated-kms-wrap",
                        "mode": "simulated_local",
                    }
                vault_envelope_path = vault_dir / f"{bundle_id}.kms-envelope.json"
                vault_envelope_path.write_text(
                    json.dumps(envelope_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

            if req.vault_mode == "SIMULATED_WORM":
                lock_payload = {
                    "bundle_id": bundle_id,
                    "locked_at": now.isoformat(),
                    "retention_policy": req.retention_policy,
                    "package_sha256": package_sha256,
                    "signature_path": str(vault_dir / signature_path.name) if signature_path else None,
                }
                vault_worm_lock_path = vault_dir / f"{bundle_id}.worm-lock.json"
                vault_worm_lock_path.write_text(
                    json.dumps(lock_payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                self._set_readonly(copied_package)
                if signature_path and signature_path.exists():
                    self._set_readonly(vault_dir / signature_path.name)
                self._set_readonly(vault_worm_lock_path)

            vault_copy_path = copied_package
            summary["vault_copy_path"] = str(copied_package)
            if vault_worm_lock_path is not None:
                summary["vault_worm_lock_path"] = str(vault_worm_lock_path)
            if vault_envelope_path is not None:
                summary["vault_envelope_path"] = str(vault_envelope_path)

            worm_endpoint = (req.external_worm_endpoint or self.default_external_worm_endpoint).strip()
            external_token = (req.external_auth_token or self.default_external_auth_token).strip()
            timeout_seconds = max(1, int(req.external_timeout_seconds or self.default_external_timeout_seconds))
            require_success = bool(req.external_require_success or self.default_external_require_success)
            if worm_endpoint:
                try:
                    archive_receipt = self._invoke_external_worm_archive(
                        endpoint=worm_endpoint,
                        bundle_id=bundle_id,
                        package_path=copied_package,
                        package_sha256=package_sha256,
                        package_size=package_size,
                        retention_policy=req.retention_policy,
                        kms_key_id=req.kms_key_id,
                        timeout_seconds=timeout_seconds,
                        auth_token=external_token or None,
                    )
                    summary["external_worm_status"] = "OK"
                    summary["external_worm_receipt"] = archive_receipt
                except Exception as exc:  # noqa: BLE001
                    if require_success:
                        raise
                    summary["external_worm_status"] = f"ERROR: {exc}"

        summary["archive_receipt"] = {
            "bundle_id": bundle_id,
            "retention_policy": req.retention_policy,
            "vault_mode": req.vault_mode,
            "vault_copy_written": bool(vault_copy_path),
            "signature_enabled": bool(signature_payload.enabled) if signature_payload else False,
            "kms_key_id": req.kms_key_id or "",
            "generated_at": now.isoformat(),
        }

        bundle_dir_str = str(bundle_dir)
        if req.cleanup_bundle_dir:
            shutil.rmtree(bundle_dir, ignore_errors=True)
            summary["bundle_dir_cleaned"] = True

        return ComplianceEvidenceExportResult(
            bundle_id=bundle_id,
            generated_at=now,
            bundle_dir=bundle_dir_str,
            package_path=str(package_path),
            package_size_bytes=package_size,
            package_sha256=package_sha256,
            signature=signature_payload,
            signature_path=str(signature_path) if signature_path else None,
            vault_copy_path=str(vault_copy_path) if vault_copy_path else None,
            file_count=len(files),
            files=files,
            summary=summary,
        )

    def verify_package(self, req: ComplianceEvidenceVerifyRequest) -> ComplianceEvidenceVerifyResult:
        package_path = Path(req.package_path)
        if not package_path.exists() or not package_path.is_file():
            return ComplianceEvidenceVerifyResult(
                package_path=req.package_path,
                package_exists=False,
                message="package file does not exist",
            )

        package_sha256 = hashlib.sha256(package_path.read_bytes()).hexdigest()
        manifest_exists = False
        manifest_valid = False
        issues: list[str] = []
        try:
            with zipfile.ZipFile(package_path, "r") as zf:
                if "manifest.json" in set(zf.namelist()):
                    manifest_exists = True
                    manifest_raw = json.loads(zf.read("manifest.json").decode("utf-8"))
                    files = manifest_raw.get("files") if isinstance(manifest_raw, dict) else None
                    manifest_valid = bool(
                        isinstance(files, list)
                        and isinstance(manifest_raw.get("bundle_id"), str)
                        and manifest_raw.get("generated_at")
                    )
        except Exception as exc:  # noqa: BLE001
            issues.append(f"manifest verify failed: {exc}")

        signature_checked = False
        signature_valid = False
        secret = (req.signing_secret or self.default_signing_secret or "").strip()
        secret_missing_issue_added = False
        signature_path = Path(req.signature_path) if req.signature_path else package_path.with_suffix(".signature.json")
        if signature_path.exists() and signature_path.is_file():
            signature_checked = True
            try:
                payload = json.loads(signature_path.read_text(encoding="utf-8"))
                digest = str(payload.get("digest") or "")
                signature = str(payload.get("signature") or "")
                signer = str(payload.get("signer") or "")
                key_id = str(payload.get("signing_key_id") or "")
                signed_at_raw = str(payload.get("signed_at") or "")
                signed_at = signed_at_raw
                try:
                    parsed = datetime.fromisoformat(signed_at_raw.replace("Z", "+00:00"))
                    signed_at = parsed.astimezone(timezone.utc).isoformat()
                except Exception:  # noqa: BLE001
                    pass
                if secret:
                    base = f"{digest}|{signer}|{key_id}|{signed_at}"
                    expected = hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
                    signature_valid = (
                        digest == package_sha256 and hmac.compare_digest(signature, expected)
                    )
                    if not signature_valid:
                        issues.append("signature invalid")
                else:
                    issues.append("signature file found but signing secret is empty")
                    secret_missing_issue_added = True
            except Exception as exc:  # noqa: BLE001
                issues.append(f"signature verify failed: {exc}")

        countersign_checked = False
        countersign_valid = False
        countersign_count = 0
        countersign_path = (
            Path(req.countersign_path)
            if req.countersign_path
            else package_path.with_suffix(".countersign.json")
        )
        if countersign_path.exists() and countersign_path.is_file():
            countersign_checked = True
            try:
                entries = self._load_countersign_entries(countersign_path)
                countersign_count = len(entries)
                if countersign_count <= 0:
                    issues.append("countersign empty")
                elif not secret:
                    if not secret_missing_issue_added:
                        issues.append("countersign file found but signing secret is empty")
                        secret_missing_issue_added = True
                else:
                    checks = [self._verify_countersign_entry(x, package_sha256, secret) for x in entries]
                    countersign_valid = all(checks)
                    if not countersign_valid:
                        issues.append("countersign invalid")
            except Exception as exc:  # noqa: BLE001
                issues.append(f"countersign verify failed: {exc}")
        elif req.require_countersign:
            issues.append("countersign file required but not found")

        if not manifest_valid:
            issues.append("manifest invalid")
        deduped_issues: list[str] = []
        seen: set[str] = set()
        for issue in issues:
            if issue in seen:
                continue
            seen.add(issue)
            deduped_issues.append(issue)
        message = "ok" if not deduped_issues else "; ".join(deduped_issues[:5])

        return ComplianceEvidenceVerifyResult(
            package_path=req.package_path,
            package_sha256=package_sha256,
            package_exists=True,
            manifest_exists=manifest_exists,
            manifest_valid=manifest_valid,
            signature_checked=signature_checked,
            signature_valid=signature_valid if signature_checked else False,
            countersign_checked=countersign_checked,
            countersign_valid=countersign_valid if countersign_checked else False,
            countersign_count=countersign_count,
            message=message,
        )

    def countersign_package(self, req: ComplianceEvidenceCounterSignRequest) -> ComplianceEvidenceCounterSignResult:
        package_path = Path(req.package_path)
        if not package_path.exists() or not package_path.is_file():
            raise FileNotFoundError(f"package file does not exist: {req.package_path}")

        package_sha256 = hashlib.sha256(package_path.read_bytes()).hexdigest()
        secret = (req.signing_secret or self.default_signing_secret or "").strip()
        if not secret:
            raise ValueError("signing secret is empty")

        signed_at = datetime.now(timezone.utc)
        base = f"{package_sha256}|{req.signer}|{req.signing_key_id}|{signed_at.isoformat()}"
        signature = hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
        entry = ComplianceEvidenceCounterSignEntry(
            signer=req.signer,
            signing_key_id=req.signing_key_id,
            signed_at=signed_at,
            digest=package_sha256,
            signature=signature,
            note=req.note,
        )

        countersign_path = (
            Path(req.countersign_path)
            if req.countersign_path
            else package_path.with_suffix(".countersign.json")
        )
        existing_entries = self._load_countersign_entries(countersign_path) if countersign_path.exists() else []
        if existing_entries and any(x.digest != package_sha256 for x in existing_entries):
            raise ValueError("existing countersign file digest mismatches package digest")
        all_entries = [*existing_entries, entry]
        payload = {
            "package_path": str(package_path),
            "package_sha256": package_sha256,
            "updated_at": signed_at.isoformat(),
            "entry_count": len(all_entries),
            "entries": [x.model_dump(mode="json") for x in all_entries],
        }
        countersign_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return ComplianceEvidenceCounterSignResult(
            package_path=str(package_path),
            package_sha256=package_sha256,
            countersign_path=str(countersign_path),
            entry_count=len(all_entries),
            last_entry=entry,
        )

    def _invoke_external_kms_wrap(
        self,
        *,
        endpoint: str,
        bundle_id: str,
        digest: str,
        kms_key_id: str,
        retention_policy: str,
        timeout_seconds: int,
        auth_token: str | None,
    ) -> dict[str, object]:
        req_payload = {
            "bundle_id": bundle_id,
            "digest": digest,
            "kms_key_id": kms_key_id,
            "retention_policy": retention_policy,
        }
        resp = self._post_json(
            endpoint=endpoint,
            payload=req_payload,
            timeout_seconds=timeout_seconds,
            auth_token=auth_token,
        )
        wrapped = str(resp.get("wrapped_digest") or "").strip()
        if not wrapped:
            raise ValueError("external kms wrap response missing wrapped_digest")
        return {
            "bundle_id": bundle_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "kms_key_id": kms_key_id,
            "digest": digest,
            "wrapped_digest": wrapped,
            "algorithm": str(resp.get("algorithm") or "external-kms-wrap"),
            "provider": str(resp.get("provider") or ""),
            "key_version": str(resp.get("key_version") or ""),
            "request_id": str(resp.get("request_id") or ""),
        }

    def _invoke_external_worm_archive(
        self,
        *,
        endpoint: str,
        bundle_id: str,
        package_path: Path,
        package_sha256: str,
        package_size: int,
        retention_policy: str,
        kms_key_id: str | None,
        timeout_seconds: int,
        auth_token: str | None,
    ) -> dict[str, object]:
        req_payload = {
            "bundle_id": bundle_id,
            "package_sha256": package_sha256,
            "package_size_bytes": package_size,
            "package_name": package_path.name,
            "retention_policy": retention_policy,
            "kms_key_id": kms_key_id or "",
            "local_path": str(package_path),
        }
        resp = self._post_json(
            endpoint=endpoint,
            payload=req_payload,
            timeout_seconds=timeout_seconds,
            auth_token=auth_token,
        )
        return {
            "provider": str(resp.get("provider") or ""),
            "archive_id": str(resp.get("archive_id") or ""),
            "object_key": str(resp.get("object_key") or ""),
            "lock_until": str(resp.get("lock_until") or ""),
            "request_id": str(resp.get("request_id") or ""),
            "endpoint": endpoint,
        }

    @staticmethod
    def _post_json(
        *,
        endpoint: str,
        payload: dict[str, object],
        timeout_seconds: int,
        auth_token: str | None,
    ) -> dict[str, object]:
        parsed = parse.urlparse(endpoint)
        if parsed.scheme == "file":
            raw_path = parse.unquote(parsed.path)
            if raw_path.startswith("/") and len(raw_path) >= 3 and raw_path[2] == ":":
                raw_path = raw_path[1:]
            path = Path(raw_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return {
                "provider": "file_endpoint",
                "archive_id": f"file-{path.stem}",
                "object_key": path.name,
                "lock_until": "",
                "request_id": "",
                "wrapped_digest": str(payload.get("digest") or ""),
                "algorithm": "file-endpoint",
            }
        if parsed.scheme in {"", "local"}:
            path = Path(endpoint.replace("local://", ""))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return {
                "provider": "local_endpoint",
                "archive_id": f"local-{path.stem}",
                "object_key": path.name,
                "lock_until": "",
                "request_id": "",
                "wrapped_digest": str(payload.get("digest") or ""),
                "algorithm": "local-endpoint",
            }

        headers = {"Content-Type": "application/json; charset=utf-8", "Accept": "application/json"}
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = request.Request(endpoint, method="POST", data=raw, headers=headers)
        with request.urlopen(req, timeout=timeout_seconds) as resp:  # noqa: S310
            text = resp.read().decode("utf-8")
        parsed_resp = json.loads(text) if text.strip() else {}
        return dict(parsed_resp) if isinstance(parsed_resp, dict) else {}

    @staticmethod
    def _sign_package(
        *,
        bundle_id: str,
        package_sha256: str,
        signer: str,
        signing_key_id: str,
        secret: str,
        signed_at: datetime,
    ) -> ComplianceEvidenceBundleSignature:
        cleaned_secret = (secret or "").strip()
        if not cleaned_secret:
            return ComplianceEvidenceBundleSignature(
                enabled=False,
                signer=signer,
                signing_key_id=signing_key_id,
                digest=package_sha256,
                signed_at=signed_at,
            )
        base = f"{package_sha256}|{signer}|{signing_key_id}|{signed_at.isoformat()}"
        signature = hmac.new(
            cleaned_secret.encode("utf-8"),
            base.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        return ComplianceEvidenceBundleSignature(
            enabled=True,
            algorithm="sha256-hmac",
            signer=signer,
            signing_key_id=signing_key_id,
            digest=package_sha256,
            signature=signature,
            signed_at=signed_at,
        )

    @staticmethod
    def _write_json(
        *,
        bundle_dir: Path,
        relative_path: str,
        payload: object,
    ) -> ComplianceEvidenceFileItem:
        path = bundle_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        raw = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        path.write_bytes(raw)
        return ComplianceEvidenceFileItem(
            name=path.name,
            relative_path=str(Path(relative_path).as_posix()),
            size_bytes=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),
        )

    @staticmethod
    def _write_jsonl(
        *,
        bundle_dir: Path,
        relative_path: str,
        rows: list[dict],
    ) -> ComplianceEvidenceFileItem:
        path = bundle_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = [json.dumps(row, ensure_ascii=False) for row in rows]
        raw = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
        path.write_bytes(raw)
        return ComplianceEvidenceFileItem(
            name=path.name,
            relative_path=str(Path(relative_path).as_posix()),
            size_bytes=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),
        )

    @staticmethod
    def _zip_bundle(*, bundle_dir: Path, package_path: Path) -> int:
        package_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(package_path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            for path in sorted(bundle_dir.rglob("*")):
                if not path.is_file():
                    continue
                arcname = path.relative_to(bundle_dir).as_posix()
                zf.write(path, arcname=arcname)
        return package_path.stat().st_size

    @staticmethod
    def _load_countersign_entries(path: Path) -> list[ComplianceEvidenceCounterSignEntry]:
        if not path.exists() or not path.is_file():
            return []
        raw = json.loads(path.read_text(encoding="utf-8"))
        items: list[object]
        if isinstance(raw, dict) and isinstance(raw.get("entries"), list):
            items = list(raw.get("entries") or [])
        elif isinstance(raw, list):
            items = raw
        else:
            raise ValueError("countersign file must be list or object with entries")
        out: list[ComplianceEvidenceCounterSignEntry] = []
        for item in items:
            out.append(ComplianceEvidenceCounterSignEntry.model_validate(item))
        return out

    @staticmethod
    def _verify_countersign_entry(
        entry: ComplianceEvidenceCounterSignEntry,
        package_sha256: str,
        secret: str,
    ) -> bool:
        if entry.digest != package_sha256:
            return False
        base = f"{entry.digest}|{entry.signer}|{entry.signing_key_id}|{entry.signed_at.isoformat()}"
        expected = hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
        return hmac.compare_digest(entry.signature, expected)

    @staticmethod
    def _set_readonly(path: Path) -> None:
        try:
            path.chmod(0o444)
        except Exception:  # noqa: BLE001
            return
