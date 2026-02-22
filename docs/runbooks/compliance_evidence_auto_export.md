# Compliance Evidence Auto-Export Runbook

## Objective

Run signed evidence export on schedule, verify integrity, and archive to immutable vault.

## Pre-check

1. Ensure env is configured:
- `COMPLIANCE_EVIDENCE_SIGNING_SECRET`
- `COMPLIANCE_EVIDENCE_VAULT_DIR`
- optional: `COMPLIANCE_EVIDENCE_EXTERNAL_WORM_ENDPOINT`
- optional: `COMPLIANCE_EVIDENCE_EXTERNAL_KMS_WRAP_ENDPOINT`
- optional: `COMPLIANCE_EVIDENCE_EXTERNAL_AUTH_TOKEN`
2. Verify at least one active export job:
- `GET /ops/jobs?active_only=true`
- `job_type == compliance_evidence_export`

## Execute manually

1. Trigger job:
- `POST /ops/jobs/{job_id}/run`
2. Inspect run result:
- `GET /ops/jobs/runs/{run_id}`
- check `bundle_id`, `package_path`, `signature_enabled`, `vault_copy_path`, `vault_worm_lock_path`, `vault_envelope_path`, `external_worm_status`, `external_kms_status`.

## External archive integration check

1. If external endpoints are enabled in export request, verify:
- `external_worm_status == OK`
- `external_kms_status == OK`
2. If strict mode is enabled (`external_require_success=true`), any external failure should fail export job.
3. Keep external receipts (`external_worm_receipt`, KMS envelope metadata) in evidence archive.

## Dual-control countersign

1. Submit countersign:
- `POST /compliance/evidence/countersign`
- payload: `package_path`, `signer`, `signing_key_id`, optional `countersign_path`, `signing_secret`.
2. Acceptance:
- response `entry_count >= 1`.
- countersign file exists and is archived with package.

## Verify package

1. Verify by API:
- `POST /compliance/evidence/verify`
- payload: `package_path`, optional `signature_path`, optional `countersign_path`, optional `require_countersign`, optional `signing_secret`.
2. Acceptance:
- `package_exists=true`
- `manifest_valid=true`
- if signature present: `signature_checked=true` and `signature_valid=true`.
- if countersign required: `countersign_checked=true`, `countersign_valid=true`, `countersign_count>=1`.

## Incident handling

1. Signature invalid:
- rotate signing secret, re-export package, compare signature payload signer/key id.
2. Vault copy missing:
- check storage permission for `COMPLIANCE_EVIDENCE_VAULT_DIR`.
3. Export job failed:
- inspect job run `error_message`, then run ad-hoc export via `POST /compliance/evidence/export`.
