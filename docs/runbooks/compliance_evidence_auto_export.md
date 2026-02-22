# 合规证据自动导出操作手册

## 目标

按计划执行带签名的证据包导出，完成完整性校验，并归档到不可变仓库（immutable vault）。

## 预检查

1. 确认环境变量已配置：
- `COMPLIANCE_EVIDENCE_SIGNING_SECRET`
- `COMPLIANCE_EVIDENCE_VAULT_DIR`
- 可选：`COMPLIANCE_EVIDENCE_EXTERNAL_WORM_ENDPOINT`
- 可选：`COMPLIANCE_EVIDENCE_EXTERNAL_KMS_WRAP_ENDPOINT`
- 可选：`COMPLIANCE_EVIDENCE_EXTERNAL_AUTH_TOKEN`
2. 确认至少存在一个激活的导出作业：
- `GET /ops/jobs?active_only=true`
- `job_type == compliance_evidence_export`

## 手动执行

1. 触发作业：
- `POST /ops/jobs/{job_id}/run`
2. 检查运行结果：
- `GET /ops/jobs/runs/{run_id}`
- 核对字段：`bundle_id`, `package_path`, `signature_enabled`, `vault_copy_path`, `vault_worm_lock_path`, `vault_envelope_path`, `external_worm_status`, `external_kms_status`。

## 外部归档集成检查

1. 若导出请求启用外部端点，需验证：
- `external_worm_status == OK`
- `external_kms_status == OK`
2. 若启用严格模式（`external_require_success=true`），任何外部失败都应导致导出作业失败。
3. 外部回执（`external_worm_receipt`、KMS envelope 元数据）要归档入证据包。

## 双人复核签名（Dual-control countersign）

1. 提交复签：
- `POST /compliance/evidence/countersign`
- payload：`package_path`, `signer`, `signing_key_id`，可选 `countersign_path`, `signing_secret`。
2. 通过标准：
- 响应 `entry_count >= 1`。
- 复签文件已生成并与证据包一同归档。

## 证据包校验

1. 通过 API 校验：
- `POST /compliance/evidence/verify`
- payload：`package_path`，可选 `signature_path`、`countersign_path`、`require_countersign`、`signing_secret`。
2. 通过标准：
- `package_exists=true`
- `manifest_valid=true`
- 若有签名：`signature_checked=true` 且 `signature_valid=true`。
- 若要求复签：`countersign_checked=true`、`countersign_valid=true`、`countersign_count>=1`。

## 事故处置

1. 签名无效：
- 轮换签名密钥，重新导出，核对签名载荷中的 signer/key id。
2. vault 拷贝缺失：
- 检查 `COMPLIANCE_EVIDENCE_VAULT_DIR` 的存储权限。
3. 导出作业失败：
- 查看作业运行 `error_message`，必要时通过 `POST /compliance/evidence/export` 做临时导出。
