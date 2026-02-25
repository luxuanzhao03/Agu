from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path

from pydantic import BaseModel, Field, model_validator

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except Exception:  # noqa: BLE001
    # Fallback for environments without pydantic-settings installed.
    class SettingsConfigDict(dict):
        pass

    class BaseSettings(BaseModel):
        model_config = SettingsConfigDict()

        @classmethod
        def _load_env_file(cls, path: str | None) -> dict[str, str]:
            if not path:
                return {}
            env_path = Path(path)
            if not env_path.exists():
                return {}
            data: dict[str, str] = {}
            for line in env_path.read_text(encoding="utf-8").splitlines():
                text = line.strip()
                if not text or text.startswith("#") or "=" not in text:
                    continue
                k, v = text.split("=", 1)
                data[k.strip()] = v.strip()
            return data

        def __init__(self, **values):
            cfg = getattr(self.__class__, "model_config", {})
            env_file = cfg.get("env_file", ".env") if isinstance(cfg, dict) else ".env"
            file_env = self._load_env_file(env_file)
            merged: dict[str, str | int | float | bool | None] = {}
            for field_name in self.__class__.model_fields:
                env_name = field_name.upper()
                if env_name in file_env:
                    merged[field_name] = file_env[env_name]
                if env_name in os.environ:
                    merged[field_name] = os.environ[env_name]
            merged.update(values)
            super().__init__(**merged)


class Settings(BaseSettings):
    app_name: str = Field(default="A-share Semi-Automated Trading Assistant")
    env: str = Field(default="dev")
    log_level: str = Field(default="INFO")

    data_provider_priority: str = Field(default="tushare,akshare")
    tushare_token: str | None = Field(default=None)
    market_data_cache_enabled: bool = Field(default=True)
    market_data_cache_db_path: str = Field(default="data/market_cache.db")

    max_single_position: float = Field(default=0.35)
    max_drawdown: float = Field(default=0.18)
    max_industry_exposure: float = Field(default=0.35)
    min_turnover_20d: float = Field(default=2_500_000.0)
    default_commission_rate: float = Field(default=0.0003)
    default_slippage_rate: float = Field(default=0.0005)
    fee_min_commission_cny: float = Field(default=5.0)
    fee_stamp_duty_sell_rate: float = Field(default=0.0005)
    fee_transfer_rate: float = Field(default=0.00001)
    small_capital_mode_enabled: bool = Field(default=False)
    small_capital_principal_cny: float = Field(default=2000.0)
    small_capital_cash_buffer_ratio: float = Field(default=0.05)
    small_capital_min_expected_edge_bps: float = Field(default=45.0)
    small_capital_lot_size: int = Field(default=100)
    enable_fundamental_enrichment: bool = Field(default=True)
    fundamental_max_staleness_days: int = Field(default=540)
    fundamental_buy_warning_score: float = Field(default=0.40)
    fundamental_buy_critical_score: float = Field(default=0.22)
    fundamental_require_data_for_buy: bool = Field(default=False)
    tushare_disclosure_warning_score: float = Field(default=0.82)
    tushare_disclosure_critical_score: float = Field(default=0.95)
    tushare_forecast_warning_pct: float = Field(default=-45.0)
    tushare_forecast_critical_pct: float = Field(default=-75.0)
    small_cap_pledge_critical_ratio: float = Field(default=65.0)
    small_cap_unlock_warning_ratio: float = Field(default=0.30)
    small_cap_unlock_critical_ratio: float = Field(default=0.60)
    small_cap_overhang_warning_score: float = Field(default=0.85)
    autotune_runtime_override_enabled: bool = Field(default=True)
    challenge_max_parallel_workers: int = Field(default=0, ge=0, le=64)

    audit_db_path: str = Field(default="data/audit.db")
    snapshot_db_path: str = Field(default="data/snapshot.db")
    replay_db_path: str = Field(default="data/replay.db")
    holdings_db_path: str = Field(default="data/holdings.db")
    strategy_gov_db_path: str = Field(default="data/strategy_gov.db")
    autotune_db_path: str = Field(default="data/autotune.db")
    license_db_path: str = Field(default="data/license.db")
    job_db_path: str = Field(default="data/job.db")
    alert_db_path: str = Field(default="data/alert.db")
    event_db_path: str = Field(default="data/event.db")
    enforce_data_license: bool = Field(default=False)

    alert_email_enabled: bool = Field(default=False)
    alert_smtp_host: str | None = Field(default=None)
    alert_smtp_port: int = Field(default=465)
    alert_smtp_username: str | None = Field(default=None)
    alert_smtp_password: str | None = Field(default=None)
    alert_smtp_use_tls: bool = Field(default=False)
    alert_smtp_use_ssl: bool = Field(default=True)
    alert_email_from: str | None = Field(default=None)
    alert_im_enabled: bool = Field(default=False)
    alert_im_default_webhook: str | None = Field(default=None)
    alert_notify_timeout_seconds: int = Field(default=10)
    alert_runbook_base_url: str = Field(default="")
    oncall_callback_signing_secret: str = Field(default="")
    oncall_callback_require_signature: bool = Field(default=False)
    oncall_callback_signature_ttl_seconds: int = Field(default=600)
    oncall_callback_mapping_json: str = Field(default="")
    oncall_reconcile_default_endpoint: str = Field(default="")
    oncall_reconcile_timeout_seconds: int = Field(default=10)

    ops_scheduler_enabled: bool = Field(default=False)
    ops_scheduler_tick_seconds: int = Field(default=30)
    ops_scheduler_timezone: str = Field(default="Asia/Shanghai")
    ops_scheduler_sla_log_cooldown_seconds: int = Field(default=1800)
    ops_scheduler_sync_alerts_from_audit: bool = Field(default=True)
    ops_job_sla_grace_minutes: int = Field(default=15)
    ops_job_running_timeout_minutes: int = Field(default=120)
    compliance_evidence_signing_secret: str = Field(default="")
    compliance_evidence_vault_dir: str = Field(default="reports/compliance_vault")
    compliance_evidence_external_worm_endpoint: str = Field(default="")
    compliance_evidence_external_kms_wrap_endpoint: str = Field(default="")
    compliance_evidence_external_auth_token: str = Field(default="")
    compliance_evidence_external_timeout_seconds: int = Field(default=10)
    compliance_evidence_external_require_success: bool = Field(default=False)

    auth_enabled: bool = Field(default=False)
    auth_header_name: str = Field(default="X-API-Key")
    auth_api_keys: str = Field(
        default="",
        description="Comma-separated key:role pairs, e.g. key1:research,key2:admin",
    )
    enforce_approved_strategy: bool = Field(default=False)
    strategy_required_approval_roles: str = Field(default="risk,audit")
    strategy_min_approval_count: int = Field(default=2)

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @model_validator(mode="before")
    @classmethod
    def _strip_string_values(cls, data):
        if not isinstance(data, dict):
            return data
        normalized: dict[str, object] = {}
        for key, value in data.items():
            if isinstance(value, str):
                normalized[key] = value.strip()
            else:
                normalized[key] = value
        return normalized

    @property
    def provider_priority_list(self) -> list[str]:
        return [item.strip().lower() for item in self.data_provider_priority.split(",") if item.strip()]

    @property
    def required_approval_roles_list(self) -> list[str]:
        return [item.strip().lower() for item in self.strategy_required_approval_roles.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
