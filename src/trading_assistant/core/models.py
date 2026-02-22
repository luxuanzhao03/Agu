from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


class SignalAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    WATCH = "WATCH"


class SignalLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class SignalCandidate(BaseModel):
    symbol: str = Field(..., description="Stock symbol, e.g. 000001")
    trade_date: date
    action: SignalAction
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    reason: str
    strategy_name: str = Field(default="trend_following")
    suggested_position: float | None = Field(default=None, ge=0.0, le=1.0)
    metadata: dict[str, str | float | int | bool | None] = Field(default_factory=dict)


class Position(BaseModel):
    symbol: str
    quantity: int = Field(default=0, ge=0)
    available_quantity: int = Field(default=0, ge=0)
    avg_cost: float = Field(default=0.0, ge=0.0)
    market_value: float = Field(default=0.0, ge=0.0)
    industry: str | None = None
    last_buy_date: date | None = None


class PortfolioSnapshot(BaseModel):
    total_value: float = Field(default=0.0, ge=0.0)
    cash: float = Field(default=0.0, ge=0.0)
    peak_value: float = Field(default=0.0, ge=0.0)
    current_drawdown: float = Field(default=0.0, ge=0.0, le=1.0)
    industry_exposure: dict[str, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize_drawdown(self) -> "PortfolioSnapshot":
        if self.peak_value > 0 and self.total_value > 0:
            inferred = max(0.0, 1 - self.total_value / self.peak_value)
            self.current_drawdown = max(self.current_drawdown, inferred)
        return self


class RiskCheckRequest(BaseModel):
    signal: SignalCandidate
    position: Position | None = None
    portfolio: PortfolioSnapshot | None = None

    is_st: bool = False
    is_suspended: bool = False
    at_limit_up: bool = False
    at_limit_down: bool = False
    avg_turnover_20d: float | None = Field(default=None, ge=0.0)
    symbol_industry: str | None = None
    fundamental_score: float | None = Field(default=None, ge=0.0, le=1.0)
    fundamental_available: bool = False
    fundamental_pit_ok: bool | None = None
    fundamental_stale_days: int | None = Field(default=None, ge=0)


class RuleHit(BaseModel):
    rule_name: str
    passed: bool
    level: SignalLevel
    message: str


class RiskCheckResult(BaseModel):
    blocked: bool
    level: SignalLevel
    hits: list[RuleHit] = Field(default_factory=list)
    summary: str
    recommendations: list[str] = Field(default_factory=list)


class TradePrepSheet(BaseModel):
    signal: SignalCandidate
    risk: RiskCheckResult
    recommendations: list[str] = Field(default_factory=list)
    disclaimer: str = (
        "This system provides research and decision support only. "
        "No auto-trading, no guaranteed returns. "
        "Users make independent decisions and bear their own risks."
    )


class GenerateSignalRequest(BaseModel):
    symbol: str = Field(..., description="Stock symbol, e.g. 000001")
    start_date: date
    end_date: date
    strategy_name: str = Field(default="trend_following")
    strategy_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    enable_event_enrichment: bool = False
    enable_fundamental_enrichment: bool = True
    fundamental_max_staleness_days: int = Field(default=540, ge=1, le=3650)
    event_lookback_days: int = Field(default=30, ge=1, le=3650)
    event_decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)
    current_position: Position | None = None
    portfolio_snapshot: PortfolioSnapshot | None = None
    industry: str | None = None

    @model_validator(mode="after")
    def _validate_dates(self) -> "GenerateSignalRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class StrategyInfo(BaseModel):
    name: str
    title: str
    description: str
    frequency: str
    params_schema: dict[str, str] = Field(default_factory=dict)


class StrategyVersionStatus(str, Enum):
    DRAFT = "DRAFT"
    IN_REVIEW = "IN_REVIEW"
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    RETIRED = "RETIRED"


class StrategyDecisionType(str, Enum):
    APPROVE = "APPROVE"
    REJECT = "REJECT"


class StrategyVersionRegisterRequest(BaseModel):
    strategy_name: str
    version: str
    description: str = ""
    params_hash: str
    created_by: str = "system"


class StrategyVersionApproveRequest(BaseModel):
    strategy_name: str
    version: str
    approved_by: str
    note: str = ""


class StrategySubmitReviewRequest(BaseModel):
    strategy_name: str
    version: str
    submitted_by: str
    note: str = ""


class StrategyDecisionRequest(BaseModel):
    strategy_name: str
    version: str
    reviewer: str
    reviewer_role: str
    decision: StrategyDecisionType
    note: str = ""


class StrategyDecisionRecord(BaseModel):
    id: int
    strategy_name: str
    version: str
    reviewer: str
    reviewer_role: str
    decision: StrategyDecisionType
    note: str
    created_at: datetime


class StrategyApprovalPolicy(BaseModel):
    required_roles: list[str] = Field(default_factory=list)
    min_approval_count: int = 2
    enforce_runtime_approved_only: bool = False


class StrategyVersionRecord(BaseModel):
    id: int
    strategy_name: str
    version: str
    status: StrategyVersionStatus
    description: str
    params_hash: str
    created_at: datetime
    created_by: str
    approved_at: datetime | None = None
    approved_by: str | None = None
    note: str = ""


class BacktestRequest(BaseModel):
    symbol: str
    start_date: date
    end_date: date
    strategy_name: str = Field(default="trend_following")
    strategy_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    enable_event_enrichment: bool = False
    enable_fundamental_enrichment: bool = True
    fundamental_max_staleness_days: int = Field(default=540, ge=1, le=3650)
    event_lookback_days: int = Field(default=30, ge=1, le=3650)
    event_decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)
    initial_cash: float = Field(default=1_000_000.0, gt=0)
    commission_rate: float = Field(default=0.0003, ge=0.0, le=0.02)
    slippage_rate: float = Field(default=0.0005, ge=0.0, le=0.02)
    lot_size: int = Field(default=100, ge=1)
    max_single_position: float = Field(default=0.05, gt=0.0, le=1.0)

    @model_validator(mode="after")
    def _validate_dates(self) -> "BacktestRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class BacktestTrade(BaseModel):
    date: date
    action: SignalAction
    price: float
    quantity: int
    cost: float
    reason: str
    blocked: bool = False


class EquityPoint(BaseModel):
    date: date
    cash: float
    position_value: float
    equity: float
    drawdown: float


class BacktestMetrics(BaseModel):
    total_return: float
    max_drawdown: float
    trade_count: int
    win_rate: float
    blocked_signal_count: int
    annualized_return: float = 0.0
    sharpe: float = 0.0


class BacktestResult(BaseModel):
    symbol: str
    strategy_name: str
    start_date: date
    end_date: date
    metrics: BacktestMetrics
    trades: list[BacktestTrade]
    equity_curve: list[EquityPoint]


class PortfolioRiskRequest(BaseModel):
    portfolio: PortfolioSnapshot
    pending_signal: SignalCandidate | None = None
    max_drawdown: float = Field(default=0.12, ge=0.0, le=1.0)
    max_industry_exposure: float = Field(default=0.2, ge=0.0, le=1.0)


class PortfolioRiskResult(BaseModel):
    blocked: bool
    level: SignalLevel
    summary: str
    hits: list[RuleHit] = Field(default_factory=list)


class AuditEventCreate(BaseModel):
    event_type: str
    action: str
    payload: dict[str, str | int | float | bool | None]
    status: str = "OK"


class AuditEventRecord(BaseModel):
    id: int
    event_time: datetime
    event_type: str
    action: str
    status: str
    payload: dict[str, str | int | float | bool | None]
    prev_hash: str | None = None
    event_hash: str | None = None


class PipelineRunRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    start_date: date
    end_date: date
    strategy_name: str = Field(default="trend_following")
    strategy_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    enable_event_enrichment: bool = False
    enable_fundamental_enrichment: bool = True
    fundamental_max_staleness_days: int = Field(default=540, ge=1, le=3650)
    event_lookback_days: int = Field(default=30, ge=1, le=3650)
    event_decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)
    industry_map: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_dates(self) -> "PipelineRunRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        return self


class PipelineSymbolResult(BaseModel):
    symbol: str
    provider: str
    signal_count: int
    blocked_count: int
    warning_count: int
    quality_passed: bool = True
    snapshot_id: int | None = None
    event_rows_used: int = 0
    fundamental_available: bool = False
    fundamental_score: float | None = None
    fundamental_source: str | None = None


class PipelineRunResult(BaseModel):
    run_id: str
    started_at: datetime
    finished_at: datetime
    strategy_name: str
    results: list[PipelineSymbolResult] = Field(default_factory=list)
    total_symbols: int = 0
    total_signals: int = 0
    total_blocked: int = 0
    total_warnings: int = 0


class DataSnapshotRegisterRequest(BaseModel):
    dataset_name: str
    symbol: str
    start_date: date
    end_date: date
    provider: str
    row_count: int = Field(ge=0)
    schema_version: str = Field(default="v1")
    content_hash: str


class DataSnapshotRecord(BaseModel):
    id: int
    created_at: datetime
    dataset_name: str
    symbol: str
    start_date: date
    end_date: date
    provider: str
    row_count: int
    schema_version: str
    content_hash: str


class DataQualityRequest(BaseModel):
    symbol: str
    start_date: date
    end_date: date
    required_fields: list[str] = Field(
        default_factory=lambda: ["trade_date", "open", "high", "low", "close", "volume", "amount"]
    )


class DataQualityIssue(BaseModel):
    issue_type: str
    severity: SignalLevel
    message: str


class DataQualityReport(BaseModel):
    symbol: str
    provider: str
    row_count: int
    issues: list[DataQualityIssue] = Field(default_factory=list)
    passed: bool


class PITValidationIssue(BaseModel):
    issue_type: str
    severity: SignalLevel
    message: str


class PITValidationResult(BaseModel):
    symbol: str
    provider: str
    passed: bool
    issues: list[PITValidationIssue] = Field(default_factory=list)


class OptimizeCandidate(BaseModel):
    symbol: str
    expected_return: float
    volatility: float = Field(gt=0)
    industry: str = "UNKNOWN"
    liquidity_score: float = Field(default=1.0, ge=0.0, le=1.0)


class PortfolioOptimizeRequest(BaseModel):
    candidates: list[OptimizeCandidate] = Field(default_factory=list)
    max_single_position: float = Field(default=0.05, gt=0, le=1)
    max_industry_exposure: float = Field(default=0.2, gt=0, le=1)
    min_weight_threshold: float = Field(default=0.005, ge=0, le=1)
    risk_aversion: float = Field(default=0.5, ge=0, le=5)
    target_gross_exposure: float = Field(default=1.0, gt=0, le=1)

    @model_validator(mode="after")
    def _validate_candidates(self) -> "PortfolioOptimizeRequest":
        if not self.candidates:
            raise ValueError("candidates must not be empty")
        return self


class OptimizedWeight(BaseModel):
    symbol: str
    weight: float
    industry: str
    score: float


class PortfolioOptimizeResult(BaseModel):
    weights: list[OptimizedWeight] = Field(default_factory=list)
    industry_exposure: dict[str, float] = Field(default_factory=dict)
    unallocated_weight: float = 0.0


class RebalancePosition(BaseModel):
    symbol: str
    quantity: int = Field(ge=0)
    last_price: float = Field(gt=0)


class RebalanceRequest(BaseModel):
    current_positions: list[RebalancePosition] = Field(default_factory=list)
    target_weights: list[OptimizedWeight] = Field(default_factory=list)
    total_equity: float = Field(gt=0)
    lot_size: int = Field(default=100, ge=1)


class RebalanceOrder(BaseModel):
    symbol: str
    side: SignalAction
    target_weight: float
    delta_weight: float
    quantity: int
    estimated_notional: float


class RebalancePlan(BaseModel):
    orders: list[RebalanceOrder] = Field(default_factory=list)
    estimated_turnover: float = 0.0


class StressScenario(BaseModel):
    name: str
    shocks: dict[str, float] = Field(default_factory=dict)
    default_shock: float = 0.0


class StressTestRequest(BaseModel):
    weights: list[OptimizedWeight] = Field(default_factory=list)
    scenarios: list[StressScenario] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_payload(self) -> "StressTestRequest":
        if not self.weights:
            raise ValueError("weights must not be empty")
        if not self.scenarios:
            raise ValueError("scenarios must not be empty")
        return self


class StressScenarioResult(BaseModel):
    scenario: str
    portfolio_return: float
    industry_breakdown: dict[str, float] = Field(default_factory=dict)


class StressTestResult(BaseModel):
    results: list[StressScenarioResult] = Field(default_factory=list)


class SignalDecisionRecord(BaseModel):
    signal_id: str
    symbol: str
    strategy_name: str
    trade_date: date
    action: SignalAction
    confidence: float
    reason: str
    suggested_position: float | None = None


class ExecutionRecordCreate(BaseModel):
    signal_id: str
    symbol: str
    execution_date: date
    side: SignalAction
    quantity: int = Field(ge=0)
    price: float = Field(ge=0)
    fee: float = Field(default=0.0, ge=0.0)
    note: str = ""


class ExecutionReplayItem(BaseModel):
    signal_id: str
    symbol: str
    signal_action: SignalAction
    executed_action: SignalAction | None
    signal_confidence: float
    executed_quantity: int
    executed_price: float
    slippage_bps: float
    followed: bool
    delay_days: int


class ExecutionReplayReport(BaseModel):
    items: list[ExecutionReplayItem] = Field(default_factory=list)
    follow_rate: float = 0.0
    avg_slippage_bps: float = 0.0
    avg_delay_days: float = 0.0


class WorkflowSignalItem(BaseModel):
    symbol: str
    provider: str
    action: SignalAction
    confidence: float
    blocked: bool
    level: SignalLevel
    reason: str
    suggested_position: float | None = None
    signal_id: str | None = None
    event_rows_used: int = 0
    fundamental_available: bool = False
    fundamental_score: float | None = None
    fundamental_source: str | None = None


class ResearchWorkflowRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
    start_date: date
    end_date: date
    strategy_name: str = Field(default="multi_factor")
    strategy_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    enable_event_enrichment: bool = False
    enable_fundamental_enrichment: bool = True
    fundamental_max_staleness_days: int = Field(default=540, ge=1, le=3650)
    event_lookback_days: int = Field(default=30, ge=1, le=3650)
    event_decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)
    industry_map: dict[str, str] = Field(default_factory=dict)
    optimize_portfolio: bool = True
    max_single_position: float = Field(default=0.05, gt=0.0, le=1.0)
    max_industry_exposure: float = Field(default=0.2, gt=0.0, le=1.0)
    target_gross_exposure: float = Field(default=1.0, gt=0.0, le=1.0)

    @model_validator(mode="after")
    def _validate_dates(self) -> "ResearchWorkflowRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        if not self.symbols:
            raise ValueError("symbols must not be empty")
        return self


class ResearchWorkflowResult(BaseModel):
    run_id: str
    generated_at: datetime
    strategy_name: str
    signals: list[WorkflowSignalItem] = Field(default_factory=list)
    optimized_portfolio: PortfolioOptimizeResult | None = None


class AlertItem(BaseModel):
    event_id: int
    event_time: datetime
    severity: SignalLevel
    source: str
    message: str
    payload: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class ServiceMetricsSummary(BaseModel):
    total_events: int
    event_type_counts: dict[str, int] = Field(default_factory=dict)
    error_events: int = 0
    warning_events: int = 0


class ModelDriftRequest(BaseModel):
    strategy_name: str
    symbol: str | None = None
    lookback_events: int = Field(default=300, ge=10, le=5000)
    return_drift_threshold: float = Field(default=0.08, ge=0.0, le=2.0)
    follow_rate_threshold: float = Field(default=0.5, ge=0.0, le=1.0)


class ModelDriftResult(BaseModel):
    strategy_name: str
    symbol: str | None
    baseline_return: float | None = None
    recent_return: float | None = None
    return_drift: float | None = None
    follow_rate: float | None = None
    status: SignalLevel
    warnings: list[str] = Field(default_factory=list)


class AuditExportResult(BaseModel):
    format: str
    row_count: int
    content: str


class AuditChainVerifyResult(BaseModel):
    valid: bool
    checked_rows: int
    broken_event_id: int | None = None
    message: str = ""


class ReportGenerateRequest(BaseModel):
    report_type: str = Field(pattern="^(signal|replay|risk)$")
    symbol: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    limit: int = Field(default=200, ge=1, le=2000)
    save_to_file: bool = True
    watermark: str = "For Research Only"


class ReportGenerateResult(BaseModel):
    title: str
    content: str
    saved_path: str | None = None


class CompliancePreflightRequest(BaseModel):
    symbol: str
    start_date: date
    end_date: date
    strategy_name: str

    @model_validator(mode="after")
    def _validate_dates(self) -> "CompliancePreflightRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class ComplianceCheckItem(BaseModel):
    check_name: str
    passed: bool
    message: str


class CompliancePreflightResult(BaseModel):
    passed: bool
    checks: list[ComplianceCheckItem] = Field(default_factory=list)


class ComplianceEvidenceExportRequest(BaseModel):
    triggered_by: str = "compliance_audit"
    strategy_name: str | None = None
    connector_name: str | None = None
    source_name: str | None = None
    audit_event_type: str | None = None
    audit_event_limit: int = Field(default=3000, ge=1, le=20000)
    audit_verify_limit: int = Field(default=10000, ge=1, le=100000)
    strategy_version_limit: int = Field(default=200, ge=1, le=2000)
    connector_run_limit: int = Field(default=500, ge=1, le=5000)
    connector_failure_limit: int = Field(default=500, ge=1, le=5000)
    connector_state_limit: int = Field(default=1000, ge=1, le=10000)
    event_lookback_days: int = Field(default=30, ge=1, le=3650)
    nlp_monitor_limit: int = Field(default=60, ge=3, le=365)
    nlp_snapshot_limit: int = Field(default=200, ge=1, le=2000)
    include_ruleset_body: bool = True
    include_feedback_summary: bool = True
    sign_bundle: bool = True
    signer: str = "compliance_system"
    signing_key_id: str = "default"
    retention_policy: str = "regulatory_7y"
    vault_mode: str = Field(default="COPY", pattern="^(COPY|SIMULATED_WORM)$")
    kms_key_id: str | None = None
    external_worm_endpoint: str | None = None
    external_kms_wrap_endpoint: str | None = None
    external_auth_token: str | None = None
    external_timeout_seconds: int = Field(default=10, ge=1, le=120)
    external_require_success: bool = False
    write_vault_copy: bool = False
    vault_dir: str | None = None
    cleanup_bundle_dir: bool = False
    output_dir: str = "reports/compliance"
    package_prefix: str = "evidence"


class ComplianceEvidenceFileItem(BaseModel):
    name: str
    relative_path: str
    size_bytes: int
    sha256: str


class ComplianceEvidenceBundleSignature(BaseModel):
    enabled: bool = False
    algorithm: str = "sha256-hmac"
    signer: str = ""
    signing_key_id: str = ""
    digest: str = ""
    signature: str = ""
    signed_at: datetime | None = None


class ComplianceEvidenceExportResult(BaseModel):
    bundle_id: str
    generated_at: datetime
    bundle_dir: str
    package_path: str
    package_size_bytes: int
    package_sha256: str
    signature: ComplianceEvidenceBundleSignature | None = None
    signature_path: str | None = None
    vault_copy_path: str | None = None
    file_count: int
    files: list[ComplianceEvidenceFileItem] = Field(default_factory=list)
    summary: dict[str, Any] = Field(default_factory=dict)


class ComplianceEvidenceVerifyRequest(BaseModel):
    package_path: str
    signature_path: str | None = None
    countersign_path: str | None = None
    require_countersign: bool = False
    signing_secret: str | None = None


class ComplianceEvidenceVerifyResult(BaseModel):
    package_path: str
    package_sha256: str = ""
    package_exists: bool = False
    manifest_exists: bool = False
    manifest_valid: bool = False
    signature_checked: bool = False
    signature_valid: bool = False
    countersign_checked: bool = False
    countersign_valid: bool = False
    countersign_count: int = 0
    message: str = ""


class ComplianceEvidenceCounterSignRequest(BaseModel):
    package_path: str
    countersign_path: str | None = None
    signer: str
    signing_key_id: str = "counter_sign_default"
    signing_secret: str | None = None
    note: str = ""


class ComplianceEvidenceCounterSignEntry(BaseModel):
    signer: str
    signing_key_id: str
    signed_at: datetime
    digest: str
    signature: str
    note: str = ""


class ComplianceEvidenceCounterSignResult(BaseModel):
    package_path: str
    package_sha256: str
    countersign_path: str
    entry_count: int
    last_entry: ComplianceEvidenceCounterSignEntry


class EventSourceType(str, Enum):
    MANUAL = "MANUAL"
    ANNOUNCEMENT = "ANNOUNCEMENT"
    NEWS = "NEWS"
    MODEL = "MODEL"


class EventPolarity(str, Enum):
    POSITIVE = "POSITIVE"
    NEGATIVE = "NEGATIVE"
    NEUTRAL = "NEUTRAL"


class EventSourceRegisterRequest(BaseModel):
    source_name: str
    source_type: EventSourceType = EventSourceType.MANUAL
    provider: str = "internal"
    timezone: str = "Asia/Shanghai"
    ingestion_lag_minutes: int = Field(default=0, ge=0, le=7 * 24 * 60)
    reliability_score: float = Field(default=0.7, ge=0.0, le=1.0)
    description: str = ""
    created_by: str = "system"
    note: str = ""


class EventSourceRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    source_name: str
    source_type: EventSourceType
    provider: str
    timezone: str
    ingestion_lag_minutes: int
    reliability_score: float
    description: str
    created_by: str
    note: str


class EventRecordCreate(BaseModel):
    event_id: str
    symbol: str
    event_type: str
    publish_time: datetime
    effective_time: datetime | None = None
    polarity: EventPolarity = EventPolarity.NEUTRAL
    score: float = Field(default=0.5, ge=0.0, le=1.0)
    confidence: float = Field(default=0.5, ge=0.0, le=1.0)
    title: str = ""
    summary: str = ""
    raw_ref: str | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_times(self) -> "EventRecordCreate":
        if self.effective_time is not None and self.effective_time < self.publish_time:
            raise ValueError("effective_time must be >= publish_time")
        return self


class EventRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    source_name: str
    event_id: str
    symbol: str
    event_type: str
    publish_time: datetime
    effective_time: datetime | None = None
    polarity: EventPolarity
    score: float
    confidence: float
    title: str
    summary: str
    raw_ref: str | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)


class EventBatchIngestRequest(BaseModel):
    source_name: str
    events: list[EventRecordCreate] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_events(self) -> "EventBatchIngestRequest":
        if not self.events:
            raise ValueError("events must not be empty")
        return self


class EventBatchIngestResult(BaseModel):
    source_name: str
    inserted: int
    updated: int
    total: int
    errors: list[str] = Field(default_factory=list)


class EventPITRow(BaseModel):
    event_id: str
    event_time: datetime
    effective_time: datetime | None = None
    used_in_trade_time: datetime | None = None


class EventPITValidationRequest(BaseModel):
    symbol: str
    rows: list[EventPITRow] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_rows(self) -> "EventPITValidationRequest":
        if not self.rows:
            raise ValueError("rows must not be empty")
        return self


class EventJoinPITRow(BaseModel):
    event_id: str
    symbol: str
    used_in_trade_time: datetime
    source_name: str | None = None


class EventJoinPITValidationRequest(BaseModel):
    rows: list[EventJoinPITRow] = Field(default_factory=list)
    strict_symbol_match: bool = True

    @model_validator(mode="after")
    def _validate_rows(self) -> "EventJoinPITValidationRequest":
        if not self.rows:
            raise ValueError("rows must not be empty")
        return self


class EventJoinPITIssue(BaseModel):
    row_index: int
    event_id: str
    issue_type: str
    severity: SignalLevel
    message: str


class EventJoinPITValidationResult(BaseModel):
    passed: bool
    checked_rows: int
    issues: list[EventJoinPITIssue] = Field(default_factory=list)


class EventFeaturePoint(BaseModel):
    trade_date: date
    event_score: float
    negative_event_score: float
    event_count: int = 0
    positive_event_count: int = 0
    negative_event_count: int = 0


class EventFeaturePreviewRequest(BaseModel):
    symbol: str
    start_date: date
    end_date: date
    lookback_days: int = Field(default=30, ge=1, le=3650)
    decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)

    @model_validator(mode="after")
    def _validate_dates(self) -> "EventFeaturePreviewRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class EventFeaturePreviewResult(BaseModel):
    symbol: str
    points: list[EventFeaturePoint] = Field(default_factory=list)


class AnnouncementRawRecord(BaseModel):
    source_event_id: str | None = None
    symbol: str | None = None
    ts_code: str | None = None
    title: str = ""
    summary: str = ""
    content: str = ""
    publish_time: datetime | None = None
    publish_time_text: str | None = None
    url: str | None = None
    metadata: dict[str, str | int | float | bool | None] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_minimum_text(self) -> "AnnouncementRawRecord":
        if not (self.title.strip() or self.summary.strip() or self.content.strip()):
            raise ValueError("at least one of title/summary/content must be provided")
        return self


class EventNLPTagScore(BaseModel):
    tag: str
    weight: float = Field(ge=0.0, le=1.0)
    matched_terms: list[str] = Field(default_factory=list)


class EventNLPScoreResult(BaseModel):
    event_type: str
    polarity: EventPolarity
    score: float = Field(ge=0.0, le=1.0)
    confidence: float = Field(ge=0.0, le=1.0)
    ruleset_version: str = "builtin-v1"
    tags: list[str] = Field(default_factory=list)
    matched_rules: list[str] = Field(default_factory=list)
    tag_scores: list[EventNLPTagScore] = Field(default_factory=list)
    rationale: str = ""


class EventNormalizePreviewRequest(BaseModel):
    source_name: str
    records: list[AnnouncementRawRecord] = Field(default_factory=list)
    default_symbol: str | None = None
    default_timezone: str = "Asia/Shanghai"
    source_reliability_score: float = Field(default=0.7, ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _validate_rows(self) -> "EventNormalizePreviewRequest":
        if not self.records:
            raise ValueError("records must not be empty")
        return self


class EventNormalizedRecord(BaseModel):
    row_index: int
    event: EventRecordCreate
    nlp: EventNLPScoreResult
    warning: str | None = None


class EventNormalizePreviewResult(BaseModel):
    source_name: str
    normalized: list[EventNormalizedRecord] = Field(default_factory=list)
    dropped: int = 0
    errors: list[str] = Field(default_factory=list)


class EventNormalizeIngestRequest(EventNormalizePreviewRequest):
    allow_partial: bool = True


class EventNormalizeIngestResult(BaseModel):
    source_name: str
    preview: EventNormalizePreviewResult
    ingest: EventBatchIngestResult | None = None


class EventNLPRule(BaseModel):
    rule_id: str
    event_type: str
    polarity: EventPolarity
    weight: float = Field(default=0.7, ge=0.0, le=1.0)
    tag: str
    patterns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_patterns(self) -> "EventNLPRule":
        if not self.patterns:
            raise ValueError("patterns must not be empty")
        return self


class EventNLPRulesetUpsertRequest(BaseModel):
    version: str
    rules: list[EventNLPRule] = Field(default_factory=list)
    created_by: str = "system"
    note: str = ""
    activate: bool = False

    @model_validator(mode="after")
    def _validate_rules(self) -> "EventNLPRulesetUpsertRequest":
        if not self.rules:
            raise ValueError("rules must not be empty")
        return self


class EventNLPRulesetActivateRequest(BaseModel):
    version: str
    activated_by: str = "system"
    note: str = ""


class EventNLPRulesetRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    version: str
    created_by: str
    note: str = ""
    is_active: bool = False
    rule_count: int = 0
    rules: list[EventNLPRule] = Field(default_factory=list)


class EventNLPWindowMetrics(BaseModel):
    source_name: str | None = None
    ruleset_version: str = "unknown"
    sample_size: int = 0
    hit_count: int = 0
    hit_rate: float = 0.0
    score_mean: float = 0.0
    score_p10: float = 0.0
    score_p50: float = 0.0
    score_p90: float = 0.0
    positive_ratio: float = 0.0
    negative_ratio: float = 0.0
    neutral_ratio: float = 0.0
    top_event_types: dict[str, int] = Field(default_factory=dict)


class EventNLPFeedbackUpsertRequest(BaseModel):
    source_name: str
    event_id: str
    label_event_type: str
    label_polarity: EventPolarity
    label_score: float | None = Field(default=None, ge=0.0, le=1.0)
    labeler: str = "system"
    note: str = ""


class EventNLPFeedbackRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    source_name: str
    event_id: str
    symbol: str
    publish_time: datetime
    predicted_event_type: str
    predicted_polarity: EventPolarity
    predicted_score: float
    label_event_type: str
    label_polarity: EventPolarity
    label_score: float | None = None
    labeler: str
    note: str = ""


class EventNLPLabelEntryUpsertRequest(BaseModel):
    source_name: str
    event_id: str
    label_event_type: str
    label_polarity: EventPolarity
    label_score: float | None = Field(default=None, ge=0.0, le=1.0)
    labeler: str = "system"
    label_version: str = "v1"
    note: str = ""


class EventNLPLabelEntryRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    source_name: str
    event_id: str
    symbol: str
    publish_time: datetime
    predicted_event_type: str
    predicted_polarity: EventPolarity
    predicted_score: float
    label_event_type: str
    label_polarity: EventPolarity
    label_score: float | None = None
    labeler: str
    label_version: str = "v1"
    note: str = ""


class EventNLPConsensusRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    source_name: str
    event_id: str
    symbol: str
    publish_time: datetime
    consensus_event_type: str
    consensus_polarity: EventPolarity
    consensus_score: float | None = None
    consensus_confidence: float = 0.0
    label_count: int = 0
    conflict: bool = False
    conflict_reasons: list[str] = Field(default_factory=list)
    adjudicated_by: str = "system"
    label_version: str = "v1"


class EventNLPFeedbackSummary(BaseModel):
    source_name: str | None = None
    start_date: date
    end_date: date
    sample_size: int = 0
    polarity_accuracy: float = 0.0
    event_type_accuracy: float = 0.0
    score_mae: float | None = None
    top_mismatches: dict[str, int] = Field(default_factory=dict)


class EventNLPContributionWindow(BaseModel):
    symbol: str
    strategy_name: str
    start_date: date
    end_date: date
    total_return_delta: float
    sharpe_delta: float
    event_row_ratio: float
    events_loaded: int


class EventNLPDriftThresholds(BaseModel):
    hit_rate_drop_warning: float = Field(default=0.08, ge=0.0, le=1.0)
    hit_rate_drop_critical: float = Field(default=0.15, ge=0.0, le=1.0)
    score_p50_shift_warning: float = Field(default=0.08, ge=0.0, le=1.0)
    score_p50_shift_critical: float = Field(default=0.18, ge=0.0, le=1.0)
    contribution_drop_warning: float = Field(default=0.03, ge=0.0, le=1.0)
    contribution_drop_critical: float = Field(default=0.08, ge=0.0, le=1.0)
    feedback_polarity_accuracy_drop_warning: float = Field(default=0.08, ge=0.0, le=1.0)
    feedback_polarity_accuracy_drop_critical: float = Field(default=0.15, ge=0.0, le=1.0)
    feedback_event_type_accuracy_drop_warning: float = Field(default=0.1, ge=0.0, le=1.0)
    feedback_event_type_accuracy_drop_critical: float = Field(default=0.2, ge=0.0, le=1.0)


class EventNLPAdjudicationRequest(BaseModel):
    source_name: str | None = None
    event_ids: list[str] = Field(default_factory=list)
    start_date: date | None = None
    end_date: date | None = None
    min_labelers: int = Field(default=2, ge=1, le=20)
    require_unanimous: bool = False
    save_consensus: bool = True
    adjudicated_by: str = "nlp_qc"
    label_version: str = "v1"

    @model_validator(mode="after")
    def _validate_scope(self) -> "EventNLPAdjudicationRequest":
        has_ids = bool(self.event_ids)
        has_window = self.start_date is not None and self.end_date is not None
        if not has_ids and not has_window:
            raise ValueError("either event_ids or start_date/end_date must be provided")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class EventNLPAdjudicationItem(BaseModel):
    source_name: str
    event_id: str
    symbol: str
    publish_time: datetime
    label_count: int
    labelers: list[str] = Field(default_factory=list)
    consensus_event_type: str | None = None
    consensus_polarity: EventPolarity | None = None
    consensus_score: float | None = None
    consensus_confidence: float = 0.0
    conflict: bool = False
    conflict_reasons: list[str] = Field(default_factory=list)


class EventNLPAdjudicationResult(BaseModel):
    generated_at: datetime
    source_name: str | None = None
    total_events: int
    adjudicated: int
    conflicts: int
    skipped: int
    items: list[EventNLPAdjudicationItem] = Field(default_factory=list)


class EventNLPLabelerPairAgreement(BaseModel):
    labeler_a: str
    labeler_b: str
    common_events: int
    event_type_agreement: float
    polarity_agreement: float
    avg_score_abs_delta: float | None = None


class EventNLPLabelConsistencySummary(BaseModel):
    source_name: str | None = None
    start_date: date
    end_date: date
    events_with_labels: int = 0
    total_label_rows: int = 0
    avg_labelers_per_event: float = 0.0
    majority_conflict_rate: float = 0.0
    avg_score_std: float | None = None
    pair_agreements: list[EventNLPLabelerPairAgreement] = Field(default_factory=list)


class EventNLPLabelSnapshotRequest(BaseModel):
    source_name: str | None = None
    start_date: date
    end_date: date
    min_labelers: int = Field(default=1, ge=1, le=20)
    include_conflicts: bool = True
    created_by: str = "nlp_qc"
    note: str = ""

    @model_validator(mode="after")
    def _validate_dates(self) -> "EventNLPLabelSnapshotRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class EventNLPLabelSnapshotRecord(BaseModel):
    id: int
    created_at: datetime
    source_name: str | None = None
    start_date: date
    end_date: date
    min_labelers: int
    include_conflicts: bool
    sample_size: int
    consensus_size: int
    conflict_size: int
    hash_sha256: str
    stats: dict[str, Any] = Field(default_factory=dict)
    created_by: str
    note: str = ""


class EventNLPDriftAlert(BaseModel):
    severity: SignalLevel
    metric: str
    message: str
    current: float | None = None
    baseline: float | None = None
    delta: float | None = None


class EventNLPDriftCheckRequest(BaseModel):
    source_name: str | None = None
    current_start: date
    current_end: date
    baseline_start: date | None = None
    baseline_end: date | None = None
    thresholds: EventNLPDriftThresholds = Field(default_factory=EventNLPDriftThresholds)
    include_contribution: bool = True
    contribution_symbol: str = "000001"
    contribution_strategy_name: str = "event_driven"
    contribution_strategy_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    contribution_event_lookback_days: int = Field(default=30, ge=1, le=3650)
    contribution_event_decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)
    contribution_initial_cash: float = Field(default=1_000_000.0, gt=0.0)
    contribution_commission_rate: float = Field(default=0.0003, ge=0.0, le=0.02)
    contribution_slippage_rate: float = Field(default=0.0005, ge=0.0, le=0.02)
    contribution_lot_size: int = Field(default=100, ge=1)
    contribution_max_single_position: float = Field(default=0.05, gt=0.0, le=1.0)
    include_feedback_quality: bool = True
    feedback_min_samples: int = Field(default=20, ge=1, le=100000)
    save_snapshot: bool = True

    @model_validator(mode="after")
    def _validate_dates(self) -> "EventNLPDriftCheckRequest":
        if self.current_start > self.current_end:
            raise ValueError("current_start must be <= current_end")
        if self.baseline_start and self.baseline_end and self.baseline_start > self.baseline_end:
            raise ValueError("baseline_start must be <= baseline_end")
        return self


class EventNLPDriftSnapshotRecord(BaseModel):
    id: int
    created_at: datetime
    source_name: str | None = None
    ruleset_version: str
    current_start: date
    current_end: date
    baseline_start: date
    baseline_end: date
    sample_size: int
    hit_rate: float
    baseline_hit_rate: float
    hit_rate_delta: float
    score_p50: float
    baseline_score_p50: float
    score_p50_delta: float
    contribution_delta: float | None = None
    feedback_polarity_accuracy_delta: float | None = None
    feedback_event_type_accuracy_delta: float | None = None
    alerts: list[EventNLPDriftAlert] = Field(default_factory=list)


class EventNLPDriftCheckResult(BaseModel):
    generated_at: datetime
    source_name: str | None = None
    ruleset_version: str
    current: EventNLPWindowMetrics
    baseline: EventNLPWindowMetrics
    hit_rate_delta: float
    score_p50_delta: float
    contribution_current: EventNLPContributionWindow | None = None
    contribution_baseline: EventNLPContributionWindow | None = None
    contribution_delta: float | None = None
    feedback_current: EventNLPFeedbackSummary | None = None
    feedback_baseline: EventNLPFeedbackSummary | None = None
    feedback_polarity_accuracy_delta: float | None = None
    feedback_event_type_accuracy_delta: float | None = None
    alerts: list[EventNLPDriftAlert] = Field(default_factory=list)
    snapshot_id: int | None = None


class EventNLPDriftMonitorPoint(BaseModel):
    snapshot_id: int
    created_at: datetime
    ruleset_version: str
    hit_rate_delta: float
    score_p50_delta: float
    contribution_delta: float | None = None
    feedback_polarity_accuracy_delta: float | None = None
    feedback_event_type_accuracy_delta: float | None = None
    warning_alerts: int = 0
    critical_alerts: int = 0


class EventNLPDriftMonitorSummary(BaseModel):
    generated_at: datetime
    source_name: str | None = None
    window_size: int
    latest_snapshot_id: int | None = None
    latest_ruleset_version: str | None = None
    latest_risk_level: SignalLevel = SignalLevel.INFO
    warning_alert_snapshots: int = 0
    critical_alert_snapshots: int = 0
    hit_rate_delta_trend: float | None = None
    score_p50_delta_trend: float | None = None
    contribution_delta_trend: float | None = None
    feedback_polarity_accuracy_delta_trend: float | None = None
    feedback_event_type_accuracy_delta_trend: float | None = None
    points: list[EventNLPDriftMonitorPoint] = Field(default_factory=list)


class EventNLPSLOPoint(BaseModel):
    window_start: datetime
    window_end: datetime
    snapshots: int = 0
    warning_snapshots: int = 0
    critical_snapshots: int = 0
    avg_hit_rate_delta: float | None = None
    avg_score_p50_delta: float | None = None
    burn_rate_warning: float = 0.0
    burn_rate_critical: float = 0.0


class EventNLPSLOHistory(BaseModel):
    generated_at: datetime
    source_name: str | None = None
    lookback_days: int
    bucket_hours: int
    total_points: int
    points: list[EventNLPSLOPoint] = Field(default_factory=list)


class EventConnectorType(str, Enum):
    TUSHARE_ANNOUNCEMENT = "TUSHARE_ANNOUNCEMENT"
    FILE_ANNOUNCEMENT = "FILE_ANNOUNCEMENT"
    HTTP_JSON_ANNOUNCEMENT = "HTTP_JSON_ANNOUNCEMENT"
    AKSHARE_ANNOUNCEMENT = "AKSHARE_ANNOUNCEMENT"


class EventConnectorRunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    DRY_RUN = "DRY_RUN"


class EventConnectorFailureStatus(str, Enum):
    PENDING = "PENDING"
    REPLAYED = "REPLAYED"
    DEAD = "DEAD"


class EventConnectorSLABreachType(str, Enum):
    FRESHNESS = "FRESHNESS"
    PENDING_BACKLOG = "PENDING_BACKLOG"
    DEAD_BACKLOG = "DEAD_BACKLOG"


class EventConnectorSLAPolicy(BaseModel):
    freshness_warning_minutes: int = Field(default=180, ge=1, le=60 * 24 * 30)
    freshness_critical_minutes: int = Field(default=720, ge=1, le=60 * 24 * 90)
    freshness_escalation_minutes: int = Field(default=1440, ge=1, le=60 * 24 * 180)
    pending_warning: int = Field(default=10, ge=0, le=100000)
    pending_critical: int = Field(default=30, ge=0, le=100000)
    pending_escalation: int = Field(default=80, ge=0, le=100000)
    dead_warning: int = Field(default=1, ge=0, le=100000)
    dead_critical: int = Field(default=5, ge=0, le=100000)
    dead_escalation: int = Field(default=20, ge=0, le=100000)

    @model_validator(mode="after")
    def _validate_order(self) -> "EventConnectorSLAPolicy":
        if not (
            self.freshness_warning_minutes <= self.freshness_critical_minutes <= self.freshness_escalation_minutes
        ):
            raise ValueError("freshness thresholds must satisfy warning <= critical <= escalation")
        if not (self.pending_warning <= self.pending_critical <= self.pending_escalation):
            raise ValueError("pending thresholds must satisfy warning <= critical <= escalation")
        if not (self.dead_warning <= self.dead_critical <= self.dead_escalation):
            raise ValueError("dead thresholds must satisfy warning <= critical <= escalation")
        return self


class EventConnectorRegisterRequest(BaseModel):
    connector_name: str
    source_name: str
    connector_type: EventConnectorType = EventConnectorType.TUSHARE_ANNOUNCEMENT
    enabled: bool = True
    fetch_limit: int = Field(default=500, ge=1, le=5000)
    poll_interval_minutes: int = Field(default=10, ge=1, le=7 * 24 * 60)
    replay_backoff_seconds: int = Field(default=300, ge=1, le=86400)
    max_retry: int = Field(default=5, ge=1, le=100)
    checkpoint_cursor: str | None = None
    checkpoint_publish_time: datetime | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    created_by: str = "system"
    note: str = ""


class EventConnectorMatrixSourceItem(BaseModel):
    source_key: str
    connector_type: EventConnectorType
    priority: int = Field(default=100, ge=0, le=10000)
    enabled: bool = True
    request_budget_per_hour: int | None = Field(default=None, ge=1, le=1000000)
    credential_aliases: list[str] = Field(default_factory=list)
    config: dict[str, Any] = Field(default_factory=dict)


class EventConnectorSourceStateRecord(BaseModel):
    connector_name: str
    source_key: str
    connector_type: EventConnectorType
    priority: int
    enabled: bool
    health_score: float = 100.0
    effective_health_score: float = 100.0
    consecutive_failures: int = 0
    total_success: int = 0
    total_failures: int = 0
    last_latency_ms: int | None = None
    last_error: str = ""
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    checkpoint_cursor: str | None = None
    checkpoint_publish_time: datetime | None = None
    is_active: bool = False


class EventConnectorRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    connector_name: str
    source_name: str
    connector_type: EventConnectorType
    enabled: bool
    fetch_limit: int
    poll_interval_minutes: int
    replay_backoff_seconds: int
    max_retry: int
    config: dict[str, Any] = Field(default_factory=dict)
    created_by: str
    note: str


class EventConnectorCheckpointRecord(BaseModel):
    connector_name: str
    checkpoint_cursor: str | None = None
    checkpoint_publish_time: datetime | None = None
    updated_at: datetime
    last_run_at: datetime | None = None
    last_success_at: datetime | None = None


class EventConnectorRunRecord(BaseModel):
    run_id: str
    connector_name: str
    source_name: str
    started_at: datetime
    finished_at: datetime | None = None
    status: EventConnectorRunStatus
    triggered_by: str
    pulled_count: int = 0
    normalized_count: int = 0
    inserted_count: int = 0
    updated_count: int = 0
    failed_count: int = 0
    replayed_count: int = 0
    checkpoint_before: str | None = None
    checkpoint_after: str | None = None
    error_message: str | None = None
    details: dict[str, Any] = Field(default_factory=dict)


class EventConnectorRunRequest(BaseModel):
    connector_name: str
    triggered_by: str = "manual"
    dry_run: bool = False
    force_full_sync: bool = False
    fetch_limit_override: int | None = Field(default=None, ge=1, le=10000)


class EventConnectorRunResult(BaseModel):
    run: EventConnectorRunRecord
    errors: list[str] = Field(default_factory=list)


class EventConnectorFailureRecord(BaseModel):
    id: int
    connector_name: str
    source_name: str
    run_id: str
    created_at: datetime
    updated_at: datetime
    status: EventConnectorFailureStatus
    retry_count: int
    next_retry_at: datetime | None = None
    last_error: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class EventConnectorFailureRepairRequest(BaseModel):
    connector_name: str
    failure_id: int
    patch_raw_record: dict[str, Any] = Field(default_factory=dict)
    patch_event: dict[str, Any] = Field(default_factory=dict)
    reset_retry_count: bool = False
    triggered_by: str = "manual_repair"
    note: str = ""

    @model_validator(mode="after")
    def _validate_patch(self) -> "EventConnectorFailureRepairRequest":
        if not self.patch_raw_record and not self.patch_event:
            raise ValueError("at least one of patch_raw_record or patch_event must be provided")
        return self


class EventConnectorFailureRepairResult(BaseModel):
    connector_name: str
    failure_id: int
    updated: bool
    failure: EventConnectorFailureRecord | None = None


class EventConnectorReplayRequest(BaseModel):
    connector_name: str
    limit: int = Field(default=100, ge=1, le=2000)
    triggered_by: str = "manual_replay"


class EventConnectorReplayResult(BaseModel):
    connector_name: str
    picked: int
    replayed: int
    failed: int
    dead: int
    errors: list[str] = Field(default_factory=list)


class EventConnectorManualReplayRequest(BaseModel):
    connector_name: str
    failure_ids: list[int] = Field(default_factory=list)
    triggered_by: str = "manual_replay_workbench"

    @model_validator(mode="after")
    def _validate_failure_ids(self) -> "EventConnectorManualReplayRequest":
        if not self.failure_ids:
            raise ValueError("failure_ids must not be empty")
        return self


class EventConnectorManualReplayItem(BaseModel):
    failure_id: int
    status: EventConnectorFailureStatus | EventConnectorRunStatus
    message: str = ""


class EventConnectorManualReplayResult(BaseModel):
    connector_name: str
    picked: int
    replayed: int
    failed: int
    dead: int
    items: list[EventConnectorManualReplayItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class EventConnectorRepairReplayItemRequest(BaseModel):
    failure_id: int
    patch_raw_record: dict[str, Any] = Field(default_factory=dict)
    patch_event: dict[str, Any] = Field(default_factory=dict)
    reset_retry_count: bool = False
    note: str = ""


class EventConnectorRepairReplayRequest(BaseModel):
    connector_name: str
    items: list[EventConnectorRepairReplayItemRequest] = Field(default_factory=list)
    triggered_by: str = "manual_repair_replay_workbench"

    @model_validator(mode="after")
    def _validate_items(self) -> "EventConnectorRepairReplayRequest":
        if not self.items:
            raise ValueError("items must not be empty")
        failure_ids = [x.failure_id for x in self.items]
        if len(set(failure_ids)) != len(failure_ids):
            raise ValueError("failure_id in items must be unique")
        return self


class EventConnectorRepairReplayResult(BaseModel):
    connector_name: str
    repaired: int
    picked: int
    replayed: int
    failed: int
    dead: int
    repaired_failure_ids: list[int] = Field(default_factory=list)
    items: list[EventConnectorManualReplayItem] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class EventConnectorSLABreach(BaseModel):
    connector_name: str
    source_name: str
    breach_type: EventConnectorSLABreachType
    severity: SignalLevel
    stage: str
    message: str
    freshness_minutes: int | None = None
    pending_failures: int = 0
    dead_failures: int = 0
    latest_run_status: EventConnectorRunStatus | None = None
    latest_run_at: datetime | None = None


class EventConnectorSLAStatus(BaseModel):
    connector_name: str
    source_name: str
    enabled: bool
    freshness_minutes: int | None = None
    pending_failures: int = 0
    dead_failures: int = 0
    latest_run_status: EventConnectorRunStatus | None = None
    latest_run_at: datetime | None = None
    severity: SignalLevel = SignalLevel.INFO
    breach_types: list[EventConnectorSLABreachType] = Field(default_factory=list)


class EventConnectorSLAReport(BaseModel):
    generated_at: datetime
    policy_defaults: EventConnectorSLAPolicy
    connector_count: int
    warning_count: int
    critical_count: int
    escalated_count: int
    statuses: list[EventConnectorSLAStatus] = Field(default_factory=list)
    breaches: list[EventConnectorSLABreach] = Field(default_factory=list)


class EventConnectorSLAAlertSyncResult(BaseModel):
    generated_at: datetime
    emitted: int
    skipped: int
    recovered: int = 0
    escalated: int = 0
    open_states: int = 0
    open_escalated: int = 0
    report: EventConnectorSLAReport


class EventConnectorSLOPoint(BaseModel):
    window_start: datetime
    window_end: datetime
    runs: int = 0
    run_success_rate: float = 0.0
    run_failure_rate: float = 0.0
    warning_breaches: int = 0
    critical_breaches: int = 0
    escalated_breaches: int = 0
    burn_rate_warning: float = 0.0
    burn_rate_critical: float = 0.0


class EventConnectorSLOHistory(BaseModel):
    generated_at: datetime
    connector_name: str | None = None
    lookback_days: int
    bucket_hours: int
    total_points: int
    points: list[EventConnectorSLOPoint] = Field(default_factory=list)


class EventConnectorSLAAlertStateRecord(BaseModel):
    dedupe_key: str
    connector_name: str
    source_name: str
    breach_type: EventConnectorSLABreachType
    stage: str
    severity: SignalLevel
    first_seen_at: datetime
    last_seen_at: datetime
    last_emitted_at: datetime | None = None
    last_recovered_at: datetime | None = None
    last_escalated_at: datetime | None = None
    repeat_count: int = 0
    escalation_level: int = 0
    escalation_reason: str = ""
    is_open: bool = True
    message: str = ""


class EventConnectorSLAAlertStateSummary(BaseModel):
    generated_at: datetime
    connector_name: str | None = None
    open_states: int
    escalated_open_states: int
    open_by_severity: dict[str, int] = Field(default_factory=dict)
    open_by_breach_type: dict[str, int] = Field(default_factory=dict)
    open_by_escalation_level: dict[str, int] = Field(default_factory=dict)


class EventConnectorOverviewItem(BaseModel):
    connector_name: str
    source_name: str
    connector_type: EventConnectorType
    enabled: bool
    active_source_key: str | None = None
    active_source_health: float | None = None
    last_run_status: EventConnectorRunStatus | None = None
    last_run_at: datetime | None = None
    last_success_at: datetime | None = None
    checkpoint_publish_time: datetime | None = None
    pending_failures: int = 0
    dead_failures: int = 0


class EventConnectorOverviewResult(BaseModel):
    generated_at: datetime
    connectors: list[EventConnectorOverviewItem] = Field(default_factory=list)


class EventCoverageDailyPoint(BaseModel):
    trade_date: date
    total_events: int
    positive_events: int
    negative_events: int
    neutral_events: int


class EventCoverageSourceItem(BaseModel):
    source_name: str
    total_events: int
    symbols: int
    last_publish_time: datetime | None = None


class EventOpsCoverageSummary(BaseModel):
    generated_at: datetime
    lookback_days: int
    total_events: int
    positive_events: int
    negative_events: int
    neutral_events: int
    symbols_covered: int
    sources_covered: int
    daily: list[EventCoverageDailyPoint] = Field(default_factory=list)
    sources: list[EventCoverageSourceItem] = Field(default_factory=list)


class EventFeatureBacktestCompareRequest(BaseModel):
    symbol: str
    start_date: date
    end_date: date
    strategy_name: str = Field(default="event_driven")
    strategy_params: dict[str, float | int | str | bool] = Field(default_factory=dict)
    event_lookback_days: int = Field(default=30, ge=1, le=3650)
    event_decay_half_life_days: float = Field(default=7.0, gt=0.0, le=365.0)
    initial_cash: float = Field(default=1_000_000.0, gt=0.0)
    commission_rate: float = Field(default=0.0003, ge=0.0, le=0.02)
    slippage_rate: float = Field(default=0.0005, ge=0.0, le=0.02)
    lot_size: int = Field(default=100, ge=1)
    max_single_position: float = Field(default=0.05, gt=0.0, le=1.0)
    save_report: bool = True
    watermark: str = "For Research Only"

    @model_validator(mode="after")
    def _validate_dates(self) -> "EventFeatureBacktestCompareRequest":
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")
        return self


class EventFeatureBacktestDelta(BaseModel):
    total_return_delta: float
    max_drawdown_delta: float
    trade_count_delta: int
    win_rate_delta: float
    annualized_return_delta: float
    sharpe_delta: float


class EventFeatureSignalDiagnostics(BaseModel):
    events_loaded: int
    event_rows_covered: int
    event_row_ratio: float
    avg_event_score: float
    avg_negative_event_score: float
    score_return_corr_1d: float | None = None


class EventFeatureBacktestCompareResult(BaseModel):
    symbol: str
    strategy_name: str
    provider: str
    baseline: BacktestMetrics
    enriched: BacktestMetrics
    delta: EventFeatureBacktestDelta
    diagnostics: EventFeatureSignalDiagnostics
    report_content: str
    report_path: str | None = None


class DataLicenseRegisterRequest(BaseModel):
    dataset_name: str
    provider: str
    licensor: str
    usage_scopes: list[str] = Field(default_factory=lambda: ["internal_research"])
    allow_export: bool = False
    enforce_watermark: str = "For Research Only"
    valid_from: date
    valid_to: date | None = None
    max_export_rows: int | None = Field(default=None, ge=1)
    created_by: str = "system"
    note: str = ""

    @model_validator(mode="after")
    def _validate_dates(self) -> "DataLicenseRegisterRequest":
        if self.valid_to and self.valid_from > self.valid_to:
            raise ValueError("valid_from must be <= valid_to")
        return self


class DataLicenseRecord(BaseModel):
    id: int
    created_at: datetime
    dataset_name: str
    provider: str
    licensor: str
    usage_scopes: list[str] = Field(default_factory=list)
    allow_export: bool
    enforce_watermark: str
    valid_from: date
    valid_to: date | None = None
    max_export_rows: int | None = None
    created_by: str
    note: str = ""


class DataLicenseCheckRequest(BaseModel):
    dataset_name: str
    provider: str
    requested_usage: str = "internal_research"
    export_requested: bool = False
    expected_rows: int = Field(default=0, ge=0)
    as_of: date | None = None


class DataLicenseCheckResult(BaseModel):
    allowed: bool
    reason: str
    watermark: str = "For Research Only"
    allow_export: bool = False
    max_export_rows: int | None = None
    matched_license_id: int | None = None
    expires_on: date | None = None


class JobType(str, Enum):
    PIPELINE_DAILY = "pipeline_daily"
    RESEARCH_WORKFLOW = "research_workflow"
    REPORT_GENERATE = "report_generate"
    EVENT_CONNECTOR_SYNC = "event_connector_sync"
    EVENT_CONNECTOR_REPLAY = "event_connector_replay"
    COMPLIANCE_EVIDENCE_EXPORT = "compliance_evidence_export"
    ALERT_ONCALL_RECONCILE = "alert_oncall_reconcile"


class EventConnectorSyncJobPayload(BaseModel):
    connector_name: str
    dry_run: bool = False
    force_full_sync: bool = False
    fetch_limit_override: int | None = Field(default=None, ge=1, le=10000)


class EventConnectorReplayJobPayload(BaseModel):
    connector_name: str
    limit: int = Field(default=100, ge=1, le=2000)


class ComplianceEvidenceExportJobPayload(BaseModel):
    request: ComplianceEvidenceExportRequest


class OncallReconcileJobPayload(BaseModel):
    provider: str = "generic_oncall"
    endpoint: str
    mapping_template: str | None = None
    limit: int = Field(default=200, ge=1, le=5000)
    dry_run: bool = False


class JobStatus(str, Enum):
    ACTIVE = "ACTIVE"
    DISABLED = "DISABLED"


class JobRunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class JobSLABreachType(str, Enum):
    INVALID_CRON = "INVALID_CRON"
    MISSED_RUN = "MISSED_RUN"
    LATEST_RUN_FAILED = "LATEST_RUN_FAILED"
    RUNNING_TIMEOUT = "RUNNING_TIMEOUT"


class JobRegisterRequest(BaseModel):
    name: str
    job_type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    owner: str
    schedule_cron: str | None = None
    enabled: bool = True
    description: str = ""


class JobDefinitionRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    name: str
    job_type: JobType
    payload: dict[str, Any] = Field(default_factory=dict)
    owner: str
    schedule_cron: str | None = None
    status: JobStatus
    description: str = ""


class JobRunRecord(BaseModel):
    run_id: str
    job_id: int
    started_at: datetime
    finished_at: datetime | None = None
    status: JobRunStatus
    triggered_by: str
    error_message: str | None = None
    result_summary: dict[str, Any] = Field(default_factory=dict)


class JobTriggerRequest(BaseModel):
    triggered_by: str


class JobScheduleTickRequest(BaseModel):
    as_of: datetime | None = None
    triggered_by: str = "scheduler_manual"


class JobScheduleTickResult(BaseModel):
    tick_time: datetime
    timezone: str
    matched_jobs: list[int] = Field(default_factory=list)
    triggered_runs: list[JobRunRecord] = Field(default_factory=list)
    skipped_jobs: list[int] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class JobSLABreach(BaseModel):
    job_id: int
    job_name: str
    breach_type: JobSLABreachType
    severity: SignalLevel
    message: str
    schedule_cron: str
    expected_run_at: datetime | None = None
    last_run_at: datetime | None = None
    delay_minutes: int | None = None


class JobSLAReport(BaseModel):
    checked_at: datetime
    timezone: str
    total_active_jobs: int
    total_scheduled_jobs: int
    breaches: list[JobSLABreach] = Field(default_factory=list)


class OpsJobStats(BaseModel):
    total_jobs: int
    active_jobs: int
    scheduled_jobs: int
    runs_last_24h: int
    success_last_24h: int
    failed_last_24h: int
    running_last_24h: int


class OpsAlertStats(BaseModel):
    unacked_total: int
    unacked_warning: int
    unacked_critical: int


class OpsExecutionStats(BaseModel):
    sample_size: int
    follow_rate: float
    avg_delay_days: float
    avg_slippage_bps: float


class OpsEventStats(BaseModel):
    lookback_days: int
    total_events: int
    active_symbols: int
    active_sources: int
    pending_failures: int
    dead_failures: int
    connector_runs_24h: int
    connector_failures_24h: int
    connector_sla_warning: int = 0
    connector_sla_critical: int = 0
    connector_sla_escalated: int = 0


class OpsDashboardSummary(BaseModel):
    generated_at: datetime
    jobs: OpsJobStats
    alerts: OpsAlertStats
    execution: OpsExecutionStats
    event: OpsEventStats | None = None
    sla: JobSLAReport
    recent_runs: list[JobRunRecord] = Field(default_factory=list)


class AlertEscalationStage(BaseModel):
    level_threshold: int = Field(default=1, ge=1, le=10)
    channel: str = "im"
    targets: list[str] = Field(default_factory=list)
    note: str = ""


class AlertSubscriptionCreateRequest(BaseModel):
    name: str
    owner: str
    event_types: list[str] = Field(default_factory=list)
    min_severity: SignalLevel = SignalLevel.WARNING
    dedupe_window_sec: int = Field(default=300, ge=0, le=86_400)
    enabled: bool = True
    channel: str = "inbox"
    channel_config: dict[str, Any] = Field(default_factory=dict)
    escalation_chain: list[AlertEscalationStage] = Field(default_factory=list)
    runbook_url: str | None = None


class AlertSubscriptionRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    name: str
    owner: str
    event_types: list[str] = Field(default_factory=list)
    min_severity: SignalLevel
    dedupe_window_sec: int
    enabled: bool
    channel: str
    channel_config: dict[str, Any] = Field(default_factory=dict)
    escalation_chain: list[AlertEscalationStage] = Field(default_factory=list)
    runbook_url: str | None = None


class AlertNotificationRecord(BaseModel):
    id: int
    subscription_id: int
    event_id: int
    created_at: datetime
    severity: SignalLevel
    source: str
    message: str
    payload: dict[str, str | int | float | bool | None] = Field(default_factory=dict)
    acked: bool = False
    acked_at: datetime | None = None


class AlertDeliveryStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class AlertDeliveryRecord(BaseModel):
    id: int
    notification_id: int
    subscription_id: int
    created_at: datetime
    channel: str
    target: str
    status: AlertDeliveryStatus
    error_message: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class OncallCallbackRequest(BaseModel):
    provider: str = "generic_oncall"
    incident_id: str | None = None
    status: str | None = None
    mapping_template: str | None = None
    timestamp: str | None = None
    signature: str | None = None
    notification_id: int | None = None
    delivery_id: int | None = None
    external_ticket_id: str | None = None
    ack_by: str = "oncall"
    note: str = ""
    raw_payload: dict[str, Any] = Field(default_factory=dict)


class OncallEventRecord(BaseModel):
    id: int
    created_at: datetime
    updated_at: datetime
    provider: str
    incident_id: str
    status: str
    notification_id: int | None = None
    delivery_id: int | None = None
    external_ticket_id: str | None = None
    acked: bool = False
    ack_by: str = ""
    note: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)


class OncallCallbackResult(BaseModel):
    provider: str
    incident_id: str
    status: str
    mapping_template: str | None = None
    signature_checked: bool = False
    signature_valid: bool = False
    linked_notification_ids: list[int] = Field(default_factory=list)
    acked_notifications: int = 0
    stored_events: int = 0
    message: str = ""


class OncallReconcileRequest(BaseModel):
    provider: str = "generic_oncall"
    endpoint: str
    mapping_template: str | None = None
    limit: int = Field(default=200, ge=1, le=5000)
    dry_run: bool = False


class OncallReconcileResult(BaseModel):
    provider: str
    endpoint: str
    mapping_template: str | None = None
    pulled: int = 0
    matched: int = 0
    callbacks: int = 0
    acked_notifications: int = 0
    dry_run: bool = False
    errors: list[str] = Field(default_factory=list)
