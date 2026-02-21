from __future__ import annotations

import logging
from functools import lru_cache

from trading_assistant.alerts.service import AlertService
from trading_assistant.alerts.store import AlertStore
from trading_assistant.alerts.dispatcher import RealAlertDispatcher
from trading_assistant.audit.service import AuditService
from trading_assistant.audit.store import AuditStore
from trading_assistant.backtest.engine import BacktestEngine
from trading_assistant.core.config import Settings, get_settings
from trading_assistant.data.akshare_provider import AkshareProvider
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.tushare_provider import TushareProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.event_connector_service import EventConnectorService
from trading_assistant.governance.event_connector_store import EventConnectorStore
from trading_assistant.governance.event_feature_compare import EventFeatureBacktestCompareService
from trading_assistant.governance.event_nlp import EventStandardizer
from trading_assistant.governance.event_nlp_governance import EventNLPGovernanceService
from trading_assistant.governance.event_nlp_store import EventNLPStore
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.event_store import EventStore
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.license_store import DataLicenseStore
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.governance.snapshot_store import DataSnapshotStore
from trading_assistant.monitoring.model_risk import ModelRiskService
from trading_assistant.ops.dashboard import OpsDashboardService
from trading_assistant.ops.job_service import JobService
from trading_assistant.ops.job_store import JobStore
from trading_assistant.ops.scheduler_worker import JobSchedulerWorker
from trading_assistant.pipeline.runner import DailyPipelineRunner
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.portfolio.rebalancer import PortfolioRebalancer
from trading_assistant.portfolio.stress import PortfolioStressTester
from trading_assistant.replay.service import ReplayService
from trading_assistant.replay.store import ReplayStore
from trading_assistant.reporting.service import ReportingService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.signal.service import SignalService
from trading_assistant.strategy.registry import StrategyRegistry
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.governance_store import StrategyGovernanceStore
from trading_assistant.workflow.research import ResearchWorkflowService

logger = logging.getLogger(__name__)


def _build_provider(name: str, settings: Settings) -> MarketDataProvider:
    if name == "akshare":
        return AkshareProvider()
    if name == "tushare":
        return TushareProvider(token=settings.tushare_token)
    raise ValueError(f"Unsupported provider: {name}")


@lru_cache
def get_data_provider() -> CompositeDataProvider:
    settings = get_settings()
    providers: list[MarketDataProvider] = []
    for name in settings.provider_priority_list:
        try:
            providers.append(_build_provider(name, settings))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Skip provider %s due to init error: %s", name, exc)
    if not providers:
        raise RuntimeError("No usable data provider. Check DATA_PROVIDER_PRIORITY and credentials.")
    return CompositeDataProvider(providers=providers)


@lru_cache
def get_factor_engine() -> FactorEngine:
    return FactorEngine()


@lru_cache
def get_strategy_registry() -> StrategyRegistry:
    return StrategyRegistry()


@lru_cache
def get_risk_engine() -> RiskEngine:
    settings = get_settings()
    return RiskEngine(
        max_single_position=settings.max_single_position,
        max_drawdown=settings.max_drawdown,
        max_industry_exposure=settings.max_industry_exposure,
        min_turnover_20d=settings.min_turnover_20d,
    )


@lru_cache
def get_signal_service() -> SignalService:
    return SignalService()


@lru_cache
def get_backtest_engine() -> BacktestEngine:
    return BacktestEngine(
        factor_engine=get_factor_engine(),
        risk_engine=get_risk_engine(),
    )


@lru_cache
def get_audit_service() -> AuditService:
    settings = get_settings()
    return AuditService(store=AuditStore(settings.audit_db_path))


@lru_cache
def get_pipeline_runner() -> DailyPipelineRunner:
    settings = get_settings()
    return DailyPipelineRunner(
        provider=get_data_provider(),
        factor_engine=get_factor_engine(),
        registry=get_strategy_registry(),
        risk_engine=get_risk_engine(),
        signal_service=get_signal_service(),
        quality_service=get_data_quality_service(),
        pit_validator=get_pit_validator(),
        event_service=get_event_service(),
        snapshot_service=get_snapshot_service(),
        license_service=get_data_license_service(),
        enforce_data_license=settings.enforce_data_license,
    )


@lru_cache
def get_snapshot_service() -> DataSnapshotService:
    settings = get_settings()
    return DataSnapshotService(store=DataSnapshotStore(settings.snapshot_db_path))


@lru_cache
def get_data_quality_service() -> DataQualityService:
    return DataQualityService()


@lru_cache
def get_data_license_service() -> DataLicenseService:
    settings = get_settings()
    return DataLicenseService(store=DataLicenseStore(settings.license_db_path))


@lru_cache
def get_event_service() -> EventService:
    settings = get_settings()
    return EventService(store=EventStore(settings.event_db_path))


@lru_cache
def get_event_connector_store() -> EventConnectorStore:
    settings = get_settings()
    return EventConnectorStore(settings.event_db_path)


@lru_cache
def get_event_nlp_store() -> EventNLPStore:
    settings = get_settings()
    return EventNLPStore(settings.event_db_path)


@lru_cache
def get_event_connector_service() -> EventConnectorService:
    return EventConnectorService(
        event_service=get_event_service(),
        connector_store=get_event_connector_store(),
        standardizer=EventStandardizer(
            active_ruleset_loader=get_event_nlp_store().load_active_ruleset,
            refresh_interval_seconds=15,
        ),
    )


@lru_cache
def get_event_feature_compare_service() -> EventFeatureBacktestCompareService:
    settings = get_settings()
    return EventFeatureBacktestCompareService(
        provider=get_data_provider(),
        factor_engine=get_factor_engine(),
        pit=get_pit_validator(),
        event_service=get_event_service(),
        registry=get_strategy_registry(),
        settings=settings,
        output_dir="reports",
    )


@lru_cache
def get_event_nlp_governance_service() -> EventNLPGovernanceService:
    return EventNLPGovernanceService(
        store=get_event_nlp_store(),
        feature_compare=get_event_feature_compare_service(),
    )


@lru_cache
def get_pit_validator() -> PITValidator:
    return PITValidator()


@lru_cache
def get_portfolio_optimizer() -> PortfolioOptimizer:
    return PortfolioOptimizer()


@lru_cache
def get_portfolio_rebalancer() -> PortfolioRebalancer:
    return PortfolioRebalancer()


@lru_cache
def get_portfolio_stress_tester() -> PortfolioStressTester:
    return PortfolioStressTester()


@lru_cache
def get_replay_service() -> ReplayService:
    settings = get_settings()
    return ReplayService(store=ReplayStore(settings.replay_db_path))


@lru_cache
def get_research_workflow_service() -> ResearchWorkflowService:
    settings = get_settings()
    return ResearchWorkflowService(
        provider=get_data_provider(),
        factor_engine=get_factor_engine(),
        registry=get_strategy_registry(),
        risk_engine=get_risk_engine(),
        optimizer=get_portfolio_optimizer(),
        replay=get_replay_service(),
        pit_validator=get_pit_validator(),
        event_service=get_event_service(),
        license_service=get_data_license_service(),
        enforce_data_license=settings.enforce_data_license,
    )


@lru_cache
def get_strategy_governance_service() -> StrategyGovernanceService:
    settings = get_settings()
    return StrategyGovernanceService(
        store=StrategyGovernanceStore(settings.strategy_gov_db_path),
        required_approval_roles=settings.required_approval_roles_list,
        min_approval_count=settings.strategy_min_approval_count,
    )


@lru_cache
def get_reporting_service() -> ReportingService:
    return ReportingService(
        replay=get_replay_service(),
        audit=get_audit_service(),
        output_dir="reports",
    )


@lru_cache
def get_model_risk_service() -> ModelRiskService:
    return ModelRiskService(
        audit=get_audit_service(),
        replay=get_replay_service(),
    )


@lru_cache
def get_alert_service() -> AlertService:
    settings = get_settings()
    return AlertService(
        store=AlertStore(settings.alert_db_path),
        audit=get_audit_service(),
        dispatcher=RealAlertDispatcher(settings=settings),
        default_runbook_base_url=settings.alert_runbook_base_url,
    )


@lru_cache
def get_job_service() -> JobService:
    settings = get_settings()
    return JobService(
        store=JobStore(settings.job_db_path),
        pipeline=get_pipeline_runner(),
        research=get_research_workflow_service(),
        reporting=get_reporting_service(),
        event_connector=get_event_connector_service(),
        scheduler_timezone=settings.ops_scheduler_timezone,
        running_timeout_minutes=settings.ops_job_running_timeout_minutes,
    )


@lru_cache
def get_ops_dashboard_service() -> OpsDashboardService:
    return OpsDashboardService(
        jobs=get_job_service(),
        alerts=get_alert_service(),
        replay=get_replay_service(),
        event_connector=get_event_connector_service(),
    )


@lru_cache
def get_job_scheduler_worker() -> JobSchedulerWorker:
    settings = get_settings()
    return JobSchedulerWorker(
        jobs=get_job_service(),
        audit=get_audit_service(),
        alerts=get_alert_service(),
        tick_seconds=settings.ops_scheduler_tick_seconds,
        sla_grace_minutes=settings.ops_job_sla_grace_minutes,
        sla_log_cooldown_seconds=settings.ops_scheduler_sla_log_cooldown_seconds,
        sync_alerts_from_audit=settings.ops_scheduler_sync_alerts_from_audit,
    )
