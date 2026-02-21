from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from trading_assistant.core.models import (
    DataLicenseCheckRequest,
    DataQualityRequest,
    DataSnapshotRegisterRequest,
    PipelineRunRequest,
    PipelineRunResult,
    PipelineSymbolResult,
    RiskCheckRequest,
    SignalLevel,
)
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.signal.service import SignalService
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.registry import StrategyRegistry


class DailyPipelineRunner:
    def __init__(
        self,
        provider: CompositeDataProvider,
        factor_engine: FactorEngine,
        registry: StrategyRegistry,
        risk_engine: RiskEngine,
        signal_service: SignalService,
        quality_service: DataQualityService,
        pit_validator: PITValidator,
        snapshot_service: DataSnapshotService,
        event_service: EventService | None = None,
        license_service: DataLicenseService | None = None,
        enforce_data_license: bool = False,
    ) -> None:
        self.provider = provider
        self.factor_engine = factor_engine
        self.registry = registry
        self.risk_engine = risk_engine
        self.signal_service = signal_service
        self.quality_service = quality_service
        self.pit_validator = pit_validator
        self.event_service = event_service
        self.snapshot_service = snapshot_service
        self.license_service = license_service
        self.enforce_data_license = enforce_data_license

    def run(self, req: PipelineRunRequest) -> PipelineRunResult:
        started_at = datetime.now(timezone.utc)
        strategy = self.registry.get(req.strategy_name)
        results: list[PipelineSymbolResult] = []
        use_event_enrichment = req.enable_event_enrichment or req.strategy_name == "event_driven"

        for symbol in req.symbols:
            try:
                used_provider, bars = self.provider.get_daily_bars_with_source(symbol, req.start_date, req.end_date)
            except Exception:  # noqa: BLE001
                results.append(
                    PipelineSymbolResult(
                        symbol=symbol,
                        provider="N/A",
                        signal_count=0,
                        blocked_count=0,
                        warning_count=0,
                        quality_passed=False,
                    )
                )
                continue
            if self.license_service is not None:
                check = self.license_service.check(
                    DataLicenseCheckRequest(
                        dataset_name="daily_bars",
                        provider=used_provider,
                        requested_usage="internal_research",
                        export_requested=False,
                        expected_rows=len(bars),
                        as_of=req.end_date,
                    )
                )
                if self.enforce_data_license and not check.allowed:
                    results.append(
                        PipelineSymbolResult(
                            symbol=symbol,
                            provider=used_provider,
                            signal_count=0,
                            blocked_count=0,
                            warning_count=0,
                            quality_passed=False,
                        )
                    )
                    continue

            quality = self.quality_service.evaluate(
                req=DataQualityRequest(symbol=symbol, start_date=req.start_date, end_date=req.end_date),
                bars=bars,
                provider=used_provider,
            )
            pit_result = self.pit_validator.validate_bars(
                symbol=symbol,
                provider=used_provider,
                bars=bars,
                as_of=req.end_date,
            )

            snapshot_id = self.snapshot_service.register(
                DataSnapshotRegisterRequest(
                    dataset_name="daily_bars",
                    symbol=symbol,
                    start_date=req.start_date,
                    end_date=req.end_date,
                    provider=used_provider,
                    row_count=len(bars),
                    content_hash=dataframe_content_hash(bars),
                )
            )

            if bars.empty or (not quality.passed) or (not pit_result.passed):
                results.append(
                    PipelineSymbolResult(
                        symbol=symbol,
                        provider=used_provider,
                        signal_count=0,
                        blocked_count=0,
                        warning_count=0,
                        quality_passed=quality.passed and pit_result.passed,
                        snapshot_id=snapshot_id,
                    )
                )
                continue

            status = self.provider.get_security_status(symbol)
            bars["is_st"] = bool(status.get("is_st", False))
            bars["is_suspended"] = bool(status.get("is_suspended", False))
            event_stats = {"events_loaded": 0}
            bars_for_features = bars
            if use_event_enrichment and self.event_service is not None:
                bars_for_features, event_stats = self.event_service.enrich_bars(
                    symbol=symbol,
                    bars=bars,
                    lookback_days=req.event_lookback_days,
                    decay_half_life_days=req.event_decay_half_life_days,
                )

            features = self.factor_engine.compute(bars_for_features)
            candidates = strategy.generate(features, StrategyContext(params=req.strategy_params))
            blocked_count = 0
            warning_count = 0
            for signal in candidates:
                risk_req = RiskCheckRequest(
                    signal=signal,
                    is_st=bool(status.get("is_st", False)),
                    is_suspended=bool(status.get("is_suspended", False)),
                    avg_turnover_20d=float(features.iloc[-1].get("turnover20", 0.0)),
                    symbol_industry=req.industry_map.get(symbol),
                )
                risk_result = self.risk_engine.evaluate(risk_req)
                _ = self.signal_service.to_trade_prep_sheet(signal, risk_result)
                if risk_result.blocked:
                    blocked_count += 1
                elif risk_result.level == SignalLevel.WARNING:
                    warning_count += 1

            results.append(
                PipelineSymbolResult(
                    symbol=symbol,
                    provider=used_provider,
                    signal_count=len(candidates),
                    blocked_count=blocked_count,
                    warning_count=warning_count,
                    quality_passed=quality.passed,
                    snapshot_id=snapshot_id,
                    event_rows_used=int(event_stats.get("events_loaded", 0)),
                )
            )

        finished_at = datetime.now(timezone.utc)
        total_signals = sum(r.signal_count for r in results)
        total_blocked = sum(r.blocked_count for r in results)
        total_warnings = sum(r.warning_count for r in results)
        return PipelineRunResult(
            run_id=uuid4().hex,
            started_at=started_at,
            finished_at=finished_at,
            strategy_name=req.strategy_name,
            results=results,
            total_symbols=len(results),
            total_signals=total_signals,
            total_blocked=total_blocked,
            total_warnings=total_warnings,
        )
