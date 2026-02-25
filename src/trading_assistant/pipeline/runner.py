from __future__ import annotations

from datetime import datetime, timezone
import logging
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
from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.data.composite_provider import CompositeDataProvider
from trading_assistant.data.utils import dataframe_content_hash
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.fundamentals.service import FundamentalService
from trading_assistant.governance.data_quality import DataQualityService
from trading_assistant.governance.event_service import EventService
from trading_assistant.governance.license_service import DataLicenseService
from trading_assistant.governance.pit_validator import PITValidator
from trading_assistant.governance.snapshot_service import DataSnapshotService
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.signal.service import SignalService
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.registry import StrategyRegistry
from trading_assistant.trading.costs import (
    estimate_roundtrip_cost_bps,
    infer_expected_edge_bps,
    required_cash_for_min_lot,
)
from trading_assistant.trading.small_capital import apply_small_capital_overrides

logger = logging.getLogger(__name__)


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
        autotune_service: AutoTuneService | None = None,
        event_service: EventService | None = None,
        license_service: DataLicenseService | None = None,
        enforce_data_license: bool = False,
        fundamental_service: FundamentalService | None = None,
        default_commission_rate: float = 0.0003,
        default_slippage_rate: float = 0.0005,
        fee_min_commission_cny: float = 5.0,
        fee_stamp_duty_sell_rate: float = 0.0005,
        fee_transfer_rate: float = 0.00001,
        small_capital_mode_enabled: bool = False,
        small_capital_principal_cny: float = 2000.0,
        small_capital_cash_buffer_ratio: float = 0.05,
        small_capital_min_expected_edge_bps: float = 45.0,
        small_capital_lot_size: int = 100,
    ) -> None:
        self.provider = provider
        self.fundamental_service = fundamental_service or FundamentalService(provider=provider)
        self.factor_engine = factor_engine
        self.registry = registry
        self.risk_engine = risk_engine
        self.signal_service = signal_service
        self.quality_service = quality_service
        self.pit_validator = pit_validator
        self.event_service = event_service
        self.autotune_service = autotune_service
        self.snapshot_service = snapshot_service
        self.license_service = license_service
        self.enforce_data_license = enforce_data_license
        self.default_commission_rate = float(default_commission_rate)
        self.default_slippage_rate = float(default_slippage_rate)
        self.fee_min_commission_cny = float(fee_min_commission_cny)
        self.fee_stamp_duty_sell_rate = float(fee_stamp_duty_sell_rate)
        self.fee_transfer_rate = float(fee_transfer_rate)
        self.small_capital_mode_enabled = bool(small_capital_mode_enabled)
        self.small_capital_principal_cny = float(small_capital_principal_cny)
        self.small_capital_cash_buffer_ratio = float(small_capital_cash_buffer_ratio)
        self.small_capital_min_expected_edge_bps = float(small_capital_min_expected_edge_bps)
        self.small_capital_lot_size = max(1, int(small_capital_lot_size))

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

            status = {
                "is_st": bool(bars.iloc[-1].get("is_st", False)),
                "is_suspended": bool(bars.iloc[-1].get("is_suspended", False)),
            }
            try:
                fetched_status = self.provider.get_security_status(symbol)
                status["is_st"] = bool(fetched_status.get("is_st", status["is_st"]))
                status["is_suspended"] = bool(fetched_status.get("is_suspended", status["is_suspended"]))
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Security status lookup failed for %s in pipeline; fallback to bars/default status: %s",
                    symbol,
                    exc,
                )
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
            fundamental_stats: dict[str, object] = {"available": False, "source": None}
            if req.enable_fundamental_enrichment and self.fundamental_service is not None:
                bars_for_features, fundamental_stats = self.fundamental_service.enrich_bars(
                    symbol=symbol,
                    bars=bars_for_features,
                    as_of=req.end_date,
                    max_staleness_days=req.fundamental_max_staleness_days,
                )

            features = self.factor_engine.compute(bars_for_features)
            small_capital_mode = bool(self.small_capital_mode_enabled or req.enable_small_capital_mode)
            small_capital_principal = float(req.small_capital_principal or self.small_capital_principal_cny)
            small_lot = max(1, self.small_capital_lot_size)
            strategy_params, _ = (
                self.autotune_service.resolve_runtime_params(
                    strategy_name=req.strategy_name,
                    symbol=symbol,
                    explicit_params=req.strategy_params,
                    use_profile=req.use_autotune_profile,
                )
                if self.autotune_service is not None
                else (dict(req.strategy_params), None)
            )
            candidates = strategy.generate(
                features,
                StrategyContext(
                    params=strategy_params,
                    market_state={
                        "enable_small_capital_mode": small_capital_mode,
                        "small_capital_principal": small_capital_principal,
                        "small_capital_lot_size": small_lot,
                        "small_capital_cash_buffer_ratio": self.small_capital_cash_buffer_ratio,
                        "commission_rate": self.default_commission_rate,
                        "min_commission_cny": self.fee_min_commission_cny,
                        "transfer_fee_rate": self.fee_transfer_rate,
                        "stamp_duty_sell_rate": self.fee_stamp_duty_sell_rate,
                        "slippage_rate": self.default_slippage_rate,
                    },
                ),
            )
            blocked_count = 0
            warning_count = 0
            latest = features.sort_values("trade_date").iloc[-1]
            latest_close = float(latest.get("close", 0.0))
            required_cash = required_cash_for_min_lot(
                price=latest_close,
                lot_size=small_lot,
                commission_rate=self.default_commission_rate,
                min_commission=self.fee_min_commission_cny,
                transfer_fee_rate=self.fee_transfer_rate,
            )
            roundtrip_cost_bps = estimate_roundtrip_cost_bps(
                price=latest_close,
                lot_size=small_lot,
                commission_rate=self.default_commission_rate,
                min_commission=self.fee_min_commission_cny,
                transfer_fee_rate=self.fee_transfer_rate,
                stamp_duty_sell_rate=self.fee_stamp_duty_sell_rate,
                slippage_rate=self.default_slippage_rate,
            )
            small_capital_note: str | None = None
            small_capital_blocked = False
            def _opt_float(value):
                return float(value) if (value is not None and value == value) else None

            latest_tushare_disclosure_risk = _opt_float(latest.get("tushare_disclosure_risk_score"))
            latest_tushare_audit_risk = _opt_float(latest.get("tushare_audit_opinion_risk"))
            latest_tushare_forecast_mid = _opt_float(latest.get("tushare_forecast_pchg_mid"))
            latest_tushare_pledge_ratio = _opt_float(latest.get("tushare_pledge_ratio"))
            latest_tushare_unlock_ratio = _opt_float(latest.get("tushare_share_float_unlock_ratio"))
            latest_tushare_holder_crowding = _opt_float(latest.get("tushare_holder_crowding_ratio"))
            latest_tushare_overhang_risk = _opt_float(latest.get("tushare_overhang_risk_score"))
            for signal in candidates:
                _ = apply_small_capital_overrides(
                    signal=signal,
                    enable_small_capital_mode=small_capital_mode,
                    principal=small_capital_principal,
                    latest_price=latest_close,
                    lot_size=small_lot,
                    commission_rate=self.default_commission_rate,
                    min_commission=self.fee_min_commission_cny,
                    transfer_fee_rate=self.fee_transfer_rate,
                    cash_buffer_ratio=self.small_capital_cash_buffer_ratio,
                    max_single_position=0.50,
                    max_positions=max(1, int(float(strategy_params.get("max_positions", 3)))),
                )
                expected_edge_bps = infer_expected_edge_bps(
                    confidence=float(signal.confidence),
                    momentum20=float(latest.get("momentum20", 0.0)),
                    event_score=float(latest.get("event_score", 0.0)) if "event_score" in latest else None,
                    fundamental_score=float(latest.get("fundamental_score", 0.5))
                    if bool(latest.get("fundamental_available", False))
                    else None,
                )
                risk_req = RiskCheckRequest(
                    signal=signal,
                    is_st=bool(status.get("is_st", False)),
                    is_suspended=bool(status.get("is_suspended", False)),
                    avg_turnover_20d=float(features.iloc[-1].get("turnover20", 0.0)),
                    symbol_industry=req.industry_map.get(symbol),
                    fundamental_score=float(latest.get("fundamental_score", 0.5))
                    if bool(latest.get("fundamental_available", False))
                    else None,
                    fundamental_available=bool(latest.get("fundamental_available", False)),
                    fundamental_pit_ok=bool(latest.get("fundamental_pit_ok", True)),
                    fundamental_stale_days=(
                        int(latest.get("fundamental_stale_days", -1))
                        if int(latest.get("fundamental_stale_days", -1)) >= 0
                        else None
                    ),
                    tushare_disclosure_risk_score=latest_tushare_disclosure_risk,
                    tushare_audit_opinion_risk=latest_tushare_audit_risk,
                    tushare_forecast_pchg_mid=latest_tushare_forecast_mid,
                    tushare_pledge_ratio=latest_tushare_pledge_ratio,
                    tushare_share_float_unlock_ratio=latest_tushare_unlock_ratio,
                    tushare_holder_crowding_ratio=latest_tushare_holder_crowding,
                    tushare_overhang_risk_score=latest_tushare_overhang_risk,
                    enable_small_capital_mode=small_capital_mode,
                    small_capital_principal=small_capital_principal,
                    available_cash=small_capital_principal,
                    latest_price=latest_close if latest_close > 0 else None,
                    lot_size=small_lot,
                    required_cash_for_min_lot=required_cash,
                    estimated_roundtrip_cost_bps=roundtrip_cost_bps,
                    expected_edge_bps=expected_edge_bps,
                    min_expected_edge_bps=float(req.small_capital_min_expected_edge_bps),
                    small_capital_cash_buffer_ratio=self.small_capital_cash_buffer_ratio,
                )
                risk_result = self.risk_engine.evaluate(risk_req)
                _ = self.signal_service.to_trade_prep_sheet(signal, risk_result)
                small_hits_all = [x for x in risk_result.hits if x.rule_name == "small_capital_tradability"]
                small_hits_failed = [x for x in small_hits_all if not x.passed]
                if small_hits_all and small_capital_note is None:
                    small_capital_note = (small_hits_failed[0].message if small_hits_failed else small_hits_all[0].message)
                if any(x.level == SignalLevel.CRITICAL for x in small_hits_failed):
                    small_capital_blocked = True
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
                    fundamental_available=bool(fundamental_stats.get("available", False)),
                    fundamental_score=float(latest.get("fundamental_score", 0.5))
                    if bool(latest.get("fundamental_available", False))
                    else None,
                    fundamental_source=str(fundamental_stats.get("source")) if fundamental_stats.get("source") else None,
                    small_capital_blocked=small_capital_blocked,
                    small_capital_note=small_capital_note,
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
