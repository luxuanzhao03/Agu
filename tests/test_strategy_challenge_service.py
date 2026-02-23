from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.autotune.store import AutoTuneStore
from trading_assistant.challenge.service import StrategyChallengeService
from trading_assistant.core.models import (
    BacktestMetrics,
    BacktestResult,
    StrategyChallengeRequest,
    StrategyChallengeRunStatus,
)
from trading_assistant.strategy.governance_service import StrategyGovernanceService
from trading_assistant.strategy.governance_store import StrategyGovernanceStore
from trading_assistant.strategy.registry import StrategyRegistry


class FakeProvider:
    def get_daily_bars_with_source(self, symbol: str, start_date: date, end_date: date):
        dates = pd.date_range(start=start_date, end=end_date, freq="B")
        rows = []
        px = 10.0
        for idx, d in enumerate(dates):
            px = px + (0.05 if idx % 6 < 4 else -0.03)
            volume = 120_000 + idx * 40
            rows.append(
                {
                    "trade_date": d.date(),
                    "symbol": symbol,
                    "open": px - 0.12,
                    "high": px + 0.18,
                    "low": px - 0.16,
                    "close": px,
                    "volume": volume,
                    "amount": volume * px,
                }
            )
        return "fake_provider", pd.DataFrame(rows)

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}


class StrategyAwareBacktestEngine:
    _base_return = {
        "trend_following": 0.23,
        "mean_reversion": 0.20,
        "multi_factor": 0.18,
        "sector_rotation": 0.15,
        "event_driven": 0.13,
        "small_capital_adaptive": 0.17,
    }
    _target = {
        "trend_following": {"entry_ma_fast": 20.0, "entry_ma_slow": 60.0, "atr_multiplier": 2.0},
        "mean_reversion": {"z_enter": 2.0, "z_exit": 0.0, "min_turnover": 5_000_000.0},
        "multi_factor": {"buy_threshold": 0.55, "sell_threshold": 0.35},
        "sector_rotation": {"sector_strength": 0.60, "risk_off_strength": 0.50},
        "event_driven": {"event_score": 0.70, "negative_event_score": 0.60},
        "small_capital_adaptive": {"buy_threshold": 0.64, "sell_threshold": 0.34, "max_positions": 3.0},
    }

    def run(self, bars: pd.DataFrame, req, strategy) -> BacktestResult:
        _ = strategy
        base = float(self._base_return.get(req.strategy_name, 0.12))
        target = self._target.get(req.strategy_name, {})

        distance = 0.0
        for key, center in target.items():
            val = float(req.strategy_params.get(key, center))
            denom = abs(center) + 1e-6
            distance += min(1.0, abs(val - center) / denom)
        if target:
            distance /= float(len(target))

        fold_shift = ((req.start_date.toordinal() % 9) - 4) * 0.003
        sample_scale = 0.94 if len(bars) < 90 else 1.0
        total_return = base * sample_scale + fold_shift - distance * 0.24
        max_drawdown = 0.11 + distance * 0.22 + max(0.0, -fold_shift * 1.3)
        sharpe = 1.05 + base - distance * 1.2 + fold_shift * 3.0
        annualized = total_return * 1.20
        trade_count = 18

        metrics = BacktestMetrics(
            total_return=round(float(max(-0.9, min(2.0, total_return))), 6),
            max_drawdown=round(float(max(0.01, min(0.95, max_drawdown))), 6),
            trade_count=trade_count,
            win_rate=0.59,
            blocked_signal_count=1,
            annualized_return=round(float(max(-1.0, min(5.0, annualized))), 6),
            sharpe=round(float(max(-5.0, min(8.0, sharpe))), 6),
        )
        return BacktestResult(
            symbol=req.symbol,
            strategy_name=req.strategy_name,
            start_date=req.start_date,
            end_date=req.end_date,
            metrics=metrics,
            trades=[],
            equity_curve=[],
        )


def _service(tmp_path: Path) -> StrategyChallengeService:
    provider = FakeProvider()
    engine = StrategyAwareBacktestEngine()
    registry = StrategyRegistry()
    gov = StrategyGovernanceService(
        store=StrategyGovernanceStore(str(tmp_path / "strategy_gov.db")),
        required_approval_roles=["risk", "audit"],
        min_approval_count=2,
    )
    autotune = AutoTuneService(
        store=AutoTuneStore(str(tmp_path / "autotune.db")),
        provider=provider,  # type: ignore[arg-type]
        backtest_engine=engine,  # type: ignore[arg-type]
        registry=registry,
        strategy_gov=gov,
        runtime_override_enabled=True,
    )
    return StrategyChallengeService(
        autotune=autotune,
        provider=provider,  # type: ignore[arg-type]
        backtest_engine=engine,  # type: ignore[arg-type]
        registry=registry,
    )


def test_strategy_challenge_run_returns_champion_and_rollout_plan(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = StrategyChallengeRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 12, 31),
        strategy_names=["trend_following", "mean_reversion", "multi_factor"],
        per_strategy_max_combinations=36,
        validation_ratio=0.25,
        min_train_bars=90,
        min_validation_bars=30,
        gate_require_validation=True,
        gate_min_validation_total_return=0.01,
        gate_max_validation_drawdown=0.45,
        gate_min_validation_sharpe=0.0,
        gate_min_validation_trade_count=1,
        gate_min_walk_forward_samples=2,
        gate_max_walk_forward_return_std=0.40,
        rollout_gray_days=10,
    )
    out = service.run(req)
    assert out.results
    assert out.evaluated_count > 0
    assert out.qualified_count >= 1
    assert out.champion_strategy is not None
    assert out.rollout_plan is not None
    assert out.rollout_plan.enabled is True
    assert out.rollout_plan.strategy_name == out.champion_strategy
    assert len(out.results) == 3
    assert out.results[0].qualified is True
    assert out.results[0].ranking_score is not None
    assert out.run_status == StrategyChallengeRunStatus.SUCCESS
    assert out.error_count == 0
    assert out.failed_strategies == []


def test_strategy_challenge_returns_no_champion_when_gate_is_too_strict(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = StrategyChallengeRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 12, 31),
        strategy_names=["trend_following", "mean_reversion"],
        per_strategy_max_combinations=24,
        gate_require_validation=True,
        gate_min_validation_total_return=0.60,
        gate_max_validation_drawdown=0.05,
        gate_min_validation_sharpe=3.0,
        gate_min_walk_forward_samples=4,
        gate_max_walk_forward_return_std=0.05,
    )
    out = service.run(req)
    assert out.qualified_count == 0
    assert out.champion_strategy is None
    assert out.runner_up_strategy is None
    assert out.rollout_plan is not None
    assert out.rollout_plan.enabled is False
    assert out.rollout_plan.rollback_triggers
    assert out.run_status == StrategyChallengeRunStatus.SUCCESS
    assert out.error_count == 0


def test_strategy_challenge_partial_failed_status(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = StrategyChallengeRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 12, 31),
        strategy_names=["trend_following", "mean_reversion"],
        search_space_map={
            "mean_reversion": {
                "z_enter": ["bad-float"],
            }
        },
        per_strategy_max_combinations=20,
    )
    out = service.run(req)
    assert out.run_status == StrategyChallengeRunStatus.PARTIAL_FAILED
    assert out.error_count == 1
    assert "mean_reversion" in out.failed_strategies


def test_strategy_challenge_uses_parallel_path_when_enabled(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.max_parallel_workers = 2
    req = StrategyChallengeRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 12, 31),
        strategy_names=["trend_following", "mean_reversion"],
    )

    called = {"parallel": False}

    def _fake_parallel(*, req: StrategyChallengeRequest, strategy_names: list[str]):
        _ = (req, strategy_names)
        called["parallel"] = True
        return [], 0

    service._evaluate_strategies_parallel = _fake_parallel  # type: ignore[method-assign]
    results, evaluated = service._evaluate_strategies(req=req, strategy_names=req.strategy_names)
    assert called["parallel"] is True
    assert results == []
    assert evaluated == 0


def test_strategy_challenge_uses_sequential_path_when_disabled(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.max_parallel_workers = 1
    req = StrategyChallengeRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 12, 31),
        strategy_names=["trend_following", "mean_reversion"],
    )

    called = {"sequential": False}

    def _fake_sequential(*, req: StrategyChallengeRequest, strategy_names: list[str]):
        _ = (req, strategy_names)
        called["sequential"] = True
        return [], 0

    service._evaluate_strategies_sequential = _fake_sequential  # type: ignore[method-assign]
    results, evaluated = service._evaluate_strategies(req=req, strategy_names=req.strategy_names)
    assert called["sequential"] is True
    assert results == []
    assert evaluated == 0
