from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from trading_assistant.autotune.service import AutoTuneService
from trading_assistant.autotune.store import AutoTuneStore
from trading_assistant.core.models import (
    AutoTuneApplyScope,
    AutoTuneRunRequest,
    BacktestMetrics,
    BacktestResult,
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
            px = px + (0.08 if idx % 5 < 3 else -0.05)
            rows.append(
                {
                    "trade_date": d.date(),
                    "symbol": symbol,
                    "open": px - 0.1,
                    "high": px + 0.2,
                    "low": px - 0.2,
                    "close": px,
                    "volume": 100_000 + idx * 50,
                    "amount": (100_000 + idx * 50) * px,
                }
            )
        return "fake_provider", pd.DataFrame(rows)

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        _ = symbol
        return {"is_st": False, "is_suspended": False}


class FakeBacktestEngine:
    def run(self, bars: pd.DataFrame, req, strategy) -> BacktestResult:
        _ = (bars, strategy)
        fast = float(req.strategy_params.get("entry_ma_fast", 20))
        slow = float(req.strategy_params.get("entry_ma_slow", 60))
        atr = float(req.strategy_params.get("atr_multiplier", 2.0))
        distance = abs(fast - 20.0) * 0.020 + abs(slow - 60.0) * 0.007 + abs(atr - 2.0) * 0.12

        total_return = max(-0.9, 0.34 - distance)
        max_drawdown = min(0.95, 0.09 + distance * 0.5)
        sharpe = max(-1.0, 1.1 - distance * 2.0)
        annualized = max(-1.0, total_return * 1.15)
        metrics = BacktestMetrics(
            total_return=round(total_return, 6),
            max_drawdown=round(max_drawdown, 6),
            trade_count=12,
            win_rate=0.61,
            blocked_signal_count=1,
            annualized_return=round(annualized, 6),
            sharpe=round(sharpe, 6),
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


class FoldVaryingBacktestEngine(FakeBacktestEngine):
    def run(self, bars: pd.DataFrame, req, strategy) -> BacktestResult:
        base = super().run(bars, req, strategy)
        shift = ((req.start_date.toordinal() % 9) - 4) * 0.012
        m = base.metrics
        varied = m.model_copy(
            update={
                "total_return": round(max(-0.95, min(0.95, float(m.total_return) + shift)), 6),
                "annualized_return": round(max(-1.0, min(5.0, float(m.annualized_return) + shift * 0.8)), 6),
            }
        )
        return base.model_copy(update={"metrics": varied})


def _service(tmp_path: Path) -> AutoTuneService:
    gov = StrategyGovernanceService(
        store=StrategyGovernanceStore(str(tmp_path / "strategy_gov.db")),
        required_approval_roles=["risk", "audit"],
        min_approval_count=2,
    )
    return AutoTuneService(
        store=AutoTuneStore(str(tmp_path / "autotune.db")),
        provider=FakeProvider(),  # type: ignore[arg-type]
        backtest_engine=FakeBacktestEngine(),  # type: ignore[arg-type]
        registry=StrategyRegistry(),
        strategy_gov=gov,
        runtime_override_enabled=True,
    )


def test_autotune_run_applies_profile_and_creates_governance_draft(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 1, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 12, "entry_ma_slow": 40, "atr_multiplier": 1.5},
        search_space={
            "entry_ma_fast": [12, 20, 26],
            "entry_ma_slow": [40, 60, 80],
            "atr_multiplier": [1.5, 2.0, 2.5],
        },
        max_combinations=60,
        validation_ratio=0.25,
        min_train_bars=80,
        min_validation_bars=30,
        auto_apply=True,
        apply_scope=AutoTuneApplyScope.SYMBOL,
        min_improvement_to_apply=0.01,
        create_governance_draft=True,
        governance_submit_review=True,
        run_by="qa_autotune",
    )
    out = service.run(req)
    assert out.evaluated_count > 1
    assert out.best is not None
    assert out.best.rank == 1
    assert out.applied is True
    assert out.applied_profile is not None
    assert out.applied_profile.scope == AutoTuneApplyScope.SYMBOL
    assert out.applied_profile.symbol == "000001"
    assert out.governance_draft_id is not None
    assert out.governance_version is not None
    assert (out.improvement_vs_baseline or 0.0) > 0.01

    active = service.get_active_profile(strategy_name="trend_following", symbol="000001")
    assert active is not None
    assert active.id == out.applied_profile.id


def test_autotune_runtime_param_merge_priority(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 10, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 15, "entry_ma_slow": 55, "atr_multiplier": 1.8},
        search_space={"entry_ma_fast": [15, 20], "entry_ma_slow": [55, 60], "atr_multiplier": [1.8, 2.0]},
        max_combinations=20,
        validation_ratio=0.2,
        min_train_bars=60,
        min_validation_bars=20,
        auto_apply=True,
        apply_scope=AutoTuneApplyScope.GLOBAL,
        create_governance_draft=False,
    )
    out = service.run(req)
    assert out.applied is True

    merged, profile = service.resolve_runtime_params(
        strategy_name="trend_following",
        symbol="000001",
        explicit_params={"entry_ma_fast": 33},
        use_profile=True,
    )
    assert profile is not None
    assert merged["entry_ma_fast"] == 33
    assert "entry_ma_slow" in merged
    assert "atr_multiplier" in merged


def test_autotune_apply_guard_blocks_when_validation_return_too_low(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 1, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 15, "entry_ma_slow": 55, "atr_multiplier": 1.8},
        search_space={"entry_ma_fast": [15, 20], "entry_ma_slow": [55, 60], "atr_multiplier": [1.8, 2.0]},
        max_combinations=20,
        validation_ratio=0.25,
        min_train_bars=80,
        min_validation_bars=30,
        auto_apply=True,
        apply_scope=AutoTuneApplyScope.SYMBOL,
        apply_min_validation_total_return=0.50,
        create_governance_draft=False,
    )
    out = service.run(req)
    assert out.best is not None
    assert out.applied is False
    assert out.apply_decision.startswith("guard_blocked:")
    assert out.best.apply_eligible is False
    assert out.best.apply_guard_reason is not None


def test_autotune_walk_forward_low_sample_penalty_applied(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 10, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 15, "entry_ma_slow": 55, "atr_multiplier": 1.8},
        search_space={"entry_ma_fast": [15, 20], "entry_ma_slow": [55, 60], "atr_multiplier": [1.8, 2.0]},
        max_combinations=20,
        validation_ratio=0.20,
        min_train_bars=60,
        min_validation_bars=20,
        auto_apply=False,
        stability_eval_top_n=10,
        walk_forward_slices=1,
        apply_min_walk_forward_samples=2,
        low_sample_penalty=0.2,
        create_governance_draft=False,
    )
    out = service.run(req)
    assert out.best is not None
    assert out.best.walk_forward_samples == 1
    assert out.best.low_sample_penalty >= 0.19


def test_autotune_param_drift_penalty_fields_are_populated(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 1, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 12, "entry_ma_slow": 40, "atr_multiplier": 1.5},
        search_space={
            "entry_ma_fast": [12, 16, 20, 24],
            "entry_ma_slow": [40, 60, 80],
            "atr_multiplier": [1.5, 2.0, 2.5],
        },
        max_combinations=100,
        validation_ratio=0.25,
        min_train_bars=80,
        min_validation_bars=30,
        auto_apply=False,
        walk_forward_slices=4,
        stability_eval_top_n=12,
        objective_weight_param_drift=0.2,
        create_governance_draft=False,
    )
    out = service.run(req)
    assert out.candidates
    assert any(item.param_drift_score is not None for item in out.candidates)
    assert any(item.param_drift_penalty >= 0 for item in out.candidates)


def test_autotune_return_variance_penalty_fields_are_populated(tmp_path: Path) -> None:
    service = _service(tmp_path)
    service.backtest_engine = FoldVaryingBacktestEngine()  # type: ignore[assignment]
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2025, 1, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 12, "entry_ma_slow": 40, "atr_multiplier": 1.5},
        search_space={
            "entry_ma_fast": [12, 16, 20, 24],
            "entry_ma_slow": [40, 60, 80],
            "atr_multiplier": [1.5, 2.0, 2.5],
        },
        max_combinations=100,
        validation_ratio=0.25,
        min_train_bars=80,
        min_validation_bars=30,
        auto_apply=False,
        walk_forward_slices=4,
        stability_eval_top_n=12,
        objective_weight_return_variance=0.5,
        create_governance_draft=False,
    )
    out = service.run(req)
    assert out.candidates
    assert any((item.walk_forward_return_std or 0.0) > 0 for item in out.candidates)
    assert any(item.return_variance_penalty > 0 for item in out.candidates)


def test_autotune_invalid_search_space_value_raises_value_error(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 10, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 15, "entry_ma_slow": 55, "atr_multiplier": 1.8},
        search_space={"entry_ma_fast": ["bad-int"], "entry_ma_slow": [55, 60], "atr_multiplier": [1.8, 2.0]},
        max_combinations=20,
        validation_ratio=0.2,
        min_train_bars=60,
        min_validation_bars=20,
        auto_apply=False,
        create_governance_draft=False,
    )
    try:
        _ = service.run(req)
        assert False, "expected ValueError for invalid int candidate value"
    except ValueError as exc:
        assert "invalid int candidate value" in str(exc)


def test_autotune_profile_rollback_and_rollout_rule(tmp_path: Path) -> None:
    service = _service(tmp_path)
    req = AutoTuneRunRequest(
        symbol="000001",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 10, 31),
        strategy_name="trend_following",
        base_strategy_params={"entry_ma_fast": 15, "entry_ma_slow": 55, "atr_multiplier": 1.8},
        search_space={"entry_ma_fast": [15, 20], "entry_ma_slow": [55, 60], "atr_multiplier": [1.8, 2.0]},
        max_combinations=20,
        validation_ratio=0.2,
        min_train_bars=60,
        min_validation_bars=20,
        auto_apply=True,
        apply_scope=AutoTuneApplyScope.SYMBOL,
        create_governance_draft=False,
    )
    first = service.run(req)
    assert first.applied is True
    first_id = int(first.applied_profile.id) if first.applied_profile else 0
    assert first_id > 0

    second_req = req.model_copy(
        update={
            "base_strategy_params": {"entry_ma_fast": 22, "entry_ma_slow": 80, "atr_multiplier": 2.5},
            "search_space": {"entry_ma_fast": [20, 22], "entry_ma_slow": [70, 80], "atr_multiplier": [2.2, 2.5]},
            "min_improvement_to_apply": -1.0,
        }
    )
    second = service.run(second_req)
    assert second.applied is True
    second_id = int(second.applied_profile.id) if second.applied_profile else 0
    assert second_id > first_id

    rolled = service.rollback_active_profile(
        strategy_name="trend_following",
        symbol="000001",
        scope=AutoTuneApplyScope.SYMBOL,
    )
    assert rolled is not None
    assert rolled.id == first_id
    active = service.get_active_profile(strategy_name="trend_following", symbol="000001")
    assert active is not None
    assert active.id == first_id

    _ = service.upsert_rollout_rule(strategy_name="trend_following", symbol=None, enabled=False, note="gray off")
    merged, profile = service.resolve_runtime_params(
        strategy_name="trend_following",
        symbol="000001",
        explicit_params={"entry_ma_fast": 31},
        use_profile=True,
    )
    assert profile is None
    assert merged == {"entry_ma_fast": 31}

    _ = service.upsert_rollout_rule(strategy_name="trend_following", symbol="000001", enabled=True, note="gray on")
    merged2, profile2 = service.resolve_runtime_params(
        strategy_name="trend_following",
        symbol="000001",
        explicit_params={"entry_ma_fast": 33},
        use_profile=True,
    )
    assert profile2 is not None
    assert merged2["entry_ma_fast"] == 33
