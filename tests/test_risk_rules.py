from datetime import date

from trading_assistant.core.models import (
    PortfolioRiskRequest,
    PortfolioSnapshot,
    Position,
    RiskCheckRequest,
    SignalAction,
    SignalCandidate,
    SignalLevel,
)
from trading_assistant.risk.engine import RiskEngine


def build_engine() -> RiskEngine:
    return RiskEngine(
        max_single_position=0.05,
        max_drawdown=0.12,
        max_industry_exposure=0.2,
        min_turnover_20d=5_000_000,
    )


def build_signal(action: SignalAction, pos: float | None = 0.05) -> SignalCandidate:
    return SignalCandidate(
        symbol="000001",
        trade_date=date(2025, 1, 2),
        action=action,
        confidence=0.8,
        reason="test",
        suggested_position=pos,
    )


def test_t_plus_one_blocks_sell_without_available_quantity() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.SELL),
        position=Position(symbol="000001", quantity=100, available_quantity=0, avg_cost=10),
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_st_blocks_buy() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        is_st=True,
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_drawdown_blocks_buy() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        portfolio=PortfolioSnapshot(total_value=88, cash=10, peak_value=100, current_drawdown=0.12),
    )
    result = engine.evaluate(req)
    assert result.blocked is False

    req2 = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        portfolio=PortfolioSnapshot(total_value=70, cash=10, peak_value=100, current_drawdown=0.30),
    )
    result2 = engine.evaluate(req2)
    assert result2.blocked is True
    assert result2.level == SignalLevel.CRITICAL


def test_liquidity_warning_does_not_block() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        avg_turnover_20d=1_000_000,
    )
    result = engine.evaluate(req)
    assert result.blocked is False
    assert result.level == SignalLevel.WARNING


def test_fundamental_critical_blocks_buy() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        avg_turnover_20d=20_000_000,
        fundamental_score=0.20,
        fundamental_available=True,
        fundamental_pit_ok=True,
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_fundamental_warning_keeps_manual_confirmation() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        avg_turnover_20d=20_000_000,
        fundamental_score=0.45,
        fundamental_available=True,
        fundamental_pit_ok=True,
    )
    result = engine.evaluate(req)
    assert result.blocked is False
    assert result.level == SignalLevel.WARNING


def test_fundamental_pit_failure_blocks_buy() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        avg_turnover_20d=20_000_000,
        fundamental_score=0.80,
        fundamental_available=True,
        fundamental_pit_ok=False,
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_small_capital_not_tradable_blocks_buy() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        enable_small_capital_mode=True,
        small_capital_principal=2000,
        available_cash=2000,
        latest_price=30,
        lot_size=100,
        required_cash_for_min_lot=3010,
        estimated_roundtrip_cost_bps=60,
        expected_edge_bps=150,
        min_expected_edge_bps=80,
        small_capital_cash_buffer_ratio=0.1,
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_small_capital_edge_below_cost_warns() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        enable_small_capital_mode=True,
        small_capital_principal=2000,
        available_cash=2000,
        latest_price=10,
        lot_size=100,
        required_cash_for_min_lot=1006,
        estimated_roundtrip_cost_bps=95,
        expected_edge_bps=120,
        min_expected_edge_bps=80,
        small_capital_cash_buffer_ratio=0.1,
    )
    result = engine.evaluate(req)
    assert result.blocked is False
    assert result.level == SignalLevel.WARNING


def test_tushare_disclosure_risk_blocks_buy() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        avg_turnover_20d=20_000_000,
        fundamental_score=0.65,
        fundamental_available=True,
        tushare_disclosure_risk_score=0.92,
        tushare_audit_opinion_risk=0.95,
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_small_cap_tushare_overhang_blocks_on_high_pledge() -> None:
    engine = build_engine()
    req = RiskCheckRequest(
        signal=build_signal(SignalAction.BUY),
        enable_small_capital_mode=True,
        small_capital_principal=5000,
        available_cash=5000,
        latest_price=12.0,
        lot_size=100,
        required_cash_for_min_lot=1206,
        estimated_roundtrip_cost_bps=65,
        expected_edge_bps=220,
        min_expected_edge_bps=80,
        avg_turnover_20d=20_000_000,
        fundamental_score=0.68,
        fundamental_available=True,
        tushare_pledge_ratio=58.0,
        tushare_overhang_risk_score=0.82,
    )
    result = engine.evaluate(req)
    assert result.blocked is True
    assert result.level == SignalLevel.CRITICAL


def test_portfolio_risk_var_es_and_loss_circuit() -> None:
    engine = build_engine()
    result = engine.evaluate_portfolio(
        PortfolioRiskRequest(
            portfolio=PortfolioSnapshot(
                total_value=100,
                cash=20,
                peak_value=120,
                current_drawdown=0.1667,
                industry_exposure={"TECH": 0.25},
                theme_exposure={"AI": 0.34},
            ),
            pending_signal=None,
            max_drawdown=0.25,
            max_industry_exposure=0.2,
            max_theme_exposure=0.3,
            daily_returns=[-0.01, -0.03, -0.04, -0.05],
            recent_trade_pnls=[-1, -2, -1, -3],
            max_consecutive_losses=3,
            max_daily_loss=0.03,
            var_confidence=0.95,
            max_var=0.03,
            max_es=0.04,
        )
    )
    assert result.var_value is not None
    assert result.es_value is not None
    assert result.level in {SignalLevel.WARNING, SignalLevel.CRITICAL}
    assert any(hit.rule_name in {"loss_circuit_breaker", "portfolio_es"} for hit in result.hits)
