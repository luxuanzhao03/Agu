from datetime import date

from trading_assistant.core.models import PortfolioSnapshot, Position, RiskCheckRequest, SignalAction, SignalCandidate, SignalLevel
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

