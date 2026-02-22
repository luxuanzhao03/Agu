from trading_assistant.trading.costs import (
    calc_side_fee,
    estimate_fill_probability,
    estimate_market_impact_rate,
    estimate_roundtrip_cost_bps,
    filled_quantity_by_probability,
    required_cash_for_min_lot,
    tiered_slippage_rate,
)
from trading_assistant.core.models import SignalAction


def test_side_fee_respects_min_commission() -> None:
    fee = calc_side_fee(
        notional=1000,
        commission_rate=0.0003,
        min_commission=5,
        transfer_fee_rate=0.00001,
        stamp_duty_sell_rate=0.0005,
        is_sell=False,
    )
    assert fee >= 5.0


def test_roundtrip_cost_for_small_lot_is_high() -> None:
    bps = estimate_roundtrip_cost_bps(
        price=10,
        lot_size=100,
        commission_rate=0.0003,
        min_commission=5,
        transfer_fee_rate=0.00001,
        stamp_duty_sell_rate=0.0005,
        slippage_rate=0.0005,
    )
    assert bps > 80


def test_required_cash_for_min_lot_includes_fee() -> None:
    needed = required_cash_for_min_lot(
        price=9.8,
        lot_size=100,
        commission_rate=0.0003,
        min_commission=5,
        transfer_fee_rate=0.00001,
    )
    assert needed > 980


def test_tiered_slippage_and_impact_increase_with_participation() -> None:
    small = tiered_slippage_rate(order_notional=100_000, avg_turnover_20d=80_000_000, base_slippage_rate=0.0005)
    large = tiered_slippage_rate(order_notional=5_000_000, avg_turnover_20d=80_000_000, base_slippage_rate=0.0005)
    assert large > small

    impact_small = estimate_market_impact_rate(
        order_notional=100_000,
        avg_turnover_20d=80_000_000,
        impact_coeff=0.18,
        impact_exponent=0.6,
    )
    impact_large = estimate_market_impact_rate(
        order_notional=5_000_000,
        avg_turnover_20d=80_000_000,
        impact_coeff=0.18,
        impact_exponent=0.6,
    )
    assert impact_large > impact_small


def test_fill_probability_and_partial_fill() -> None:
    prob_limit = estimate_fill_probability(
        side=SignalAction.BUY,
        is_suspended=False,
        at_limit_up=True,
        avg_turnover_20d=20_000_000,
        order_notional=1_000_000,
        probability_floor=0.02,
    )
    prob_normal = estimate_fill_probability(
        side=SignalAction.BUY,
        is_suspended=False,
        at_limit_up=False,
        avg_turnover_20d=20_000_000,
        order_notional=100_000,
        probability_floor=0.02,
    )
    assert prob_normal >= prob_limit
    qty = filled_quantity_by_probability(desired_qty=2000, lot_size=100, fill_probability=0.35)
    assert qty % 100 == 0
    assert 0 <= qty <= 2000
