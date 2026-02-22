from trading_assistant.trading.costs import (
    calc_side_fee,
    estimate_roundtrip_cost_bps,
    required_cash_for_min_lot,
)


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

