from __future__ import annotations


def calc_commission(notional: float, rate: float, min_commission: float) -> float:
    if notional <= 0:
        return 0.0
    return max(float(min_commission), float(notional) * float(rate))


def calc_transfer_fee(notional: float, rate: float) -> float:
    if notional <= 0:
        return 0.0
    return float(notional) * float(rate)


def calc_stamp_duty(notional: float, sell_rate: float, is_sell: bool) -> float:
    if (notional <= 0) or (not is_sell):
        return 0.0
    return float(notional) * float(sell_rate)


def calc_side_fee(
    *,
    notional: float,
    commission_rate: float,
    min_commission: float,
    transfer_fee_rate: float,
    stamp_duty_sell_rate: float,
    is_sell: bool,
) -> float:
    if notional <= 0:
        return 0.0
    return (
        calc_commission(notional=notional, rate=commission_rate, min_commission=min_commission)
        + calc_transfer_fee(notional=notional, rate=transfer_fee_rate)
        + calc_stamp_duty(notional=notional, sell_rate=stamp_duty_sell_rate, is_sell=is_sell)
    )


def estimate_roundtrip_cost_bps(
    *,
    price: float,
    lot_size: int,
    commission_rate: float,
    min_commission: float,
    transfer_fee_rate: float,
    stamp_duty_sell_rate: float,
    slippage_rate: float,
) -> float:
    if price <= 0 or lot_size <= 0:
        return 0.0
    notional = float(price) * int(lot_size)
    buy_fee = calc_side_fee(
        notional=notional,
        commission_rate=commission_rate,
        min_commission=min_commission,
        transfer_fee_rate=transfer_fee_rate,
        stamp_duty_sell_rate=stamp_duty_sell_rate,
        is_sell=False,
    )
    sell_fee = calc_side_fee(
        notional=notional,
        commission_rate=commission_rate,
        min_commission=min_commission,
        transfer_fee_rate=transfer_fee_rate,
        stamp_duty_sell_rate=stamp_duty_sell_rate,
        is_sell=True,
    )
    slip_cost = notional * max(0.0, float(slippage_rate)) * 2.0
    total = buy_fee + sell_fee + slip_cost
    if notional <= 0:
        return 0.0
    return total / notional * 10000.0


def required_cash_for_min_lot(
    *,
    price: float,
    lot_size: int,
    commission_rate: float,
    min_commission: float,
    transfer_fee_rate: float,
) -> float:
    if price <= 0 or lot_size <= 0:
        return 0.0
    notional = float(price) * int(lot_size)
    buy_fee = calc_side_fee(
        notional=notional,
        commission_rate=commission_rate,
        min_commission=min_commission,
        transfer_fee_rate=transfer_fee_rate,
        stamp_duty_sell_rate=0.0,
        is_sell=False,
    )
    return notional + buy_fee


def infer_expected_edge_bps(
    *,
    confidence: float,
    momentum20: float | None = None,
    event_score: float | None = None,
    fundamental_score: float | None = None,
) -> float:
    c = max(0.0, min(1.0, float(confidence)))
    base = max(0.0, (c - 0.5) * 400.0)
    if momentum20 is not None:
        base += max(-80.0, min(120.0, float(momentum20) * 300.0))
    if event_score is not None:
        base += max(0.0, min(80.0, (float(event_score) - 0.5) * 200.0))
    if fundamental_score is not None:
        base += max(-40.0, min(60.0, (float(fundamental_score) - 0.5) * 120.0))
    return max(0.0, base)

