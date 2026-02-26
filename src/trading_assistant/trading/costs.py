from __future__ import annotations

import math

from trading_assistant.core.models import SignalAction


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
    if momentum20 is not None and math.isfinite(float(momentum20)):
        base += max(-80.0, min(120.0, float(momentum20) * 300.0))
    if event_score is not None and math.isfinite(float(event_score)):
        base += max(0.0, min(80.0, (float(event_score) - 0.5) * 200.0))
    if fundamental_score is not None and math.isfinite(float(fundamental_score)):
        base += max(-40.0, min(60.0, (float(fundamental_score) - 0.5) * 120.0))
    return max(0.0, base)


def tiered_slippage_rate(
    *,
    order_notional: float,
    avg_turnover_20d: float | None,
    base_slippage_rate: float,
) -> float:
    """
    Piece-wise slippage uplift by order participation ratio.
    """
    base = max(0.0, float(base_slippage_rate))
    adv = float(avg_turnover_20d or 0.0)
    if order_notional <= 0 or adv <= 0:
        return base

    ratio = float(order_notional) / max(adv, 1.0)
    # Each tier adds extra slippage in rate terms (1bp = 0.0001).
    if ratio <= 0.005:
        uplift = 0.0
    elif ratio <= 0.015:
        uplift = 0.0002
    elif ratio <= 0.03:
        uplift = 0.0005
    elif ratio <= 0.06:
        uplift = 0.0010
    else:
        uplift = 0.0020
    return base + uplift


def estimate_market_impact_rate(
    *,
    order_notional: float,
    avg_turnover_20d: float | None,
    impact_coeff: float,
    impact_exponent: float,
) -> float:
    """
    Square-root like impact model: impact = coeff * participation^exponent.
    """
    adv = float(avg_turnover_20d or 0.0)
    if order_notional <= 0 or adv <= 0:
        return 0.0
    ratio = max(0.0, float(order_notional) / max(adv, 1.0))
    coeff = max(0.0, float(impact_coeff))
    exponent = max(0.1, min(2.0, float(impact_exponent)))
    return coeff * (ratio**exponent) * 0.001


def estimate_fill_probability(
    *,
    side: SignalAction,
    is_suspended: bool,
    at_limit_up: bool = False,
    at_limit_down: bool = False,
    is_one_word_limit_up: bool = False,
    is_one_word_limit_down: bool = False,
    avg_turnover_20d: float | None = None,
    order_notional: float | None = None,
    probability_floor: float = 0.02,
) -> float:
    floor = max(0.0, min(1.0, float(probability_floor)))
    if is_suspended:
        return 0.0
    if side == SignalAction.BUY and is_one_word_limit_up:
        return floor
    if side == SignalAction.SELL and is_one_word_limit_down:
        return floor
    if side == SignalAction.BUY and at_limit_up:
        return max(floor, 0.15)
    if side == SignalAction.SELL and at_limit_down:
        return max(floor, 0.15)

    adv = float(avg_turnover_20d or 0.0)
    notional = float(order_notional or 0.0)
    if adv <= 0 or notional <= 0:
        return 1.0
    participation = max(0.0, notional / max(adv, 1.0))
    # Logistic decay on participation ratio.
    prob = 1.0 / (1.0 + math.exp(18.0 * (participation - 0.035)))
    return max(floor, min(1.0, prob))


def filled_quantity_by_probability(*, desired_qty: int, lot_size: int, fill_probability: float) -> int:
    if desired_qty <= 0 or lot_size <= 0:
        return 0
    prob = max(0.0, min(1.0, float(fill_probability)))
    filled = int((desired_qty * prob) // lot_size) * lot_size
    return max(0, min(int(desired_qty), filled))
