from __future__ import annotations

from trading_assistant.core.models import SignalAction, SignalCandidate
from trading_assistant.trading.costs import required_cash_for_min_lot


def apply_small_capital_overrides(
    *,
    signal: SignalCandidate,
    enable_small_capital_mode: bool,
    principal: float,
    latest_price: float,
    lot_size: int,
    commission_rate: float,
    min_commission: float,
    transfer_fee_rate: float,
    cash_buffer_ratio: float,
    max_single_position: float,
    max_positions: int = 3,
) -> str | None:
    """Apply affordability-aware overrides to BUY signals for small accounts."""
    if (not enable_small_capital_mode) or signal.action != SignalAction.BUY:
        return None
    if principal <= 0 or latest_price <= 0 or lot_size <= 0:
        return "Small-capital override skipped due to invalid principal/price/lot_size."

    usable_cash = float(principal) * max(0.0, 1.0 - float(cash_buffer_ratio))
    min_lot_cash = required_cash_for_min_lot(
        price=latest_price,
        lot_size=lot_size,
        commission_rate=commission_rate,
        min_commission=min_commission,
        transfer_fee_rate=transfer_fee_rate,
    )
    min_lot_position = float(latest_price) * int(lot_size) / float(principal)

    if min_lot_cash > usable_cash:
        signal.action = SignalAction.WATCH
        signal.suggested_position = None
        signal.reason = (
            f"{signal.reason} [small-capital override] Not enough usable cash for one lot: "
            f"{usable_cash:.2f} < {min_lot_cash:.2f}."
        )
        signal.metadata["small_capital_override"] = "downgraded_not_affordable"
        return signal.reason

    if min_lot_position > float(max_single_position):
        signal.action = SignalAction.WATCH
        signal.suggested_position = None
        signal.reason = (
            f"{signal.reason} [small-capital override] One-lot position ratio {min_lot_position:.2%} "
            f"exceeds max_single_position {float(max_single_position):.2%}."
        )
        signal.metadata["small_capital_override"] = "downgraded_over_concentrated"
        return signal.reason

    budget_position = usable_cash / max(1, int(max_positions)) / float(principal)
    suggested = signal.suggested_position if signal.suggested_position is not None else budget_position
    suggested = max(float(suggested), min_lot_position)
    suggested = min(float(max_single_position), suggested)
    signal.suggested_position = round(suggested, 4)
    signal.metadata["small_capital_override"] = "position_adjusted"
    signal.metadata["small_capital_min_lot_position"] = round(min_lot_position, 5)
    signal.metadata["small_capital_budget_position"] = round(budget_position, 5)
    return None
