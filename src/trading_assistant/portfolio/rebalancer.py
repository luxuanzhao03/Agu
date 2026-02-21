from __future__ import annotations

from trading_assistant.core.models import RebalanceOrder, RebalancePlan, RebalanceRequest, SignalAction


class PortfolioRebalancer:
    def build_plan(self, req: RebalanceRequest) -> RebalancePlan:
        current_by_symbol = {
            p.symbol: (p.quantity, p.last_price, (p.quantity * p.last_price) / req.total_equity if req.total_equity > 0 else 0.0)
            for p in req.current_positions
        }

        orders: list[RebalanceOrder] = []
        turnover = 0.0
        target_symbols = {t.symbol for t in req.target_weights}

        # Rebalance target symbols.
        for t in req.target_weights:
            cur_qty, cur_price, cur_weight = current_by_symbol.get(t.symbol, (0, 0.0, 0.0))
            delta = t.weight - cur_weight
            if abs(delta) < 1e-6:
                continue

            ref_price = cur_price if cur_price > 0 else 1.0
            target_value_delta = delta * req.total_equity
            qty = int(abs(target_value_delta) / ref_price / req.lot_size) * req.lot_size
            if qty <= 0:
                continue

            side = SignalAction.BUY if delta > 0 else SignalAction.SELL
            est_notional = qty * ref_price
            turnover += est_notional
            orders.append(
                RebalanceOrder(
                    symbol=t.symbol,
                    side=side,
                    target_weight=round(t.weight, 6),
                    delta_weight=round(delta, 6),
                    quantity=qty,
                    estimated_notional=round(est_notional, 2),
                )
            )

        # Exit symbols not in target.
        for p in req.current_positions:
            if p.symbol in target_symbols:
                continue
            qty = int(p.quantity / req.lot_size) * req.lot_size
            if qty <= 0:
                continue
            est_notional = qty * p.last_price
            turnover += est_notional
            orders.append(
                RebalanceOrder(
                    symbol=p.symbol,
                    side=SignalAction.SELL,
                    target_weight=0.0,
                    delta_weight=round(-((p.quantity * p.last_price) / req.total_equity), 6),
                    quantity=qty,
                    estimated_notional=round(est_notional, 2),
                )
            )

        return RebalancePlan(orders=orders, estimated_turnover=round(turnover, 2))

