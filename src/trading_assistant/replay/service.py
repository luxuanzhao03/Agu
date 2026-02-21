from __future__ import annotations

from datetime import date, datetime

from trading_assistant.core.models import (
    ExecutionRecordCreate,
    ExecutionReplayItem,
    ExecutionReplayReport,
    SignalDecisionRecord,
)
from trading_assistant.replay.store import ReplayStore


class ReplayService:
    def __init__(self, store: ReplayStore) -> None:
        self.store = store

    def record_signal(self, record: SignalDecisionRecord) -> str:
        return self.store.record_signal(record)

    def record_execution(self, record: ExecutionRecordCreate) -> int:
        if not self.store.signal_exists(record.signal_id):
            raise KeyError(f"signal_id '{record.signal_id}' not found")
        return self.store.record_execution(record)

    def list_signals(self, symbol: str | None = None, limit: int = 200) -> list[SignalDecisionRecord]:
        return self.store.list_signals(symbol=symbol, limit=limit)

    def report(
        self,
        symbol: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> ExecutionReplayReport:
        rows = self.store.load_pairs(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
        items: list[ExecutionReplayItem] = []
        followed_count = 0
        slippage_sum = 0.0
        slippage_count = 0
        delay_sum = 0
        delay_count = 0

        for row in rows:
            signal_id = str(row["signal_id"])
            signal_action = self.store.parse_action(str(row["signal_action"]))
            executed_action = self.store.parse_action(str(row["executed_action"])) if row["executed_action"] else None
            confidence = float(row["confidence"])
            quantity = int(row["quantity"] or 0)
            executed_price = float(row["price"] or 0.0)
            signal_date = datetime.fromisoformat(str(row["trade_date"])).date()
            exec_date = datetime.fromisoformat(str(row["execution_date"])).date() if row["execution_date"] else signal_date

            followed = signal_action == executed_action and quantity > 0
            if followed:
                followed_count += 1

            # Without reference execution price from order book, we use 0 baseline slippage.
            slippage_bps = 0.0
            if quantity > 0:
                slippage_sum += slippage_bps
                slippage_count += 1

            delay_days = max(0, (exec_date - signal_date).days) if quantity > 0 else 0
            if quantity > 0:
                delay_sum += delay_days
                delay_count += 1

            items.append(
                ExecutionReplayItem(
                    signal_id=signal_id,
                    symbol=str(row["symbol"]),
                    signal_action=signal_action,
                    executed_action=executed_action,
                    signal_confidence=confidence,
                    executed_quantity=quantity,
                    executed_price=executed_price,
                    slippage_bps=slippage_bps,
                    followed=followed,
                    delay_days=delay_days,
                )
            )

        total = len(items)
        follow_rate = 0.0 if total == 0 else followed_count / total
        avg_slippage_bps = 0.0 if slippage_count == 0 else slippage_sum / slippage_count
        avg_delay_days = 0.0 if delay_count == 0 else delay_sum / delay_count
        return ExecutionReplayReport(
            items=items,
            follow_rate=round(follow_rate, 6),
            avg_slippage_bps=round(avg_slippage_bps, 6),
            avg_delay_days=round(avg_delay_days, 6),
        )
