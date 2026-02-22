from __future__ import annotations

from datetime import date, datetime

from trading_assistant.core.models import (
    ExecutionAttributionItem,
    ExecutionAttributionReport,
    ExecutionRecordCreate,
    ExecutionReplayItem,
    ExecutionReplayReport,
    SignalLevel,
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

    def attribution(
        self,
        symbol: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int = 500,
    ) -> ExecutionAttributionReport:
        replay = self.report(symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
        reason_counts: dict[str, int] = {}
        items: list[ExecutionAttributionItem] = []

        def _add(reason_code: str, item: ExecutionReplayItem, detail: str, suggestion: str, severity: SignalLevel) -> None:
            reason_counts[reason_code] = reason_counts.get(reason_code, 0) + 1
            items.append(
                ExecutionAttributionItem(
                    signal_id=item.signal_id,
                    symbol=item.symbol,
                    reason_code=reason_code,
                    severity=severity,
                    detail=detail,
                    suggestion=suggestion,
                )
            )

        for item in replay.items:
            if item.executed_quantity <= 0:
                _add(
                    "NO_EXECUTION",
                    item,
                    "Signal has no manual execution record.",
                    "提高执行跟单覆盖率；必要时降低单笔最小交易手门槛或改为分批下单。",
                    SignalLevel.WARNING,
                )
                continue
            if not item.followed:
                _add(
                    "ACTION_MISMATCH",
                    item,
                    f"Signal={item.signal_action.value}, execution={item.executed_action.value if item.executed_action else 'NONE'}.",
                    "复核执行SOP与策略动作映射，避免 BUY/SELL 方向偏差。",
                    SignalLevel.WARNING,
                )
            if item.delay_days >= 2:
                _add(
                    "EXECUTION_DELAY",
                    item,
                    f"Execution delayed by {item.delay_days} days.",
                    "缩短人工审批链路，或将策略参数改为更耐延迟的持有周期。",
                    SignalLevel.WARNING,
                )
            if abs(float(item.slippage_bps)) >= 35:
                _add(
                    "HIGH_SLIPPAGE",
                    item,
                    f"Observed slippage={item.slippage_bps:.1f} bps.",
                    "提高流动性门槛并调高回测滑点/冲击成本参数。",
                    SignalLevel.WARNING,
                )

        suggestions: list[str] = []
        total = max(1, len(replay.items))
        no_exec_ratio = reason_counts.get("NO_EXECUTION", 0) / total
        mismatch_ratio = reason_counts.get("ACTION_MISMATCH", 0) / total
        delay_ratio = reason_counts.get("EXECUTION_DELAY", 0) / total
        if no_exec_ratio >= 0.30:
            suggestions.append("执行覆盖不足：建议降低信号密度，优先跟踪高置信度信号。")
        if mismatch_ratio >= 0.15:
            suggestions.append("动作偏差偏高：建议收紧买卖阈值，减少边界信号翻转。")
        if delay_ratio >= 0.20 or replay.avg_delay_days >= 1.0:
            suggestions.append("执行延迟偏高：建议增加持有期容错并降低短周期换手。")
        if replay.follow_rate <= 0.60:
            suggestions.append("整体跟随率偏低：建议下调 max_positions 或提升单票置信度门槛。")
        if not suggestions:
            suggestions.append("执行偏差可控：建议保持参数，按周滚动复核。")

        return ExecutionAttributionReport(
            sample_size=len(replay.items),
            follow_rate=replay.follow_rate,
            avg_delay_days=replay.avg_delay_days,
            avg_slippage_bps=replay.avg_slippage_bps,
            reason_counts=reason_counts,
            suggestions=suggestions,
            items=items,
        )
