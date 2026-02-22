from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import math

import pandas as pd

from trading_assistant.core.models import (
    OptimizeCandidate,
    PortfolioBacktestMetrics,
    PortfolioBacktestRequest,
    PortfolioBacktestResult,
    PortfolioRiskRequest,
    PortfolioSnapshot,
    PortfolioBacktestTrade,
    PortfolioEquityPoint,
    PortfolioOptimizeRequest,
    SignalAction,
    SignalLevel,
)
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import BaseStrategy, StrategyContext
from trading_assistant.trading.costs import (
    calc_side_fee,
    estimate_fill_probability,
    estimate_market_impact_rate,
    filled_quantity_by_probability,
    tiered_slippage_rate,
)


@dataclass
class _Holding:
    qty: int = 0
    avg_cost: float = 0.0


class PortfolioBacktestEngine:
    def __init__(
        self,
        *,
        factor_engine: FactorEngine,
        optimizer: PortfolioOptimizer,
        risk_engine: RiskEngine | None = None,
    ) -> None:
        self.factor_engine = factor_engine
        self.optimizer = optimizer
        self.risk_engine = risk_engine

    def run(
        self,
        *,
        bars_by_symbol: dict[str, pd.DataFrame],
        req: PortfolioBacktestRequest,
        strategy: BaseStrategy,
        params_by_symbol: dict[str, dict[str, float | int | str | bool]] | None = None,
    ) -> PortfolioBacktestResult:
        normalized = self._normalize_bars(bars_by_symbol)
        calendar = self._build_calendar(normalized)
        if not calendar:
            return PortfolioBacktestResult(
                strategy_name=req.strategy_name,
                symbols=list(req.symbols),
                start_date=req.start_date,
                end_date=req.end_date,
                metrics=PortfolioBacktestMetrics(
                    total_return=0.0,
                    annualized_return=0.0,
                    max_drawdown=0.0,
                    sharpe=0.0,
                    trade_count=0,
                    avg_utilization=0.0,
                    avg_cash_ratio=1.0,
                ),
                trades=[],
                equity_curve=[],
            )

        holdings: dict[str, _Holding] = {symbol: _Holding() for symbol in req.symbols}
        last_price: dict[str, float] = {}
        feature_cache: dict[str, pd.DataFrame] = {}
        latest_turnover: dict[str, float] = {}
        latest_flags: dict[str, dict[str, bool]] = {}

        cash = float(req.initial_cash)
        peak = float(req.initial_cash)
        trades: list[PortfolioBacktestTrade] = []
        equity_curve: list[PortfolioEquityPoint] = []
        industry_breach_count = 0
        theme_breach_count = 0
        risk_blocked_days = 0
        risk_warning_days = 0
        daily_returns: list[float] = []
        recent_trade_pnls: list[float] = []
        prev_equity = float(req.initial_cash)

        gross_target = max(0.0, min(req.target_gross_exposure, 1.0 - req.cash_reserve_ratio))
        rebalance_step = max(1, int(req.rebalance_interval_days))

        for idx, trade_day in enumerate(calendar):
            # Mark-to-market update.
            for symbol, frame in normalized.items():
                row = frame.get(trade_day)
                if row is None:
                    continue
                close = float(row.get("close", 0.0) or 0.0)
                if close > 0:
                    last_price[symbol] = close

            pre_exposure, pre_industry_exposure, pre_theme_exposure = self._exposure(
                holdings=holdings,
                last_price=last_price,
                industry_map=req.industry_map,
                theme_map=req.theme_map,
            )
            pre_equity = cash + pre_exposure
            if pre_equity <= 0:
                continue
            if idx > 0 and prev_equity > 0:
                daily_returns.append(float(pre_equity) / float(prev_equity) - 1.0)
                max_return_keep = max(100, int(req.risk_return_lookback_days) * 2)
                if len(daily_returns) > max_return_keep:
                    daily_returns = daily_returns[-max_return_keep:]
            peak = max(peak, pre_equity)
            pre_drawdown = max(0.0, 1.0 - pre_equity / peak)

            if idx % rebalance_step == 0:
                targets = self._build_targets(
                    req=req,
                    normalized=normalized,
                    feature_cache=feature_cache,
                    latest_turnover=latest_turnover,
                    latest_flags=latest_flags,
                    strategy=strategy,
                    trade_day=trade_day,
                    gross_target=gross_target,
                    params_by_symbol=params_by_symbol or {},
                )
                targets = self._apply_theme_cap(
                    targets=targets,
                    theme_map=req.theme_map,
                    max_theme_exposure=req.max_theme_exposure,
                )

                if req.enable_portfolio_risk_control and self.risk_engine is not None:
                    risk_result = self.risk_engine.evaluate_portfolio(
                        PortfolioRiskRequest(
                            portfolio=PortfolioSnapshot(
                                total_value=float(pre_equity),
                                cash=float(cash),
                                peak_value=float(peak),
                                current_drawdown=float(pre_drawdown),
                                industry_exposure=pre_industry_exposure,
                                theme_exposure=pre_theme_exposure,
                            ),
                            max_drawdown=req.risk_max_drawdown,
                            max_industry_exposure=req.max_industry_exposure,
                            max_theme_exposure=req.max_theme_exposure,
                            daily_returns=daily_returns[-int(req.risk_return_lookback_days) :],
                            recent_trade_pnls=recent_trade_pnls[-int(req.risk_loss_streak_lookback_trades) :],
                            max_consecutive_losses=req.risk_max_consecutive_losses,
                            max_daily_loss=req.risk_max_daily_loss,
                            var_confidence=req.risk_var_confidence,
                            max_var=req.risk_max_var,
                            max_es=req.risk_max_es,
                        )
                    )
                    if risk_result.blocked:
                        risk_blocked_days += 1
                        targets = {}
                    elif risk_result.level == SignalLevel.WARNING:
                        risk_warning_days += 1

                cash, realized_trade_pnls = self._rebalance(
                    req=req,
                    trade_day=trade_day,
                    targets=targets,
                    holdings=holdings,
                    last_price=last_price,
                    latest_turnover=latest_turnover,
                    latest_flags=latest_flags,
                    cash=cash,
                    trades=trades,
                )
                if realized_trade_pnls:
                    recent_trade_pnls.extend(realized_trade_pnls)
                    max_pnl_keep = max(100, int(req.risk_loss_streak_lookback_trades) * 2)
                    if len(recent_trade_pnls) > max_pnl_keep:
                        recent_trade_pnls = recent_trade_pnls[-max_pnl_keep:]

            exposure, industry_exposure, theme_exposure = self._exposure(
                holdings=holdings,
                last_price=last_price,
                industry_map=req.industry_map,
                theme_map=req.theme_map,
            )
            equity = cash + exposure
            if equity <= 0:
                continue
            peak = max(peak, equity)
            drawdown = max(0.0, 1.0 - equity / peak)
            utilization = exposure / equity
            cash_ratio = cash / equity
            if any(value > req.max_industry_exposure + 1e-9 for value in industry_exposure.values()):
                industry_breach_count += 1
            if any(value > req.max_theme_exposure + 1e-9 for value in theme_exposure.values()):
                theme_breach_count += 1
            equity_curve.append(
                PortfolioEquityPoint(
                    date=trade_day,
                    equity=round(equity, 2),
                    cash=round(cash, 2),
                    gross_exposure=round(exposure, 2),
                    utilization=round(utilization, 6),
                    drawdown=round(drawdown, 6),
                )
            )
            prev_equity = float(equity)

        metrics = self._metrics(
            req=req,
            equity_curve=equity_curve,
            trade_count=len([x for x in trades if x.quantity > 0]),
            industry_breach_count=industry_breach_count,
            theme_breach_count=theme_breach_count,
            risk_blocked_days=risk_blocked_days,
            risk_warning_days=risk_warning_days,
        )
        _, industry_exposure, theme_exposure = self._exposure(
            holdings=holdings,
            last_price=last_price,
            industry_map=req.industry_map,
            theme_map=req.theme_map,
        )
        final_equity = equity_curve[-1].equity if equity_curve else req.initial_cash
        final_weights = self._final_weights(
            holdings=holdings,
            last_price=last_price,
            equity=max(1.0, float(final_equity)),
        )
        return PortfolioBacktestResult(
            strategy_name=req.strategy_name,
            symbols=list(req.symbols),
            start_date=req.start_date,
            end_date=req.end_date,
            metrics=metrics,
            trades=trades,
            equity_curve=equity_curve,
            final_weights=final_weights,
            industry_exposure=industry_exposure,
            theme_exposure=theme_exposure,
        )

    @staticmethod
    def _normalize_bars(bars_by_symbol: dict[str, pd.DataFrame]) -> dict[str, dict[date, dict[str, object]]]:
        out: dict[str, dict[date, dict[str, object]]] = {}
        for symbol, frame in bars_by_symbol.items():
            if frame is None or frame.empty:
                out[symbol] = {}
                continue
            tmp = frame.sort_values("trade_date").copy()
            tmp["trade_date"] = pd.to_datetime(tmp["trade_date"], errors="coerce").dt.date
            tmp = tmp[tmp["trade_date"].notna()]
            out[symbol] = {
                d: row
                for d, row in zip(tmp["trade_date"], tmp.to_dict("records"), strict=False)
            }
        return out

    @staticmethod
    def _build_calendar(normalized: dict[str, dict[date, dict[str, object]]]) -> list[date]:
        dates: set[date] = set()
        for values in normalized.values():
            dates.update(values.keys())
        return sorted(dates)

    def _build_targets(
        self,
        *,
        req: PortfolioBacktestRequest,
        normalized: dict[str, dict[date, dict[str, object]]],
        feature_cache: dict[str, pd.DataFrame],
        latest_turnover: dict[str, float],
        latest_flags: dict[str, dict[str, bool]],
        strategy: BaseStrategy,
        trade_day: date,
        gross_target: float,
        params_by_symbol: dict[str, dict[str, float | int | str | bool]],
    ) -> dict[str, float]:
        candidates: list[OptimizeCandidate] = []
        for symbol in req.symbols:
            rows = normalized.get(symbol, {})
            if not rows:
                continue
            history_rows = [row for d, row in rows.items() if d <= trade_day]
            if not history_rows:
                continue
            history = pd.DataFrame(history_rows).sort_values("trade_date").reset_index(drop=True)
            features = self.factor_engine.compute(history)
            feature_cache[symbol] = features
            latest = features.iloc[-1]
            latest_turnover[symbol] = float(latest.get("turnover20", 0.0) or 0.0)
            latest_flags[symbol] = {
                "is_suspended": bool(latest.get("is_suspended", False)),
                "at_limit_up": bool(latest.get("at_limit_up", False)),
                "at_limit_down": bool(latest.get("at_limit_down", False)),
                "is_one_word_limit_up": bool(latest.get("is_one_word_limit_up", False)),
                "is_one_word_limit_down": bool(latest.get("is_one_word_limit_down", False)),
            }

            strategy_signals = strategy.generate(
                features,
                StrategyContext(params=params_by_symbol.get(symbol, req.strategy_params), market_state={}),
            )
            if not strategy_signals:
                continue
            signal = strategy_signals[-1]
            if signal.action != SignalAction.BUY:
                continue
            momentum = float(latest.get("momentum20", 0.0) or 0.0)
            fundamental = float(latest.get("fundamental_score", 0.5) or 0.5)
            expected = 0.65 * momentum + 0.35 * (fundamental - 0.5)
            volatility = max(0.001, float(latest.get("volatility20", 0.01) or 0.01))
            liquidity = min(1.0, latest_turnover[symbol] / 40_000_000.0)
            candidates.append(
                OptimizeCandidate(
                    symbol=symbol,
                    expected_return=float(expected),
                    volatility=volatility,
                    industry=req.industry_map.get(symbol, "UNKNOWN"),
                    liquidity_score=float(liquidity),
                )
            )
        if not candidates:
            return {}
        optimized = self.optimizer.optimize(
            PortfolioOptimizeRequest(
                candidates=candidates,
                max_single_position=req.max_single_position,
                max_industry_exposure=req.max_industry_exposure,
                target_gross_exposure=gross_target,
            )
        )
        return {x.symbol: float(x.weight) for x in optimized.weights}

    @staticmethod
    def _apply_theme_cap(
        *,
        targets: dict[str, float],
        theme_map: dict[str, str],
        max_theme_exposure: float,
    ) -> dict[str, float]:
        if not targets:
            return {}
        cap = max(0.0, min(1.0, float(max_theme_exposure)))
        theme_used: dict[str, float] = {}
        adjusted: dict[str, float] = {}
        for symbol, weight in sorted(targets.items(), key=lambda x: x[1], reverse=True):
            theme = theme_map.get(symbol, "UNKNOWN")
            used = theme_used.get(theme, 0.0)
            remain = cap - used
            if remain <= 0:
                continue
            w = min(float(weight), remain)
            if w <= 0:
                continue
            adjusted[symbol] = w
            theme_used[theme] = used + w
        return adjusted

    def _rebalance(
        self,
        *,
        req: PortfolioBacktestRequest,
        trade_day: date,
        targets: dict[str, float],
        holdings: dict[str, _Holding],
        last_price: dict[str, float],
        latest_turnover: dict[str, float],
        latest_flags: dict[str, dict[str, bool]],
        cash: float,
        trades: list[PortfolioBacktestTrade],
    ) -> tuple[float, list[float]]:
        equity = cash + sum(h.qty * max(0.0, float(last_price.get(s, 0.0))) for s, h in holdings.items())
        if equity <= 0:
            return cash, []
        realized_trade_pnls: list[float] = []
        current_weights = {
            symbol: (
                (holdings[symbol].qty * max(0.0, float(last_price.get(symbol, 0.0))) / equity)
                if equity > 0
                else 0.0
            )
            for symbol in holdings
        }
        universe = sorted(set(list(current_weights.keys()) + list(targets.keys())))
        for symbol in universe:
            price = max(0.0, float(last_price.get(symbol, 0.0)))
            if price <= 0:
                continue
            target_weight = float(targets.get(symbol, 0.0))
            current_weight = float(current_weights.get(symbol, 0.0))
            delta_weight = target_weight - current_weight
            delta_value = equity * delta_weight
            desired_qty = int(abs(delta_value) / price / req.lot_size) * req.lot_size
            if desired_qty <= 0:
                continue
            side = SignalAction.BUY if delta_weight > 0 else SignalAction.SELL
            turnover = float(latest_turnover.get(symbol, 0.0))
            flags = latest_flags.get(symbol, {})
            order_notional = desired_qty * price
            if req.enable_realistic_cost_model:
                slip_rate = tiered_slippage_rate(
                    order_notional=order_notional,
                    avg_turnover_20d=turnover,
                    base_slippage_rate=req.slippage_rate,
                )
                impact_rate = estimate_market_impact_rate(
                    order_notional=order_notional,
                    avg_turnover_20d=turnover,
                    impact_coeff=req.impact_cost_coeff,
                    impact_exponent=req.impact_cost_exponent,
                )
                fill_prob = estimate_fill_probability(
                    side=side,
                    is_suspended=bool(flags.get("is_suspended", False)),
                    at_limit_up=bool(flags.get("at_limit_up", False)),
                    at_limit_down=bool(flags.get("at_limit_down", False)),
                    is_one_word_limit_up=bool(flags.get("is_one_word_limit_up", False)),
                    is_one_word_limit_down=bool(flags.get("is_one_word_limit_down", False)),
                    avg_turnover_20d=turnover,
                    order_notional=order_notional,
                    probability_floor=req.fill_probability_floor,
                )
            else:
                slip_rate = req.slippage_rate
                impact_rate = 0.0
                fill_prob = 1.0
            qty = filled_quantity_by_probability(
                desired_qty=desired_qty,
                lot_size=req.lot_size,
                fill_probability=fill_prob,
            )
            if qty <= 0:
                continue

            if side == SignalAction.BUY:
                trade_price = price * (1.0 + slip_rate + impact_rate)
                while qty > 0:
                    gross = qty * trade_price
                    fee = calc_side_fee(
                        notional=gross,
                        commission_rate=req.commission_rate,
                        min_commission=req.min_commission_cny,
                        transfer_fee_rate=req.transfer_fee_rate,
                        stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                        is_sell=False,
                    )
                    total = gross + fee
                    if total <= cash:
                        break
                    qty -= req.lot_size
                if qty <= 0:
                    continue
                gross = qty * trade_price
                fee = calc_side_fee(
                    notional=gross,
                    commission_rate=req.commission_rate,
                    min_commission=req.min_commission_cny,
                    transfer_fee_rate=req.transfer_fee_rate,
                    stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                    is_sell=False,
                )
                total = gross + fee
                cash -= total
                holding = holdings[symbol]
                old_qty = holding.qty
                old_cost = holding.avg_cost
                holding.qty += qty
                if holding.qty > 0:
                    holding.avg_cost = ((old_qty * old_cost) + total) / holding.qty
                trades.append(
                    PortfolioBacktestTrade(
                        date=trade_day,
                        symbol=symbol,
                        action=SignalAction.BUY,
                        price=round(trade_price, 4),
                        quantity=qty,
                        notional=round(gross, 2),
                        fee=round(fee, 2),
                        fill_ratio=round(qty / max(1, desired_qty), 4),
                        reason="portfolio_rebalance_buy",
                    )
                )
            else:
                holding = holdings[symbol]
                available = min(holding.qty, qty)
                if available <= 0:
                    continue
                avg_cost = float(holding.avg_cost)
                trade_price = price * (1.0 - slip_rate - impact_rate)
                gross = available * trade_price
                fee = calc_side_fee(
                    notional=gross,
                    commission_rate=req.commission_rate,
                    min_commission=req.min_commission_cny,
                    transfer_fee_rate=req.transfer_fee_rate,
                    stamp_duty_sell_rate=req.stamp_duty_sell_rate,
                    is_sell=True,
                )
                net = gross - fee
                cash += net
                realized_trade_pnls.append(float(net - available * avg_cost))
                holding.qty -= available
                if holding.qty <= 0:
                    holding.qty = 0
                    holding.avg_cost = 0.0
                trades.append(
                    PortfolioBacktestTrade(
                        date=trade_day,
                        symbol=symbol,
                        action=SignalAction.SELL,
                        price=round(trade_price, 4),
                        quantity=available,
                        notional=round(gross, 2),
                        fee=round(fee, 2),
                        fill_ratio=round(available / max(1, desired_qty), 4),
                        reason="portfolio_rebalance_sell",
                    )
                )
        return cash, realized_trade_pnls

    @staticmethod
    def _exposure(
        *,
        holdings: dict[str, _Holding],
        last_price: dict[str, float],
        industry_map: dict[str, str],
        theme_map: dict[str, str],
    ) -> tuple[float, dict[str, float], dict[str, float]]:
        gross = 0.0
        industry_raw: dict[str, float] = {}
        theme_raw: dict[str, float] = {}
        for symbol, holding in holdings.items():
            px = max(0.0, float(last_price.get(symbol, 0.0)))
            value = px * max(0, int(holding.qty))
            if value <= 0:
                continue
            gross += value
            industry = industry_map.get(symbol, "UNKNOWN")
            theme = theme_map.get(symbol, "UNKNOWN")
            industry_raw[industry] = industry_raw.get(industry, 0.0) + value
            theme_raw[theme] = theme_raw.get(theme, 0.0) + value
        if gross <= 0:
            return 0.0, {}, {}
        industry = {k: v / gross for k, v in industry_raw.items()}
        theme = {k: v / gross for k, v in theme_raw.items()}
        return gross, industry, theme

    @staticmethod
    def _final_weights(*, holdings: dict[str, _Holding], last_price: dict[str, float], equity: float) -> dict[str, float]:
        if equity <= 0:
            return {}
        out: dict[str, float] = {}
        for symbol, holding in holdings.items():
            value = holding.qty * max(0.0, float(last_price.get(symbol, 0.0)))
            if value <= 0:
                continue
            out[symbol] = round(value / equity, 6)
        return out

    @staticmethod
    def _metrics(
        *,
        req: PortfolioBacktestRequest,
        equity_curve: list[PortfolioEquityPoint],
        trade_count: int,
        industry_breach_count: int,
        theme_breach_count: int,
        risk_blocked_days: int,
        risk_warning_days: int,
    ) -> PortfolioBacktestMetrics:
        if not equity_curve:
            return PortfolioBacktestMetrics(
                total_return=0.0,
                annualized_return=0.0,
                max_drawdown=0.0,
                sharpe=0.0,
                trade_count=trade_count,
                avg_utilization=0.0,
                avg_cash_ratio=1.0,
                industry_breach_count=industry_breach_count,
                theme_breach_count=theme_breach_count,
                risk_blocked_days=risk_blocked_days,
                risk_warning_days=risk_warning_days,
            )
        final_equity = float(equity_curve[-1].equity)
        total_return = final_equity / max(1.0, float(req.initial_cash)) - 1.0
        days = max(1, (req.end_date - req.start_date).days)
        annualized = (1.0 + total_return) ** (365.0 / days) - 1.0 if total_return > -1.0 else -1.0
        max_drawdown = max((float(x.drawdown) for x in equity_curve), default=0.0)
        utilizations = [float(x.utilization) for x in equity_curve]
        cash_ratios = [max(0.0, min(1.0, float(x.cash / x.equity))) for x in equity_curve if x.equity > 0]

        returns: list[float] = []
        prev = float(equity_curve[0].equity)
        for point in equity_curve[1:]:
            if prev > 0:
                returns.append(float(point.equity) / prev - 1.0)
            prev = float(point.equity)
        sharpe = 0.0
        if returns:
            mean_ret = sum(returns) / len(returns)
            var = sum((x - mean_ret) ** 2 for x in returns) / len(returns)
            std = math.sqrt(var)
            if std > 1e-12:
                sharpe = mean_ret / std * math.sqrt(252.0)

        return PortfolioBacktestMetrics(
            total_return=round(total_return, 6),
            annualized_return=round(annualized, 6),
            max_drawdown=round(max_drawdown, 6),
            sharpe=round(sharpe, 6),
            trade_count=int(trade_count),
            avg_utilization=round(sum(utilizations) / len(utilizations), 6),
            avg_cash_ratio=round(sum(cash_ratios) / len(cash_ratios), 6) if cash_ratios else 1.0,
            industry_breach_count=int(industry_breach_count),
            theme_breach_count=int(theme_breach_count),
            risk_blocked_days=int(risk_blocked_days),
            risk_warning_days=int(risk_warning_days),
        )
