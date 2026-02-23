from __future__ import annotations

import math

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext
from trading_assistant.trading.costs import required_cash_for_min_lot


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _scale(value: float, low: float, high: float) -> float:
    if high <= low:
        return 0.5
    return _clip01((float(value) - low) / (high - low))


class SmallCapitalAdaptiveStrategy(BaseStrategy):
    info = StrategyInfo(
        name="small_capital_adaptive",
        title="Small Capital Adaptive",
        description=(
            "Designed for principals below CNY 10k: affordability-aware, low-turnover, "
            "trend-and-quality mixed signal with dynamic position sizing."
        ),
        frequency="D",
        params_schema={
            "buy_threshold": "float",
            "sell_threshold": "float",
            "min_turnover20": "float",
            "max_volatility20": "float",
            "min_momentum20_buy": "float",
            "max_momentum20_buy": "float",
            "min_fundamental_score_buy": "float",
            "max_positions": "int",
            "cash_buffer_ratio": "float",
            "risk_per_trade": "float",
            "max_single_position": "float",
            "min_tushare_advanced_score_buy": "float",
        },
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        params = context.params
        market = context.market_state

        buy_threshold = float(params.get("buy_threshold", 0.62))
        sell_threshold = float(params.get("sell_threshold", 0.34))
        min_turnover20 = float(params.get("min_turnover20", 12_000_000.0))
        max_volatility20 = float(params.get("max_volatility20", 0.045))
        min_momentum20_buy = float(params.get("min_momentum20_buy", -0.02))
        max_momentum20_buy = float(params.get("max_momentum20_buy", 0.18))
        min_fundamental_score_buy = float(params.get("min_fundamental_score_buy", 0.45))
        max_positions = max(1, int(params.get("max_positions", 3)))
        cash_buffer_ratio = max(0.0, min(0.5, float(params.get("cash_buffer_ratio", 0.10))))
        risk_per_trade = max(0.001, min(0.05, float(params.get("risk_per_trade", 0.01))))
        max_single_position = max(0.05, min(1.0, float(params.get("max_single_position", 0.35))))
        min_tushare_advanced_score_buy = float(params.get("min_tushare_advanced_score_buy", 0.40))

        principal = float(market.get("small_capital_principal", params.get("small_capital_principal", 10_000.0)))
        principal = max(500.0, principal)
        lot_size = max(1, int(market.get("small_capital_lot_size", 100)))
        commission_rate = float(market.get("commission_rate", 0.0003))
        min_commission_cny = float(market.get("min_commission_cny", 5.0))
        transfer_fee_rate = float(market.get("transfer_fee_rate", 0.00001))

        latest = features.iloc[-1]
        close = max(0.0, float(latest.get("close", 0.0)))
        ma20 = float(latest.get("ma20", close))
        ma60 = float(latest.get("ma60", ma20))
        atr14 = max(0.0, float(latest.get("atr14", 0.0)))
        momentum20 = float(latest.get("momentum20", 0.0))
        momentum60 = float(latest.get("momentum60", 0.0))
        volatility20 = max(0.0, float(latest.get("volatility20", 0.0)))
        turnover20 = max(0.0, float(latest.get("turnover20", 0.0)))
        event_score = float(latest.get("event_score", 0.0))
        negative_event_score = float(latest.get("negative_event_score", 0.0))
        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_advanced_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced_score = (
            float(latest.get("tushare_advanced_score", 0.5)) if tushare_advanced_available else 0.5
        )
        tushare_tradability_score = (
            float(latest.get("tushare_tradability_score", 0.5)) if tushare_advanced_available else 0.5
        )
        tushare_moneyflow_score = float(latest.get("tushare_moneyflow_score", 0.5)) if tushare_advanced_available else 0.5
        tushare_disclosure_risk = (
            float(latest.get("tushare_disclosure_risk_score", 0.5)) if tushare_advanced_available else 0.0
        )
        tushare_overhang_risk = (
            float(latest.get("tushare_overhang_risk_score", 0.5)) if tushare_advanced_available else 0.0
        )
        tushare_pledge_ratio = float(latest.get("tushare_pledge_ratio", 0.0)) if tushare_advanced_available else 0.0
        tushare_unlock_ratio = (
            float(latest.get("tushare_share_float_unlock_ratio", 0.0)) if tushare_advanced_available else 0.0
        )

        min_lot_cash = required_cash_for_min_lot(
            price=close,
            lot_size=lot_size,
            commission_rate=commission_rate,
            min_commission=min_commission_cny,
            transfer_fee_rate=transfer_fee_rate,
        )
        usable_cash = principal * max(0.0, 1.0 - cash_buffer_ratio)
        affordable = close > 0 and min_lot_cash <= usable_cash
        lot_position = 1.0 if principal <= 0 else (close * lot_size / principal if close > 0 else 1.0)
        diversified_for_one_lot = lot_position <= max_single_position

        trend_score = (
            0.50 * _scale(momentum60, -0.10, 0.25)
            + 0.25 * (1.0 if close >= ma20 else 0.0)
            + 0.25 * (1.0 if ma20 >= ma60 * 0.995 else 0.0)
        )
        pullback_score = _clip01(1.0 - abs(momentum20 - 0.05) / 0.15)
        low_vol_score = _clip01(1.0 - _scale(volatility20, 0.0, max_volatility20 * 1.3))
        liquidity_score = _scale(turnover20, min_turnover20 * 0.6, min_turnover20 * 2.5)
        event_balance_score = _clip01(0.5 + (event_score - negative_event_score) * 0.8)

        score = (
            0.25 * trend_score
            + 0.12 * pullback_score
            + 0.16 * low_vol_score
            + 0.12 * liquidity_score
            + 0.10 * event_balance_score
            + 0.10 * fundamental_score
            + 0.15 * tushare_advanced_score
            - 0.07 * tushare_disclosure_risk
            - 0.05 * tushare_overhang_risk
        )
        if fundamental_available and fundamental_score < min_fundamental_score_buy:
            score -= 0.12
        if tushare_advanced_available and tushare_advanced_score < min_tushare_advanced_score_buy:
            score -= 0.10
        if not affordable:
            score -= 0.25
        if not diversified_for_one_lot:
            score -= 0.15
        score = _clip01(score)

        buy_guards_passed = (
            affordable
            and diversified_for_one_lot
            and (turnover20 >= min_turnover20)
            and (volatility20 <= max_volatility20)
            and (min_momentum20_buy <= momentum20 <= max_momentum20_buy)
            and ((not fundamental_available) or (fundamental_score >= min_fundamental_score_buy))
            and ((not tushare_advanced_available) or (tushare_advanced_score >= min_tushare_advanced_score_buy))
            and ((not tushare_advanced_available) or (tushare_tradability_score >= 0.30))
            and ((not tushare_advanced_available) or (tushare_disclosure_risk < 0.80))
            and ((not tushare_advanced_available) or (tushare_overhang_risk < 0.85))
            and ((not tushare_advanced_available) or (tushare_pledge_ratio < 50.0))
            and ((not tushare_advanced_available) or (tushare_unlock_ratio < 0.45))
            and (negative_event_score < 0.70)
        )

        action = SignalAction.WATCH
        reason = f"Small-cap score {score:.3f} in neutral zone."

        if negative_event_score >= 0.70:
            action = SignalAction.SELL
            reason = f"Negative event risk elevated ({negative_event_score:.2f})."
        elif close < ma20 * 0.965 and momentum20 < 0:
            action = SignalAction.SELL
            reason = "Price breaks below MA20 with weakening momentum."
        elif score >= buy_threshold and buy_guards_passed:
            action = SignalAction.BUY
            reason = f"Small-cap score {score:.3f} >= {buy_threshold:.3f} and tradability guards passed."
        elif score <= sell_threshold and momentum20 < -0.04:
            action = SignalAction.SELL
            reason = f"Small-cap score {score:.3f} <= {sell_threshold:.3f} with negative momentum."
        elif not affordable:
            reason = (
                f"Minimum lot cash {min_lot_cash:.2f} exceeds usable cash {usable_cash:.2f}; "
                "wait for cheaper opportunity."
            )
        elif not diversified_for_one_lot:
            reason = (
                f"One-lot notional ratio {lot_position:.1%} exceeds per-position cap {max_single_position:.1%}; "
                "skip to avoid over-concentration."
            )
        elif turnover20 < min_turnover20:
            reason = f"Liquidity low (turnover20={turnover20:.0f} < {min_turnover20:.0f})."
        elif volatility20 > max_volatility20:
            reason = f"Volatility too high (vol20={volatility20:.3f} > {max_volatility20:.3f})."
        elif tushare_advanced_available and tushare_advanced_score < min_tushare_advanced_score_buy:
            reason = (
                f"Tushare advanced score {tushare_advanced_score:.3f} "
                f"< {min_tushare_advanced_score_buy:.3f}; skip low-quality setup."
            )
        elif tushare_disclosure_risk >= 0.80:
            reason = f"Disclosure risk too high ({tushare_disclosure_risk:.2f}); skip small-cap entry."
        elif tushare_overhang_risk >= 0.85:
            reason = f"Overhang risk too high ({tushare_overhang_risk:.2f}); skip small-cap entry."
        elif tushare_pledge_ratio >= 50.0:
            reason = f"Pledge ratio too high ({tushare_pledge_ratio:.1f}%); skip small-cap entry."
        elif tushare_unlock_ratio >= 0.45:
            reason = f"Unlock pressure too high ({tushare_unlock_ratio:.1%}); skip small-cap entry."

        suggested_position = None
        if action == SignalAction.BUY and close > 0:
            per_trade_budget = usable_cash / max_positions
            max_lot_count = int(math.floor(per_trade_budget / max(min_lot_cash, 1e-9)))
            max_lot_count = max(1, max_lot_count)
            budget_position = (max_lot_count * close * lot_size) / principal if principal > 0 else max_single_position

            atr_ratio = atr14 / close if close > 0 else 0.0
            stop_distance = max(0.02, atr_ratio * 2.0)
            risk_position = risk_per_trade / stop_distance

            floor_position = lot_position
            suggested_position = min(max_single_position, max(floor_position, min(budget_position, risk_position)))

        confidence_base = 0.55 * score + 0.30 * trend_score + 0.15 * (1.0 - min(1.0, negative_event_score))
        confidence = min(0.92, max(0.22, confidence_base))

        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=confidence,
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=round(float(suggested_position), 4) if suggested_position is not None else None,
                metadata={
                    "small_cap_score": round(score, 4),
                    "trend_score": round(trend_score, 4),
                    "pullback_score": round(pullback_score, 4),
                    "low_vol_score": round(low_vol_score, 4),
                    "liquidity_score": round(liquidity_score, 4),
                    "event_balance_score": round(event_balance_score, 4),
                    "min_lot_cash": round(min_lot_cash, 3),
                    "usable_cash": round(usable_cash, 3),
                    "lot_position_ratio": round(lot_position, 5),
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_tradability_score": round(tushare_tradability_score, 4),
                    "tushare_moneyflow_score": round(tushare_moneyflow_score, 4),
                    "tushare_disclosure_risk_score": round(tushare_disclosure_risk, 4),
                    "tushare_overhang_risk_score": round(tushare_overhang_risk, 4),
                    "tushare_pledge_ratio": round(tushare_pledge_ratio, 4),
                    "tushare_share_float_unlock_ratio": round(tushare_unlock_ratio, 4),
                    "tushare_advanced_available": tushare_advanced_available,
                },
            )
        ]
