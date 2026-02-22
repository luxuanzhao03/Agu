from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class MultiFactorStrategy(BaseStrategy):
    info = StrategyInfo(
        name="multi_factor",
        title="Multi Factor",
        description="Technical + fundamental multi-factor scorer for candidate ranking.",
        frequency="D/W",
        params_schema={
            "buy_threshold": "float",
            "sell_threshold": "float",
            "w_momentum": "float",
            "w_quality": "float",
            "w_low_vol": "float",
            "w_liquidity": "float",
            "w_fundamental": "float",
            "w_tushare_advanced": "float",
            "min_fundamental_score_buy": "float",
            "min_tushare_score_buy": "float",
        },
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        buy_threshold = float(context.params.get("buy_threshold", 0.55))
        sell_threshold = float(context.params.get("sell_threshold", 0.35))
        w_momentum = float(context.params.get("w_momentum", 0.35))
        w_quality = float(context.params.get("w_quality", 0.20))
        w_low_vol = float(context.params.get("w_low_vol", 0.15))
        w_liquidity = float(context.params.get("w_liquidity", 0.15))
        w_fundamental = float(context.params.get("w_fundamental", 0.15))
        w_tushare_advanced = float(context.params.get("w_tushare_advanced", 0.10))
        min_fundamental_score_buy = float(context.params.get("min_fundamental_score_buy", 0.35))
        min_tushare_score_buy = float(context.params.get("min_tushare_score_buy", 0.30))

        latest = features.sort_values("trade_date").iloc[-1]
        momentum = max(-0.5, min(0.5, float(latest.get("momentum60", 0.0)))) + 0.5
        quality = max(-0.5, min(0.5, float(latest.get("momentum20", 0.0)))) + 0.5
        low_vol = 1.0 - min(1.0, float(latest.get("volatility20", 0.0)) * 5)
        turnover = float(latest.get("turnover20", 0.0))
        liquidity = min(1.0, turnover / 30_000_000)
        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced = float(latest.get("tushare_advanced_score", 0.5)) if tushare_available else 0.5

        weighted_components = [
            (momentum, w_momentum),
            (quality, w_quality),
            (low_vol, w_low_vol),
            (liquidity, w_liquidity),
            (fundamental, w_fundamental),
            (tushare_advanced, w_tushare_advanced),
        ]
        total_weight = sum(max(0.0, w) for _, w in weighted_components)
        if total_weight <= 0:
            score = 0.5
        else:
            score = sum(float(v) * max(0.0, w) for v, w in weighted_components) / total_weight

        if score >= buy_threshold:
            action = SignalAction.BUY
            reason = f"Multi-factor score {score:.3f} >= {buy_threshold:.3f}."
        elif score <= sell_threshold:
            action = SignalAction.SELL
            reason = f"Multi-factor score {score:.3f} <= {sell_threshold:.3f}."
        else:
            action = SignalAction.WATCH
            reason = f"Multi-factor score {score:.3f} in neutral range."

        if action == SignalAction.BUY and fundamental_available and fundamental < min_fundamental_score_buy:
            action = SignalAction.WATCH
            reason = (
                f"Technical score reached buy zone, but fundamental score {fundamental:.3f} "
                f"< {min_fundamental_score_buy:.3f}; downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_available and tushare_advanced < min_tushare_score_buy:
            action = SignalAction.WATCH
            reason = (
                f"Technical score reached buy zone, but tushare advanced score {tushare_advanced:.3f} "
                f"< {min_tushare_score_buy:.3f}; downgraded to WATCH."
            )

        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=min(0.95, max(0.25, score)),
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=0.03 if action == SignalAction.BUY else None,
                metadata={
                    "factor_score": round(score, 4),
                    "momentum": round(momentum, 4),
                    "quality": round(quality, 4),
                    "low_vol": round(low_vol, 4),
                    "liquidity": round(liquidity, 4),
                    "fundamental_score": round(fundamental, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced, 4),
                    "tushare_advanced_available": tushare_available,
                },
            )
        ]
