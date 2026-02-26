from __future__ import annotations

import pandas as pd

from trading_assistant.core.models import SignalAction, SignalCandidate, StrategyInfo
from trading_assistant.strategy.base import BaseStrategy, StrategyContext


class TrendPullbackStrategy(BaseStrategy):
    info = StrategyInfo(
        name="trend_pullback",
        title="Trend Pullback",
        description="Buy pullbacks inside an uptrend: require positive medium-term momentum and negative short-term z-score.",
        frequency="D",
        params_schema={
            "min_momentum60": "float",
            "pullback_z_enter": "float",
            "pullback_z_exit": "float",
            "min_turnover": "float",
            "max_volatility20": "float",
            "risk_on_min": "float",
            "risk_off_strength_max": "float",
        },
    )

    def generate(self, features: pd.DataFrame, context: StrategyContext | None = None) -> list[SignalCandidate]:
        if features.empty:
            return []

        context = context or StrategyContext()
        min_momentum60 = float(context.params.get("min_momentum60", 0.02))
        pullback_z_enter = float(context.params.get("pullback_z_enter", -0.45))
        pullback_z_exit = float(context.params.get("pullback_z_exit", 0.20))
        min_turnover = float(context.params.get("min_turnover", 2_000_000.0))
        max_volatility20 = float(context.params.get("max_volatility20", 0.08))
        risk_on_min = float(context.params.get("risk_on_min", 0.45))
        risk_off_strength_max = float(context.params.get("risk_off_strength_max", 0.58))

        latest = features.iloc[-1]

        def _opt_float(value: object) -> float | None:
            try:
                out = float(value)
            except Exception:  # noqa: BLE001
                return None
            if out != out:  # NaN
                return None
            return out

        momentum60 = _opt_float(latest.get("momentum60"))
        momentum20 = _opt_float(latest.get("momentum20"))
        zscore20 = _opt_float(latest.get("zscore20"))
        turnover20 = _opt_float(latest.get("turnover20"))
        volatility20 = _opt_float(latest.get("volatility20"))

        market_state = dict(context.market_state or {})
        style_regime_raw = market_state.get("regime", latest.get("style_regime", "NEUTRAL"))
        style_regime = str(style_regime_raw or "NEUTRAL").strip().upper() or "NEUTRAL"
        style_risk_on_score = _opt_float(market_state.get("risk_on_score", latest.get("style_risk_on_score")))
        if style_risk_on_score is None:
            style_risk_on_score = 0.5
        risk_off_strength = _opt_float(market_state.get("risk_off_strength"))
        if risk_off_strength is None:
            risk_off_strength = 0.5

        fundamental_available = bool(latest.get("fundamental_available", False))
        fundamental_score = float(latest.get("fundamental_score", 0.5)) if fundamental_available else 0.5
        tushare_advanced_available = bool(latest.get("tushare_advanced_available", False))
        tushare_advanced_score = float(latest.get("tushare_advanced_score", 0.5)) if tushare_advanced_available else 0.5
        disclosure_risk = (
            float(latest.get("tushare_disclosure_risk_score", 0.5)) if tushare_advanced_available else 0.5
        )

        missing_factors: list[str] = []
        if momentum60 is None:
            missing_factors.append("momentum60")
        if zscore20 is None:
            missing_factors.append("zscore20")
        if turnover20 is None:
            missing_factors.append("turnover20")
        if volatility20 is None:
            missing_factors.append("volatility20")

        market_risk_off = (
            style_regime == "RISK_OFF"
            or float(style_risk_on_score) < risk_on_min
            or float(risk_off_strength) > risk_off_strength_max
        )

        if missing_factors:
            action = SignalAction.WATCH
            reason = "Insufficient factor history: " + ", ".join(missing_factors) + "."
        elif float(turnover20) < min_turnover:
            action = SignalAction.WATCH
            reason = "Liquidity too low for pullback execution."
        elif float(volatility20) > max_volatility20:
            action = SignalAction.WATCH
            reason = f"Volatility too high ({float(volatility20):.3f}) for pullback entry."
        elif market_risk_off:
            action = SignalAction.WATCH
            reason = "Market regime is risk-off; suspend pullback buying."
        elif float(momentum60) >= min_momentum60 and float(zscore20) <= pullback_z_enter:
            action = SignalAction.BUY
            reason = (
                f"Trend-confirmed pullback: momentum60={float(momentum60):.3f} >= {min_momentum60:.3f}, "
                f"zscore20={float(zscore20):.3f} <= {pullback_z_enter:.3f}."
            )
        elif float(momentum60) < min_momentum60 * 0.5 or float(zscore20) >= pullback_z_exit:
            action = SignalAction.SELL
            reason = "Pullback edge faded (trend weakened or rebound completed)."
        elif momentum20 is not None and float(momentum20) < -0.08:
            action = SignalAction.SELL
            reason = "Short-term downside acceleration detected."
        else:
            action = SignalAction.WATCH
            reason = "No valid trend-pullback setup."

        if action == SignalAction.BUY and fundamental_available and fundamental_score < 0.25:
            action = SignalAction.WATCH
            reason = (
                f"Trend-pullback setup exists, but fundamental score {fundamental_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and tushare_advanced_available and tushare_advanced_score < 0.20:
            action = SignalAction.WATCH
            reason = (
                f"Trend-pullback setup exists, but tushare advanced score {tushare_advanced_score:.3f} is too weak; "
                "downgraded to WATCH."
            )
        if action == SignalAction.BUY and disclosure_risk >= 0.90:
            action = SignalAction.WATCH
            reason = f"Trend-pullback entry blocked by disclosure risk ({disclosure_risk:.2f})."

        trend_strength = 0.0
        if momentum60 is not None:
            trend_strength = max(0.0, min(1.0, (float(momentum60) - min_momentum60 + 0.10) / 0.20))
        pullback_strength = 0.0
        if zscore20 is not None:
            pullback_strength = max(0.0, min(1.0, abs(min(float(zscore20), 0.0)) / max(0.2, abs(pullback_z_enter) * 1.8)))
        base_confidence = max(0.20, min(0.95, 0.30 + 0.35 * trend_strength + 0.35 * pullback_strength))
        if not fundamental_available and not tushare_advanced_available:
            confidence = base_confidence
        else:
            confidence = min(
                0.95,
                max(0.20, 0.65 * base_confidence + 0.20 * fundamental_score + 0.15 * (1.0 - disclosure_risk)),
            )

        return [
            SignalCandidate(
                symbol=str(latest["symbol"]),
                trade_date=latest["trade_date"],
                action=action,
                confidence=confidence,
                reason=reason,
                strategy_name=self.info.name,
                suggested_position=0.07 if action == SignalAction.BUY else None,
                metadata={
                    "momentum60": (round(float(momentum60), 4) if momentum60 is not None else None),
                    "momentum20": (round(float(momentum20), 4) if momentum20 is not None else None),
                    "zscore20": (round(float(zscore20), 4) if zscore20 is not None else None),
                    "turnover20": (round(float(turnover20), 2) if turnover20 is not None else None),
                    "volatility20": (round(float(volatility20), 4) if volatility20 is not None else None),
                    "style_regime": style_regime,
                    "style_risk_on_score": round(float(style_risk_on_score), 4),
                    "risk_off_strength": round(float(risk_off_strength), 4),
                    "fundamental_score": round(fundamental_score, 4),
                    "fundamental_available": fundamental_available,
                    "tushare_advanced_score": round(tushare_advanced_score, 4),
                    "tushare_advanced_available": tushare_advanced_available,
                    "tushare_disclosure_risk_score": round(disclosure_risk, 4),
                    "missing_factors": (",".join(missing_factors) if missing_factors else None),
                },
            )
        ]

