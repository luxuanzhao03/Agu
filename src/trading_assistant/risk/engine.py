from __future__ import annotations

import math

from trading_assistant.core.models import (
    PortfolioRiskRequest,
    PortfolioRiskResult,
    RiskCheckRequest,
    RiskCheckResult,
    RuleHit,
    SignalAction,
    SignalLevel,
)
from trading_assistant.risk.rules import (
    DrawdownRule,
    FundamentalQualityRule,
    IndustryExposureRule,
    LimitPriceRule,
    LiquidityRule,
    PositionLimitRule,
    RiskRule,
    SmallCapitalTradabilityRule,
    STRule,
    SuspensionRule,
    TPlusOneRule,
    TushareDisclosureAndOverhangRule,
)


class RiskEngine:
    def __init__(
        self,
        max_single_position: float,
        max_drawdown: float,
        max_industry_exposure: float,
        min_turnover_20d: float,
        fundamental_buy_warning_score: float = 0.50,
        fundamental_buy_critical_score: float = 0.35,
        fundamental_require_data_for_buy: bool = False,
        tushare_disclosure_warning_score: float = 0.75,
        tushare_disclosure_critical_score: float = 0.90,
        tushare_forecast_warning_pct: float = -35.0,
        tushare_forecast_critical_pct: float = -60.0,
        small_cap_pledge_critical_ratio: float = 50.0,
        small_cap_unlock_warning_ratio: float = 0.20,
        small_cap_unlock_critical_ratio: float = 0.45,
        small_cap_overhang_warning_score: float = 0.75,
    ) -> None:
        self.rules: list[RiskRule] = [
            TPlusOneRule(),
            STRule(),
            SuspensionRule(),
            LimitPriceRule(),
            PositionLimitRule(max_single_position=max_single_position),
            LiquidityRule(min_turnover_20d=min_turnover_20d),
            SmallCapitalTradabilityRule(),
            DrawdownRule(max_drawdown=max_drawdown),
            IndustryExposureRule(max_industry_exposure=max_industry_exposure),
            FundamentalQualityRule(
                warning_score=fundamental_buy_warning_score,
                critical_score=fundamental_buy_critical_score,
                require_data_for_buy=fundamental_require_data_for_buy,
            ),
            TushareDisclosureAndOverhangRule(
                disclosure_warning_score=tushare_disclosure_warning_score,
                disclosure_critical_score=tushare_disclosure_critical_score,
                forecast_warning_pct=tushare_forecast_warning_pct,
                forecast_critical_pct=tushare_forecast_critical_pct,
                small_cap_pledge_critical_ratio=small_cap_pledge_critical_ratio,
                small_cap_unlock_warning_ratio=small_cap_unlock_warning_ratio,
                small_cap_unlock_critical_ratio=small_cap_unlock_critical_ratio,
                small_cap_overhang_warning_score=small_cap_overhang_warning_score,
            ),
        ]
        self.max_drawdown = max_drawdown
        self.max_industry_exposure = max_industry_exposure

    def evaluate(self, req: RiskCheckRequest) -> RiskCheckResult:
        hits = [rule.check(req) for rule in self.rules]
        failed_critical = [h for h in hits if (not h.passed) and h.level == SignalLevel.CRITICAL]
        failed_warning = [h for h in hits if (not h.passed) and h.level == SignalLevel.WARNING]

        recommendations: list[str] = []
        if failed_critical:
            recommendations.append("Hard risk rules triggered. Block execution and move to review queue.")
            return RiskCheckResult(
                blocked=True,
                level=SignalLevel.CRITICAL,
                hits=hits,
                summary="Hard risk limits triggered.",
                recommendations=recommendations,
            )

        if failed_warning:
            recommendations.extend(
                [
                    "Signal can proceed only after manual confirmation.",
                    "Consider reducing target position size.",
                ]
            )
            return RiskCheckResult(
                blocked=False,
                level=SignalLevel.WARNING,
                hits=hits,
                summary="Execution risk warnings triggered.",
                recommendations=recommendations,
            )

        recommendations.append("All configured risk checks passed.")
        return RiskCheckResult(
            blocked=False,
            level=SignalLevel.INFO,
            hits=hits,
            summary="Risk validation passed.",
            recommendations=recommendations,
        )

    def evaluate_portfolio(self, req: PortfolioRiskRequest) -> PortfolioRiskResult:
        hits: list[RuleHit] = []
        var_value, es_value = self._historical_var_es(req.daily_returns, confidence=req.var_confidence)
        if req.portfolio.current_drawdown > req.max_drawdown:
            hits.append(
                RuleHit(
                    rule_name="portfolio_drawdown",
                    passed=False,
                    level=SignalLevel.CRITICAL,
                    message=(
                        f"Portfolio drawdown {req.portfolio.current_drawdown:.2%} "
                        f"exceeds threshold {req.max_drawdown:.2%}."
                    ),
                )
            )

        if req.portfolio.industry_exposure:
            top_industry = max(req.portfolio.industry_exposure.items(), key=lambda x: x[1])
            if float(top_industry[1]) > req.max_industry_exposure:
                hits.append(
                    RuleHit(
                        rule_name="industry_concentration",
                        passed=False,
                        level=SignalLevel.WARNING,
                        message=(
                            f"Industry {top_industry[0]} exposure {float(top_industry[1]):.2%} "
                            f"exceeds threshold {req.max_industry_exposure:.2%}."
                        ),
                    )
                )

        if req.portfolio.theme_exposure:
            top_theme = max(req.portfolio.theme_exposure.items(), key=lambda x: x[1])
            if float(top_theme[1]) > req.max_theme_exposure:
                hits.append(
                    RuleHit(
                        rule_name="theme_concentration",
                        passed=False,
                        level=SignalLevel.WARNING,
                        message=(
                            f"Theme {top_theme[0]} exposure {float(top_theme[1]):.2%} "
                            f"exceeds threshold {req.max_theme_exposure:.2%}."
                        ),
                    )
                )

        latest_daily_return = req.daily_returns[-1] if req.daily_returns else None
        if latest_daily_return is not None and float(latest_daily_return) <= -float(req.max_daily_loss):
            hits.append(
                RuleHit(
                    rule_name="daily_max_loss",
                    passed=False,
                    level=SignalLevel.CRITICAL,
                    message=(
                        f"Daily return {float(latest_daily_return):.2%} <= "
                        f"-max_daily_loss {-req.max_daily_loss:.2%}."
                    ),
                )
            )

        consecutive_losses = self._consecutive_losses(req.recent_trade_pnls)
        if consecutive_losses >= req.max_consecutive_losses:
            hits.append(
                RuleHit(
                    rule_name="loss_circuit_breaker",
                    passed=False,
                    level=SignalLevel.CRITICAL,
                    message=(
                        f"Consecutive losses {consecutive_losses} >= threshold {req.max_consecutive_losses}."
                    ),
                )
            )

        if var_value is not None and var_value > req.max_var:
            hits.append(
                RuleHit(
                    rule_name="portfolio_var",
                    passed=False,
                    level=SignalLevel.WARNING,
                    message=f"Portfolio VaR {var_value:.2%} exceeds {req.max_var:.2%}.",
                )
            )
        if es_value is not None and es_value > req.max_es:
            hits.append(
                RuleHit(
                    rule_name="portfolio_es",
                    passed=False,
                    level=SignalLevel.CRITICAL,
                    message=f"Portfolio ES {es_value:.2%} exceeds {req.max_es:.2%}.",
                )
            )

        if req.pending_signal and req.pending_signal.action == SignalAction.BUY:
            # Industry exposure check requires pending signal metadata: industry.
            ind = str(req.pending_signal.metadata.get("industry", ""))
            if ind:
                current = float(req.portfolio.industry_exposure.get(ind, 0.0))
                projected = current + float(req.pending_signal.suggested_position or 0.0)
                if projected > req.max_industry_exposure:
                    hits.append(
                        RuleHit(
                            rule_name="industry_exposure",
                            passed=False,
                            level=SignalLevel.WARNING,
                            message=(
                                f"Projected {ind} exposure {projected:.2%} "
                                f"exceeds {req.max_industry_exposure:.2%}."
                            ),
                        )
                    )
            theme = str(req.pending_signal.metadata.get("theme", ""))
            if theme:
                current = float(req.portfolio.theme_exposure.get(theme, 0.0))
                projected = current + float(req.pending_signal.suggested_position or 0.0)
                if projected > req.max_theme_exposure:
                    hits.append(
                        RuleHit(
                            rule_name="theme_exposure",
                            passed=False,
                            level=SignalLevel.WARNING,
                            message=(
                                f"Projected {theme} exposure {projected:.2%} "
                                f"exceeds {req.max_theme_exposure:.2%}."
                            ),
                        )
                    )

        if any(hit.level == SignalLevel.CRITICAL for hit in hits):
            return PortfolioRiskResult(
                blocked=True,
                level=SignalLevel.CRITICAL,
                summary="Portfolio-level hard risk triggered.",
                hits=hits,
                var_value=var_value,
                es_value=es_value,
            )
        if hits:
            return PortfolioRiskResult(
                blocked=False,
                level=SignalLevel.WARNING,
                summary="Portfolio-level warning triggered.",
                hits=hits,
                var_value=var_value,
                es_value=es_value,
            )
        return PortfolioRiskResult(
            blocked=False,
            level=SignalLevel.INFO,
            summary="Portfolio-level risk checks passed.",
            hits=[],
            var_value=var_value,
            es_value=es_value,
        )

    @staticmethod
    def _consecutive_losses(pnls: list[float]) -> int:
        count = 0
        for pnl in reversed(pnls):
            if float(pnl) < 0:
                count += 1
            else:
                break
        return count

    @staticmethod
    def _historical_var_es(returns: list[float], *, confidence: float) -> tuple[float | None, float | None]:
        if not returns:
            return None, None
        losses = sorted(max(0.0, -float(x)) for x in returns)
        if not losses:
            return 0.0, 0.0
        idx = int(math.ceil(confidence * len(losses))) - 1
        idx = max(0, min(len(losses) - 1, idx))
        var_value = float(losses[idx])
        tail = losses[idx:]
        es_value = float(sum(tail) / len(tail)) if tail else var_value
        return var_value, es_value
