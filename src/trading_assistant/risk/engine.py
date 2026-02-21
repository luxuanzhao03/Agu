from __future__ import annotations

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
    IndustryExposureRule,
    LimitPriceRule,
    LiquidityRule,
    PositionLimitRule,
    RiskRule,
    STRule,
    SuspensionRule,
    TPlusOneRule,
)


class RiskEngine:
    def __init__(
        self,
        max_single_position: float,
        max_drawdown: float,
        max_industry_exposure: float,
        min_turnover_20d: float,
    ) -> None:
        self.rules: list[RiskRule] = [
            TPlusOneRule(),
            STRule(),
            SuspensionRule(),
            LimitPriceRule(),
            PositionLimitRule(max_single_position=max_single_position),
            LiquidityRule(min_turnover_20d=min_turnover_20d),
            DrawdownRule(max_drawdown=max_drawdown),
            IndustryExposureRule(max_industry_exposure=max_industry_exposure),
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

        if any(hit.level == SignalLevel.CRITICAL for hit in hits):
            return PortfolioRiskResult(
                blocked=True,
                level=SignalLevel.CRITICAL,
                summary="Portfolio-level hard risk triggered.",
                hits=hits,
            )
        if hits:
            return PortfolioRiskResult(
                blocked=False,
                level=SignalLevel.WARNING,
                summary="Portfolio-level warning triggered.",
                hits=hits,
            )
        return PortfolioRiskResult(
            blocked=False,
            level=SignalLevel.INFO,
            summary="Portfolio-level risk checks passed.",
            hits=[],
        )
