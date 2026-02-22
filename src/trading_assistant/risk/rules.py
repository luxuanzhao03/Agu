from __future__ import annotations

from abc import ABC, abstractmethod

from trading_assistant.core.models import RiskCheckRequest, RuleHit, SignalAction, SignalLevel


class RiskRule(ABC):
    name: str

    @abstractmethod
    def check(self, req: RiskCheckRequest) -> RuleHit:
        """Validate one risk rule and return rule hit details."""


class TPlusOneRule(RiskRule):
    name = "t_plus_one"

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.signal.action != SignalAction.SELL:
            return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Not a SELL action.")

        available_qty = req.position.available_quantity if req.position else 0
        if available_qty <= 0:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message="T+1 constraint hit: no available quantity for selling.",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="T+1 validation passed.")


class STRule(RiskRule):
    name = "st_filter"

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.signal.action == SignalAction.BUY and req.is_st:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message="ST/risk-warning stock is blocked for new BUY signals.",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="ST validation passed.")


class SuspensionRule(RiskRule):
    name = "suspension_filter"

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.signal.action in {SignalAction.BUY, SignalAction.SELL} and req.is_suspended:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message="Security is suspended.",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Suspension validation passed.")


class LimitPriceRule(RiskRule):
    name = "limit_price"

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.signal.action == SignalAction.BUY and req.at_limit_up:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message="Near/up-limit-up, BUY may not be filled.",
            )
        if req.signal.action == SignalAction.SELL and req.at_limit_down:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message="Near/at-limit-down, SELL may not be filled.",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Limit-price validation passed.")


class PositionLimitRule(RiskRule):
    name = "single_position_limit"

    def __init__(self, max_single_position: float) -> None:
        self.max_single_position = max_single_position

    def check(self, req: RiskCheckRequest) -> RuleHit:
        target = req.signal.suggested_position
        if req.signal.action == SignalAction.BUY and target is not None and target > self.max_single_position:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message=f"Target position {target:.2%} exceeds limit {self.max_single_position:.2%}.",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Single-position limit passed.")


class LiquidityRule(RiskRule):
    name = "liquidity_min_turnover"

    def __init__(self, min_turnover_20d: float) -> None:
        self.min_turnover_20d = min_turnover_20d

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.signal.action not in {SignalAction.BUY, SignalAction.SELL}:
            return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Not an executable signal.")

        turnover = req.avg_turnover_20d if req.avg_turnover_20d is not None else 0.0
        if turnover < self.min_turnover_20d:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message=f"Avg turnover20 {turnover:.2f} below threshold {self.min_turnover_20d:.2f}.",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Liquidity validation passed.")


class DrawdownRule(RiskRule):
    name = "portfolio_drawdown"

    def __init__(self, max_drawdown: float) -> None:
        self.max_drawdown = max_drawdown

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.portfolio is None:
            return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="No portfolio snapshot.")

        if req.portfolio.current_drawdown > self.max_drawdown:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message=(
                    f"Portfolio drawdown {req.portfolio.current_drawdown:.2%} "
                    f"exceeds limit {self.max_drawdown:.2%}."
                ),
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Drawdown validation passed.")


class IndustryExposureRule(RiskRule):
    name = "industry_exposure"

    def __init__(self, max_industry_exposure: float) -> None:
        self.max_industry_exposure = max_industry_exposure

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.portfolio is None or not req.symbol_industry or req.signal.action != SignalAction.BUY:
            return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Industry check not applicable.")

        current = float(req.portfolio.industry_exposure.get(req.symbol_industry, 0.0))
        incremental = float(req.signal.suggested_position or 0.0)
        projected = current + incremental
        if projected > self.max_industry_exposure:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message=(
                    f"Projected industry exposure {projected:.2%} exceeds "
                    f"limit {self.max_industry_exposure:.2%}."
                ),
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Industry exposure validation passed.")


class FundamentalQualityRule(RiskRule):
    name = "fundamental_quality"

    def __init__(
        self,
        *,
        warning_score: float,
        critical_score: float,
        require_data_for_buy: bool,
    ) -> None:
        self.warning_score = warning_score
        self.critical_score = critical_score
        self.require_data_for_buy = require_data_for_buy

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if req.signal.action != SignalAction.BUY:
            return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Not a BUY action.")

        if req.fundamental_pit_ok is False:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message="Fundamental PIT check failed (publish time later than trade as-of).",
            )

        if req.fundamental_score is None:
            if self.require_data_for_buy:
                return RuleHit(
                    rule_name=self.name,
                    passed=False,
                    level=SignalLevel.WARNING,
                    message="No fundamental snapshot found; require manual confirmation.",
                )
            return RuleHit(
                rule_name=self.name,
                passed=True,
                level=SignalLevel.INFO,
                message="No fundamental snapshot; fallback to technical/event factors.",
            )

        if req.fundamental_score < self.critical_score:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.CRITICAL,
                message=(
                    f"Fundamental score {req.fundamental_score:.3f} below critical floor "
                    f"{self.critical_score:.3f}."
                ),
            )
        if req.fundamental_score < self.warning_score:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message=(
                    f"Fundamental score {req.fundamental_score:.3f} below warning floor "
                    f"{self.warning_score:.3f}."
                ),
            )
        if req.fundamental_stale_days is not None and req.fundamental_stale_days >= 0 and req.fundamental_stale_days > 540:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message=f"Fundamental snapshot is stale ({req.fundamental_stale_days} days).",
            )
        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Fundamental quality passed.")


class SmallCapitalTradabilityRule(RiskRule):
    name = "small_capital_tradability"

    def check(self, req: RiskCheckRequest) -> RuleHit:
        if not req.enable_small_capital_mode:
            return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Small-capital mode disabled.")
        if req.signal.action != SignalAction.BUY:
            return RuleHit(
                rule_name=self.name,
                passed=True,
                level=SignalLevel.INFO,
                message="Small-capital tradability check applies to BUY actions only.",
            )

        available_cash = req.available_cash
        if available_cash is None:
            available_cash = req.small_capital_principal

        if available_cash is None:
            return RuleHit(
                rule_name=self.name,
                passed=False,
                level=SignalLevel.WARNING,
                message="Small-capital mode is enabled but available cash is unknown.",
            )

        required_cash = req.required_cash_for_min_lot
        if required_cash is not None:
            # Keep some cash buffer to avoid capital exhaustion at small account scale.
            max_usable_cash = float(available_cash) * max(0.0, 1.0 - float(req.small_capital_cash_buffer_ratio))
            if max_usable_cash < float(required_cash):
                return RuleHit(
                    rule_name=self.name,
                    passed=False,
                    level=SignalLevel.CRITICAL,
                    message=(
                        f"Not tradable for small account: usable_cash={max_usable_cash:.2f}, "
                        f"required_cash_for_lot={float(required_cash):.2f}."
                    ),
                )

        expected = req.expected_edge_bps
        cost = req.estimated_roundtrip_cost_bps
        edge_floor = req.min_expected_edge_bps
        if expected is not None and cost is not None and edge_floor is not None:
            required = float(cost) + float(edge_floor)
            if float(expected) < required:
                return RuleHit(
                    rule_name=self.name,
                    passed=False,
                    level=SignalLevel.WARNING,
                    message=(
                        f"Expected edge {float(expected):.1f}bps < required {required:.1f}bps "
                        "(cost + safety margin)."
                    ),
                )

        return RuleHit(rule_name=self.name, passed=True, level=SignalLevel.INFO, message="Small-capital tradability passed.")
