from __future__ import annotations

from collections import defaultdict

from trading_assistant.core.models import StressScenarioResult, StressTestRequest, StressTestResult


class PortfolioStressTester:
    def run(self, req: StressTestRequest) -> StressTestResult:
        outputs: list[StressScenarioResult] = []
        for scenario in req.scenarios:
            total_ret = 0.0
            breakdown: dict[str, float] = defaultdict(float)
            for w in req.weights:
                shock = scenario.shocks.get(w.industry, scenario.default_shock)
                contrib = w.weight * shock
                total_ret += contrib
                breakdown[w.industry] += contrib
            outputs.append(
                StressScenarioResult(
                    scenario=scenario.name,
                    portfolio_return=round(total_ret, 6),
                    industry_breakdown={k: round(v, 6) for k, v in breakdown.items()},
                )
            )
        return StressTestResult(results=outputs)

