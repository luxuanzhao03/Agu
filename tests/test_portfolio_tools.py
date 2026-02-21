from trading_assistant.core.models import (
    OptimizeCandidate,
    PortfolioOptimizeRequest,
    RebalancePosition,
    RebalanceRequest,
    StressScenario,
    StressTestRequest,
)
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.portfolio.rebalancer import PortfolioRebalancer
from trading_assistant.portfolio.stress import PortfolioStressTester


def test_optimizer_respects_caps() -> None:
    optimizer = PortfolioOptimizer()
    req = PortfolioOptimizeRequest(
        candidates=[
            OptimizeCandidate(symbol="000001", expected_return=0.12, volatility=0.2, industry="BANK"),
            OptimizeCandidate(symbol="000002", expected_return=0.10, volatility=0.2, industry="BANK"),
            OptimizeCandidate(symbol="000003", expected_return=0.09, volatility=0.18, industry="TECH"),
        ],
        max_single_position=0.1,
        max_industry_exposure=0.15,
        target_gross_exposure=0.3,
    )
    result = optimizer.optimize(req)
    assert all(w.weight <= 0.1 + 1e-8 for w in result.weights)
    assert all(v <= 0.15 + 1e-8 for v in result.industry_exposure.values())


def test_rebalancer_builds_orders() -> None:
    rebalancer = PortfolioRebalancer()
    plan = rebalancer.build_plan(
        RebalanceRequest(
            current_positions=[RebalancePosition(symbol="000001", quantity=1000, last_price=10)],
            target_weights=[],
            total_equity=100000,
            lot_size=100,
        )
    )
    assert len(plan.orders) == 1
    assert plan.orders[0].symbol == "000001"


def test_stress_tester_outputs_scenarios() -> None:
    tester = PortfolioStressTester()
    optimize = PortfolioOptimizer().optimize(
        PortfolioOptimizeRequest(
            candidates=[OptimizeCandidate(symbol="000001", expected_return=0.1, volatility=0.2, industry="BANK")],
            max_single_position=0.1,
            max_industry_exposure=0.2,
            target_gross_exposure=0.1,
        )
    )
    result = tester.run(
        StressTestRequest(
            weights=optimize.weights,
            scenarios=[StressScenario(name="mild", shocks={"BANK": -0.03}, default_shock=-0.01)],
        )
    )
    assert len(result.results) == 1
