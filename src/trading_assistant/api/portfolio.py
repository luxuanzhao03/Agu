from __future__ import annotations

from fastapi import APIRouter, Depends

from trading_assistant.audit.service import AuditService
from trading_assistant.core.container import (
    get_audit_service,
    get_portfolio_optimizer,
    get_portfolio_rebalancer,
    get_portfolio_stress_tester,
    get_risk_engine,
)
from trading_assistant.core.models import (
    PortfolioOptimizeRequest,
    PortfolioOptimizeResult,
    PortfolioRiskRequest,
    PortfolioRiskResult,
    RebalancePlan,
    RebalanceRequest,
    StressTestRequest,
    StressTestResult,
)
from trading_assistant.core.security import AuthContext, UserRole, require_roles
from trading_assistant.portfolio.optimizer import PortfolioOptimizer
from trading_assistant.portfolio.rebalancer import PortfolioRebalancer
from trading_assistant.portfolio.stress import PortfolioStressTester
from trading_assistant.risk.engine import RiskEngine

router = APIRouter(prefix="/portfolio", tags=["portfolio"])


@router.post("/risk/check", response_model=PortfolioRiskResult)
def check_portfolio_risk(
    req: PortfolioRiskRequest,
    risk_engine: RiskEngine = Depends(get_risk_engine),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.RISK, UserRole.PORTFOLIO)),
) -> PortfolioRiskResult:
    result = risk_engine.evaluate_portfolio(req)
    audit.log(
        event_type="portfolio_risk",
        action="check",
        payload={
            "blocked": result.blocked,
            "level": result.level.value,
            "drawdown": req.portfolio.current_drawdown,
        },
    )
    return result


@router.post("/optimize", response_model=PortfolioOptimizeResult)
def optimize_portfolio(
    req: PortfolioOptimizeRequest,
    optimizer: PortfolioOptimizer = Depends(get_portfolio_optimizer),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.PORTFOLIO, UserRole.RESEARCH)),
) -> PortfolioOptimizeResult:
    result = optimizer.optimize(req)
    audit.log(
        event_type="portfolio_optimize",
        action="optimize",
        payload={
            "candidate_count": len(req.candidates),
            "selected_count": len(result.weights),
            "unallocated": result.unallocated_weight,
        },
    )
    return result


@router.post("/rebalance/plan", response_model=RebalancePlan)
def build_rebalance_plan(
    req: RebalanceRequest,
    rebalancer: PortfolioRebalancer = Depends(get_portfolio_rebalancer),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.PORTFOLIO)),
) -> RebalancePlan:
    plan = rebalancer.build_plan(req)
    audit.log(
        event_type="portfolio_rebalance",
        action="plan",
        payload={
            "orders": len(plan.orders),
            "turnover": plan.estimated_turnover,
        },
    )
    return plan


@router.post("/stress-test", response_model=StressTestResult)
def stress_test_portfolio(
    req: StressTestRequest,
    tester: PortfolioStressTester = Depends(get_portfolio_stress_tester),
    audit: AuditService = Depends(get_audit_service),
    _auth: AuthContext = Depends(require_roles(UserRole.PORTFOLIO, UserRole.RISK)),
) -> StressTestResult:
    result = tester.run(req)
    audit.log(
        event_type="portfolio_stress",
        action="run",
        payload={
            "scenarios": len(req.scenarios),
            "weights": len(req.weights),
        },
    )
    return result
