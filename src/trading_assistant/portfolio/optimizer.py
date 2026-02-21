from __future__ import annotations

from collections import defaultdict

from trading_assistant.core.models import OptimizedWeight, PortfolioOptimizeRequest, PortfolioOptimizeResult


class PortfolioOptimizer:
    """
    Constraint-first heuristic optimizer:
    - rank candidates by risk-adjusted score
    - allocate with per-symbol and per-industry caps
    - keep residual as unallocated cash weight
    """

    def optimize(self, req: PortfolioOptimizeRequest) -> PortfolioOptimizeResult:
        ranked = []
        for c in req.candidates:
            score = c.expected_return - req.risk_aversion * c.volatility * 0.1 + 0.1 * c.liquidity_score
            ranked.append((c, score))
        ranked.sort(key=lambda x: x[1], reverse=True)

        weights: list[OptimizedWeight] = []
        industry_alloc: dict[str, float] = defaultdict(float)
        allocated = 0.0

        for candidate, score in ranked:
            if score <= 0:
                continue
            if allocated >= req.target_gross_exposure:
                break

            remaining = req.target_gross_exposure - allocated
            industry_room = req.max_industry_exposure - industry_alloc[candidate.industry]
            if industry_room <= 0:
                continue

            w = min(req.max_single_position, remaining, industry_room)
            if w < req.min_weight_threshold:
                continue

            weights.append(
                OptimizedWeight(
                    symbol=candidate.symbol,
                    weight=round(w, 6),
                    industry=candidate.industry,
                    score=round(score, 6),
                )
            )
            industry_alloc[candidate.industry] += w
            allocated += w

        unallocated = max(0.0, req.target_gross_exposure - allocated)
        return PortfolioOptimizeResult(
            weights=weights,
            industry_exposure={k: round(v, 6) for k, v in industry_alloc.items()},
            unallocated_weight=round(unallocated, 6),
        )

