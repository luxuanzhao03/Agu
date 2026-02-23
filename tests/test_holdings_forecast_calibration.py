from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from trading_assistant.holdings.service import HoldingService


def test_calibrate_expected_return_uses_historical_bias() -> None:
    preds = [(-0.03 + i * 0.0012) for i in range(80)]
    reals = [0.002 + 0.65 * x for x in preds]

    adjusted, weight = HoldingService._calibrate_expected_return(
        raw_expected_return=0.04,
        predicted_returns=preds,
        realized_returns=reals,
    )
    assert weight > 0.2
    assert adjusted < 0.04
    assert -0.08 <= adjusted <= 0.08


def test_calibrate_up_probability_reduces_overconfidence() -> None:
    probs = [0.05 + (i % 10) * 0.09 for i in range(120)]
    outcomes = [1.0 if p >= 0.70 else 0.0 for p in probs]

    adjusted, weight = HoldingService._calibrate_up_probability(
        raw_up_probability=0.92,
        predicted_probs=probs,
        realized_ups=outcomes,
    )
    assert weight > 0.0
    assert abs(adjusted - 0.92) > 1e-6
    assert 0.05 <= adjusted <= 0.95


def test_build_forecast_calibration_history_outputs_rows() -> None:
    start = date(2025, 1, 1)
    rows: list[dict[str, object]] = []
    close = 10.0
    for i in range(90):
        close *= 1.0 + (0.0008 if i % 3 != 0 else -0.0004)
        rows.append(
            {
                "trade_date": start + timedelta(days=i),
                "close": close,
                "ma20": close * 0.995,
                "momentum5": 0.02 if i % 4 else -0.01,
                "momentum20": 0.05 if i % 5 else -0.02,
                "volatility20": 0.015,
                "fundamental_score": 0.62,
                "tushare_advanced_score": 0.58,
                "event_score": 0.08,
                "negative_event_score": 0.02,
            }
        )
    features = pd.DataFrame(rows)

    history = HoldingService._build_forecast_calibration_history(features=features, max_samples=60)
    assert len(history) >= 40
    first = history[0]
    assert len(first) == 4
    assert -0.08 <= first[0] <= 0.08
    assert 0.05 <= first[1] <= 0.95
