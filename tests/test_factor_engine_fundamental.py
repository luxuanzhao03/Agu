from datetime import date

import pandas as pd

from trading_assistant.factors.engine import FactorEngine


def build_row(**kwargs: object) -> dict[str, object]:
    base: dict[str, object] = {
        "trade_date": date(2025, 1, 2),
        "symbol": "000001",
        "open": 10.0,
        "high": 10.2,
        "low": 9.8,
        "close": 10.1,
        "volume": 100_000,
        "amount": 1_010_000,
        "is_suspended": False,
        "is_st": False,
        "fundamental_available": True,
        "fundamental_pit_ok": True,
        "fundamental_stale_days": 40,
        "fundamental_is_stale": False,
        "roe": 14.0,
        "revenue_yoy": 12.0,
        "net_profit_yoy": 18.0,
        "gross_margin": 34.0,
        "debt_to_asset": 42.0,
        "ocf_to_profit": 1.0,
    }
    base.update(kwargs)
    return base


def test_factor_engine_builds_fundamental_scores() -> None:
    df = pd.DataFrame([build_row()])
    out = FactorEngine().compute(df)
    latest = out.iloc[-1]
    assert 0.0 <= float(latest["fundamental_score"]) <= 1.0
    assert float(latest["fundamental_score"]) > 0.55
    assert float(latest["fundamental_completeness"]) == 1.0


def test_factor_engine_penalizes_stale_and_pit_failed() -> None:
    good = FactorEngine().compute(pd.DataFrame([build_row()])).iloc[-1]
    bad = FactorEngine().compute(
        pd.DataFrame(
            [
                build_row(
                    fundamental_pit_ok=False,
                    fundamental_is_stale=True,
                    fundamental_stale_days=700,
                )
            ]
        )
    ).iloc[-1]
    assert float(bad["fundamental_score"]) < float(good["fundamental_score"])


def test_factor_engine_builds_tushare_advanced_scores() -> None:
    row = build_row(
        ts_turnover_rate=1.8,
        ts_turnover_rate_f=1.6,
        ts_volume_ratio=1.2,
        ts_pe_ttm=14.5,
        ts_pb=1.9,
        ts_ps_ttm=2.3,
        ts_dv_ttm=2.1,
        ts_circ_mv=650_000.0,
        ts_net_mf_amount=120_000.0,
        ts_main_net_mf_amount=95_000.0,
        ts_buy_elg_amount=260_000.0,
        ts_sell_elg_amount=120_000.0,
        ts_buy_lg_amount=220_000.0,
        ts_sell_lg_amount=110_000.0,
        ts_up_limit=11.2,
        ts_down_limit=9.2,
    )
    out = FactorEngine().compute(pd.DataFrame([row]))
    latest = out.iloc[-1]
    assert bool(latest["tushare_advanced_available"]) is True
    assert 0.0 <= float(latest["tushare_advanced_score"]) <= 1.0
    assert float(latest["tushare_advanced_score"]) > 0.50


def test_factor_engine_tushare_advanced_defaults_when_missing() -> None:
    out = FactorEngine().compute(pd.DataFrame([build_row()]))
    latest = out.iloc[-1]
    assert bool(latest["tushare_advanced_available"]) is False
    assert float(latest["tushare_advanced_score"]) == 0.5


def test_factor_engine_builds_statement_disclosure_overhang_scores() -> None:
    row = build_row(
        ts_income_total_revenue=8_000_000_000.0,
        ts_income_operate_profit=1_360_000_000.0,
        ts_income_net_profit_attr=980_000_000.0,
        ts_bs_total_assets=20_000_000_000.0,
        ts_bs_total_liab=8_200_000_000.0,
        ts_bs_total_cur_assets=7_300_000_000.0,
        ts_bs_total_cur_liab=4_000_000_000.0,
        ts_cf_operate_cashflow=1_120_000_000.0,
        ts_forecast_pchg_mid=16.0,
        ts_express_yoy_net_profit=22.0,
        ts_audit_opinion_risk=0.0,
        ts_pledge_ratio=9.0,
        ts_share_float=1_050_000_000.0,
        ts_holder_num=82_000.0,
        ts_turnover_rate=1.8,
        ts_pe_ttm=16.0,
        ts_pb=1.7,
        ts_ps_ttm=2.1,
        ts_net_mf_amount=120_000.0,
        ts_up_limit=11.3,
        ts_down_limit=9.3,
    )
    out = FactorEngine().compute(pd.DataFrame([row]))
    latest = out.iloc[-1]
    assert float(latest["fundamental_statement_quality_score"]) > 0.55
    assert float(latest["tushare_disclosure_score"]) > 0.55
    assert float(latest["tushare_disclosure_risk_score"]) < 0.50
    assert float(latest["tushare_overhang_score"]) > 0.50
    assert float(latest["tushare_overhang_risk_score"]) < 0.55
