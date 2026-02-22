from __future__ import annotations

from datetime import date

import pandas as pd

from trading_assistant.data.tushare_provider import TushareProvider


class FakePro:
    def daily(self, *, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        _ = (ts_code, start_date, end_date)
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20250102",
                    "open": 10.0,
                    "high": 10.4,
                    "low": 9.9,
                    "close": 10.2,
                    "vol": 100_000,
                    "amount": 1_020_000.0,
                },
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20250103",
                    "open": 10.2,
                    "high": 10.6,
                    "low": 10.1,
                    "close": 10.5,
                    "vol": 120_000,
                    "amount": 1_260_000.0,
                },
            ]
        )

    def daily_basic(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "trade_date": "20250102",
                    "turnover_rate": 1.2,
                    "turnover_rate_f": 1.1,
                    "volume_ratio": 1.0,
                    "pe_ttm": 15.0,
                    "pb": 1.8,
                    "ps_ttm": 2.6,
                    "dv_ttm": 1.9,
                    "circ_mv": 550_000.0,
                },
                {
                    "trade_date": "20250103",
                    "turnover_rate": 1.6,
                    "turnover_rate_f": 1.4,
                    "volume_ratio": 1.2,
                    "pe_ttm": 15.5,
                    "pb": 1.9,
                    "ps_ttm": 2.7,
                    "dv_ttm": 1.8,
                    "circ_mv": 560_000.0,
                },
            ]
        )

    def moneyflow(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "trade_date": "20250102",
                    "net_mf_amount": 80_000.0,
                    "buy_elg_amount": 200_000.0,
                    "sell_elg_amount": 150_000.0,
                    "buy_lg_amount": 180_000.0,
                    "sell_lg_amount": 140_000.0,
                },
                {
                    "trade_date": "20250103",
                    "net_mf_amount": 100_000.0,
                    "buy_elg_amount": 240_000.0,
                    "sell_elg_amount": 130_000.0,
                    "buy_lg_amount": 210_000.0,
                    "sell_lg_amount": 120_000.0,
                },
            ]
        )

    def stk_limit(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {"trade_date": "20250102", "up_limit": 11.22, "down_limit": 9.18},
                {"trade_date": "20250103", "up_limit": 11.55, "down_limit": 9.45},
            ]
        )

    def adj_factor(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {"trade_date": "20250102", "adj_factor": 1.0},
                {"trade_date": "20250103", "adj_factor": 1.0},
            ]
        )

    def fina_indicator(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "ann_date": "20241231",
                    "end_date": "20241231",
                    "roe": 14.2,
                    "or_yoy": 11.0,
                    "np_yoy": 16.5,
                    "grossprofit_margin": 31.0,
                    "debt_to_assets": 43.0,
                    "ocf_to_or": 1.05,
                    "eps": 0.86,
                }
            ]
        )

    def income(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "ann_date": "20241231",
                    "end_date": "20241231",
                    "total_revenue": 8_000_000_000.0,
                    "operate_profit": 1_300_000_000.0,
                    "n_income_attr_p": 950_000_000.0,
                }
            ]
        )

    def balancesheet(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "ann_date": "20241231",
                    "end_date": "20241231",
                    "total_assets": 20_000_000_000.0,
                    "total_liab": 8_000_000_000.0,
                    "total_cur_assets": 7_000_000_000.0,
                    "total_cur_liab": 4_000_000_000.0,
                }
            ]
        )

    def cashflow(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "ann_date": "20241231",
                    "end_date": "20241231",
                    "n_cashflow_act": 1_100_000_000.0,
                    "n_cashflow_inv_act": -250_000_000.0,
                }
            ]
        )

    def forecast(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "ann_date": "20250101",
                    "p_change_min": -10.0,
                    "p_change_max": 5.0,
                    "type": "预增",
                }
            ]
        )

    def express(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {
                    "ann_date": "20250101",
                    "yoy_sales": 12.0,
                    "yoy_net_profit": 18.0,
                }
            ]
        )

    def fina_audit(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame([{"ann_date": "20250101", "audit_result": "标准无保留意见"}])

    def pledge_stat(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame([{"ann_date": "20250101", "pledge_ratio": 12.5, "pledge_amount": 35000000.0}])

    def share_float(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {"float_date": "20241231", "float_share": 1_000_000_000.0},
                {"float_date": "20250103", "float_share": 1_120_000_000.0},
            ]
        )

    def stk_holdernumber(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame(
            [
                {"ann_date": "20241231", "holder_num": 80_000},
                {"ann_date": "20250103", "holder_num": 90_000},
            ]
        )

    def trade_cal(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame([{"cal_date": "20250102", "is_open": 1}])

    def stock_basic(self, **kwargs: object) -> pd.DataFrame:
        _ = kwargs
        return pd.DataFrame([{"ts_code": "000001.SZ", "name": "平安银行"}])


def _build_provider() -> TushareProvider:
    provider = object.__new__(TushareProvider)
    provider._pro = FakePro()
    return provider


def test_tushare_daily_bars_auto_enrich_advanced_fields() -> None:
    provider = _build_provider()
    bars = provider.get_daily_bars("000001", date(2025, 1, 2), date(2025, 1, 3))
    assert not bars.empty
    assert "ts_turnover_rate" in bars.columns
    assert "ts_net_mf_amount" in bars.columns
    assert "ts_up_limit" in bars.columns
    assert "ts_adj_factor" in bars.columns
    assert "ts_income_total_revenue" in bars.columns
    assert "ts_bs_total_assets" in bars.columns
    assert "ts_cf_operate_cashflow" in bars.columns
    assert "ts_forecast_pchg_mid" in bars.columns
    assert "ts_audit_opinion_risk" in bars.columns
    assert "ts_pledge_ratio" in bars.columns
    assert "ts_share_float" in bars.columns
    assert "ts_holder_num" in bars.columns
    latest = bars.sort_values("trade_date").iloc[-1]
    assert float(latest["ts_turnover_rate"]) == 1.6
    assert float(latest["ts_net_mf_amount"]) == 100_000.0
    assert float(latest["ts_pledge_ratio"]) == 12.5
    assert float(latest["ts_holder_num"]) == 90_000


def test_tushare_capabilities_reflect_points_and_api_availability() -> None:
    provider = _build_provider()
    capabilities = provider.list_advanced_capabilities(user_points=2120)
    by_name = {item["dataset_name"]: item for item in capabilities}
    assert by_name["daily_basic"]["ready_to_call"] is True
    assert by_name["moneyflow"]["ready_to_call"] is True
    assert by_name["forecast"]["ready_to_call"] is True
    assert by_name["fina_audit"]["ready_to_call"] is True
    assert by_name["pledge_stat"]["ready_to_call"] is True
    assert by_name["bak_daily"]["eligible"] is False


def test_tushare_prefetch_reports_per_dataset_status() -> None:
    provider = _build_provider()
    out = provider.prefetch_advanced_datasets(
        symbol="000001",
        start_date=date(2025, 1, 1),
        end_date=date(2025, 1, 31),
        user_points=2120,
        include_ineligible=False,
    )
    assert out["summary"]["total"] > 0
    by_name = {item["dataset_name"]: item for item in out["results"]}
    assert by_name["daily_basic"]["status"] == "success"
    assert by_name["moneyflow"]["status"] == "success"
    assert by_name["income"]["status"] == "success"
    assert by_name["forecast"]["status"] == "success"
    assert by_name["pledge_stat"]["status"] == "success"
    assert by_name["bak_daily"]["status"] == "skipped_ineligible"
