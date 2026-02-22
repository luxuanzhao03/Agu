from __future__ import annotations

import numpy as np
import pandas as pd


class FactorEngine:
    """Daily factor engine for strategy and risk modules."""

    def compute(self, bars: pd.DataFrame) -> pd.DataFrame:
        if bars.empty:
            return bars

        df = bars.sort_values("trade_date").copy()

        # Trend features.
        df["ma5"] = df["close"].rolling(window=5, min_periods=1).mean()
        df["ma20"] = df["close"].rolling(window=20, min_periods=1).mean()
        df["ma60"] = df["close"].rolling(window=60, min_periods=1).mean()

        # ATR and volatility features.
        prev_close = df["close"].shift(1)
        tr_1 = df["high"] - df["low"]
        tr_2 = (df["high"] - prev_close).abs()
        tr_3 = (df["low"] - prev_close).abs()
        df["tr"] = pd.concat([tr_1, tr_2, tr_3], axis=1).max(axis=1)
        df["atr14"] = df["tr"].rolling(window=14, min_periods=1).mean()

        ret = df["close"].pct_change().fillna(0.0)
        df["ret_1d"] = ret
        df["momentum5"] = df["close"].pct_change(5).fillna(0.0)
        df["momentum20"] = df["close"].pct_change(20).fillna(0.0)
        df["momentum60"] = df["close"].pct_change(60).fillna(0.0)
        df["volatility20"] = ret.rolling(window=20, min_periods=2).std().fillna(0.0)

        # Mean-reversion features.
        close_std20 = df["close"].rolling(window=20, min_periods=2).std().replace(0.0, np.nan)
        df["zscore20"] = ((df["close"] - df["ma20"]) / close_std20).replace([np.inf, -np.inf], 0.0).fillna(0.0)

        # Liquidity features.
        df["turnover20"] = df["amount"].rolling(window=20, min_periods=1).mean().fillna(0.0)

        # Event placeholders for event-driven strategy.
        if "event_score" not in df.columns:
            df["event_score"] = 0.0
        if "negative_event_score" not in df.columns:
            df["negative_event_score"] = 0.0

        # Fundamental enrichment placeholders + composite scoring.
        fundamental_numeric_cols = (
            "roe",
            "revenue_yoy",
            "net_profit_yoy",
            "gross_margin",
            "debt_to_asset",
            "ocf_to_profit",
            "eps",
        )
        missing_fundamental = [col for col in fundamental_numeric_cols if col not in df.columns]
        if missing_fundamental:
            df = df.assign(**{col: np.nan for col in missing_fundamental})
        df[list(fundamental_numeric_cols)] = df[list(fundamental_numeric_cols)].apply(pd.to_numeric, errors="coerce")
        if "fundamental_available" not in df.columns:
            df["fundamental_available"] = False
        else:
            df["fundamental_available"] = df["fundamental_available"].fillna(False).astype(bool)
        if "fundamental_pit_ok" not in df.columns:
            df["fundamental_pit_ok"] = True
        else:
            df["fundamental_pit_ok"] = df["fundamental_pit_ok"].fillna(True).astype(bool)
        if "fundamental_is_stale" not in df.columns:
            df["fundamental_is_stale"] = False
        else:
            df["fundamental_is_stale"] = df["fundamental_is_stale"].fillna(False).astype(bool)
        if "fundamental_stale_days" not in df.columns:
            df["fundamental_stale_days"] = -1
        else:
            df["fundamental_stale_days"] = pd.to_numeric(df["fundamental_stale_days"], errors="coerce").fillna(-1).astype(int)

        profitability = self._scale_clip(df["roe"], low=0.0, high=20.0)
        growth = 0.5 * self._scale_clip(df["revenue_yoy"], low=-20.0, high=40.0) + 0.5 * self._scale_clip(
            df["net_profit_yoy"], low=-25.0, high=50.0
        )
        quality = 0.6 * self._scale_clip(df["gross_margin"], low=8.0, high=45.0) + 0.4 * self._scale_clip(
            df["ocf_to_profit"], low=0.0, high=1.2
        )
        leverage = 1.0 - self._scale_clip(df["debt_to_asset"], low=25.0, high=80.0)

        base_fundamental = (
            0.30 * profitability.fillna(0.5)
            + 0.30 * growth.fillna(0.5)
            + 0.25 * quality.fillna(0.5)
            + 0.15 * leverage.fillna(0.5)
        )
        completeness = (
            df[["roe", "revenue_yoy", "net_profit_yoy", "gross_margin", "debt_to_asset", "ocf_to_profit"]]
            .notna()
            .sum(axis=1)
            / 6.0
        )
        score = base_fundamental * completeness + 0.5 * (1.0 - completeness)
        score = np.where(df["fundamental_is_stale"], score - 0.15, score)
        score = np.where(df["fundamental_pit_ok"], score, score - 0.25)
        score = np.clip(score, 0.0, 1.0)

        df["fundamental_profitability_score"] = np.clip(profitability.fillna(0.5), 0.0, 1.0)
        df["fundamental_growth_score"] = np.clip(growth.fillna(0.5), 0.0, 1.0)
        df["fundamental_quality_score"] = np.clip(quality.fillna(0.5), 0.0, 1.0)
        df["fundamental_leverage_score"] = np.clip(leverage.fillna(0.5), 0.0, 1.0)
        df["fundamental_completeness"] = np.clip(completeness.fillna(0.0), 0.0, 1.0)
        df["fundamental_score"] = score

        # Tushare advanced-data features:
        # 1) market microstructure (daily_basic/moneyflow/stk_limit/adj_factor)
        # 2) financial statements (income/balancesheet/cashflow)
        # 3) disclosure risk (forecast/express/fina_audit)
        # 4) ownership/supply risk (pledge/share_float/stk_holdernumber)
        tushare_numeric_cols = (
            "ts_turnover_rate",
            "ts_turnover_rate_f",
            "ts_volume_ratio",
            "ts_pe",
            "ts_pe_ttm",
            "ts_pb",
            "ts_ps",
            "ts_ps_ttm",
            "ts_dv_ratio",
            "ts_dv_ttm",
            "ts_total_mv",
            "ts_circ_mv",
            "ts_net_mf_amount",
            "ts_main_net_mf_amount",
            "ts_buy_elg_amount",
            "ts_sell_elg_amount",
            "ts_buy_lg_amount",
            "ts_sell_lg_amount",
            "ts_up_limit",
            "ts_down_limit",
            "ts_adj_factor",
            "ts_income_total_revenue",
            "ts_income_revenue",
            "ts_income_operate_profit",
            "ts_income_net_profit_attr",
            "ts_income_basic_eps",
            "ts_bs_total_assets",
            "ts_bs_total_liab",
            "ts_bs_total_cur_assets",
            "ts_bs_total_cur_liab",
            "ts_bs_money_cap",
            "ts_cf_operate_cashflow",
            "ts_cf_invest_cashflow",
            "ts_cf_finance_cashflow",
            "ts_cf_capex",
            "ts_forecast_pchg_min",
            "ts_forecast_pchg_max",
            "ts_forecast_pchg_mid",
            "ts_forecast_np_min",
            "ts_forecast_np_max",
            "ts_express_yoy_sales",
            "ts_express_yoy_op",
            "ts_express_yoy_tp",
            "ts_express_yoy_dedu_np",
            "ts_express_yoy_net_profit",
            "ts_express_diluted_eps",
            "ts_audit_opinion_risk",
            "ts_pledge_ratio",
            "ts_pledge_amount",
            "ts_share_float",
            "ts_share_float_ratio",
            "ts_holder_num",
        )
        missing_tushare = [col for col in tushare_numeric_cols if col not in df.columns]
        if missing_tushare:
            df = df.assign(**{col: np.nan for col in missing_tushare})
        df[list(tushare_numeric_cols)] = df[list(tushare_numeric_cols)].apply(pd.to_numeric, errors="coerce")

        pe_ttm = pd.to_numeric(df["ts_pe_ttm"], errors="coerce")
        pb = pd.to_numeric(df["ts_pb"], errors="coerce")
        ps_ttm = pd.to_numeric(df["ts_ps_ttm"], errors="coerce")
        dv_ttm = pd.to_numeric(df["ts_dv_ttm"], errors="coerce")

        pe_score = 1.0 - self._scale_clip(pe_ttm.where(pe_ttm > 0), low=8.0, high=60.0)
        pb_score = 1.0 - self._scale_clip(pb.where(pb > 0), low=0.8, high=8.0)
        ps_score = 1.0 - self._scale_clip(ps_ttm.where(ps_ttm > 0), low=0.6, high=12.0)
        div_score = self._scale_clip(dv_ttm, low=0.0, high=4.0)
        valuation_score = (
            0.40 * pe_score.fillna(0.5)
            + 0.30 * pb_score.fillna(0.5)
            + 0.20 * ps_score.fillna(0.5)
            + 0.10 * div_score.fillna(0.5)
        )
        valuation_score = np.where(pe_ttm.fillna(1.0) <= 0, np.maximum(0.05, valuation_score - 0.25), valuation_score)

        amount_base = pd.to_numeric(df["amount"], errors="coerce").replace(0.0, np.nan)
        net_mf_ratio = (pd.to_numeric(df["ts_net_mf_amount"], errors="coerce") / amount_base).replace([np.inf, -np.inf], np.nan)
        main_mf_ratio = (
            pd.to_numeric(df["ts_main_net_mf_amount"], errors="coerce") / amount_base
        ).replace([np.inf, -np.inf], np.nan)
        elg_mf_ratio = (
            (
                pd.to_numeric(df["ts_buy_elg_amount"], errors="coerce")
                - pd.to_numeric(df["ts_sell_elg_amount"], errors="coerce")
            )
            / amount_base
        ).replace([np.inf, -np.inf], np.nan)
        moneyflow_score = (
            0.40 * self._scale_clip(net_mf_ratio, low=-0.20, high=0.20).fillna(0.5)
            + 0.35 * self._scale_clip(main_mf_ratio, low=-0.15, high=0.15).fillna(0.5)
            + 0.25 * self._scale_clip(elg_mf_ratio, low=-0.12, high=0.12).fillna(0.5)
        )

        turnover_rate_eff = pd.to_numeric(df["ts_turnover_rate_f"], errors="coerce").fillna(
            pd.to_numeric(df["ts_turnover_rate"], errors="coerce")
        )
        volume_ratio = pd.to_numeric(df["ts_volume_ratio"], errors="coerce")
        circ_mv = pd.to_numeric(df["ts_circ_mv"], errors="coerce")
        turnover_rate_score = self._scale_clip(turnover_rate_eff, low=0.5, high=8.0)
        volume_ratio_score = self._scale_clip(volume_ratio, low=0.7, high=3.0)
        circ_mv_score = self._scale_clip(circ_mv, low=80_000.0, high=3_000_000.0)

        up_limit = pd.to_numeric(df["ts_up_limit"], errors="coerce")
        down_limit = pd.to_numeric(df["ts_down_limit"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        band = (up_limit - down_limit).where(lambda x: x > 0)
        mid = (up_limit + down_limit) / 2.0
        limit_distance = ((close - mid).abs() / band).replace([np.inf, -np.inf], np.nan)
        limit_space_score = (1.0 - limit_distance.clip(0.0, 1.0)).fillna(0.5)
        tradability_score = (
            0.35 * turnover_rate_score.fillna(0.5)
            + 0.25 * volume_ratio_score.fillna(0.5)
            + 0.20 * circ_mv_score.fillna(0.5)
            + 0.20 * limit_space_score.fillna(0.5)
        )

        # Statement quality from income + balance sheet + cashflow.
        income_revenue = pd.to_numeric(df["ts_income_total_revenue"], errors="coerce").fillna(
            pd.to_numeric(df["ts_income_revenue"], errors="coerce")
        )
        income_operate_profit = pd.to_numeric(df["ts_income_operate_profit"], errors="coerce")
        income_net_profit = pd.to_numeric(df["ts_income_net_profit_attr"], errors="coerce")
        bs_total_assets = pd.to_numeric(df["ts_bs_total_assets"], errors="coerce")
        bs_total_liab = pd.to_numeric(df["ts_bs_total_liab"], errors="coerce")
        bs_cur_assets = pd.to_numeric(df["ts_bs_total_cur_assets"], errors="coerce")
        bs_cur_liab = pd.to_numeric(df["ts_bs_total_cur_liab"], errors="coerce")
        cf_operate = pd.to_numeric(df["ts_cf_operate_cashflow"], errors="coerce")

        statement_op_margin = (income_operate_profit / income_revenue.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
        statement_ocf_to_np = (cf_operate / income_net_profit.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
        statement_debt_to_asset = (bs_total_liab / bs_total_assets.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
        statement_current_ratio = (bs_cur_assets / bs_cur_liab.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)

        op_margin_score = self._scale_clip(statement_op_margin, low=0.02, high=0.22)
        ocf_np_score = self._scale_clip(statement_ocf_to_np, low=0.3, high=1.5)
        debt_asset_score = 1.0 - self._scale_clip(statement_debt_to_asset, low=0.25, high=0.80)
        current_ratio_score = self._scale_clip(statement_current_ratio, low=0.8, high=2.5)
        statement_quality_score = (
            0.30 * op_margin_score.fillna(0.5)
            + 0.30 * ocf_np_score.fillna(0.5)
            + 0.20 * debt_asset_score.fillna(0.5)
            + 0.20 * current_ratio_score.fillna(0.5)
        )
        statement_completeness = pd.concat(
            [statement_op_margin, statement_ocf_to_np, statement_debt_to_asset, statement_current_ratio],
            axis=1,
        ).notna().sum(axis=1) / 4.0
        statement_quality_score = statement_quality_score * statement_completeness + 0.5 * (1.0 - statement_completeness)

        # Merge statement quality into core fundamental score so all legacy strategies/risk gates benefit.
        score = (
            0.70 * pd.to_numeric(df["fundamental_score"], errors="coerce").fillna(0.5)
            + 0.30 * statement_quality_score.fillna(0.5)
        )
        score = np.clip(score, 0.0, 1.0)
        df["fundamental_statement_quality_score"] = np.clip(statement_quality_score.fillna(0.5), 0.0, 1.0)
        df["fundamental_statement_completeness"] = np.clip(statement_completeness.fillna(0.0), 0.0, 1.0)
        df["fundamental_score"] = score

        # Disclosure/event risk from forecast + express + audit.
        forecast_mid = pd.to_numeric(df["ts_forecast_pchg_mid"], errors="coerce").fillna(
            (
                pd.to_numeric(df["ts_forecast_pchg_min"], errors="coerce")
                + pd.to_numeric(df["ts_forecast_pchg_max"], errors="coerce")
            )
            / 2.0
        )
        express_growth = pd.to_numeric(df["ts_express_yoy_net_profit"], errors="coerce").fillna(
            0.6 * pd.to_numeric(df["ts_express_yoy_dedu_np"], errors="coerce")
            + 0.4 * pd.to_numeric(df["ts_express_yoy_sales"], errors="coerce")
        )
        audit_risk = pd.to_numeric(df["ts_audit_opinion_risk"], errors="coerce").clip(0.0, 1.0)

        forecast_score = self._scale_clip(forecast_mid, low=-50.0, high=45.0)
        express_score = self._scale_clip(express_growth, low=-40.0, high=45.0)
        audit_score = (1.0 - audit_risk.fillna(0.5)).clip(0.0, 1.0)
        disclosure_score = (
            0.35 * forecast_score.fillna(0.5)
            + 0.35 * express_score.fillna(0.5)
            + 0.30 * audit_score.fillna(0.5)
        )
        disclosure_neg = pd.concat(
            [
                (1.0 - forecast_score.fillna(0.5)),
                (1.0 - express_score.fillna(0.5)),
                audit_risk.fillna(0.5),
            ],
            axis=1,
        ).max(axis=1)
        disclosure_risk_score = disclosure_neg.clip(0.0, 1.0)

        # Ownership/supply overhang from pledge + float unlock + holder crowding.
        pledge_ratio = pd.to_numeric(df["ts_pledge_ratio"], errors="coerce")
        share_float = pd.to_numeric(df["ts_share_float"], errors="coerce")
        holder_num = pd.to_numeric(df["ts_holder_num"], errors="coerce")
        share_float_unlock_ratio = (share_float / share_float.shift(1).replace(0.0, np.nan) - 1.0).replace(
            [np.inf, -np.inf], np.nan
        )
        holder_crowding_ratio = (holder_num / holder_num.shift(1).replace(0.0, np.nan) - 1.0).replace(
            [np.inf, -np.inf], np.nan
        )

        pledge_risk = self._scale_clip(pledge_ratio, low=8.0, high=60.0)
        unlock_risk = self._scale_clip(share_float_unlock_ratio, low=0.0, high=0.30)
        holder_crowding_risk = self._scale_clip(holder_crowding_ratio, low=0.0, high=0.25)
        overhang_risk_score = (
            0.45 * pledge_risk.fillna(0.5)
            + 0.30 * unlock_risk.fillna(0.5)
            + 0.25 * holder_crowding_risk.fillna(0.5)
        )
        overhang_score = (1.0 - overhang_risk_score).clip(0.0, 1.0)

        # Final advanced score with explicit weights for all newly integrated groups.
        advanced_raw = (
            0.22 * valuation_score
            + 0.20 * moneyflow_score
            + 0.18 * tradability_score
            + 0.15 * statement_quality_score.fillna(0.5)
            + 0.13 * disclosure_score.fillna(0.5)
            + 0.12 * overhang_score.fillna(0.5)
        )

        advanced_key_cols = [
            "ts_pe_ttm",
            "ts_pb",
            "ts_ps_ttm",
            "ts_turnover_rate",
            "ts_net_mf_amount",
            "ts_up_limit",
            "ts_down_limit",
            "ts_income_total_revenue",
            "ts_bs_total_assets",
            "ts_cf_operate_cashflow",
            "ts_forecast_pchg_mid",
            "ts_express_yoy_net_profit",
            "ts_audit_opinion_risk",
            "ts_pledge_ratio",
            "ts_share_float",
            "ts_holder_num",
        ]
        advanced_completeness = df[advanced_key_cols].notna().sum(axis=1) / float(len(advanced_key_cols))
        advanced_score = advanced_raw * advanced_completeness + 0.5 * (1.0 - advanced_completeness)
        advanced_score = np.clip(advanced_score, 0.0, 1.0)

        advanced_completeness_clipped = np.clip(advanced_completeness.fillna(0.0), 0.0, 1.0)
        df = df.assign(
            tushare_statement_quality_score=np.clip(statement_quality_score.fillna(0.5), 0.0, 1.0),
            tushare_statement_debt_to_asset=statement_debt_to_asset,
            tushare_statement_current_ratio=statement_current_ratio,
            tushare_statement_ocf_to_np=statement_ocf_to_np,
            tushare_disclosure_score=np.clip(disclosure_score.fillna(0.5), 0.0, 1.0),
            tushare_disclosure_risk_score=np.clip(disclosure_risk_score.fillna(0.5), 0.0, 1.0),
            tushare_forecast_pchg_mid=forecast_mid,
            tushare_audit_opinion_risk=audit_risk.fillna(0.5),
            tushare_overhang_score=np.clip(overhang_score.fillna(0.5), 0.0, 1.0),
            tushare_overhang_risk_score=np.clip(overhang_risk_score.fillna(0.5), 0.0, 1.0),
            tushare_pledge_ratio=pledge_ratio,
            tushare_share_float_unlock_ratio=share_float_unlock_ratio,
            tushare_holder_crowding_ratio=holder_crowding_ratio,
            tushare_valuation_score=np.clip(valuation_score, 0.0, 1.0),
            tushare_moneyflow_score=np.clip(moneyflow_score, 0.0, 1.0),
            tushare_tradability_score=np.clip(tradability_score, 0.0, 1.0),
            tushare_advanced_completeness=advanced_completeness_clipped,
            tushare_advanced_available=advanced_completeness_clipped > 0.0,
            tushare_advanced_score=advanced_score,
        )

        return df

    @staticmethod
    def _scale_clip(series: pd.Series, low: float, high: float) -> pd.Series:
        if high <= low:
            return pd.Series(np.full(len(series), 0.5), index=series.index, dtype=float)
        out = (series - low) / (high - low)
        return out.clip(0.0, 1.0)
