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
        for col in ("roe", "revenue_yoy", "net_profit_yoy", "gross_margin", "debt_to_asset", "ocf_to_profit", "eps"):
            if col not in df.columns:
                df[col] = np.nan
            df[col] = pd.to_numeric(df[col], errors="coerce")
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

        return df

    @staticmethod
    def _scale_clip(series: pd.Series, low: float, high: float) -> pd.Series:
        if high <= low:
            return pd.Series(np.full(len(series), 0.5), index=series.index, dtype=float)
        out = (series - low) / (high - low)
        return out.clip(0.0, 1.0)
