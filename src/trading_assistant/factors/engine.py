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

        return df

