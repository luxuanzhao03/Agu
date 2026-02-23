from __future__ import annotations

from datetime import date
import hashlib

import pandas as pd


def date_to_yyyymmdd(value: date) -> str:
    return value.strftime("%Y%m%d")


def normalize_symbol_to_tushare(symbol: str) -> str:
    symbol = symbol.strip()
    if "." in symbol:
        return symbol.upper()
    if symbol.startswith(("6", "9")):
        return f"{symbol}.SH"
    return f"{symbol}.SZ"


def normalize_akshare_daily(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "is_suspended",
                "is_st",
            ]
        )

    column_map = {
        "日期": "trade_date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
    }
    normalized = df.rename(columns=column_map).copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.date
    normalized["symbol"] = symbol
    normalized["is_suspended"] = False
    normalized["is_st"] = False
    return normalized[
        [
            "trade_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "is_suspended",
            "is_st",
        ]
    ].sort_values("trade_date")


def normalize_akshare_intraday(df: pd.DataFrame, symbol: str, interval: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(
            columns=[
                "bar_time",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "interval",
                "is_suspended",
                "is_st",
            ]
        )

    column_aliases = {
        "bar_time": ("bar_time", "datetime", "time", "时间", "日期", "date"),
        "open": ("open", "开盘"),
        "close": ("close", "收盘"),
        "high": ("high", "最高"),
        "low": ("low", "最低"),
        "volume": ("volume", "成交量"),
        "amount": ("amount", "成交额", "成交额(元)", "成交金额"),
    }

    out = df.copy()
    rename_map: dict[str, str] = {}
    lower_cols = {str(col).strip().lower(): str(col) for col in out.columns}
    for target, candidates in column_aliases.items():
        found = None
        for cand in candidates:
            key = str(cand).strip().lower()
            if key in lower_cols:
                found = lower_cols[key]
                break
        if found is not None:
            rename_map[found] = target
    out = out.rename(columns=rename_map)

    required = ["bar_time", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in out.columns:
            return pd.DataFrame(
                columns=[
                    "bar_time",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "amount",
                    "interval",
                    "is_suspended",
                    "is_st",
                ]
            )

    out["bar_time"] = pd.to_datetime(out["bar_time"], errors="coerce")
    out = out[out["bar_time"].notna()].copy()
    if out.empty:
        return pd.DataFrame(
            columns=[
                "bar_time",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "interval",
                "is_suspended",
                "is_st",
            ]
        )

    for col in ("open", "high", "low", "close", "volume"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "amount" not in out.columns:
        out["amount"] = out["close"].fillna(0.0) * out["volume"].fillna(0.0)
    else:
        out["amount"] = pd.to_numeric(out["amount"], errors="coerce")
        amount_missing = out["amount"].isna()
        if amount_missing.any():
            out.loc[amount_missing, "amount"] = out.loc[amount_missing, "close"].fillna(0.0) * out.loc[
                amount_missing, "volume"
            ].fillna(0.0)

    out["symbol"] = symbol
    out["interval"] = interval
    out["is_suspended"] = False
    out["is_st"] = False

    out = out.dropna(subset=["open", "high", "low", "close"])
    if out.empty:
        return pd.DataFrame(
            columns=[
                "bar_time",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "interval",
                "is_suspended",
                "is_st",
            ]
        )

    return out[
        [
            "bar_time",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "amount",
            "interval",
            "is_suspended",
            "is_st",
        ]
    ].sort_values("bar_time")


def normalize_tushare_daily(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "trade_date",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "is_suspended",
                "is_st",
            ]
        )
    normalized = df.rename(
        columns={
            "ts_code": "symbol",
        }
    ).copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.date
    normalized["symbol"] = normalized["symbol"].str.split(".").str[0]
    normalized["is_suspended"] = False
    normalized["is_st"] = normalized.get("name", pd.Series([""] * len(normalized))).astype(str).str.contains("ST")
    return normalized[
        [
            "trade_date",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "vol",
            "amount",
            "is_suspended",
            "is_st",
        ]
    ].rename(columns={"vol": "volume"}).sort_values("trade_date")


def dataframe_content_hash(df: pd.DataFrame) -> str:
    if df.empty:
        return hashlib.sha256(b"empty").hexdigest()
    normalized = df.sort_index(axis=1).copy()
    for col in normalized.columns:
        if str(normalized[col].dtype).startswith("datetime"):
            normalized[col] = normalized[col].astype(str)
    csv_bytes = normalized.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(csv_bytes).hexdigest()
