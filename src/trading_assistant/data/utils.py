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
