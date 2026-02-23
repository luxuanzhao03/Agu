from __future__ import annotations

from datetime import date, datetime, timedelta
import logging
import math
import re
from typing import Any

import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.utils import date_to_yyyymmdd, normalize_akshare_daily, normalize_akshare_intraday

logger = logging.getLogger(__name__)


class AkshareProvider(MarketDataProvider):
    name = "akshare"

    _INTRADAY_INTERVAL_MAP: dict[str, str] = {
        "1m": "1",
        "5m": "5",
        "15m": "15",
        "30m": "30",
        "60m": "60",
        "1h": "60",
    }

    def __init__(self) -> None:
        import akshare as ak

        self._ak = ak

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        raw = self._ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=date_to_yyyymmdd(start_date),
            end_date=date_to_yyyymmdd(end_date),
            adjust="qfq",
        )
        return normalize_akshare_daily(raw, symbol=symbol)

    def get_intraday_bars(
        self,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        *,
        interval: str = "15m",
    ) -> pd.DataFrame:
        interval_key = str(interval).strip().lower()
        period = self._INTRADAY_INTERVAL_MAP.get(interval_key)
        if period is None:
            raise ValueError(f"unsupported interval '{interval}', use one of: {', '.join(sorted(self._INTRADAY_INTERVAL_MAP))}")

        start_text = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
        end_text = end_datetime.strftime("%Y-%m-%d %H:%M:%S")

        # Preferred endpoint for A-share minute bars on Eastmoney.
        fetchers: list[tuple[str, dict[str, Any]]] = [
            (
                "stock_zh_a_hist_min_em",
                {
                    "symbol": symbol,
                    "start_date": start_text,
                    "end_date": end_text,
                    "period": period,
                    "adjust": "qfq",
                },
            ),
            (
                "stock_zh_a_hist_min",
                {
                    "symbol": symbol,
                    "period": period,
                    "start_date": start_text,
                    "end_date": end_text,
                    "adjust": "qfq",
                },
            ),
        ]

        errors: list[str] = []
        for method_name, kwargs in fetchers:
            method = getattr(self._ak, method_name, None)
            if method is None:
                continue
            try:
                raw = method(**kwargs)
                frame = normalize_akshare_intraday(raw, symbol=symbol, interval=interval_key)
                if frame is None or frame.empty:
                    raise ValueError("empty intraday result")
                frame = frame[
                    (frame["bar_time"] >= pd.to_datetime(start_datetime))
                    & (frame["bar_time"] <= pd.to_datetime(end_datetime))
                ].copy()
                if frame.empty:
                    raise ValueError("no rows in requested intraday range")
                return frame.reset_index(drop=True)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{method_name}: {exc}")

        details = "; ".join(errors) if errors else "no available akshare intraday method"
        raise RuntimeError(f"failed to fetch akshare intraday bars for {symbol}: {details}")

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        raw = self._ak.tool_trade_date_hist_sina()
        calendar = raw.rename(columns={"trade_date": "trade_date"}).copy()
        calendar["trade_date"] = pd.to_datetime(calendar["trade_date"]).dt.date
        mask = (calendar["trade_date"] >= start_date) & (calendar["trade_date"] <= end_date)
        result = calendar.loc[mask, ["trade_date"]].copy()
        result["is_open"] = True
        return result

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        spot = self._ak.stock_zh_a_spot_em()
        match = spot.loc[spot["代码"] == symbol]
        if match.empty:
            return {"is_st": False, "is_suspended": False}
        name = str(match.iloc[0].get("名称", ""))
        is_st = "ST" in name.upper()
        return {"is_st": is_st, "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        errors: list[str] = []
        for fetcher_name in ("_fetch_financial_analysis_indicator", "_fetch_financial_abstract"):
            fetcher = getattr(self, fetcher_name)
            try:
                frame = fetcher(symbol=symbol, as_of=as_of)
                snapshot = self._normalize_fundamental_frame(frame=frame, as_of=as_of)
                if snapshot:
                    return snapshot
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{fetcher_name}: {exc}")
        raise RuntimeError(
            "failed to load fundamental snapshot from akshare; "
            + ("; ".join(errors) if errors else "no usable dataset")
        )

    def get_corporate_event_snapshot(
        self,
        symbol: str,
        as_of: date,
        *,
        lookback_days: int = 120,
    ) -> dict[str, object]:
        report_dates = self._build_recent_quarter_dates(as_of=as_of, limit=max(2, min(8, lookback_days // 45 + 2)))
        pos_score = 0.0
        neg_score = 0.0
        event_count = 0
        latest_publish_date: date | None = None
        latest_report_date: date | None = None
        latest_forecast_mid: float | None = None
        latest_express_growth: float | None = None

        for rep in report_dates:
            rep_text = rep.strftime("%Y%m%d")
            yjyg = self._safe_api_call("stock_yjyg_em", date=rep_text)
            if yjyg is not None and not yjyg.empty:
                for _, row in self._filter_symbol_rows(yjyg, symbol=symbol).iterrows():
                    pub = self._extract_row_date(row)
                    if pub is not None and pub > as_of:
                        continue
                    event_count += 1
                    if pub is not None and (latest_publish_date is None or pub > latest_publish_date):
                        latest_publish_date = pub
                    if rep <= as_of and (latest_report_date is None or rep > latest_report_date):
                        latest_report_date = rep

                    p_min = self._extract_row_numeric(row, ("下限", "最小", "p_change_min", "变动"))
                    p_max = self._extract_row_numeric(row, ("上限", "最大", "p_change_max", "变动"))
                    if p_min is not None and p_max is not None:
                        mid = (p_min + p_max) / 2.0
                    else:
                        mid = p_min if p_min is not None else p_max
                    if mid is None:
                        continue
                    latest_forecast_mid = float(mid)
                    age_days = (as_of - pub).days if pub is not None else max(1, int(lookback_days))
                    decay = math.exp(-math.log(2.0) * max(0, age_days) / 60.0)
                    if mid >= 8.0:
                        pos_score += min(1.0, (mid - 6.0) / 40.0) * decay
                    elif mid <= -10.0:
                        neg_score += min(1.0, abs(mid + 8.0) / 45.0) * decay

            yjkb = self._safe_api_call("stock_yjkb_em", date=rep_text)
            if yjkb is not None and not yjkb.empty:
                for _, row in self._filter_symbol_rows(yjkb, symbol=symbol).iterrows():
                    pub = self._extract_row_date(row)
                    if pub is not None and pub > as_of:
                        continue
                    event_count += 1
                    if pub is not None and (latest_publish_date is None or pub > latest_publish_date):
                        latest_publish_date = pub
                    if rep <= as_of and (latest_report_date is None or rep > latest_report_date):
                        latest_report_date = rep
                    yoy = self._extract_row_numeric(row, ("净利润同比", "yoy_net_profit", "增长", "同比"))
                    if yoy is None:
                        continue
                    latest_express_growth = float(yoy)
                    age_days = (as_of - pub).days if pub is not None else max(1, int(lookback_days))
                    decay = math.exp(-math.log(2.0) * max(0, age_days) / 75.0)
                    if yoy >= 8.0:
                        pos_score += min(1.0, (yoy - 6.0) / 55.0) * decay
                    elif yoy <= -12.0:
                        neg_score += min(1.0, abs(yoy + 10.0) / 65.0) * decay

        # Optional notice intensity for near-term event risk.
        notice_rows = 0
        for lag in range(0, min(6, max(2, lookback_days // 20))):
            day = as_of - timedelta(days=lag)
            day_text = day.strftime("%Y%m%d")
            notice = self._safe_api_call("stock_notice_report", symbol="全部", date=day_text)
            if notice is None or notice.empty:
                continue
            matched = self._filter_symbol_rows(notice, symbol=symbol)
            if matched.empty:
                continue
            notice_rows += len(matched)
        if notice_rows > 0:
            # More notices imply higher short-term uncertainty; cap the effect.
            neg_score += min(0.35, math.log1p(float(notice_rows)) / 8.5)

        event_score = self._clip01(pos_score)
        negative_event_score = self._clip01(neg_score)
        earnings_revision_score = self._clip01(0.5 + 0.7 * (event_score - negative_event_score))
        if latest_publish_date is None:
            disclosure_timing_score = 0.5
            freshness_days = None
        else:
            freshness_days = max(0, (as_of - latest_publish_date).days)
            disclosure_timing_score = self._clip01(1.0 - (freshness_days / 180.0))

        return {
            "event_score": float(event_score),
            "negative_event_score": float(negative_event_score),
            "earnings_revision_score": float(earnings_revision_score),
            "disclosure_timing_score": float(disclosure_timing_score),
            "event_count": int(event_count),
            "notice_count": int(notice_rows),
            "latest_publish_date": latest_publish_date,
            "latest_report_date": latest_report_date,
            "freshness_days": freshness_days,
            "forecast_pchg_mid": latest_forecast_mid,
            "express_yoy_net_profit": latest_express_growth,
        }

    def get_market_style_snapshot(
        self,
        as_of: date,
        *,
        lookback_days: int = 30,
    ) -> dict[str, object]:
        start_text = (as_of - timedelta(days=max(45, int(lookback_days) * 3))).strftime("%Y%m%d")
        end_text = as_of.strftime("%Y%m%d")

        hsgt = self._safe_api_call("stock_hsgt_fund_flow_summary_em")
        margin_sse = self._safe_api_call("stock_margin_sse", start_date=start_text, end_date=end_text)
        margin_szse = self._safe_api_call("stock_margin_szse", date=end_text)
        market_flow = self._safe_api_call("stock_market_fund_flow")
        theme_hot = self._safe_api_call("stock_hot_rank_em")

        flow_series = self._extract_numeric_series(
            hsgt,
            preferred_keywords=("净", "north", "net", "buy", "amt", "flow"),
        )
        if flow_series is None:
            flow_series = self._extract_numeric_series(
                market_flow,
                preferred_keywords=("主力", "净", "main", "net", "flow", "amount"),
            )

        flow_score = 0.5
        flow_z = 0.0
        flow_5d: float | None = None
        flow_20d: float | None = None
        if flow_series is not None and len(flow_series) >= 3:
            flow_5d = float(flow_series.tail(min(5, len(flow_series))).mean())
            flow_20d = float(flow_series.tail(min(20, len(flow_series))).mean())
            std = float(flow_series.tail(min(120, len(flow_series))).std(ddof=0) or 0.0)
            denom = max(1e-9, std if std > 0 else abs(flow_series.tail(min(20, len(flow_series))).mean()) or 1.0)
            flow_z = (flow_5d - flow_20d) / denom
            flow_score = self._clip01(0.5 + flow_z * 0.18)

        margin_series = self._extract_numeric_series(
            margin_sse,
            preferred_keywords=("融资余额", "rzye", "balance", "margin", "financing"),
        )
        leverage_score = 0.5
        leverage_z = 0.0
        margin_5d: float | None = None
        margin_20d: float | None = None
        if margin_series is not None and len(margin_series) >= 3:
            margin_5d = float(margin_series.tail(min(5, len(margin_series))).mean())
            margin_20d = float(margin_series.tail(min(20, len(margin_series))).mean())
            std = float(margin_series.tail(min(120, len(margin_series))).std(ddof=0) or 0.0)
            denom = max(1e-9, std if std > 0 else abs(margin_series.tail(min(20, len(margin_series))).mean()) or 1.0)
            leverage_z = (margin_5d - margin_20d) / denom
            leverage_score = self._clip01(0.5 + leverage_z * 0.16)
        else:
            # Use single-day SZSE summary as weak backup.
            sz_margin = self._extract_numeric_series(
                margin_szse,
                preferred_keywords=("融资余额", "rzye", "balance", "margin", "financing"),
            )
            if sz_margin is not None and len(sz_margin) > 0:
                leverage_score = self._clip01(0.5 + (float(sz_margin.iloc[-1]) > 0) * 0.03)

        theme_heat_score = 0.5
        hot_size = int(len(theme_hot.index)) if isinstance(theme_hot, pd.DataFrame) else 0
        if hot_size > 0:
            theme_heat_score = self._clip01(0.35 + min(300, hot_size) / 600.0)
        theme_heat_score = self._clip01(theme_heat_score + min(2.5, abs(flow_z)) * 0.08)

        risk_on_score = self._clip01(0.56 * flow_score + 0.32 * leverage_score + 0.12 * theme_heat_score)
        if risk_on_score >= 0.58:
            regime = "RISK_ON"
        elif risk_on_score <= 0.42:
            regime = "RISK_OFF"
        else:
            regime = "NEUTRAL"

        return {
            "risk_on_score": float(risk_on_score),
            "flow_score": float(flow_score),
            "leverage_score": float(leverage_score),
            "theme_heat_score": float(theme_heat_score),
            "northbound_net_5d": flow_5d,
            "northbound_net_20d": flow_20d,
            "margin_balance_5d": margin_5d,
            "margin_balance_20d": margin_20d,
            "hot_rank_count": hot_size,
            "regime": regime,
        }

    def _fetch_financial_analysis_indicator(self, symbol: str, as_of: date) -> pd.DataFrame:
        start_year = str(max(1900, as_of.year - 6))
        return self._ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start_year)

    def _fetch_financial_abstract(self, symbol: str, as_of: date) -> pd.DataFrame:
        _ = as_of
        return self._ak.stock_financial_abstract(symbol=symbol)

    def _safe_api_call(self, method_name: str, **kwargs) -> pd.DataFrame:
        method = getattr(self._ak, method_name, None)
        if method is None:
            return pd.DataFrame()
        try:
            frame = method(**kwargs)
        except Exception as exc:  # noqa: BLE001
            logger.warning("akshare method %s failed: %s", method_name, exc)
            return pd.DataFrame()
        if frame is None or not isinstance(frame, pd.DataFrame):
            return pd.DataFrame()
        return frame

    @staticmethod
    def _clip01(value: float) -> float:
        return max(0.0, min(1.0, float(value)))

    @staticmethod
    def _build_recent_quarter_dates(*, as_of: date, limit: int = 6) -> list[date]:
        points: list[date] = []
        for year in range(as_of.year - 2, as_of.year + 1):
            for month, day in ((3, 31), (6, 30), (9, 30), (12, 31)):
                try:
                    d = date(year, month, day)
                except Exception:  # noqa: BLE001
                    continue
                if d <= as_of:
                    points.append(d)
        points = sorted(points, reverse=True)
        return points[: max(1, int(limit))]

    @staticmethod
    def _find_symbol_column(frame: pd.DataFrame) -> str | None:
        if frame is None or frame.empty:
            return None
        for col in frame.columns:
            key = str(col).strip().lower()
            if key in {"代码", "股票代码", "symbol", "ts_code", "证券代码"}:
                return str(col)
            if "code" in key or "代码" in key:
                return str(col)
        return None

    def _filter_symbol_rows(self, frame: pd.DataFrame, *, symbol: str) -> pd.DataFrame:
        if frame is None or frame.empty:
            return pd.DataFrame(columns=list(frame.columns) if isinstance(frame, pd.DataFrame) else [])
        out = frame.copy()
        sym_col = self._find_symbol_column(out)
        if sym_col is None:
            # If no symbol column, return as-is (caller can still parse aggregate signals).
            return out
        key = str(symbol).strip().upper()
        vals = out[sym_col].astype(str).str.strip().str.upper()
        matched = out[vals == key].copy()
        if not matched.empty:
            return matched
        # Some sources include exchange suffix, e.g. 000001.SZ.
        matched = out[vals.str.startswith(key)].copy()
        return matched

    @staticmethod
    def _extract_row_date(row: pd.Series) -> date | None:
        for col in row.index:
            key = str(col).strip().lower()
            if "date" in key or "时间" in key or "日期" in key or "公告" in key:
                parsed = AkshareProvider._parse_date(row.get(col))
                if parsed is not None:
                    return parsed
        return None

    @staticmethod
    def _extract_row_numeric(row: pd.Series, keywords: tuple[str, ...]) -> float | None:
        best_val: float | None = None
        best_score = -1
        for col in row.index:
            key = str(col).strip().lower()
            score = 0
            for idx, kw in enumerate(keywords):
                kw_key = str(kw).strip().lower()
                if kw_key and kw_key in key:
                    score += (len(keywords) - idx) * 4
            if score <= 0:
                continue
            parsed = AkshareProvider._parse_float(row.get(col))
            if parsed is None:
                continue
            if score > best_score:
                best_score = score
                best_val = float(parsed)
        return best_val

    @staticmethod
    def _find_date_column(frame: pd.DataFrame) -> str | None:
        if frame is None or frame.empty:
            return None
        for col in frame.columns:
            key = str(col).strip().lower()
            if key in {"trade_date", "date", "日期"}:
                return str(col)
            if "date" in key or "时间" in key or "日期" in key:
                return str(col)
        return None

    def _extract_numeric_series(
        self,
        frame: pd.DataFrame,
        *,
        preferred_keywords: tuple[str, ...],
    ) -> pd.Series | None:
        if frame is None or frame.empty:
            return None
        date_col = self._find_date_column(frame)
        if date_col is not None:
            out = frame.copy()
            out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
            out = out[out[date_col].notna()].sort_values(date_col)
        else:
            out = frame.copy()

        best_col: str | None = None
        best_score = -1
        for col in out.columns:
            if str(col) == str(date_col):
                continue
            ser = pd.to_numeric(out[col], errors="coerce")
            valid = int(ser.notna().sum())
            if valid <= 0:
                continue
            key = str(col).strip().lower()
            score = min(valid, 200)
            for idx, kw in enumerate(preferred_keywords):
                kw_key = str(kw).strip().lower()
                if kw_key and kw_key in key:
                    score += (len(preferred_keywords) - idx) * 4
            if score > best_score:
                best_score = score
                best_col = str(col)
        if best_col is None:
            return None
        ser = pd.to_numeric(out[best_col], errors="coerce").dropna().astype(float)
        if ser.empty:
            return None
        return ser.reset_index(drop=True)

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        text = text.replace("/", "-").replace(".", "-")
        # Keep only the first date-like token.
        match = re.search(r"\d{4}-\d{1,2}-\d{1,2}", text)
        if match:
            text = match.group(0)
        else:
            match = re.search(r"\d{8}", text)
            if match:
                token = match.group(0)
                text = f"{token[0:4]}-{token[4:6]}-{token[6:8]}"
        try:
            parsed = pd.to_datetime(text, errors="coerce")
        except Exception:  # noqa: BLE001
            return None
        if pd.isna(parsed):
            return None
        return parsed.date()

    @staticmethod
    def _parse_float(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            out = float(value)
            if pd.isna(out):
                return None
            return out
        text = str(value).strip()
        if not text or text in {"--", "nan", "None", "N/A", "null"}:
            return None
        is_pct = "%" in text
        text = text.replace("%", "").replace(",", "").replace("，", "")
        text = text.replace("倍", "").replace("x", "").replace("X", "")
        text = text.strip()
        try:
            out = float(text)
        except Exception:  # noqa: BLE001
            return None
        if is_pct:
            return out
        return out

    @classmethod
    def _pick_metric_value(cls, values: dict[str, Any], aliases: list[str]) -> float | None:
        if not values:
            return None
        normalized = {str(k).strip().lower(): v for k, v in values.items() if str(k).strip()}
        for alias in aliases:
            key = alias.strip().lower()
            if key in normalized:
                parsed = cls._parse_float(normalized[key])
                if parsed is not None:
                    return parsed
        for alias in aliases:
            key = alias.strip().lower()
            for cand, val in normalized.items():
                if key in cand or cand in key:
                    parsed = cls._parse_float(val)
                    if parsed is not None:
                        return parsed
        return None

    def _normalize_fundamental_frame(self, frame: pd.DataFrame, as_of: date) -> dict[str, object]:
        if frame is None or frame.empty:
            return {}
        df = frame.copy()
        df.columns = [str(c).strip() for c in df.columns]
        df = df.reset_index(drop=True)

        row_values: dict[str, Any] = {}
        report_date: date | None = None
        publish_date: date | None = None

        metric_col = str(df.columns[0])
        metric_col_lower = metric_col.lower()
        is_pivot = metric_col_lower in {"指标", "选项", "metric", "项目"} or any(
            word in metric_col_lower for word in ("指标", "metric", "项目")
        )

        if is_pivot and len(df.columns) >= 2:
            date_candidates: list[tuple[date, str]] = []
            for col in df.columns[1:]:
                d = self._parse_date(col)
                if d is not None:
                    date_candidates.append((d, col))
            if date_candidates:
                eligible = [x for x in date_candidates if x[0] <= as_of]
                target_date, target_col = max(eligible or date_candidates, key=lambda x: x[0])
                report_date = target_date
                for _, row in df.iterrows():
                    metric_name = str(row.get(metric_col, "")).strip()
                    if metric_name:
                        row_values[metric_name] = row.get(target_col)
            else:
                first = df.iloc[0].to_dict()
                row_values.update(first)
        else:
            report_cols = [
                "报告期",
                "报告日期",
                "报告时间",
                "截止日期",
                "end_date",
                "report_date",
            ]
            publish_cols = [
                "公告日期",
                "披露日期",
                "公告时间",
                "发布时间",
                "ann_date",
                "publish_date",
            ]
            report_col = next((c for c in report_cols if c in df.columns), None)
            publish_col = next((c for c in publish_cols if c in df.columns), None)
            scored_rows: list[tuple[date, date, pd.Series]] = []
            for _, row in df.iterrows():
                pub = self._parse_date(row.get(publish_col)) if publish_col else None
                rep = self._parse_date(row.get(report_col)) if report_col else None
                score_date = pub or rep
                if score_date is None:
                    continue
                if score_date <= as_of:
                    scored_rows.append((pub or rep or as_of, rep or pub or as_of, row))
            if scored_rows:
                selected = max(scored_rows, key=lambda x: (x[0], x[1]))[2]
            else:
                selected = df.iloc[0]
            row_values.update(selected.to_dict())
            report_date = self._parse_date(selected.get(report_col)) if report_col else None
            publish_date = self._parse_date(selected.get(publish_col)) if publish_col else None

        if not row_values:
            return {}

        roe = self._pick_metric_value(row_values, ["净资产收益率(%)", "净资产收益率", "ROE", "ROE(%)"])
        revenue_yoy = self._pick_metric_value(
            row_values,
            ["营业收入同比增长率(%)", "营业总收入同比增长率(%)", "营业收入同比增长", "营业总收入同比增长", "营收同比(%)"],
        )
        net_profit_yoy = self._pick_metric_value(
            row_values,
            ["净利润同比增长率(%)", "归母净利润同比增长率(%)", "归母净利润同比增长", "净利润同比增长", "扣非净利润同比增长率(%)"],
        )
        gross_margin = self._pick_metric_value(row_values, ["销售毛利率(%)", "毛利率(%)", "毛利率"])
        debt_to_asset = self._pick_metric_value(row_values, ["资产负债率(%)", "资产负债率"])
        ocf_to_profit = self._pick_metric_value(
            row_values,
            ["经营现金净流量/净利润", "经营活动现金流量净额/净利润", "经营现金流量净额/净利润", "经营现金流/净利润"],
        )
        eps = self._pick_metric_value(row_values, ["基本每股收益", "每股收益", "EPS", "eps"])
        publish_date = publish_date or self._parse_date(
            row_values.get("公告日期")
            or row_values.get("披露日期")
            or row_values.get("公告时间")
            or row_values.get("ann_date")
            or row_values.get("publish_date")
        )
        report_date = report_date or self._parse_date(
            row_values.get("报告期")
            or row_values.get("报告日期")
            or row_values.get("报告时间")
            or row_values.get("end_date")
            or row_values.get("report_date")
        )

        metrics = [roe, revenue_yoy, net_profit_yoy, gross_margin, debt_to_asset, ocf_to_profit, eps]
        if all(v is None for v in metrics):
            return {}

        return {
            "report_date": report_date,
            "publish_date": publish_date,
            "roe": roe,
            "revenue_yoy": revenue_yoy,
            "net_profit_yoy": net_profit_yoy,
            "gross_margin": gross_margin,
            "debt_to_asset": debt_to_asset,
            "ocf_to_profit": ocf_to_profit,
            "eps": eps,
        }
