from __future__ import annotations

from datetime import date
import re
from typing import Any

import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.utils import date_to_yyyymmdd, normalize_akshare_daily


class AkshareProvider(MarketDataProvider):
    name = "akshare"

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

    def _fetch_financial_analysis_indicator(self, symbol: str, as_of: date) -> pd.DataFrame:
        start_year = str(max(1900, as_of.year - 6))
        return self._ak.stock_financial_analysis_indicator(symbol=symbol, start_year=start_year)

    def _fetch_financial_abstract(self, symbol: str, as_of: date) -> pd.DataFrame:
        _ = as_of
        return self._ak.stock_financial_abstract(symbol=symbol)

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
