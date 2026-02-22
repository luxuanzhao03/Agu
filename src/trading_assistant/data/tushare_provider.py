from __future__ import annotations

from datetime import date
import logging
from typing import Any

import numpy as np
import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.utils import date_to_yyyymmdd, normalize_symbol_to_tushare, normalize_tushare_daily

logger = logging.getLogger(__name__)


class TushareProvider(MarketDataProvider):
    name = "tushare"

    _ADVANCED_DATASET_CATALOG: tuple[dict[str, Any], ...] = (
        {
            "dataset_name": "daily_basic",
            "api_name": "daily_basic",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "market_microstructure",
            "integrated_in_system": True,
            "integrated_targets": ["factor_engine", "multi_factor", "small_capital_adaptive"],
            "notes": "估值、换手、市值等关键字段。",
        },
        {
            "dataset_name": "moneyflow",
            "api_name": "moneyflow",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "market_microstructure",
            "integrated_in_system": True,
            "integrated_targets": ["factor_engine", "multi_factor", "small_capital_adaptive"],
            "notes": "主力/超大单净流入，可用于事件与趋势确认。",
        },
        {
            "dataset_name": "stk_limit",
            "api_name": "stk_limit",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "tradability",
            "integrated_in_system": True,
            "integrated_targets": ["factor_engine", "risk"],
            "notes": "涨跌停价格边界。",
        },
        {
            "dataset_name": "adj_factor",
            "api_name": "adj_factor",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "price_adjustment",
            "integrated_in_system": True,
            "integrated_targets": ["factor_engine"],
            "notes": "复权因子，可做一致性检查。",
        },
        {
            "dataset_name": "fina_indicator",
            "api_name": "fina_indicator",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental",
            "integrated_in_system": True,
            "integrated_targets": ["fundamental_enrichment", "strategy"],
            "notes": "财务指标快照主表。",
        },
        {
            "dataset_name": "income",
            "api_name": "income",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental",
            "integrated_in_system": True,
            "integrated_targets": ["fundamental_enrichment", "factor_engine", "strategy"],
            "notes": "利润表，生成经营质量因子。",
        },
        {
            "dataset_name": "balancesheet",
            "api_name": "balancesheet",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental",
            "integrated_in_system": True,
            "integrated_targets": ["fundamental_enrichment", "factor_engine", "risk"],
            "notes": "资产负债表，生成杠杆与偿债能力因子。",
        },
        {
            "dataset_name": "cashflow",
            "api_name": "cashflow",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental",
            "integrated_in_system": True,
            "integrated_targets": ["fundamental_enrichment", "factor_engine", "strategy"],
            "notes": "现金流量表，生成现金流质量因子。",
        },
        {
            "dataset_name": "forecast",
            "api_name": "forecast",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental_event",
            "integrated_in_system": True,
            "integrated_targets": ["event_governance", "factor_engine", "risk"],
            "notes": "业绩预告，生成盈利指引冲击因子。",
        },
        {
            "dataset_name": "express",
            "api_name": "express",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental_event",
            "integrated_in_system": True,
            "integrated_targets": ["event_governance", "factor_engine", "risk"],
            "notes": "业绩快报，生成经营改善/恶化因子。",
        },
        {
            "dataset_name": "dividend",
            "api_name": "dividend",
            "min_points_hint": 2000,
            "param_profile": "ts_code_only",
            "category": "corporate_action",
            "integrated_in_system": False,
            "integrated_targets": ["fundamental_enrichment"],
            "notes": "分红送转信息。",
        },
        {
            "dataset_name": "fina_audit",
            "api_name": "fina_audit",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "fundamental_quality",
            "integrated_in_system": True,
            "integrated_targets": ["risk", "factor_engine", "strategy"],
            "notes": "审计意见与审计机构，生成审计风险因子。",
        },
        {
            "dataset_name": "top10_holders",
            "api_name": "top10_holders",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "ownership",
            "integrated_in_system": False,
            "integrated_targets": ["research"],
            "notes": "前十大股东。",
        },
        {
            "dataset_name": "top10_floatholders",
            "api_name": "top10_floatholders",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "ownership",
            "integrated_in_system": False,
            "integrated_targets": ["research"],
            "notes": "前十大流通股东。",
        },
        {
            "dataset_name": "pledge_stat",
            "api_name": "pledge_stat",
            "min_points_hint": 2000,
            "param_profile": "ts_code_only",
            "category": "risk_event",
            "integrated_in_system": True,
            "integrated_targets": ["risk", "factor_engine", "small_capital_adaptive"],
            "notes": "股权质押统计，生成股权质押风险因子。",
        },
        {
            "dataset_name": "pledge_detail",
            "api_name": "pledge_detail",
            "min_points_hint": 2000,
            "param_profile": "ts_code_only",
            "category": "risk_event",
            "integrated_in_system": False,
            "integrated_targets": ["risk"],
            "notes": "股权质押明细。",
        },
        {
            "dataset_name": "repurchase",
            "api_name": "repurchase",
            "min_points_hint": 2000,
            "param_profile": "ts_code_only",
            "category": "corporate_action",
            "integrated_in_system": False,
            "integrated_targets": ["event_governance"],
            "notes": "回购事件。",
        },
        {
            "dataset_name": "block_trade",
            "api_name": "block_trade",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "microstructure",
            "integrated_in_system": False,
            "integrated_targets": ["research"],
            "notes": "大宗交易。",
        },
        {
            "dataset_name": "share_float",
            "api_name": "share_float",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "corporate_action",
            "integrated_in_system": True,
            "integrated_targets": ["risk", "factor_engine", "small_capital_adaptive"],
            "notes": "限售股解禁，生成供给冲击风险因子。",
        },
        {
            "dataset_name": "stk_holdernumber",
            "api_name": "stk_holdernumber",
            "min_points_hint": 2000,
            "param_profile": "ts_code_date_range",
            "category": "ownership",
            "integrated_in_system": True,
            "integrated_targets": ["research", "factor_engine", "risk"],
            "notes": "股东人数变化，生成拥挤度因子。",
        },
        {
            "dataset_name": "bak_daily",
            "api_name": "bak_daily",
            "min_points_hint": 5000,
            "param_profile": "ts_code_date_range",
            "category": "market_microstructure",
            "integrated_in_system": False,
            "integrated_targets": ["research"],
            "notes": "官方积分说明通常为 5000 档，2120 积分默认不可用。",
        },
    )

    def __init__(self, token: str | None) -> None:
        import tushare as ts

        if not token:
            raise ValueError("Tushare token is required when tushare provider is enabled.")
        self._pro = ts.pro_api(token)

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        ts_code = normalize_symbol_to_tushare(symbol)
        start_text = date_to_yyyymmdd(start_date)
        end_text = date_to_yyyymmdd(end_date)

        raw = self._pro.daily(ts_code=ts_code, start_date=start_text, end_date=end_text)
        bars = normalize_tushare_daily(raw)
        if bars.empty:
            return bars

        try:
            bars = self._enrich_daily_bars_from_advanced(
                bars=bars,
                ts_code=ts_code,
                start_date=start_text,
                end_date=end_text,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("tushare advanced enrichment failed for %s: %s", ts_code, exc)
        return bars

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        raw = self._pro.trade_cal(
            exchange="SSE",
            start_date=date_to_yyyymmdd(start_date),
            end_date=date_to_yyyymmdd(end_date),
        )
        calendar = raw.rename(columns={"cal_date": "trade_date", "is_open": "is_open"}).copy()
        calendar["trade_date"] = pd.to_datetime(calendar["trade_date"]).dt.date
        calendar["is_open"] = calendar["is_open"].astype(int).eq(1)
        return calendar[["trade_date", "is_open"]]

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        ts_code = normalize_symbol_to_tushare(symbol)
        basic = self._pro.stock_basic(ts_code=ts_code, fields="ts_code,name")
        if basic.empty:
            return {"is_st": False, "is_suspended": False}
        name = str(basic.iloc[0].get("name", ""))
        return {"is_st": "ST" in name.upper(), "is_suspended": False}

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        ts_code = normalize_symbol_to_tushare(symbol)
        start = date_to_yyyymmdd(date(max(1990, as_of.year - 6), 1, 1))
        end = date_to_yyyymmdd(as_of)
        try:
            frame = self._pro.fina_indicator(ts_code=ts_code, start_date=start, end_date=end)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"tushare fina_indicator failed: {exc}") from exc
        if frame is None or frame.empty:
            raise RuntimeError("tushare fina_indicator returned empty result")

        df = frame.copy()
        sort_cols = [c for c in ("ann_date", "end_date") if c in df.columns]
        if sort_cols:
            df = df.sort_values(by=sort_cols, ascending=False)
        selected = df.iloc[0]

        report_date = self._parse_date(selected.get("end_date"))
        publish_date = self._parse_date(selected.get("ann_date"))
        snapshot = {
            "report_date": report_date,
            "publish_date": publish_date,
            "roe": self._parse_float(selected.get("roe")),
            "revenue_yoy": self._parse_float(selected.get("or_yoy")),
            "net_profit_yoy": self._parse_float(selected.get("np_yoy")),
            "gross_margin": self._parse_float(selected.get("grossprofit_margin")),
            "debt_to_asset": self._parse_float(selected.get("debt_to_assets")),
            "ocf_to_profit": self._parse_float(selected.get("ocf_to_or")),
            "eps": self._parse_float(selected.get("eps")),
        }

        snapshot = self._augment_snapshot_with_statements(
            snapshot=snapshot,
            ts_code=ts_code,
            start_date=start,
            end_date=end,
        )

        if all(
            snapshot.get(k) is None
            for k in ("roe", "revenue_yoy", "net_profit_yoy", "gross_margin", "debt_to_asset", "ocf_to_profit")
        ):
            raise RuntimeError("tushare fina_indicator has no usable core metrics")
        return snapshot

    def _augment_snapshot_with_statements(
        self,
        *,
        snapshot: dict[str, object],
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> dict[str, object]:
        out = dict(snapshot)
        income = self._safe_fetch_dataset_by_name(
            dataset_name="income",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        bs = self._safe_fetch_dataset_by_name(
            dataset_name="balancesheet",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        cf = self._safe_fetch_dataset_by_name(
            dataset_name="cashflow",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )

        inc = self._latest_by_date(income)
        bal = self._latest_by_date(bs)
        cash = self._latest_by_date(cf)

        net_profit = self._parse_float(inc.get("n_income_attr_p"))
        operate_profit = self._parse_float(inc.get("operate_profit"))
        total_revenue = self._parse_float(inc.get("total_revenue")) or self._parse_float(inc.get("revenue"))
        total_assets = self._parse_float(bal.get("total_assets"))
        total_liab = self._parse_float(bal.get("total_liab"))
        total_cur_assets = self._parse_float(bal.get("total_cur_assets"))
        total_cur_liab = self._parse_float(bal.get("total_cur_liab"))
        operating_cashflow = self._parse_float(cash.get("n_cashflow_act"))

        if out.get("debt_to_asset") is None and total_assets and total_assets > 0 and total_liab is not None:
            out["debt_to_asset"] = 100.0 * float(total_liab) / float(total_assets)
        if out.get("ocf_to_profit") is None and net_profit and abs(net_profit) > 1e-9 and operating_cashflow is not None:
            out["ocf_to_profit"] = float(operating_cashflow) / float(net_profit)
        if out.get("gross_margin") is None and total_revenue and total_revenue > 0 and operate_profit is not None:
            out["gross_margin"] = 100.0 * float(operate_profit) / float(total_revenue)

        if (out.get("report_date") is None) and inc:
            out["report_date"] = self._parse_date(inc.get("end_date"))
        if (out.get("publish_date") is None) and inc:
            out["publish_date"] = self._parse_date(inc.get("ann_date") or inc.get("f_ann_date"))

        # Supplemental fields for downstream diagnostics and factor blending.
        out["statement_total_assets"] = total_assets
        out["statement_total_liab"] = total_liab
        out["statement_total_cur_assets"] = total_cur_assets
        out["statement_total_cur_liab"] = total_cur_liab
        out["statement_operating_cashflow"] = operating_cashflow
        out["statement_net_profit_attr"] = net_profit
        out["statement_operate_profit"] = operate_profit
        out["statement_total_revenue"] = total_revenue
        return out

    def _latest_by_date(self, frame: pd.DataFrame) -> dict[str, Any]:
        if frame is None or frame.empty:
            return {}
        df = frame.copy()
        date_col = next((c for c in ("ann_date", "f_ann_date", "end_date", "trade_date") if c in df.columns), None)
        if date_col is not None:
            df["_sort_date"] = pd.to_datetime(df[date_col], errors="coerce")
            df = df.sort_values("_sort_date", ascending=False)
        return dict(df.iloc[0].to_dict()) if not df.empty else {}

    def list_advanced_capabilities(self, user_points: int = 0) -> list[dict[str, Any]]:
        points = max(0, int(user_points))
        capabilities: list[dict[str, Any]] = []
        for item in self._ADVANCED_DATASET_CATALOG:
            min_points = int(item.get("min_points_hint", 0) or 0)
            api_name = str(item["api_name"])
            api_available = hasattr(self._pro, api_name)
            eligible = points >= min_points
            integrated_targets = list(item.get("integrated_targets", []))
            capabilities.append(
                {
                    "dataset_name": str(item["dataset_name"]),
                    "api_name": api_name,
                    "category": str(item.get("category", "")),
                    "min_points_hint": min_points,
                    "eligible": eligible,
                    "api_available": api_available,
                    "ready_to_call": bool(eligible and api_available),
                    "integrated_in_system": bool(item.get("integrated_in_system", False)),
                    "integrated_targets": integrated_targets,
                    "notes": str(item.get("notes", "")),
                }
            )
        capabilities.sort(
            key=lambda x: (
                not bool(x.get("integrated_in_system", False)),
                not bool(x.get("ready_to_call", False)),
                str(x.get("dataset_name", "")),
            )
        )
        return capabilities

    def prefetch_advanced_datasets(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        user_points: int = 0,
        include_ineligible: bool = False,
    ) -> dict[str, Any]:
        ts_code = normalize_symbol_to_tushare(symbol)
        start_text = date_to_yyyymmdd(start_date)
        end_text = date_to_yyyymmdd(end_date)

        capabilities = self.list_advanced_capabilities(user_points=user_points)
        results: list[dict[str, Any]] = []

        for capability in capabilities:
            dataset_name = str(capability["dataset_name"])
            if (not include_ineligible) and (not bool(capability.get("eligible", False))):
                results.append(
                    {
                        **capability,
                        "status": "skipped_ineligible",
                        "row_count": 0,
                        "column_count": 0,
                        "used_params": {},
                        "error": "",
                    }
                )
                continue
            if not bool(capability.get("api_available", False)):
                results.append(
                    {
                        **capability,
                        "status": "skipped_api_unavailable",
                        "row_count": 0,
                        "column_count": 0,
                        "used_params": {},
                        "error": "",
                    }
                )
                continue

            try:
                frame, used_params = self._fetch_dataset_by_spec(
                    spec=capability,
                    ts_code=ts_code,
                    start_date=start_text,
                    end_date=end_text,
                )
                results.append(
                    {
                        **capability,
                        "status": "success",
                        "row_count": int(len(frame)),
                        "column_count": int(len(frame.columns)),
                        "used_params": used_params,
                        "error": "",
                    }
                )
            except Exception as exc:  # noqa: BLE001
                results.append(
                    {
                        **capability,
                        "status": "failed",
                        "row_count": 0,
                        "column_count": 0,
                        "used_params": {},
                        "error": str(exc),
                    }
                )

        success = sum(1 for x in results if x.get("status") == "success")
        failed = sum(1 for x in results if x.get("status") == "failed")
        skipped = len(results) - success - failed

        return {
            "symbol": symbol,
            "ts_code": ts_code,
            "start_date": start_date,
            "end_date": end_date,
            "user_points": int(max(0, user_points)),
            "include_ineligible": bool(include_ineligible),
            "results": results,
            "summary": {
                "total": len(results),
                "success": success,
                "failed": failed,
                "skipped": skipped,
            },
        }

    def _enrich_daily_bars_from_advanced(
        self,
        bars: pd.DataFrame,
        *,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        merged = bars.copy()

        daily_basic_raw = self._safe_fetch_dataset_by_name(
            dataset_name="daily_basic",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        moneyflow_raw = self._safe_fetch_dataset_by_name(
            dataset_name="moneyflow",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        stk_limit_raw = self._safe_fetch_dataset_by_name(
            dataset_name="stk_limit",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        adj_factor_raw = self._safe_fetch_dataset_by_name(
            dataset_name="adj_factor",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        income_raw = self._safe_fetch_dataset_by_name(
            dataset_name="income",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        balancesheet_raw = self._safe_fetch_dataset_by_name(
            dataset_name="balancesheet",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        cashflow_raw = self._safe_fetch_dataset_by_name(
            dataset_name="cashflow",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        forecast_raw = self._safe_fetch_dataset_by_name(
            dataset_name="forecast",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        express_raw = self._safe_fetch_dataset_by_name(
            dataset_name="express",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        fina_audit_raw = self._safe_fetch_dataset_by_name(
            dataset_name="fina_audit",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        pledge_raw = self._safe_fetch_dataset_by_name(
            dataset_name="pledge_stat",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        share_float_raw = self._safe_fetch_dataset_by_name(
            dataset_name="share_float",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )
        holdernumber_raw = self._safe_fetch_dataset_by_name(
            dataset_name="stk_holdernumber",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
        )

        for frame in (
            self._normalize_daily_basic(daily_basic_raw),
            self._normalize_moneyflow(moneyflow_raw),
            self._normalize_stk_limit(stk_limit_raw),
            self._normalize_adj_factor(adj_factor_raw),
        ):
            merged = self._merge_by_trade_date(merged, frame)

        for frame in (
            self._normalize_income(income_raw),
            self._normalize_balancesheet(balancesheet_raw),
            self._normalize_cashflow(cashflow_raw),
            self._normalize_forecast(forecast_raw),
            self._normalize_express(express_raw),
            self._normalize_fina_audit(fina_audit_raw),
            self._normalize_pledge_stat(pledge_raw),
            self._normalize_share_float(share_float_raw),
            self._normalize_holdernumber(holdernumber_raw),
        ):
            merged = self._merge_by_asof_date(merged, frame)

        return merged.sort_values("trade_date").reset_index(drop=True)

    def _safe_fetch_dataset_by_name(
        self,
        *,
        dataset_name: str,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        try:
            frame, _ = self._fetch_dataset_by_name(
                dataset_name=dataset_name,
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
            )
            return frame
        except Exception as exc:  # noqa: BLE001
            logger.warning("tushare dataset '%s' fetch failed for %s: %s", dataset_name, ts_code, exc)
            return pd.DataFrame()

    def _fetch_dataset_by_name(
        self,
        *,
        dataset_name: str,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        spec = next((x for x in self._ADVANCED_DATASET_CATALOG if x["dataset_name"] == dataset_name), None)
        if spec is None:
            raise ValueError(f"unknown dataset: {dataset_name}")
        frame, used_params = self._fetch_dataset_by_spec(spec=spec, ts_code=ts_code, start_date=start_date, end_date=end_date)
        return frame, used_params

    def _fetch_dataset_by_spec(
        self,
        *,
        spec: dict[str, Any],
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> tuple[pd.DataFrame, dict[str, Any]]:
        api_name = str(spec["api_name"])
        profile = str(spec.get("param_profile", "none"))
        api = getattr(self._pro, api_name, None)
        if api is None:
            raise ValueError(f"tushare api '{api_name}' is not available")

        errors: list[str] = []
        for params in self._build_param_candidates(profile=profile, ts_code=ts_code, start_date=start_date, end_date=end_date):
            try:
                frame = api(**params)
                if frame is None:
                    return pd.DataFrame(), params
                if not isinstance(frame, pd.DataFrame):
                    raise RuntimeError(f"API returned non-DataFrame type: {type(frame)}")
                return frame, params
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{params}: {exc}")
                continue

        summary = "; ".join(errors[:5])
        raise RuntimeError(f"tushare api '{api_name}' call failed after retries: {summary}")

    @staticmethod
    def _build_param_candidates(
        *,
        profile: str,
        ts_code: str,
        start_date: str,
        end_date: str,
    ) -> list[dict[str, Any]]:
        if profile == "ts_code_date_range":
            return [
                {"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
                {"ts_code": ts_code, "ann_date": end_date},
                {"ts_code": ts_code, "trade_date": end_date},
                {"ts_code": ts_code},
            ]
        if profile == "ts_code_only":
            return [{"ts_code": ts_code}]
        if profile == "date_range":
            return [
                {"start_date": start_date, "end_date": end_date},
                {"trade_date": end_date},
                {},
            ]
        if profile == "trade_date":
            return [{"trade_date": end_date}, {}]
        return [{}]

    def _normalize_daily_basic(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_trade_date_frame(
            frame,
            {
                "turnover_rate": "ts_turnover_rate",
                "turnover_rate_f": "ts_turnover_rate_f",
                "volume_ratio": "ts_volume_ratio",
                "pe": "ts_pe",
                "pe_ttm": "ts_pe_ttm",
                "pb": "ts_pb",
                "ps": "ts_ps",
                "ps_ttm": "ts_ps_ttm",
                "dv_ratio": "ts_dv_ratio",
                "dv_ttm": "ts_dv_ttm",
                "total_mv": "ts_total_mv",
                "circ_mv": "ts_circ_mv",
            },
        )

    def _normalize_moneyflow(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = self._normalize_trade_date_frame(
            frame,
            {
                "net_mf_amount": "ts_net_mf_amount",
                "buy_elg_amount": "ts_buy_elg_amount",
                "sell_elg_amount": "ts_sell_elg_amount",
                "buy_lg_amount": "ts_buy_lg_amount",
                "sell_lg_amount": "ts_sell_lg_amount",
                "buy_md_amount": "ts_buy_md_amount",
                "sell_md_amount": "ts_sell_md_amount",
                "buy_sm_amount": "ts_buy_sm_amount",
                "sell_sm_amount": "ts_sell_sm_amount",
            },
        )
        if out.empty:
            return out

        required = {"ts_buy_elg_amount", "ts_sell_elg_amount", "ts_buy_lg_amount", "ts_sell_lg_amount"}
        if required.issubset(set(out.columns)):
            out["ts_main_net_mf_amount"] = (
                out["ts_buy_elg_amount"]
                + out["ts_buy_lg_amount"]
                - out["ts_sell_elg_amount"]
                - out["ts_sell_lg_amount"]
            )
        return out

    def _normalize_stk_limit(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_trade_date_frame(
            frame,
            {
                "up_limit": "ts_up_limit",
                "down_limit": "ts_down_limit",
            },
        )

    def _normalize_adj_factor(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_trade_date_frame(frame, {"adj_factor": "ts_adj_factor"})

    def _normalize_income(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "total_revenue": "ts_income_total_revenue",
                "revenue": "ts_income_revenue",
                "operate_profit": "ts_income_operate_profit",
                "n_income_attr_p": "ts_income_net_profit_attr",
                "basic_eps": "ts_income_basic_eps",
            },
        )

    def _normalize_balancesheet(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "total_assets": "ts_bs_total_assets",
                "total_liab": "ts_bs_total_liab",
                "total_cur_assets": "ts_bs_total_cur_assets",
                "total_cur_liab": "ts_bs_total_cur_liab",
                "money_cap": "ts_bs_money_cap",
            },
        )

    def _normalize_cashflow(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "n_cashflow_act": "ts_cf_operate_cashflow",
                "n_cashflow_inv_act": "ts_cf_invest_cashflow",
                "n_cash_flows_fnc_act": "ts_cf_finance_cashflow",
                "c_pay_acq_const_fiolta": "ts_cf_capex",
            },
        )

    def _normalize_forecast(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "p_change_min": "ts_forecast_pchg_min",
                "p_change_max": "ts_forecast_pchg_max",
                "net_profit_min": "ts_forecast_np_min",
                "net_profit_max": "ts_forecast_np_max",
            },
            text_mapping={
                "type": "ts_forecast_type",
                "summary": "ts_forecast_summary",
            },
        )
        if out.empty:
            return out
        p_min = pd.to_numeric(out.get("ts_forecast_pchg_min"), errors="coerce")
        p_max = pd.to_numeric(out.get("ts_forecast_pchg_max"), errors="coerce")
        out["ts_forecast_pchg_mid"] = (p_min + p_max) / 2.0
        return out

    def _normalize_express(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "yoy_sales": "ts_express_yoy_sales",
                "yoy_op": "ts_express_yoy_op",
                "yoy_tp": "ts_express_yoy_tp",
                "yoy_dedu_np": "ts_express_yoy_dedu_np",
                "yoy_net_profit": "ts_express_yoy_net_profit",
                "diluted_eps": "ts_express_diluted_eps",
            },
            text_mapping={
                "perf_summary": "ts_express_perf_summary",
            },
        )

    def _normalize_fina_audit(self, frame: pd.DataFrame) -> pd.DataFrame:
        out = self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={},
            text_mapping={
                "audit_result": "ts_audit_result",
            },
        )
        if out.empty:
            return out
        out["ts_audit_opinion_risk"] = out["ts_audit_result"].apply(self._audit_result_to_risk)
        return out

    def _normalize_pledge_stat(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "pledge_ratio": "ts_pledge_ratio",
                "pledge_amount": "ts_pledge_amount",
            },
        )

    def _normalize_share_float(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "float_share": "ts_share_float",
                "float_ratio": "ts_share_float_ratio",
            },
            date_columns=("float_date", "ann_date", "end_date", "trade_date"),
        )

    def _normalize_holdernumber(self, frame: pd.DataFrame) -> pd.DataFrame:
        return self._normalize_asof_frame(
            frame=frame,
            numeric_mapping={
                "holder_num": "ts_holder_num",
            },
        )

    def _normalize_asof_frame(
        self,
        *,
        frame: pd.DataFrame,
        numeric_mapping: dict[str, str],
        text_mapping: dict[str, str] | None = None,
        date_columns: tuple[str, ...] = ("ann_date", "f_ann_date", "end_date", "trade_date"),
    ) -> pd.DataFrame:
        text_mapping = text_mapping or {}
        expected_cols = ["trade_date", *numeric_mapping.values(), *text_mapping.values()]
        if frame is None or frame.empty:
            return pd.DataFrame(columns=expected_cols)

        df = frame.copy()
        date_col = next((c for c in date_columns if c in df.columns), None)
        if date_col is None:
            return pd.DataFrame(columns=expected_cols)
        df["trade_date"] = pd.to_datetime(df[date_col], errors="coerce").dt.date
        df = df[df["trade_date"].notna()]
        if df.empty:
            return pd.DataFrame(columns=expected_cols)

        available_numeric = {src: dst for src, dst in numeric_mapping.items() if src in df.columns}
        available_text = {src: dst for src, dst in text_mapping.items() if src in df.columns}
        keep_cols = ["trade_date", *available_numeric.keys(), *available_text.keys()]
        out = df[keep_cols].rename(columns={**available_numeric, **available_text})
        for col in available_numeric.values():
            out[col] = pd.to_numeric(out[col], errors="coerce")
        for col in available_text.values():
            out[col] = out[col].astype(str)
        for col in expected_cols:
            if col not in out.columns:
                out[col] = np.nan if col != "trade_date" else None

        out = out.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")
        return out[expected_cols].reset_index(drop=True)

    def _normalize_trade_date_frame(self, frame: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
        cols = ["trade_date", *mapping.values()]
        if frame is None or frame.empty or "trade_date" not in frame.columns:
            return pd.DataFrame(columns=cols)

        df = frame.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
        df = df[df["trade_date"].notna()]
        if df.empty:
            return pd.DataFrame(columns=cols)

        keep = ["trade_date", *[src for src in mapping if src in df.columns]]
        out = df[keep].rename(columns=mapping)
        for col in out.columns:
            if col == "trade_date":
                continue
            out[col] = pd.to_numeric(out[col], errors="coerce")

        out = out.sort_values("trade_date").drop_duplicates(subset=["trade_date"], keep="last")
        return out.reset_index(drop=True)

    @staticmethod
    def _merge_by_trade_date(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        if right is None or right.empty:
            return left
        if "trade_date" not in right.columns:
            return left

        left_df = left.copy()
        cols = ["trade_date", *[c for c in right.columns if c != "trade_date" and c not in left_df.columns]]
        if len(cols) <= 1:
            return left_df
        return left_df.merge(right[cols], on="trade_date", how="left")

    @staticmethod
    def _merge_by_asof_date(left: pd.DataFrame, right: pd.DataFrame) -> pd.DataFrame:
        if right is None or right.empty:
            return left
        if "trade_date" not in right.columns:
            return left

        left_df = left.copy().sort_values("trade_date")
        extra_cols = [c for c in right.columns if c != "trade_date" and c not in left_df.columns]
        if not extra_cols:
            return left_df

        left_tmp = left_df.copy()
        right_tmp = right[["trade_date", *extra_cols]].copy().sort_values("trade_date")
        left_tmp["_asof_dt"] = pd.to_datetime(left_tmp["trade_date"], errors="coerce")
        right_tmp["_asof_dt"] = pd.to_datetime(right_tmp["trade_date"], errors="coerce")
        left_tmp = left_tmp[left_tmp["_asof_dt"].notna()]
        right_tmp = right_tmp[right_tmp["_asof_dt"].notna()]
        if left_tmp.empty or right_tmp.empty:
            return left_df

        merged = pd.merge_asof(
            left_tmp.sort_values("_asof_dt"),
            right_tmp.drop(columns=["trade_date"]).sort_values("_asof_dt"),
            on="_asof_dt",
            direction="backward",
            allow_exact_matches=True,
        )
        merged = merged.drop(columns=["_asof_dt"])
        return merged.sort_values("trade_date").reset_index(drop=True)

    @staticmethod
    def _audit_result_to_risk(value: Any) -> float:
        text = str(value or "").strip().upper()
        if not text:
            return 0.5
        high_risk_words = ("否定", "无法表示", "无法表述", "DISCLAIMER", "ADVERSE")
        medium_risk_words = ("保留", "强调事项", "强调", "QUALIFIED", "EMPHASIS")
        if any(word in text for word in high_risk_words):
            return 1.0
        if any(word in text for word in medium_risk_words):
            return 0.7
        if ("无保留" in text) or ("UNQUALIFIED" in text) or ("标准" in text):
            return 0.0
        return 0.4

    @staticmethod
    def _parse_date(value: Any) -> date | None:
        if value is None:
            return None
        text = str(value).strip()
        if not text:
            return None
        if len(text) == 8 and text.isdigit():
            text = f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        parsed = pd.to_datetime(text, errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()

    @staticmethod
    def _parse_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            out = float(value)
        except Exception:  # noqa: BLE001
            return None
        if pd.isna(out):
            return None
        return out
