from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from trading_assistant.data.composite_provider import CompositeDataProvider


class FundamentalService:
    def __init__(self, provider: CompositeDataProvider) -> None:
        self.provider = provider

    def enrich_bars(
        self,
        *,
        symbol: str,
        bars: pd.DataFrame,
        as_of: date,
        max_staleness_days: int = 540,
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        out = bars.copy()
        if out.empty:
            return self._inject_defaults(out), {"available": False, "reason": "empty_bars"}
        try:
            source, snapshot = self.provider.get_fundamental_snapshot_with_source(symbol=symbol, as_of=as_of)
        except Exception as exc:  # noqa: BLE001
            out = self._inject_defaults(out)
            return out, {"available": False, "reason": str(exc)}

        if not snapshot:
            out = self._inject_defaults(out)
            return out, {"available": False, "reason": "empty_snapshot", "source": source}

        report_date = self._to_date(snapshot.get("report_date"))
        publish_date = self._to_date(snapshot.get("publish_date"))
        pit_ok = True if publish_date is None else publish_date <= as_of
        stale_anchor = report_date or publish_date
        stale_days = (as_of - stale_anchor).days if stale_anchor is not None else None
        is_stale = bool(stale_days is not None and stale_days > max_staleness_days)

        metric_map: dict[str, float | None] = {
            "roe": self._to_float(snapshot.get("roe")),
            "revenue_yoy": self._to_float(snapshot.get("revenue_yoy")),
            "net_profit_yoy": self._to_float(snapshot.get("net_profit_yoy")),
            "gross_margin": self._to_float(snapshot.get("gross_margin")),
            "debt_to_asset": self._to_float(snapshot.get("debt_to_asset")),
            "ocf_to_profit": self._to_float(snapshot.get("ocf_to_profit")),
            "eps": self._to_float(snapshot.get("eps")),
        }
        available = any(v is not None for v in metric_map.values())
        if not available:
            out = self._inject_defaults(out)
            return out, {
                "available": False,
                "source": source,
                "reason": "all_metrics_missing",
                "report_date": report_date.isoformat() if report_date else None,
                "publish_date": publish_date.isoformat() if publish_date else None,
            }

        for key, value in metric_map.items():
            out[key] = np.nan if value is None else float(value)
        out["fundamental_available"] = True
        out["fundamental_pit_ok"] = bool(pit_ok)
        out["fundamental_stale_days"] = int(stale_days) if stale_days is not None else -1
        out["fundamental_is_stale"] = bool(is_stale)
        out["fundamental_source"] = str(source)
        out["fundamental_report_date"] = report_date
        out["fundamental_publish_date"] = publish_date
        return out, {
            "available": True,
            "source": source,
            "pit_ok": bool(pit_ok),
            "stale_days": int(stale_days) if stale_days is not None else None,
            "is_stale": bool(is_stale),
            "report_date": report_date.isoformat() if report_date else None,
            "publish_date": publish_date.isoformat() if publish_date else None,
        }

    @staticmethod
    def _inject_defaults(bars: pd.DataFrame) -> pd.DataFrame:
        out = bars.copy()
        for col in ("roe", "revenue_yoy", "net_profit_yoy", "gross_margin", "debt_to_asset", "ocf_to_profit", "eps"):
            out[col] = np.nan
        out["fundamental_available"] = False
        out["fundamental_pit_ok"] = True
        out["fundamental_stale_days"] = -1
        out["fundamental_is_stale"] = False
        out["fundamental_source"] = "N/A"
        out["fundamental_report_date"] = None
        out["fundamental_publish_date"] = None
        return out

    @staticmethod
    def _to_float(value: Any) -> float | None:
        if value is None:
            return None
        try:
            out = float(value)
        except Exception:  # noqa: BLE001
            return None
        if pd.isna(out):
            return None
        return out

    @staticmethod
    def _to_date(value: Any) -> date | None:
        if value is None:
            return None
        if isinstance(value, date):
            return value
        parsed = pd.to_datetime(str(value), errors="coerce")
        if pd.isna(parsed):
            return None
        return parsed.date()

