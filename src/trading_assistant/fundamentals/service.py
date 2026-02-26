from __future__ import annotations

from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from trading_assistant.data.composite_provider import CompositeDataProvider


class FundamentalService:
    def __init__(self, provider: CompositeDataProvider) -> None:
        self.provider = provider

    def enrich_bars_point_in_time(
        self,
        *,
        symbol: str,
        bars: pd.DataFrame,
        max_staleness_days: int = 540,
        anchor_frequency: str = "month",
    ) -> tuple[pd.DataFrame, dict[str, object]]:
        """
        Point-in-time (PIT) fundamental enrichment.

        The legacy `enrich_bars(..., as_of=...)` injects a single snapshot into all rows,
        which is fine for "current view" but makes fundamentals constant in research windows.
        This PIT method samples snapshots along the bar timeline and backward-fills (merge-asof)
        so each trading day uses only information available up to that date (anti look-ahead).
        """
        out = bars.copy()
        if out.empty:
            return self._inject_defaults(out), {"available": False, "reason": "empty_bars"}
        if "trade_date" not in out.columns:
            raise ValueError("bars must contain 'trade_date' column for PIT enrichment.")

        trade_dt = pd.to_datetime(out["trade_date"], errors="coerce")
        if trade_dt.isna().any():
            raise ValueError("bars.trade_date contains invalid dates.")

        working = out.copy()
        working["_trade_dt"] = trade_dt
        working = working.sort_values("_trade_dt").reset_index(drop=True)
        start_dt = working["_trade_dt"].iloc[0].date()
        end_dt = working["_trade_dt"].iloc[-1].date()

        anchors = self._build_anchor_dates(trade_dt=working["_trade_dt"], frequency=anchor_frequency)
        # Ensure early dates are covered without look-ahead.
        anchors = sorted({start_dt, *anchors})

        snapshots: list[dict[str, object]] = []
        errors: list[str] = []
        sources: set[str] = set()
        for anchor in anchors:
            try:
                source, snapshot = self.provider.get_fundamental_snapshot_with_source(symbol=symbol, as_of=anchor)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"{anchor.isoformat()}: {exc}")
                continue

            if not snapshot:
                errors.append(f"{anchor.isoformat()}: empty_snapshot ({source})")
                continue

            report_date = self._to_date(snapshot.get("report_date"))
            publish_date = self._to_date(snapshot.get("publish_date"))
            metric_map: dict[str, float | None] = {
                "roe": self._to_float(snapshot.get("roe")),
                "revenue_yoy": self._to_float(snapshot.get("revenue_yoy")),
                "net_profit_yoy": self._to_float(snapshot.get("net_profit_yoy")),
                "gross_margin": self._to_float(snapshot.get("gross_margin")),
                "debt_to_asset": self._to_float(snapshot.get("debt_to_asset")),
                "ocf_to_profit": self._to_float(snapshot.get("ocf_to_profit")),
                "eps": self._to_float(snapshot.get("eps")),
            }
            if not any(v is not None for v in metric_map.values()):
                errors.append(f"{anchor.isoformat()}: all_metrics_missing ({source})")
                continue

            row: dict[str, object] = {
                "as_of": anchor,
                "fundamental_source": str(source),
                "fundamental_report_date": report_date,
                "fundamental_publish_date": publish_date,
            }
            for key, value in metric_map.items():
                row[key] = np.nan if value is None else float(value)
            snapshots.append(row)
            sources.add(str(source))

        if not snapshots:
            out = self._inject_defaults(out)
            return out, {
                "available": False,
                "reason": "no_valid_snapshots",
                "anchor_frequency": str(anchor_frequency),
                "start_date": start_dt.isoformat(),
                "end_date": end_dt.isoformat(),
                "errors": errors[:6],
            }

        snap = pd.DataFrame(snapshots).sort_values("as_of").reset_index(drop=True)
        snap["_asof_dt"] = pd.to_datetime(snap["as_of"])

        merged = pd.merge_asof(
            working.sort_values("_trade_dt"),
            snap.sort_values("_asof_dt"),
            left_on="_trade_dt",
            right_on="_asof_dt",
            direction="backward",
        )

        metric_cols = ("roe", "revenue_yoy", "net_profit_yoy", "gross_margin", "debt_to_asset", "ocf_to_profit", "eps")
        merged[list(metric_cols)] = merged[list(metric_cols)].apply(pd.to_numeric, errors="coerce")
        merged["fundamental_available"] = merged[list(metric_cols)].notna().any(axis=1)

        pub = pd.to_datetime(merged["fundamental_publish_date"], errors="coerce")
        rep = pd.to_datetime(merged["fundamental_report_date"], errors="coerce")
        stale_anchor = rep.fillna(pub)
        stale_days = (merged["_trade_dt"] - stale_anchor).dt.days
        merged["fundamental_stale_days"] = stale_days.fillna(-1).astype(int)
        merged["fundamental_is_stale"] = merged["fundamental_stale_days"].gt(int(max_staleness_days))
        merged["fundamental_pit_ok"] = pub.isna() | pub.le(merged["_trade_dt"])

        merged = merged.drop(columns=["as_of", "_asof_dt", "_trade_dt"], errors="ignore")
        merged["trade_date"] = pd.to_datetime(merged["trade_date"], errors="coerce").dt.date

        return merged, {
            "available": True,
            "mode": "pit",
            "anchor_frequency": str(anchor_frequency),
            "start_date": start_dt.isoformat(),
            "end_date": end_dt.isoformat(),
            "anchors": int(len(anchors)),
            "successful_snapshots": int(len(snapshots)),
            "sources": sorted(sources),
            "errors": errors[:6],
        }

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

    @staticmethod
    def _build_anchor_dates(*, trade_dt: pd.Series, frequency: str) -> list[date]:
        freq = str(frequency or "").strip().lower()
        if freq in ("d", "day", "daily"):
            anchors = sorted({d.date() for d in trade_dt})
            return anchors

        df = pd.DataFrame({"_dt": trade_dt})
        if freq in ("w", "week", "weekly"):
            iso = df["_dt"].dt.isocalendar()
            keys = iso["year"].astype(str) + "-" + iso["week"].astype(str)
            grouped = df.groupby(keys, sort=True)["_dt"].max()
            return [d.date() for d in grouped.sort_values().to_list()]
        if freq in ("q", "quarter", "quarterly"):
            grouped = df.groupby(df["_dt"].dt.to_period("Q"), sort=True)["_dt"].max()
            return [d.date() for d in grouped.sort_values().to_list()]
        # Default to monthly anchors.
        grouped = df.groupby(df["_dt"].dt.to_period("M"), sort=True)["_dt"].max()
        return [d.date() for d in grouped.sort_values().to_list()]
