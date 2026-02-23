from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from trading_assistant.core.models import RiskCheckRequest, SignalAction
from trading_assistant.data.akshare_provider import AkshareProvider
from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.tushare_provider import TushareProvider
from trading_assistant.factors.engine import FactorEngine
from trading_assistant.risk.engine import RiskEngine
from trading_assistant.strategy.base import StrategyContext
from trading_assistant.strategy.small_capital_adaptive import SmallCapitalAdaptiveStrategy
from trading_assistant.trading.costs import (
    estimate_roundtrip_cost_bps,
    infer_expected_edge_bps,
    required_cash_for_min_lot,
)
from trading_assistant.trading.small_capital import apply_small_capital_overrides


STRATEGY_2000_PARAMS: dict[str, float | int | bool | str] = {
    "buy_threshold": 0.68,
    "sell_threshold": 0.32,
    "min_turnover20": 15_000_000,
    "max_volatility20": 0.035,
    "min_momentum20_buy": -0.01,
    "max_momentum20_buy": 0.12,
    "min_fundamental_score_buy": 0.50,
    "max_positions": 2,
    "cash_buffer_ratio": 0.08,
    "risk_per_trade": 0.008,
    "max_single_position": 0.60,
    "min_tushare_advanced_score_buy": 0.40,
}


@dataclass
class ScanRow:
    symbol: str
    provider: str
    action: str
    confidence: float
    blocked: bool
    risk_level: str
    close: float | None
    suggested_position: float | None
    suggested_lots: int
    max_buy_price: float | None
    buy_price_low: float | None
    buy_price_high: float | None
    reason: str
    small_capital_note: str | None
    error: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-click full market scan for CNY 2000 account")
    parser.add_argument("--run-id", default="", help="run id for progress tracking")
    parser.add_argument(
        "--start-date",
        default=(date.today() - timedelta(days=240)).isoformat(),
        help="start date in YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        default=(date.today() - timedelta(days=1)).isoformat(),
        help="end date in YYYY-MM-DD",
    )
    parser.add_argument("--principal", type=float, default=2000.0, help="small account principal in CNY")
    parser.add_argument("--lot-size", type=int, default=100, help="lot size")
    parser.add_argument("--cash-buffer-ratio", type=float, default=0.10, help="cash buffer ratio")
    parser.add_argument("--max-single-position", type=float, default=0.60, help="max single position ratio")
    parser.add_argument("--min-edge-bps", type=float, default=140.0, help="minimum expected edge bps")
    parser.add_argument("--max-symbols", type=int, default=0, help="0 means all symbols")
    parser.add_argument("--sleep-ms", type=int, default=0, help="sleep milliseconds between symbols")
    parser.add_argument("--top-n", type=int, default=30, help="top candidate count for csv")
    parser.add_argument("--output-jsonl", default="reports/full_market_signals_2000.jsonl", help="output jsonl path")
    parser.add_argument("--output-summary", default="reports/full_market_summary_2000.json", help="output summary json path")
    parser.add_argument("--output-csv", default="reports/buy_candidates_2000.csv", help="output csv path")
    parser.add_argument("--progress-file", default="", help="optional progress json path")
    parser.add_argument(
        "--keep-proxy",
        action="store_true",
        help="keep proxy environment variables (default clears broken local proxy)",
    )
    return parser.parse_args()


def _clear_proxy_env() -> None:
    for key in ("ALL_PROXY", "HTTP_PROXY", "HTTPS_PROXY", "GIT_HTTP_PROXY", "GIT_HTTPS_PROXY"):
        os.environ[key] = ""
        os.environ[key.lower()] = ""


def _read_tushare_token() -> str:
    token = str(os.getenv("TUSHARE_TOKEN", "")).strip()
    if token:
        return token
    env_path = ROOT_DIR / ".env"
    if not env_path.exists():
        return ""
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if line.startswith("TUSHARE_TOKEN="):
            return line.split("=", 1)[1].strip()
    return ""


def _parse_date(raw: str) -> date:
    return date.fromisoformat(raw)


def _to_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    parsed = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out):
        return None
    return out


def _infer_limit_flags(close_today: float, close_yesterday: float, is_st: bool) -> tuple[bool, bool]:
    if close_yesterday <= 0:
        return False, False
    pct = close_today / close_yesterday - 1
    limit = 0.05 if is_st else 0.10
    return pct >= (limit - 0.0005), pct <= (-limit + 0.0005)


def _resolve_small_capital_note(risk_result) -> str | None:
    hits = list(getattr(risk_result, "hits", []) or [])
    matched = [h for h in hits if getattr(h, "rule_name", "") == "small_capital_tradability"]
    failed = [h for h in matched if not bool(getattr(h, "passed", True))]
    target = failed[0] if failed else (matched[0] if matched else None)
    return str(getattr(target, "message", "")) if target is not None else None


def _max_affordable_price(
    *,
    usable_cash: float,
    lot_size: int,
    commission_rate: float,
    min_commission: float,
    transfer_fee_rate: float,
) -> float:
    if usable_cash <= 0 or lot_size <= 0:
        return 0.0
    lo = 0.0
    hi = max(5.0, usable_cash / lot_size * 2.0)
    for _ in range(40):
        mid = (lo + hi) / 2.0
        need = required_cash_for_min_lot(
            price=mid,
            lot_size=lot_size,
            commission_rate=commission_rate,
            min_commission=min_commission,
            transfer_fee_rate=transfer_fee_rate,
        )
        if need <= usable_cash:
            lo = mid
        else:
            hi = mid
    return round(lo, 3)


def _load_universe(token: str, max_symbols: int) -> list[str]:
    import tushare as ts

    pro = ts.pro_api(token)
    df = pro.stock_basic(exchange="", list_status="L", fields="symbol,name,market")
    df = df.dropna(subset=["symbol"]).copy()
    df["symbol"] = df["symbol"].astype(str)
    df["name"] = df["name"].astype(str)
    df["market"] = df["market"].astype(str)
    # Keep regular A-share boards and remove ST.
    df = df[df["market"].isin(["主板", "创业板", "科创板"])]
    df = df[~df["name"].str.upper().str.contains("ST", na=False)]
    df = df.sort_values("symbol")
    symbols = df["symbol"].drop_duplicates().tolist()
    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    return symbols


def _get_daily_bars(
    *,
    providers: list[MarketDataProvider],
    symbol: str,
    start_date: date,
    end_date: date,
) -> tuple[str, pd.DataFrame, dict[str, bool]]:
    errors: list[str] = []
    for provider in providers:
        try:
            bars = provider.get_daily_bars(symbol, start_date, end_date)
            if bars is None or bars.empty:
                raise RuntimeError("empty bars")
            status = provider.get_security_status(symbol)
            out = bars.sort_values("trade_date").copy()
            out["is_st"] = bool(status.get("is_st", False))
            out["is_suspended"] = bool(status.get("is_suspended", False))
            return provider.name, out, status
        except Exception as exc:
            errors.append(f"{provider.name}: {exc}")
    raise RuntimeError("; ".join(errors))


def _enrich_fundamental(
    *,
    bars: pd.DataFrame,
    providers: list[MarketDataProvider],
    symbol: str,
    as_of: date,
    max_staleness_days: int = 540,
) -> pd.DataFrame:
    out = bars.copy()
    metric_cols = ("roe", "revenue_yoy", "net_profit_yoy", "gross_margin", "debt_to_asset", "ocf_to_profit", "eps")
    for col in metric_cols:
        out[col] = pd.NA
    out["fundamental_available"] = False
    out["fundamental_pit_ok"] = True
    out["fundamental_stale_days"] = -1
    out["fundamental_is_stale"] = False

    for provider in providers:
        try:
            snapshot = provider.get_fundamental_snapshot(symbol=symbol, as_of=as_of)
        except NotImplementedError:
            continue
        except Exception:
            continue
        if not snapshot:
            continue

        metric_map = {
            "roe": _to_float(snapshot.get("roe")),
            "revenue_yoy": _to_float(snapshot.get("revenue_yoy")),
            "net_profit_yoy": _to_float(snapshot.get("net_profit_yoy")),
            "gross_margin": _to_float(snapshot.get("gross_margin")),
            "debt_to_asset": _to_float(snapshot.get("debt_to_asset")),
            "ocf_to_profit": _to_float(snapshot.get("ocf_to_profit")),
            "eps": _to_float(snapshot.get("eps")),
        }
        available = any(v is not None for v in metric_map.values())
        if not available:
            continue

        report_date = _to_date(snapshot.get("report_date"))
        publish_date = _to_date(snapshot.get("publish_date"))
        pit_ok = True if publish_date is None else publish_date <= as_of
        stale_anchor = report_date or publish_date
        stale_days = (as_of - stale_anchor).days if stale_anchor is not None else -1
        is_stale = stale_days >= 0 and stale_days > max_staleness_days

        for key, value in metric_map.items():
            out[key] = pd.NA if value is None else value
        out["fundamental_available"] = True
        out["fundamental_pit_ok"] = bool(pit_ok)
        out["fundamental_stale_days"] = int(stale_days)
        out["fundamental_is_stale"] = bool(is_stale)
        return out
    return out


def _enrich_event_score(
    *,
    bars: pd.DataFrame,
    providers: list[MarketDataProvider],
    symbol: str,
    as_of: date,
) -> pd.DataFrame:
    out = bars.copy()
    if "event_score" not in out.columns:
        out["event_score"] = 0.0
    if "negative_event_score" not in out.columns:
        out["negative_event_score"] = 0.0
    for provider in providers:
        try:
            snapshot = provider.get_corporate_event_snapshot(symbol=symbol, as_of=as_of, lookback_days=120)
        except NotImplementedError:
            continue
        except Exception:
            continue
        if not snapshot:
            continue
        event_score = _to_float(snapshot.get("event_score"))
        negative_event_score = _to_float(snapshot.get("negative_event_score"))
        if event_score is not None:
            out["event_score"] = float(event_score)
        if negative_event_score is not None:
            out["negative_event_score"] = float(negative_event_score)
        return out
    return out


def _scan_symbol(
    *,
    symbol: str,
    start_date: date,
    end_date: date,
    providers: list[MarketDataProvider],
    factor_engine: FactorEngine,
    strategy: SmallCapitalAdaptiveStrategy,
    risk_engine: RiskEngine,
    principal: float,
    lot_size: int,
    cash_buffer_ratio: float,
    max_single_position: float,
    min_edge_bps: float,
) -> ScanRow:
    commission_rate = 0.0003
    min_commission_cny = 5.0
    transfer_fee_rate = 0.00001
    stamp_duty_sell_rate = 0.0005
    slippage_rate = 0.0005

    try:
        provider_name, bars, status = _get_daily_bars(
            providers=providers,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
    except Exception as exc:
        return ScanRow(
            symbol=symbol,
            provider="-",
            action="ERROR",
            confidence=0.0,
            blocked=True,
            risk_level="CRITICAL",
            close=None,
            suggested_position=None,
            suggested_lots=0,
            max_buy_price=None,
            buy_price_low=None,
            buy_price_high=None,
            reason="failed to fetch bars",
            small_capital_note=None,
            error=str(exc),
        )

    if len(bars) < 90:
        return ScanRow(
            symbol=symbol,
            provider=provider_name,
            action="SKIP",
            confidence=0.0,
            blocked=True,
            risk_level="WARNING",
            close=None,
            suggested_position=None,
            suggested_lots=0,
            max_buy_price=None,
            buy_price_low=None,
            buy_price_high=None,
            reason=f"insufficient bars ({len(bars)})",
            small_capital_note=None,
            error=None,
        )

    enriched = _enrich_fundamental(bars=bars, providers=providers, symbol=symbol, as_of=end_date)
    enriched = _enrich_event_score(bars=enriched, providers=providers, symbol=symbol, as_of=end_date)
    features = factor_engine.compute(enriched)
    latest = features.sort_values("trade_date").iloc[-1]
    previous = features.sort_values("trade_date").iloc[-2] if len(features) >= 2 else latest

    close = float(latest.get("close", 0.0))
    at_limit_up, at_limit_down = _infer_limit_flags(
        close_today=close,
        close_yesterday=float(previous.get("close", close)),
        is_st=bool(status.get("is_st", False)),
    )
    context = StrategyContext(
        params={
            **STRATEGY_2000_PARAMS,
            "max_single_position": max_single_position,
            "cash_buffer_ratio": cash_buffer_ratio,
        },
        market_state={
            "enable_small_capital_mode": True,
            "small_capital_principal": principal,
            "small_capital_lot_size": lot_size,
            "small_capital_cash_buffer_ratio": cash_buffer_ratio,
            "commission_rate": commission_rate,
            "min_commission_cny": min_commission_cny,
            "transfer_fee_rate": transfer_fee_rate,
            "stamp_duty_sell_rate": stamp_duty_sell_rate,
            "slippage_rate": slippage_rate,
        },
    )
    candidates = strategy.generate(features, context=context)
    if not candidates:
        return ScanRow(
            symbol=symbol,
            provider=provider_name,
            action="NONE",
            confidence=0.0,
            blocked=True,
            risk_level="INFO",
            close=round(close, 3) if close > 0 else None,
            suggested_position=None,
            suggested_lots=0,
            max_buy_price=None,
            buy_price_low=None,
            buy_price_high=None,
            reason="no signal candidate",
            small_capital_note=None,
            error=None,
        )

    signal = candidates[-1]
    _ = apply_small_capital_overrides(
        signal=signal,
        enable_small_capital_mode=True,
        principal=principal,
        latest_price=close,
        lot_size=lot_size,
        commission_rate=commission_rate,
        min_commission=min_commission_cny,
        transfer_fee_rate=transfer_fee_rate,
        cash_buffer_ratio=cash_buffer_ratio,
        max_single_position=max_single_position,
        max_positions=max(1, int(float(context.params.get("max_positions", 2)))),
    )

    required_cash = required_cash_for_min_lot(
        price=close,
        lot_size=lot_size,
        commission_rate=commission_rate,
        min_commission=min_commission_cny,
        transfer_fee_rate=transfer_fee_rate,
    )
    roundtrip_cost_bps = estimate_roundtrip_cost_bps(
        price=close,
        lot_size=lot_size,
        commission_rate=commission_rate,
        min_commission=min_commission_cny,
        transfer_fee_rate=transfer_fee_rate,
        stamp_duty_sell_rate=stamp_duty_sell_rate,
        slippage_rate=slippage_rate,
    )
    expected_edge_bps = infer_expected_edge_bps(
        confidence=float(signal.confidence),
        momentum20=float(latest.get("momentum20", 0.0)),
        event_score=float(latest.get("event_score", 0.0)) if "event_score" in latest else None,
        fundamental_score=(
            float(latest.get("fundamental_score", 0.5))
            if bool(latest.get("fundamental_available", False))
            else None
        ),
    )

    risk = risk_engine.evaluate(
        RiskCheckRequest(
            signal=signal,
            is_st=bool(status.get("is_st", False)),
            is_suspended=bool(status.get("is_suspended", False)),
            at_limit_up=at_limit_up,
            at_limit_down=at_limit_down,
            avg_turnover_20d=float(latest.get("turnover20", 0.0)),
            fundamental_score=(
                float(latest.get("fundamental_score", 0.5))
                if bool(latest.get("fundamental_available", False))
                else None
            ),
            fundamental_available=bool(latest.get("fundamental_available", False)),
            fundamental_pit_ok=bool(latest.get("fundamental_pit_ok", True)),
            fundamental_stale_days=(
                int(latest.get("fundamental_stale_days", -1))
                if int(latest.get("fundamental_stale_days", -1)) >= 0
                else None
            ),
            tushare_disclosure_risk_score=(
                _to_float(latest.get("tushare_disclosure_risk_score"))
            ),
            tushare_audit_opinion_risk=(
                _to_float(latest.get("tushare_audit_opinion_risk"))
            ),
            tushare_forecast_pchg_mid=(
                _to_float(latest.get("tushare_forecast_pchg_mid"))
            ),
            tushare_pledge_ratio=(
                _to_float(latest.get("tushare_pledge_ratio"))
            ),
            tushare_share_float_unlock_ratio=(
                _to_float(latest.get("tushare_share_float_unlock_ratio"))
            ),
            tushare_holder_crowding_ratio=(
                _to_float(latest.get("tushare_holder_crowding_ratio"))
            ),
            tushare_overhang_risk_score=(
                _to_float(latest.get("tushare_overhang_risk_score"))
            ),
            enable_small_capital_mode=True,
            small_capital_principal=principal,
            available_cash=principal,
            latest_price=close if close > 0 else None,
            lot_size=lot_size,
            required_cash_for_min_lot=required_cash,
            estimated_roundtrip_cost_bps=roundtrip_cost_bps,
            expected_edge_bps=expected_edge_bps,
            min_expected_edge_bps=min_edge_bps,
            small_capital_cash_buffer_ratio=cash_buffer_ratio,
        )
    )

    usable_cash = principal * (1.0 - cash_buffer_ratio)
    max_by_position = principal * max_single_position / lot_size
    max_by_cash = _max_affordable_price(
        usable_cash=usable_cash,
        lot_size=lot_size,
        commission_rate=commission_rate,
        min_commission=min_commission_cny,
        transfer_fee_rate=transfer_fee_rate,
    )
    max_buy_price = round(min(max_by_position, max_by_cash), 3)

    suggested_position = float(signal.suggested_position) if signal.suggested_position is not None else None
    suggested_lots = 0
    if signal.action == SignalAction.BUY and suggested_position is not None and close > 0:
        target_cash = principal * max(0.0, suggested_position)
        suggested_lots = max(1, int(target_cash / (close * lot_size)))
    elif signal.action == SignalAction.BUY and close > 0:
        suggested_lots = 1

    buy_price_low = None
    buy_price_high = None
    if signal.action == SignalAction.BUY and close > 0:
        high = min(max_buy_price, close * 1.01)
        low = min(close, high) * 0.995
        if high > 0 and high >= low:
            buy_price_low = round(low, 3)
            buy_price_high = round(high, 3)

    return ScanRow(
        symbol=symbol,
        provider=provider_name,
        action=str(getattr(signal.action, "value", signal.action)),
        confidence=round(float(signal.confidence), 6),
        blocked=bool(risk.blocked),
        risk_level=str(getattr(risk.level, "value", risk.level)),
        close=round(close, 3) if close > 0 else None,
        suggested_position=round(suggested_position, 6) if suggested_position is not None else None,
        suggested_lots=suggested_lots,
        max_buy_price=max_buy_price,
        buy_price_low=buy_price_low,
        buy_price_high=buy_price_high,
        reason=str(signal.reason),
        small_capital_note=_resolve_small_capital_note(risk),
        error=None,
    )


def _is_buy_candidate(row: dict[str, Any]) -> bool:
    return (
        str(row.get("action", "")) == "BUY"
        and (not bool(row.get("blocked", False)))
        and str(row.get("risk_level", "")) != "CRITICAL"
        and row.get("close") is not None
        and row.get("max_buy_price") is not None
        and float(row.get("close")) <= float(row.get("max_buy_price"))
        and row.get("buy_price_high") is not None
    )


def _collect_top_candidates_from_jsonl(*, jsonl_path: Path, top_n: int) -> list[dict[str, Any]]:
    import heapq

    top_n = max(1, int(top_n))
    heap: list[tuple[float, int, dict[str, Any]]] = []
    row_idx = 0
    with jsonl_path.open("r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            row = json.loads(line)
            if not _is_buy_candidate(row):
                row_idx += 1
                continue
            confidence = float(row.get("confidence", 0.0))
            item = (confidence, row_idx, row)
            if len(heap) < top_n:
                heapq.heappush(heap, item)
            else:
                if confidence > heap[0][0]:
                    heapq.heapreplace(heap, item)
            row_idx += 1
    top = sorted(heap, key=lambda x: x[0], reverse=True)
    return [x[2] for x in top]


def _write_top_csv(*, top_rows: list[dict[str, Any]], output_csv: Path) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "symbol",
        "provider",
        "confidence",
        "close",
        "suggested_lots",
        "max_buy_price",
        "buy_price_low",
        "buy_price_high",
        "risk_level",
        "small_capital_note",
        "reason",
    ]
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in top_rows:
            writer.writerow(
                {
                    "symbol": row.get("symbol", ""),
                    "provider": row.get("provider", ""),
                    "confidence": row.get("confidence", 0.0),
                    "close": row.get("close", ""),
                    "suggested_lots": row.get("suggested_lots", 0),
                    "max_buy_price": row.get("max_buy_price", ""),
                    "buy_price_low": row.get("buy_price_low", ""),
                    "buy_price_high": row.get("buy_price_high", ""),
                    "risk_level": row.get("risk_level", ""),
                    "small_capital_note": row.get("small_capital_note", "") or "",
                    "reason": row.get("reason", ""),
                }
            )


def _write_summary_json(
    *,
    output_summary: Path,
    total: int,
    buy_pass: int,
    errors: int,
    top_rows: list[dict[str, Any]],
    output_jsonl: Path,
    output_csv: Path,
) -> None:
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "total_symbols": int(total),
        "buy_pass_symbols": int(buy_pass),
        "error_symbols": int(errors),
        "jsonl_path": str(output_jsonl),
        "csv_path": str(output_csv),
        "top_candidates": top_rows,
    }
    output_summary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_progress(progress_file: Path | None, payload: dict[str, Any]) -> None:
    if progress_file is None:
        return
    try:
        progress_file.parent.mkdir(parents=True, exist_ok=True)
        progress_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    except Exception:
        # progress write is best-effort and must not block scan
        return


def main() -> None:
    args = parse_args()
    start_date = _parse_date(args.start_date)
    end_date = _parse_date(args.end_date)
    if start_date > end_date:
        raise ValueError("start_date must be <= end_date")
    if args.principal <= 0:
        raise ValueError("principal must be > 0")
    if args.lot_size <= 0:
        raise ValueError("lot_size must be > 0")

    if not args.keep_proxy:
        _clear_proxy_env()

    token = _read_tushare_token()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN missing (set env or .env)")

    providers: list[MarketDataProvider] = []
    try:
        providers.append(TushareProvider(token=token))
    except Exception as exc:
        print(f"[warn] tushare init failed: {exc}")
    try:
        providers.append(AkshareProvider())
    except Exception as exc:
        print(f"[warn] akshare init failed: {exc}")
    if not providers:
        raise RuntimeError("no data provider available")

    print("[info] loading stock universe ...")
    symbols = _load_universe(token=token, max_symbols=max(0, int(args.max_symbols)))
    print(f"[info] universe size: {len(symbols)}")

    factor_engine = FactorEngine()
    strategy = SmallCapitalAdaptiveStrategy()
    risk_engine = RiskEngine(
        max_single_position=float(args.max_single_position),
        max_drawdown=0.12,
        max_industry_exposure=0.20,
        min_turnover_20d=5_000_000,
    )

    sleep_sec = max(0, int(args.sleep_ms)) / 1000.0
    output_jsonl = ROOT_DIR / args.output_jsonl
    output_summary = ROOT_DIR / args.output_summary
    output_csv = ROOT_DIR / args.output_csv
    progress_file = (ROOT_DIR / args.progress_file) if str(args.progress_file or "").strip() else None
    run_id = str(args.run_id or "").strip()
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    output_summary.parent.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    total_scanned = 0
    total_buy_pass = 0
    total_errors = 0
    total = len(symbols)
    started_at_iso = pd.Timestamp.utcnow().isoformat()
    _write_progress(
        progress_file,
        {
            "run_id": run_id or None,
            "status": "RUNNING",
            "total_symbols": int(total),
            "scanned_symbols": 0,
            "buy_pass_symbols": 0,
            "error_symbols": 0,
            "progress_pct": 0.0,
            "started_at": started_at_iso,
            "updated_at": started_at_iso,
            "finished_at": None,
            "message": "scan started",
        },
    )
    with output_jsonl.open("w", encoding="utf-8") as sink:
        for idx, symbol in enumerate(symbols, start=1):
            row = _scan_symbol(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                providers=providers,
                factor_engine=factor_engine,
                strategy=strategy,
                risk_engine=risk_engine,
                principal=float(args.principal),
                lot_size=int(args.lot_size),
                cash_buffer_ratio=float(args.cash_buffer_ratio),
                max_single_position=float(args.max_single_position),
                min_edge_bps=float(args.min_edge_bps),
            )
            row_dict = asdict(row)
            sink.write(json.dumps(row_dict, ensure_ascii=False) + "\n")
            total_scanned += 1
            if row.error is not None:
                total_errors += 1
            if _is_buy_candidate(row_dict):
                total_buy_pass += 1
            if idx % 20 == 0 or idx == total:
                now_iso = pd.Timestamp.utcnow().isoformat()
                _write_progress(
                    progress_file,
                    {
                        "run_id": run_id or None,
                        "status": "RUNNING",
                        "total_symbols": int(total),
                        "scanned_symbols": int(total_scanned),
                        "buy_pass_symbols": int(total_buy_pass),
                        "error_symbols": int(total_errors),
                        "progress_pct": round(float(total_scanned) / max(float(total), 1.0) * 100.0, 2),
                        "current_symbol": symbol,
                        "started_at": started_at_iso,
                        "updated_at": now_iso,
                        "finished_at": None,
                        "message": f"scanning {symbol}",
                    },
                )
            if idx % 50 == 0 or idx == total:
                print(f"[progress] {idx}/{total} scanned, buy_pass={total_buy_pass}, errors={total_errors}")
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    top_rows = _collect_top_candidates_from_jsonl(jsonl_path=output_jsonl, top_n=int(args.top_n))
    _write_top_csv(top_rows=top_rows, output_csv=output_csv)
    _write_summary_json(
        output_summary=output_summary,
        total=total_scanned,
        buy_pass=total_buy_pass,
        errors=total_errors,
        top_rows=top_rows,
        output_jsonl=output_jsonl,
        output_csv=output_csv,
    )
    finished_at_iso = pd.Timestamp.utcnow().isoformat()
    _write_progress(
        progress_file,
        {
            "run_id": run_id or None,
            "status": "COMPLETED",
            "total_symbols": int(total_scanned),
            "scanned_symbols": int(total_scanned),
            "buy_pass_symbols": int(total_buy_pass),
            "error_symbols": int(total_errors),
            "progress_pct": 100.0,
            "started_at": started_at_iso,
            "updated_at": finished_at_iso,
            "finished_at": finished_at_iso,
            "message": "scan completed",
        },
    )

    print(f"[done] jsonl  : {output_jsonl}")
    print(f"[done] summary: {output_summary}")
    print(f"[done] csv    : {output_csv}")
    print(f"[done] top candidates: {len(top_rows)}")
    for row in top_rows[:10]:
        print(
            f"  {row.get('symbol')} close={row.get('close')} conf={float(row.get('confidence', 0.0)):.3f} "
            f"buy={row.get('buy_price_low')}-{row.get('buy_price_high')} lots={row.get('suggested_lots')}"
        )


if __name__ == "__main__":
    main()
