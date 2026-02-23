from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Callable, Iterable

import pandas as pd

from trading_assistant.data.base import MarketDataProvider
from trading_assistant.data.cache_store import LocalTimeseriesCache
from trading_assistant.data.exceptions import DataProviderError

logger = logging.getLogger(__name__)


class CompositeDataProvider(MarketDataProvider):
    name = "composite"

    def __init__(
        self,
        providers: Iterable[MarketDataProvider],
        *,
        cache_store: LocalTimeseriesCache | None = None,
        enable_cache: bool = False,
    ) -> None:
        self.providers = list(providers)
        if not self.providers:
            raise ValueError("At least one provider must be configured.")
        self.cache_store = cache_store
        self.enable_cache = bool(enable_cache and cache_store is not None)

    def get_provider_by_name(self, name: str) -> MarketDataProvider | None:
        key = name.strip().lower()
        for provider in self.providers:
            if provider.name.strip().lower() == key:
                return provider
        return None

    def list_provider_names(self) -> list[str]:
        return [provider.name for provider in self.providers]

    def _call_with_fallback(
        self,
        method_name: str,
        *args,
        allow_empty_df: bool = False,
        allow_empty_dict: bool = False,
        return_source: bool = False,
        **kwargs,
    ):
        errors: list[str] = []
        for provider in self.providers:
            try:
                fn: Callable = getattr(provider, method_name)
                result = fn(*args, **kwargs)
                if isinstance(result, pd.DataFrame) and result.empty and not allow_empty_df:
                    raise RuntimeError("empty result")
                if isinstance(result, dict) and (not result) and not allow_empty_dict:
                    raise RuntimeError("empty result")
                if return_source:
                    return provider.name, result
                return result
            except Exception as exc:  # noqa: BLE001
                msg = f"{provider.name}: {exc}"
                logger.warning("Provider %s failed for %s: %s", provider.name, method_name, exc)
                errors.append(msg)
        raise DataProviderError(f"All providers failed for {method_name}: {'; '.join(errors)}")

    def get_daily_bars(self, symbol: str, start_date: date, end_date: date) -> pd.DataFrame:
        _, bars = self.get_daily_bars_with_source(symbol, start_date, end_date)
        return bars

    def get_trade_calendar(self, start_date: date, end_date: date) -> pd.DataFrame:
        return self._call_with_fallback("get_trade_calendar", start_date, end_date, allow_empty_df=True)

    def get_security_status(self, symbol: str) -> dict[str, bool]:
        return self._call_with_fallback("get_security_status", symbol)

    def get_intraday_bars(
        self,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        *,
        interval: str = "15m",
    ) -> pd.DataFrame:
        _, bars = self.get_intraday_bars_with_source(
            symbol=symbol,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            interval=interval,
        )
        return bars

    def get_daily_bars_with_source(self, symbol: str, start_date: date, end_date: date) -> tuple[str, pd.DataFrame]:
        errors: list[str] = []
        for provider in self.providers:
            try:
                if self.enable_cache and self.cache_store is not None:
                    bars = self._get_daily_bars_with_cache(
                        provider=provider,
                        symbol=symbol,
                        start_date=start_date,
                        end_date=end_date,
                    )
                else:
                    bars = provider.get_daily_bars(symbol, start_date, end_date)
                if bars.empty:
                    raise RuntimeError("empty result")
                return provider.name, bars
            except Exception as exc:  # noqa: BLE001
                msg = f"{provider.name}: {exc}"
                logger.warning("Provider %s failed for get_daily_bars: %s", provider.name, exc)
                errors.append(msg)
        raise DataProviderError(f"All providers failed for get_daily_bars: {'; '.join(errors)}")

    def get_trade_calendar_with_source(self, start_date: date, end_date: date) -> tuple[str, pd.DataFrame]:
        return self._call_with_fallback(
            "get_trade_calendar",
            start_date,
            end_date,
            allow_empty_df=True,
            return_source=True,
        )

    def get_intraday_bars_with_source(
        self,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        *,
        interval: str = "15m",
    ) -> tuple[str, pd.DataFrame]:
        errors: list[str] = []
        interval_key = str(interval).strip().lower()
        for provider in self.providers:
            try:
                if self.enable_cache and self.cache_store is not None:
                    bars = self._get_intraday_bars_with_cache(
                        provider=provider,
                        symbol=symbol,
                        start_datetime=start_datetime,
                        end_datetime=end_datetime,
                        interval=interval_key,
                    )
                else:
                    bars = provider.get_intraday_bars(
                        symbol,
                        start_datetime,
                        end_datetime,
                        interval=interval_key,
                    )
                if bars.empty:
                    raise RuntimeError("empty result")
                return provider.name, bars
            except Exception as exc:  # noqa: BLE001
                msg = f"{provider.name}: {exc}"
                logger.warning("Provider %s failed for get_intraday_bars: %s", provider.name, exc)
                errors.append(msg)
        raise DataProviderError(f"All providers failed for get_intraday_bars: {'; '.join(errors)}")

    def get_fundamental_snapshot(self, symbol: str, as_of: date) -> dict[str, object]:
        return self._call_with_fallback("get_fundamental_snapshot", symbol, as_of)

    def get_fundamental_snapshot_with_source(self, symbol: str, as_of: date) -> tuple[str, dict[str, object]]:
        return self._call_with_fallback(
            "get_fundamental_snapshot",
            symbol,
            as_of,
            return_source=True,
        )

    def get_corporate_event_snapshot(
        self,
        symbol: str,
        as_of: date,
        *,
        lookback_days: int = 120,
    ) -> dict[str, object]:
        _, snapshot = self.get_corporate_event_snapshot_with_source(symbol=symbol, as_of=as_of, lookback_days=lookback_days)
        return snapshot

    def get_corporate_event_snapshot_with_source(
        self,
        *,
        symbol: str,
        as_of: date,
        lookback_days: int = 120,
    ) -> tuple[str, dict[str, object]]:
        return self._call_with_fallback(
            "get_corporate_event_snapshot",
            symbol,
            as_of,
            lookback_days=lookback_days,
            allow_empty_dict=True,
            return_source=True,
        )

    def get_market_style_snapshot(
        self,
        as_of: date,
        *,
        lookback_days: int = 30,
    ) -> dict[str, object]:
        _, snapshot = self.get_market_style_snapshot_with_source(as_of=as_of, lookback_days=lookback_days)
        return snapshot

    def get_market_style_snapshot_with_source(
        self,
        *,
        as_of: date,
        lookback_days: int = 30,
    ) -> tuple[str, dict[str, object]]:
        return self._call_with_fallback(
            "get_market_style_snapshot",
            as_of,
            lookback_days=lookback_days,
            allow_empty_dict=True,
            return_source=True,
        )

    def _get_daily_bars_with_cache(
        self,
        *,
        provider: MarketDataProvider,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        assert self.cache_store is not None
        min_date, max_date, count = self.cache_store.coverage(provider=provider.name, symbol=symbol)
        missing_ranges: list[tuple[date, date]] = []
        if count <= 0 or min_date is None or max_date is None:
            missing_ranges.append((start_date, end_date))
        else:
            if start_date < min_date:
                missing_ranges.append((start_date, min_date - timedelta(days=1)))
            if end_date > max_date:
                missing_ranges.append((max_date + timedelta(days=1), end_date))

        cached_before_fetch = self.cache_store.load_daily_bars(
            provider=provider.name,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        expected_trade_dates = self._expected_trade_dates(
            provider=provider,
            start_date=start_date,
            end_date=end_date,
        )
        if expected_trade_dates:
            cached_dates = set()
            if not cached_before_fetch.empty:
                cached_dates = set(
                    pd.to_datetime(cached_before_fetch["trade_date"], errors="coerce")
                    .dropna()
                    .dt.date
                    .tolist()
                )
            missing_ranges.extend(
                self._missing_ranges_from_expected_dates(
                    expected_dates=expected_trade_dates,
                    cached_dates=cached_dates,
                )
            )

        missing_ranges = self._merge_ranges(missing_ranges)

        for missing_start, missing_end in missing_ranges:
            if missing_start > missing_end:
                continue
            fetched = provider.get_daily_bars(symbol, missing_start, missing_end)
            if fetched is None or fetched.empty:
                continue
            self.cache_store.upsert_daily_bars(provider=provider.name, symbol=symbol, bars=fetched)

        cached = self.cache_store.load_daily_bars(
            provider=provider.name,
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if cached.empty:
            # Force direct fetch when cache has no rows for requested range.
            direct = provider.get_daily_bars(symbol, start_date, end_date)
            if direct is not None and not direct.empty:
                self.cache_store.upsert_daily_bars(provider=provider.name, symbol=symbol, bars=direct)
                return direct.sort_values("trade_date").reset_index(drop=True)
        return cached.sort_values("trade_date").reset_index(drop=True)

    def _get_intraday_bars_with_cache(
        self,
        *,
        provider: MarketDataProvider,
        symbol: str,
        start_datetime: datetime,
        end_datetime: datetime,
        interval: str,
    ) -> pd.DataFrame:
        assert self.cache_store is not None
        min_time, max_time, count = self.cache_store.intraday_coverage(
            provider=provider.name,
            symbol=symbol,
            interval=interval,
        )
        need_fetch = (
            count <= 0
            or min_time is None
            or max_time is None
            or start_datetime < min_time
            or end_datetime > max_time
        )

        cached = self.cache_store.load_intraday_bars(
            provider=provider.name,
            symbol=symbol,
            interval=interval,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        )
        if cached.empty:
            need_fetch = True

        if need_fetch:
            fetched = provider.get_intraday_bars(
                symbol,
                start_datetime,
                end_datetime,
                interval=interval,
            )
            if fetched is not None and not fetched.empty:
                self.cache_store.upsert_intraday_bars(
                    provider=provider.name,
                    symbol=symbol,
                    interval=interval,
                    bars=fetched,
                )
            cached = self.cache_store.load_intraday_bars(
                provider=provider.name,
                symbol=symbol,
                interval=interval,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
            )

        return cached.sort_values("bar_time").reset_index(drop=True)

    @staticmethod
    def _merge_ranges(ranges: list[tuple[date, date]]) -> list[tuple[date, date]]:
        normalized = [(s, e) for s, e in ranges if s <= e]
        if not normalized:
            return []
        normalized.sort(key=lambda x: (x[0], x[1]))
        merged: list[tuple[date, date]] = [normalized[0]]
        for start, end in normalized[1:]:
            prev_start, prev_end = merged[-1]
            if start <= (prev_end + timedelta(days=1)):
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))
        return merged

    def _expected_trade_dates(
        self,
        *,
        provider: MarketDataProvider,
        start_date: date,
        end_date: date,
    ) -> list[date]:
        try:
            calendar = provider.get_trade_calendar(start_date, end_date)
        except Exception:  # noqa: BLE001
            return []
        if calendar is None or calendar.empty or "trade_date" not in calendar.columns:
            return []
        frame = calendar.copy()
        frame["trade_date"] = pd.to_datetime(frame["trade_date"], errors="coerce").dt.date
        frame = frame[frame["trade_date"].notna()]
        if frame.empty:
            return []

        if "is_open" in frame.columns:
            open_mask = frame["is_open"].apply(self._as_bool)
            frame = frame[open_mask]
        if frame.empty:
            return []
        out = sorted(set(d for d in frame["trade_date"].tolist() if start_date <= d <= end_date))
        return out

    @staticmethod
    def _missing_ranges_from_expected_dates(
        *,
        expected_dates: list[date],
        cached_dates: set[date],
    ) -> list[tuple[date, date]]:
        if not expected_dates:
            return []
        ranges: list[tuple[date, date]] = []
        start_missing: date | None = None
        end_missing: date | None = None
        for d in expected_dates:
            if d in cached_dates:
                if start_missing is not None and end_missing is not None:
                    ranges.append((start_missing, end_missing))
                start_missing = None
                end_missing = None
                continue
            if start_missing is None:
                start_missing = d
            end_missing = d
        if start_missing is not None and end_missing is not None:
            ranges.append((start_missing, end_missing))
        return ranges

    @staticmethod
    def _as_bool(value: object) -> bool:
        if isinstance(value, str):
            key = value.strip().lower()
            if key in {"1", "true", "yes", "y", "open"}:
                return True
            if key in {"0", "false", "no", "n", "closed"}:
                return False
        return bool(value)
