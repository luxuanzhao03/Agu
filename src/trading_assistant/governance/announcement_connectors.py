from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib import parse, request
from zoneinfo import ZoneInfo

import pandas as pd

from trading_assistant.core.models import AnnouncementRawRecord, EventConnectorType


def _parse_cursor(cursor: str | None) -> datetime | None:
    if not cursor:
        return None
    try:
        dt = datetime.fromisoformat(cursor)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def _safe_meta(value: Any) -> str | int | float | bool | None:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _pick(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row[name] is not None and str(row[name]).strip():
            return row[name]
    return None


def _parse_time_cell(value: Any, timezone_name: str = "Asia/Shanghai") -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    raw = str(value).strip()
    if not raw:
        return None
    try:
        tz = ZoneInfo(timezone_name)
    except Exception:  # noqa: BLE001
        tz = timezone.utc
    # Tushare-style yyyymmdd.
    if raw.isdigit() and len(raw) == 8:
        try:
            dt = datetime.strptime(raw, "%Y%m%d")
            return dt.replace(tzinfo=tz).astimezone(timezone.utc)
        except ValueError:
            return None
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=tz).astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=tz)
        return dt.astimezone(timezone.utc)
    except ValueError:
        return None


@dataclass
class AnnouncementFetchResult:
    records: list[AnnouncementRawRecord]
    next_cursor: str | None
    checkpoint_publish_time: datetime | None


class AnnouncementConnector:
    def fetch(self, *, cursor: str | None, limit: int) -> AnnouncementFetchResult:  # pragma: no cover - interface
        raise NotImplementedError


class FileAnnouncementConnector(AnnouncementConnector):
    def __init__(self, config: dict[str, Any]) -> None:
        path = str(config.get("file_path", "")).strip()
        if not path:
            raise ValueError("file connector requires config.file_path")
        self.path = Path(path)
        self.timezone = str(config.get("timezone", "Asia/Shanghai"))

    def fetch(self, *, cursor: str | None, limit: int) -> AnnouncementFetchResult:
        if not self.path.exists():
            raise FileNotFoundError(f"connector file not found: {self.path}")
        rows: list[dict[str, Any]]
        if self.path.suffix.lower() == ".jsonl":
            rows = [
                json.loads(line)
                for line in self.path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        else:
            parsed = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(parsed, list):
                raise ValueError("connector file must be a list of records")
            rows = [dict(item) for item in parsed]

        cursor_dt = _parse_cursor(cursor)
        items: list[AnnouncementRawRecord] = []
        latest: datetime | None = cursor_dt

        for row in rows:
            publish_time = _parse_time_cell(
                _pick(row, "publish_time", "publish_time_text", "ann_date", "pub_date"),
                timezone_name=self.timezone,
            )
            if publish_time is None:
                continue
            if cursor_dt is not None and publish_time <= cursor_dt:
                continue
            metadata = {
                key: _safe_meta(value)
                for key, value in row.items()
                if key
                not in {
                    "source_event_id",
                    "event_id",
                    "symbol",
                    "ts_code",
                    "title",
                    "summary",
                    "content",
                    "publish_time",
                    "publish_time_text",
                    "url",
                }
            }
            items.append(
                AnnouncementRawRecord(
                    source_event_id=str(_pick(row, "source_event_id", "event_id", "ann_id"))
                    if _pick(row, "source_event_id", "event_id", "ann_id")
                    else None,
                    symbol=str(_pick(row, "symbol")) if _pick(row, "symbol") else None,
                    ts_code=str(_pick(row, "ts_code")) if _pick(row, "ts_code") else None,
                    title=str(_pick(row, "title", "ann_title", "name") or ""),
                    summary=str(_pick(row, "summary", "brief", "desc") or ""),
                    content=str(_pick(row, "content", "detail", "body") or ""),
                    publish_time=publish_time,
                    url=str(_pick(row, "url", "link")) if _pick(row, "url", "link") else None,
                    metadata=metadata,
                )
            )
            if latest is None or publish_time > latest:
                latest = publish_time
            if len(items) >= limit:
                break
        return AnnouncementFetchResult(
            records=items,
            next_cursor=_to_iso(latest),
            checkpoint_publish_time=latest,
        )


class TushareAnnouncementConnector(AnnouncementConnector):
    def __init__(self, config: dict[str, Any]) -> None:
        token = str(config.get("token", "")).strip()
        if not token:
            raise ValueError("tushare connector requires token in config.token")
        self.token = token
        self.api_name = str(config.get("api_name", "anns_d")).strip() or "anns_d"
        self.start_param = str(config.get("start_param", "start_date")).strip() or "start_date"
        self.end_param = str(config.get("end_param", "end_date")).strip() or "end_date"
        self.symbol_param = str(config.get("symbol_param", "ts_code")).strip() or "ts_code"
        self.symbol_value = str(config.get("ts_code", "")).strip() or None
        self.lookback_days = int(config.get("lookback_days", 2))
        self.timezone = str(config.get("timezone", "Asia/Shanghai"))
        self.extra_params = dict(config.get("request_params", {}))

    def fetch(self, *, cursor: str | None, limit: int) -> AnnouncementFetchResult:
        import tushare as ts

        pro = ts.pro_api(self.token)
        fn = getattr(pro, self.api_name, None)
        if fn is None:
            raise ValueError(f"tushare API '{self.api_name}' is not available")

        cursor_dt = _parse_cursor(cursor)
        now_local = datetime.now(ZoneInfo(self.timezone))
        start_local = now_local - timedelta(days=365)
        if cursor_dt is not None:
            start_local = cursor_dt.astimezone(ZoneInfo(self.timezone)) - timedelta(days=max(0, self.lookback_days))

        params = dict(self.extra_params)
        params[self.start_param] = start_local.strftime("%Y%m%d")
        params[self.end_param] = now_local.strftime("%Y%m%d")
        if self.symbol_value:
            params[self.symbol_param] = self.symbol_value

        # Some deployments expose slightly different signatures; try a degraded fallback.
        try:
            frame = fn(**params)
        except TypeError:
            params.pop(self.start_param, None)
            params.pop(self.end_param, None)
            frame = fn(**params)

        if not isinstance(frame, pd.DataFrame):
            raise ValueError("tushare announcement API did not return DataFrame")
        if frame.empty:
            return AnnouncementFetchResult(records=[], next_cursor=cursor, checkpoint_publish_time=cursor_dt)

        rows = frame.to_dict(orient="records")
        items: list[AnnouncementRawRecord] = []
        latest = cursor_dt
        for row in rows:
            publish_time = _parse_time_cell(
                _pick(row, "f_ann_date", "ann_date", "publish_time", "pub_date"),
                timezone_name=self.timezone,
            )
            if publish_time is None:
                continue
            if cursor_dt is not None and publish_time <= cursor_dt:
                continue
            source_event_id = _pick(row, "ann_id", "id", "event_id")
            symbol = _pick(row, "ts_code", "symbol", "code")
            title = _pick(row, "title", "ann_title", "name", "headline")
            summary = _pick(row, "summary", "brief", "ann_type")
            content = _pick(row, "content", "detail", "body")
            url = _pick(row, "url", "link")
            metadata = {
                key: _safe_meta(value)
                for key, value in row.items()
                if key
                not in {
                    "ann_id",
                    "id",
                    "event_id",
                    "ts_code",
                    "symbol",
                    "code",
                    "title",
                    "ann_title",
                    "name",
                    "headline",
                    "summary",
                    "brief",
                    "ann_type",
                    "content",
                    "detail",
                    "body",
                    "f_ann_date",
                    "ann_date",
                    "publish_time",
                    "pub_date",
                    "url",
                    "link",
                }
            }
            items.append(
                AnnouncementRawRecord(
                    source_event_id=str(source_event_id) if source_event_id else None,
                    symbol=str(symbol) if symbol else None,
                    ts_code=str(symbol) if symbol else None,
                    title=str(title or ""),
                    summary=str(summary or ""),
                    content=str(content or ""),
                    publish_time=publish_time,
                    url=str(url) if url else None,
                    metadata=metadata,
                )
            )
            if latest is None or publish_time > latest:
                latest = publish_time
            if len(items) >= limit:
                break

        items.sort(key=lambda x: x.publish_time or datetime(1970, 1, 1, tzinfo=timezone.utc))
        return AnnouncementFetchResult(
            records=items,
            next_cursor=_to_iso(latest),
            checkpoint_publish_time=latest,
        )


def _extract_from_path(payload: Any, path: str) -> Any:
    if not path:
        return payload
    node = payload
    for part in [x for x in path.split(".") if x.strip()]:
        if isinstance(node, dict):
            node = node.get(part)
            continue
        if isinstance(node, list):
            try:
                idx = int(part)
            except ValueError:
                return None
            if idx < 0 or idx >= len(node):
                return None
            node = node[idx]
            continue
        return None
    return node


class HttpJsonAnnouncementConnector(AnnouncementConnector):
    def __init__(self, config: dict[str, Any]) -> None:
        url = str(config.get("url", "")).strip()
        if not url:
            raise ValueError("http_json connector requires config.url")
        self.url = url
        self.method = str(config.get("method", "GET")).upper()
        self.headers = dict(config.get("headers", {}))
        self.query_params = dict(config.get("query_params", {}))
        self.body = dict(config.get("body", {}))
        self.records_path = str(config.get("records_path", "")).strip()
        self.cursor_param = str(config.get("cursor_param", "cursor")).strip() or "cursor"
        self.limit_param = str(config.get("limit_param", "limit")).strip() or "limit"
        self.timeout_seconds = max(1, min(int(config.get("timeout_seconds", 10)), 60))
        self.timezone = str(config.get("timezone", "Asia/Shanghai"))

    def fetch(self, *, cursor: str | None, limit: int) -> AnnouncementFetchResult:
        payload = self._load_payload(cursor=cursor, limit=limit)
        rows = _extract_from_path(payload, self.records_path) if self.records_path else payload
        if not isinstance(rows, list):
            raise ValueError("http_json connector response must map to a list")

        cursor_dt = _parse_cursor(cursor)
        items: list[AnnouncementRawRecord] = []
        latest = cursor_dt
        for row in rows:
            if not isinstance(row, dict):
                continue
            publish_time = _parse_time_cell(
                _pick(row, "publish_time", "publish_time_text", "f_ann_date", "ann_date", "pub_date", "time"),
                timezone_name=self.timezone,
            )
            if publish_time is None:
                continue
            if cursor_dt is not None and publish_time <= cursor_dt:
                continue
            source_event_id = _pick(row, "source_event_id", "event_id", "ann_id", "id")
            symbol = _pick(row, "ts_code", "symbol", "code")
            title = _pick(row, "title", "ann_title", "headline", "name")
            summary = _pick(row, "summary", "brief", "description", "desc")
            content = _pick(row, "content", "detail", "body", "text")
            url = _pick(row, "url", "link")
            metadata = {
                key: _safe_meta(value)
                for key, value in row.items()
                if key
                not in {
                    "source_event_id",
                    "event_id",
                    "ann_id",
                    "id",
                    "ts_code",
                    "symbol",
                    "code",
                    "title",
                    "ann_title",
                    "headline",
                    "name",
                    "summary",
                    "brief",
                    "description",
                    "desc",
                    "content",
                    "detail",
                    "body",
                    "text",
                    "publish_time",
                    "publish_time_text",
                    "f_ann_date",
                    "ann_date",
                    "pub_date",
                    "time",
                    "url",
                    "link",
                }
            }
            items.append(
                AnnouncementRawRecord(
                    source_event_id=str(source_event_id) if source_event_id else None,
                    symbol=str(symbol) if symbol else None,
                    ts_code=str(symbol) if symbol else None,
                    title=str(title or ""),
                    summary=str(summary or ""),
                    content=str(content or ""),
                    publish_time=publish_time,
                    url=str(url) if url else None,
                    metadata=metadata,
                )
            )
            if latest is None or publish_time > latest:
                latest = publish_time
            if len(items) >= limit:
                break

        items.sort(key=lambda x: x.publish_time or datetime(1970, 1, 1, tzinfo=timezone.utc))
        return AnnouncementFetchResult(
            records=items,
            next_cursor=_to_iso(latest),
            checkpoint_publish_time=latest,
        )

    def _load_payload(self, *, cursor: str | None, limit: int) -> Any:
        parsed = parse.urlparse(self.url)
        if parsed.scheme == "file":
            local_path = Path(parse.unquote(parsed.path))
            return json.loads(local_path.read_text(encoding="utf-8"))
        if parsed.scheme in {"", "local"}:
            local_path = Path(self.url.replace("local://", ""))
            if local_path.exists():
                return json.loads(local_path.read_text(encoding="utf-8"))

        params = dict(self.query_params)
        params[self.limit_param] = str(limit)
        if cursor:
            params[self.cursor_param] = cursor
        query = parse.urlencode(params, doseq=True)
        url = self.url
        if query:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{query}"

        data: bytes | None = None
        if self.method in {"POST", "PUT", "PATCH"}:
            body = dict(self.body)
            body[self.limit_param] = limit
            if cursor:
                body[self.cursor_param] = cursor
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")

        req = request.Request(
            url=url,
            method=self.method,
            data=data,
            headers={"Accept": "application/json", **self.headers},
        )
        with request.urlopen(req, timeout=self.timeout_seconds) as resp:  # noqa: S310
            raw = resp.read().decode("utf-8")
        return json.loads(raw)


def build_announcement_connector(connector_type: EventConnectorType, config: dict[str, Any]) -> AnnouncementConnector:
    if connector_type == EventConnectorType.FILE_ANNOUNCEMENT:
        return FileAnnouncementConnector(config=config)
    if connector_type == EventConnectorType.TUSHARE_ANNOUNCEMENT:
        return TushareAnnouncementConnector(config=config)
    if connector_type == EventConnectorType.HTTP_JSON_ANNOUNCEMENT:
        return HttpJsonAnnouncementConnector(config=config)
    raise ValueError(f"unsupported connector type: {connector_type.value}")
