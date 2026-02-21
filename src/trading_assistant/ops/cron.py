from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


def _parse_int(value: str, field: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"invalid integer '{value}' for field '{field}'") from exc


def _normalize_dow(value: int) -> int:
    if value == 7:
        return 0
    return value


def _python_weekday_to_cron(value: int) -> int:
    # Python weekday: Monday=0 ... Sunday=6
    # Cron weekday: Sunday=0 ... Saturday=6
    return (value + 1) % 7


@dataclass(frozen=True)
class CronField:
    name: str
    minimum: int
    maximum: int
    values: frozenset[int]
    raw_any: bool = False

    @classmethod
    def parse(cls, name: str, token: str, minimum: int, maximum: int, *, allow_dow_7: bool = False) -> "CronField":
        text = token.strip()
        if not text:
            raise ValueError(f"empty token for field '{name}'")

        raw_any = text == "*"
        values: set[int] = set()

        for part in text.split(","):
            part = part.strip()
            if not part:
                raise ValueError(f"empty list part in field '{name}'")
            values |= _expand_part(part, name, minimum, maximum, allow_dow_7=allow_dow_7)

        if not values:
            raise ValueError(f"field '{name}' resolves to empty values")
        full = set(range(minimum, maximum + 1))
        raw_any = raw_any or values == full
        return cls(name=name, minimum=minimum, maximum=maximum, values=frozenset(values), raw_any=raw_any)

    def contains(self, value: int) -> bool:
        return value in self.values


def _expand_part(part: str, field: str, minimum: int, maximum: int, *, allow_dow_7: bool) -> set[int]:
    step = 1
    base = part
    if "/" in part:
        base, step_text = part.split("/", 1)
        step = _parse_int(step_text, field)
        if step <= 0:
            raise ValueError(f"step must be > 0 in field '{field}'")

    values: list[int] = []
    if base == "*":
        start = minimum
        end = maximum
        values = list(range(start, end + 1))
    elif "-" in base:
        start_text, end_text = base.split("-", 1)
        start = _parse_int(start_text, field)
        end = _parse_int(end_text, field)
        if end < start:
            raise ValueError(f"invalid range '{base}' for field '{field}'")
        values = list(range(start, end + 1))
    else:
        values = [_parse_int(base, field)]

    out: set[int] = set()
    for idx, raw in enumerate(values):
        if idx % step != 0:
            continue
        normalized = _normalize_dow(raw) if allow_dow_7 else raw
        upper = 7 if allow_dow_7 else maximum
        if raw < minimum or raw > upper:
            raise ValueError(f"value '{raw}' out of range for field '{field}'")
        if normalized < minimum or normalized > maximum:
            raise ValueError(f"value '{normalized}' out of range for field '{field}'")
        out.add(normalized)
    return out


@dataclass(frozen=True)
class CronSchedule:
    expression: str
    minute: CronField
    hour: CronField
    day_of_month: CronField
    month: CronField
    day_of_week: CronField

    @classmethod
    def parse(cls, expression: str) -> "CronSchedule":
        text = expression.strip()
        parts = [p for p in text.split() if p]
        if len(parts) != 5:
            raise ValueError("cron expression must contain exactly 5 fields: minute hour day month weekday")

        minute = CronField.parse("minute", parts[0], 0, 59)
        hour = CronField.parse("hour", parts[1], 0, 23)
        day_of_month = CronField.parse("day_of_month", parts[2], 1, 31)
        month = CronField.parse("month", parts[3], 1, 12)
        day_of_week = CronField.parse("day_of_week", parts[4], 0, 6, allow_dow_7=True)
        return cls(
            expression=text,
            minute=minute,
            hour=hour,
            day_of_month=day_of_month,
            month=month,
            day_of_week=day_of_week,
        )

    def matches(self, dt: datetime) -> bool:
        current = dt.replace(second=0, microsecond=0)
        cron_dow = _python_weekday_to_cron(current.weekday())

        if not self.minute.contains(current.minute):
            return False
        if not self.hour.contains(current.hour):
            return False
        if not self.month.contains(current.month):
            return False

        dom_match = self.day_of_month.contains(current.day)
        dow_match = self.day_of_week.contains(cron_dow)
        if self.day_of_month.raw_any and self.day_of_week.raw_any:
            day_match = True
        elif self.day_of_month.raw_any:
            day_match = dow_match
        elif self.day_of_week.raw_any:
            day_match = dom_match
        else:
            # Standard cron behavior: when both are restricted, either match is accepted.
            day_match = dom_match or dow_match
        return day_match

    def next_after(self, dt: datetime, max_minutes: int = 527040) -> datetime | None:
        cursor = dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for _ in range(max_minutes):
            if self.matches(cursor):
                return cursor
            cursor += timedelta(minutes=1)
        return None

    def previous_at_or_before(self, dt: datetime, max_minutes: int = 527040) -> datetime | None:
        cursor = dt.replace(second=0, microsecond=0)
        for _ in range(max_minutes):
            if self.matches(cursor):
                return cursor
            cursor -= timedelta(minutes=1)
        return None
