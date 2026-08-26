from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl

from .models import ReportConfig


@dataclass(frozen=True)
class ReportingCalendar:
    name: str
    timezone: str
    periods_per_year: int
    day_cutoff_hour: int = 0
    trading_weekdays: tuple[int, ...] = tuple(range(7))
    holidays: frozenset[date] = frozenset()

    @classmethod
    def from_config(cls, config: ReportConfig) -> "ReportingCalendar":
        return cls(
            name=config.calendar,
            timezone=config.timezone,
            periods_per_year=config.annualization,
            day_cutoff_hour=config.day_cutoff_hour,
            trading_weekdays=config.trading_weekdays
            if config.trading_weekdays is not None
            else tuple(range(7))
            if config.calendar == "crypto_utc"
            else tuple(range(5)),
            holidays=frozenset(date.fromisoformat(item) for item in config.calendar_holidays),
        )

    def session_for_timestamp(self, value: datetime) -> date:
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo(self.timezone))
        else:
            value = value.astimezone(ZoneInfo(self.timezone))
        value -= timedelta(hours=self.day_cutoff_hour)
        session = value.date()
        while session.weekday() not in self.trading_weekdays or session in self.holidays:
            session -= timedelta(days=1)
        return session

    def session_expr(self, timestamp: str = "timestamp") -> pl.Expr:
        value = pl.col(timestamp)
        if isinstance(value, pl.Expr):
            value = value.dt.convert_time_zone(self.timezone)
        if self.day_cutoff_hour:
            value = value.dt.offset_by(f"-{self.day_cutoff_hour}h")
        return value.dt.date().alias("session")
