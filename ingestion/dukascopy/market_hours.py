"""When FX and metals trade, hour by hour. Used to tell "not published yet" from "closed".

The market follows New York time: each session runs from 18:00 the evening before to 17:00,
the daily break is the 17:00 hour, and the week runs from Sunday 18:00 to Friday 17:00. In
UTC that moves by an hour with US daylight saving, so everything is decided in New York time.

Holidays, checked against two years of Dukascopy XAUUSD bars:
  * no session at all: Christmas Day, New Year's Day, Good Friday
  * sessions that end early (the hours from the close to 17:00 have no ticks):
      13:00  US holidays that fall on a Friday (July 4th, Juneteenth, observed days included)
      14:00  December 24th
      15:00  other US holidays (MLK, Presidents', Memorial, Juneteenth, July 4th, Labor Day,
             Thanksgiving) and the Friday after Thanksgiving
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

from dateutil.easter import easter

NEW_YORK = ZoneInfo("America/New_York")


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """n-th weekday (0 = Monday) of a month; n = -1 is the last one."""
    if n > 0:
        first = date(year, month, 1)
        return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))
    last = (date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def _observed(day: date) -> date:
    """US federal rule: a Saturday holiday is observed on Friday, a Sunday one on Monday."""
    return day - timedelta(days=1) if day.weekday() == 5 else day + timedelta(days=1) if day.weekday() == 6 else day


@lru_cache(maxsize=64)
def closed_sessions(year: int) -> frozenset[date]:
    """Session dates (the New York day a session ends on) with no trading at all."""
    return frozenset({date(year, 12, 25), date(year, 1, 1), easter(year) - timedelta(days=2)})


@lru_cache(maxsize=64)
def early_closes(year: int) -> dict[date, int]:
    """Session date -> the New York hour trading stops at."""
    holidays = [
        _nth_weekday(year, 1, 0, 3),  # Martin Luther King Jr. Day
        _nth_weekday(year, 2, 0, 3),  # Presidents' Day
        _nth_weekday(year, 5, 0, -1),  # Memorial Day
        _observed(date(year, 6, 19)),  # Juneteenth
        _observed(date(year, 7, 4)),  # Independence Day
        _nth_weekday(year, 9, 0, 1),  # Labor Day
    ]
    thanksgiving = _nth_weekday(year, 11, 3, 4)
    out = {day: 13 if day.weekday() == 4 else 15 for day in holidays}
    out[thanksgiving] = 15
    out[thanksgiving + timedelta(days=1)] = 15
    out.setdefault(date(year, 12, 24), 14)
    return out


def session_date(ny: datetime) -> date:
    """The session a New York time belongs to: from 18:00 on, it is the next day's."""
    return (ny + timedelta(hours=6)).date()


def is_open(hour: datetime) -> bool:
    """True when the market trades during the hour starting at `hour` (any timezone-aware datetime)."""
    ny = hour.astimezone(NEW_YORK)
    weekday = ny.weekday()
    if weekday == 5 or (weekday == 4 and ny.hour >= 17) or (weekday == 6 and ny.hour < 18) or ny.hour == 17:
        return False
    session = session_date(ny)
    if session in closed_sessions(session.year):
        return False
    close = early_closes(session.year).get(session)
    return close is None or ny.date() != session or ny.hour < close


def open_hours(day: date) -> int:
    """How many UTC hours of `day` the market is open."""
    from datetime import timezone

    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return sum(is_open(start + timedelta(hours=h)) for h in range(24))
