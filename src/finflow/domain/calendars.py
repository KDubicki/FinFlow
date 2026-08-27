"""Trading-calendar lookups.

A thin, cached wrapper over ``exchange_calendars`` so that the registry can
reject an unknown calendar code at load time rather than discovering it during a
backfill. Kept in ``domain`` because "which days does this venue trade" is
domain knowledge, not an adapter concern.
"""

from __future__ import annotations

from datetime import date
from functools import lru_cache

import exchange_calendars


@lru_cache(maxsize=1)
def known_calendars() -> frozenset[str]:
    """Every calendar code ``exchange_calendars`` recognises.

    Cached because the registry validates one code per instrument and the
    underlying call rebuilds its list on every invocation.
    """
    return frozenset(exchange_calendars.get_calendar_names())


def is_known_calendar(code: str) -> bool:
    """True when ``code`` is a calendar the project can actually resolve."""
    return code in known_calendars()


def trading_days(code: str, start: date, end: date) -> list[date]:
    """Every session ``code`` trades between ``start`` and ``end``, inclusive.

    This is what makes a missing bar diagnosable. Absent a calendar, a gap on a
    US holiday and a gap caused by a failed fetch look identical, and only one
    of them is an incident (``PROJECT.md`` §6.8).
    """
    # Built for the requested window, then filtered in Python. Asking the
    # calendar for a range is tempting but raises when either bound falls
    # outside its own first or last session -- which is exactly what happens
    # when the range starts on a holiday, i.e. most 1 January.
    calendar = exchange_calendars.get_calendar(code, start=str(start), end=str(end))
    return [d.date() for d in calendar.sessions if start <= d.date() <= end]
