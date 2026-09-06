"""Trading-calendar lookups.

A thin, cached wrapper over ``exchange_calendars`` so that the registry can
reject an unknown calendar code at load time rather than discovering it during a
backfill. Kept in ``domain`` because "which days does this venue trade" is
domain knowledge, not an adapter concern.
"""

from __future__ import annotations

from datetime import date, timedelta
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
    # Widened by a day at each end before construction. `get_calendar` requires
    # start < end, so a single-day range -- an instrument with exactly one bar,
    # which happens on a first backfill -- would otherwise raise.
    window_start = start - timedelta(days=1)
    window_end = end + timedelta(days=1)
    calendar = exchange_calendars.get_calendar(code, start=str(window_start), end=str(window_end))
    return [d.date() for d in calendar.sessions if start <= d.date() <= end]


def sessions_behind(code: str, data_as_of: date, today: date) -> int:
    """Completed sessions with no data — how stale the pipeline actually is.

    Calendar days are the wrong unit for this question and the digest proved it:
    a Friday bar read on a Sunday morning is two days old and perfectly fresh,
    and a system that says "degraded" every weekend has taught its reader to
    ignore the word by the second Monday.

    ``today`` itself is excluded. The run happens before that session's bar
    exists, so counting it would make every run stale by one.
    """
    if data_as_of >= today:
        return 0
    return len(trading_days(code, data_as_of + timedelta(days=1), today - timedelta(days=1)))
