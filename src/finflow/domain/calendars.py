"""Trading-calendar lookups.

A thin, cached wrapper over ``exchange_calendars`` so that the registry can
reject an unknown calendar code at load time rather than discovering it during a
backfill. Kept in ``domain`` because "which days does this venue trade" is
domain knowledge, not an adapter concern.
"""

from __future__ import annotations

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
