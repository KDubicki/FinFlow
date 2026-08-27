"""The only source of "now".

Ambient ``date.today()`` makes the system untestable, makes "evaluate as of
2019-06-03" impossible, and quietly breaks a backfill that runs across midnight
(``PROJECT.md`` §4.2). Time is injected everywhere instead, and
``tests/test_no_ambient_time.py`` fails the build on a relapse.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Supplies the current instant to code that must not read it directly."""

    def now(self) -> datetime:
        """Return the current instant, timezone-aware and in UTC."""
        ...

    def today(self) -> date:
        """Return the current UTC date.

        UTC rather than local time on purpose: the pipeline is scheduled in UTC
        (``PROJECT.md`` §11.6), so a local-time "today" would shift twice a year
        against the runs that produced the data.
        """
        ...
