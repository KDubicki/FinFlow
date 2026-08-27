"""Test doubles for the ports.

These live in ``tests`` rather than ``src`` deliberately: a fake that ships in
the package is a fake that eventually gets imported by production code.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta


class FrozenClock:
    """A clock that does not move unless a test moves it.

    Satisfies the ``Clock`` protocol structurally, which is the point of using
    ``Protocol`` rather than a base class: the fake owes the port nothing but a
    matching shape.
    """

    def __init__(self, at: datetime | date) -> None:
        if isinstance(at, datetime):
            self._at = at if at.tzinfo else at.replace(tzinfo=UTC)
        else:
            self._at = datetime(at.year, at.month, at.day, tzinfo=UTC)

    def now(self) -> datetime:
        """Return the instant this clock was frozen at."""
        return self._at

    def today(self) -> date:
        """Return the UTC date this clock was frozen at."""
        return self._at.date()

    def advance(self, **delta: float) -> None:
        """Move the clock forward, for tests that need two distinct instants."""
        self._at += timedelta(**delta)
