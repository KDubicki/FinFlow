"""The real clock."""

from __future__ import annotations

from datetime import UTC, date, datetime


class SystemClock:
    """Reads the operating system clock in UTC.

    The one place in the codebase permitted to call ``datetime.now``; the guard
    test excludes ``adapters`` for exactly this reason.
    """

    def now(self) -> datetime:
        """Return the current instant in UTC."""
        return datetime.now(UTC)

    def today(self) -> date:
        """Return the current UTC date."""
        return self.now().date()
