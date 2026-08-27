"""The operational-store seam.

Small, transactional and **authoritative**: nothing here is derivable from the
raw zone, which is exactly the criterion that put it in a different store from
the warehouse (``PROJECT.md`` §4.3).

At this stage it holds watermarks only. ``pipeline_runs``, the outbox and
migrations arrive with the warehouse and the first scheduled run.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol, runtime_checkable

from finflow.contracts.sources import SourceKey


@dataclass(frozen=True)
class Watermark:
    """How far ingestion has got for one ``(source, symbol)`` pair."""

    source: SourceKey
    symbol: str
    last_loaded_date: date | None = None
    last_run_at: datetime | None = None
    row_count: int = 0
    deferred_until: datetime | None = None
    """Set when a rate limit was hit. The next run skips this pair until the
    window passes, which is how ``SourceRateLimited`` resumes cleanly rather
    than re-hitting the cap on the first symbol every morning."""

    def is_deferred(self, now: datetime) -> bool:
        """True when this pair should be skipped for the moment."""
        return self.deferred_until is not None and now < self.deferred_until


@runtime_checkable
class OpsStore(Protocol):
    """Authoritative operational state.

    Contract: writes are transactional, and two processes may write
    concurrently — which is why this is SQLite in WAL mode rather than the
    analytical store (``PROJECT.md`` §4.3).
    """

    def watermark(self, source: SourceKey, symbol: str) -> Watermark | None:
        """Return one watermark, or None if the pair has never been ingested."""
        ...

    def watermarks(self) -> tuple[Watermark, ...]:
        """Every watermark, ordered by source then symbol."""
        ...

    def save_watermark(self, watermark: Watermark) -> None:
        """Insert or update one watermark."""
        ...

    def defer(self, source: SourceKey, symbol: str, until: datetime) -> None:
        """Mark a pair deferred without disturbing its loaded-date progress."""
        ...
