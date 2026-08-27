"""The operational-store seam.

Small, transactional and **authoritative**: nothing here is derivable from the
raw zone, which is exactly the criterion that put it in a different store from
the warehouse (``PROJECT.md`` §4.3).

It holds watermarks and ``pipeline_runs``. The outbox arrives with the first
scheduled run.
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


@dataclass(frozen=True)
class PipelineRun:
    """One execution of the pipeline, successful or not.

    Makes "when did this last actually work" a query rather than a log scroll
    (``PROJECT.md`` §11.2). Every run writes a row, including the ones that
    fail — a run that leaves no trace is indistinguishable from one that never
    started.
    """

    run_id: str
    started_at: datetime
    ended_at: datetime | None = None
    status: str = "running"
    rows_written: int = 0
    snapshot_id: str | None = None
    manifest_ref: str | None = None
    error: str | None = None


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

    def save_run(self, run: PipelineRun) -> None:
        """Insert or update one pipeline run."""
        ...

    def runs(self, limit: int = 20) -> tuple[PipelineRun, ...]:
        """The most recent runs, newest first."""
        ...

    def last_successful_run(self) -> PipelineRun | None:
        """The most recent run that finished cleanly, if there is one."""
        ...
