"""The analytical-store seam.

Derived, disposable, and rebuildable from the raw zone in seconds
(``PROJECT.md`` §4.3). The one guarantee that matters is **exactly one writer**:
DuckDB takes an exclusive file lock, so a second read-write connection does not
queue, it fails — and it fails the scheduled run, at 05:30, because something
else had the file open.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

import polars as pl


@runtime_checkable
class Warehouse(Protocol):
    """A SQL store holding bronze tables, marts and derived facts.

    Contract:

    - At most one read-write connection exists at a time. A second is refused.
    - ``query`` is safe from a read-only connection and never mutates.
    - ``register`` makes a frame visible to SQL without a round trip through
      disk, which is what keeps the loader a query rather than a copy.
    """

    def query(self, sql: str, **params: Any) -> pl.DataFrame:
        """Run a SELECT and return the result."""
        ...

    def execute(self, sql: str, **params: Any) -> None:
        """Run a statement for its effect. Requires the writer."""
        ...

    def register(self, name: str, frame: pl.DataFrame) -> None:
        """Expose a frame to SQL under ``name`` for the life of the connection."""
        ...

    def unregister(self, name: str) -> None:
        """Withdraw a registered frame, so it stops appearing as a view."""
        ...

    def tables(self) -> tuple[str, ...]:
        """Every table and view name, ordered."""
        ...
