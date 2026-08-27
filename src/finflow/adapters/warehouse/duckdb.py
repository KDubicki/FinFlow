"""DuckDB as the analytical store.

Single-writer discipline is the whole point of this module. DuckDB holds an
exclusive lock on the database file, so two read-write connections cannot
coexist — and the way that failure actually shows up is the 05:30 run dying
because a Streamlit session left overnight still has the file open
(``PROJECT.md`` §4.5).

So: one object owns the writer, everything else asks for a reader, and taking a
second writer raises something with a message that says what to do rather than
DuckDB's own lock error.
"""

from __future__ import annotations

from pathlib import Path
from types import TracebackType
from typing import Any

import duckdb
import polars as pl

from finflow.contracts.errors import FinFlowError
from finflow.logging import get_logger

log = get_logger(__name__)


class WarehouseLockedError(FinFlowError):
    """Another process holds the write lock on the warehouse."""

    def __init__(self, path: Path, cause: str) -> None:
        super().__init__(
            f"cannot open {path} for writing — another process holds the lock. "
            f"Close any open UI, notebook or duckdb shell, or open read-only. ({cause})"
        )
        self.path = path


class DuckDBWarehouse:
    """A connection to one DuckDB file.

    Construct with ``read_only=True`` for anything that is not the pipeline run.
    Readers can coexist with each other and, on a serving snapshot, with the
    writer working on the live file.
    """

    def __init__(self, path: Path | str, *, read_only: bool = False) -> None:
        self._path = Path(path)
        self._read_only = read_only
        if not read_only:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        elif not self._path.exists():
            # A read-only open of a missing file creates an empty one, which is
            # worse than failing: the caller gets a warehouse with no tables and
            # no explanation.
            raise FileNotFoundError(f"no warehouse at {self._path}")

        try:
            self._conn = duckdb.connect(str(self._path), read_only=read_only)
        except (duckdb.IOException, duckdb.ConnectionException) as exc:
            # IOException is the cross-process file lock; ConnectionException is
            # the in-process refusal to reopen the same file with a different
            # configuration. Both mean "someone else has it".
            raise WarehouseLockedError(self._path, str(exc)) from exc

    # ---- lifecycle -------------------------------------------------------

    def __enter__(self) -> DuckDBWarehouse:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """Release the connection, and with it the file lock."""
        self._conn.close()

    @property
    def path(self) -> Path:
        """Where this warehouse lives."""
        return self._path

    @property
    def is_read_only(self) -> bool:
        """True when this connection cannot write."""
        return self._read_only

    # ---- queries ---------------------------------------------------------

    def query(self, sql: str, **params: Any) -> pl.DataFrame:
        """Run a SELECT and return the result as a Polars frame."""
        return self._conn.execute(sql, params or None).pl()

    def execute(self, sql: str, **params: Any) -> None:
        """Run a statement for its effect."""
        self._conn.execute(sql, params or None)

    def register(self, name: str, frame: pl.DataFrame) -> None:
        """Expose a Polars frame to SQL without writing it to disk first."""
        self._conn.register(name, frame)

    def unregister(self, name: str) -> None:
        """Withdraw a registered frame."""
        self._conn.unregister(name)

    def tables(self) -> tuple[str, ...]:
        """Every table and view in the main schema, ordered."""
        rows = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name"
        ).fetchall()
        return tuple(str(row[0]) for row in rows)

    def snapshot_to(self, destination: Path) -> None:
        """Copy this warehouse to ``destination`` atomically.

        Written to a temporary file next to the destination and then renamed, so
        a reader either sees the whole previous snapshot or the whole new one —
        never a half-copied file. This is what lets the API and the UI read
        while a pipeline run is writing (``PROJECT.md`` §4.5).
        """
        destination.parent.mkdir(parents=True, exist_ok=True)
        staged = destination.with_suffix(destination.suffix + ".staging")
        staged.unlink(missing_ok=True)
        # ATTACH + COPY FROM DATABASE produces a consistent copy without
        # requiring the source to be quiesced.
        source = self._conn.execute("SELECT current_database()").fetchone()
        assert source is not None
        self._conn.execute(f"ATTACH '{staged}' AS snapshot")
        try:
            self._conn.execute(f'COPY FROM DATABASE "{source[0]}" TO snapshot')
        finally:
            self._conn.execute("DETACH snapshot")
        staged.replace(destination)
        log.info("snapshot_promoted", destination=str(destination))
