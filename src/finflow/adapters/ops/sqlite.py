"""The operational store, on SQLite.

SQLite rather than DuckDB is deliberate (``PROJECT.md`` §4.3): this is a
row-store workload of small transactional writes from more than one process, and
WAL mode handles concurrent writers, which DuckDB's single-writer model does not.

Schema changes go through ``migrations``, applied on start. This store is the one
piece of state a rebuild cannot recreate, so a change to it has to be a
migration rather than an edit only new installations would see.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

from finflow.adapters.ops.migrations import current_version, migrate
from finflow.contracts.sources import SourceKey
from finflow.logging import get_logger
from finflow.ports.ops_store import PipelineRun, Watermark

log = get_logger(__name__)


class SqliteOpsStore:
    """Watermarks in a SQLite file, in WAL mode."""

    def __init__(self, path: Path | str) -> None:
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Outside a transaction: journal_mode cannot be changed from inside one.
        # WAL is a property of the file, so this survives, but setting it on
        # every open is harmless and means a restored backup gets it too.
        conn = sqlite3.connect(self._path, isolation_level=None)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            applied = migrate(conn)
            if applied:
                log.info("ops_store_migrated", applied=applied, version=current_version(conn))
        finally:
            conn.close()

    @property
    def path(self) -> Path:
        """Where this store lives, for the backup job."""
        return self._path

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._path, isolation_level=None, timeout=30.0)
        try:
            conn.execute("BEGIN")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def watermark(self, source: SourceKey, symbol: str) -> Watermark | None:
        """Return one watermark, or None if the pair has never been ingested."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM watermarks WHERE source = ? AND symbol = ?",
                (str(source), symbol),
            ).fetchone()
        return _to_watermark(row) if row else None

    def watermarks(self) -> tuple[Watermark, ...]:
        """Every watermark, ordered by source then symbol."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM watermarks ORDER BY source, symbol").fetchall()
        return tuple(_to_watermark(row) for row in rows)

    def save_watermark(self, watermark: Watermark) -> None:
        """Insert or update one watermark."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watermarks
                    (source, symbol, last_loaded_date, last_run_at, row_count, deferred_until)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, symbol) DO UPDATE SET
                    last_loaded_date = excluded.last_loaded_date,
                    last_run_at      = excluded.last_run_at,
                    row_count        = excluded.row_count,
                    deferred_until   = excluded.deferred_until
                """,
                (
                    str(watermark.source),
                    watermark.symbol,
                    _iso(watermark.last_loaded_date),
                    _iso(watermark.last_run_at),
                    watermark.row_count,
                    _iso(watermark.deferred_until),
                ),
            )

    def defer(self, source: SourceKey, symbol: str, until: datetime) -> None:
        """Mark a pair deferred without disturbing its loaded-date progress.

        A separate statement rather than a read-modify-write of the whole row,
        because deferring must not roll back a ``last_loaded_date`` written by a
        concurrent run.
        """
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO watermarks (source, symbol, deferred_until)
                VALUES (?, ?, ?)
                ON CONFLICT(source, symbol) DO UPDATE SET deferred_until = excluded.deferred_until
                """,
                (str(source), symbol, until.isoformat()),
            )

    # ---- pipeline runs ---------------------------------------------------

    def save_run(self, run: PipelineRun) -> None:
        """Insert or update one pipeline run."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO pipeline_runs
                    (run_id, started_at, ended_at, status, rows_written,
                     snapshot_id, manifest_ref, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    ended_at     = excluded.ended_at,
                    status       = excluded.status,
                    rows_written = excluded.rows_written,
                    snapshot_id  = excluded.snapshot_id,
                    manifest_ref = excluded.manifest_ref,
                    error        = excluded.error
                """,
                (
                    run.run_id,
                    run.started_at.isoformat(),
                    _iso(run.ended_at),
                    run.status,
                    run.rows_written,
                    run.snapshot_id,
                    run.manifest_ref,
                    run.error,
                ),
            )

    def runs(self, limit: int = 20) -> tuple[PipelineRun, ...]:
        """The most recent runs, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM pipeline_runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return tuple(_to_run(row) for row in rows)

    def last_successful_run(self) -> PipelineRun | None:
        """The most recent run that finished cleanly, if there is one."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM pipeline_runs WHERE status = 'succeeded' "
                "ORDER BY started_at DESC LIMIT 1"
            ).fetchone()
        return _to_run(row) if row else None

    @property
    def schema_version(self) -> int:
        """The applied migration version, asserted by the deploy smoke test."""
        conn = sqlite3.connect(self._path)
        try:
            return current_version(conn)
        finally:
            conn.close()


def _to_run(row: tuple[object, ...]) -> PipelineRun:
    run_id, started, ended, status, rows, snapshot, manifest, error = row
    return PipelineRun(
        run_id=str(run_id),
        started_at=datetime.fromisoformat(str(started)),
        ended_at=datetime.fromisoformat(str(ended)) if ended else None,
        status=str(status),
        rows_written=int(str(rows)) if rows is not None else 0,
        snapshot_id=str(snapshot) if snapshot else None,
        manifest_ref=str(manifest) if manifest else None,
        error=str(error) if error else None,
    )


def _iso(value: date | datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _to_watermark(row: tuple[object, ...]) -> Watermark:
    source, symbol, loaded, run_at, count, deferred = row
    return Watermark(
        source=SourceKey(str(source)),
        symbol=str(symbol),
        last_loaded_date=date.fromisoformat(str(loaded)) if loaded else None,
        last_run_at=datetime.fromisoformat(str(run_at)) if run_at else None,
        row_count=int(str(count)) if count is not None else 0,
        deferred_until=datetime.fromisoformat(str(deferred)) if deferred else None,
    )
