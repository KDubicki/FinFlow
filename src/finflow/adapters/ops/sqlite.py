"""The operational store, on SQLite.

SQLite rather than DuckDB is deliberate (``PROJECT.md`` §4.3): this is a
row-store workload of small transactional writes from more than one process, and
WAL mode handles concurrent writers, which DuckDB's single-writer model does not.

At this stage it holds watermarks only. ``pipeline_runs``, the outbox and proper
versioned migrations arrive with the warehouse and the first scheduled run.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path

from finflow.contracts.sources import SourceKey
from finflow.ports.ops_store import Watermark

_SCHEMA = """
CREATE TABLE IF NOT EXISTS watermarks (
    source           TEXT NOT NULL,
    symbol           TEXT NOT NULL,
    last_loaded_date TEXT,
    last_run_at      TEXT,
    row_count        INTEGER NOT NULL DEFAULT 0,
    deferred_until   TEXT,
    PRIMARY KEY (source, symbol)
);
"""


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
            conn.executescript(_SCHEMA)
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
