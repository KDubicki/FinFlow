"""The operational store, on SQLite.

SQLite rather than DuckDB is deliberate (``PROJECT.md`` §4.3): this is a
row-store workload of small transactional writes from more than one process, and
WAL mode handles concurrent writers, which DuckDB's single-writer model does not.

Schema changes go through ``migrations``, applied on start. This store is the one
piece of state a rebuild cannot recreate, so a change to it has to be a
migration rather than an edit only new installations would see.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from finflow.adapters.ops.migrations import current_version, migrate
from finflow.contracts.sources import SourceKey
from finflow.domain.decision import Decision
from finflow.logging import get_logger
from finflow.ports.ops_store import (
    ActualPosition,
    Control,
    ControlKind,
    JournalEntry,
    OutboxEntry,
    PipelineRun,
    Watermark,
)

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

    # ---- decisions -------------------------------------------------------

    def save_decision(self, decision: Decision, *, run_id: str, now: datetime) -> None:
        """Record one decision and explode its target portfolio.

        ``INSERT OR REPLACE`` rather than plain insert: a decision id is a
        content address, so re-writing one can only ever write the same content,
        and refusing would fail a re-run that is otherwise correct.
        """
        payload = json.dumps(decision.to_payload(), sort_keys=True)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO decisions
                    (decision_id, strategy_id, strategy_version, scope, as_of,
                     data_as_of, snapshot_id, run_id, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.decision_id,
                    decision.strategy_id,
                    decision.strategy_version,
                    str(decision.scope),
                    decision.as_of.isoformat(),
                    _iso(decision.data_as_of),
                    decision.snapshot_id,
                    run_id,
                    payload,
                    now.isoformat(),
                ),
            )
            conn.execute(
                "DELETE FROM positions_target WHERE decision_id = ?", (decision.decision_id,)
            )
            conn.executemany(
                """
                INSERT INTO positions_target (decision_id, instrument, weight, units, generated_at)
                VALUES (?, ?, ?, NULL, ?)
                """,
                [
                    (decision.decision_id, p.symbol, p.weight, now.isoformat())
                    for p in decision.positions
                ],
            )

    def decisions(self, *, strategy_id: str | None = None, limit: int = 20) -> tuple[Decision, ...]:
        """The most recent decisions, newest first."""
        sql = "SELECT payload FROM decisions"
        params: tuple[object, ...] = ()
        if strategy_id is not None:
            sql += " WHERE strategy_id = ?"
            params = (strategy_id,)
        sql += " ORDER BY as_of DESC, created_at DESC LIMIT ?"
        with self._connect() as conn:
            rows = conn.execute(sql, (*params, limit)).fetchall()
        return tuple(Decision.from_payload(json.loads(str(row[0]))) for row in rows)

    # ---- the outbox ------------------------------------------------------

    def enqueue(self, decision: Decision, *, now: datetime) -> bool:
        """Add one decision to the outbox, ignoring a duplicate."""
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT OR IGNORE INTO alerts_outbox
                    (strategy_id, strategy_version, decision_id, scope, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.strategy_id,
                    decision.strategy_version,
                    decision.decision_id,
                    str(decision.scope),
                    json.dumps(decision.to_payload(), sort_keys=True),
                    now.isoformat(),
                ),
            )
            return cursor.rowcount > 0

    def claim(self, *, now: datetime, lease: timedelta, limit: int = 10) -> tuple[OutboxEntry, ...]:
        """Take ownership of pending rows for the length of the lease.

        Selection and update happen in one transaction, so two processes cannot
        both claim the same row -- the ops store is the one place in this
        system where concurrent writers are expected (``PROJECT.md`` §4.3).
        """
        cutoff = (now - lease).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM alerts_outbox
                WHERE sent_at IS NULL
                  AND (claimed_at IS NULL OR claimed_at < ?)
                ORDER BY id
                LIMIT ?
                """,
                (cutoff, limit),
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE alerts_outbox SET claimed_at = ?, attempts = attempts + 1 WHERE id = ?",
                    (now.isoformat(), row[0]),
                )
        # The returned entries carry the *incremented* attempt count: the row
        # in hand was read before the update, and a caller deciding whether to
        # abandon a poison message must see the attempt it is about to make.
        return tuple(
            replace(_to_entry(row, claimed_at=now), attempts=_to_entry(row).attempts + 1)
            for row in rows
        )

    def mark_sent(self, entry_id: int, *, now: datetime, provider_id: str | None = None) -> None:
        """Record that a claimed row was delivered."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE alerts_outbox SET sent_at = ?, provider_id = ?, last_error = NULL "
                "WHERE id = ?",
                (now.isoformat(), provider_id, entry_id),
            )

    def release(self, entry_id: int, *, error: str) -> None:
        """Return a claimed row to the queue after a failed send."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE alerts_outbox SET claimed_at = NULL, last_error = ? WHERE id = ?",
                (error[:500], entry_id),
            )

    def last_enqueued(self, strategy_id: str) -> OutboxEntry | None:
        """The newest outbox row for one strategy, sent or not."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM alerts_outbox WHERE strategy_id = ? ORDER BY id DESC LIMIT 1",
                (strategy_id,),
            ).fetchone()
        return _to_entry(row) if row else None

    def pending(self) -> tuple[OutboxEntry, ...]:
        """Every undelivered row, oldest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts_outbox WHERE sent_at IS NULL ORDER BY id"
            ).fetchall()
        return tuple(_to_entry(row) for row in rows)

    def sent(self, *, since: datetime | None = None) -> tuple[OutboxEntry, ...]:
        """Delivered rows, optionally only those sent since an instant."""
        sql = "SELECT * FROM alerts_outbox WHERE sent_at IS NOT NULL"
        params: tuple[object, ...] = ()
        if since is not None:
            sql += " AND sent_at >= ?"
            params = (since.isoformat(),)
        sql += " ORDER BY id"
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return tuple(_to_entry(row) for row in rows)

    # ---- controls --------------------------------------------------------

    def set_control(self, control: Control) -> None:
        """Insert or replace one control."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO controls (kind, scope, until, set_at, reason)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(kind, scope) DO UPDATE SET
                    until  = excluded.until,
                    set_at = excluded.set_at,
                    reason = excluded.reason
                """,
                (
                    str(control.kind),
                    control.scope,
                    _iso(control.until),
                    _iso(control.set_at) or "",
                    control.reason,
                ),
            )

    def clear_control(self, kind: ControlKind, scope: str) -> bool:
        """Remove one control. False when there was nothing to remove."""
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM controls WHERE kind = ? AND scope = ?", (str(kind), scope)
            )
            return cursor.rowcount > 0

    def controls(self, *, today: date | None = None) -> tuple[Control, ...]:
        """Controls in force on ``today``, or every stored control when None."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM controls ORDER BY kind, scope").fetchall()
        found = tuple(_to_control(row) for row in rows)
        return found if today is None else tuple(c for c in found if c.covers(today))

    # ---- holdings and the journal ---------------------------------------

    def save_position(self, position: ActualPosition) -> None:
        """Insert or update one holding. Zero units deletes the row.

        Deleting rather than storing a zero keeps "what do I hold" free of
        tombstones, and a position the user has exited is not information the
        digest should keep repeating.
        """
        with self._connect() as conn:
            if position.units == 0:
                conn.execute(
                    "DELETE FROM positions_actual WHERE instrument = ?", (position.symbol,)
                )
                return
            conn.execute(
                """
                INSERT INTO positions_actual (instrument, units, avg_cost, currency, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(instrument) DO UPDATE SET
                    units      = excluded.units,
                    avg_cost   = coalesce(excluded.avg_cost, positions_actual.avg_cost),
                    currency   = excluded.currency,
                    updated_at = excluded.updated_at
                """,
                (
                    position.symbol,
                    position.units,
                    position.avg_cost,
                    position.currency,
                    _iso(position.updated_at) or "",
                ),
            )

    def positions(self) -> tuple[ActualPosition, ...]:
        """Every recorded holding, by symbol."""
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM positions_actual ORDER BY instrument").fetchall()
        return tuple(
            ActualPosition(
                symbol=str(row[0]),
                units=float(str(row[1])),
                avg_cost=float(str(row[2])) if row[2] is not None else None,
                currency=str(row[3]),
                updated_at=datetime.fromisoformat(str(row[4])) if row[4] else None,
            )
            for row in rows
        )

    def record_journal(self, entry: JournalEntry) -> None:
        """Append one line to the decision journal."""
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO decision_journal (decision_id, action, reason, recorded_at) "
                "VALUES (?, ?, ?, ?)",
                (entry.decision_id, entry.action, entry.reason, _iso(entry.recorded_at) or ""),
            )

    def journal(self, limit: int = 20) -> tuple[JournalEntry, ...]:
        """The most recent journal entries, newest first."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT decision_id, action, reason, recorded_at FROM decision_journal "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return tuple(
            JournalEntry(
                decision_id=str(row[0]),
                action=str(row[1]),
                reason=str(row[2]),
                recorded_at=datetime.fromisoformat(str(row[3])) if row[3] else None,
            )
            for row in rows
        )

    # ---- command intake --------------------------------------------------

    def command_cursor(self, source: str) -> int | None:
        """The last update id applied from ``source``, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_update_id FROM command_cursor WHERE source = ?", (source,)
            ).fetchone()
        return int(str(row[0])) if row else None

    def save_command_cursor(self, source: str, update_id: int, *, now: datetime) -> None:
        """Advance the cursor after applying a batch."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO command_cursor (source, last_update_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(source) DO UPDATE SET
                    last_update_id = excluded.last_update_id,
                    updated_at     = excluded.updated_at
                """,
                (source, update_id, now.isoformat()),
            )

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


def _to_entry(row: tuple[object, ...], *, claimed_at: datetime | None = None) -> OutboxEntry:
    (
        entry_id,
        strategy_id,
        strategy_version,
        decision_id,
        scope,
        payload,
        created_at,
        claimed,
        sent_at,
        _provider_id,
        attempts,
        last_error,
    ) = row
    parsed: dict[str, Any] = json.loads(str(payload))
    return OutboxEntry(
        entry_id=int(str(entry_id)),
        strategy_id=str(strategy_id),
        strategy_version=str(strategy_version),
        decision_id=str(decision_id),
        scope=str(scope),
        payload=parsed,
        created_at=datetime.fromisoformat(str(created_at)),
        claimed_at=claimed_at
        if claimed_at is not None
        else (datetime.fromisoformat(str(claimed)) if claimed else None),
        sent_at=datetime.fromisoformat(str(sent_at)) if sent_at else None,
        attempts=int(str(attempts)) if attempts is not None else 0,
        last_error=str(last_error) if last_error else None,
    )


def _to_control(row: tuple[object, ...]) -> Control:
    kind, scope, until, set_at, reason = row
    return Control(
        kind=ControlKind(str(kind)),
        scope=str(scope),
        until=date.fromisoformat(str(until)) if until else None,
        set_at=datetime.fromisoformat(str(set_at)) if set_at else None,
        reason=str(reason),
    )
