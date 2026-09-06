"""Versioned migrations for the operational store.

The analytical store is disposable, so its schema can be recreated at will. This
one is authoritative and cannot be dropped (``PROJECT.md`` §11.4), so schema
changes are versioned and applied on start — adding a column to a running system
has to be a migration rather than an edit to a ``CREATE TABLE`` that only new
installations would ever see.

Migrations are append-only and never edited once shipped. Editing one means
existing databases skip the change silently, which is the failure this exists to
prevent.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence

MIGRATIONS: Sequence[tuple[int, str, str]] = (
    (
        1,
        "watermarks",
        """
        CREATE TABLE IF NOT EXISTS watermarks (
            source           TEXT NOT NULL,
            symbol           TEXT NOT NULL,
            last_loaded_date TEXT,
            last_run_at      TEXT,
            row_count        INTEGER NOT NULL DEFAULT 0,
            deferred_until   TEXT,
            PRIMARY KEY (source, symbol)
        );
        """,
    ),
    (
        2,
        "pipeline_runs",
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id       TEXT PRIMARY KEY,
            started_at   TEXT NOT NULL,
            ended_at     TEXT,
            status       TEXT NOT NULL,
            rows_written INTEGER NOT NULL DEFAULT 0,
            snapshot_id  TEXT,
            manifest_ref TEXT,
            error        TEXT
        );
        CREATE INDEX IF NOT EXISTS pipeline_runs_started
            ON pipeline_runs (started_at DESC);
        """,
    ),
    (
        3,
        "decisions_and_delivery",
        """
        -- One decision per strategy per evaluation (PROJECT.md §9.4). The
        -- payload is the whole target portfolio, so a decision is readable
        -- long after the warehouse it was computed from has been rebuilt.
        CREATE TABLE IF NOT EXISTS decisions (
            decision_id      TEXT PRIMARY KEY,
            strategy_id      TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            scope            TEXT NOT NULL,
            as_of            TEXT NOT NULL,
            data_as_of       TEXT,
            snapshot_id      TEXT,
            run_id           TEXT,
            payload          TEXT NOT NULL,
            created_at       TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS decisions_as_of ON decisions (as_of DESC);

        -- The netted target, exploded one row per instrument, so "what should
        -- I hold" is a query rather than a JSON parse.
        CREATE TABLE IF NOT EXISTS positions_target (
            decision_id  TEXT NOT NULL,
            instrument   TEXT NOT NULL,
            weight       REAL NOT NULL,
            units        REAL,
            generated_at TEXT NOT NULL,
            PRIMARY KEY (decision_id, instrument)
        );

        -- Delivery is per decision and atomic. The unique key carries
        -- strategy_version because an edited strategy's new signal for the
        -- same bar would otherwise collide with the old key and be suppressed
        -- as a duplicate -- a silent miss (PROJECT.md §9.4).
        CREATE TABLE IF NOT EXISTS alerts_outbox (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            strategy_id      TEXT NOT NULL,
            strategy_version TEXT NOT NULL,
            decision_id      TEXT NOT NULL,
            scope            TEXT NOT NULL,
            payload          TEXT NOT NULL,
            created_at       TEXT NOT NULL,
            claimed_at       TEXT,
            sent_at          TEXT,
            provider_id      TEXT,
            attempts         INTEGER NOT NULL DEFAULT 0,
            last_error       TEXT,
            UNIQUE (strategy_id, strategy_version, decision_id)
        );
        CREATE INDEX IF NOT EXISTS alerts_outbox_pending
            ON alerts_outbox (sent_at, claimed_at);

        -- What the user actually holds. Nothing automated writes this.
        CREATE TABLE IF NOT EXISTS positions_actual (
            instrument TEXT PRIMARY KEY,
            units      REAL NOT NULL,
            avg_cost   REAL,
            currency   TEXT NOT NULL DEFAULT 'USD',
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS controls (
            kind   TEXT NOT NULL,
            scope  TEXT NOT NULL,
            until  TEXT,
            set_at TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (kind, scope)
        );

        CREATE TABLE IF NOT EXISTS decision_journal (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_id  TEXT NOT NULL,
            action       TEXT NOT NULL,
            reason       TEXT NOT NULL DEFAULT '',
            recorded_at  TEXT NOT NULL
        );

        -- Where command intake got to. The run is a timer, not a daemon
        -- (PROJECT.md §4.5), so the cursor has to outlive the process.
        CREATE TABLE IF NOT EXISTS command_cursor (
            source        TEXT PRIMARY KEY,
            last_update_id INTEGER NOT NULL,
            updated_at    TEXT NOT NULL
        );
        """,
    ),
)

_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def applied_versions(conn: sqlite3.Connection) -> set[int]:
    """Versions already present in this database."""
    conn.executescript(_VERSION_TABLE)
    return {int(row[0]) for row in conn.execute("SELECT version FROM schema_migrations")}


def migrate(conn: sqlite3.Connection) -> list[int]:
    """Apply every outstanding migration in order. Returns what was applied.

    Idempotent: running it against an up-to-date database applies nothing and
    returns an empty list, which is what makes it safe to call on every start.
    """
    done = applied_versions(conn)
    applied: list[int] = []
    for version, name, sql in MIGRATIONS:
        if version in done:
            continue
        conn.executescript(sql)
        conn.execute("INSERT INTO schema_migrations (version, name) VALUES (?, ?)", (version, name))
        applied.append(version)
    # Committed explicitly. sqlite3's default isolation opens an implicit
    # transaction, so without this the inserts roll back on close and every
    # start re-applies every migration -- silently, and forever.
    conn.commit()
    return applied


def current_version(conn: sqlite3.Connection) -> int:
    """The highest applied version, or 0 for an empty database."""
    versions = applied_versions(conn)
    return max(versions) if versions else 0
