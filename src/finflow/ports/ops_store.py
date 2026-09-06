"""The operational-store seam.

Small, transactional and **authoritative**: nothing here is derivable from the
raw zone, which is exactly the criterion that put it in a different store from
the warehouse (``PROJECT.md`` §4.3).

It holds watermarks, ``pipeline_runs``, the decisions the evaluator made, the
alert outbox that delivers them exactly once, and the controls and holdings the
user maintains by hand (``PROJECT.md`` §9.3). None of it can be recomputed from
the raw zone, which is why losing it is the one incident a rebuild cannot fix.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from finflow.contracts.sources import SourceKey
from finflow.domain.decision import Decision


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


class ControlKind(StrEnum):
    """The four ways a user overrules the system (``PROJECT.md`` §7.7)."""

    PAUSE = "pause"
    """A strategy stops issuing instructions but keeps computing and recording,
    so "what would it have done" is still answerable when the pause ends."""

    MUTE = "mute"
    """One instrument is excluded from targets until a date."""

    HOLD = "hold"
    """All rebalancing is suspended for a stated period."""


HOLD_SCOPE = "*"
"""The scope a ``hold`` is recorded against. A literal rather than NULL, so the
primary key ``(kind, scope)`` keeps working."""


@dataclass(frozen=True)
class Control:
    """One active override."""

    kind: ControlKind
    scope: str
    """The strategy id for a pause, the symbol for a mute, ``*`` for a hold."""

    until: date | None = None
    set_at: datetime | None = None
    reason: str = ""

    def covers(self, today: date) -> bool:
        """True when this control is still in force.

        A pause has no end date on purpose — it is a decision, not a holiday —
        while a mute and a hold must state one, because an override that
        quietly becomes permanent is the failure §7.7 exists to prevent.
        """
        return self.until is None or today < self.until

    def describe(self) -> str:
        """One line for the digest's withheld list."""
        window = f" until {self.until}" if self.until else ""
        because = f" ({self.reason})" if self.reason else ""
        return f"{self.kind} {self.scope}{window}{because}"


@dataclass(frozen=True)
class ActualPosition:
    """What the user says they actually hold (``PROJECT.md`` §7.6).

    Nothing automated ever writes this. It is not broker integration; it is the
    user telling the system what they own, which is what turns "GLD triggered"
    into "here is the difference between what you hold and what you should".
    """

    symbol: str
    units: float
    avg_cost: float | None = None
    currency: str = "USD"
    updated_at: datetime | None = None


@dataclass(frozen=True)
class OutboxEntry:
    """One decision waiting to be delivered, or already delivered.

    The payload is the whole decision. Delivery must not have to re-query
    anything: a row may be sent by a later run than the one that wrote it,
    against a warehouse that has been rebuilt in between.
    """

    entry_id: int
    strategy_id: str
    strategy_version: str
    decision_id: str
    scope: str
    payload: dict[str, Any]
    created_at: datetime
    claimed_at: datetime | None = None
    sent_at: datetime | None = None
    attempts: int = 0
    last_error: str | None = None

    @property
    def is_sent(self) -> bool:
        """True once the provider has acknowledged it."""
        return self.sent_at is not None


@dataclass(frozen=True)
class JournalEntry:
    """What the user did about a decision, and why (``PROJECT.md`` §7.7).

    Written when the override is made, while the reasoning is still available.
    This costs a sentence a month and turns "I usually get these calls right"
    from a belief into a record.
    """

    decision_id: str
    action: str
    """``followed``, ``overridden`` or ``ignored``."""

    reason: str = ""
    recorded_at: datetime | None = None


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

    # ---- decisions and delivery -----------------------------------------

    def save_decision(self, decision: Decision, *, run_id: str, now: datetime) -> None:
        """Record one decision and its target portfolio.

        Written whether or not it is delivered: a paused strategy keeps
        computing and recording, which is the counterfactual §7.7 depends on.
        """
        ...

    def decisions(self, *, strategy_id: str | None = None, limit: int = 20) -> tuple[Decision, ...]:
        """The most recent decisions, newest first."""
        ...

    def enqueue(self, decision: Decision, *, now: datetime) -> bool:
        """Add one decision to the outbox. Returns False when it is already there.

        Keyed on ``(strategy_id, strategy_version, decision_id)`` and inserted
        with an ignore-on-conflict, so re-running the pipeline over unchanged
        data enqueues nothing rather than sending a second copy.
        """
        ...

    def claim(self, *, now: datetime, lease: timedelta, limit: int = 10) -> tuple[OutboxEntry, ...]:
        """Take ownership of pending rows for the length of the lease.

        A row claimed by a run that then died becomes claimable again once the
        lease expires, which is what stops one crash silencing an alert forever.
        """
        ...

    def mark_sent(self, entry_id: int, *, now: datetime, provider_id: str | None = None) -> None:
        """Record that a claimed row was delivered."""
        ...

    def release(self, entry_id: int, *, error: str) -> None:
        """Return a claimed row to the queue after a failed send."""
        ...

    def last_enqueued(self, strategy_id: str) -> OutboxEntry | None:
        """The newest outbox row for one strategy, sent or not.

        What "the last thing we told the user" means. Read before enqueuing so
        an unchanged target produces no second message — the single most
        important guard on the digest staying worth reading.
        """
        ...

    def pending(self) -> tuple[OutboxEntry, ...]:
        """Every undelivered row, oldest first."""
        ...

    def sent(self, *, since: datetime | None = None) -> tuple[OutboxEntry, ...]:
        """Delivered rows, for the digest's "what did we say today" line."""
        ...

    # ---- controls, holdings and the journal ------------------------------

    def set_control(self, control: Control) -> None:
        """Insert or replace one control."""
        ...

    def clear_control(self, kind: ControlKind, scope: str) -> bool:
        """Remove one control. Returns False when there was nothing to remove."""
        ...

    def controls(self, *, today: date | None = None) -> tuple[Control, ...]:
        """Controls in force on ``today``, or every stored control when None.

        Expired ones are filtered rather than deleted: an override that
        happened is part of the record even after it lapses.
        """
        ...

    def save_position(self, position: ActualPosition) -> None:
        """Insert or update one actual holding. A zero closes the position."""
        ...

    def positions(self) -> tuple[ActualPosition, ...]:
        """Every recorded holding, by symbol."""
        ...

    def record_journal(self, entry: JournalEntry) -> None:
        """Append one line to the decision journal."""
        ...

    def journal(self, limit: int = 20) -> tuple[JournalEntry, ...]:
        """The most recent journal entries, newest first."""
        ...

    # ---- command intake --------------------------------------------------

    def command_cursor(self, source: str) -> int | None:
        """The last update id applied from ``source``, or None.

        Persisted rather than held in memory because the process exits between
        runs: without it, every run would re-apply every command it could still
        see, and ``/position GLD 0`` would be executed forever.
        """
        ...

    def save_command_cursor(self, source: str, update_id: int, *, now: datetime) -> None:
        """Advance the cursor after applying a batch."""
        ...
