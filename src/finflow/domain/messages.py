"""What the user actually reads.

Two messages exist, and the whole product lives or dies on the first one:

- **The daily digest**, sent at a fixed time whether or not anything happened.
  It has to be readable in ten seconds on a phone and say "no action" on a quiet
  day. If it takes longer or manufactures activity, that is a bug of the first
  order — an unread digest is a system with no users (``PROJECT.md`` §1.2).
- **A decision message**, when a rule produces a target portfolio to act on.

Rendering is pure and lives in the domain so that "is this readable?" is a test
over a string rather than a subscription to the bot. Plain text, no markup: a
symbol containing an underscore is enough to make Telegram reject a Markdown
message, and a delivery that fails on formatting is an alert that never arrives.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date

from finflow.domain.decision import Decision
from finflow.domain.drift import DriftLine

OK = "ok"
FAILED = "failed"
SKIPPED = "skipped"
DEGRADED = "degraded"

_MARK = {OK: "ok", FAILED: "FAILED", SKIPPED: "skipped", DEGRADED: "degraded"}

MAX_WITHHELD = 6
"""How many withheld lines the digest prints before summarising the rest.

Six fits on a phone screen alongside everything else. The count of what is not
shown is always printed, so the message is bounded without ever implying the
list is complete."""


@dataclass(frozen=True, slots=True)
class Step:
    """One stage of the daily run, and how it went."""

    name: str
    status: str = OK
    detail: str = ""

    def describe(self) -> str:
        """``ingest   ok  412 rows``."""
        return f"{self.name:<9}{_MARK.get(self.status, self.status):<9}{self.detail}".rstrip()


@dataclass(frozen=True, slots=True)
class Digest:
    """Everything the daily message reports.

    Assembled by the application layer from the ops store and the warehouse;
    rendered here. The split is what lets a test assert the wording of a stale,
    half-failed run without arranging for one to happen.
    """

    as_of: date
    run_id: str
    steps: tuple[Step, ...] = ()
    bars_ingested: int = 0
    partitions_written: int = 0
    freshest: tuple[tuple[str, date | None], ...] = ()
    """``(universe, freshest bar date)`` — per universe, because a stale
    commodity feed and a stale rates feed are different incidents."""

    checks_passed: int = 0
    checks_failed: int = 0
    restatements: int = 0
    strategies_evaluated: int = 0
    decisions_delivered: int = 0
    drift: tuple[DriftLine, ...] = ()
    withheld: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()
    sessions_behind: int = 0
    """Completed trading sessions with no data. Sessions rather than calendar
    days because a Friday bar read on a Sunday is fresh, and a digest that says
    "degraded" every weekend teaches its reader to ignore the word."""
    snapshot_id: str | None = None
    disk_used_pct: float | None = None
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def status(self) -> str:
        """The one word at the top of the message."""
        if any(step.status == FAILED for step in self.steps) or self.failures:
            return FAILED
        if self.checks_failed or self.sessions_behind:
            return DEGRADED
        return OK

    @property
    def actions(self) -> tuple[DriftLine, ...]:
        """The drift lines actually asking for a trade."""
        return tuple(line for line in self.drift if line.needs_action)


def render_digest(digest: Digest) -> str:
    """Render the daily digest.

    Ordered by what a reader needs first: whether to do anything, then whether
    to trust it, then the numbers. The order is deliberate — a digest that
    opens with row counts trains the reader to skip to the end, and then to
    skip it entirely.
    """
    status = digest.status
    header = f"FinFlow {digest.as_of} · {status}"
    lines = [header, ""]

    if status == FAILED:
        lines.append("The run did not complete. No instruction is issued today.")
    elif not digest.actions:
        lines.append("No action.")
    else:
        lines.extend(line.describe() for line in digest.actions)
    lines.append("")

    if digest.sessions_behind:
        sessions = "session" if digest.sessions_behind == 1 else "sessions"
        lines.append(
            f"! Data is {digest.sessions_behind} trading {sessions} behind — "
            f"treat any target as stale."
        )
    for failure in digest.failures:
        lines.append(f"! {failure}")
    if digest.failures or digest.sessions_behind:
        lines.append("")

    dates = [day for _, day in digest.freshest if day is not None]
    newest = max(dates) if dates else None
    lines.append(
        f"Data      {digest.bars_ingested} rows · fresh to {newest.isoformat()}"
        if newest
        else f"Data      {digest.bars_ingested} rows · no bars"
    )
    # Only the universes that are *behind* are named. Listing all of them puts a
    # 95-character line on a phone screen and buries the one that is stale.
    behind = [
        f"{universe} {day.isoformat() if day else 'never'}"
        for universe, day in digest.freshest
        if day != newest
    ]
    lines.extend(_wrap("Behind", behind))
    lines.append(
        f"Checks    {digest.checks_passed} passed · {digest.checks_failed} failed "
        f"· {digest.restatements} restatements"
    )
    lines.append(
        f"Rules     {digest.strategies_evaluated} evaluated · "
        f"{digest.decisions_delivered} delivered"
    )

    holdings = [line for line in digest.drift if line.actual_units]
    if holdings:
        lines.append("")
        lines.append("Held")
        lines.extend(
            f"  {line.symbol} {line.actual_units:g} · {line.actual_weight:.0%} "
            f"(target {line.target_weight:.0%})"
            for line in holdings
        )
    elif digest.drift:
        lines.append("")
        lines.append("Held      nothing recorded — /position GLD 12 to tell it what you own")

    if digest.withheld:
        # Never silent: a control the user forgot they set is indistinguishable
        # from a bug unless the digest lists it (PROJECT.md §7.7). Capped, not
        # truncated -- a forty-instrument universe can withhold a dozen names on
        # a quiet day, and a message nobody finishes reading suppresses just as
        # effectively as one that never mentioned them.
        lines.append("")
        lines.append("Withheld")
        lines.extend(f"  {item}" for item in digest.withheld[:MAX_WITHHELD])
        remaining = len(digest.withheld) - MAX_WITHHELD
        if remaining > 0:
            lines.append(f"  ... and {remaining} more — /status, or the run log")

    if digest.notes:
        lines.append("")
        lines.extend(digest.notes)

    if digest.disk_used_pct is not None and digest.disk_used_pct >= 75:
        lines.append("")
        lines.append(f"! Disk {digest.disk_used_pct:.0f}% full on the box.")

    lines.append("")
    lines.append(f"run {digest.run_id} · snapshot {digest.snapshot_id or 'none'}")
    return "\n".join(lines)


def _wrap(label: str, items: Sequence[str], width: int = 68) -> list[str]:
    """Lay a list out under a label, wrapped so no line runs off a phone screen.

    Continuation lines are indented under the label rather than repeating it,
    which is what keeps the digest scannable as a column of headings.
    """
    if not items:
        return []
    lines: list[str] = []
    current = f"{label:<10}"
    for item in items:
        if len(current) + len(item) + 3 > width and current.strip():
            lines.append(current.rstrip())
            current = " " * 10
        current += f"{item} · "
    lines.append(current.rstrip().rstrip("·").rstrip())
    return lines


def render_decision(decision: Decision, *, today: date, sessions_behind: int = 0) -> str:
    """Render one decision as an instruction.

    Every message carries ``as_of``, ``strategy_version`` and ``snapshot_id``
    (daily-operations standard 3), so a stale pipeline produces a *visibly*
    stale alert rather than a confident one (``PROJECT.md`` §7.3).
    """
    lines = [f"{decision.strategy_id} · {decision.as_of}", ""]

    if decision.is_flat:
        lines.append("Target: flat — hold no position from this strategy.")
    else:
        lines.append("Target portfolio:")
        for position in sorted(decision.positions, key=lambda p: (-p.weight, p.symbol)):
            line = f"  {position.symbol}  {position.weight:.0%}"
            if position.buy_instead:
                # Not a footnote: under PRIIPs the named fund cannot be bought
                # from an EU account, so an instruction without this is one the
                # reader cannot act on (PROJECT.md §5.7).
                line += f"  -> buy {position.buy_instead}"
            lines.append(line)

    if sessions_behind:
        lines.append("")
        lines.append(
            f"! Built from data as of {decision.data_as_of} — "
            f"{sessions_behind} trading session(s) behind, "
            f"{decision.staleness_days(today)} days old. "
            f"Check the pipeline before acting."
        )

    if decision.withheld:
        lines.append("")
        lines.append("Withheld:")
        lines.extend(f"  {item.describe()}" for item in decision.withheld)

    lines.append("")
    lines.append(
        f"as_of {decision.as_of} · v{decision.strategy_version} · "
        f"snapshot {decision.snapshot_id or 'none'} · {decision.decision_id}"
    )
    return "\n".join(lines)
