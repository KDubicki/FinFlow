"""Drain the outbox — claim, send, mark.

Delivery is **per decision and atomic** (``PROJECT.md`` §9.4). One row carries a
whole target portfolio, so a crash cannot deliver half a rotation: either the
message went or it did not, and the next run finds the row exactly as it left it.

The guarantee this actually provides, stated honestly:

- **No decision is enqueued twice.** The decision id is a content address and
  the outbox is unique on ``(strategy_id, strategy_version, decision_id)``, so
  re-running the pipeline over unchanged data queues nothing.
- **A crash between claim and send loses nothing.** The claim expires with its
  lease and the next run picks the row up.
- **A crash between send and mark can duplicate one message.** Telegram exposes
  no idempotency key, so this window cannot be closed — only made small, by
  marking immediately after the call returns. It is written down here rather
  than discovered during an incident, and one repeated message is the correct
  side of that trade against one silently lost instruction.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta

from finflow.domain.calendars import sessions_behind
from finflow.domain.decision import Decision
from finflow.domain.messages import render_decision
from finflow.logging import get_logger
from finflow.ports.clock import Clock
from finflow.ports.notifier import Notifier, NotifierError
from finflow.ports.ops_store import OpsStore

log = get_logger(__name__)

DEFAULT_LEASE = timedelta(minutes=30)
"""Long enough that a slow send is not double-claimed, short enough that a run
killed mid-delivery is retried on the next timer rather than the next day."""

MAX_ATTEMPTS = 6
"""After this many failures a row stops being retried and is reported in the
digest instead. A poison message must not be able to block the queue behind it,
and it must not be retried forever in silence either."""


@dataclass
class DeliveryOutcome:
    """What the delivery step did."""

    sent: int = 0
    failed: int = 0
    abandoned: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def summary(self) -> str:
        """One line for a log or a digest."""
        return f"{self.sent} sent, {self.failed} failed, {len(self.abandoned)} abandoned"


class DeliverAlerts:
    """Send every claimable outbox row, once."""

    def __init__(
        self,
        *,
        ops_store: OpsStore,
        notifier: Notifier,
        clock: Clock,
        lease: timedelta = DEFAULT_LEASE,
        limit: int = 10,
        calendar: str = "XNYS",
    ) -> None:
        self._ops = ops_store
        self._notifier = notifier
        self._clock = clock
        self._lease = lease
        self._limit = limit
        self._calendar = calendar

    def run(self) -> DeliveryOutcome:
        """Claim pending decisions and deliver them."""
        outcome = DeliveryOutcome()
        claimed = self._ops.claim(now=self._clock.now(), lease=self._lease, limit=self._limit)

        for entry in claimed:
            if entry.attempts > MAX_ATTEMPTS:
                # Marked sent so it stops being claimed, but reported loudly:
                # the digest names it, so nothing disappears quietly.
                self._ops.mark_sent(entry.entry_id, now=self._clock.now())
                outcome.abandoned.append(f"{entry.decision_id} after {entry.attempts} attempts")
                log.error("alert_abandoned", decision=entry.decision_id, attempts=entry.attempts)
                continue

            decision = Decision.from_payload(entry.payload)
            today = self._clock.today()
            text = render_decision(
                decision,
                today=today,
                sessions_behind=(
                    sessions_behind(self._calendar, decision.data_as_of, today)
                    if decision.data_as_of
                    else 0
                ),
            )
            try:
                provider_id = self._notifier.send(text)
            except NotifierError as exc:
                self._ops.release(entry.entry_id, error=str(exc))
                outcome.failed += 1
                outcome.errors.append(f"{entry.decision_id}: {exc}")
                log.warning("alert_delivery_failed", decision=entry.decision_id, error=str(exc))
                continue

            self._ops.mark_sent(entry.entry_id, now=self._clock.now(), provider_id=provider_id)
            outcome.sent += 1
            log.info(
                "alert_delivered",
                decision=entry.decision_id,
                strategy=entry.strategy_id,
                version=entry.strategy_version,
                provider_id=provider_id,
            )

        return outcome
