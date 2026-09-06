"""The outbound-message and inbound-command seams.

Two protocols rather than one because they have different consumers and
different failure meanings: a delivery that fails must leave the outbox row
claimable again, while a command drain that fails must leave the cursor where it
was so nothing is lost. Fusing them would make one class responsible for both,
and the day the bot is replaced by email the split is what makes only half of it
change.

Both earn their place at their second implementation (``PROJECT.md`` §4.6):
Telegram and a console adapter for a run with no token, plus recording fakes in
the tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable


class NotifierError(RuntimeError):
    """A message could not be delivered.

    Deliberately one class: the caller's only decision is whether to leave the
    outbox row for the next run, and no distinction between a 500 and a timeout
    changes that answer.
    """


@runtime_checkable
class Notifier(Protocol):
    """Sends a message to whoever is running this system.

    Contract:

    - ``send`` returns a provider message id, or raises ``NotifierError``.
    - ``send`` is **not** idempotent and makes no attempt to be. Deduplication
      is the outbox's job (``PROJECT.md`` §9.4), because the provider offers no
      key we could deduplicate on.
    """

    def send(self, text: str) -> str:
        """Deliver one message, returning the provider's id for it."""
        ...


@dataclass(frozen=True, slots=True)
class InboundMessage:
    """One message the user sent to the bot."""

    update_id: int
    """Monotonic per-chat cursor. Draining acknowledges up to this id, which is
    what stops a command being applied twice on consecutive runs."""

    chat_id: str
    text: str
    sent_at: datetime | None = None


@runtime_checkable
class CommandInbox(Protocol):
    """Reads the commands waiting for the run that is starting.

    Contract:

    - ``drain`` returns messages with ``update_id`` greater than ``after``, in
      order, and performs **one** call. M4 is a timer firing a CLI run, not a
      daemon (``PROJECT.md`` §4.5), so there is no long poll and no process to
      hold one open.
    - ``drain`` never blocks for longer than the transport timeout, because it
      runs on the critical path of the daily pipeline.
    """

    def drain(self, after: int | None = None) -> tuple[InboundMessage, ...]:
        """Fetch pending messages newer than ``after``."""
        ...
