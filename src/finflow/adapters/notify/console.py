"""The no-credential pair.

A run with no bot token still has to be a *complete* run — it evaluates, it
records, it drains its outbox — because the alternative is a code path that only
ever executes in production and is therefore only ever debugged there. That is
what ``--dry-run`` and CI exercise.

Printing to stdout rather than swallowing: the message is the deliverable, and a
notifier that silently discarded it would make a dry run look successful while
proving nothing.
"""

from __future__ import annotations

import sys
from typing import TextIO

from finflow.ports.notifier import InboundMessage


class ConsoleNotifier:
    """Writes messages to a stream instead of a chat."""

    def __init__(self, stream: TextIO | None = None) -> None:
        self._stream = stream if stream is not None else sys.stdout
        self._counter = 0

    def send(self, text: str) -> str:
        """Print the message and return a synthetic id."""
        self._counter += 1
        self._stream.write(f"\n--- message {self._counter} ---\n{text}\n")
        self._stream.flush()
        return f"console-{self._counter}"


class SilentInbox:
    """A command inbox with nothing in it.

    Deliberately not an error: with no bot configured there are no commands, and
    the daily run's command step should be a no-op rather than a special case
    the entrypoint has to branch around.
    """

    def drain(self, after: int | None = None) -> tuple[InboundMessage, ...]:  # noqa: ARG002
        """Always empty. ``after`` is part of the port's shape, not a choice here."""
        return ()
