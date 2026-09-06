"""Test doubles for the ports.

These live in ``tests`` rather than ``src`` deliberately: a fake that ships in
the package is a fake that eventually gets imported by production code.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime, timedelta

from finflow.ports.notifier import InboundMessage, NotifierError


class FrozenClock:
    """A clock that does not move unless a test moves it.

    Satisfies the ``Clock`` protocol structurally, which is the point of using
    ``Protocol`` rather than a base class: the fake owes the port nothing but a
    matching shape.
    """

    def __init__(self, at: datetime | date) -> None:
        if isinstance(at, datetime):
            self._at = at if at.tzinfo else at.replace(tzinfo=UTC)
        else:
            self._at = datetime(at.year, at.month, at.day, tzinfo=UTC)

    def now(self) -> datetime:
        """Return the instant this clock was frozen at."""
        return self._at

    def today(self) -> date:
        """Return the UTC date this clock was frozen at."""
        return self._at.date()

    def advance(self, **delta: float) -> None:
        """Move the clock forward, for tests that need two distinct instants."""
        self._at += timedelta(**delta)


class RecordingNotifier:
    """A notifier that remembers instead of sending.

    The recording fake the alert tests are written against: "was this delivered
    exactly once" is a question about a list, not about a chat.
    """

    def __init__(self, *, fail_times: int = 0, fail_with: str = "telegram is down") -> None:
        self.sent: list[str] = []
        self._fail_times = fail_times
        self._fail_with = fail_with

    def send(self, text: str) -> str:
        """Record the message, or fail the first ``fail_times`` calls."""
        if self._fail_times > 0:
            self._fail_times -= 1
            raise NotifierError(self._fail_with)
        self.sent.append(text)
        return f"msg-{len(self.sent)}"

    @property
    def last(self) -> str:
        """The most recent message, for a readability assertion."""
        return self.sent[-1] if self.sent else ""


class CrashingNotifier:
    """Sends, then dies before the caller can mark the row.

    Simulates the one window the outbox cannot close by itself: the process is
    killed between the provider accepting the message and the store recording
    that it did.
    """

    class Crash(BaseException):
        """Not an ``Exception``: nothing in the delivery path may catch it."""

    def __init__(self) -> None:
        self.sent: list[str] = []

    def send(self, text: str) -> str:
        """Record the send, then crash the process the way a SIGKILL would."""
        self.sent.append(text)
        raise self.Crash("killed mid-delivery")


class FakeInbox:
    """A command inbox holding whatever a test put in it."""

    def __init__(self, messages: Sequence[InboundMessage] = ()) -> None:
        self.messages = list(messages)
        self.drained_after: list[int | None] = []

    def drain(self, after: int | None = None) -> tuple[InboundMessage, ...]:
        """Return the messages newer than ``after``, recording the cursor used."""
        self.drained_after.append(after)
        return tuple(m for m in self.messages if after is None or m.update_id > after)


class RecordingHeartbeat:
    """Records which side of the dead-man's switch was pinged."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def start(self, run_id: str) -> None:
        """Record a start ping."""
        self.calls.append(("start", run_id))

    def success(self, run_id: str, detail: str = "") -> None:
        """Record a success ping."""
        self.calls.append(("success", run_id))

    def failure(self, run_id: str, detail: str = "") -> None:
        """Record a failure ping."""
        self.calls.append(("failure", run_id))

    @property
    def outcome(self) -> str | None:
        """Whichever of success/failure was pinged, if either was."""
        return next((kind for kind, _ in reversed(self.calls) if kind != "start"), None)
