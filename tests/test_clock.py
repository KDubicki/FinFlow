"""The clock port and its two implementations."""

from __future__ import annotations

from datetime import UTC, date, datetime

from finflow.adapters.clock import SystemClock
from finflow.ports.clock import Clock
from tests.fakes import FrozenClock


def test_system_clock_is_utc_aware() -> None:
    now = SystemClock().now()
    assert now.tzinfo is not None
    assert now.utcoffset() == UTC.utcoffset(None)


def test_system_clock_today_matches_now() -> None:
    clock = SystemClock()
    assert clock.today() == clock.now().date()


def test_both_implementations_satisfy_the_port() -> None:
    # runtime_checkable only verifies method presence, which is all a structural
    # port promises; mypy checks the signatures.
    assert isinstance(SystemClock(), Clock)
    assert isinstance(FrozenClock(date(2026, 8, 27)), Clock)


def test_frozen_clock_does_not_move() -> None:
    clock = FrozenClock(datetime(2019, 6, 3, 14, 30, tzinfo=UTC))
    assert clock.now() == clock.now()
    assert clock.today() == date(2019, 6, 3)


def test_frozen_clock_accepts_a_bare_date() -> None:
    assert FrozenClock(date(2019, 6, 3)).now() == datetime(2019, 6, 3, tzinfo=UTC)


def test_frozen_clock_advances_on_request() -> None:
    clock = FrozenClock(date(2019, 6, 3))
    clock.advance(days=2)
    assert clock.today() == date(2019, 6, 5)
