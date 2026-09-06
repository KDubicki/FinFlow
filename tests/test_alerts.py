"""The outbox, and the acceptance criterion it exists for.

> Killing the workflow mid-delivery and re-running sends no duplicates and no
> partial portfolios.

Both halves are asserted here. "No partial portfolios" is structural — a
decision is one row carrying one target portfolio, so there is no batch to be
half-way through. "No duplicates" is the claim that needs the crash tests.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.application.deliver_alerts import MAX_ATTEMPTS, DeliverAlerts, DeliveryOutcome
from finflow.domain.decision import Decision, TargetPosition
from finflow.ports.notifier import Notifier
from tests.fakes import CrashingNotifier, FrozenClock, RecordingNotifier

NOW = dt.datetime(2026, 9, 6, 5, 30, tzinfo=dt.UTC)
LEASE = dt.timedelta(minutes=30)


def decision(*, positions: tuple[str, ...] = ("GLD",), version: str = "v1") -> Decision:
    weight = 1.0 / len(positions) if positions else 0.0
    return Decision(
        strategy_id="sma_cross_precious_metals",
        strategy_version=version,
        as_of=dt.date(2026, 9, 5),
        data_as_of=dt.date(2026, 9, 4),
        positions=tuple(TargetPosition(symbol, weight) for symbol in positions),
        snapshot_id="snap-1",
    )


@pytest.fixture
def ops(tmp_path: Path) -> SqliteOpsStore:
    return SqliteOpsStore(tmp_path / "ops.sqlite")


class TestIdentity:
    def test_the_same_target_on_the_same_data_is_the_same_decision(self) -> None:
        assert decision().decision_id == decision().decision_id

    def test_a_different_target_is_a_different_decision(self) -> None:
        assert decision().decision_id != decision(positions=("GLD", "SLV")).decision_id

    def test_an_edited_strategy_does_not_collide_with_the_old_one(self) -> None:
        # PROJECT.md §9.4: without strategy_version in the key, an edited
        # strategy's new signal for the same bar is suppressed as a duplicate.
        # That is a silent miss, which is the worst failure this system has.
        assert decision().decision_id != decision(version="v2").decision_id

    def test_a_later_bar_produces_a_new_decision(self) -> None:
        # The mid-morning retry that finally gets the late vendor's data must
        # be delivered, not deduplicated against the stale morning attempt.
        stale = decision()
        fresh = Decision(
            strategy_id=stale.strategy_id,
            strategy_version=stale.strategy_version,
            as_of=stale.as_of,
            data_as_of=dt.date(2026, 9, 5),
            positions=stale.positions,
        )
        assert stale.decision_id != fresh.decision_id


class TestEnqueueing:
    def test_the_same_decision_is_enqueued_once(self, ops: SqliteOpsStore) -> None:
        assert ops.enqueue(decision(), now=NOW) is True
        assert ops.enqueue(decision(), now=NOW) is False
        assert len(ops.pending()) == 1

    def test_a_changed_decision_is_enqueued_alongside(self, ops: SqliteOpsStore) -> None:
        ops.enqueue(decision(), now=NOW)
        ops.enqueue(decision(positions=("GLD", "SLV")), now=NOW)
        assert len(ops.pending()) == 2

    def test_the_whole_portfolio_travels_in_one_row(self, ops: SqliteOpsStore) -> None:
        # Never one row per instrument: keying per instrument is exactly what
        # delivers half a rotation (PROJECT.md §9.4).
        ops.enqueue(decision(positions=("GLD", "IAU", "SLV")), now=NOW)
        (entry,) = ops.pending()
        assert len(entry.payload["positions"]) == 3


class TestDelivery:
    def _deliver(
        self, ops: SqliteOpsStore, notifier: Notifier, at: dt.datetime = NOW
    ) -> DeliveryOutcome:
        return DeliverAlerts(
            ops_store=ops, notifier=notifier, clock=FrozenClock(at), lease=LEASE
        ).run()

    def test_a_queued_decision_is_delivered_once(self, ops: SqliteOpsStore) -> None:
        ops.enqueue(decision(), now=NOW)
        notifier = RecordingNotifier()

        assert self._deliver(ops, notifier).sent == 1
        assert self._deliver(ops, notifier).sent == 0
        assert len(notifier.sent) == 1
        assert ops.pending() == ()

    def test_the_message_carries_as_of_version_and_snapshot(self, ops: SqliteOpsStore) -> None:
        # Daily-operations standard 3. A message that cannot say what it was
        # built from cannot be trusted at 06:00.
        ops.enqueue(decision(), now=NOW)
        notifier = RecordingNotifier()
        self._deliver(ops, notifier)
        assert "as_of 2026-09-05" in notifier.last
        assert "v1" in notifier.last
        assert "snap-1" in notifier.last

    def test_a_stale_decision_says_it_is_stale(self, ops: SqliteOpsStore) -> None:
        ops.enqueue(decision(), now=NOW)
        notifier = RecordingNotifier()
        self._deliver(ops, notifier, at=NOW + dt.timedelta(days=6))
        assert "days old" in notifier.last
        assert "Check the pipeline before acting" in notifier.last

    def test_a_crash_between_claim_and_send_delivers_on_the_next_run(
        self, ops: SqliteOpsStore
    ) -> None:
        # The row is claimed and the process dies. Nothing was sent, so the
        # lease must expire and the next run must pick it up — exactly once.
        ops.enqueue(decision(), now=NOW)
        claimed = ops.claim(now=NOW, lease=LEASE)
        assert len(claimed) == 1

        # Same minute: still leased, so a concurrent run sees nothing.
        notifier = RecordingNotifier()
        assert self._deliver(ops, notifier, at=NOW + dt.timedelta(minutes=1)).sent == 0
        assert notifier.sent == []

        # After the lease: delivered, once.
        assert self._deliver(ops, notifier, at=NOW + dt.timedelta(hours=2)).sent == 1
        assert len(notifier.sent) == 1

    def test_a_crash_mid_delivery_and_a_rerun_send_no_duplicate_portfolio(
        self, ops: SqliteOpsStore
    ) -> None:
        # The acceptance criterion, run as the pipeline runs it: evaluate,
        # enqueue, die during delivery, then re-run the whole thing. The second
        # run re-enqueues nothing because the decision id is a content address,
        # so the outbox still holds exactly one row for that instruction.
        ops.enqueue(decision(positions=("GLD", "IAU")), now=NOW)
        crashing = CrashingNotifier()
        with pytest.raises(CrashingNotifier.Crash):
            self._deliver(ops, crashing)

        assert ops.enqueue(decision(positions=("GLD", "IAU")), now=NOW) is False
        assert len(ops.pending()) == 1

        notifier = RecordingNotifier()
        assert self._deliver(ops, notifier, at=NOW + dt.timedelta(hours=2)).sent == 1
        assert len(notifier.sent) == 1
        assert "GLD" in notifier.last and "IAU" in notifier.last

    def test_a_failed_send_is_retried_rather_than_lost(self, ops: SqliteOpsStore) -> None:
        ops.enqueue(decision(), now=NOW)
        notifier = RecordingNotifier(fail_times=1)

        first = self._deliver(ops, notifier)
        assert first.failed == 1
        assert len(ops.pending()) == 1

        # Released rather than left claimed, so the retry does not wait out the
        # lease: a transient Telegram error should cost seconds, not an hour.
        second = self._deliver(ops, notifier, at=NOW + dt.timedelta(minutes=1))
        assert second.sent == 1

    def test_a_poison_message_is_abandoned_loudly_rather_than_retried_forever(
        self, ops: SqliteOpsStore
    ) -> None:
        ops.enqueue(decision(), now=NOW)
        notifier = RecordingNotifier(fail_times=MAX_ATTEMPTS + 5)

        at = NOW
        for _ in range(MAX_ATTEMPTS + 1):
            at += dt.timedelta(minutes=1)
            outcome = self._deliver(ops, notifier, at=at)

        assert outcome.abandoned
        assert ops.pending() == ()
        assert notifier.sent == []
