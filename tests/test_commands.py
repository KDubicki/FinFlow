"""The controls the user overrules the system with.

Parsing is pure, so most of this is a table of strings and expectations. The
part that is not — draining, applying and acknowledging — is tested against the
one failure that matters operationally: a command must be applied **once**, even
though the process that applied it exits before the user's next message.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import pytest

from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.application.apply_commands import SOURCE, ApplyCommands
from finflow.domain.commands import CommandError, CommandKind, parse
from finflow.ports.notifier import InboundMessage
from finflow.ports.ops_store import ControlKind
from finflow.registry import load_registry
from finflow.registry.models import Registry
from tests.fakes import FakeInbox, FrozenClock, RecordingNotifier

TODAY = dt.date(2026, 9, 6)
NOW = dt.datetime(2026, 9, 6, 5, 30, tzinfo=dt.UTC)
REGISTRY_DIR = Path(__file__).resolve().parents[1] / "instruments"


@pytest.fixture
def registry() -> Registry:
    return load_registry(REGISTRY_DIR)


class TestParsing:
    def test_a_position_carries_units_and_optional_cost(self) -> None:
        command = parse("/position gld 12.5 180.20", today=TODAY)
        assert command.kind is CommandKind.POSITION
        assert (command.symbol, command.units, command.avg_cost) == ("GLD", 12.5, 180.20)

    def test_a_mute_accepts_a_relative_window(self) -> None:
        assert parse("/mute GLD 14d", today=TODAY).until == dt.date(2026, 9, 20)
        assert parse("/mute GLD 2w", today=TODAY).until == dt.date(2026, 9, 20)

    def test_a_mute_accepts_an_absolute_date(self) -> None:
        assert parse("/mute GLD 2026-12-01", today=TODAY).until == dt.date(2026, 12, 1)

    def test_a_mute_with_no_end_date_cannot_be_expressed(self) -> None:
        # An override that quietly becomes permanent is the failure §7.7 exists
        # to prevent, so the grammar has no way to write one.
        with pytest.raises(CommandError, match="usage"):
            parse("/mute GLD", today=TODAY)

    def test_a_date_in_the_past_is_refused(self) -> None:
        with pytest.raises(CommandError, match="not in the future"):
            parse("/mute GLD 2026-01-01", today=TODAY)

    def test_the_bot_suffix_telegram_adds_in_groups_is_ignored(self) -> None:
        assert parse("/status@finflow_bot", today=TODAY).kind is CommandKind.STATUS

    def test_a_short_position_is_refused(self) -> None:
        with pytest.raises(CommandError, match="long-only"):
            parse("/position GLD -5", today=TODAY)

    def test_an_unknown_command_replies_with_the_list(self) -> None:
        with pytest.raises(CommandError, match="unknown command"):
            parse("/rebalance now", today=TODAY)

    def test_a_non_command_is_not_parsed_at_all(self) -> None:
        with pytest.raises(CommandError, match="not a command"):
            parse("good morning", today=TODAY)


class TestApplying:
    def _apply(
        self, ops: SqliteOpsStore, registry: Registry, *texts: str, start: int = 100
    ) -> tuple[object, RecordingNotifier, FakeInbox]:
        inbox = FakeInbox(
            [
                InboundMessage(update_id=start + i, chat_id="1", text=text)
                for i, text in enumerate(texts)
            ]
        )
        notifier = RecordingNotifier()
        outcome = ApplyCommands(
            ops_store=ops,
            inbox=inbox,
            notifier=notifier,
            clock=FrozenClock(NOW),
            registry=registry,
            known_strategies=("sma_cross_precious_metals",),
        ).run()
        return outcome, notifier, inbox

    @pytest.fixture
    def ops(self, tmp_path: Path) -> SqliteOpsStore:
        return SqliteOpsStore(tmp_path / "ops.sqlite")

    def test_a_position_is_recorded(self, ops: SqliteOpsStore, registry: Registry) -> None:
        self._apply(ops, registry, "/position GLD 12")
        (position,) = ops.positions()
        assert (position.symbol, position.units) == ("GLD", 12.0)

    def test_zero_units_closes_a_position(self, ops: SqliteOpsStore, registry: Registry) -> None:
        self._apply(ops, registry, "/position GLD 12")
        self._apply(ops, registry, "/position GLD 0", start=200)
        assert ops.positions() == ()

    def test_a_pause_and_a_resume_round_trip(self, ops: SqliteOpsStore, registry: Registry) -> None:
        self._apply(ops, registry, "/pause sma_cross_precious_metals")
        assert [c.kind for c in ops.controls()] == [ControlKind.PAUSE]
        self._apply(ops, registry, "/resume sma_cross_precious_metals", start=200)
        assert ops.controls() == ()

    def test_an_unknown_symbol_is_rejected_with_the_registry_listed(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        outcome, notifier, _ = self._apply(ops, registry, "/position XYZ 3")
        assert outcome.rejected  # type: ignore[attr-defined]
        assert ops.positions() == ()
        assert "registered:" in notifier.last

    def test_an_unknown_strategy_is_rejected(self, ops: SqliteOpsStore, registry: Registry) -> None:
        outcome, _, _ = self._apply(ops, registry, "/pause not_a_strategy")
        assert outcome.rejected  # type: ignore[attr-defined]

    def test_chatter_is_ignored_without_a_reply(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # The digest lands in this chat too. Replying to every stray message
        # makes the channel unreadable, which is the failure that ends the
        # product.
        outcome, notifier, _ = self._apply(ops, registry, "morning")
        assert notifier.sent == []
        assert outcome.applied == []  # type: ignore[attr-defined]

    def test_a_batch_gets_one_reply_not_one_per_command(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        _, notifier, _ = self._apply(ops, registry, "/position GLD 12", "/position SLV 4")
        assert len(notifier.sent) == 1
        assert notifier.last.count("✓") == 2

    def test_the_reply_says_when_the_command_took_effect(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        _, notifier, _ = self._apply(ops, registry, "/position GLD 12")
        assert "next one" in notifier.last

    def test_a_command_is_applied_once_across_runs(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # The cursor is the only thing standing between "the user set a
        # position" and "the pipeline re-applies it every morning forever".
        self._apply(ops, registry, "/position GLD 12")
        assert ops.command_cursor(SOURCE) == 100

        _, notifier, inbox = self._apply(ops, registry, "/position GLD 12")
        assert inbox.drained_after == [100]
        assert notifier.sent == []

    def test_status_answers_without_changing_anything(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        self._apply(ops, registry, "/position GLD 12")
        _, notifier, _ = self._apply(ops, registry, "/status", start=200)
        assert "GLD 12" in notifier.last
        assert "queued alerts: 0" in notifier.last

    def test_help_lists_every_control(self, ops: SqliteOpsStore, registry: Registry) -> None:
        _, notifier, _ = self._apply(ops, registry, "/help")
        for verb in ("/position", "/pause", "/mute", "/hold"):
            assert verb in notifier.last

    def test_a_failed_reply_does_not_undo_the_command(
        self, ops: SqliteOpsStore, registry: Registry
    ) -> None:
        # The command is already applied and the cursor already moved. A
        # Telegram outage must not cause tomorrow to apply everything twice.
        inbox = FakeInbox([InboundMessage(update_id=7, chat_id="1", text="/position GLD 9")])
        ApplyCommands(
            ops_store=ops,
            inbox=inbox,
            notifier=RecordingNotifier(fail_times=5),
            clock=FrozenClock(NOW),
            registry=registry,
        ).run()
        assert ops.positions()[0].units == 9.0
        assert ops.command_cursor(SOURCE) == 7
