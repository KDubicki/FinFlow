"""Command intake without a long-lived process.

M4 is a timer firing a CLI run, not a daemon (``PROJECT.md`` §4.5), so nothing
is polling Telegram for ``/position`` or ``/pause``. The run drains pending
updates with a single call before it evaluates, applies what it finds, and says
in its reply when each was applied. Commands therefore take effect at the start
of the next scheduled run rather than immediately — which the bot states, rather
than leaving the user to work out from an alert that did not change.

The cursor is persisted, not held in memory: the process exits between runs, so
without it every run would re-apply every command it could still see.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from finflow.domain.commands import Command, CommandError, CommandKind, help_text, is_command, parse
from finflow.logging import get_logger
from finflow.ports.clock import Clock
from finflow.ports.notifier import CommandInbox, Notifier, NotifierError
from finflow.ports.ops_store import (
    HOLD_SCOPE,
    ActualPosition,
    Control,
    ControlKind,
    OpsStore,
)
from finflow.registry.errors import RegistryValidationError
from finflow.registry.models import Registry

log = get_logger(__name__)

SOURCE = "telegram"


@dataclass
class CommandOutcome:
    """What the intake step did, for the digest."""

    applied: list[str] = field(default_factory=list)
    rejected: list[str] = field(default_factory=list)
    seen: int = 0

    def summary(self) -> str:
        """One line for a log or a digest."""
        return f"{len(self.applied)} applied, {len(self.rejected)} rejected of {self.seen} seen"


class ApplyCommands:
    """Read the controls waiting for this run, apply them, and reply once."""

    def __init__(
        self,
        *,
        ops_store: OpsStore,
        inbox: CommandInbox,
        notifier: Notifier,
        clock: Clock,
        registry: Registry,
        known_strategies: tuple[str, ...] = (),
        source: str = SOURCE,
    ) -> None:
        self._ops = ops_store
        self._inbox = inbox
        self._notifier = notifier
        self._clock = clock
        self._registry = registry
        self._strategies = known_strategies
        self._source = source

    def run(self) -> CommandOutcome:
        """Drain, apply, advance the cursor, reply."""
        outcome = CommandOutcome()
        cursor = self._ops.command_cursor(self._source)
        messages = self._inbox.drain(after=cursor)
        outcome.seen = len(messages)
        if not messages:
            return outcome

        today = self._clock.today()
        replies: list[str] = []
        highest = cursor or 0

        for message in messages:
            highest = max(highest, message.update_id)
            if not is_command(message.text):
                # The digest lands in this chat too. Replying to every stray
                # message would make the channel unreadable, which is the one
                # failure this product does not survive.
                continue
            try:
                command = parse(message.text, today=today)
                reply = self._apply(command)
            except CommandError as exc:
                outcome.rejected.append(f"{message.text.strip()}: {exc}")
                replies.append(f"✗ {message.text.strip()} — {exc}")
                continue
            outcome.applied.append(reply)
            replies.append(f"✓ {reply}")

        # The cursor advances before the reply is attempted: a Telegram outage
        # must not cause every command to be applied a second time tomorrow.
        self._ops.save_command_cursor(self._source, highest, now=self._clock.now())

        if replies:
            self._reply(replies)
        log.info("commands_applied", **{"source": self._source, "summary": outcome.summary()})
        return outcome

    # ---- one command -----------------------------------------------------

    def _apply(self, command: Command) -> str:
        now = self._clock.now()
        match command.kind:
            case CommandKind.POSITION:
                assert command.symbol is not None and command.units is not None
                self._require_symbol(command.symbol)
                self._ops.save_position(
                    ActualPosition(
                        symbol=command.symbol,
                        units=command.units,
                        avg_cost=command.avg_cost,
                        updated_at=now,
                    )
                )
                return f"{command.symbol}: holding {command.units:g} units"

            case CommandKind.PAUSE:
                assert command.strategy is not None
                self._require_strategy(command.strategy)
                self._ops.set_control(
                    Control(
                        kind=ControlKind.PAUSE,
                        scope=command.strategy,
                        set_at=now,
                        reason="paused by the user",
                    )
                )
                return f"{command.strategy} paused — it keeps computing, it stops instructing"

            case CommandKind.RESUME:
                assert command.strategy is not None
                cleared = self._ops.clear_control(ControlKind.PAUSE, command.strategy)
                return (
                    f"{command.strategy} resumed"
                    if cleared
                    else f"{command.strategy} was not paused"
                )

            case CommandKind.MUTE:
                assert command.symbol is not None
                self._require_symbol(command.symbol)
                self._ops.set_control(
                    Control(
                        kind=ControlKind.MUTE,
                        scope=command.symbol,
                        until=command.until,
                        set_at=now,
                        reason="muted by the user",
                    )
                )
                return f"{command.symbol} muted until {command.until}"

            case CommandKind.UNMUTE:
                assert command.symbol is not None
                cleared = self._ops.clear_control(ControlKind.MUTE, command.symbol)
                return f"{command.symbol} unmuted" if cleared else f"{command.symbol} was not muted"

            case CommandKind.HOLD:
                self._ops.set_control(
                    Control(
                        kind=ControlKind.HOLD,
                        scope=HOLD_SCOPE,
                        until=command.until,
                        set_at=now,
                        reason="hold set by the user",
                    )
                )
                return f"rebalancing on hold until {command.until}"

            case CommandKind.UNHOLD:
                cleared = self._ops.clear_control(ControlKind.HOLD, HOLD_SCOPE)
                return "hold lifted" if cleared else "no hold was set"

            case CommandKind.STATUS:
                return self._status()

            case _:
                return help_text()

    def _status(self) -> str:
        """A one-message answer to "what is this thing currently doing?"."""
        controls = self._ops.controls(today=self._clock.today())
        positions = self._ops.positions()
        last = self._ops.last_successful_run()
        lines = [
            f"last good run: {last.started_at.date() if last else 'never'}",
            f"holdings: {', '.join(f'{p.symbol} {p.units:g}' for p in positions) or 'none recorded'}",
            f"controls: {', '.join(c.describe() for c in controls) or 'none'}",
            f"queued alerts: {len(self._ops.pending())}",
        ]
        return "status — " + " · ".join(lines)

    def _require_symbol(self, symbol: str) -> None:
        try:
            self._registry.instrument(symbol)
        except RegistryValidationError as exc:
            raise CommandError(str(exc)) from None

    def _require_strategy(self, strategy_id: str) -> None:
        if self._strategies and strategy_id not in self._strategies:
            raise CommandError(
                f"unknown strategy {strategy_id!r}; running: {', '.join(self._strategies)}"
            )

    def _reply(self, replies: list[str]) -> None:
        """One message for the whole batch.

        Not one per command: a reply per line turns a three-command evening into
        three notifications, and the digest is already competing for the same
        attention.
        """
        text = "\n".join(
            [
                *replies,
                "",
                "Applied at the start of this run. Anything sent from now on takes "
                "effect on the next one.",
            ]
        )
        try:
            self._notifier.send(text)
        except NotifierError as exc:
            # The commands are already applied and the cursor already moved. A
            # failed acknowledgement is worth a log line, never a failed run.
            log.warning("command_reply_failed", error=str(exc))
