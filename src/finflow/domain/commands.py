"""Parsing the controls the user overrules the system with.

``PROJECT.md`` §7.7: a system that only issues instructions will be disobeyed,
and once it is being disobeyed silently its record of what it recommended stops
corresponding to anything. So the overrides are first-class input, and parsing
them is a pure function over a string — no clock, no store, no network — which
is what makes every edge case a unit test rather than a Telegram conversation.

``today`` is an argument rather than a lookup so that ``/mute GLD 14d`` resolves
deterministically. A relative date is the form people actually type; an absolute
one is what gets stored.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from enum import StrEnum


class CommandKind(StrEnum):
    """Every control the bot understands."""

    POSITION = "position"
    PAUSE = "pause"
    RESUME = "resume"
    MUTE = "mute"
    UNMUTE = "unmute"
    HOLD = "hold"
    UNHOLD = "unhold"
    STATUS = "status"
    HELP = "help"


class CommandError(ValueError):
    """The message was addressed to the bot but could not be understood.

    Carries usage text rather than a bare complaint: the reply is the only
    documentation a phone user has.
    """


@dataclass(frozen=True, slots=True)
class Command:
    """One parsed control.

    A single shape with optional fields rather than a class per verb. There are
    nine verbs and they all resolve to "write one row", so nine classes would
    buy a type distinction the application layer immediately discards.
    """

    kind: CommandKind
    raw: str
    symbol: str | None = None
    strategy: str | None = None
    units: float | None = None
    avg_cost: float | None = None
    until: date | None = None


USAGE = {
    CommandKind.POSITION: "/position <SYMBOL> <units> [avg_cost]",
    CommandKind.PAUSE: "/pause <strategy>",
    CommandKind.RESUME: "/resume <strategy>",
    CommandKind.MUTE: "/mute <SYMBOL> <until: 2026-10-01 or 14d>",
    CommandKind.UNMUTE: "/unmute <SYMBOL>",
    CommandKind.HOLD: "/hold <until: 2026-10-01 or 14d>",
    CommandKind.UNHOLD: "/unhold",
    CommandKind.STATUS: "/status",
    CommandKind.HELP: "/help",
}

_SYMBOL = re.compile(r"^[A-Z0-9][A-Z0-9.\-]{0,15}$")
_RELATIVE = re.compile(r"^(?P<count>\d{1,4})(?P<unit>[dwm])$")


def is_command(text: str) -> bool:
    """True when a message is addressed to the bot at all.

    Anything else is ignored in silence. The chat is also where the digest
    lands, so replying to every stray message would make the channel unreadable
    — which is the failure mode this product cannot survive.
    """
    return text.strip().startswith("/")


def parse(text: str, *, today: date) -> Command:
    """Parse one command, or raise ``CommandError`` with usage text."""
    raw = text.strip()
    if not is_command(raw):
        raise CommandError("not a command")

    # Telegram appends @botname when several bots share a group.
    head, *rest = raw.split()
    verb = head[1:].split("@", 1)[0].lower()
    try:
        kind = CommandKind(verb)
    except ValueError:
        raise CommandError(f"unknown command /{verb}. Try: {', '.join(sorted(USAGE))}") from None

    match kind:
        case CommandKind.POSITION:
            return _position(kind, raw, rest)
        case CommandKind.PAUSE | CommandKind.RESUME:
            return Command(kind=kind, raw=raw, strategy=_one_argument(kind, rest))
        case CommandKind.MUTE:
            return _mute(kind, raw, rest, today)
        case CommandKind.UNMUTE:
            return Command(kind=kind, raw=raw, symbol=_symbol(_one_argument(kind, rest)))
        case CommandKind.HOLD:
            return Command(kind=kind, raw=raw, until=_until(_one_argument(kind, rest), today))
        case _:
            return Command(kind=kind, raw=raw)


def _one_argument(kind: CommandKind, rest: list[str]) -> str:
    if len(rest) != 1:
        raise CommandError(f"usage: {USAGE[kind]}")
    return rest[0]


def _position(kind: CommandKind, raw: str, rest: list[str]) -> Command:
    if len(rest) not in (2, 3):
        raise CommandError(f"usage: {USAGE[kind]}")
    symbol = _symbol(rest[0])
    units = _number(rest[1], "units")
    if units < 0:
        # Short positions are not modelled anywhere else in the system, so
        # accepting one here would produce a drift line nothing can act on.
        raise CommandError("units cannot be negative — this system is long-only")
    avg_cost = _number(rest[2], "avg_cost") if len(rest) == 3 else None
    return Command(kind=kind, raw=raw, symbol=symbol, units=units, avg_cost=avg_cost)


def _mute(kind: CommandKind, raw: str, rest: list[str], today: date) -> Command:
    if len(rest) != 2:
        raise CommandError(f"usage: {USAGE[kind]}")
    return Command(kind=kind, raw=raw, symbol=_symbol(rest[0]), until=_until(rest[1], today))


def _symbol(value: str) -> str:
    symbol = value.upper()
    if not _SYMBOL.match(symbol):
        raise CommandError(f"{value!r} is not a symbol")
    return symbol


def _number(value: str, field: str) -> float:
    try:
        return float(value.replace("_", ""))
    except ValueError:
        raise CommandError(f"{field} must be a number, got {value!r}") from None


def _until(value: str, today: date) -> date:
    """Accept ``2026-10-01``, ``14d``, ``2w`` or ``3m``.

    A mute with no end date is the control that quietly becomes permanent, so
    the grammar has no way to express one. ``/unmute`` is how a mute ends early.
    """
    relative = _RELATIVE.match(value.lower())
    if relative:
        count = int(relative.group("count"))
        days = {"d": 1, "w": 7, "m": 30}[relative.group("unit")]
        return today + timedelta(days=count * days)
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise CommandError(f"{value!r} is not a date. Use 2026-10-01, or 14d / 2w / 3m") from None
    if parsed <= today:
        raise CommandError(f"{parsed} is not in the future")
    return parsed


def help_text() -> str:
    """The reply to ``/help``, and to anything unparseable."""
    lines = ["FinFlow controls:"]
    lines.extend(f"  {usage}" for _, usage in sorted(USAGE.items()))
    lines.append("")
    lines.append(
        "Commands are applied at the start of the next scheduled run, not "
        "immediately — this is a timer, not a daemon."
    )
    return "\n".join(lines)
