"""Outbound messages and inbound commands.

One adapter per transport. Telegram happens to satisfy both ports because it is
one API in both directions; the console pair exists so a run with no bot token
is still a complete run rather than a crash, which is what makes ``--dry-run``
and CI work without a credential.
"""

from __future__ import annotations

from finflow.adapters.notify.console import ConsoleNotifier, SilentInbox
from finflow.adapters.notify.telegram import TelegramBot

__all__ = ["ConsoleNotifier", "SilentInbox", "TelegramBot"]
