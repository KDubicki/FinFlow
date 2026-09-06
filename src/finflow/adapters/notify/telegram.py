"""Telegram, in both directions.

One class for both ports because Telegram is one API: ``sendMessage`` out,
``getUpdates`` in. Splitting it into two adapters would double the credential
handling to express a seam the transport does not have.

Three decisions worth stating, because each one is a failure that has happened
to somebody:

- **Plain text, no ``parse_mode``.** Markdown rejects unescaped ``_`` and ``*``,
  which appear in symbols and in error messages, and a delivery that fails on
  formatting is an alert that never arrives.
- **Messages are split, never truncated.** Telegram's limit is 4096 characters
  and a digest can exceed it on a bad day — precisely the day the tail of the
  message matters.
- **Only the configured chat may issue commands.** A bot token is discoverable,
  and ``/position GLD 0`` from a stranger would silently corrupt the holdings
  the digest is computed from.
"""

from __future__ import annotations

from typing import Any

import httpx

from finflow.logging import get_logger
from finflow.ports.notifier import InboundMessage, NotifierError

log = get_logger(__name__)

API_ROOT = "https://api.telegram.org"
MAX_MESSAGE = 4000
"""Below Telegram's 4096 so a split never lands on the boundary."""


class TelegramBot:
    """Sends digests and decisions; reads back the user's controls."""

    def __init__(
        self,
        *,
        client: httpx.Client,
        token: str,
        chat_id: str,
        base_url: str = API_ROOT,
        timeout: float = 20.0,
    ) -> None:
        self._client = client
        self._token = token
        self._chat_id = str(chat_id)
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    # ---- Notifier --------------------------------------------------------

    def send(self, text: str) -> str:
        """Deliver one message, splitting it if it is too long.

        Returns the last provider message id. A split message is still one
        delivery from the outbox's point of view: it is claimed once and marked
        once, so a failure halfway through is retried as a whole.
        """
        provider_id = ""
        for chunk in _split(text):
            payload = self._call(
                "sendMessage",
                {
                    "chat_id": self._chat_id,
                    "text": chunk,
                    "disable_web_page_preview": True,
                },
            )
            provider_id = str(payload.get("message_id", ""))
        return provider_id

    # ---- CommandInbox ----------------------------------------------------

    def drain(self, after: int | None = None) -> tuple[InboundMessage, ...]:
        """Fetch pending commands in a single call.

        ``timeout=0`` on purpose: this runs on the critical path of the daily
        pipeline, and a long poll would hold the 05:30 run open waiting for a
        message that is not coming.
        """
        params: dict[str, Any] = {"timeout": 0, "allowed_updates": '["message"]'}
        if after is not None:
            # Telegram treats offset as "confirm everything below this", which
            # is also how it drops updates we have already applied.
            params["offset"] = after + 1

        payload = self._call("getUpdates", params, method_verb="GET")
        updates = payload if isinstance(payload, list) else []

        messages: list[InboundMessage] = []
        for update in updates:
            message = update.get("message") or {}
            chat_id = str((message.get("chat") or {}).get("id", ""))
            text = message.get("text")
            if not text:
                continue
            if chat_id != self._chat_id:
                # Logged rather than ignored in silence: someone talking to the
                # bot is worth knowing about, even though it changes nothing.
                log.warning("telegram_foreign_chat", chat_id=chat_id)
                continue
            messages.append(
                InboundMessage(
                    update_id=int(update["update_id"]),
                    chat_id=chat_id,
                    text=str(text),
                )
            )
        return tuple(messages)

    # ---- transport -------------------------------------------------------

    def _call(self, method: str, params: dict[str, Any], *, method_verb: str = "POST") -> Any:
        url = f"{self._base_url}/bot{self._token}/{method}"
        try:
            response = (
                self._client.get(url, params=params, timeout=self._timeout)
                if method_verb == "GET"
                else self._client.post(url, json=params, timeout=self._timeout)
            )
        except httpx.HTTPError as exc:
            raise NotifierError(f"telegram {method} failed: {exc}") from exc

        if response.status_code >= 400:
            # The body carries Telegram's own description, which is the only
            # thing that distinguishes a wrong chat id from a revoked token.
            raise NotifierError(
                f"telegram {method} returned {response.status_code}: {response.text[:300]}"
            )
        try:
            body = response.json()
        except ValueError as exc:
            raise NotifierError(f"telegram {method} returned non-JSON") from exc
        if not body.get("ok"):
            raise NotifierError(f"telegram {method} refused: {body.get('description')}")
        return body.get("result")


def _split(text: str, limit: int = MAX_MESSAGE) -> list[str]:
    """Break a long message on line boundaries.

    On lines rather than characters because the digest is a table of short
    lines, and a split mid-number is the one thing worse than a long message.
    """
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current: list[str] = []
    size = 0
    for line in text.split("\n"):
        # A single line longer than the limit is hard-split; nothing else can
        # be done with it, and it never happens with the messages we render.
        pieces = [line[i : i + limit] for i in range(0, len(line), limit)] or [""]
        for piece in pieces:
            if size + len(piece) + 1 > limit and current:
                chunks.append("\n".join(current))
                current, size = [], 0
            current.append(piece)
            size += len(piece) + 1
    if current:
        chunks.append("\n".join(current))
    return chunks
