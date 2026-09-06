"""The Telegram adapter, against a mocked API.

No live calls, ever: the point of the port is that the pipeline can be tested
without a bot, and the point of these tests is that the *transport* behaves when
the API misbehaves. Every case here is something Telegram actually does — a 401
after a token is revoked, a 400 for a wrong chat id, a message over the length
limit, and a stranger who found the bot.
"""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from finflow.adapters.notify.telegram import MAX_MESSAGE, TelegramBot
from finflow.ports.notifier import NotifierError

BASE = "https://api.telegram.org"
TOKEN = "123:fake"
CHAT = "5551234"


@pytest.fixture
def bot() -> TelegramBot:
    return TelegramBot(client=httpx.Client(), token=TOKEN, chat_id=CHAT, base_url=BASE)


def ok(result: object) -> httpx.Response:
    return httpx.Response(200, json={"ok": True, "result": result})


class TestSending:
    @respx.mock
    def test_a_message_goes_to_the_configured_chat(self, bot: TelegramBot) -> None:
        route = respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(ok({"message_id": 42}))
        assert bot.send("hello") == "42"
        assert route.calls.last.request.read().decode().count(CHAT) == 1

    @respx.mock
    def test_no_parse_mode_is_requested(self, bot: TelegramBot) -> None:
        # Markdown rejects an unescaped underscore, and strategy ids have them.
        # A delivery that fails on formatting is an alert that never arrives.
        route = respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(ok({"message_id": 1}))
        bot.send("sma_cross_precious_metals fired")
        assert "parse_mode" not in route.calls.last.request.read().decode()

    @respx.mock
    def test_a_long_message_is_split_rather_than_truncated(self, bot: TelegramBot) -> None:
        route = respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(ok({"message_id": 1}))
        bot.send("\n".join(f"line {i}" for i in range(2000)))
        assert route.call_count > 1
        sent = [json.loads(call.request.read())["text"] for call in route.calls]
        assert all(len(chunk) <= MAX_MESSAGE for chunk in sent)
        # Split, not truncated: every line still arrives, in order.
        assert "\n".join(sent).splitlines() == [f"line {i}" for i in range(2000)]

    @respx.mock
    def test_a_revoked_token_raises_with_the_reason(self, bot: TelegramBot) -> None:
        respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(
            httpx.Response(401, json={"ok": False, "description": "Unauthorized"})
        )
        with pytest.raises(NotifierError, match="401"):
            bot.send("hello")

    @respx.mock
    def test_a_transport_failure_becomes_a_notifier_error(self, bot: TelegramBot) -> None:
        # So the outbox row is released and retried, rather than an httpx
        # exception escaping into the pipeline and failing the whole run.
        respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(
            side_effect=httpx.ConnectError("no route to host")
        )
        with pytest.raises(NotifierError, match="failed"):
            bot.send("hello")

    @respx.mock
    def test_an_ok_false_body_is_still_a_failure(self, bot: TelegramBot) -> None:
        respx.post(f"{BASE}/bot{TOKEN}/sendMessage").mock(
            httpx.Response(200, json={"ok": False, "description": "chat not found"})
        )
        with pytest.raises(NotifierError, match="chat not found"):
            bot.send("hello")


class TestDraining:
    def _update(self, update_id: int, text: str, chat: str = CHAT) -> dict[str, object]:
        return {
            "update_id": update_id,
            "message": {"chat": {"id": int(chat)}, "text": text},
        }

    @respx.mock
    def test_pending_commands_come_back_in_order(self, bot: TelegramBot) -> None:
        respx.get(f"{BASE}/bot{TOKEN}/getUpdates").mock(
            ok([self._update(1, "/status"), self._update(2, "/position GLD 3")])
        )
        messages = bot.drain()
        assert [m.update_id for m in messages] == [1, 2]

    @respx.mock
    def test_the_cursor_is_sent_as_the_next_offset(self, bot: TelegramBot) -> None:
        route = respx.get(f"{BASE}/bot{TOKEN}/getUpdates").mock(ok([]))
        bot.drain(after=41)
        assert route.calls.last.request.url.params["offset"] == "42"

    @respx.mock
    def test_a_stranger_cannot_issue_commands(self, bot: TelegramBot) -> None:
        # A bot token is discoverable. `/position GLD 0` from a stranger would
        # silently corrupt the holdings the digest is computed from.
        respx.get(f"{BASE}/bot{TOKEN}/getUpdates").mock(
            ok([self._update(1, "/position GLD 0", chat="9999")])
        )
        assert bot.drain() == ()

    @respx.mock
    def test_a_message_with_no_text_is_skipped(self, bot: TelegramBot) -> None:
        respx.get(f"{BASE}/bot{TOKEN}/getUpdates").mock(
            ok([{"update_id": 1, "message": {"chat": {"id": int(CHAT)}}}])
        )
        assert bot.drain() == ()

    @respx.mock
    def test_the_call_does_not_long_poll(self, bot: TelegramBot) -> None:
        # It runs on the critical path of the 05:30 run; a long poll would hold
        # the pipeline open waiting for a message that is not coming.
        route = respx.get(f"{BASE}/bot{TOKEN}/getUpdates").mock(ok([]))
        bot.drain()
        assert route.calls.last.request.url.params["timeout"] == "0"
