"""Vendor failures, and that each maps to the right class in the taxonomy.

The Stooq tests are the load-bearing ones. Stooq answers **HTTP 200 whatever
happens**, so a client that trusts the status code will eventually parse an
error page into a price bar and nothing downstream will ever know.
``tests/fixtures/stooq_blocked.html`` is a real captured block page.
"""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
import respx

from finflow.adapters.sources.fred import FredClient
from finflow.adapters.sources.http import HttpFetcher
from finflow.adapters.sources.stooq import StooqClient
from finflow.contracts.errors import (
    AuthenticationFailed,
    MalformedResponse,
    SourceRateLimited,
    SourceUnavailable,
    SymbolNotFound,
)
from finflow.domain.retry import policy_for

STOOQ_URL = "https://stooq.test/q/d/l/"
FRED_URL = "https://fred.test/fred"
FIXTURES = Path(__file__).parent / "fixtures"
START, END = __import__("datetime").date(2024, 1, 2), __import__("datetime").date(2024, 1, 5)


def stooq(mock: respx.MockRouter) -> StooqClient:
    return StooqClient(
        HttpFetcher(source="stooq", client=httpx.Client(timeout=5.0)), base_url=STOOQ_URL
    )


def fred(mock: respx.MockRouter) -> FredClient:
    return FredClient(
        HttpFetcher(source="fred", client=httpx.Client(timeout=5.0)),
        base_url=FRED_URL,
        api_key="k",
    )


class TestStooqRefusesNonCsv:
    @respx.mock
    def test_the_real_captured_block_page_raises_rather_than_parsing(self) -> None:
        # This is the whole reason the client validates before parsing. The page
        # arrives with HTTP 200 and would otherwise reach a CSV reader.
        page = (FIXTURES / "stooq_blocked.html").read_text(encoding="utf-8")
        respx.get(url__startswith=STOOQ_URL).respond(
            200, text=page, headers={"content-type": "text/html; charset=utf-8"}
        )
        with pytest.raises(SourceRateLimited, match="HTML rather than CSV"):
            stooq(respx.mock).fetch("gld.us", START, END)

    @respx.mock
    def test_the_block_page_defers_the_source_rather_than_retrying(self) -> None:
        page = (FIXTURES / "stooq_blocked.html").read_text(encoding="utf-8")
        respx.get(url__startswith=STOOQ_URL).respond(200, text=page)
        with pytest.raises(SourceRateLimited) as caught:
            stooq(respx.mock).fetch("gld.us", START, END)
        policy = policy_for(caught.value)
        assert policy.defer_source
        assert policy.attempts == 0

    @respx.mock
    def test_html_is_caught_even_when_the_content_type_lies(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(
            200, text="<!DOCTYPE html><html></html>", headers={"content-type": "text/plain"}
        )
        with pytest.raises(SourceRateLimited):
            stooq(respx.mock).fetch("gld.us", START, END)

    @respx.mock
    def test_an_unknown_symbol_is_not_a_rate_limit(self) -> None:
        # Conflating the two would defer the whole source over one typo.
        respx.get(url__startswith=STOOQ_URL).respond(200, text="No data\n")
        with pytest.raises(SymbolNotFound):
            stooq(respx.mock).fetch("nope.us", START, END)

    @respx.mock
    def test_an_unexpected_header_is_malformed_and_keeps_the_payload(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(200, text="Foo,Bar\n1,2\n")
        with pytest.raises(MalformedResponse) as caught:
            stooq(respx.mock).fetch("gld.us", START, END)
        assert caught.value.payload is not None
        assert policy_for(caught.value).quarantine

    @respx.mock
    def test_an_empty_body_is_malformed(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(200, text="")
        with pytest.raises(MalformedResponse, match="empty response"):
            stooq(respx.mock).fetch("gld.us", START, END)

    @respx.mock
    def test_a_negative_price_never_becomes_a_bar(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(
            200, text="Date,Open,High,Low,Close,Volume\n2024-01-02,-1,2,0.5,1,100\n"
        )
        with pytest.raises(MalformedResponse):
            stooq(respx.mock).fetch("gld.us", START, END)


class TestTransportFailures:
    @respx.mock
    def test_a_500_is_transient_and_retried(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(503)
        with pytest.raises(SourceUnavailable, match="HTTP 503") as caught:
            stooq(respx.mock).fetch("gld.us", START, END)
        assert policy_for(caught.value).attempts > 0

    @respx.mock
    def test_a_429_is_a_rate_limit_and_honours_retry_after(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(429, headers={"Retry-After": "120"})
        with pytest.raises(SourceRateLimited) as caught:
            stooq(respx.mock).fetch("gld.us", START, END)
        assert caught.value.retry_after is not None
        assert caught.value.retry_after.total_seconds() == 120

    @respx.mock
    def test_an_unparseable_retry_after_is_ignored_not_fatal(self) -> None:
        respx.get(url__startswith=STOOQ_URL).respond(429, headers={"Retry-After": "Wed, 21 Oct"})
        with pytest.raises(SourceRateLimited) as caught:
            stooq(respx.mock).fetch("gld.us", START, END)
        assert caught.value.retry_after is None

    @respx.mock
    def test_a_timeout_is_unavailable_not_a_crash(self) -> None:
        respx.get(url__startswith=STOOQ_URL).mock(side_effect=httpx.ConnectTimeout("slow"))
        with pytest.raises(SourceUnavailable, match="timed out"):
            stooq(respx.mock).fetch("gld.us", START, END)

    @respx.mock
    def test_a_connection_reset_is_unavailable(self) -> None:
        respx.get(url__startswith=STOOQ_URL).mock(side_effect=httpx.ConnectError("reset"))
        with pytest.raises(SourceUnavailable, match="transport error"):
            stooq(respx.mock).fetch("gld.us", START, END)


class TestFredErrors:
    @respx.mock
    def test_a_bad_key_fails_the_run_loudly(self) -> None:
        respx.get(url__startswith=FRED_URL).respond(
            400, json={"error_message": "Bad Request. The value for variable api_key is invalid."}
        )
        with pytest.raises(AuthenticationFailed, match="FINFLOW_FRED_API_KEY") as caught:
            fred(respx.mock).fetch("DFII10", START, END)
        # The one policy that stops everything: no later series can work around it.
        assert policy_for(caught.value).fail_run

    @respx.mock
    def test_an_unknown_series_blames_the_registry_not_the_run(self) -> None:
        respx.get(url__startswith=FRED_URL).respond(
            400, json={"error_message": "The series does not exist."}
        )
        with pytest.raises(SymbolNotFound) as caught:
            fred(respx.mock).fetch("NOPE", START, END)
        assert policy_for(caught.value).registry_incident
        assert not policy_for(caught.value).fail_run

    @respx.mock
    def test_non_json_is_malformed(self) -> None:
        respx.get(url__startswith=FRED_URL).respond(200, text="<html>maintenance</html>")
        with pytest.raises(MalformedResponse, match="not JSON"):
            fred(respx.mock).fetch("DFII10", START, END)

    @respx.mock
    def test_the_missing_marker_is_dropped_not_read_as_zero(self) -> None:
        # FRED writes "." for a missing observation. Reading it as 0.0 would put
        # a real-yield print of zero into the feature frame.
        respx.get(url__startswith=FRED_URL).respond(
            200,
            json={
                "observations": [
                    {"date": "2024-01-02", "value": "."},
                    {"date": "2024-01-03", "value": "1.80"},
                ]
            },
        )
        frame = fred(respx.mock).fetch("DFII10", START, END)
        assert len(frame) == 1
        assert frame["value"][0] == 1.80

    @respx.mock
    def test_all_missing_values_yields_an_empty_frame(self) -> None:
        respx.get(url__startswith=FRED_URL).respond(
            200, json={"observations": [{"date": "2024-01-02", "value": "."}]}
        )
        assert fred(respx.mock).fetch("DFII10", START, END).is_empty()

    @respx.mock
    def test_a_vintage_aware_read_requests_alfred_realtime_params(self) -> None:
        route = respx.get(url__startswith=FRED_URL).respond(200, json={"observations": []})
        client = FredClient(
            HttpFetcher(source="fred", client=httpx.Client(timeout=5.0)),
            base_url=FRED_URL,
            api_key="k",
            vintage_aware=True,
        )
        client.fetch("CPIAUCSL", START, END)
        query = route.calls.last.request.url.params
        # Without these, the read returns today's opinion of history, which is
        # the lookahead PROJECT.md §6.3 exists to prevent.
        assert query["realtime_start"] == START.isoformat()
        assert query["realtime_end"] == END.isoformat()

    @respx.mock
    def test_a_plain_read_does_not_send_realtime_params(self) -> None:
        route = respx.get(url__startswith=FRED_URL).respond(200, json={"observations": []})
        fred(respx.mock).fetch("DFII10", START, END)
        assert "realtime_start" not in route.calls.last.request.url.params
