"""Conformance suite for the ``SourceClient`` port.

One parametrized class run against every implementation, including the
synthetic one. This is the test that makes "adding a source is one interface"
true rather than aspirational: a new client proves itself against the same
contract instead of against its own assumptions.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import httpx
import patito as pt
import polars as pl
import pytest
import respx

from finflow.adapters.sources.fred import FredClient
from finflow.adapters.sources.http import HttpFetcher
from finflow.adapters.sources.stooq import StooqClient
from finflow.adapters.sources.synthetic import SyntheticClient
from finflow.contracts.frames import MacroObservation, OhlcvBar, validate_frame
from finflow.ports.source import SourceClient

STOOQ_URL = "https://stooq.test/q/d/l/"
FRED_URL = "https://fred.test/fred"

STOOQ_CSV = """Date,Open,High,Low,Close,Volume
2024-01-02,190.1,192.4,189.5,191.2,7000000
2024-01-03,191.2,191.9,188.7,189.4,6500000
"""

FRED_JSON = {
    "observations": [
        {"date": "2024-01-02", "value": "1.75", "realtime_start": "2024-01-03"},
        {"date": "2024-01-03", "value": ".", "realtime_start": "2024-01-04"},
        {"date": "2024-01-04", "value": "1.80", "realtime_start": "2024-01-05"},
    ]
}


@pytest.fixture
def client(request: pytest.FixtureRequest) -> Iterator[SourceClient]:
    """Every implementation, each with its network mocked out."""
    kind = request.param
    if kind == "synthetic":
        yield SyntheticClient(seed=7)
        return

    with respx.mock(assert_all_called=False) as mock:
        http = httpx.Client(timeout=5.0)
        if kind == "stooq":
            mock.get(url__startswith=STOOQ_URL).respond(
                200, text=STOOQ_CSV, headers={"content-type": "text/plain"}
            )
            yield StooqClient(HttpFetcher(source="stooq", client=http), base_url=STOOQ_URL)
        elif kind == "fred":
            mock.get(url__startswith=FRED_URL).respond(200, json=FRED_JSON)
            yield FredClient(
                HttpFetcher(source="fred", client=http), base_url=FRED_URL, api_key="k"
            )
        else:  # pragma: no cover
            raise AssertionError(kind)


pytestmark = pytest.mark.parametrize(
    "client", ["synthetic", "stooq", "fred"], indirect=True, ids=["synthetic", "stooq", "fred"]
)


def _schema(client: SourceClient) -> type[pt.Model]:
    return MacroObservation if client.capabilities().supports_macro else OhlcvBar


class TestSourceClientContract:
    def test_satisfies_the_port(self, client: SourceClient) -> None:
        assert isinstance(client, SourceClient)

    def test_declares_capabilities(self, client: SourceClient) -> None:
        caps = client.capabilities()
        # A source that serves neither grain is a source nothing can use.
        assert caps.supports_ohlcv or caps.supports_macro

    def test_returns_a_frame_matching_its_declared_contract(self, client: SourceClient) -> None:
        frame = client.fetch(
            "DFII10" if client.capabilities().supports_macro else "gld.us",
            date(2024, 1, 2),
            date(2024, 1, 5),
        )
        assert isinstance(frame, pl.DataFrame)
        validate_frame(_schema(client), frame)

    def test_an_inverted_range_returns_an_empty_frame_not_an_error(
        self, client: SourceClient
    ) -> None:
        # A fund that had not listed yet simply has no bars; that is not a fault.
        frame = client.fetch(
            "DFII10" if client.capabilities().supports_macro else "gld.us",
            date(2024, 1, 5),
            date(2024, 1, 2),
        )
        assert frame.is_empty()
        assert set(frame.columns) == set(_schema(client).columns)

    def test_an_empty_frame_still_carries_the_right_columns(self, client: SourceClient) -> None:
        # Downstream concatenation breaks on a schema-less empty frame.
        frame = client.fetch("x", date(2024, 1, 5), date(2024, 1, 2))
        assert list(frame.columns) == list(_schema(client).columns)

    def test_fetch_is_repeatable(self, client: SourceClient) -> None:
        symbol = "DFII10" if client.capabilities().supports_macro else "gld.us"
        first = client.fetch(symbol, date(2024, 1, 2), date(2024, 1, 5))
        second = client.fetch(symbol, date(2024, 1, 2), date(2024, 1, 5))
        assert first.equals(second)

    def test_fetch_has_no_side_effects_on_the_client(self, client: SourceClient) -> None:
        before = client.capabilities()
        client.fetch(
            "DFII10" if before.supports_macro else "gld.us", date(2024, 1, 2), date(2024, 1, 5)
        )
        assert client.capabilities() == before
