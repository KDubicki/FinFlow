"""Live vendor tests.

Marked ``integration`` and **never run on a pull request**. Their job is not to
verify logic — the unit suite does that against recorded fixtures — but to
notice the day a vendor changes its format or starts refusing us. That is a
thing to learn from a nightly job, not from a morning with no digest.

    make test-live      (or: pytest -m integration)
"""

from __future__ import annotations

import datetime as dt

import pytest

from finflow.adapters.sources.fred import FredClient
from finflow.adapters.sources.http import HttpFetcher, build_client
from finflow.adapters.sources.stooq import StooqClient
from finflow.config import Settings, get_settings
from finflow.contracts.errors import SourceRateLimited
from finflow.contracts.frames import MacroObservation, OhlcvBar, validate_frame

pytestmark = pytest.mark.integration

START = dt.date(2024, 1, 2)
END = dt.date(2024, 1, 31)


@pytest.fixture(scope="module")
def settings() -> Settings:
    return get_settings()


def test_stooq_still_serves_csv(settings: Settings) -> None:
    """Stooq returns parseable bars, or tells us clearly that it will not.

    A ``SourceRateLimited`` here is a *pass with a warning*, not a failure: the
    client behaved correctly by refusing to parse. The failure this test exists
    to catch is a format change that slips through as plausible-looking data.
    """
    client = StooqClient(
        HttpFetcher(
            source="stooq",
            client=build_client(
                timeout=settings.http_timeout_seconds, user_agent=settings.http_user_agent
            ),
        ),
        base_url=settings.stooq_base_url,
    )
    try:
        frame = client.fetch("gld.us", START, END)
    except SourceRateLimited as exc:
        pytest.skip(f"Stooq is gating this network, and the client detected it: {exc}")

    assert not frame.is_empty()
    validate_frame(OhlcvBar, frame)
    dates = frame["date"].to_list()
    assert min(dates) >= START
    assert max(dates) <= END


def test_fred_still_serves_observations(settings: Settings) -> None:
    if settings.fred_api_key is None:
        pytest.skip("FINFLOW_FRED_API_KEY is not set")

    client = FredClient(
        HttpFetcher(
            source="fred",
            client=build_client(
                timeout=settings.http_timeout_seconds, user_agent=settings.http_user_agent
            ),
        ),
        base_url=settings.fred_base_url,
        api_key=settings.fred_api_key.get_secret_value(),
    )
    frame = client.fetch("DFII10", START, END)

    assert not frame.is_empty()
    validate_frame(MacroObservation, frame)


def test_fred_vintage_read_returns_dated_observations(settings: Settings) -> None:
    """ALFRED actually honours the realtime window.

    Worth asserting live: if this silently stopped working, every macro feature
    would quietly become a lookahead (``PROJECT.md`` §6.3) and nothing in the
    unit suite would notice, because the fixture would still say what we told it.
    """
    if settings.fred_api_key is None:
        pytest.skip("FINFLOW_FRED_API_KEY is not set")

    client = FredClient(
        HttpFetcher(
            source="fred",
            client=build_client(
                timeout=settings.http_timeout_seconds, user_agent=settings.http_user_agent
            ),
        ),
        base_url=settings.fred_base_url,
        api_key=settings.fred_api_key.get_secret_value(),
        vintage_aware=True,
    )
    frame = client.fetch("CPIAUCSL", dt.date(2023, 1, 1), dt.date(2023, 6, 30))

    assert not frame.is_empty()
    assert frame["vintage_date"].null_count() == 0
