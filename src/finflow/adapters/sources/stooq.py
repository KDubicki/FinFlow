"""Stooq — daily OHLCV as CSV over HTTPS.

The important part of this client is not the parsing. It is that **Stooq
answers with HTTP 200 whatever happens**: a daily cap, an anti-bot gate or an
unknown symbol all arrive as a 200 with a body that is not CSV. A client that
trusts the status code and hands the body to a parser will eventually turn an
error page into a price bar, and nothing downstream would ever know.

So the response is validated *before* it is parsed — content type first, then
the header row — and anything that is not a CSV table raises rather than
returning. ``tests/fixtures/stooq_blocked.html`` is a real captured block page
and the test asserts exactly this.
"""

from __future__ import annotations

import io
from datetime import date

import polars as pl

from finflow.adapters.sources.http import HttpFetcher
from finflow.contracts.errors import (
    MalformedResponse,
    SourceRateLimited,
    SymbolNotFound,
)
from finflow.contracts.frames import OhlcvBar, validate_frame
from finflow.contracts.sources import SourceKey
from finflow.ports.source import SourceCapabilities

SOURCE = SourceKey.STOOQ
EXPECTED_HEADER = ("Date", "Open", "High", "Low", "Close", "Volume")

_EMPTY = pl.DataFrame(schema={f: OhlcvBar.dtypes[f] for f in OhlcvBar.columns})


class StooqClient:
    """Fetches daily bars for one vendor symbol, e.g. ``gld.us``."""

    def __init__(self, fetcher: HttpFetcher, *, base_url: str) -> None:
        self._fetcher = fetcher
        self._base_url = base_url

    def capabilities(self) -> SourceCapabilities:
        """Stooq needs no key and publishes no quota; the cap is real regardless."""
        return SourceCapabilities(
            key=SOURCE,
            supports_ohlcv=True,
            supports_macro=False,
            requires_auth=False,
            max_requests_per_day=None,
        )

    def fetch(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Return daily bars for ``symbol`` between ``start`` and ``end``."""
        if start > end:
            return _EMPTY.clone()

        response = self._fetcher.get(
            self._base_url,
            params={
                "s": symbol,
                "d1": start.strftime("%Y%m%d"),
                "d2": end.strftime("%Y%m%d"),
                "i": "d",
            },
            symbol=symbol,
        )
        text = response.text
        self._reject_non_csv(text, response.headers.get("content-type", ""), symbol)
        return self._parse(text, symbol)

    def _reject_non_csv(self, text: str, content_type: str, symbol: str) -> None:
        """Refuse anything that is not a CSV table, before parsing it.

        Order matters. A blocked request and an unknown symbol both arrive as
        HTTP 200, and only the body distinguishes them.
        """
        stripped = text.lstrip()

        if stripped.startswith(("<", "<!")) or "text/html" in content_type.lower():
            # A daily cap or the JavaScript proof-of-work interstitial. Both mean
            # "come back later", and neither is worth retrying inside this run.
            raise SourceRateLimited(
                "responded with HTML rather than CSV — rate-limited or gated",
                source=SOURCE,
                symbol=symbol,
            )

        if not stripped:
            raise MalformedResponse("empty response body", source=SOURCE, symbol=symbol)

        first_line = stripped.splitlines()[0].strip()

        if first_line.lower().startswith("no data"):
            raise SymbolNotFound(
                "vendor reports no data for this symbol", source=SOURCE, symbol=symbol
            )

        if tuple(first_line.split(",")) != EXPECTED_HEADER:
            raise MalformedResponse(
                f"unexpected header {first_line!r}; expected {','.join(EXPECTED_HEADER)}",
                source=SOURCE,
                symbol=symbol,
                payload=text[:2000],
            )

    def _parse(self, text: str, symbol: str) -> pl.DataFrame:
        try:
            frame = pl.read_csv(
                io.StringIO(text),
                schema_overrides={
                    "Date": pl.Date,
                    "Open": pl.Float64,
                    "High": pl.Float64,
                    "Low": pl.Float64,
                    "Close": pl.Float64,
                    "Volume": pl.Float64,
                },
            )
        except Exception as exc:
            raise MalformedResponse(
                f"could not parse CSV: {exc}", source=SOURCE, symbol=symbol, payload=text[:2000]
            ) from exc

        if frame.is_empty():
            return _EMPTY.clone()

        frame = frame.rename(
            {
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        ).with_columns(pl.lit(symbol).alias("symbol"))
        frame = frame.select(OhlcvBar.columns).sort("date")

        try:
            validate_frame(OhlcvBar, frame)
        except ValueError as exc:
            raise MalformedResponse(
                str(exc), source=SOURCE, symbol=symbol, payload=text[:2000]
            ) from exc
        return frame
