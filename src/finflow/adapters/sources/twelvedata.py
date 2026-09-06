"""Twelve Data — daily OHLCV as JSON over HTTPS.

This client is the milestone's real test. ``PROJECT.md`` §4.2 claims that adding
a source costs *one interface*, and the only way to find out is to write a
second one and count what else had to change. The honest boundary is this
module, one settings key and one line at the composition root; anything beyond
those three means the port was wrong, and finding that out now is much cheaper
than finding it out at A1.

Its role is reconciliation rather than primary supply (``PROJECT.md`` §6.6): the
primary source always wins on write, and a secondary only ever produces a
divergence flag. That is why the daily budget matters more than the latency —
800 calls a day is generous for eight reconciliation fetches and nowhere near
enough for a forty-instrument backfill.

The vendor quirk worth knowing, and the reason this file is not three lines
shorter: **Twelve Data returns HTTP 200 with an error body.** A blown quota
arrives as ``{"code": 429, "status": "error"}`` with a 200 status line, exactly
as Stooq returns an HTML block page with a 200. Two vendors, the same trap: the
status code is not the answer, the body is.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from finflow.adapters.sources.http import HttpFetcher
from finflow.contracts.errors import (
    AuthenticationFailed,
    MalformedResponse,
    SourceRateLimited,
    SourceUnavailable,
    SymbolNotFound,
)
from finflow.contracts.frames import OhlcvBar, validate_frame
from finflow.contracts.sources import SourceKey
from finflow.ports.source import SourceCapabilities

SOURCE = SourceKey.TWELVEDATA
DAILY_QUOTA = 800
"""The documented free-tier allowance. Stated because it is *documented* — the
distinction ``SourceCapabilities`` draws between a known quota and an undocumented
one is what stops the budget planner treating Stooq's silence as "unlimited"."""

MAX_OUTPUT_SIZE = 5000
"""Rows per call. Roughly twenty years of daily bars, so a full backfill of one
instrument is one request rather than a paging loop."""

_EMPTY = pl.DataFrame(schema={f: OhlcvBar.dtypes[f] for f in OhlcvBar.columns})

_COLUMNS = {
    "datetime": "date",
    "open": "open",
    "high": "high",
    "low": "low",
    "close": "close",
    "volume": "volume",
}


class TwelveDataClient:
    """Fetches daily bars for one vendor symbol, e.g. ``SPY``."""

    def __init__(self, fetcher: HttpFetcher, *, base_url: str, api_key: str) -> None:
        self._fetcher = fetcher
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key

    def capabilities(self) -> SourceCapabilities:
        """Keyed, quota-documented, prices only."""
        return SourceCapabilities(
            key=SOURCE,
            supports_ohlcv=True,
            supports_macro=False,
            requires_auth=True,
            max_requests_per_day=DAILY_QUOTA,
        )

    def fetch(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Return daily bars for ``symbol`` between ``start`` and ``end``."""
        if start > end:
            return _EMPTY.clone()

        response = self._fetcher.get(
            f"{self._base_url}/time_series",
            params={
                "symbol": symbol,
                "interval": "1day",
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "outputsize": str(MAX_OUTPUT_SIZE),
                "order": "ASC",
                "format": "JSON",
                "apikey": self._api_key,
            },
            symbol=symbol,
        )

        try:
            payload = response.json()
        except ValueError as exc:
            raise MalformedResponse(
                "response was not JSON", source=SOURCE, symbol=symbol, payload=response.text[:2000]
            ) from exc

        self._raise_for_body(payload, symbol)
        return self._parse(payload, symbol)

    def _raise_for_body(self, payload: Any, symbol: str) -> None:
        """Map the vendor's in-band errors onto the taxonomy.

        Checked before any parsing, because every one of these arrives with an
        HTTP 200 and a body that is a perfectly valid JSON object — just not the
        one that was asked for.
        """
        if not isinstance(payload, dict):
            raise MalformedResponse(
                f"expected a JSON object, got {type(payload).__name__}",
                source=SOURCE,
                symbol=symbol,
            )
        if payload.get("status") != "error":
            return

        code = int(payload.get("code", 0))
        message = str(payload.get("message", "no message"))

        if code == 429:
            # The whole source is abandoned for the run and its symbols are
            # deferred; the vendor supplies no Retry-After, so the ingestion
            # service picks the window.
            raise SourceRateLimited(f"quota exhausted: {message}", source=SOURCE, symbol=symbol)
        if code in (401, 403):
            # The one failure no later instrument can work around.
            raise AuthenticationFailed(f"key rejected: {message}", source=SOURCE, symbol=symbol)
        if code == 404:
            raise SymbolNotFound(message, source=SOURCE, symbol=symbol)
        if code >= 500:
            raise SourceUnavailable(f"vendor error {code}: {message}", source=SOURCE, symbol=symbol)
        raise MalformedResponse(
            f"vendor error {code}: {message}", source=SOURCE, symbol=symbol, payload=str(payload)
        )

    def _parse(self, payload: dict[str, Any], symbol: str) -> pl.DataFrame:
        values = payload.get("values")
        if values is None:
            raise MalformedResponse(
                "no 'values' in response", source=SOURCE, symbol=symbol, payload=str(payload)[:2000]
            )
        if not values:
            # A range before the fund listed. Not an error: it has no bars.
            return _EMPTY.clone()

        try:
            frame = pl.DataFrame(values, infer_schema_length=None)
        except Exception as exc:
            raise MalformedResponse(
                f"could not read values: {exc}",
                source=SOURCE,
                symbol=symbol,
                payload=str(payload)[:2000],
            ) from exc

        missing = sorted(set(_COLUMNS) - set(frame.columns))
        if missing:
            raise MalformedResponse(
                f"missing column(s) {', '.join(missing)}",
                source=SOURCE,
                symbol=symbol,
                payload=str(payload)[:2000],
            )

        try:
            # Every numeric field arrives as a *string*, and volume is absent
            # for some instrument types rather than zero. Casting strictly here
            # means a vendor that starts sending "N/A" fails the contract
            # instead of quietly becoming a null price.
            frame = (
                frame.select(list(_COLUMNS))
                .rename(_COLUMNS)
                .with_columns(
                    pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=True),
                    *[
                        pl.col(column).cast(pl.Float64, strict=True)
                        for column in ("open", "high", "low", "close", "volume")
                    ],
                )
                .with_columns(pl.lit(symbol).alias("symbol"))
                .select(OhlcvBar.columns)
                .sort("date")
            )
        except Exception as exc:
            raise MalformedResponse(
                f"could not parse values: {exc}",
                source=SOURCE,
                symbol=symbol,
                payload=str(payload)[:2000],
            ) from exc

        try:
            validate_frame(OhlcvBar, frame)
        except ValueError as exc:
            raise MalformedResponse(
                str(exc), source=SOURCE, symbol=symbol, payload=str(payload)[:2000]
            ) from exc
        return frame
