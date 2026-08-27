"""FRED and ALFRED — macro series.

FRED returns the series as it stands today. ALFRED returns it as it stood on a
past date, which is the difference between a backtest that could have been run
and one that quietly used a number published six weeks later
(``PROJECT.md`` §6.3).

The same endpoint serves both: passing ``realtime_start``/``realtime_end`` turns
a FRED read into an ALFRED one, so vintage awareness is a parameter rather than
a second client.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import polars as pl

from finflow.adapters.sources.http import HttpFetcher
from finflow.contracts.errors import (
    AuthenticationFailed,
    MalformedResponse,
    SymbolNotFound,
)
from finflow.contracts.frames import MacroObservation, validate_frame
from finflow.contracts.sources import SourceKey
from finflow.ports.source import SourceCapabilities

SOURCE = SourceKey.FRED

_EMPTY = pl.DataFrame(schema={f: MacroObservation.dtypes[f] for f in MacroObservation.columns})


class FredClient:
    """Fetches one macro series by its FRED identifier, e.g. ``DFII10``."""

    def __init__(
        self,
        fetcher: HttpFetcher,
        *,
        base_url: str,
        api_key: str,
        vintage_aware: bool = False,
    ) -> None:
        self._fetcher = fetcher
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._vintage_aware = vintage_aware

    def capabilities(self) -> SourceCapabilities:
        """Macro only, keyed, and able to read a past vintage."""
        return SourceCapabilities(
            key=SOURCE,
            supports_ohlcv=False,
            supports_macro=True,
            requires_auth=True,
            vintage_aware=True,
            max_requests_per_day=None,
        )

    def fetch(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        """Return observations for series ``symbol`` between ``start`` and ``end``."""
        if start > end:
            return _EMPTY.clone()

        params = {
            "series_id": symbol,
            "api_key": self._api_key,
            "file_type": "json",
            "observation_start": start.isoformat(),
            "observation_end": end.isoformat(),
        }
        if self._vintage_aware:
            # Ask for every vintage in the window rather than today's opinion,
            # which is what makes a point-in-time read possible at all.
            params["realtime_start"] = start.isoformat()
            params["realtime_end"] = end.isoformat()

        response = self._fetcher.get(
            f"{self._base_url}/series/observations", params=params, symbol=symbol
        )
        self._raise_for_api_error(response.status_code, response.text, symbol)

        try:
            payload: dict[str, Any] = response.json()
        except ValueError as exc:
            raise MalformedResponse(
                f"response was not JSON: {exc}",
                source=SOURCE,
                symbol=symbol,
                payload=response.text[:2000],
            ) from exc

        return self._parse(payload.get("observations", []), symbol, response.text)

    def _raise_for_api_error(self, status: int, text: str, symbol: str) -> None:
        """FRED reports its own errors as 400 with a message, not by status alone."""
        if status < 400:
            return
        lowered = text.lower()
        if "api_key" in lowered or status in {401, 403}:
            raise AuthenticationFailed(
                "FRED rejected the API key — check FINFLOW_FRED_API_KEY",
                source=SOURCE,
                symbol=symbol,
            )
        if "does not exist" in lowered or "not a valid series" in lowered:
            raise SymbolNotFound("FRED does not know this series", source=SOURCE, symbol=symbol)
        raise MalformedResponse(
            f"HTTP {status}: {text[:200]}", source=SOURCE, symbol=symbol, payload=text[:2000]
        )

    def _parse(self, observations: list[dict[str, Any]], symbol: str, raw: str) -> pl.DataFrame:
        rows = [
            {
                "series_id": symbol,
                "observation_date": obs["date"],
                "value": float(obs["value"]),
                # ALFRED returns realtime_start per observation; on a plain FRED
                # read every row carries the same one, which is not a vintage.
                "vintage_date": obs.get("realtime_start") if self._vintage_aware else None,
            }
            # "." is FRED's missing marker. Dropping it here rather than
            # downstream keeps "no observation" distinct from "value zero".
            for obs in observations
            if obs.get("value") not in (".", None, "")
        ]
        if not rows:
            return _EMPTY.clone()

        try:
            frame = pl.DataFrame(rows).with_columns(
                pl.col("observation_date").str.to_date(),
                pl.col("vintage_date").cast(pl.String).str.to_date(),
            )
        except Exception as exc:
            raise MalformedResponse(
                f"could not build frame: {exc}", source=SOURCE, symbol=symbol, payload=raw[:2000]
            ) from exc

        frame = frame.select(MacroObservation.columns).sort("observation_date")
        try:
            validate_frame(MacroObservation, frame)
        except ValueError as exc:
            raise MalformedResponse(
                str(exc), source=SOURCE, symbol=symbol, payload=raw[:2000]
            ) from exc
        return frame
