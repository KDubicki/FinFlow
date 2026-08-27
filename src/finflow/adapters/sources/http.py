"""Shared HTTP plumbing for source clients.

Transport failures map to the taxonomy here, once, so no client invents its own
opinion about what a connection reset means. Timing and byte counts are logged
on every call — the two numbers that tell you whether a slow run is the vendor
or the pipeline.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import httpx

from finflow.contracts.errors import SourceRateLimited, SourceUnavailable
from finflow.domain.ratelimit import TokenBucket
from finflow.logging import get_logger

log = get_logger(__name__)


class HttpFetcher:
    """A rate-limited HTTP GET that reports failures in the taxonomy's terms."""

    def __init__(
        self,
        *,
        source: str,
        client: httpx.Client,
        bucket: TokenBucket | None = None,
        sleep: object = time.sleep,
    ) -> None:
        self._source = source
        self._client = client
        self._bucket = bucket
        self._sleep = sleep

    def get(self, url: str, *, params: dict[str, str], symbol: str | None = None) -> httpx.Response:
        """Fetch a URL, waiting for the rate limiter first."""
        if self._bucket is not None:
            wait = self._bucket.take(datetime.now(UTC))
            if wait > timedelta(0):
                log.debug("rate_limit_wait", source=self._source, seconds=wait.total_seconds())
                self._sleep(wait.total_seconds())  # type: ignore[operator]

        started = time.perf_counter()
        try:
            response = self._client.get(url, params=params)
        except httpx.TimeoutException as exc:
            raise SourceUnavailable(
                f"timed out after {self._client.timeout.read}s", source=self._source, symbol=symbol
            ) from exc
        except httpx.TransportError as exc:
            raise SourceUnavailable(
                f"transport error: {exc}", source=self._source, symbol=symbol
            ) from exc

        elapsed_ms = (time.perf_counter() - started) * 1000
        log.info(
            "source_request",
            source=self._source,
            symbol=symbol,
            status=response.status_code,
            bytes=len(response.content),
            elapsed_ms=round(elapsed_ms, 1),
        )
        self._raise_for_status(response, symbol)
        return response

    def _raise_for_status(self, response: httpx.Response, symbol: str | None) -> None:
        if response.status_code == 429:
            raise SourceRateLimited(
                "HTTP 429",
                source=self._source,
                symbol=symbol,
                retry_after=_retry_after(response),
            )
        if response.status_code >= 500:
            raise SourceUnavailable(
                f"HTTP {response.status_code}", source=self._source, symbol=symbol
            )


def _retry_after(response: httpx.Response) -> timedelta | None:
    """Honour the vendor's own back-off hint when it supplies one."""
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return timedelta(seconds=float(raw))
    except ValueError:
        return None


def build_client(*, timeout: float, user_agent: str) -> httpx.Client:
    """A configured httpx client.

    Redirects are followed because vendors move endpoints; the timeout is
    explicit because httpx's default is no timeout at all, which is how an
    unattended run hangs until someone notices days later.
    """
    return httpx.Client(
        timeout=httpx.Timeout(timeout),
        follow_redirects=True,
        headers={"User-Agent": user_agent},
    )
