"""Building real objects from settings.

This is the composition root (``PROJECT.md`` §4.1). Nothing inward of here
constructs a client, opens a database or reads a credential — which is what
makes the synthetic source, the in-memory store and the frozen clock work in
tests without patching anything.
"""

from __future__ import annotations

from finflow.adapters.clock import SystemClock
from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.adapters.sources.fred import FredClient
from finflow.adapters.sources.http import HttpFetcher, build_client
from finflow.adapters.sources.stooq import StooqClient
from finflow.adapters.sources.synthetic import SyntheticClient
from finflow.adapters.storage import LocalObjectStore
from finflow.config import Settings
from finflow.contracts.sources import SourceKey
from finflow.domain.ratelimit import TokenBucket
from finflow.logging import get_logger
from finflow.ports.source import SourceClient

log = get_logger(__name__)


def build_sources(settings: Settings, *, offline: bool = False) -> dict[SourceKey, SourceClient]:
    """Construct the source clients this configuration can actually use.

    A source with no credential is **left out rather than constructed broken**.
    The alternative is a client that raises ``AuthenticationFailed`` on first
    use, which fails the whole run — a missing key should mean "that source is
    unavailable", not "nothing runs".
    """
    synthetic = SyntheticClient(seed=settings.synthetic_seed)
    if offline:
        # Every source key maps to the synthetic generator, so a demo or a test
        # exercises the real wiring without touching the network.
        return dict.fromkeys(
            (SourceKey.STOOQ, SourceKey.FRED, SourceKey.TWELVEDATA, SourceKey.SYNTHETIC),
            synthetic,
        )

    http = build_client(timeout=settings.http_timeout_seconds, user_agent=settings.http_user_agent)
    sources: dict[SourceKey, SourceClient] = {
        SourceKey.SYNTHETIC: synthetic,
        SourceKey.STOOQ: StooqClient(
            HttpFetcher(
                source=str(SourceKey.STOOQ),
                client=http,
                bucket=TokenBucket(per_minute=settings.stooq_requests_per_minute),
            ),
            base_url=settings.stooq_base_url,
        ),
    }

    if settings.fred_api_key is not None:
        sources[SourceKey.FRED] = FredClient(
            HttpFetcher(
                source=str(SourceKey.FRED),
                client=http,
                bucket=TokenBucket(per_minute=settings.fred_requests_per_minute),
            ),
            base_url=settings.fred_base_url,
            api_key=settings.fred_api_key.get_secret_value(),
        )
    else:
        log.warning("source_unavailable", source="fred", reason="FINFLOW_FRED_API_KEY not set")

    return sources


def build_object_store(settings: Settings) -> LocalObjectStore:
    """The object store, rooted at the data directory.

    Rooted at ``data_dir`` rather than ``raw_dir`` because the key layout
    already prefixes ``raw/`` (``domain.layout``), and because manifests live
    alongside the raw zone rather than inside it.
    """
    return LocalObjectStore(settings.data_dir)


def build_ops_store(settings: Settings) -> SqliteOpsStore:
    """The operational store — the one piece of state a rebuild cannot recreate."""
    return SqliteOpsStore(settings.data_dir / "ops.sqlite")


def build_clock() -> SystemClock:
    """The real clock. The only one outside tests."""
    return SystemClock()
