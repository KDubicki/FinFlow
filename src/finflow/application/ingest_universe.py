"""Ingest every enabled instrument from every source that can serve it.

This is where the retry policy of ``PROJECT.md`` §6.7 is actually applied.
Clients raise; this decides what a failure means. Keeping that decision in one
place is what stops five clients growing five subtly different retry loops, and
it is why the policy is a lookup table rather than a chain of ``except`` blocks
scattered across adapters.

The failure domain is the instrument (``PROJECT.md`` §4.4): one symbol failing
must not stop the other thirty-nine. The single exception is a bad credential,
which no later instrument can work around.
"""

from __future__ import annotations

import io
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date, timedelta

import polars as pl

from finflow.contracts.errors import AuthenticationFailed, SourceError
from finflow.contracts.frames import PROVENANCE
from finflow.contracts.sources import SourceKey
from finflow.domain.layout import RawPartition, manifest_key
from finflow.domain.manifest import Manifest
from finflow.domain.retry import policy_for
from finflow.logging import get_logger
from finflow.ports.clock import Clock
from finflow.ports.object_store import ObjectStore
from finflow.ports.ops_store import OpsStore, Watermark
from finflow.ports.source import SourceClient
from finflow.registry.models import Registry

log = get_logger(__name__)


@dataclass
class IngestionOutcome:
    """What one run did, for the digest and for ``pipeline_runs``."""

    run_id: str
    manifest: Manifest | None = None
    written: list[RawPartition] = field(default_factory=list)
    skipped: dict[str, str] = field(default_factory=dict)
    failed: dict[str, str] = field(default_factory=dict)
    deferred_sources: set[SourceKey] = field(default_factory=set)
    rows: int = 0

    @property
    def snapshot_id(self) -> str | None:
        """The manifest hash, once the run has produced one."""
        return self.manifest.snapshot_id if self.manifest else None

    def summary(self) -> str:
        """One line, for a log or a Telegram digest."""
        return (
            f"{len(self.written)} partitions, {self.rows} rows, "
            f"{len(self.skipped)} skipped, {len(self.failed)} failed"
        )


class IngestUniverse:
    """Fetch, validate and land raw data for the registry's enabled instruments."""

    def __init__(
        self,
        *,
        registry: Registry,
        sources: Mapping[SourceKey, SourceClient],
        object_store: ObjectStore,
        ops_store: OpsStore,
        clock: Clock,
        deferral: timedelta = timedelta(hours=12),
        sleep: object = time.sleep,
    ) -> None:
        self._registry = registry
        self._sources = sources
        self._objects = object_store
        self._ops = ops_store
        self._clock = clock
        self._deferral = deferral
        self._sleep = sleep

    def run(self, *, symbols: Sequence[str] | None = None, full: bool = False) -> IngestionOutcome:
        """Ingest, returning what happened rather than raising on partial failure.

        ``full`` ignores watermarks and re-fetches from ``backfill_start``. That
        is safe precisely because the raw zone is append-only: a re-fetch is a
        new partition holding a later opinion, never an overwrite.
        """
        outcome = IngestionOutcome(run_id=uuid.uuid4().hex[:12])
        instruments = [
            i for i in self._registry.enabled() if symbols is None or i.symbol in set(symbols)
        ]
        log.info("ingestion_started", run_id=outcome.run_id, instruments=len(instruments))

        for instrument in instruments:
            for source_key, vendor_symbol in instrument.sources.items():
                client = self._sources.get(source_key)
                if client is None:
                    # The registry validates that source keys are implementable;
                    # this only fires when a run is wired with a subset.
                    outcome.skipped[f"{source_key}:{instrument.symbol}"] = "source not wired"
                    continue
                if source_key in outcome.deferred_sources:
                    outcome.skipped[f"{source_key}:{instrument.symbol}"] = "source deferred earlier"
                    self._defer(source_key, instrument.symbol)
                    continue
                self._ingest_one(
                    client=client,
                    source_key=source_key,
                    symbol=instrument.symbol,
                    vendor_symbol=vendor_symbol,
                    start=self._start_for(
                        source_key, instrument.symbol, instrument.backfill_start, full
                    ),
                    outcome=outcome,
                )

        outcome.manifest = Manifest.of(
            outcome.run_id,
            self._clock.now(),
            outcome.written,
            registry_commit=self._registry.commit.sha,
        )
        self._objects.put(manifest_key(outcome.run_id), outcome.manifest.to_json().encode("utf-8"))
        log.info(
            "ingestion_finished",
            run_id=outcome.run_id,
            snapshot_id=outcome.snapshot_id,
            summary=outcome.summary(),
        )
        return outcome

    # ---- one (source, symbol) pair ---------------------------------------

    def _ingest_one(
        self,
        *,
        client: SourceClient,
        source_key: SourceKey,
        symbol: str,
        vendor_symbol: str,
        start: date | None,
        outcome: IngestionOutcome,
    ) -> None:
        pair = f"{source_key}:{symbol}"
        if start is None:
            outcome.skipped[pair] = "deferred until later"
            return

        end = self._clock.today()
        if start > end:
            outcome.skipped[pair] = "already up to date"
            return

        try:
            frame = self._fetch_with_policy(client, source_key, vendor_symbol, start, end)
        except AuthenticationFailed:
            # The one failure no later instrument can work around.
            raise
        except SourceError as exc:
            self._handle_failure(exc, source_key, symbol, pair, outcome)
            return

        if frame.is_empty():
            outcome.skipped[pair] = "no rows in range"
            return

        partition = self._land(frame, source_key, symbol, outcome.run_id)
        outcome.written.append(partition)
        outcome.rows += len(frame)
        self._ops.save_watermark(
            Watermark(
                source=source_key,
                symbol=symbol,
                last_loaded_date=self._last_date(frame),
                last_run_at=self._clock.now(),
                row_count=len(frame),
                deferred_until=None,
            )
        )

    def _fetch_with_policy(
        self,
        client: SourceClient,
        source_key: SourceKey,
        vendor_symbol: str,
        start: date,
        end: date,
    ) -> pl.DataFrame:
        """Fetch, retrying only where the error class says retrying helps."""
        attempt = 0
        while True:
            try:
                return client.fetch(vendor_symbol, start, end)
            except SourceError as exc:
                policy = policy_for(exc)
                attempt += 1
                if attempt > policy.attempts:
                    raise
                delay = policy.backoff_seconds(attempt)
                if policy.jitter:
                    # Deterministic jitter: enough to desynchronise retries
                    # without making a test wait on a random number.
                    delay *= 1.0 + (attempt % 3) * 0.1
                log.warning(
                    "source_retry",
                    source=str(source_key),
                    symbol=vendor_symbol,
                    attempt=attempt,
                    delay_seconds=round(delay, 2),
                    error=type(exc).__name__,
                )
                self._sleep(delay)  # type: ignore[operator]

    def _handle_failure(
        self,
        exc: SourceError,
        source_key: SourceKey,
        symbol: str,
        pair: str,
        outcome: IngestionOutcome,
    ) -> None:
        policy = policy_for(exc)
        outcome.failed[pair] = f"{type(exc).__name__}: {exc.message}"
        log.warning(
            "ingestion_failed",
            source=str(source_key),
            symbol=symbol,
            error=type(exc).__name__,
            registry_incident=policy.registry_incident,
        )
        if policy.defer_source:
            # Abandon the whole source, not just this symbol: the cap is per
            # vendor, so continuing would spend the remaining budget on refusals.
            outcome.deferred_sources.add(source_key)
            self._defer(source_key, symbol, getattr(exc, "retry_after", None))

    def _defer(
        self, source_key: SourceKey, symbol: str, retry_after: timedelta | None = None
    ) -> None:
        until = self._clock.now() + (retry_after or self._deferral)
        self._ops.defer(source_key, symbol, until)

    # ---- helpers ---------------------------------------------------------

    def _start_for(
        self, source_key: SourceKey, symbol: str, backfill_start: date, full: bool
    ) -> date | None:
        """Where to resume, or None when this pair is still deferred."""
        if full:
            return backfill_start
        mark = self._ops.watermark(source_key, symbol)
        if mark is None:
            return backfill_start
        if mark.is_deferred(self._clock.now()):
            return None
        if mark.last_loaded_date is None:
            return backfill_start
        # Re-fetch the last loaded day: a vendor may restate it, and the raw
        # zone is append-only so an extra overlapping row costs nothing.
        return mark.last_loaded_date

    def _land(
        self, frame: pl.DataFrame, source_key: SourceKey, symbol: str, run_id: str
    ) -> RawPartition:
        """Stamp provenance and write one immutable partition."""
        ingested_at = self._clock.now()
        stamped = frame.with_columns(
            pl.lit(str(source_key)).alias("source"),
            pl.lit(ingested_at).alias("ingested_at"),
            pl.lit(run_id).alias("ingestion_run_id"),
        )
        assert set(PROVENANCE) <= set(stamped.columns)

        partition = RawPartition(source=str(source_key), symbol=symbol, ingested_at=ingested_at)
        buffer = io.BytesIO()
        stamped.write_parquet(buffer)
        self._objects.put(partition.key, buffer.getvalue())
        return partition

    @staticmethod
    def _last_date(frame: pl.DataFrame) -> date | None:
        column = "date" if "date" in frame.columns else "observation_date"
        value = frame[column].max()
        return value if isinstance(value, date) else None
