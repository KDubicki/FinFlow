"""The ingestion use case: failure isolation, resumption and convergence.

The behaviours here are the ones that decide whether an unattended run degrades
or lies. A source outage must cost the affected instruments and nothing else; a
rate limit must resume rather than re-hitting the cap every morning; and a
re-fetch must land beside the old data rather than on top of it.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.adapters.sources.synthetic import SyntheticClient
from finflow.adapters.storage import InMemoryObjectStore
from finflow.application.ingest_universe import IngestUniverse
from finflow.contracts.errors import (
    AuthenticationFailed,
    MalformedResponse,
    SourceRateLimited,
    SourceUnavailable,
    SymbolNotFound,
)
from finflow.contracts.sources import SourceKey
from finflow.domain.layout import parse_raw_key, raw_prefix
from finflow.ports.source import SourceCapabilities
from finflow.registry import load_registry
from finflow.registry.models import Registry
from tests.fakes import FrozenClock

NOW = datetime(2026, 8, 27, 5, 12, tzinfo=UTC)
REGISTRY_DIR = Path(__file__).resolve().parents[1] / "instruments"


class FailingClient:
    """Raises a chosen error; counts calls so retries are observable."""

    def __init__(self, error: Exception, *, fail_times: int | None = None) -> None:
        self._error = error
        self._fail_times = fail_times
        self.calls = 0
        self._inner = SyntheticClient(seed=1)

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            key=SourceKey.STOOQ, supports_ohlcv=True, supports_macro=False, requires_auth=False
        )

    def fetch(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        self.calls += 1
        if self._fail_times is None or self.calls <= self._fail_times:
            raise self._error
        return self._inner.fetch(symbol, start, end)


class PerSymbolClient:
    """Fails for one symbol, succeeds for the rest."""

    def __init__(self, bad: str, error: Exception) -> None:
        self._bad, self._error = bad, error
        self._inner = SyntheticClient(seed=1)

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            key=SourceKey.STOOQ, supports_ohlcv=True, supports_macro=False, requires_auth=False
        )

    def fetch(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        if symbol == self._bad:
            raise self._error
        return self._inner.fetch(symbol, start, end)


@pytest.fixture
def registry() -> Registry:
    return load_registry(REGISTRY_DIR)


@pytest.fixture
def ops(tmp_path: Path) -> SqliteOpsStore:
    return SqliteOpsStore(tmp_path / "ops.sqlite")


def build(
    registry: Registry, ops: SqliteOpsStore, client: object, **kw: object
) -> tuple[IngestUniverse, InMemoryObjectStore]:
    """Wire ``client`` as Stooq only.

    Twelve Data gets its own healthy client, because several slice instruments
    declare both sources and sharing one object would make call counts
    ambiguous -- and because "one vendor is down while another is up" is the
    situation that actually arises.
    """
    store = InMemoryObjectStore()
    use_case = IngestUniverse(
        registry=registry,
        sources={
            SourceKey.STOOQ: client,  # type: ignore[dict-item]
            SourceKey.TWELVEDATA: SyntheticClient(seed=99),
        },
        object_store=store,
        ops_store=ops,
        clock=FrozenClock(NOW),
        sleep=lambda _seconds: None,
        **kw,  # type: ignore[arg-type]
    )
    return use_case, store


class TestHappyPath:
    def test_lands_a_partition_per_source_and_symbol(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        use_case, store = build(registry, ops, SyntheticClient(seed=1))
        outcome = use_case.run()

        assert outcome.failed == {}
        assert outcome.rows > 0
        assert len(store.list(raw_prefix())) == len(outcome.written)

    def test_stamps_provenance_on_every_row(self, registry: Registry, ops: SqliteOpsStore) -> None:
        use_case, store = build(registry, ops, SyntheticClient(seed=1))
        outcome = use_case.run(symbols=["GLD"])

        frame = pl.read_parquet(store.get(outcome.written[0].key))
        assert frame["source"].unique().to_list() == ["stooq"]
        assert frame["ingestion_run_id"].unique().to_list() == [outcome.run_id]

    def test_writes_a_manifest_whose_snapshot_covers_what_was_read(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        use_case, store = build(registry, ops, SyntheticClient(seed=1))
        outcome = use_case.run(symbols=["GLD"])

        assert outcome.manifest is not None
        assert outcome.manifest.partition_count == len(outcome.written)
        assert outcome.manifest.registry_commit == registry.commit.sha
        assert store.exists(f"manifests/{outcome.run_id}.json")

    def test_records_a_watermark_so_the_next_run_resumes(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        use_case, _ = build(registry, ops, SyntheticClient(seed=1))
        use_case.run(symbols=["GLD"])

        mark = ops.watermark(SourceKey.STOOQ, "GLD")
        assert mark is not None
        assert mark.last_loaded_date is not None
        assert mark.row_count > 0
        assert mark.deferred_until is None


class TestConvergence:
    def test_a_refetch_lands_beside_the_old_partition_never_on_top(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        # Vendors restate history, so idempotency here is convergent rather than
        # identical (PROJECT.md §6.2). Both opinions are kept.
        use_case, store = build(registry, ops, SyntheticClient(seed=1))
        use_case.run(symbols=["GLD"], full=True)
        use_case._clock = FrozenClock(NOW + timedelta(hours=1))
        use_case.run(symbols=["GLD"], full=True)

        keys = store.list(raw_prefix("stooq", "GLD"))
        assert len(keys) == 2
        assert parse_raw_key(keys[0]).ingested_at < parse_raw_key(keys[1]).ingested_at

    def test_resolving_the_partitions_gives_one_row_per_symbol_and_date(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        use_case, store = build(registry, ops, SyntheticClient(seed=1))
        use_case.run(symbols=["GLD"], full=True)
        use_case._clock = FrozenClock(NOW + timedelta(hours=1))
        use_case.run(symbols=["GLD"], full=True)

        keys = store.list(raw_prefix("stooq", "GLD"))
        combined = pl.concat([pl.read_parquet(store.get(k)) for k in keys])
        resolved = combined.sort("ingested_at").group_by(["symbol", "date"]).last()

        assert len(resolved) == combined.select(["symbol", "date"]).n_unique()
        assert len(resolved) < len(combined)


class TestFailureIsolation:
    def test_one_bad_instrument_does_not_stop_the_others(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        # The failure domain is the instrument (PROJECT.md §4.4).
        error = SymbolNotFound("unknown", source="stooq", symbol="gdx.us")
        use_case, _ = build(registry, ops, PerSymbolClient("gdx.us", error))
        outcome = use_case.run()

        assert any("GDX" in pair for pair in outcome.failed)
        assert outcome.rows > 0
        assert len(outcome.written) > 0

    def test_a_malformed_response_is_recorded_and_the_run_continues(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        error = MalformedResponse("bad header", source="stooq", symbol="gdx.us", payload=b"<html>")
        use_case, _ = build(registry, ops, PerSymbolClient("gdx.us", error))
        outcome = use_case.run()

        assert any("MalformedResponse" in reason for reason in outcome.failed.values())
        assert len(outcome.written) > 0

    def test_a_bad_credential_stops_the_whole_run(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        # The one failure no later instrument can work around.
        error = AuthenticationFailed("expired key", source="stooq")
        use_case, _ = build(registry, ops, FailingClient(error))
        with pytest.raises(AuthenticationFailed):
            use_case.run()

    def test_a_transient_outage_is_retried_then_succeeds(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        client = FailingClient(SourceUnavailable("503", source="stooq"), fail_times=2)
        use_case, _ = build(registry, ops, client)
        outcome = use_case.run(symbols=["GLD"])

        assert client.calls == 3  # two failures, then the success
        assert outcome.failed == {}

    def test_retries_are_bounded(self, registry: Registry, ops: SqliteOpsStore) -> None:
        client = FailingClient(SourceUnavailable("503", source="stooq"))
        use_case, _ = build(registry, ops, client)
        outcome = use_case.run(symbols=["GLD"])

        assert client.calls == 4  # the first try plus three retries
        assert outcome.failed


class TestRateLimiting:
    def test_a_rate_limit_abandons_the_source_rather_than_burning_the_budget(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        client = FailingClient(SourceRateLimited("capped", source="stooq"))
        use_case, _ = build(registry, ops, client)
        outcome = use_case.run()

        assert SourceKey.STOOQ in outcome.deferred_sources
        # One attempt, then every remaining Stooq symbol is skipped rather than
        # tried -- the cap is per vendor, so continuing spends the remaining
        # budget on refusals.
        assert client.calls == 1
        assert len(outcome.skipped) > 1
        # And the healthy vendor is unaffected: deferral is per source.
        assert {p.source for p in outcome.written} == {"twelvedata"}

    def test_a_rate_limit_is_never_retried_in_run(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        client = FailingClient(SourceRateLimited("capped", source="stooq"))
        use_case, _ = build(registry, ops, client)
        use_case.run(symbols=["GLD"])
        assert client.calls == 1

    def test_deferral_is_recorded_so_the_next_run_resumes_cleanly(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        client = FailingClient(SourceRateLimited("capped", source="stooq"))
        use_case, _ = build(registry, ops, client, deferral=timedelta(hours=6))
        use_case.run(symbols=["GLD"])

        mark = ops.watermark(SourceKey.STOOQ, "GLD")
        assert mark is not None
        assert mark.deferred_until == NOW + timedelta(hours=6)
        assert mark.is_deferred(NOW)
        assert not mark.is_deferred(NOW + timedelta(hours=7))

    def test_the_vendors_own_retry_after_wins_over_the_default(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        error = SourceRateLimited("slow down", source="stooq", retry_after=timedelta(minutes=30))
        use_case, _ = build(registry, ops, FailingClient(error), deferral=timedelta(hours=12))
        use_case.run(symbols=["GLD"])

        mark = ops.watermark(SourceKey.STOOQ, "GLD")
        assert mark is not None
        assert mark.deferred_until == NOW + timedelta(minutes=30)

    def test_a_deferred_pair_is_skipped_until_the_window_passes(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        ops.defer(SourceKey.STOOQ, "GLD", NOW + timedelta(hours=6))
        use_case, _ = build(registry, ops, SyntheticClient(seed=1))

        skipped = use_case.run(symbols=["GLD"])
        assert "stooq:GLD" in skipped.skipped

        use_case._clock = FrozenClock(NOW + timedelta(hours=7))
        resumed = use_case.run(symbols=["GLD"])
        assert any(p.source == "stooq" for p in resumed.written)


class TestIncrementalRuns:
    def test_a_second_run_refetches_only_from_the_watermark(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        use_case, store = build(registry, ops, SyntheticClient(seed=1))
        first = use_case.run(symbols=["GLD"])

        use_case._clock = FrozenClock(NOW + timedelta(days=1))
        second = use_case.run(symbols=["GLD"])

        assert second.rows < first.rows
        assert len(store.list(raw_prefix("stooq", "GLD"))) == 2

    def test_the_last_loaded_day_is_refetched_so_a_restatement_is_seen(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        # A vendor may restate the most recent bar; the raw zone is append-only,
        # so an overlapping row costs nothing and a missed restatement does.
        use_case, _ = build(registry, ops, SyntheticClient(seed=1))
        use_case.run(symbols=["GLD"])
        mark = ops.watermark(SourceKey.STOOQ, "GLD")
        assert mark is not None

        use_case._clock = FrozenClock(NOW + timedelta(days=1))
        second = use_case.run(symbols=["GLD"])
        assert second.rows >= 1

    def test_a_source_that_is_not_wired_is_skipped_not_fatal(
        self, registry: Registry, ops: SqliteOpsStore
    ) -> None:
        use_case = IngestUniverse(
            registry=registry,
            sources={},  # nothing wired at all
            object_store=InMemoryObjectStore(),
            ops_store=ops,
            clock=FrozenClock(NOW),
        )
        outcome = use_case.run(symbols=["GLD"])
        assert outcome.failed == {}
        assert "stooq:GLD" in outcome.skipped
