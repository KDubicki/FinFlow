"""The raw-zone key layout and the run manifest.

Both sides of the key convention live in one module precisely so they cannot
drift; these tests are what hold that promise. The manifest tests pin down the
property that motivated it: a snapshot id keyed on *data*, not on when the run
happened or what it was called.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from finflow.domain.layout import (
    RawPartition,
    manifest_key,
    parse_raw_key,
    raw_key,
    raw_prefix,
)
from finflow.domain.manifest import Manifest

AT = datetime(2026, 8, 27, 5, 12, tzinfo=UTC)


class TestRawKeys:
    def test_key_has_the_documented_shape(self) -> None:
        assert raw_key("stooq", "GLD", AT) == (
            "raw/source=stooq/symbol=GLD/ingested=20260827T051200000000Z/data.parquet"
        )

    def test_key_round_trips(self) -> None:
        partition = parse_raw_key(raw_key("fred", "DFII10", AT))
        assert partition == RawPartition("fred", "DFII10", AT)
        assert partition.key == raw_key("fred", "DFII10", AT)

    def test_keys_sort_chronologically(self) -> None:
        # Lexicographic order is chronological order, which is what makes
        # "the latest partition" a listing rather than a scan.
        keys = [raw_key("stooq", "GLD", AT + timedelta(hours=h)) for h in (2, 0, 1)]
        assert sorted(keys) == [
            raw_key("stooq", "GLD", AT),
            raw_key("stooq", "GLD", AT + timedelta(hours=1)),
            raw_key("stooq", "GLD", AT + timedelta(hours=2)),
        ]

    def test_a_naive_timestamp_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            raw_key("stooq", "GLD", datetime(2026, 8, 27, 5, 12))

    def test_a_non_utc_timestamp_is_normalised(self) -> None:
        other = AT.astimezone(tz=None).replace(tzinfo=None).replace(tzinfo=UTC)
        assert raw_key("stooq", "GLD", AT) == raw_key("stooq", "GLD", AT.astimezone(UTC))
        assert other is not None

    def test_separators_in_a_symbol_are_rejected(self) -> None:
        with pytest.raises(ValueError, match="may not contain"):
            raw_key("stooq", "GL/D", AT)

    @pytest.mark.parametrize(
        "key",
        [
            "raw/source=stooq/symbol=GLD/data.parquet",
            "raw/source=stooq/symbol=GLD/ingested=nonsense/data.parquet",
            "manifests/run.json",
            "raw/source=stooq/symbol=GLD/ingested=20260827T051200000000Z/other.parquet",
        ],
    )
    def test_a_non_partition_key_is_a_loud_failure(self, key: str) -> None:
        # A stray file in the raw zone must not become a mystery row later.
        with pytest.raises(ValueError, match="not a raw partition key"):
            parse_raw_key(key)

    def test_prefixes_narrow_progressively(self) -> None:
        assert raw_prefix() == "raw/"
        assert raw_prefix("stooq") == "raw/source=stooq/"
        assert raw_prefix("stooq", "GLD") == "raw/source=stooq/symbol=GLD/"
        assert raw_key("stooq", "GLD", AT).startswith(raw_prefix("stooq", "GLD"))

    def test_manifest_key(self) -> None:
        assert manifest_key("run-1") == "manifests/run-1.json"


class TestManifest:
    def _partitions(self) -> list[RawPartition]:
        return [
            RawPartition("stooq", "GLD", AT),
            RawPartition("stooq", "SPY", AT),
            RawPartition("fred", "DFII10", AT),
        ]

    def test_groups_partitions_by_source_and_symbol(self) -> None:
        manifest = Manifest.of("run-1", AT, self._partitions())
        assert set(manifest.entries) == {"stooq:GLD", "stooq:SPY", "fred:DFII10"}
        assert manifest.partition_count == 3

    def test_snapshot_id_ignores_run_identity_and_wall_clock(self) -> None:
        # Two runs that read the same data share a snapshot, which is what makes
        # "nothing changed today" expressible.
        first = Manifest.of("run-1", AT, self._partitions())
        second = Manifest.of("run-2", AT + timedelta(days=30), self._partitions())
        assert first.snapshot_id == second.snapshot_id

    def test_snapshot_id_ignores_the_order_partitions_arrived_in(self) -> None:
        partitions = self._partitions()
        assert (
            Manifest.of("r", AT, partitions).snapshot_id
            == Manifest.of("r", AT, list(reversed(partitions))).snapshot_id
        )

    def test_backfilling_one_instrument_changes_only_that_snapshot_input(self) -> None:
        # The bug that motivated hashing a manifest rather than max(ingested_at):
        # a GLD backfill must not imply every instrument changed.
        base = self._partitions()
        with_backfill = [*base, RawPartition("stooq", "GLD", AT + timedelta(days=1))]

        before = Manifest.of("r", AT, base)
        after = Manifest.of("r", AT, with_backfill)

        assert before.snapshot_id != after.snapshot_id
        assert before.entries["stooq:SPY"] == after.entries["stooq:SPY"]
        assert len(after.entries["stooq:GLD"]) == 2

    def test_an_empty_manifest_still_has_a_stable_id(self) -> None:
        assert Manifest.of("r", AT, []).snapshot_id == Manifest.of("other", AT, []).snapshot_id

    def test_serialises_with_the_snapshot_id_included(self) -> None:
        import json

        manifest = Manifest.of("run-1", AT, self._partitions(), registry_commit="abc123")
        payload = json.loads(manifest.to_json())

        assert payload["run_id"] == "run-1"
        assert payload["registry_commit"] == "abc123"
        assert payload["snapshot_id"] == manifest.snapshot_id
        assert payload["entries"]["fred:DFII10"] == [RawPartition("fred", "DFII10", AT).key]
