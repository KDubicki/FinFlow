"""Conformance suite for the ``ObjectStore`` port.

One parametrized class run against every implementation, so a new store proves
itself against the same contract rather than against its own assumptions. This
is the test that makes "swap the raw zone for R2" a configuration change.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from finflow.adapters.storage import InMemoryObjectStore, LocalObjectStore, S3ObjectStore
from finflow.contracts.errors import ObjectAlreadyExists, ObjectNotFound
from finflow.ports.object_store import ObjectStore

BUCKET = "finflow-test"


@pytest.fixture
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[ObjectStore]:
    """Yield each implementation in turn, configured identically."""
    kind = request.param
    if kind == "memory":
        yield InMemoryObjectStore()
    elif kind == "local":
        yield LocalObjectStore(tmp_path / "raw")
    elif kind == "s3":
        moto = pytest.importorskip("moto")
        with moto.mock_aws():
            import boto3

            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket=BUCKET)
            yield S3ObjectStore(client, BUCKET)
    else:  # pragma: no cover - guarded by the parametrisation
        raise AssertionError(kind)


pytestmark = pytest.mark.parametrize(
    "store", ["memory", "local", "s3"], indirect=True, ids=["memory", "local", "s3"]
)


class TestObjectStoreContract:
    """Every implementation must honour all of this."""

    def test_satisfies_the_port(self, store: ObjectStore) -> None:
        assert isinstance(store, ObjectStore)

    def test_round_trips_bytes(self, store: ObjectStore) -> None:
        store.put("raw/a.parquet", b"\x00\x01binary\xff")
        assert store.get("raw/a.parquet") == b"\x00\x01binary\xff"

    def test_exists_reflects_writes(self, store: ObjectStore) -> None:
        assert not store.exists("raw/a.parquet")
        store.put("raw/a.parquet", b"x")
        assert store.exists("raw/a.parquet")

    def test_writes_are_once_only(self, store: ObjectStore) -> None:
        # The raw zone is the one unrecoverable asset, so overwriting is an
        # error rather than a no-op (PROJECT.md §11.3).
        store.put("raw/a.parquet", b"first")
        with pytest.raises(ObjectAlreadyExists, match="refusing to overwrite"):
            store.put("raw/a.parquet", b"second")
        assert store.get("raw/a.parquet") == b"first"

    def test_missing_key_raises(self, store: ObjectStore) -> None:
        with pytest.raises(ObjectNotFound, match="no such object"):
            store.get("raw/absent.parquet")

    def test_listing_is_lexicographically_ordered(self, store: ObjectStore) -> None:
        for key in ("raw/c", "raw/a", "raw/b"):
            store.put(key, b"x")
        assert store.list("raw/") == ("raw/a", "raw/b", "raw/c")

    def test_listing_filters_by_prefix(self, store: ObjectStore) -> None:
        store.put("raw/source=stooq/x", b"x")
        store.put("raw/source=fred/y", b"y")
        store.put("manifests/run.json", b"{}")
        assert store.list("raw/source=stooq/") == ("raw/source=stooq/x",)
        assert store.list("manifests/") == ("manifests/run.json",)

    def test_empty_listing_is_empty(self, store: ObjectStore) -> None:
        assert store.list("nothing/") == ()

    def test_nested_keys_round_trip(self, store: ObjectStore) -> None:
        key = "raw/source=stooq/symbol=GLD/ingested=20260827T051200000000Z/data.parquet"
        store.put(key, b"parquet")
        assert store.get(key) == b"parquet"
        assert store.list("raw/") == (key,)

    def test_the_port_has_no_delete(self, store: ObjectStore) -> None:
        # Not an oversight. The port refuses to express the operation that
        # would lose the raw zone, and the real credential cannot perform one.
        for forbidden in ("delete", "remove", "unlink", "pop"):
            assert not hasattr(store, forbidden)


class TestLocalStoreSpecifics:
    """Behaviour only the filesystem store can get wrong."""

    def test_rejects_absolute_and_traversing_keys(self, store: ObjectStore, tmp_path: Path) -> None:
        if not isinstance(store, LocalObjectStore):
            pytest.skip("filesystem-specific")
        for key in ("/etc/passwd", "../escape", "raw/../../escape"):
            with pytest.raises(ValueError, match=r"may not traverse|must be relative"):
                store.put(key, b"x")

    def test_leaves_no_partial_files_behind(self, store: ObjectStore) -> None:
        if not isinstance(store, LocalObjectStore):
            pytest.skip("filesystem-specific")
        store.put("raw/a.parquet", b"x")
        leftovers = list(store.root.rglob("*.partial"))
        assert not leftovers, f"staging files not cleaned up: {leftovers}"

    def test_a_partial_file_is_never_listed(self, store: ObjectStore) -> None:
        if not isinstance(store, LocalObjectStore):
            pytest.skip("filesystem-specific")
        store.put("raw/a.parquet", b"x")
        (store.root / "raw" / "b.partial").write_bytes(b"half")
        assert store.list("raw/") == ("raw/a.parquet",)


class TestS3Specifics:
    """Behaviour only the bucket-backed store can get wrong."""

    def test_a_prefix_scopes_the_store(self, store: ObjectStore) -> None:
        if not isinstance(store, S3ObjectStore):
            pytest.skip("bucket-specific")
        # Keys are stored under the prefix but never exposed with it, so the
        # same code path works whether or not a bucket is shared.
        scoped: Any = S3ObjectStore(store._client, BUCKET, prefix="finflow")
        scoped.put("raw/a", b"x")
        assert scoped.list("raw/") == ("raw/a",)
        assert store.list("finflow/") == ("finflow/raw/a",)

    def test_listing_pages_beyond_one_thousand_keys(self, store: ObjectStore) -> None:
        if not isinstance(store, S3ObjectStore):
            pytest.skip("bucket-specific")
        # S3 truncates a listing at 1000; the paginator is what makes a full
        # backfill's worth of partitions enumerable.
        for i in range(1005):
            store.put(f"raw/{i:05d}", b"x")
        assert len(store.list("raw/")) == 1005
