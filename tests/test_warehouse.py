"""The analytical store, the bronze loader, and the ops-store backup.

Three acceptance criteria are asserted here rather than demonstrated by hand,
because each one is a claim the project makes about itself: the warehouse is
disposable, a corrupt row cannot reach the marts, and the ops-store backup
actually restores.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import polars as pl
import pytest

from finflow.adapters.ops.backup import backup, latest_backup, prune, restore
from finflow.adapters.ops.sqlite import SqliteOpsStore
from finflow.adapters.storage import InMemoryObjectStore, LocalObjectStore
from finflow.adapters.warehouse import DuckDBWarehouse
from finflow.application.build_warehouse import BuildWarehouse
from finflow.contracts.sources import SourceKey
from finflow.domain.layout import raw_key
from finflow.ports.ops_store import PipelineRun, Watermark
from finflow.ports.warehouse import Warehouse
from finflow.registry import load_registry
from finflow.registry.models import Registry

NOW = dt.datetime(2026, 8, 27, 5, 12, tzinfo=dt.UTC)
REGISTRY_DIR = Path(__file__).resolve().parents[1] / "instruments"


@pytest.fixture
def registry() -> Registry:
    return load_registry(REGISTRY_DIR)


def bar(
    symbol: str, day: dt.date, close: float, run: str, ingested: dt.datetime, **kw: float
) -> dict[str, object]:
    row = {
        "symbol": symbol,
        "date": day,
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": 1_000.0,
        "source": "stooq",
        "ingested_at": ingested,
        "ingestion_run_id": run,
    }
    row.update(kw)
    return row


def land(
    store: InMemoryObjectStore, rows: list[dict[str, object]], *, ingested: dt.datetime
) -> None:
    import io as _io

    frame = pl.DataFrame(rows)
    buffer = _io.BytesIO()
    frame.write_parquet(buffer)
    store.put(raw_key("stooq", str(rows[0]["symbol"]), ingested), buffer.getvalue())


class TestSingleWriter:
    def test_a_second_writer_in_another_process_is_refused(self, tmp_path: Path) -> None:
        # The way this actually bites is the scheduled run dying because a UI
        # session left open overnight still holds the file (PROJECT.md §4.5) --
        # always another process, which is what DuckDB's lock covers. Two
        # connections inside one process share an instance instead, so this has
        # to be tested across a real process boundary or it tests nothing.
        import subprocess
        import sys
        import textwrap

        path = tmp_path / "w.duckdb"
        with DuckDBWarehouse(path) as warehouse:
            warehouse.execute("CREATE TABLE t AS SELECT 1 AS a")
            script = textwrap.dedent(f"""
                from finflow.adapters.warehouse import DuckDBWarehouse, WarehouseLockedError
                try:
                    DuckDBWarehouse({str(path)!r})
                except WarehouseLockedError as exc:
                    print("REFUSED", exc)
                else:
                    print("ACQUIRED")
            """)
            result = subprocess.run(
                [sys.executable, "-c", script], capture_output=True, text=True, check=True
            )

        assert result.stdout.startswith("REFUSED")
        assert "another process holds the lock" in result.stdout

    def test_a_reader_in_another_process_can_open_the_live_file(self, tmp_path: Path) -> None:
        import subprocess
        import sys
        import textwrap

        path = tmp_path / "w.duckdb"
        with DuckDBWarehouse(path) as warehouse:
            warehouse.execute("CREATE TABLE t AS SELECT 7 AS a")

        script = textwrap.dedent(f"""
            from finflow.adapters.warehouse import DuckDBWarehouse
            w = DuckDBWarehouse({str(path)!r}, read_only=True)
            print(w.query("SELECT a FROM t").to_dicts())
        """)
        result = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=True
        )
        assert "7" in result.stdout

    def test_the_lock_is_released_on_close(self, tmp_path: Path) -> None:
        with DuckDBWarehouse(tmp_path / "w.duckdb"):
            pass
        with DuckDBWarehouse(tmp_path / "w.duckdb"):
            pass

    def test_two_writers_against_the_ops_store_both_succeed(self, tmp_path: Path) -> None:
        # The whole reason for two stores: SQLite in WAL mode handles the small
        # concurrent transactional writes DuckDB's model is wrong for (§4.3).
        first = SqliteOpsStore(tmp_path / "ops.sqlite")
        second = SqliteOpsStore(tmp_path / "ops.sqlite")
        first.save_watermark(Watermark(SourceKey.STOOQ, "GLD", row_count=1))
        second.save_watermark(Watermark(SourceKey.FRED, "DFII10", row_count=2))
        assert len(first.watermarks()) == 2

    def test_the_warehouse_satisfies_its_port(self, tmp_path: Path) -> None:
        with DuckDBWarehouse(tmp_path / "w.duckdb") as warehouse:
            assert isinstance(warehouse, Warehouse)

    def test_a_missing_warehouse_is_not_silently_created_for_a_reader(self, tmp_path: Path) -> None:
        # A read-only open of a missing file yields an empty warehouse with no
        # explanation, which is worse than failing.
        with pytest.raises(FileNotFoundError):
            DuckDBWarehouse(tmp_path / "absent.duckdb", read_only=True)


class TestSnapshotPromotion:
    def test_a_snapshot_is_readable_while_the_writer_is_open(self, tmp_path: Path) -> None:
        with DuckDBWarehouse(tmp_path / "w.duckdb") as warehouse:
            warehouse.execute("CREATE TABLE t AS SELECT 42 AS a")
            warehouse.snapshot_to(tmp_path / "serving.duckdb")

            reader = DuckDBWarehouse(tmp_path / "serving.duckdb", read_only=True)
            assert reader.query("SELECT a FROM t").to_dicts() == [{"a": 42}]
            assert reader.is_read_only
            reader.close()

    def test_promotion_replaces_the_previous_snapshot_atomically(self, tmp_path: Path) -> None:
        with DuckDBWarehouse(tmp_path / "w.duckdb") as warehouse:
            warehouse.execute("CREATE TABLE t AS SELECT 1 AS a")
            warehouse.snapshot_to(tmp_path / "serving.duckdb")
            warehouse.execute("CREATE OR REPLACE TABLE t AS SELECT 2 AS a")
            warehouse.snapshot_to(tmp_path / "serving.duckdb")

        reader = DuckDBWarehouse(tmp_path / "serving.duckdb", read_only=True)
        assert reader.query("SELECT a FROM t").to_dicts() == [{"a": 2}]
        reader.close()
        assert not list(tmp_path.glob("*.staging"))


class TestBronzeLoader:
    def _build(
        self, tmp_path: Path, registry: Registry, store: InMemoryObjectStore | LocalObjectStore
    ) -> DuckDBWarehouse:
        warehouse = DuckDBWarehouse(tmp_path / "w.duckdb")
        BuildWarehouse(object_store=store, warehouse=warehouse, registry=registry).run(
            snapshot_id="snap"
        )
        return warehouse

    def test_the_latest_reading_wins(self, tmp_path: Path, registry: Registry) -> None:
        store = InMemoryObjectStore()
        land(store, [bar("GLD", dt.date(2024, 1, 2), 190.0, "r1", NOW)], ingested=NOW)
        later = NOW + dt.timedelta(days=1)
        land(store, [bar("GLD", dt.date(2024, 1, 2), 195.0, "r2", later)], ingested=later)

        warehouse = self._build(tmp_path, registry, store)
        rows = warehouse.query("SELECT close FROM bronze_ohlcv").to_dicts()
        warehouse.close()
        assert rows == [{"close": 195.0}]

    def test_a_changed_value_is_recorded_as_a_restatement(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        # Absorbing it silently makes a backtest irreproducible and hides a
        # corporate action (PROJECT.md §6.2).
        store = InMemoryObjectStore()
        land(store, [bar("GLD", dt.date(2024, 1, 2), 190.0, "r1", NOW)], ingested=NOW)
        later = NOW + dt.timedelta(days=1)
        land(store, [bar("GLD", dt.date(2024, 1, 2), 195.0, "r2", later)], ingested=later)

        warehouse = self._build(tmp_path, registry, store)
        rows = warehouse.query("SELECT * FROM dq_restatements").to_dicts()
        warehouse.close()
        assert len(rows) == 1
        assert rows[0]["old_run_id"] == "r1"
        assert rows[0]["new_run_id"] == "r2"

    def test_an_unchanged_reread_is_not_a_restatement(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        # Every incremental run deliberately re-fetches the last loaded day, so
        # treating an agreeing re-read as a restatement would cry wolf daily.
        store = InMemoryObjectStore()
        land(store, [bar("GLD", dt.date(2024, 1, 2), 190.0, "r1", NOW)], ingested=NOW)
        later = NOW + dt.timedelta(days=1)
        land(store, [bar("GLD", dt.date(2024, 1, 2), 190.0, "r2", later)], ingested=later)

        warehouse = self._build(tmp_path, registry, store)
        count = warehouse.query("SELECT count(*) AS n FROM dq_restatements").to_dicts()[0]["n"]
        warehouse.close()
        assert count == 0

    def test_a_corrupt_row_lands_in_quarantine_not_in_bronze(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        store = InMemoryObjectStore()
        land(
            store,
            [
                bar("GLD", dt.date(2024, 1, 2), 190.0, "r1", NOW),
                bar("GLD", dt.date(2024, 1, 3), 190.0, "r1", NOW, high=1.0, low=500.0),
            ],
            ingested=NOW,
        )
        warehouse = self._build(tmp_path, registry, store)
        good = warehouse.query("SELECT date FROM bronze_ohlcv").to_dicts()
        bad = warehouse.query("SELECT date, rejection_reason FROM bronze_quarantine").to_dicts()
        warehouse.close()

        assert good == [{"date": dt.date(2024, 1, 2)}]
        assert len(bad) == 1
        assert bad[0]["rejection_reason"] == "high < low"

    def test_readings_from_two_vendors_do_not_overwrite_each_other(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        # Source belongs in the bronze key: choosing between vendors is a mart
        # decision, and collapsing them here made every row look restated.
        store = InMemoryObjectStore()
        land(store, [bar("GLD", dt.date(2024, 1, 2), 190.0, "r1", NOW)], ingested=NOW)
        import io as _io

        frame = pl.DataFrame(
            [bar("GLD", dt.date(2024, 1, 2), 191.0, "r1", NOW, **{})]
        ).with_columns(pl.lit("twelvedata").alias("source"))
        buf = _io.BytesIO()
        frame.write_parquet(buf)
        store.put(raw_key("twelvedata", "GLD", NOW), buf.getvalue())

        warehouse = self._build(tmp_path, registry, store)
        rows = warehouse.query("SELECT source, close FROM bronze_ohlcv ORDER BY source").to_dicts()
        restatements = warehouse.query("SELECT count(*) AS n FROM dq_restatements").to_dicts()
        warehouse.close()

        assert len(rows) == 2
        assert restatements[0]["n"] == 0

    def test_the_registry_is_projected_into_the_warehouse(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        warehouse = self._build(tmp_path, registry, InMemoryObjectStore())
        symbols = warehouse.query(
            "SELECT symbol FROM registry_instruments ORDER BY symbol"
        ).to_dicts()
        warehouse.close()
        assert [r["symbol"] for r in symbols] == sorted(registry.symbols)

    def test_a_rebuild_from_raw_reproduces_the_bronze_layer(
        self, tmp_path: Path, registry: Registry
    ) -> None:
        # "The warehouse is disposable" is either true or it is a slogan.
        store = LocalObjectStore(tmp_path / "raw")
        import io as _io

        frame = pl.DataFrame(
            [bar("GLD", dt.date(2024, 1, d), 190.0 + d, "r1", NOW) for d in (2, 3)]
        )
        buf = _io.BytesIO()
        frame.write_parquet(buf)
        store.put(raw_key("stooq", "GLD", NOW), buf.getvalue())

        first = self._build(tmp_path, registry, store)
        before = first.query("SELECT * FROM bronze_ohlcv ORDER BY date").to_dicts()
        first.close()

        (tmp_path / "w.duckdb").unlink()
        second = self._build(tmp_path, registry, store)
        after = second.query("SELECT * FROM bronze_ohlcv ORDER BY date").to_dicts()
        second.close()

        assert before == after


class TestOpsBackup:
    def _store(self, tmp_path: Path) -> SqliteOpsStore:
        store = SqliteOpsStore(tmp_path / "ops.sqlite")
        store.save_watermark(Watermark(SourceKey.STOOQ, "GLD", dt.date(2024, 1, 2), NOW, 100))
        store.save_run(PipelineRun("r1", NOW, NOW, "succeeded", 100, "snap"))
        return store

    def test_a_restored_backup_preserves_watermarks_and_runs(self, tmp_path: Path) -> None:
        self._store(tmp_path)
        backup(tmp_path / "ops.sqlite", tmp_path / "backups", now=NOW)
        (tmp_path / "ops.sqlite").unlink()

        archive = latest_backup(tmp_path / "backups")
        assert archive is not None
        restore(archive, tmp_path / "ops.sqlite")

        restored = SqliteOpsStore(tmp_path / "ops.sqlite")
        mark = restored.watermark(SourceKey.STOOQ, "GLD")
        assert mark is not None
        assert mark.last_loaded_date == dt.date(2024, 1, 2)
        assert mark.row_count == 100
        last = restored.last_successful_run()
        assert last is not None
        assert last.run_id == "r1"

    def test_a_corrupt_backup_fails_before_overwriting_the_live_store(self, tmp_path: Path) -> None:
        # Otherwise a bad restore turns a recoverable incident into an
        # unrecoverable one.
        self._store(tmp_path)
        corrupt = tmp_path / "backups" / "ops-20260101T000000Z.sqlite"
        corrupt.parent.mkdir(parents=True)
        corrupt.write_bytes(b"not a database at all")

        with pytest.raises(Exception, match=r"integrity|database|migrations"):
            restore(corrupt, tmp_path / "ops.sqlite")

        surviving = SqliteOpsStore(tmp_path / "ops.sqlite")
        assert surviving.watermark(SourceKey.STOOQ, "GLD") is not None

    def test_an_empty_database_is_rejected_as_a_backup(self, tmp_path: Path) -> None:
        import sqlite3

        empty = tmp_path / "empty.sqlite"
        sqlite3.connect(empty).close()
        with pytest.raises(ValueError, match="no applied migrations"):
            restore(empty, tmp_path / "ops.sqlite")

    def test_backing_up_a_missing_store_fails_loudly(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            backup(tmp_path / "nothing.sqlite", tmp_path / "backups", now=NOW)

    def test_pruning_keeps_the_most_recent(self, tmp_path: Path) -> None:
        self._store(tmp_path)
        for day in range(1, 6):
            backup(
                tmp_path / "ops.sqlite",
                tmp_path / "backups",
                now=NOW + dt.timedelta(days=day),
            )
        removed = prune(tmp_path / "backups", keep_daily=2)
        assert len(removed) == 3
        assert len(list((tmp_path / "backups").glob("ops-*"))) == 2

    def test_latest_backup_is_none_when_there_are_none(self, tmp_path: Path) -> None:
        assert latest_backup(tmp_path / "nowhere") is None


class TestMigrations:
    def test_migrations_are_applied_once_and_are_idempotent(self, tmp_path: Path) -> None:
        first = SqliteOpsStore(tmp_path / "ops.sqlite")
        assert first.schema_version == 2
        second = SqliteOpsStore(tmp_path / "ops.sqlite")
        assert second.schema_version == 2

    def test_an_existing_database_gains_new_tables(self, tmp_path: Path) -> None:
        import sqlite3

        from finflow.adapters.ops.migrations import migrate

        path = tmp_path / "ops.sqlite"
        conn = sqlite3.connect(path)
        applied = migrate(conn)
        conn.close()
        assert applied == [1, 2]

        conn = sqlite3.connect(path)
        assert migrate(conn) == []
        conn.close()
