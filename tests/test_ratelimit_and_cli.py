"""The rate limiter, the write lock and the backfill entrypoint.

The bucket computes waits rather than performing them, so none of these tests
spends a real second. The lock tests matter because a manual backfill started
during the scheduled run is the realistic collision (``PROJECT.md`` §11.6).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pydantic import SecretStr

from finflow.config import Settings
from finflow.contracts.sources import SourceKey
from finflow.domain.ratelimit import TokenBucket
from finflow.entrypoints.cli.backfill import main
from finflow.entrypoints.cli.locking import ExclusiveLock, LockHeldError
from finflow.entrypoints.cli.wiring import build_sources

T0 = datetime(2026, 1, 1, tzinfo=UTC)


class TestTokenBucket:
    def test_a_full_bucket_allows_a_burst_without_waiting(self) -> None:
        bucket = TokenBucket(per_minute=60, capacity=3)
        assert [bucket.take(T0) for _ in range(3)] == [timedelta(0)] * 3

    def test_the_next_request_past_the_burst_must_wait(self) -> None:
        bucket = TokenBucket(per_minute=60, capacity=1)
        bucket.take(T0)
        assert bucket.take(T0) == timedelta(seconds=1)

    def test_tokens_refill_over_time(self) -> None:
        bucket = TokenBucket(per_minute=60, capacity=2)
        bucket.take(T0)
        bucket.take(T0)
        assert bucket.wait_for(T0 + timedelta(seconds=2)) == timedelta(0)

    def test_refill_is_capped_at_capacity(self) -> None:
        # An idle hour must not buy an unlimited burst; that is how a cap gets
        # hit on the first minute of a backfill.
        bucket = TokenBucket(per_minute=60, capacity=2)
        bucket.take(T0)
        assert bucket.wait_for(T0 + timedelta(hours=1)) == timedelta(0)
        assert bucket.tokens <= 2

    def test_a_slower_rate_means_a_longer_wait(self) -> None:
        bucket = TokenBucket(per_minute=6, capacity=1)
        bucket.take(T0)
        assert bucket.take(T0) == timedelta(seconds=10)

    def test_wait_for_does_not_consume(self) -> None:
        bucket = TokenBucket(per_minute=60, capacity=1)
        assert bucket.wait_for(T0) == timedelta(0)
        assert bucket.wait_for(T0) == timedelta(0)
        assert bucket.take(T0) == timedelta(0)

    def test_take_accounts_for_a_caller_that_ignores_the_delay(self) -> None:
        # Consuming unconditionally means an impatient caller is still counted
        # rather than silently exceeding the cap.
        bucket = TokenBucket(per_minute=60, capacity=1)
        bucket.take(T0)
        bucket.take(T0)
        assert bucket.take(T0) > timedelta(seconds=1)

    def test_a_non_positive_rate_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must be positive"):
            TokenBucket(per_minute=0)


class TestExclusiveLock:
    def test_a_second_acquirer_is_refused(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.lock"
        with ExclusiveLock(path), pytest.raises(LockHeldError), ExclusiveLock(path):
            pass

    def test_the_lock_is_reusable_after_release(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.lock"
        with ExclusiveLock(path):
            pass
        with ExclusiveLock(path):
            pass

    def test_the_lock_is_released_even_when_the_body_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "pipeline.lock"
        with pytest.raises(RuntimeError), ExclusiveLock(path):
            raise RuntimeError("boom")
        with ExclusiveLock(path):
            pass

    def test_the_directory_is_created_if_absent(self, tmp_path: Path) -> None:
        with ExclusiveLock(tmp_path / "nested" / "deeper" / "pipeline.lock"):
            pass


class TestWiring:
    def test_offline_maps_every_source_to_the_synthetic_generator(self, tmp_path: Path) -> None:
        sources = build_sources(Settings(_env_file=None), offline=True)  # type: ignore[call-arg]
        assert set(sources) >= {SourceKey.STOOQ, SourceKey.FRED}
        assert len({id(c) for c in sources.values()}) == 1

    def test_a_source_without_a_credential_is_omitted_not_built_broken(self) -> None:
        # A client that raises AuthenticationFailed on first use fails the whole
        # run; a missing key should mean "that source is unavailable" instead.
        sources = build_sources(Settings(_env_file=None, fred_api_key=None))  # type: ignore[call-arg]
        assert SourceKey.FRED not in sources
        assert SourceKey.STOOQ in sources

    def test_a_configured_credential_builds_the_source(self) -> None:
        sources = build_sources(Settings(_env_file=None, fred_api_key=SecretStr("k")))  # type: ignore[call-arg]
        assert SourceKey.FRED in sources


class TestBackfillCommand:
    def _settings(self, tmp_path: Path) -> Settings:
        return Settings(  # type: ignore[call-arg]
            _env_file=None,
            data_dir=tmp_path / "data",
            registry_dir=Path(__file__).resolve().parents[1] / "instruments",
        )

    def test_an_offline_run_succeeds_and_lands_data(self, tmp_path: Path) -> None:
        assert main(["--offline", "--symbols", "GLD"], settings=self._settings(tmp_path)) == 0
        assert list((tmp_path / "data" / "raw").rglob("*.parquet"))

    def test_a_manifest_is_written(self, tmp_path: Path) -> None:
        main(["--offline", "--symbols", "GLD"], settings=self._settings(tmp_path))
        assert list((tmp_path / "data" / "manifests").glob("*.json"))

    def test_a_missing_registry_exits_with_a_message_not_a_traceback(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        settings = Settings(  # type: ignore[call-arg]
            _env_file=None, data_dir=tmp_path / "data", registry_dir=tmp_path / "nowhere"
        )
        assert main(["--offline"], settings=settings) == 2
        assert "does not exist" in capsys.readouterr().err

    def test_a_second_run_exits_cleanly_while_the_lock_is_held(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Exiting rather than queueing: the scheduled run is already doing this
        # work, and it must never be half-written by a second writer.
        settings = self._settings(tmp_path)
        (tmp_path / "data").mkdir(parents=True, exist_ok=True)
        with ExclusiveLock(tmp_path / "data" / "pipeline.lock"):
            assert main(["--offline", "--symbols", "GLD"], settings=settings) == 3
        assert "another run holds the lock" in capsys.readouterr().err
