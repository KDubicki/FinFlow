"""Token-bucket rate limiting.

Pure: the bucket computes *how long to wait*, and the caller does the waiting.
That keeps the sleep — and the clock — out of the domain layer, and it makes
the policy testable without any test spending real seconds.
"""

from __future__ import annotations

from datetime import datetime, timedelta

NO_WAIT = timedelta(0)


class TokenBucket:
    """Allows ``rate`` requests per minute, with a burst of ``capacity``.

    A bucket per source, because the caps are per vendor and a shared limiter
    would let a chatty source starve a quiet one.
    """

    def __init__(self, *, per_minute: float, capacity: int | None = None) -> None:
        if per_minute <= 0:
            raise ValueError("per_minute must be positive")
        self._refill_per_second = per_minute / 60.0
        self._capacity = float(capacity if capacity is not None else max(1, int(per_minute)))
        self._tokens = self._capacity
        self._last: datetime | None = None

    @property
    def tokens(self) -> float:
        """Tokens available as of the last observation, for tests and logging."""
        return self._tokens

    def _refill(self, now: datetime) -> None:
        if self._last is None:
            self._last = now
            return
        elapsed = (now - self._last).total_seconds()
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._refill_per_second)
            self._last = now

    def wait_for(self, now: datetime) -> timedelta:
        """How long to wait before one request may be made. Does not consume."""
        self._refill(now)
        if self._tokens >= 1:
            return NO_WAIT
        return timedelta(seconds=(1 - self._tokens) / self._refill_per_second)

    def take(self, now: datetime) -> timedelta:
        """Consume one token, returning how long the caller should have waited.

        Consumes unconditionally: a caller that honours the returned delay is
        correctly limited, and one that ignores it is still accounted for rather
        than silently exceeding the cap.
        """
        wait = self.wait_for(now)
        self._tokens -= 1
        return wait
