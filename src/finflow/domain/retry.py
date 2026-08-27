"""What to do about each class of source failure.

The policy table is domain knowledge, not adapter knowledge: it says what a
rate limit *means*, independently of which vendor produced it. The ingestion
service reads this table; clients never see it (``PROJECT.md`` §6.7).
"""

from __future__ import annotations

from dataclasses import dataclass

from finflow.contracts.errors import (
    AuthenticationFailed,
    MalformedResponse,
    SourceError,
    SourceRateLimited,
    SourceUnavailable,
    SymbolNotFound,
)


@dataclass(frozen=True)
class RetryPolicy:
    """How the ingestion service reacts to one class of failure."""

    attempts: int
    """Retries *after* the first try. Zero means the first failure is final."""

    backoff_base_seconds: float = 0.0
    """Base for exponential back-off; the nth retry waits base * 2**n."""

    jitter: bool = False
    """Spread retries so a recovering vendor is not hit by a synchronised wave."""

    defer_source: bool = False
    """Abandon the whole source and defer its remaining symbols to the next run."""

    quarantine: bool = False
    """Capture the payload for inspection rather than discarding it."""

    registry_incident: bool = False
    """Blame the registry entry rather than the run — the config is wrong."""

    fail_run: bool = False
    """Stop the run. Reserved for problems no later instrument can work around."""

    def backoff_seconds(self, attempt: int) -> float:
        """Delay before retry number ``attempt`` (1-based), before jitter."""
        return self.backoff_base_seconds * float(2 ** (attempt - 1))


# PROJECT.md §6.7, one row per error class.
_POLICIES: dict[type[SourceError], RetryPolicy] = {
    SourceRateLimited: RetryPolicy(attempts=0, defer_source=True),
    SourceUnavailable: RetryPolicy(attempts=3, backoff_base_seconds=1.0, jitter=True),
    SymbolNotFound: RetryPolicy(attempts=0, registry_incident=True),
    MalformedResponse: RetryPolicy(attempts=0, quarantine=True),
    AuthenticationFailed: RetryPolicy(attempts=0, fail_run=True),
}

_UNKNOWN = RetryPolicy(attempts=0, quarantine=True)
"""An error outside the taxonomy is treated as malformed: no retry, keep the
evidence. Guessing that an unrecognised failure is transient is how a pipeline
hammers a vendor over a permanent problem."""


def policy_for(error: SourceError | type[SourceError]) -> RetryPolicy:
    """Return the policy for an error class, or for an instance of one."""
    kind = error if isinstance(error, type) else type(error)
    for candidate in kind.__mro__:
        if candidate in _POLICIES:
            return _POLICIES[candidate]
    return _UNKNOWN


def known_policies() -> dict[type[SourceError], RetryPolicy]:
    """The whole table, for tests that assert every error class is covered."""
    return dict(_POLICIES)
