"""The retry policy table.

The policies matter more than they look: they are the difference between a rate
limit costing one deferred symbol and a rate limit getting the IP blocked. So
each row of ``PROJECT.md`` §6.7 is asserted rather than assumed, and a test
fails if a new error class is added without a policy.
"""

from __future__ import annotations

import pytest

from finflow.contracts.errors import (
    AuthenticationFailed,
    MalformedResponse,
    SourceError,
    SourceRateLimited,
    SourceUnavailable,
    SymbolNotFound,
)
from finflow.domain.retry import known_policies, policy_for


def test_a_rate_limit_is_never_retried_in_run() -> None:
    # Retrying a cap is how a soft block becomes a hard one.
    policy = policy_for(SourceRateLimited)
    assert policy.attempts == 0
    assert policy.defer_source


def test_an_outage_backs_off_with_jitter() -> None:
    policy = policy_for(SourceUnavailable)
    assert policy.attempts == 3
    assert policy.jitter
    assert [policy.backoff_seconds(n) for n in (1, 2, 3)] == [1.0, 2.0, 4.0]


def test_an_unknown_symbol_blames_the_registry_not_the_run() -> None:
    policy = policy_for(SymbolNotFound)
    assert policy.attempts == 0
    assert policy.registry_incident
    assert not policy.fail_run


def test_a_malformed_response_is_kept_for_inspection() -> None:
    policy = policy_for(MalformedResponse)
    assert policy.attempts == 0
    assert policy.quarantine


def test_a_bad_credential_fails_the_run_loudly() -> None:
    # No amount of waiting fixes an expired key, and degrading quietly would
    # only delay the moment someone notices.
    policy = policy_for(AuthenticationFailed)
    assert policy.fail_run
    assert policy.attempts == 0


def test_policy_accepts_an_instance_as_well_as_a_class() -> None:
    error = SourceRateLimited("capped", source="stooq", symbol="GLD")
    assert policy_for(error) == policy_for(SourceRateLimited)


def test_a_subclass_inherits_its_parents_policy() -> None:
    class VendorSpecificOutage(SourceUnavailable):
        pass

    assert policy_for(VendorSpecificOutage) == policy_for(SourceUnavailable)


def test_an_unknown_error_is_treated_as_malformed_never_as_transient() -> None:
    # Guessing that an unrecognised failure is transient is how a pipeline
    # hammers a vendor over a permanent problem.
    class Surprise(SourceError):
        pass

    policy = policy_for(Surprise)
    assert policy.attempts == 0
    assert policy.quarantine


@pytest.mark.parametrize(
    "error_class",
    [
        SourceRateLimited,
        SourceUnavailable,
        SymbolNotFound,
        MalformedResponse,
        AuthenticationFailed,
    ],
)
def test_every_taxonomy_member_has_an_explicit_policy(
    error_class: type[SourceError],
) -> None:
    assert error_class in known_policies(), (
        f"{error_class.__name__} has no policy; it would silently fall back to "
        f"quarantine, which may not be what you want"
    )


def test_only_one_policy_fails_the_whole_run() -> None:
    # A failure domain is the instrument partition (PROJECT.md §4.4), so a
    # second run-killing policy would be a design change, not a tweak.
    failing = [k.__name__ for k, v in known_policies().items() if v.fail_run]
    assert failing == ["AuthenticationFailed"]


class TestErrorMessages:
    """Failures have to be attributable without reconstructing context."""

    def test_source_and_symbol_appear_in_the_message(self) -> None:
        error = SourceRateLimited("daily cap hit", source="stooq", symbol="GLD")
        assert str(error) == "[stooq:GLD] daily cap hit"
        assert error.source == "stooq"
        assert error.symbol == "GLD"

    def test_a_source_level_failure_needs_no_symbol(self) -> None:
        assert str(SourceUnavailable("503", source="fred")) == "[fred] 503"

    def test_a_rate_limit_can_carry_the_vendors_own_retry_after(self) -> None:
        from datetime import timedelta

        error = SourceRateLimited("slow down", source="twelvedata", retry_after=timedelta(60))
        assert error.retry_after == timedelta(60)

    def test_a_malformed_response_carries_the_payload(self) -> None:
        error = MalformedResponse("not csv", source="stooq", symbol="GLD", payload=b"<html>")
        assert error.payload == b"<html>"
