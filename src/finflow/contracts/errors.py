"""The source error taxonomy.

Every ``SourceClient`` raises from this closed set and nothing else. Retry and
back-off belong to the *error class*, not to the vendor, so the policy lives in
``domain.retry`` and is applied once by the shared ingestion service. That is
what stops five clients growing five subtly different retry loops
(``PROJECT.md`` §6.7).

Clients raise. They do not retry.
"""

from __future__ import annotations

from datetime import timedelta


class FinFlowError(Exception):
    """Base for every error the platform raises deliberately."""


class SourceError(FinFlowError):
    """A vendor could not supply the data that was asked for.

    Carries the source and symbol so that a failure is attributable without the
    caller reconstructing context from a log line.
    """

    def __init__(self, message: str, *, source: str, symbol: str | None = None) -> None:
        location = f"{source}:{symbol}" if symbol else source
        super().__init__(f"[{location}] {message}")
        self.source = source
        self.symbol = symbol
        self.message = message


class SourceRateLimited(SourceError):
    """A quota or per-IP cap was hit.

    Never retried within a run. The whole source is abandoned, the remaining
    symbols are marked deferred, and the next run resumes from there.

    ``retry_after`` is honoured when the vendor supplies one; otherwise the
    ingestion service picks the deferral window.
    """

    def __init__(
        self,
        message: str,
        *,
        source: str,
        symbol: str | None = None,
        retry_after: timedelta | None = None,
    ) -> None:
        super().__init__(message, source=source, symbol=symbol)
        self.retry_after = retry_after


class SourceUnavailable(SourceError):
    """A 5xx, a timeout or a connection reset. Transient; worth retrying."""


class SymbolNotFound(SourceError):
    """The vendor does not know this symbol.

    A quality incident against the *registry entry*, not against the run: the
    run is fine, the configuration is wrong, and retrying cannot help.
    """


class MalformedResponse(SourceError):
    """The response parsed but failed the frame contract.

    The payload is carried so it can be written to quarantine and looked at,
    rather than discarded into a log message.
    """

    def __init__(
        self,
        message: str,
        *,
        source: str,
        symbol: str | None = None,
        payload: bytes | str | None = None,
    ) -> None:
        super().__init__(message, source=source, symbol=symbol)
        self.payload = payload


class AuthenticationFailed(SourceError):
    """A missing, bad or expired credential.

    Fails the run loudly. This is an operator problem and no amount of waiting
    fixes it, so degrading quietly would only delay the moment someone notices.
    """


class ObjectStoreError(FinFlowError):
    """Base for object-store failures."""


class ObjectAlreadyExists(ObjectStoreError):
    """A write-once key was written twice.

    The raw zone is append-only and is the one unrecoverable asset
    (``PROJECT.md`` §11.3), so overwriting is an error rather than a no-op.
    """

    def __init__(self, key: str) -> None:
        super().__init__(f"refusing to overwrite existing object: {key}")
        self.key = key


class ObjectNotFound(ObjectStoreError):
    """A key that was asked for is not in the store."""

    def __init__(self, key: str) -> None:
        super().__init__(f"no such object: {key}")
        self.key = key
