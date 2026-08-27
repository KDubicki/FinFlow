"""Data source identity.

A source key names a vendor the project actually implements. It is deliberately
a closed set: a registry entry pointing at a source with no client is a typo
that would otherwise surface as an empty series months later.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BeforeValidator


class SourceKey(StrEnum):
    """Vendors with a ``SourceClient`` implementation, or a plan to have one.

    Alpha Vantage and ``yfinance`` are described in ``PROJECT.md`` §6.1 but are
    deliberately absent: they are reachable through the same port if anyone ever
    writes them, and until then a registry entry naming one is an error.
    """

    STOOQ = "stooq"
    FRED = "fred"
    TWELVEDATA = "twelvedata"
    SYNTHETIC = "synthetic"


def _normalise(value: Any) -> Any:
    """Accept ``FRED`` as well as ``fred``.

    ``PROJECT.md`` §5.4 writes source names in upper case and §5.2 in lower; both
    read naturally in YAML, so neither is worth a validation error.
    """
    return value.strip().lower() if isinstance(value, str) else value


SourceKeyField = Annotated[SourceKey, BeforeValidator(_normalise)]
"""``SourceKey`` for use in a model, tolerant of the case used in the YAML."""
