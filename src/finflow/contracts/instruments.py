"""Shared instrument vocabulary.

These enums are in ``contracts`` rather than ``registry`` because the warehouse
models, the feature frames and the cost model all have to agree on them.
"""

from __future__ import annotations

from enum import StrEnum


class AssetClass(StrEnum):
    """Top-level classification, used for cost and baseline defaults."""

    EQUITY = "equity"
    COMMODITY = "commodity"
    RATES = "rates"
    CREDIT = "credit"
    CURRENCY = "currency"


class ReturnBasis(StrEnum):
    """Whether a price series includes distributions.

    The MVP is ``PRICE`` everywhere and says so on every report
    (``PROJECT.md`` §6.4). ``TOTAL`` exists in the schema from the start so that
    a distributions feed is additive rather than a migration.
    """

    PRICE = "price"
    TOTAL = "total"


class Frequency(StrEnum):
    """Native publication frequency of a series."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
