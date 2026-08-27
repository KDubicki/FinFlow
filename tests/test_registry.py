"""The registry: loading, validation, and date-effective membership.

Invalid-registry tests assert on the message, not just the exception type. An
error that fires for the wrong reason is a test that will keep passing after the
check it was written for has been deleted.
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from finflow.contracts.instruments import AssetClass, ReturnBasis
from finflow.contracts.sources import SourceKey
from finflow.registry import Registry, RegistryValidationError, load_registry

REPO_ROOT = Path(__file__).resolve().parents[1]
SHIPPED = REPO_ROOT / "instruments"

VALID_INSTRUMENT = """
instruments:
  - symbol: GLD
    name: SPDR Gold Shares
    asset_class: commodity
    exchange: ARCA
    calendar: XNYS
    inception: 2004-11-18
    backfill_start: 2004-12-01
    sources: { stooq: gld.us }
    costs: { commission_bps: 2, spread_bps: 2 }
"""


def write(directory: Path, **files: str) -> Path:
    """Write a throwaway registry and return its directory."""
    for name, body in files.items():
        (directory / f"{name}.yml").write_text(body, encoding="utf-8")
    return directory


# ---- The shipped registry -------------------------------------------------


@pytest.fixture(scope="module")
def shipped() -> Registry:
    return load_registry(SHIPPED)


def test_shipped_registry_loads(shipped: Registry) -> None:
    assert shipped.symbols == ("GLD", "IAU", "SLV", "GDX", "SPY", "QQQ", "TLT", "HYG")
    assert shipped.universe_names == ("precious_metals", "equity_core", "rates_credit")


def test_shipped_registry_is_all_price_return(shipped: Registry) -> None:
    # PROJECT.md §6.4: the MVP asserts price return everywhere.
    assert all(i.return_basis is ReturnBasis.PRICE for i in shipped.instruments)


def test_every_instrument_carries_a_cost_floor(shipped: Registry) -> None:
    # PROJECT.md §5.7: a strategy may raise these, never lower them, so an
    # instrument without one would silently get whatever a strategy asked for.
    assert all(i.costs.commission_bps > 0 for i in shipped.instruments)


def test_shipped_registry_loads_well_under_100ms() -> None:
    start = time.perf_counter()
    load_registry(SHIPPED)
    assert (time.perf_counter() - start) * 1000 < 100


def test_queries(shipped: Registry) -> None:
    assert len(shipped.enabled()) == 8
    assert [i.symbol for i in shipped.universe("equity_core")] == ["SPY", "QQQ"]
    assert shipped.sources_for("SPY")[SourceKey.STOOQ] == "spy.us"
    assert shipped.instrument("GLD").asset_class is AssetClass.COMMODITY
    assert shipped.macro_series("vix").source_id == "VIXCLS"


def test_unknown_lookups_name_what_is_available(shipped: Registry) -> None:
    with pytest.raises(RegistryValidationError, match=r"unknown instrument 'NOPE'.*GLD"):
        shipped.instrument("NOPE")
    with pytest.raises(RegistryValidationError, match=r"unknown universe 'nope'.*equity_core"):
        shipped.universe("nope")
    with pytest.raises(RegistryValidationError, match=r"unknown macro series 'nope'.*vix"):
        shipped.macro_series("nope")


# ---- Immutability ---------------------------------------------------------


def test_registry_is_immutable(shipped: Registry) -> None:
    with pytest.raises(ValidationError, match=r"frozen"):
        shipped.instruments = ()
    with pytest.raises(ValidationError, match=r"frozen"):
        shipped.instrument("GLD").enabled = False
    with pytest.raises(AttributeError):
        shipped.instruments.append(None)  # type: ignore[attr-defined]


def test_sources_cannot_be_mutated_through_a_query(shipped: Registry) -> None:
    sources = shipped.sources_for("SPY")
    with pytest.raises(TypeError):
        sources[SourceKey.STOOQ] = "hacked"  # type: ignore[index]
    assert shipped.sources_for("SPY")[SourceKey.STOOQ] == "spy.us"


# ---- Date-effective membership -------------------------------------------
#
# The eleven sector SPDRs are not in the shipped registry — they arrive with the
# full universe. The *mechanism* is what M1 owes, so it is tested against a
# fixture that exercises both date-effective members at once.

SECTORS = """
universes:
  sectors:
    description: The eleven SPDR sector funds
    members:
      - XLE
      - XLF
      - XLK
      - XLV
      - XLI
      - XLP
      - XLU
      - XLB
      - XLY
      - { symbol: XLRE, from: 2015-10-08 }
      - { symbol: XLC,  from: 2018-06-19 }
    benchmark: XLE
"""


def _sector_instruments() -> str:
    rows = "\n".join(
        f"""  - symbol: {sym}
    name: SPDR sector fund {sym}
    asset_class: equity
    exchange: ARCA
    calendar: XNYS
    inception: 1998-12-16
    backfill_start: 1999-01-01
    sources: {{ stooq: {sym.lower()}.us }}
    costs: {{ commission_bps: 2, spread_bps: 2 }}"""
        for sym in (
            "XLE XLF XLK XLV XLI XLP XLU XLB XLY XLRE XLC".split()  # noqa: SIM905
        )
    )
    return f"instruments:\n{rows}\n"


@pytest.fixture
def sectors(tmp_path: Path) -> Registry:
    return load_registry(write(tmp_path, funds=_sector_instruments(), universes=SECTORS))


@pytest.mark.parametrize(
    ("as_of", "expected"),
    [
        (date(2010, 1, 1), 9),  # before XLRE and XLC existed
        (date(2015, 10, 8), 10),  # XLRE listed, inclusive of its first day
        (date(2018, 6, 19), 11),  # XLC listed
        (None, 11),  # no date: every member that ever belonged
    ],
)
def test_membership_is_resolved_as_of_the_evaluation_date(
    sectors: Registry, as_of: date | None, expected: int
) -> None:
    assert len(sectors.universe("sectors", as_of=as_of)) == expected


def test_a_2010_backtest_does_not_see_XLRE(sectors: Registry) -> None:
    members = {i.symbol for i in sectors.universe("sectors", as_of=date(2010, 1, 1))}
    assert "XLRE" not in members
    assert "XLC" not in members
    assert "XLE" in members


def test_membership_may_end(tmp_path: Path) -> None:
    ended = SECTORS.replace("- XLE\n", "- { symbol: XLE, to: 2012-01-01 }\n")
    registry = load_registry(write(tmp_path, funds=_sector_instruments(), universes=ended))
    assert "XLE" in {i.symbol for i in registry.universe("sectors", as_of=date(2011, 12, 31))}
    assert "XLE" not in {i.symbol for i in registry.universe("sectors", as_of=date(2012, 1, 1))}


# ---- Validation: each failure for the right reason ------------------------


def test_duplicate_symbol_across_files_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(RegistryValidationError, match=r"duplicate symbol\(s\).*GLD"):
        load_registry(write(tmp_path, a=VALID_INSTRUMENT, b=VALID_INSTRUMENT))


def test_universe_member_must_exist(tmp_path: Path) -> None:
    universes = """
universes:
  metals:
    members: [GLD, IAU]
    benchmark: GLD
"""
    with pytest.raises(RegistryValidationError, match=r"'metals' references unknown.*IAU"):
        load_registry(write(tmp_path, a=VALID_INSTRUMENT, u=universes))


def test_universe_benchmark_must_exist(tmp_path: Path) -> None:
    universes = """
universes:
  metals:
    members: [GLD]
    benchmark: SPY
"""
    with pytest.raises(RegistryValidationError, match=r"'metals' has unknown benchmark 'SPY'"):
        load_registry(write(tmp_path, a=VALID_INSTRUMENT, u=universes))


def test_unknown_calendar_is_rejected(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT.replace("calendar: XNYS", "calendar: XMOON")
    with pytest.raises(RegistryValidationError, match=r"calendar 'XMOON' is not recognised"):
        load_registry(write(tmp_path, a=bad))


def test_backfill_before_inception_is_rejected(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT.replace("backfill_start: 2004-12-01", "backfill_start: 1999-01-01")
    with pytest.raises(RegistryValidationError, match=r"backfill_start.*precedes inception"):
        load_registry(write(tmp_path, a=bad))


def test_delisted_before_inception_is_rejected(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT + "    delisted: 2000-01-01\n"
    with pytest.raises(RegistryValidationError, match=r"delisted.*is not after inception"):
        load_registry(write(tmp_path, a=bad))


def test_unimplemented_source_is_rejected(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT.replace("{ stooq: gld.us }", "{ alphavantage: GLD }")
    with pytest.raises(RegistryValidationError, match=r"sources.*[Ii]nput should be"):
        load_registry(write(tmp_path, a=bad))


def test_source_key_case_is_tolerated(tmp_path: Path) -> None:
    upper = VALID_INSTRUMENT.replace("{ stooq: gld.us }", "{ STOOQ: gld.us }")
    registry = load_registry(write(tmp_path, a=upper))
    assert registry.sources_for("GLD")[SourceKey.STOOQ] == "gld.us"


def test_total_return_is_rejected_until_a_distributions_source_exists(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT + "    return_basis: total\n"
    with pytest.raises(RegistryValidationError, match=r"return_basis 'total' is not supported"):
        load_registry(write(tmp_path, a=bad))


def test_lowercase_symbol_is_rejected(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT.replace("symbol: GLD", "symbol: gld")
    with pytest.raises(RegistryValidationError, match=r"symbol.*pattern"):
        load_registry(write(tmp_path, a=bad))


def test_unknown_field_is_rejected(tmp_path: Path) -> None:
    bad = VALID_INSTRUMENT + "    liquidity: high\n"
    with pytest.raises(RegistryValidationError, match=r"liquidity.*not permitted"):
        load_registry(write(tmp_path, a=bad))


def test_unknown_top_level_key_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(RegistryValidationError, match=r"unknown top-level key\(s\) portfolios"):
        load_registry(write(tmp_path, a="portfolios:\n  - name: x\n"))


def test_vintage_aware_requires_revised(tmp_path: Path) -> None:
    bad = """
series:
  - id: cpi
    source: fred
    source_id: CPIAUCSL
    unit: index
    frequency: monthly
    release_lag_days: 14
    revised: false
    vintage_aware: true
"""
    with pytest.raises(RegistryValidationError, match=r"vintage_aware requires revised"):
        load_registry(write(tmp_path, m=bad))


def test_membership_window_must_be_ordered(tmp_path: Path) -> None:
    universes = """
universes:
  metals:
    members: [{ symbol: GLD, from: 2015-01-01, to: 2010-01-01 }]
    benchmark: GLD
"""
    with pytest.raises(RegistryValidationError, match=r"membership ends.*before it starts"):
        load_registry(write(tmp_path, a=VALID_INSTRUMENT, u=universes))


def test_repeated_universe_member_is_rejected(tmp_path: Path) -> None:
    universes = """
universes:
  metals:
    members: [GLD, GLD]
    benchmark: GLD
"""
    with pytest.raises(RegistryValidationError, match=r"repeated member\(s\) GLD"):
        load_registry(write(tmp_path, a=VALID_INSTRUMENT, u=universes))


def test_malformed_yaml_names_the_file(tmp_path: Path) -> None:
    with pytest.raises(RegistryValidationError, match=r"(?s)invalid YAML.*broken\.yml"):
        load_registry(write(tmp_path, broken="instruments: [\n"))


def test_missing_directory_is_reported(tmp_path: Path) -> None:
    with pytest.raises(RegistryValidationError, match=r"registry directory does not exist"):
        load_registry(tmp_path / "nowhere")


def test_empty_directory_is_reported(tmp_path: Path) -> None:
    with pytest.raises(RegistryValidationError, match=r"no \*\.yml files found"):
        load_registry(tmp_path)
