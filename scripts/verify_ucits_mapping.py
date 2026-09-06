#!/usr/bin/env python
"""Is the UCITS line actually a substitute for the US original?

The registry says "buy CSPX.UK instead of SPY". That claim is worth exactly as
much as the evidence behind it, and until this script runs there is none — two
funds tracking the same index can still differ by more than any edge this system
is looking for, through domicile, share class, currency line and fee.

Three measurements, per mapped pair (``PROJECT.md`` §5.7):

- **Return correlation** on overlapping sessions. Below ~0.95 and it is not a
  substitute, it is a different bet with a similar name.
- **Tracking difference**, annualized. The systematic drift. An *accumulating*
  UCITS line will drift upward against a distributing US line by roughly the
  dividend yield — that is not tracking error, it is the price-return bias of
  §6.4 showing up as a number, and it is the single most likely reason a pair
  looks broken when it is fine.
- **History length.** The UCITS lines are younger, often by a decade. A backtest
  over the research universe cannot be executed over the live one before the
  live one existed, and this is where that date comes from.

Reads the warehouse; makes no network calls of its own. Run it after a backfill
that included both sides of a pair:

    uv run python scripts/verify_ucits_mapping.py --data-dir ./data
"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from finflow.adapters.warehouse import DuckDBWarehouse
from finflow.registry import load_registry

REPO_ROOT = Path(__file__).resolve().parents[1]

MIN_CORRELATION = 0.95
"""Below this, the mapping is reported as unfit rather than merely noted. Two
funds on the same index correlate at 0.99+ on daily returns; 0.95 is a generous
floor that only a genuinely different exposure fails."""

MIN_OVERLAP_SESSIONS = 250
"""A year of common history. Any less and the correlation is an anecdote."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="verify-ucits", description=__doc__)
    parser.add_argument("--data-dir", type=Path, default=REPO_ROOT / "data")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when any mapping is unfit or unmeasurable. For CI.",
    )
    return parser


def returns(warehouse: DuckDBWarehouse, symbol: str) -> pl.DataFrame:
    """Daily log-ish simple returns for one symbol, in date order."""
    bars = warehouse.query(
        "SELECT date, close FROM fct_ohlcv_daily WHERE symbol = '" + symbol + "' ORDER BY date"
    )
    if bars.height < 2:
        return pl.DataFrame(schema={"date": pl.Date, "ret": pl.Float64})
    return bars.with_columns(
        (pl.col("close") / pl.col("close").shift(1) - 1).alias("ret")
    ).drop_nulls()


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    registry = load_registry(REPO_ROOT / "instruments")
    warehouse_path = args.data_dir / "warehouse.duckdb"
    if not warehouse_path.exists():
        print(f"no warehouse at {warehouse_path} — backfill and build first")
        return 1

    pairs = [
        (instrument.symbol, instrument.ucits_equivalent)
        for instrument in registry.instruments
        if instrument.ucits_equivalent is not None
    ]
    if not pairs:
        print("no UCITS mappings in the registry")
        return 0

    print(
        f"{'US line':<10} {'UCITS line':<12} {'sessions':>9} {'corr':>7} {'track/yr':>9}  verdict"
    )
    problems = 0

    with DuckDBWarehouse(warehouse_path, read_only=True) as warehouse:
        for us_symbol, ucits_symbol in pairs:
            assert ucits_symbol is not None
            us = returns(warehouse, us_symbol)
            eu = returns(warehouse, ucits_symbol)
            overlap = us.join(eu, on="date", how="inner", suffix="_eu")

            if overlap.height < MIN_OVERLAP_SESSIONS:
                # Not a failure of the mapping: a failure to have measured it.
                # Reported as such, because "unverified" and "wrong" call for
                # different responses.
                print(
                    f"{us_symbol:<10} {ucits_symbol:<12} {overlap.height:>9} "
                    f"{'-':>7} {'-':>9}  UNMEASURED (needs {MIN_OVERLAP_SESSIONS} common sessions)"
                )
                problems += 1
                continue

            correlation = overlap.select(pl.corr("ret", "ret_eu")).item()
            # Annualized difference in mean daily return. Positive means the
            # UCITS line outran the US one -- which for an accumulating share
            # class is expected, and is the dividend yield in disguise.
            drift = (overlap["ret_eu"].mean() - overlap["ret"].mean()) * 252 * 100

            verdict = (
                "ok" if correlation is not None and correlation >= MIN_CORRELATION else "UNFIT"
            )
            if verdict == "UNFIT":
                problems += 1
            print(
                f"{us_symbol:<10} {ucits_symbol:<12} {overlap.height:>9} "
                f"{correlation:>7.3f} {drift:>8.2f}%  {verdict}"
            )

    print()
    print(
        "Positive tracking difference on an accumulating line is the distribution "
        "yield, not a defect (PROJECT.md §6.4)."
    )
    if problems and args.strict:
        print(f"{problems} mapping(s) unfit or unmeasured", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
