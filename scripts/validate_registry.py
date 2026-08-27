#!/usr/bin/env python
"""Validate instruments/*.yml and print what was loaded.

Run on every pull request. Adding an instrument is meant to be a one-file edit
with no code change (``PROJECT.md`` §5.5), which only holds if a malformed entry
is caught here rather than during a backfill three days later.

Validation is deliberately offline. Whether a vendor actually returns data for
the symbol is the nightly live-source job's question: a pull-request check that
depends on Stooq being up is a check that fails for reasons unrelated to the
pull request, and one that gets ignored shortly afterwards.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from finflow.registry import RegistryError, load_registry

REGISTRY_DIR = Path(__file__).resolve().parents[1] / "instruments"


def main() -> int:
    started = time.perf_counter()
    try:
        registry = load_registry(REGISTRY_DIR)
    except RegistryError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    elapsed_ms = (time.perf_counter() - started) * 1000

    live = registry.enabled()
    dirty = "  (dirty)" if registry.commit.dirty else ""
    print(f"Registry OK - loaded in {elapsed_ms:.0f} ms")
    print(f"  instruments   {len(registry.instruments)} ({len(live)} live)")
    print(f"  universes     {len(registry.universes)}")
    print(f"  macro series  {len(registry.macro)}")
    print(f"  commit        {registry.commit.sha or 'unversioned'}{dirty}")

    for universe in registry.universes:
        members = ", ".join(i.symbol for i in registry.universe(universe.name))
        print(f"  {universe.name:<16} [{members}]  benchmark={universe.benchmark}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
