#!/usr/bin/env python
"""Fail if mart SQL uses DuckDB-specific syntax.

``PROJECT.md`` §10 claims the marts survive a lakehouse migration unchanged.
That claim is worth exactly as much as the check behind it, so DuckDB-specific
constructs are confined to staging and macros, and this makes the boundary a
build failure rather than a preference.

It is a grep, not a parser, and it is deliberately a small deny-list: the point
is to catch the constructs that would actually break on Spark or Trino, not to
prove portability.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

MARTS = Path(__file__).resolve().parents[1] / "dbt" / "models" / "marts"

# Each entry: (pattern, why it is not portable, what to do instead).
DENY = [
    (r"\bqualify\b", "QUALIFY is DuckDB/Snowflake-only", "use a row_number CTE"),
    (r"\bselect\s+\*\s+exclude\b", "SELECT * EXCLUDE is DuckDB-only", "list the columns"),
    (r"\bselect\s+\*\s+replace\b", "SELECT * REPLACE is DuckDB-only", "list the columns"),
    (r"\blist_\w+\(", "list_* functions are DuckDB-only", "use standard array functions"),
    (r"\bstruct_pack\(", "struct_pack is DuckDB-only", "use a standard struct literal"),
    (r"\bread_parquet\(", "read_parquet reads the local filesystem", "use a source()"),
    (r"\bread_csv(_auto)?\(", "read_csv reads the local filesystem", "use a source()"),
    (r"::\s*\w+", "the :: cast shorthand is not universal", "use cast(x as type)"),
]


def main() -> int:
    problems: list[str] = []
    for path in sorted(MARTS.rglob("*.sql")):
        text = path.read_text(encoding="utf-8")
        # Strip line comments so a rationale mentioning QUALIFY is not a hit.
        body = "\n".join(line.split("--")[0] for line in text.splitlines())
        for pattern, why, instead in DENY:
            if re.search(pattern, body, flags=re.IGNORECASE):
                problems.append(f"{path.relative_to(MARTS.parents[2])}: {why} — {instead}")

    if problems:
        print("Mart SQL must stay dialect-neutral (PROJECT.md §10):", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1

    checked = len(list(MARTS.rglob("*.sql")))
    print(f"Dialect neutrality OK — {checked} mart models checked against {len(DENY)} rules")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
