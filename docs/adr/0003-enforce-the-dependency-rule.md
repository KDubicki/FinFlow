# 3. Enforce the dependency rule in CI

Date: 2026-08-27

## Status

Accepted

## Context

`PROJECT.md` §4.1 describes six layers with dependencies pointing strictly
inward. Every claim the project makes about itself rests on that arrangement
holding: that instruments are configuration, that the marts survive a lakehouse
migration, that backtest and live evaluation cannot drift, that a Dagster asset
is a thin wrapper rather than the logic itself.

An arrangement described only in a document is a description of the past. The
specific way this rots is well known and fast: under time pressure a use case
reaches for a vendor client because it is right there, a Dagster asset grows a
join because the data is already in scope, and within a few months the domain
layer cannot be tested without an orchestrator and cannot be migrated at all.
Nothing announces this happening. It is visible only as a slow rise in how hard
each change is.

The alternative to enforcement is code review, which for a solo project means
the same person who wrote the shortcut deciding whether the shortcut was fine.

## Decision

The layering is expressed as `import-linter` contracts in `.importlinter` and
run as a CI job named `imports`, before the test job.

Two contracts:

- **A layers contract.** Dependencies point strictly inward. `ports` and
  `registry` are declared as independent siblings, because §4.1 permits each to
  import `contracts` and `domain` but not the other. Layers that do not exist
  yet are parenthesised, so the contract holds before and after they appear
  without anyone editing the file to enable it.
- **A forbidden contract.** No package inward of the adapters may import
  `httpx`, `requests`, `urllib`, `duckdb`, `sqlite3`, `dagster`, `mlflow`,
  `telegram` or `boto3`.

A `forbidden` contract cannot name a module that does not exist yet, so its
source list must be extended by hand as packages appear. A rule that depends on
someone remembering is the rule this project exists to avoid, so
`tests/test_layering.py` fails the build when a new inner package is added and
the list is not extended.

Polars is deliberately not forbidden. §4.1 forbids `polars.io` in `domain`, not
Polars itself: `domain` compiles the strategy AST to Polars expressions and
`contracts` defines Patito frame schemas over it. import-linter cannot name a
subpackage of an external package, and the reader functions the rule was aimed
at hang off the top-level namespace anyway, so a contract here would be theatre.
What holds that line instead is that `domain` is handed frames rather than
paths, asserted by the purity test that lands with the backtest engine.

Time is treated the same way. `Clock` is a port; `SystemClock` is the only code
permitted to read the system clock; a pytest AST walk fails the build if any
package other than `adapters/` or `entrypoints/` calls `datetime.now`,
`date.today` or `time.time`. It is written as an exclusion rather than a list so
that it covers each new package automatically.

## Consequences

A violation is a build failure in seconds rather than a review comment, and the
`imports` job runs before the test suite so it fails fast.

The contracts are falsifiable and were falsified deliberately before being
trusted: `import duckdb` in `domain` breaks the forbidden contract, an adapter
import in `domain` breaks the layers contract, and an unlisted new package fails
the sync test. A guard nobody has watched fail is not yet a guard.

The cost is roughly forty lines of configuration and one test file, plus the
friction of occasionally being told that a convenient import is not allowed.
That friction is the entire point: it arrives at the moment the shortcut is
taken rather than six months later.

The rule is also what makes the Stage 4 migration a testable hypothesis rather
than a slogan. If A1 turns out to be an adapter swap, the boundaries were right;
if it is not, the contracts will have recorded exactly where they leaked.
