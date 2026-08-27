# FinFlow — Multi-Asset ETF Data Platform

**Design document**
Status: Approved — MVP is the Lean Warehouse, evolving into the Lakehouse

---

## 1. What this is

FinFlow is a data platform for exchange-traded funds across asset classes. It ingests price and
macro data for an extensible universe of ETFs, models it into a queryable warehouse, computes
predictive features, and lets a user define a trading strategy, backtest it against history, run it
live, and receive an alert on Telegram when it fires.

### 1.1 Who it is for, and the job it does

One user: its author, managing their own long-term savings from a Polish brokerage account. Not a
team, not a customer, not a hypothetical.

The job, stated as the user would state it:

> **Every morning, tell me whether my portfolio should change — clearly enough that I can act
> without re-checking your work, and honestly enough that you tell me when you cannot be trusted.**

Every design decision in this document is answerable to that sentence. The system runs unattended
and pushes to Telegram because a tool that requires opening a laptop before breakfast does not get
used. It refuses to emit a signal on unvalidated data because a wrong instruction is worse than no
instruction. It reports its own uncertainty because the failure mode that actually costs money is
not a missing alert — it is a confident one that should not have been sent.

The corollary, which shapes the roadmap more than anything else: **most days the honest answer is
"do nothing", and the product has to be good at saying that.** A daily message that manufactures
activity to justify itself gets muted within a fortnight, and a muted system is worth nothing.

### 1.2 What "working" looks like

Success criteria that can be checked rather than felt:

| | Target |
|---|---|
| Digest arrives on every trading day | 100% — a missing one is an incident, not an inconvenience |
| Days requiring no action | ≥ 95%. Below that, the strategies are too twitchy or the bands too tight |
| Decisions acted on within a day of arriving | ≥ 90% — if they are ignored, they are not trusted |
| Instructions issued on stale or unvalidated data | **zero**, and this is the one number with no tolerance |
| Time from strategy idea to backtest with deflated statistics | < 30 minutes |
| Time to add an instrument | one file, no code, < 10 minutes |

### 1.3 What this is not

Naming the non-goals early, because each is a thing the system could plausibly grow into and none of
them serves the job above:

- **Not a broker.** It never places an order and holds no credentials that could. The user executes.
- **Not a research notebook.** Ad-hoc exploration is a valid activity and belongs somewhere else;
  every path through this system is a repeatable one.
- **Not multi-user.** No accounts, no tenancy, no sharing. One person, one portfolio.
- **Not real-time.** Daily bars, one decision a day. Intraday is an explicit non-goal for the
  foreseeable version and a large source of avoided complexity.
- **Not a source of financial advice**, and no component should be used to trade money the user
  cannot afford to lose.

The platform is **instrument-agnostic by design**. Adding a new ETF is a configuration change — one
entry in a registry file — not a code change. The initial universe emphasises precious metals, but
nothing in the architecture is specific to gold.

Four capabilities on one platform:

1. **Collect** — daily OHLCV for every enabled instrument, plus the macro series that drive them:
   real yields, the dollar index, credit spreads, VIX, CPI.
2. **Predict** — walk-forward models for forward return *direction* and realized *volatility*,
   trained per instrument and cross-sectionally across a universe, evaluated honestly.
3. **Simulate** — strategies declared in YAML, single-instrument or cross-sectional, backtested
   with realistic costs and slippage, always compared against a benchmark.
4. **Alert** — the same strategy definition runs live; when it triggers, a Telegram bot sends
   the signal.

> **Educational project.** Nothing here is financial advice, and no component should be used to
> trade real money.

---

## 2. Scale reality — read this before any technology argument

Stating the size up front, because most of the design follows from it and because a plan that
pretends to be bigger than it is fools nobody:

| Quantity | Value |
|---|---|
| Instruments at full universe | ~40 |
| Daily bars per instrument | ~2,500 (10y) to ~8,000 (30y) |
| **Total fact rows, all history** | **~300,000** |
| Compressed on disk | ~15–25 MB Parquet |
| New rows per day | ~40 |
| Full rebuild from raw | seconds |
| Peak working-set memory | < 1 GB |

Every number above is an *estimate*, derived from instrument count and history length. M5 backfills
the full universe and replaces them with measurements taken on the actual machine; until it does,
they are labelled as estimates and nothing is argued from their precision.

This is a small-data problem. Every "big data" tool in this document is therefore justified on
grounds *other than* necessity — learning, portability, or demonstrating a boundary — and that
justification is stated explicitly wherever one appears.

The corollary shapes the whole architecture: **the difficulty here is not throughput, it is
correctness under messy inputs and staying alive unattended.** So the design spends its complexity
budget on seams, contracts and failure isolation, and spends almost none on distribution.

---

## 3. Design principles

| Principle | What it means in practice |
|---|---|
| **Instruments are configuration** | Adding, disabling or re-sourcing an instrument is a registry edit reviewed in version control. No code path is ever named after a ticker. |
| **Ship a working slice early** | A finished narrow pipeline beats an unfinished broad one. One instrument, one source, one rule, one Telegram message — green and scheduled — before anything widens. |
| **It has to run without me** | The daily path runs on a schedule on a host that is not a laptop. Failure is detected and reported; silence is treated as failure. |
| **Dependencies point inward** | The core computes; adapters talk to the outside world; nothing in the core knows what a vendor, a database or Dagster is. Enforced in CI, not by good intentions (§4.1). |
| **Decisions are prefix-stable** | A decision computed on data up to date *D* must equal the decision computed today for date *D*. This single invariant subsumes lookahead, and it is what makes "one definition, two runtimes" true rather than aspirational (§7.3). |
| **The raw zone is append-only** | Vendors restate history. Raw partitions are never overwritten, so any past state of the data is recoverable and a backtest can be pinned to it. |
| **Derived state is disposable; operational state is not** | Two stores, different guarantees, different backup policies (§4.3). Anything in the warehouse must be reconstructible from raw. |
| **Bad data stops the line, narrowly** | Failed checks block the affected instrument's downstream assets — not the other thirty-nine (§4.4). |
| **Reproducible by construction** | Every backtest is keyed by strategy AST, registry commit, data manifest and code revision. |
| **Report what is true** | Publish the metrics the models actually achieve, including the weak ones, and the biases the data still carries. |
| **Right-sized on purpose** | Tool choices are argued from the row counts in §2, not from a job description. |

---

## 4. Architecture

The first draft described *what* to build in some detail and *how it is arranged* not at all. That
omission is where a project like this rots: within a few months `backtest` imports `ml`, the
Telegram bot re-implements backtest invocation, and Dagster is welded into logic that then cannot be
tested or migrated. This section is the load-bearing part of the document.

### 4.1 Layers and the dependency rule

Six layers. **Dependencies point strictly inward**; no layer may import one listed below it.

| Layer | Contains | May import |
|---|---|---|
| `contracts` | Frame and record schemas, versioned. The vocabulary everything shares. | nothing internal |
| `domain` | Pure logic: calendars, cost model, performance metrics, expression AST, the evaluator, feature definitions. No IO, no clock, no globals. | `contracts` |
| `registry` | The registry value object and its loader. | `contracts`, `domain` |
| `ports` | Protocols only: `SourceClient`, `ObjectStore`, `Warehouse`, `OpsStore`, `Notifier`, `Clock`, `ModelProvider`. | `contracts`, `domain` |
| `application` | Use cases: `IngestUniverse`, `BuildWarehouse`, `EvaluateStrategies`, `RunBacktest`, `DeliverAlerts`. Orchestrates domain objects through ports. Knows no concrete adapter. | all of the above |
| `adapters` · `entrypoints` | Stooq, FRED, DuckDB, SQLite, R2, Telegram, MLflow · CLI, FastAPI, Streamlit, Dagster definitions. | everything |

Two consequences worth being explicit about, because they are the ones that pay off:

- **The domain layer has no idea Dagster exists.** A Dagster asset is a five-line wrapper around an
  application use case — configuration and IO management, never business logic. This is what makes
  the pipeline debuggable from a REPL, testable without an orchestrator, and migratable in Stage 4
  without a rewrite. A Dagster asset containing a `join` is a bug.
- **Adapters are wired only at the composition root** (`entrypoints/*`). Nothing else constructs a
  `StooqClient` or opens a database. This is what makes the synthetic source, the in-memory
  warehouse and the fake clock work in tests without patching.

Enforced by [`import-linter`](https://import-linter.readthedocs.io/) contracts running in CI. That
is roughly fifteen lines of config, and it converts an architectural intention into a build failure.

### 4.2 Ports — the extensibility seams, named

"Adding a source is one interface" is a claim the first draft made without defining the interface.
Each port below is a stability promise; each has at least two implementations, one of which is a
test double.

| Port | Real implementations | Test double | The contract it must honour |
|---|---|---|---|
| `SourceClient` | Stooq, FRED, Twelve Data | Synthetic, recorded fixtures | `fetch()` returns the canonical frame or raises from the error taxonomy (§6.7); declares `capabilities()`; never retries internally |
| `ObjectStore` | Local filesystem, S3/R2 | In-memory | Write-once keys; listing is ordered; no update-in-place |
| `Warehouse` | DuckDB | In-memory DuckDB | Exactly one writer (§4.4); read connections are read-only |
| `OpsStore` | SQLite | In-memory SQLite | Transactional; supports the outbox claim-and-mark pattern |
| `Notifier` | Telegram | Recording fake | At-least-once delivery; caller owns idempotency |
| `Clock` | System | Frozen / offset | The **only** source of "now" in the system |
| `ModelProvider` | MLflow | Null provider | May be absent; absence is a first-class state (§7.5) |

`Clock` deserves the emphasis. Ambient `date.today()` scattered through feature code makes the
system untestable, makes "evaluate as of 2019-06-03" impossible, and quietly breaks backfills that
run at midnight. Time is injected, and a test greps the AST of `src/finflow/{domain,application}`
for `datetime.now` and `date.today` and fails on a hit.

### 4.3 Two stores, not one

The first draft — and my own first revision — put `alerts_outbox`, `pipeline_runs` and the
watermarks in the same DuckDB file as the marts, while also claiming "the warehouse is derived and
never backed up; it is rebuilt from raw." Those two statements cannot both be true: you cannot
rebuild "which alerts were already sent" from price history. Separating them fixes the
contradiction and clarifies the backup policy.

| | **Analytical store** — `warehouse.duckdb` | **Operational store** — `ops.sqlite` |
|---|---|---|
| Holds | dims, facts, features, backtests, dq results | outbox, delivered alerts, subscriptions, `pipeline_runs`, watermarks |
| Authority | derived — raw zone is the source of truth | **authoritative — nothing else knows this** |
| Rebuildable | yes, from raw, in seconds | no |
| Backup | none needed; monthly CI proves the rebuild | daily, small, and actually restored-tested |
| Writers | one, the pipeline run | pipeline run and the alert worker |
| Readers | serving snapshot, read-only | alert worker, API health endpoint |
| Size | ~50 MB | < 1 MB |

SQLite rather than DuckDB for the operational side is deliberate: it is a row-store with real
concurrent-writer handling (WAL mode), which is exactly the workload — small transactional
claim-and-mark writes from two processes — that DuckDB's single-writer model is wrong for.

This split also removes the awkward mixed-prefix table list in §9 and gives the Stage 4 migration a
clean line: the analytical store moves to Delta; the operational store becomes Postgres (which A4
needs anyway for CDC).

### 4.4 Failure domains and blast radius

"A failed check halts downstream assets" is too vague to implement. The isolation unit is stated:

- **The failure domain is the instrument partition.** One instrument failing its checks blocks that
  instrument's features, signals and alerts. The other thirty-nine complete.
- **Universe-level assets degrade rather than fail.** A cross-sectional feature computed over
  `sectors` with nine of eleven members healthy is computed over nine, and records which two were
  excluded and why.
- **Cross-sectional strategies need a quorum, and this is a real trading decision, not a technical
  one.** Rotating into "the top 3 of the 6 sectors that happened to load today" is not the strategy
  that was backtested. Each cross-sectional strategy declares `min_quorum` (default 0.9); below it
  the rebalance is **skipped**, the previous positions are held, and the digest says so. Silently
  ranking a distorted universe is the kind of bug that produces confident wrong output, which is the
  worst kind.
- **Alert suppression is per-instrument**, never global — a global halt on one bad ticker is how a
  circuit breaker becomes something people disable.

### 4.5 Processes, and who holds which lock

| Process | Runs where | Writes | Reads |
|---|---|---|---|
| `dagster-daemon` + run workers | VPS | `warehouse.duckdb` (sole writer), `ops.sqlite` | raw zone |
| `alert-worker` | VPS | `ops.sqlite` | serving snapshot (read-only) |
| `api` | VPS, localhost-bound | nothing | serving snapshot (read-only), `ops.sqlite` (read-only) |
| `streamlit` | VPS | nothing | the API only — never a database |
| `dagster-webserver` | VPS, localhost-bound | Dagster's own storage | — |

**This is the topology from M6 onward**, when there is a host to keep processes alive. Before that
the whole daily path is a single short-lived CLI process on a GitHub Actions runner (§11.1): it
delivers alerts in-process rather than through a worker, drains pending Telegram commands with one
`getUpdates` poll at the start of the run, and is consequently the sole writer of both stores. The
`alert-worker` row above becomes real at M6; the outbox and its claim-and-mark semantics exist from
M4 regardless, because they are what make the delivery idempotent, not what make it concurrent.

The serving snapshot is a copy of `warehouse.duckdb` promoted atomically after a successful build. A
Streamlit session holding the live file open would make the next 05:30 run fail with a lock error —
a bug that only appears once something actually runs unattended, which is precisely the class this
project exists to take seriously.

Five processes on a 4 GB box is not free. Each carries an explicit `mem_limit` in the compose
overlay, sized so that a runaway pipeline run **cannot** OOM-kill the alert worker — because the
kernel's OOM killer picks its victim by score, not by importance, and the process that matters most
here is the smallest one. The write-lock column above is enforced rather than documented (§11.6);
Spark, when it runs at all, runs as a one-off container with its own limit and never alongside the
daily path.

### 4.6 Where this would be over-engineering, and why it is not

Six layers for a solo project invites the obvious objection. The answer: this project's central
claim is that its seams are in the right place — that instruments are configuration, that the marts
survive a lakehouse migration, that backtest and live cannot drift. Without explicit boundaries
those claims are unfalsifiable, and `import-linter` makes them cheap to hold.

The pragmatic limits are stated so the structure does not become cargo cult:

- **A package appears when it has two implementations or two consumers, not before.** `ports` is
  justified at the first test double; `compute/spark` is not created until M10 actually needs it.
- `contracts` and `domain` may collapse into one package if `domain` stays small. The dependency
  *rule* is what matters, not the folder count.
- No dependency-injection framework. Constructor arguments and one wiring module.
- No repository pattern over the warehouse; dbt owns the SQL and the `Warehouse` port is thin.

---

## 5. The instrument registry

This is the core extensibility mechanism and the most important *domain* decision in the project.

### 5.1 Registry as code

Instruments live in version-controlled YAML under `instruments/`, not in a database table:

- Changes are **reviewable** — adding an instrument is a pull request with a diff.
- Backfills are **reproducible** — the registry state at any commit is recoverable.
- The pipeline is **auditable** — you can answer "when did we start tracking this, and why".

A database-backed registry would allow runtime additions without a deploy, but loses all three
properties. Recorded as an ADR.

Architecturally the `Registry` is an **immutable value object**, loaded once at the composition root
and injected — not a module-level singleton imported from twenty places, which is the shape it
drifts into by default. Its git commit SHA and commit date are resolved at load time and stamped
into the object, so nothing downstream shells out to git in the middle of a computation.

### 5.2 Instrument definition

```yaml
# instruments/equity_us.yml
instruments:
  - symbol: SPY
    name: SPDR S&P 500 ETF Trust
    asset_class: equity
    sub_class: us_large_cap
    exchange: ARCA
    currency: USD
    calendar: XNYS
    inception: 1993-01-22
    backfill_start: 1993-02-01
    delisted: null            # set to a date instead of deleting the entry
    sources:
      stooq: spy.us           # primary
      twelvedata: SPY         # reconciliation, recent window only
    return_basis: price       # price | total — see §6.4
    distribution_yield_hint: 0.013   # documentation only; flags where price-return understates
    costs: { commission_bps: 2, spread_bps: 1 }   # per-instrument; SGOL is not SPY (§5.7)
    min_adv_usd: 50_000_000   # below this, no signal is emitted
    ucits_equivalent: CSPX.UK # what an EU retail investor can actually buy (§5.7)
    enabled: true
    tags: [core, benchmark, xtb]     # free-form; `xtb` marks broker availability
```

Every field is validated by a Pydantic model at load time. A malformed or duplicate entry fails CI
before it can reach a pipeline run.

`delisted` and `enabled` are separate on purpose. `enabled: false` stops future ingestion;
`delisted` records that the fund ceased to exist. Neither ever removes the row — see §6.5.

### 5.3 Universes

Instruments are grouped into named universes. Strategies and cross-sectional models reference a
universe rather than a hard-coded list, so extending the universe automatically extends everything
built on it.

```yaml
# instruments/universes.yml
universes:
  precious_metals:
    description: Gold and silver, metal and miners
    members: [GLD, IAU, SGOL, SLV, GDX, GDXJ, SIL]
    benchmark: GLD

  broad_commodities:
    members: [DBC, USO, UNG, DBA, PDBC]
    benchmark: DBC

  equity_core:
    members: [SPY, QQQ, IWM, EFA, EEM]
    benchmark: SPY

  sectors:
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
      - { symbol: XLRE, from: 2015-10-08 }   # date-effective membership
      - { symbol: XLC,  from: 2018-06-19 }
    benchmark: SPY

  rates_credit:
    members: [TLT, IEF, SHY, LQD, HYG, TIP]
    benchmark: IEF

  cross_asset:
    description: One representative per asset class, for regime detection
    members: [SPY, TLT, GLD, DBC, UUP, HYG]
    benchmark: SPY
```

Membership is **resolved as-of the evaluation date**, never as-of today. A backtest of `sectors`
starting in 2010 holds nine members, not eleven, and a test asserts it.

### 5.4 Macro series

Macro drivers are registered separately, since they are levels rather than tradeable prices, and
because they carry two properties prices do not: a **release lag** and a **revision history**.

```yaml
# instruments/macro.yml
series:
  - id: us10y_real
    source: FRED
    source_id: DFII10
    unit: percent
    frequency: daily
    release_lag_days: 1        # published next business day
    revised: false
    transform: [level, delta_5d, delta_20d]

  - id: cpi_headline
    source: FRED
    source_id: CPIAUCSL
    unit: index
    frequency: monthly
    release_lag_days: 14       # ~mid-month, for the prior month
    revised: true              # seasonal factors restated annually
    vintage_aware: true        # must be read via ALFRED realtime params
    transform: [yoy, mom]
```

`release_lag_days`, `revised` and `vintage_aware` are not decoration. They are what stops the
pipeline from quietly using March's CPI print in a decision made on 1 March. See §6.3.

### 5.5 Adding a new instrument — the whole workflow

1. Add an entry to the appropriate `instruments/*.yml` file.
2. Open a PR. CI validates the schema **offline** — types, duplicate symbols, calendar codes, date
   ordering, and that each declared source key is one the project implements. That the source
   *actually returns data* for the symbol is checked by the nightly live-source job (§13), not on
   the PR: a pull-request check that depends on a vendor being up is a check that fails for reasons
   unrelated to the pull request, and one that gets ignored shortly afterwards.
3. On merge, a Dagster sensor detects the registry change and **registers a new dynamic partition**.
4. The backfill runs for that instrument's partitions only — no full recompute of anything.
5. Quality checks run against the new series before it is admitted to the marts.
6. Any universe containing it, and every strategy and model referencing that universe, picks it up
   on the next scheduled run.

No code changes. No migrations. That property is the point of the whole design.

### 5.6 What multi-asset makes possible

- **Cross-sectional ranking.** Momentum and value ranks across a universe, enabling rotation
  strategies (hold the top *n* sectors by 12-1 momentum).
- **Relative strength.** GDX/GLD (miners versus metal), SPY/TLT (stocks versus bonds), HYG/IEF
  (credit risk appetite) — each a genuine regime signal.
- **Rolling correlation and regime detection** over the `cross_asset` universe.
- **Panel models.** One model on stacked instrument-date observations generalises far better than a
  per-instrument model fit on a few thousand rows.
- **Breadth.** Percentage of a universe above its 200-day moving average.

### 5.7 Tradeability — costs, liquidity, and whether you can buy it at all

Three registry fields exist because a backtest that ignores any of them produces returns that cannot
be earned. They are cheap to add and each removes a specific class of fake alpha.

**Costs are per instrument, not global.** A flat `commission_bps: 5, slippage_bps: 3` across the
universe is roughly right for SPY and roughly fiction for the rest:

| Instrument | Typical spread | A flat 3 bps assumption |
|---|---|---|
| SPY, QQQ, IWM | ~0.5 bp | conservative — fine |
| GLD, TLT, HYG | ~1–2 bps | about right |
| GDXJ, SIL, SGOL | ~10–25 bps | **understates cost by 5–8×** |
| UNG, DBA, PDBC | ~15–30 bps | **understates cost by 5–10×** |

A monthly-rebalanced rotation over thin ETFs at a true 25 bps round trip gives up several percent a
year that the backtest never sees. Costs therefore live on the instrument, default by asset class,
and a strategy may override but never below the registry floor. Slippage additionally scales with
the instrument's realized volatility, because spreads widen exactly when signals fire.

**Liquidity gates signal emission.** `min_adv_usd` is checked as a data-quality rule: an instrument
whose 20-day average dollar volume falls below the floor emits no signal that day and the digest
says so. For a private account the binding constraint is rarely one's own size — it is that thin
instruments have unstable spreads and gap on open.

**Most importantly: an EU retail investor generally cannot buy US-domiciled ETFs.** Under PRIIPs,
funds without a KID are not offered to EU retail clients, which excludes SPY, GLD, TLT, HYG, XLE and
essentially the entire universe above from a Polish brokerage account. A system that faithfully
computes a target portfolio nobody can execute is a research toy.

So the registry carries `ucits_equivalent`, and a `tradeable_eu` universe is maintained alongside
the research universes:

| Research symbol | UCITS equivalent | Note |
|---|---|---|
| SPY | CSPX / VUSA | accumulating vs distributing changes the tax picture |
| GLD | SGLN / IGLN | physically backed, same exposure |
| TLT | DTLA | USD-hedged variants also exist |
| HYG | IHYU | thinner in Europe |
| EEM | EIMI | |
| XLE, XLF, … | partial | US sector coverage in UCITS form is incomplete — the `sectors` rotation may be research-only, and saying so is better than pretending |

The research universe stays as it is: it has the longest history, the cleanest data and the best
liquidity, which makes it the right place to *test* a hypothesis. Live decisions are then filtered
to `tradeable_eu` via `tradeable_only: true`, and any strategy whose live universe differs from its
research universe reports both, so the gap between what was validated and what is executable is
visible rather than assumed away.

---

## 6. Data sources and the honest data problems

All sources are free and require no brokerage account. Sources are declared per instrument in the
registry, so a new provider is one `SourceClient` implementation (§4.2) plus a registry reference.

### 6.1 Batch — daily and historical

| Source | Coverage | Auth | Role and known limits |
|---|---|---|---|
| **Stooq** | US ETFs and equities, indices, FX — long daily history | none | **Primary.** Plain CSV over HTTPS, wide symbol coverage. Undocumented per-IP daily cap; returns an HTML error page rather than an HTTP error when exceeded, and blocks cloud egress ranges aggressively. |
| **FRED / ALFRED** | Real yields, dollar index, CPI, credit spreads, VIX | free key | Macro series. ALFRED is the same API with `realtime_start` — the vintage-aware read path (§6.3). |
| **Twelve Data** | OHLCV, FX, intraday | free key | Reconciliation only. ~800 calls/day and 8/min: fine for a daily cross-check on a rolling window, hopeless for a 30-year backfill. |
| **Alpha Vantage** | OHLCV, FX | free key | Tertiary fallback. 25 requests/day — manual repairs only. |
| **Synthetic generator** | Deterministic pseudo-OHLCV | none | Always available. Makes CI hermetic and `make demo` work offline. |

**Removed from the original design:** Nasdaq Data Link — its free tier no longer covers the ETF
reference data the plan assumed. `yfinance` stays excluded as a primary source (ambiguous terms,
breaks without warning) but is permitted behind a flag as a third opinion during reconciliation.

Both Alpha Vantage and that `yfinance` third opinion are **registered but unbuilt**: no milestone in
`IMPLEMENTATION.md` implements either, and both are backlog items reached through the same
`SourceClient` port. Naming them here is a statement about where the seam is, not a commitment to
write them. The sources actually built are Stooq, FRED/ALFRED, Twelve Data and the synthetic
generator.

### 6.2 Problem one — vendors restate history

Stooq applies split adjustments retroactively. Re-downloading GLD's 2019 bars after a split gives
different numbers than the download taken last year. This breaks two assumptions:

- "Re-running ingest produces byte-identical partitions" is **false** and cannot be an acceptance
  criterion. Idempotency here means *convergent*, not *identical*.
- A backtest "keyed by data version" is meaningless if the data underneath was overwritten.

So the raw zone is **append-only by ingestion run**:

```
data/raw/source=stooq/symbol=GLD/ingested=2026-08-26T05:12:00Z/data.parquet   # never rewritten
data/raw/source=stooq/symbol=GLD/ingested=2026-11-04T05:11:00Z/data.parquet   # a later opinion
```

Bronze resolves to the latest observation per `(symbol, date)`. When a re-fetch changes a previously
ingested value, the difference is written to `dq_restatements` with both run ids, and a large
restatement raises an alert. Corporate actions are then *observed* rather than assumed.

**Reproducibility is keyed by a manifest, not by a timestamp.** My first revision used
`snapshot_id = max(ingested_at)`, which is wrong: backfilling GLD's 1990s history today would bump
the id and falsely imply every other instrument had changed. Instead each pipeline run writes a
**manifest** listing, per `(source, symbol)`, the ingestion-run ids admitted to bronze;
`snapshot_id = hash(manifest)` and the manifest itself is stored. Reproduction is then exact and
per-instrument, and it is precisely what Delta time travel replaces in A1.

### 6.3 Problem two — macro data has a release lag and a revision history

This is where "no lookahead" is most easily violated while looking correct.

`CPIAUCSL` for March is dated 1 March in FRED. It is *published* around 10 April, and its seasonally
adjusted values are *restated* every February. A backtest joining CPI on its observation date reads
a number that did not exist for six weeks, computed with factors that did not exist for a year.

- **Release-date join.** Macro features join on `available_from = observation_date +
  release_lag_days`, never on `observation_date`.
- **Vintage-aware read.** For `vintage_aware` series the FRED client passes ALFRED's
  `realtime_start` / `realtime_end` and stores grain `(series_id, observation_date, vintage_date)`.
  `fct_macro_daily` is a first-release view over that.

Daily market-based series (`DFII10`, `DTWEXBGS`, `VIXCLS`) are not revised: one-business-day lag, no
vintage dimension.

### 6.4 Problem three — price return is not total return

Stooq's `.us` series are split-adjusted but not dividend-adjusted, and no free source gives clean
ETF distribution history. This matters wildly unevenly:

| Instrument | Approx. distribution yield | Effect on a 10-year backtest |
|---|---|---|
| GLD, SLV, SGOL | 0% | none — price return is correct |
| SPY, QQQ | ~1.2% | modest drag on long-only results |
| TLT, IEF, LQD | ~3–4% | material |
| **HYG** | **~6–7%** | **a long-only price-return backtest is simply wrong** |

The decision: **the MVP is explicitly price-return, `return_basis: price` everywhere, stated on
every backtest report and in the UI.** Building a fake total-return series from an unreliable
distribution feed would be worse — a wrong number that looks authoritative.

Consequences carried deliberately: `rates_credit` strategies ship as relative or long/short forms
where carry largely cancels; `return_basis: total` exists in the schema from day one so a
distribution source is additive; `docs/RESULTS.md` reports the bias per universe.

### 6.5 Problem four — survivorship bias, and why it is nearly unfixable here

Stooq drops delisted symbols, so a universe assembled today from a live vendor contains only
survivors and every long-horizon backtest is flattered.

- **The append-only raw zone is the archive.** Anything ingested once is kept forever, so an
  instrument that delists *while the project is running* keeps its history. This is the deeper
  reason for append-only.
- **`delisted` is recorded, never deleted**, and membership is date-effective (§5.3), so the
  pipeline is structurally able to handle a dead fund — it just cannot retroactively obtain ones
  that died before the project started.
- **The bias is quantified and published** rather than waved at.

### 6.6 Cross-source reconciliation

Instruments declaring two sources are compared in `dq_source_agreement` over a rolling recent window
(default 30 trading days — what Twelve Data's quota affords). The primary source always wins on
write; the secondary only ever produces a flag.

### 6.7 The error taxonomy — where retry policy actually lives

Retry and back-off belong to the *error class*, not to the source. Every `SourceClient` raises from
one closed taxonomy, and the shared ingestion layer maps each class to a policy. This is what stops
five clients from growing five subtly different retry loops.

| Error | Means | Policy |
|---|---|---|
| `SourceRateLimited` | Quota or per-IP cap hit | **Never retried in-run.** Abort the source, mark remaining symbols deferred, resume next run |
| `SourceUnavailable` | 5xx, timeout, connection reset | Exponential back-off with jitter, bounded attempts |
| `SymbolNotFound` | Vendor does not know this symbol | No retry. Quality incident against the registry entry, not the run |
| `MalformedResponse` | Parsed but failed the contract | No retry. Payload captured to quarantine for inspection |
| `AuthenticationFailed` | Bad or expired key | No retry. Fails the run loudly — this is an operator problem |

Stooq's rate-limit response is an HTML page with HTTP 200, so the client validates content type and
header row *before* parsing and raises `SourceRateLimited`. A test feeds it a captured real error
page. Without that, the pipeline ingests an error message as a price bar.

### 6.8 Multi-asset ingestion concerns

- **Trading calendars.** Instruments carry a `calendar` code, aligned via `exchange_calendars`. A
  missing bar on a US holiday is expected; on a trading day it is an incident.
- **Currency.** MVP is USD-only and US-listed. The `currency` field exists so FX normalisation is
  additive; for a non-US author that plus a PLN reporting view is the obvious first extension.
- **Inception dates.** Backfills start at `backfill_start`, so history is never fabricated.

---

## 7. Strategy definition and the evaluator

### 7.1 The document

A single YAML document defines a strategy. The backtest engine and the live evaluator both consume
it, so there is no possible drift between what was validated and what runs.

```yaml
apiVersion: finflow/v1                 # the contract; see §7.4
name: gold_momentum_riskoff
type: single
instrument: GLD
signals:
  - id: sma_cross
    when: "sma(close, 20) > sma(close, 50)"
  - id: real_yield_falling
    when: "delta(us10y_real, 5) < 0"
  - id: ml_confirm
    when: "p_up_5d > 0.58"
entry: "sma_cross and real_yield_falling and ml_confirm"
exit:  "sma(close, 20) < sma(close, 50) or drawdown > 0.06"
sizing:
  type: vol_target
  annual_vol: 0.10
  vol_source: forecast                 # forecast | trailing — see §8.2
  max_leverage: 1.0
  rebalance_band: 0.20
costs: from_registry                   # per-instrument floors (§5.7); may be raised, never lowered
tradeable_only: true                   # live decisions filtered to tradeable_eu (§5.7)
on_missing_model: strict               # strict | skip | permissive — see §7.5
alerts: { telegram: true, cooldown_bars: 3 }
```

A note on the example itself, since a design document teaches by what it shows: this one is
**illustrative, not recommended**. A moving-average cross conjoined with a macro delta and a model
confirmation is three weakly-motivated conditions multiplied together, and the space of such rules
is large enough that some of them backtest well by construction. The strategies shipped in
`strategies/` are deliberately the ones with published out-of-sample support — cross-sectional and
time-series momentum, a trend filter, inverse-volatility weighting — precisely so that
`docs/RESULTS.md` is reporting on hypotheses that existed before the data was looked at.

```yaml
apiVersion: finflow/v1
name: sector_rotation
type: cross_sectional
universe: sectors
rank_by: "momentum(close, 252, skip=21)"
select:  { top_n: 3, rebalance: monthly }
filter:  "close > sma(close, 200)"
weights: { scheme: inverse_vol, lookback: 60 }
min_quorum: 0.9                        # §4.4 — below this, hold, do not rotate
costs:   from_registry
benchmark:                             # a portfolio, not only a ticker
  weights: { SPY: 0.6, IEF: 0.4 }
  rebalance: quarterly
alerts:  { telegram: true, on: rebalance }
```

The benchmark being a *portfolio* matters more than it looks. For a private investor the honest
comparison is not "did this beat SPY" but "did this beat what I would otherwise have held" — for
most people a static global-equity or 60/40 allocation. Beating SPY with 60% average exposure is
not skill, and reporting it as excess return hides that. Every backtest therefore reports
beta-adjusted alpha and information ratio against the benchmark portfolio, not just excess return.

`rebalance_band` on vol targeting is not cosmetic: naive daily vol targeting rebalances every day and
generates turnover that dwarfs the signal once costs are on.

### 7.2 The DSL is a compiler, and the AST is the interface

The expression strings are a *surface syntax*. The stable artifact is the **typed AST** they compile
to, and that distinction resolves several problems at once.

```
YAML → parse → AST → type-check → compile → Polars expression
                ▲
                └── this is what gets hashed, stored and versioned
```

- **No `eval`, ever.** A hand-written parser over a closed grammar; the function registry is a
  typed whitelist (`sma`, `ema`, `rsi`, `delta`, `momentum`, `rank`, `drawdown`, comparisons,
  boolean logic). A test feeds it `__import__("os").system("...")` and asserts a parse error.
- **A minimal type system**, because these expressions are not all the same kind of thing:
  `Scalar`, `Series[float]`, `Series[bool]`, `CrossSection[float]`. It catches `rank_by: "close > 5"`
  (a boolean where a rankable is required) and `filter: "sma(close, 20)"` (a float where a predicate
  is required) at load time rather than at 05:00 on a Tuesday.
- **The AST is what the run hash keys on**, not the string. Reformatting whitespace does not
  invalidate a stored backtest; changing a `20` to a `21` does.
- **The AST is persisted with the run**, so a five-year-old backtest is reproducible even after the
  surface syntax has moved on.

### 7.3 One evaluator — the invariant that makes the claim true

"One definition, two runtimes" is the highest-risk claim in this document, because the natural
implementations differ: the backtester is vectorized over full history, and live evaluation wants
today's decision. Two code paths means eventual drift, and drift here is invisible.

The resolution is to **not have two paths**. The engine splits into:

```
decide(features, ast, as_of)       -> Decision        pure; reads only data up to as_of
simulate(decisions, bars, costs)   -> fills, metrics  needs the NEXT bar, so backtest-only
```

`Decision` is the entity of §9.4: one object per evaluation, holding the full target portfolio.
Live evaluation is `decide()` with `as_of` set to today. A backtest is the same call over a range of
dates, with `simulate()` turning the resulting decisions into fills. There is no second
implementation to drift.

That makes one invariant load-bearing — the **prefix-stability** property from §3:

> for any date *D*: `decide(features[:D], ast, D) == decide(features_full, ast, D)`

The left side is what live evaluation could have computed on *D*, given only what existed then. The
right side is what a backtest run today computes for *D*, given everything. Equality means no future
data reached the decision.

A property test asserts it over a sample of random dates. This is stronger and cheaper than a
bespoke lookahead test, because it catches the whole family at once: a forward-shifted join, a
`shift(-1)`, a full-sample z-score, a global rank, an `ffill` that reaches backwards. The volume
z-score in §8's feature list is exactly the trap — computed over the full sample it is a lookahead
bug that no unit test would notice and this property catches immediately.

Every emitted signal carries `as_of`, `strategy_version` (the AST hash) and `snapshot_id`, so a
stale pipeline produces a *visibly* stale alert rather than a confident one.

### 7.4 Versioning the strategy contract

Backtest runs are keyed by strategy and stored forever, so the strategy document is a published
contract:

- `apiVersion: finflow/v1` is mandatory. An unknown version fails loudly.
- Within a major version, changes are **additive only**: new optional fields with defaults that
  preserve existing behaviour.
- A breaking change means `v2`, plus a migration function `v1 → v2`, plus retention of the v1 parser
  for as long as v1 runs exist in `fct_backtest_runs`.
- The same policy applies to the frame contracts in §9. Written down in `docs/CONTRACTS.md`.

### 7.5 Degrading when the model is absent

My previous revision let ML clauses carry `optional: true`, defaulting to true when no model exists.
That is a silent-correctness hazard: it turns a three-condition AND into a two-condition AND — a
*different strategy* running under the same name and firing the same alerts. Replaced by an explicit
per-strategy policy:

| `on_missing_model` | Behaviour |
|---|---|
| `strict` **(default)** | The strategy does not run. The digest says why. |
| `skip` | Clauses depending on the model are dropped, and **the AST hash changes**, so the degraded run is a distinguishable variant that cannot be confused with the full strategy. |
| `permissive` | Clauses evaluate true. Allowed only for strategies explicitly marked as such, and every alert is tagged `degraded`. |

The rule underneath: *any* change to what is actually evaluated must change the run identity.

### 7.6 Portfolio state — the difference between an alerting system and a useful one

The design so far emits a signal when a rule fires. Used daily, that is the wrong output. What a
person actually needs each morning is not "GLD triggered" but:

> **This is what you should hold. This is what you do hold. Here is the difference, and here is
> whether it is worth trading.**

Two additions close that gap, and both are small.

**Actual holdings are recorded.** A `positions_actual` table in the operational store (§9.3), edited
through `/position GLD 12` or a YAML file, holds units and average cost. Nothing automated touches
it — this is not broker integration, it is the user telling the system what they own. The daily
digest then reports target versus actual drift per instrument, and stays silent when drift is inside
the rebalance band. A system that says "no action" on most days is one that gets read on the days it
does not.

**Decisions are netted across strategies before they become instructions.** Four strategies running
at once will overlap — several will be long GLD in the same risk-off regime, and two may disagree
outright. Emitting four independent alerts pushes portfolio construction onto the reader at exactly
the moment they are least equipped to do it. So a portfolio step sits after evaluation:

```
strategy A decision ─┐
strategy B decision ─┼──▶ net positions ──▶ apply gross/net caps ──▶ apply rebalance band
strategy C decision ─┘         ▲                    ▲                          │
                     per-strategy capital     total exposure limit             ▼
                     allocation weights                              one instruction set
```

`portfolio.yml` declares capital allocation per strategy, a maximum gross exposure, a maximum single
instrument weight, and a minimum trade size below which drift is not worth the spread. The netted
result is one `Decision` per day for the whole account, which is also the correct grain for the
idempotency key of §9.4.

That netted object is a `Decision` like any other, but its scope is the account rather than a
strategy: `scope: portfolio`, with `strategy_id` null and the contributing per-strategy decision ids
recorded on it. Both scopes share one table, one outbox and one delivery path, so nothing downstream
needs to know which kind it is holding — and the per-strategy decisions are still persisted, because
they are the counterfactual §7.7 depends on.

**Risk is reported alongside, not left implicit.** The digest carries ex-ante portfolio volatility
from the covariance of held instruments, current drawdown from peak, largest single-instrument
weight, and effective number of independent bets. These are the four numbers a desk looks at every
morning, they are computable from features that already exist, and they are what turns a list of
tickers into a portfolio.

### 7.7 The user overrules the system, and the system remembers

A system that only issues instructions will be disobeyed — and once it is being disobeyed silently,
its record of what it recommended stops corresponding to anything. Every real user of a tool like
this has weeks where they will not act: a known expense next month, a tax-year constraint, a
holiday, or simply not believing it this time. Those are legitimate, and the product has to have
somewhere to put them.

Four controls, all cheap, all through the Telegram bot:

| Control | Effect | Why it exists |
|---|---|---|
| `/pause <strategy>` | Stops issuing instructions; **keeps computing and recording** | Preserves the counterfactual, so "what would it have done" is still answerable when the pause ends |
| `/mute <instrument> <until>` | That instrument is excluded from targets until the date | A pending sale, a tax constraint, a fund you have decided you dislike |
| `/hold` | Suspends all rebalancing for a stated period | Travel, or a month where you simply do not want to trade |
| Override | The user sets a position deliberately away from target | Recorded as a decision, not treated as drift to nag about |

Two rules keep these from quietly corrupting the record:

- **Nothing is suppressed silently.** The digest always lists what was withheld and why — a muted
  instrument, a paused strategy, a suppressed instruction. A control the user forgot they set is
  indistinguishable from a bug, and the digest is what prevents that.
- **Every override carries a one-line reason**, written into a decision journal in the operational
  store when it is made, while the reasoning is still available. Reviewed quarterly against what
  subsequently happened. This costs a sentence a month and is the single most useful thing the
  system can do for the person using it, because it turns "I usually get these calls right" from a
  belief into a record.

---

## 8. Modeling and evaluation

Models are **an optional enrichment of a pipeline that already works**, not a dependency of it —
architecturally, `ModelProvider` is a port with a null implementation, and the domain layer has no
compile-time dependency on LightGBM or MLflow. This is the single biggest de-risking decision in the
plan: the ML layer is the part most likely to disappoint, and nothing that matters waits behind it.

### 8.1 Targets, baselines and honest metrics

- **Two targets.** Sign of the n-day forward return, and n-day realized volatility. Volatility is
  genuinely forecastable; direction is close to unforecastable. Modeling both makes the contrast
  explicit rather than hiding it.
- **Panel training.** Stacked instrument-date observations with instrument identity as a
  categorical. Far more training data than any single series, and it generalises to newly added
  instruments without retraining from scratch.
- **Baselines matched to the target.** Naive, drift and ARIMA are *return* baselines and say nothing
  about a volatility model. Direction is benchmarked against naive, drift and the unconditional
  base rate; volatility against EWMA (RiskMetrics λ=0.94), GARCH(1,1) and **HAR-RV**. HAR-RV is the
  right bar to clear because it is three regressions on daily, weekly and monthly realized variance,
  it is genuinely hard to beat, and a gradient-boosted model that cannot beat it has learned
  nothing. Volatility models are scored with QLIKE and MSE on realized variance, not with AUC.
- **Walk-forward validation with purging and embargo.** A random split on time series is silently
  wrong and produces flattering nonsense.
- **Features.** Per-instrument: multi-horizon returns, realized volatility, ATR, RSI, distance from
  moving averages, rolling volume z-score (rolling — see §7.3). Cross-sectional: momentum rank,
  volatility rank, relative strength versus benchmark, breadth. Macro: real-yield and dollar deltas
  joined on release date. Plus calendar effects.
- **Model output is a feature, not a special case.** Predictions land in `fct_features_daily`
  alongside everything else, so the strategy evaluator has no notion of "an ML column" and the
  prefix-stability property covers predictions too.
- **One retrain cadence, written down.** Monthly walk-forward refit; promotion only on beating the
  incumbent out-of-sample. No hand-tuning between refits — that is how research quietly overfits the
  whole sample.

**Expected honest outcome:** direction prediction near 0.52 AUC, barely better than a coin flip,
while volatility forecasting is clearly useful. Both get published.

### 8.2 The volatility model has to earn its keep

The first draft forecast volatility, reported the metric, and then sized positions from *trailing*
realized volatility — so the one model that works fed nothing. That is backwards. `vol_source:
forecast` routes the forecast into position sizing (§7.1), which is where a good volatility estimate
is actually worth money:

- Vol targeting with a forward-looking estimate reduces exposure *before* a vol spike rather than
  one window after it, which is most of the benefit.
- The improvement is measurable end-to-end: run the same strategy with `trailing` and `forecast` and
  compare realized volatility against target, plus drawdown. That comparison is a far better test of
  the model than QLIKE, because it answers the question that matters.
- It also gives the ML layer a purpose that survives direction prediction being useless — which,
  per the expectation above, it will be.

Correlation deserves the same treatment. The `cross_asset` correlation matrix is currently computed
and looked at. Feeding it into a gross-exposure overlay — de-risk when cross-asset correlations
converge, since that is diversification failing — is a few lines and uses a feature that already
exists. It is nonetheless deliberately in the backlog rather than in a milestone
(`IMPLEMENTATION.md`, backlog): the volatility overlay above has to be measured on its own first, or
the two arrive entangled and neither can be attributed.

### 8.3 Evaluation that survives multiple testing

This is the largest methodological gap in the original design, and closing it is perhaps a single
evening's work.

You will run dozens of backtests. Whichever one looks best will look better than it is, because it
was *selected* for looking best. Reporting its Sharpe as if it were the result of a single
pre-registered experiment is the mistake at the heart of most published backtests, and the platform
is already storing exactly what is needed to avoid it:

- **The trial count is not an estimate — it is a query.** `fct_backtest_runs` holds every run ever
  executed. The number of distinct configurations tried against a given universe and period is
  `SELECT count(*)`, which is the input almost nobody has and this system gets for free.
- **Deflated Sharpe Ratio** (Bailey & López de Prado). Adjusts an observed Sharpe for the number of
  trials, the sample length, and the skew and kurtosis of returns, and returns the probability that
  the true Sharpe exceeds zero. Roughly twenty lines of code given the trial count.
- **Probability of Backtest Overfitting** via combinatorially symmetric cross-validation: split the
  sample, rank configurations in-sample, observe where they rank out-of-sample. A PBO above ~0.5
  means the selection procedure has no skill, regardless of how good the winner looks.
- **Confidence intervals on every Sharpe, always.** With SE(SR) ≈ √((1 + SR²/2)/T), ten years of
  daily data gives a standard error near 0.32. A strategy at 0.65 against a benchmark at 0.40 is
  *indistinguishable from noise*, and reporting the two point estimates side by side implies a
  difference the data cannot support. Intervals make that visible without any argument.

`docs/RESULTS.md` reports, for every headline number: the point estimate, its confidence interval,
the deflated value, the trial count it was deflated by, and the PBO of the selection procedure.

This is not statistical fastidiousness for its own sake — it is the input to the only decision that
matters here. A modest but *defensible* Sharpe tells the user how much of their savings to put
behind a strategy; an undeflated 1.8 tells them nothing and invites them to size it wrongly. §15
turns those numbers into an explicit rule for how much money follows how much evidence.

### 8.4 What actually lands in the account

Backtest returns are gross of two things that are not small for a Polish private investor.

**Tax.** Capital gains are taxed at 19% on realization (PIT-38), with no wash-sale rule and no
tax-advantaged wrapper for a normal brokerage account. This makes turnover expensive in a way the
cost model does not capture: a monthly rotation realizes gains continuously, while buy-and-hold
defers indefinitely. On a strategy at 300% annual turnover the drag is material — plausibly more
than the commission and spread combined. Backtests therefore report gross, net-of-cost, **and**
net-of-cost-and-tax under a stated realization assumption, so a high-turnover strategy is compared
to buy-and-hold on the basis that actually reaches the account. Dividend withholding on
US-domiciled funds is a further 15% under the treaty, which is one more argument for the UCITS
mapping of §5.7 (Irish-domiciled funds reclaim part of it internally).

**Currency.** An unhedged USD ETF held by a PLN-based investor is a joint position in the asset and
in the dollar, and USD/PLN has moved 20%+ in a year more than once. Reporting only USD returns
hides half the risk. Every performance report carries both currencies, and portfolio volatility in
§7.6 is computed in PLN terms, because that is the volatility actually experienced.

Neither of these makes a strategy good or bad by itself. Both change the ranking between strategies,
which is the entire point of computing them.

---

## 9. Data model and contracts

### 9.1 Contracts: the right tool at the right grain

The first draft said "Pydantic schemas at every stage boundary". That is a category error at one of
those boundaries: the pipeline moves Polars frames, and pushing 300k rows through per-record
Pydantic validation is both slow and the wrong shape. Split by grain:

| Boundary | Tool | Why |
|---|---|---|
| Registry, settings, strategy YAML, API request/response | **Pydantic** | Single records, rich validation, good errors |
| DataFrames between pipeline stages | **Patito** (Pydantic-backed Polars schemas) | One schema definition, validated as a frame — dtypes, nullability, uniqueness — in one vectorized pass |
| dbt models | **dbt model contracts** (`contract: enforced`) | Column names and types enforced in the warehouse, not just in Python |

All three live in `src/finflow/contracts/`, which imports nothing internal and is imported by
everyone — the shared vocabulary of §4.1.

### 9.2 Analytical store — `warehouse.duckdb`

```
dim_instrument   (SCD2 from the registry; valid_from = git commit date of the change)
  instrument_key · symbol · name · asset_class · sub_class · exchange
  currency · calendar · inception · delisted · enabled · return_basis
  valid_from · valid_to · is_current · registry_commit

dim_universe                     bridge_universe_member
  universe_key · name              universe_key · instrument_key
  description · benchmark_symbol   valid_from · valid_to      (date-effective, §5.3)

dim_source                       dim_date
  source_key · name · tier         date_key · date · trading_day_xnys · month_end · quarter_end

fct_ohlcv_daily        grain: instrument × date        (latest opinion)
  open · high · low · close · volume · return_basis
  source · ingested_at · snapshot_id · quality_flag

fct_macro_daily        grain: series × observation_date        (first release)
  value · available_from · vintage_date · is_revised

fct_features_daily     grain: instrument × date  (per-instrument, cross-sectional, predictions)
fct_signals            grain: strategy × strategy_version × instrument × date
fct_decisions          grain: decision_id · scope (strategy | portfolio)  (§7.6, §9.4)
                       as_of · decision_ts · snapshot_id · target positions
                       strategy_id and strategy_version are null at portfolio scope
fct_backtest_runs      grain: run_id   (+ fct_backtest_positions, fct_backtest_metrics)

dq_results             grain: check × asset × partition × run
dq_source_agreement    grain: instrument × date × source_pair
dq_restatements        grain: instrument × date × (old_run, new_run)      (§6.2)
```

Two notes on the SCD2 dimension, because both are commonly got wrong: `valid_from` comes from the
**git commit date of the registry change**, not the pipeline run date — otherwise a backfill run in
November stamps an August change with November. And the registry is the *only* writer of
`dim_instrument`; nothing downstream inserts an instrument it discovered in a feed.

`strategy_version` in the `fct_signals` grain is not optional. Without it, comparing signals before
and after a strategy edit is impossible, and — worse — see §9.4.

Every fact table is partitioned by instrument and date, which is what makes single-instrument
backfills cheap.

### 9.3 Operational store — `ops.sqlite`

```
alerts_outbox      unique (strategy_id, strategy_version, decision_id) — one row per decision
                   scope · payload · created_at · claimed_at · sent_at · attempts
                   instrument, bar_ts and rule_id live inside the payload, never in the key:
                   keying per instrument is exactly what delivers half a rotation (§9.4)
subscriptions      chat_id · strategy_id · created_at
positions_actual   instrument · units · avg_cost · currency · updated_at      (§7.6)
positions_target   decision_id · instrument · weight · units · generated_at   (netted, §7.6)
controls           kind (pause|mute|hold) · scope · until · set_at · reason   (§7.7)
decision_journal   decision_id · action (followed|overridden|ignored) · reason · recorded_at
pipeline_runs      run_id · started_at · ended_at · status · rows_written · snapshot_id · manifest_ref
watermarks         (source, symbol) -> last_loaded_date · last_run_at · row_count · deferred_until
```

Authoritative, small, backed up daily, restore tested. Nothing here is derivable from the raw zone,
which is exactly the criterion that put it in a different store (§4.3).

### 9.4 Modeling a decision, not just a signal

The original alert key `(strategy_id, instrument, bar_ts, rule_id)` has two defects that only appear
in use:

- **It is not stable across strategy edits.** Edit the YAML, and the new signal for the same bar
  collides with the old key and is suppressed as a duplicate — a silent miss. `strategy_version` is
  therefore part of the key.
- **It cannot represent a rebalance.** A rotation strategy's output is a *set* of target positions,
  not a per-instrument event. Deduplicating a rebalance instrument-by-instrument can deliver half a
  rotation if the worker dies mid-batch.

So the domain gets an explicit `Decision` entity: one decision per strategy per evaluation, holding
the full target portfolio, written to `fct_decisions` and referenced by `decision_id` from every
outbox row. Delivery is per-decision and atomic. A single-instrument entry is then just a decision
with one position, and both strategy types share one code path.

---

## 10. MVP — the Lean Warehouse

**Decision: this is what gets built first.** No cluster, no broker, no object store. Everything runs
in-process; the platform boots in seconds and the full test suite runs in CI in under two minutes.
At 300k rows (§2) this is not a compromise — it is the correct architecture, and §12 is honest about
the lakehouse being a learning exercise rather than a necessity.

```
  Stooq · FRED/ALFRED · Twelve Data     instruments/*.yml  (registry)
            │                                    │
            ▼  adapters/sources (httpx + Polars) │ drives partitions,
     append-only · watermarked · retried  ◀──────┘ sources, calendars, lags
     per error class (§6.7)
            │
     raw Parquet in an ObjectStore, partitioned by ingestion run
            │        (never overwritten; a manifest pins each run, §6.2)
            ▼
   warehouse.duckdb (single writer)  ◀── dbt (staging → intermediate → marts)
            │                             contracts enforced · tests · docs
            │
            ├──▶ atomic snapshot promotion ──▶ serving copy (read-only)
            │                                        │
     marts: dim_instrument · fct_ohlcv_daily         │
            fct_features_daily · fct_decisions       │
            │                                        │
            ▼                                        ▼
     application use cases                    entrypoints
     ┌──────────────┬──────────────┐          ┌─────────────────┐
     │ EvaluateStr. │ RunBacktest  │          │ FastAPI ──▶ UI  │
     │  decide()    │  + simulate()│          └─────────────────┘
     └──────┬───────┴──────────────┘
            │            ops.sqlite (authoritative)
            └──▶ Decision ──▶ alerts_outbox ──▶ alert-worker ──▶ Telegram

  Dagster (thin asset wrappers), partitioned by instrument × date
  GitHub Actions runs the whole thing on every PR — and, until M6, in production
```

| Layer | Choice | Why this one |
|---|---|---|
| Language | Python 3.12, `uv`, ruff, mypy (strict), pytest | — |
| Architecture | Layered with an enforced dependency rule (`import-linter`) | §4.1 |
| Registry | YAML + Pydantic, versioned in git, immutable value object | §5.1 |
| Frame contracts | Patito over Polars; dbt model contracts in SQL | §9.1 |
| Ingestion | httpx + Polars, error-class retry policy, append-only landing | §6.2, §6.7 |
| Calendars | `exchange_calendars` | Distinguishes a holiday from a missing bar |
| Analytical store | DuckDB, file-backed, snapshot-promoted for serving | 300k rows; zero operational surface |
| Operational store | SQLite (WAL) | Two concurrent writers, small transactions — the workload DuckDB is wrong for (§4.3) |
| Transform | dbt-duckdb — staging / intermediate / marts | Portable SQL; survives the §12 migration unchanged |
| Data quality | dbt tests + `dbt-expectations` + freshness + reconciliation + restatements | §13 |
| ML | LightGBM, scikit-learn, MLflow — behind `ModelProvider` | Optional layer (§8) |
| Serving | FastAPI over the snapshot; Streamlit over the API | §4.5 |
| Alerts | python-telegram-bot, outbox in `ops.sqlite`, per-decision | §9.4 |
| Orchestration | Dagster — thin wrappers only | §4.1 |
| Packaging | Multi-stage image, pinned base digest, non-root, lockfile-only install; Spark in a separate image | §11.4 |
| Deploy | GHCR images tagged by git SHA; `make deploy` / `make rollback`; smoke test gates the deploy | §11.4 |
| Host | A small VPS, rebuildable from one `cloud-init.yaml`; every port bound to localhost | §11.5 |
| CI/CD | lint → type → **imports** → registry → secrets/deps/image scan → test → `dbt build` → build → deploy | §11.7 |

**One architectural constraint that costs nothing now and saves Stage 4:** mart SQL stays
dialect-neutral. DuckDB-specific functions are confined to staging models and macros, so the claim
"the marts survive the lakehouse migration unchanged" is a falsifiable bet rather than a hope. It
gets falsified cheaply in A1 if it was wrong.

**Trade-off accepted.** No Kafka, no CDC, no real-time path, and only a thin Spark surface. In
exchange the project is realistically finishable, and because everything runs in CI the pipeline is
genuinely tested rather than merely testable.

---

## 11. Running it every day

A platform that only runs when its author types `make` is a demo. Everything below is cheap.

### 11.1 Where it runs

| Stage | Host | Why |
|---|---|---|
| **From the vertical slice (M4)** | GitHub Actions scheduled workflow | Nothing to operate, secrets already managed, and the run log is a record that the pipeline works. Cron fires late under load and the job caps at 6h — irrelevant for a 90-second run. |
| **From orchestration (M6) onward** | A small always-on VPS running Docker Compose | Dagster wants a long-lived daemon and a webserver (§4.5). |
| Never | A laptop | It will be shut at 05:00. |

State on GitHub Actions is the honest problem: the runner is ephemeral. Raw Parquet goes to an
S3-compatible bucket (Cloudflare R2's free tier is several hundred times the requirement) via the
`ObjectStore` port; `warehouse.duckdb` is rebuilt from raw on every run — seconds at this size, and
a *feature*, since it proves the analytical store is disposable. `ops.sqlite` is the one
genuinely stateful piece; it is pulled and pushed around each run, or lives on the VPS from M6.

### 11.2 Knowing it still works

Silence must never read as success.

- **Dead-man's switch.** The run pings healthchecks.io on success. No ping within the grace window
  and it emails — catching the failure mode where the job never started, which in-job error
  handling structurally cannot.
- **Daily digest.** A fixed-time Telegram message regardless of signals, in two halves. *Pipeline
  health*: bars ingested, freshest date per universe, checks passed/failed, restatements,
  instruments excluded from cross-sectional universes and why. *Portfolio*: netted target versus
  actual holdings with drift outside the rebalance band flagged (netting arrives at M7; from M4 the
  same line reports the single running strategy's target), anything withheld by a pause, mute
  or hold (§7.7), ex-ante portfolio volatility in PLN, drawdown from peak, largest single-instrument
  weight (§7.6). Most days it should say no action — which is what makes it read on the days it
  does not, and §1.2 makes that a target rather than an accident. A missing digest is itself the
  alarm.
- **Staleness on the face of every alert.** Signals carry `as_of`, `strategy_version` and
  `snapshot_id` (§7.3).
- **`pipeline_runs`** makes "when did this last actually work" a query rather than a log scroll.

### 11.3 Not losing the data

- **Raw zone**: append-only, in object storage, bucket versioning on, with a lifecycle rule rather
  than manual pruning. The pipeline's R2 token is scoped **read and write but not delete** — a
  one-line decision that makes "I accidentally wiped thirty years of history" structurally
  impossible rather than merely unlikely.
- **Analytical store**: derived, never backed up, rebuilt from raw. A monthly CI job asserts the
  rebuild reproduces the marts.
- **Operational store**: small, authoritative, and the only thing that genuinely needs backing up.
  Nightly `VACUUM INTO` (an SQLite-consistent snapshot, unlike copying a live file), gzipped,
  age-encrypted, pushed to a **different bucket** from the raw zone with 30 daily and 12 monthly
  copies retained. A backup on the same box is not a backup.
- **The restore is exercised, not assumed.** A monthly CI job pulls the latest backup, restores it
  into a scratch container and asserts the watermarks and outbox read back correctly. An untested
  restore is a hope with a cron schedule.
- **Stated recovery targets**, so "how bad is it" has an answer before it happens: losing the VPS
  costs under an hour (§11.5) and at most one day of alerts; losing the ops store costs one day;
  losing the raw zone is the only unrecoverable event, which is why it is the one thing with
  versioning, a delete-less token and a separate lifecycle policy.

### 11.4 Getting code onto the box

Undefined deployment is where a working system quietly becomes a snowflake. The whole process is
one command, and the same one every time.

```
merge to main ──▶ CI builds image, tags it ghcr.io/…/finflow:<git-sha> and :latest
                            │
   make deploy SHA=abc1234  ├──▶ ssh, docker compose pull, up -d, run smoke test
                            │
   make rollback            └──▶ same thing with the previous SHA
```

- **Images are tagged by git SHA**, never only `latest`. `latest` is a convenience for humans;
  every deploy and every rollback names a SHA, so "what is actually running" is answerable.
- **Rollback is a first-class command**, not a procedure to reconstruct at 06:00 while the digest is
  missing. It is exercised once, deliberately, during M6 — a rollback path that has never been run
  is not a rollback path.
- **A smoke test gates the deploy**: after `up -d`, run the pipeline against the synthetic source in
  dry-run mode and assert it produces a Decision. Failing that, the deploy rolls itself back. This
  catches a broken image at 22:00 rather than at 05:30.
- **The Dockerfile is boring on purpose**: multi-stage, `uv sync --frozen` from the lockfile, a
  pinned base **digest** rather than a moving `python:3.12` tag, non-root user, no build toolchain
  in the final layer. PySpark lives in a **separate image** — a JVM adds roughly a gigabyte, and the
  daily path has no reason to carry it.
- **Compose is one base file plus a `prod` overlay**, and the overlay differs only in image tags,
  resource limits, restart policies and log configuration. Never in behaviour — environment-
  conditional business logic is forbidden, so `FINFLOW_ENV` never selects a code path.
- **The ops store has migrations.** The analytical store is disposable so its schema can be
  recreated at will; `ops.sqlite` is authoritative and cannot be dropped, so adding
  `positions_actual` to a running system is a versioned migration applied on start, with the
  migration table checked by the smoke test. This is the one piece of state where a bad deploy is
  not recoverable by rebuilding.

### 11.5 The box

A single small VPS is a pet unless it is reproducible, and it is exposed to the internet within
minutes of existing.

- **Rebuildable from a single `cloud-init.yaml`**: create user, install Docker, pull the compose
  files, load secrets, start. The recovery procedure is "provision a new box, run one command,
  restore the ops store" — and it is timed once so §11.3's target is a measurement rather than a
  guess.
- **Nothing binds to a public interface.** Every published port is `127.0.0.1:<port>`, and access is
  over an SSH tunnel or Tailscale. This matters more than it sounds: Docker inserts its own
  iptables rules and **publishing a port bypasses UFW entirely**, so a `ports: 8000:8000` with a
  tidy firewall behind it is still open to the world. Explicit localhost binding is the fix, and it
  is a review rule, not a preference.
- Baseline hardening: key-only SSH, no root login, `unattended-upgrades` for security patches,
  fail2ban. Done once, at the start.
- Secrets on the box live in a single root-owned `.env` (mode 600) referenced by compose, never in
  the compose file, never in the image, never in a build arg.

### 11.6 What fills up, what locks, and what crash-loops

Three unglamorous failure modes account for most small-host outages. Each gets an explicit answer,
because each is invisible until the morning it is not.

**Disk fills.** A 40 GB box running Docker accumulates faster than intuition suggests, and the data
in §2 is the *smallest* thing on it:

| What grows | Rate | Bound |
|---|---|---|
| Container logs | unbounded by default | `json-file` driver, `max-size: 10m`, `max-file: 3`, set in the compose base |
| Dagster run and event storage | ~40 partitions × N assets × daily, forever | Retention policy; runs older than 90 days purged by a weekly job |
| MLflow artifacts | one model per monthly refit | Keep the last 12 plus any referenced by a promoted version |
| Docker images and build cache | every deploy | `docker image prune` weekly; keep the last three SHAs for rollback |
| The actual data | ~25 MB/year | not the problem |

A disk-usage check runs with the daily pipeline and warns into the digest at 75%. Filling the disk
is the single most likely way this system dies, and it is entirely preventable.

**Two writers.** §4.5 documents that only the pipeline writes the warehouse, but documentation does
not prevent a manual backfill from starting while the scheduled run is in flight — which is exactly
the scenario that occurs, because a backfill is something you kick off *because* something looked
wrong. So the constraint is enforced: an `flock` on the CLI entrypoint, Dagster run-concurrency
limits on the tagged pipeline, and a `concurrency:` group on the GitHub workflow. A second run waits
or exits cleanly; it never corrupts and never half-writes.

**Crash loops.** `restart: unless-stopped` will restart a broken alert worker forever, silently,
and the dead-man's switch does not cover it because the *pipeline* still succeeds. So each
long-running service has a compose healthcheck, and a restart count above a threshold is reported
into the digest. A service that is down is fine; a service that is down and nobody knows is not.

**Schedule realism.** Everything is scheduled in UTC — never local time, because DST silently shifts
a local-time cron twice a year. The run is timed for well after the US close, tolerates a late
vendor rather than failing hard, and retries mid-morning before declaring a bad day. GitHub Actions
cron can fire fifteen or more minutes late under load, so nothing is scheduled tight against a
deadline.

### 11.7 Supply chain and secrets

Small, cheap, and each closes a hole that a solo project usually leaves open:

- **Secret scanning** (`gitleaks`) in pre-commit *and* CI. Pre-commit alone is not enough — it is
  bypassable with `--no-verify`, and the one time it matters is the one time someone is in a hurry.
  A committed token is the only mistake here with a real-world cost.
- **Dependency updates and advisories** via Dependabot, grouped weekly so it is one PR rather than
  forty. `pip-audit` in CI fails on a known-vulnerable dependency.
- **Container scanning** with `trivy` on the built image, failing on high severity.
- **Lockfile-only installs everywhere** — `uv sync --frozen` in CI and in the image, so the thing
  tested is the thing shipped.
- **Least-privilege credentials, separated by role**: a pipeline token that can read and write the
  raw bucket but not delete, a distinct backup token scoped to the backup bucket, a read-only token
  for anything that only reads. Rotation is documented in the runbook with the blast radius of each
  — for a solo project the realistic policy is "rotate on suspicion and on schedule annually",
  written down rather than implied.
- **Never `pull_request_target`** in any workflow. It is the one GitHub Actions footgun that hands
  secrets to untrusted code, and it is the most common way a repository leaks a token.

---

## 12. Extension — the Lakehouse

Once the MVP is complete, green and running daily, the same system is promoted onto lakehouse
infrastructure. The dbt models and application use cases survive nearly unchanged — which is the
architecture's testable hypothesis, not a slogan: if §10's dialect-neutrality constraint and §4.1's
dependency rule held, A1 and A2 are adapter swaps. If they did not, that is worth finding out.

**Stated plainly: at 300k rows, none of this is required.** Each step is justified by what it
teaches or proves, and any can be skipped without harming the working system.

```
  Stooq · FRED  ──▶ batch ingest ──┐
                                   ├──▶  BRONZE  raw, append-only, quarantine
  Finnhub WS ──▶ producer ──▶      │        Delta Lake on MinIO (S3 API)
                  Redpanda         │
  Postgres  ──▶ Debezium CDC ──────┘
  (ops store)   strategies, subscriptions
                                   ▼
                        PySpark batch jobs
                  SILVER  deduped · adjusted · calendar-aligned
                                   ▼
                           dbt (star schema)
                  GOLD    same marts as MVP, same SQL
                                   ▼
        ┌──────────────┬────────────────┬─────────────────────┐
        │  MLflow      │ Backtest engine│ FastAPI + gRPC      │
        └──────────────┴────────────────┴─────────────────────┘

  Dagster · Great Expectations · Grafana · Terraform on AWS
```

| Step | Change | What it genuinely buys, here |
|---|---|---|
| A1 | DuckDB → Delta Lake on MinIO | Time travel replaces the hand-rolled manifest of §6.2 — a real simplification, and the one step with a concrete payoff at this scale. Also the first hard test of the `Warehouse` port |
| A2 | Transforms → PySpark | Nothing, at 300k rows. Kept to produce the `docs/SCALING.md` benchmark and to prove the compute layer was swappable |
| A3 | Finnhub WS → Redpanda → Structured Streaming | Genuinely new capability: intraday signals rather than end-of-day. The only step that changes what the product can do |
| A4 | Debezium CDC on Postgres | The ops store of §4.3 becomes Postgres, which CDC needs anyway. Polling would do; CDC is the learning goal and is labelled as such |
| A5 | Terraform → AWS (S3, Glue, EMR Serverless) | Reproducible infrastructure — and the point where it stops being free, so it stays `plan`-only unless deliberately funded |
| A6 | OpenTelemetry → Prometheus → Grafana | Freshness, lag and quality over time. Real value after months of unattended running |
| A7 | gRPC `SignalService.StreamSignals` | Streaming interface for programmatic consumers |

**Migration is incremental.** Each step ships independently and the system stays working throughout.

---

## 13. Engineering details worth building in

Cheap to add, and each addresses a failure mode a naive pipeline hits in production.

- **Enforced dependency rule.** `import-linter` contracts in CI (§4.1). Architecture that is not
  enforced is documentation of an intention.
- **No ambient time.** `Clock` injected; a test greps `domain` and `application` for `datetime.now`
  and `date.today` (§4.2).
- **Prefix-stability property test** over random dates (§7.3) — one test that subsumes the whole
  lookahead family.
- **Quarantine and narrow circuit breaker.** Malformed rows land in `bronze_quarantine` with a
  reason; failed checks block *that instrument's* downstream assets (§4.4).
- **Quorum policy on cross-sectional strategies.** Below `min_quorum`, hold rather than rotate into
  a distorted ranking (§4.4).
- **Restatement detection.** Re-fetched history that disagrees with stored history is recorded, not
  absorbed (§6.2).
- **Vendor-error detection.** Stooq returns HTML on rate limit; the client validates shape before
  parsing, tested against a captured real error page (§6.7).
- **Point-in-time macro.** Release-date joins and ALFRED vintages (§6.3).
- **Per-instrument partitioning.** Adding the 40th ETF costs the same as adding the 4th.
- **Single-writer discipline.** One read-write DuckDB connection ever; serving reads a promoted
  snapshot; a test asserts the API cannot acquire a write lock (§4.5).
- **Reproducible backtests.** Keyed by `hash(strategy_ast + registry_commit + manifest + code_sha)`,
  with the AST persisted alongside.
- **Decision-level idempotent alerts.** Unique on `(strategy_id, strategy_version, decision_id)`;
  delivery is atomic per decision; a restart re-sends nothing and delivers no half-rotations (§9.4).
- **Transactional outbox** in the operational store — no lost and no duplicated messages.
- **Survivorship handling.** Recorded, structurally supported, residual measured and published
  (§6.5).
- **Versioned contracts** with an additive-only policy inside a major version, in
  `docs/CONTRACTS.md` (§7.4).
- **Deflated Sharpe and PBO computed automatically**, with the trial count read out of
  `fct_backtest_runs` rather than estimated (§8.3). Every reported Sharpe carries a confidence
  interval.
- **Per-instrument cost floors and a liquidity gate** in the registry, with slippage scaling in
  volatility (§5.7). A strategy may raise a cost assumption, never lower it.
- **Tradeability separated from research.** Live decisions filter to `tradeable_eu`; any strategy
  whose live universe differs from its research universe reports both (§5.7).
- **Net-of-tax and PLN reporting** alongside gross USD, so turnover is priced the way the account
  experiences it (§8.4).
- **Portfolio netting across strategies** before anything becomes an instruction, with gross, single
  name and minimum-trade caps (§7.6).
- **User controls that never suppress silently** — pause, mute and hold are always listed in the
  digest, because a control the user forgot they set is indistinguishable from a bug (§7.7).
- **A decision journal** capturing why an instruction was followed, overridden or ignored, written
  while the reasoning still exists and reviewed quarterly (§7.7).
- **Enforced single-writer, not just documented** — `flock`, Dagster concurrency limits and a
  workflow concurrency group, because a manual backfill during a scheduled run is the realistic
  collision (§11.6).
- **Bounded growth**: log rotation, Dagster run retention, MLflow artifact retention, image pruning,
  and a disk check that warns into the digest at 75%. Filling the disk is the likeliest way a small
  host dies (§11.6).
- **One-command deploy and rollback**, images tagged by git SHA, gated by a post-deploy smoke test
  against the synthetic source (§11.4).
- **Migrations on the operational store**, since it is the one piece of state a rebuild cannot
  recreate (§11.4).
- **Backups offsite, encrypted, and restore-tested monthly in CI** — an untested restore is a hope
  with a cron schedule (§11.3).
- **Least-privilege credentials**: the pipeline's object-store token can read and write but not
  delete (§11.7).
- **Secret, dependency and image scanning in CI**, with `gitleaks` in both pre-commit and CI because
  pre-commit is bypassable (§11.7).
- **Architecture decision records** in `docs/adr/` — the dependency rule; two stores rather than one;
  registry as code; DuckDB versus Postgres; append-only raw zone; AST as the strategy interface;
  price versus total return; ML as an optional port; Delta versus Iceberg; Dagster versus Airflow.
- **`docs/RUNBOOK.md`** — what each alert means and the first three things to check, written the day
  the pipeline starts running unattended.
- **`docs/DATA_QUALITY.md`** — every check, its threshold, its owner, and what happens when it fires.

---

## 14. Comparison of the two stages

| Dimension | MVP — Lean Warehouse | Extension — Lakehouse |
|---|:---:|:---:|
| Analytical storage | DuckDB file + Parquet in R2 | Delta Lake on S3/MinIO |
| Operational storage | SQLite | Postgres (A4 needs it for CDC) |
| Compute | In-process Polars + one Spark benchmark | PySpark throughout |
| Latency | End-of-day | Intraday streaming |
| Runs end-to-end in CI | Fully | Partly |
| Local RAM required | < 1 GB | ~12 GB |
| Adding an instrument | Registry edit | Registry edit (unchanged) |
| Application layer | Unchanged | Unchanged |
| dbt marts | Same SQL | Same SQL |
| **Needed for 300k rows** | **yes** | **no, and the docs say so** |

The last four rows are the architecture's claim about itself: neither the extensibility mechanism
nor the business logic changes between stages, only the adapters underneath — and the project is
willing to say when those adapters are unnecessary.

---

## 15. The trust ladder — how much money follows how much evidence

The most important product decision in this document is not a feature. It is the rule connecting
what the system has proven to how much the user acts on it, decided now, in advance, while it is a
calm choice rather than one made in the middle of a good month.

Four rungs. Each has an entry gate that is checkable, not a feeling.

| Rung | What the user does | Gate to reach it |
|---|---|---|
| **0 — Shadow** | Nothing. The system runs, computes decisions, sends the digest; the user reads it and acts on none of it | Reached at M4 |
| **1 — Paper** | Records the system's targets in a paper book alongside the real portfolio | 30 consecutive days of green runs; every digest reconciled by hand at least once a week and found correct |
| **2 — Sleeve** | A capped share of savings — start at 5%, never more than 10% — follows the netted target | 3 months at rung 1 with **zero** instructions issued on stale or unvalidated data; paper tracking within tolerance of the backtest; PBO below 0.5 and the deflated Sharpe interval excluding zero (§8.3) |
| **3 — Allocation** | A deliberately sized allocation, still capped, still alongside a passive core | 12 months at rung 2; live performance degradation from backtest within the expected range; at least one real incident handled without a wrong instruction reaching the user |

Three rules make the ladder real rather than decorative:

- **Rungs are climbed slowly and dropped immediately.** Any incident where an instruction was issued
  on bad data drops the system one rung, regardless of the outcome. The outcome is irrelevant —
  being right by luck on unvalidated data is the same defect as being wrong.
- **Time at a rung is not a substitute for evidence, and evidence is not a substitute for time.**
  Both gates must pass. A strategy with a beautiful deflated Sharpe and six weeks of live history
  has not shown that the *pipeline* is trustworthy, which is a different question from whether the
  *strategy* is.
- **The passive core is never in scope.** The system manages a sleeve at the edge of a portfolio
  whose centre is a boring index allocation it does not touch. Nothing in this document is an
  argument for changing that, and the ceiling at rung 3 exists to keep the question closed.

The honest expectation, stated here so it is not a disappointment later: cross-sectional momentum
with a trend filter is one of the better-supported anomalies, and after realistic costs, 19% tax on
a monthly rebalance and the FX exposure of §8.4, it will most likely land **at or near a passive
60/40** for an account this size. Volatility targeting is the more probable real improvement, and
it improves risk rather than return. A system that reaches rung 2, reports that clearly, and lets
its author stop wondering has done its job — the alternative was wondering indefinitely.

---

## 16. Repository layout

The tree follows §4.1 exactly, so the dependency rule is visible from the filesystem and a violation
is obvious in a diff before `import-linter` even runs.

```
finflow/
├── README.md                  # architecture diagram, quickstart, results, live status badge
├── PROJECT.md                 # this document
├── IMPLEMENTATION.md          # milestone-by-milestone build plan
├── Makefile                   # make up · make demo · make test · make backfill · make daily
├── docker-compose.yml
├── pyproject.toml
├── .importlinter              # the dependency rule, enforced in CI
├── .github/workflows/
│   ├── ci.yml                 # lint → type → imports → registry → test → dbt → build
│   └── daily.yml              # the scheduled production run (M4 → M6)
├── instruments/               # THE REGISTRY — add an ETF here
│   ├── equity_us.yml · commodities.yml · rates_credit.yml · macro.yml · universes.yml
├── deploy/
│   ├── cloud-init.yaml        # the whole box, reproducibly
│   ├── compose.prod.yml       # image tags, mem_limits, restart, log rotation — no behaviour
│   ├── backup.sh              # VACUUM INTO → gzip → age → offsite bucket
│   └── smoke.sh               # post-deploy: synthetic run must produce a Decision
├── migrations/                # versioned ops-store migrations (the one non-rebuildable state)
├── docs/
│   ├── adr/ · DATA_QUALITY.md · CONTRACTS.md · RUNBOOK.md · RESULTS.md
│   ├── ADDING_AN_INSTRUMENT.md · SCALING.md · architecture.png
├── src/finflow/
│   ├── contracts/             # frame + record schemas, versioned. Imports nothing internal
│   ├── domain/                # calendars · costs · metrics · expression AST · decide() · features
│   ├── registry/              # immutable registry value object + loader
│   ├── ports/                 # SourceClient · ObjectStore · Warehouse · OpsStore
│   │                          # Notifier · Clock · ModelProvider
│   ├── adapters/
│   │   ├── sources/           # stooq · fred · twelvedata · synthetic
│   │   ├── storage/           # local fs · s3/r2
│   │   ├── warehouse/         # duckdb
│   │   ├── ops/               # sqlite
│   │   ├── notify/            # telegram
│   │   └── models/            # mlflow + lightgbm, behind ModelProvider
│   ├── application/           # IngestUniverse · BuildWarehouse · EvaluateStrategies
│   │                          # RunBacktest · DeliverAlerts
│   ├── compute/               # polars/ (default) · spark/ (M10 benchmark)
│   └── entrypoints/           # cli/ · api/ · ui/ · dagster_defs/   ← the only wiring
├── data/                      # local raw zone · warehouse.duckdb · serving.duckdb · ops.sqlite
│                              # gitignored; everything but ops.sqlite is rebuildable (§4.3)
├── dbt/                       # staging → intermediate → marts, contracts, tests, docs
├── strategies/                # example YAML strategies · portfolio.yml (netting, §7.6)
└── tests/                     # unit · property · contracts · spark (chispa) · integration · dbt
```

---

## 17. Next steps

See `IMPLEMENTATION.md` for the milestone-by-milestone build plan.
