# FinFlow — Implementation Plan

Milestone-by-milestone build plan. See `PROJECT.md` for architecture and rationale; §4 there defines
the layering that this plan builds outward-in.

**Rule for every milestone:** it is not done until CI is green and the acceptance criteria pass.
Never start the next milestone on a red build.

**Second rule:** the plan is ordered by **what it gives you**, not by technical dependency. The
pipeline becomes useful daily at M4, and everything after that improves something already running.
If the work stalls at any point past M4, what is left is a working system rather than a half-built
one — which matters more here than anywhere, because a side project's real failure mode is not
running out of skill but running out of evenings.

**Third rule:** build **outward-in**. Domain and contracts first, adapters second, entrypoints last.
The reason is practical rather than aesthetic — a use case written before its adapters is testable
on the day it is written, whereas one written after them arrives already tangled with a vendor
client and a database handle.

---

## Sequencing

| Milestone | What you can do once it is done |
|---|---|
| M0 ✅ | — |
| **Stage 1 — vertical slice: get it running daily** | |
| M1 · Architecture skeleton and instrument registry | Add an instrument in one file |
| M2 · Ingestion — ports, adapters, error taxonomy | Have the price history on disk |
| M3 · Two stores and dbt marts | Query a clean series |
| **M4 · First light — decision → Telegram, on a schedule** | **Read a daily digest. Rung 0 of the trust ladder** |
| | *from here it is in daily use, and everything after improves it* |
| **Stage 2 — widen and harden** | |
| M5 · Full universe, costs, tradeability | Act on instruments you can actually buy |
| M6 · Orchestration, data quality, deployment | Trust that a bad day produces no instruction. **Rung 1** |
| M7 · Features, strategy compiler, engine, portfolio | Write a strategy in YAML; get one netted instruction a day |
| **Stage 3 — depth and confidence** | |
| M8 · Models and deflated evaluation | Know how much to believe it. **Gate for rung 2** |
| M9 · Serving — API and UI | Look at a backtest instead of reading a message |
| M10 · Compute benchmark and documentation | Pick it up again after six months away |
| **Stage 4 — optional** | |
| A1–A7 · Lakehouse extension | Nothing the user asked for — see `PROJECT.md` §12 |

**Critical path:** M0 → M1 → M2 → M3 → M4. Everything after M4 improves a running system and can be
reordered to taste. M10 can run alongside M8 and M9.

M7 is by some distance the largest, and is meant to be split — features, then the compiler, then the
engine — rather than attempted in one run at it.

### The cut list

Written down now, while it is a calm decision rather than one made at the point of giving up:

1. **A1–A7 entirely.** `PROJECT.md` §12 already says the lakehouse is optional, and the sequencing
   table marks it as delivering nothing the user asked for.
2. **The Streamlit UI.** Telegram is the daily interface, so the bot commands stay. Keep the API —
   it is small, and it is what the UI would have consumed.
3. **M8's models.** `ModelProvider` has a null implementation and strategies declare
   `on_missing_model: strict`, so cutting ML costs a section of `RESULTS.md` and nothing
   operational. One exception, below.
4. **The Spark job in M10.** Keep `docs/SCALING.md` — it is the actual deliverable. Note honestly
   what cutting the job costs: the crossover stops being a measurement. The document then reports
   measured Polars wall time and peak memory at 300k, 3M and 30M synthetic rows, states the
   crossover as an argument from published Spark benchmarks, and **says which of the two each number
   is**. A labelled estimate is a fine artifact; an unlabelled one is the thing this project exists
   to avoid.

### What never gets cut

These survive regardless of which milestone slips. Together they are the difference between a system
that can be trusted with money and one that cannot:

- **The import-linter contracts** (M1). Unenforced architecture is a description of the past.
- **`gitleaks`** (M1). A committed token is the only mistake here with a cost outside the project.
- **The delete-less object-store token** (M2). The raw zone is the one unrecoverable asset.
- **The two-store split** (M3). Without it, "the warehouse is disposable" is simply false.
- **The scheduled run and the dead-man's switch** (M4). Everything else is a demo without them.
- **The run mutex, the offsite backup and the ops-store migrations** (M4/M6) — the three that cannot
  be fixed after the fact.
- **Per-instrument cost floors and `tradeable_eu`** (M5). Instructions have to be executable and
  honestly priced.
- **Retention and the disk check** (M6). The likeliest way the host dies.
- **Portfolio netting** (M7). Four contradictory alerts are worse than none.
- **The prefix-stability property** (M7). The one test that keeps lookahead out.
- **Deflated Sharpe, PBO and confidence intervals** — nominally M8, but they belong to the backtest
  engine and **move into M7 if M8 is cut**, since they matter most when there is no model to blame.

Everything else is negotiable.

### How much architecture, and where to stop

The dependency rule, the port protocols, the `Clock` and the composition root are a small amount of
work spread across M1–M3, and they pay for themselves the first time a test needs a fake source, the
first time the alert worker and the pipeline want to write at once, and again at A1.

The counter-discipline from `PROJECT.md` §4.6 applies throughout: **a package appears when it has
two implementations or two consumers, not before.** Do not scaffold six empty directories in M1.

---

# Stage 0 — foundation

## M0 — Repository foundation  ✅ DONE (2026-08-26)

- [x] `pyproject.toml` with `uv`, package under `src/finflow/`
- [x] Tooling config: ruff (lint + format), mypy `strict = true`, pytest with coverage
- [x] `.pre-commit-config.yaml`: ruff, ruff-format, mypy, trailing whitespace, YAML check
- [x] `Makefile`: `install`, `lint`, `format`, `typecheck`, `test`, `test-fast`, `cov`, `check`,
      `clean`, plus stubs for `up`, `down`, `demo`, `backfill`, `daily`, `docs`
- [x] `.github/workflows/ci.yml`: lint → typecheck → test, on push and PR
- [x] `.env.example` with every key documented; real `.env` gitignored
- [x] `.gitignore`; ADRs `0001-record-architecture-decisions`,
      `0002-configuration-via-pydantic-settings`
- [x] Structured logging via `structlog`, JSON in non-TTY environments
- [x] `src/finflow/config.py` — Pydantic `Settings` reading env, no scattered `os.getenv`

**Result:** 12 tests passing, 98.7% coverage (floor 80%), mypy strict clean, ruff clean. Python
3.12.13 pinned via `uv`.

---

# Stage 1 — the vertical slice

Goal of the whole stage: **by the end of M4, a scheduled job that runs without you and sends a
Telegram message.** Eight instruments, two sources, one rule, no ML, no orchestrator, no UI. Resist
widening anything until it is green and scheduled.

## M1 — Architecture skeleton and instrument registry

*Goal: the dependency rule is enforced from the first commit of real code, and adding an ETF is a
one-file edit from this point forward.*

### Tasks — skeleton
- [ ] Create only the packages this milestone needs: `contracts/`, `domain/`, `registry/` and
      `ports/`. `ports/` earns its place immediately under the §4.6 rule — `Clock` has two
      implementations on day one — and `SystemClock` is therefore the first `adapters/` module.
      `application/` and `entrypoints/` arrive in M2, when they have something to hold
- [ ] `.importlinter` with layer contracts per `PROJECT.md` §4.1, plus a forbidden-import contract:
      `domain` may not import `polars.io`, `httpx`, `duckdb`, `dagster`
- [ ] CI job `imports`: `lint-imports`, running before the test job so a violation fails fast
- [ ] `gitleaks` in pre-commit **and** as a CI job — pre-commit is bypassable with `--no-verify`, and
      a committed token is the only mistake in this project with a real-world cost
- [ ] Dependabot, grouped weekly; `pip-audit` in CI. Cheap to set up, and it stops the dependency
      graph rotting quietly between bursts of work
- [ ] `Clock` protocol in `ports/`, `SystemClock` in `adapters/`, `FrozenClock` in `tests/` —
      placed now, because every later milestone would otherwise reach for `date.today()`
- [ ] Guard test: AST-walk every package under `src/finflow/` **except `adapters/` and
      `entrypoints/`**, failing on `datetime.now` / `date.today` / `time.time`. Written as an
      exclusion rather than a list, it covers `domain` and `registry` today and picks up
      `application` in M2 with no edit — which is the point, because `application` is where ambient
      time actually creeps in (`PROJECT.md` §4.2)
- [ ] ADR: the dependency rule, and what it buys (testability, A1 migration, no Dagster in logic)

### Tasks — registry
- [ ] Pydantic models: `Instrument`, `UniverseMember`, `Universe`, `MacroSeries`, `Registry`
- [ ] Loader merging every `instruments/*.yml` into one validated, **immutable** `Registry`,
      constructed once at the composition root and injected — never a module-level singleton
- [ ] Git SHA and commit date resolved **at load time** and stamped into the object, so nothing
      downstream shells out to git mid-computation
- [ ] Validation rules:
  - unique symbols across all files
  - every universe member exists as an instrument
  - `backfill_start >= inception`; `delisted > inception` when set
  - every declared source key is one the project implements — checked at M1 against the source-key
    enum in `contracts/`, and tied to the concrete client registry in M2 once clients exist
  - valid `calendar` code recognised by `exchange_calendars`
  - `return_basis` in `{price, total}`; MVP asserts `price` everywhere (`PROJECT.md` §6.4)
- [ ] Date-effective membership: `members` accepts a bare symbol or `{symbol, from, to}`, and
      `registry.universe("sectors", as_of=date)` resolves it. XLRE (2015) and XLC (2018) are the
      test cases — a 2010 backtest of `sectors` must see nine members
- [ ] Seed **only the vertical-slice subset** — eight instruments; the full universe is M5. The
      slice uses the *final* universe names with fewer members, so M5 only ever **adds** members and
      never redefines one. An earlier draft put TLT and HYG in `equity_core`, which would have made
      M5 a silent change of meaning for a name the strategies already referenced:
  - `precious_metals` — GLD, IAU, SLV, GDX  (benchmark GLD, final)
  - `equity_core` — SPY, QQQ  (benchmark SPY, final)
  - `rates_credit` — TLT, HYG  (benchmark TLT until IEF arrives in M5 — the one slice value that
    does change, which is safe only because no backtest is persisted before M7)
- [ ] `instruments/macro.yml` — DFII10, DTWEXBGS, VIXCLS, with `release_lag_days` and `revised`
- [ ] Query helpers: `enabled()`, `universe(name, as_of=)`, `sources_for(symbol)`, `commit`
- [ ] Tests: valid registry loads; duplicate symbol fails; unknown member fails; bad calendar fails;
      membership as-of 2010 excludes XLRE; the `Registry` is genuinely immutable
- [ ] CI job `registry`: validate every `instruments/*.yml` on each PR
- [ ] `docs/ADDING_AN_INSTRUMENT.md`; ADR: registry as code versus database table

### Acceptance
- `lint-imports` passes, and deliberately adding `import duckdb` to `domain/` fails CI
- The registry loads and validates in under 100 ms
- Every invalid-registry test fails for the *right* reason, asserted on the message
- `registry.universe("sectors", as_of=date(2010,1,1))` returns nine members
- Adding an instrument requires touching exactly one YAML file

---

## M2 — Ingestion: ports, adapters, error taxonomy

*Goal: raw data on disk, append-only, for any registered instrument — behind interfaces that make
the vendor replaceable and the tests hermetic.*

Scope is Stooq + FRED + synthetic. Twelve Data reconciliation is M5.

### Tasks — ports and contracts
- [ ] `ports/`: `SourceClient` (`fetch(symbol, start, end) -> pl.DataFrame`, `capabilities()`),
      `ObjectStore` (write-once keys, ordered listing, no update-in-place)
- [ ] **Error taxonomy** in `contracts/errors.py` (`PROJECT.md` §6.7): `SourceRateLimited`,
      `SourceUnavailable`, `SymbolNotFound`, `MalformedResponse`, `AuthenticationFailed`
- [ ] Retry/back-off policy keyed by **error class, in the shared ingestion service** — never inside
      a client. Clients raise; they do not retry. This is what stops five clients growing five
      subtly different retry loops
- [ ] `contracts/frames.py`: Patito schemas for the canonical OHLCV and macro frames — one
      definition, validated in a single vectorized pass, not per-record Pydantic (`PROJECT.md` §9.1)
- [ ] `application/ingest_universe.py`: the use case, depending only on ports

### Tasks — adapters
- [ ] `StooqClient` — CSV over HTTPS, symbol mapping from the registry. **Must detect the rate-limit
      response**: Stooq returns an HTML page with HTTP 200 when the cap is hit. Validate content type
      and header row *before* parsing; raise `SourceRateLimited`. Test against a captured real page
      (`tests/fixtures/stooq_ratelimit.html`) — without this, an error message gets ingested as a bar
- [ ] `FredClient` — macro series. Pass ALFRED `realtime_start`/`realtime_end` for `vintage_aware`
      series; store `(series_id, observation_date, vintage_date, value)`
- [ ] `SyntheticClient` — deterministic seeded OHLCV with realistic vol clustering, for CI and demos
- [ ] `LocalObjectStore` and `S3ObjectStore` behind the one port; in-memory fake for tests
- [ ] R2 token scoped **read + write, no delete**, and bucket versioning enabled before the first
      real backfill. The raw zone is the one unrecoverable asset (`PROJECT.md` §11.3)
- [ ] Token-bucket rate limiting per source, configured in settings
- [ ] Request/response logging with timing and byte counts

### Tasks — storage and state
- [ ] **Append-only landing zone** (`PROJECT.md` §6.2):
      `raw/source=<s>/symbol=<sym>/ingested=<iso8601>/data.parquet`, never rewritten
- [ ] **Manifest per run**: the ingestion-run ids admitted per `(source, symbol)`;
      `snapshot_id = hash(manifest)`. Not `max(ingested_at)` — that would make a GLD backfill look
      like every instrument changed
- [ ] Watermarks in the operational store from the start: a minimal `ports/ops_store.py` and a
      SQLite adapter holding **only** `watermarks` —
      `(source, symbol) -> last_loaded_date, last_run_at, row_count, deferred_until`.
      M3 formalises the port and adds `pipeline_runs`, backup/restore and migrations; M4 adds the
      outbox. The port appears here rather than in M3 because M2 already has two consumers of it
- [ ] `deferred_until` is how `SourceRateLimited` resumes cleanly on the next run
- [ ] ADR: the append-only raw zone, and why idempotency here is *convergent* rather than identical
      (`PROJECT.md` §6.2)

### Tests
- [ ] Recorded HTTP fixtures (`respx`); no live network in the unit suite
- [ ] Each error class raised by a fixture and mapped to the right policy
- [ ] **Convergence test** — ingest twice, assert bronze resolves to one row per `(symbol, date)`
      with the later `ingested_at`. The original "byte-identical partitions" criterion is impossible
      (`PROJECT.md` §6.2)
- [ ] **Port conformance suite** — one parametrized test class run against every `SourceClient`
      implementation including the synthetic one, so a new source proves itself against the same
      contract. This is the test that makes "adding a source is one interface" true
- [ ] Integration test marked `integration`, hitting real sources, run nightly and manually — never
      on PRs

### Acceptance
- `make backfill` populates the eight slice instruments from `backfill_start` to today
- Running it twice leaves both raw partitions on disk and one row per `(symbol, date)` in bronze
- The unit suite passes with networking disabled
- A simulated Stooq rate-limit page produces `SourceRateLimited` and a `deferred_until`, not a bar
- A source outage degrades gracefully: other instruments complete, the failure is recorded
- The port conformance suite passes for all three clients

---

## M3 — Two stores and dbt marts

*Goal: a modeled star schema you can query, and a clean separation between what is derived and what
is authoritative.*

### Tasks — the store split *(`PROJECT.md` §4.3)*
- [ ] `ports/warehouse.py` and `ports/ops_store.py`
- [ ] `DuckDBWarehouse` — **exactly one** read-write connection, owned by one object; every other
      consumer gets `read_only=True`. A test asserts a second writer is rejected
- [ ] `SqliteOpsStore` in WAL mode — `pipeline_runs`, `watermarks`, and (M4) the outbox. Small,
      authoritative, two concurrent writers
- [ ] Backup and **restore** script for the ops store; the restore path is exercised in a test, not
      assumed
- [ ] ADRs: two stores rather than one — why "the warehouse is disposable" is false if the outbox
      lives in it; DuckDB versus Postgres for the analytical store; price return versus total return
      in the MVP (`PROJECT.md` §6.4)

### Tasks — warehouse
- [ ] Loader: raw Parquet → `bronze_ohlcv` / `bronze_macro`, resolving to the latest `ingested_at`
      per `(symbol, date)` and stamping `snapshot_id` from the manifest
- [ ] `bronze_quarantine` for contract failures, with a rejection reason and the captured payload
- [ ] `dq_restatements` — a re-fetch that changes a stored value is recorded with both run ids, not
      absorbed (`PROJECT.md` §6.2)
- [ ] dbt project (`dbt-duckdb`) in three layers:
  - **staging** — `stg_ohlcv`, `stg_macro`: typed, renamed, deduplicated on `(symbol, date, source)`
  - **intermediate** — `int_calendar_aligned`, `int_macro_released` (joins on `available_from =
    observation_date + release_lag_days`, `PROJECT.md` §6.3), `int_source_reconciled`
  - **marts** — `dim_instrument` (SCD2, `valid_from` = registry commit date), `dim_universe`,
    `bridge_universe_member` (date-effective), `dim_date`, `dim_source`, `fct_ohlcv_daily`,
    `fct_macro_daily`
- [ ] **`contract: enforced`** on every mart model — column names and types enforced in the
      warehouse, not only in Python (`PROJECT.md` §9.1)
- [ ] **Dialect neutrality in marts**: DuckDB-specific SQL confined to staging and macros, so the
      A1 claim is falsifiable. A `grep` check in CI over `models/marts/` for a small deny-list
- [ ] dbt tests: `unique`, `not_null`, `relationships`, `accepted_values`, plus dbt-expectations for
      `close > 0`, `high >= low >= 0`, `high >= close >= low`, row-count ranges
- [ ] Custom generic test: **no gaps on trading days** per instrument, calendar-aware
- [ ] Incremental materialisation on facts, partitioned by instrument
- [ ] Atomic snapshot promotion to `serving.duckdb` on a successful build — the read-only copy
      every consumer in `PROJECT.md` §4.5 reads from
- [ ] `dbt docs generate` wired into the Makefile

### Acceptance
- `dbt build` completes green over the slice universe with contracts enforced
- `SELECT * FROM fct_ohlcv_daily WHERE symbol='GLD'` returns a clean continuous series
- Adding an instrument to the registry and re-running produces its rows with no model edits
- Deliberately corrupting a source row lands it in quarantine, not in the marts
- **Rebuild-from-raw test**: deleting `warehouse.duckdb` and rebuilding reproduces identical marts
- **Restore test**: dropping `ops.sqlite` and restoring from backup preserves watermarks
- A monthly macro series is null in the feature frame until its release date
- A second writer against the warehouse fails; a second writer against the ops store succeeds

---

## M4 — First light  ⭐

*Goal: the pipeline runs without you and tells you something. This is the milestone that turns the
repo into a system.*

The whole point is narrowness — but **nothing here is throwaway**. The previous plan had a hardcoded
rule "deleted at M7"; instead the rule is expressed as a hand-constructed AST passed to the same
`decide()` the compiler will target in M7. The surface syntax arrives later; the evaluation path is
the final one from day one.

### Tasks
- [ ] `domain/decision.py`: the `Decision` entity — one per strategy per evaluation, holding the
      full target portfolio, `as_of`, `strategy_version`, `snapshot_id` (`PROJECT.md` §9.4)
- [ ] `domain/evaluator.py`: `decide(features, ast, as_of) -> Decision`. Pure, clock-injected, no IO
- [ ] The first rule as a **hand-built AST** — SMA(20)/SMA(50) cross over the slice universe. Three
      node types, no parser. M7 adds the parser that produces the same shape
- [ ] `application/evaluate_strategies.py` and `application/deliver_alerts.py`
- [ ] `alerts_outbox` in the ops store, unique on `(strategy_id, strategy_version, decision_id)`,
      with claim-and-mark semantics. **Delivery is per-decision and atomic** — never per-instrument,
      or a crash mid-batch delivers half a rotation (`PROJECT.md` §9.4)
- [ ] `TelegramNotifier` behind the `Notifier` port; recording fake for tests. Every message carries
      `as_of`, `strategy_version` and `snapshot_id`
- [ ] `entrypoints/cli/daily.py`: ingest → load → `dbt build` → evaluate → deliver, each step logged
      and its status written to `pipeline_runs`. This is the composition root — the **only** place
      that constructs adapters
- [ ] **Daily digest** — a fixed-time message regardless of signals: bars ingested, freshest date per
      universe, checks passed/failed, restatements, decisions evaluated and fired
- [ ] `positions_actual` in the ops store plus a `/position <sym> <units>` command, and a digest line
      reporting **target versus actual drift** (`PROJECT.md` §7.6). This is what makes the daily
      message worth reading rather than worth muting — most days it should say no action
- [ ] `/pause`, `/mute <sym> <until>` and `/hold` (`PROJECT.md` §7.7). A paused strategy keeps
      computing and recording so the counterfactual survives; **nothing is ever suppressed silently**
      — the digest always lists what was withheld and why
- [ ] **Command intake without a long-lived process.** The GitHub Actions runner is ephemeral, so
      there is no bot daemon to poll for `/position`, `/pause`, `/mute` or `/hold` until the VPS
      arrives in M6. Instead the daily run drains pending updates with a single `getUpdates` call
      before it evaluates, applies and records them, and the digest states when each was applied.
      Commands therefore take effect on the next run rather than immediately, which is stated in the
      bot's reply rather than left for the user to discover. The `alert-worker` of `PROJECT.md` §4.5
      becomes a real process at M6; delivery until then is in-process in the same CLI run
- [ ] **Dead-man's switch** — ping healthchecks.io on success; grace window set so a missed run
      emails within the hour
- [ ] `.github/workflows/daily.yml` — scheduled workflow; R2 credentials and Telegram token from
      GitHub secrets; raw zone pushed to the bucket; `warehouse.duckdb` rebuilt from raw each run;
      `ops.sqlite` round-tripped
- [ ] Tests: exactly-once delivery across a simulated crash between claim and mark; a stale snapshot
      produces a message that says it is stale; a `FrozenClock` makes the whole run deterministic
- [ ] `concurrency:` group on the workflow so a manual backfill and the schedule cannot overlap, and
      an `flock` in the CLI entrypoint so the same holds locally (`PROJECT.md` §11.6)
- [ ] Schedule in **UTC**, timed well after the US close, tolerant of a late vendor, with a
      mid-morning retry before declaring a bad day
- [ ] Nightly ops-store backup: `VACUUM INTO` → gzip → age-encrypt → a **separate** bucket, 30 daily
      and 12 monthly retained
- [ ] Versioned migrations for `ops.sqlite`, applied on start. It is the one piece of state a rebuild
      cannot recreate (`PROJECT.md` §11.4)
- [ ] `docs/RUNBOOK.md` v1 — what each alert means, first three things to check

### Acceptance
- The scheduled workflow has run green on **seven consecutive days** with no manual intervention.
  This is the acceptance criterion; do not move on before it is met
- The digest is readable in ten seconds on a phone and says "no action" on a quiet day. If it takes
  longer or manufactures activity, fix that before widening anything — an unread digest is a system
  with no users (`PROJECT.md` §1.2)
- **Rung 0 of the trust ladder is reached** (`PROJECT.md` §15): the system runs, the user acts on
  nothing, and the 30-day clock toward rung 1 starts
- A Telegram digest arrives every day, and a decision message arrives when the rule fires
- Killing the workflow mid-delivery and re-running sends no duplicates and no partial portfolios
- Deliberately breaking the Stooq client causes a failure message, not silence
- Deleting the local warehouse and re-running rebuilds it from the bucket
- `lint-imports` still passes: `entrypoints` is the only package importing an adapter

---

# Stage 2 — widen and harden

Everything from here improves something already in daily use. Keep the daily run green throughout.

## M5 — Full universe, costs and tradeability

*Goal: prove the registry claim — widen from 8 instruments to ~40 with no code changes.*

### Tasks
- [ ] Extend the registry to the full universe: `precious_metals` (GLD, IAU, SGOL, SLV, GDX, GDXJ,
      SIL), `equity_core` (SPY, QQQ, IWM, EFA, EEM), `sectors` (eleven SPDRs, date-effective),
      `rates_credit` (TLT, IEF, SHY, LQD, HYG, TIP), `broad_commodities` (DBC, USO, UNG, DBA, PDBC),
      `cross_asset` (SPY, TLT, GLD, DBC, UUP, HYG)
- [ ] Remaining macro series: T10Y2Y, BAMLH0A0HYM2, CPIAUCSL (vintage-aware, monthly)
- [ ] `TwelveDataClient` — reconciliation on a rolling 30-trading-day window, quota-aware. It should
      require **no changes to `domain/`, `application/` or `dbt/`**; the honest boundary is the new
      module in `adapters/sources/`, one settings key and one line at the composition root. Anything
      beyond those three means the port was wrong, and that is worth fixing now rather than at A1
- [ ] `dq_source_agreement` and a divergence threshold per asset class
- [ ] Rate-limit budgeting: ~40 instruments daily against Stooq's per-IP cap, with sequencing,
      jitter and resumable partial backfill via `deferred_until`
- [ ] Backfill the full history — expect several sessions and rate limits; that is the milestone's
      real lesson and it belongs in the runbook
- [ ] **Per-instrument cost floors** (`costs: { commission_bps, spread_bps }`) with asset-class
      defaults, and slippage scaling in realized volatility. A flat 3 bps across GDXJ, SIL, UNG and
      PDBC understates the true round trip by 5–10× and manufactures alpha (`PROJECT.md` §5.7)
- [ ] `min_adv_usd` as a data-quality gate: below the floor, no signal is emitted that day
- [ ] **`ucits_equivalent` and a `tradeable_eu` universe.** Under PRIIPs an EU retail account cannot
      buy SPY, GLD, TLT or the SPDR sectors, so without this the system computes target portfolios
      that cannot be executed. Map what maps (CSPX, SGLN, DTLA, IHYU, EIMI), mark the rest
      research-only, and source the UCITS lines from Stooq's `.uk` / `.de` symbols
- [ ] Verify UCITS data quality against the US original: correlation of returns, tracking difference,
      and history length. Expect shorter history and thinner volume — record both
- [ ] Measure and record: total rows, on-disk size, full-rebuild wall time → `PROJECT.md` §2

### Acceptance
- ~40 instruments, the 6 research universes and `tradeable_eu` load, backfill and build with **no
  changes to `domain/`, `application/`, `dbt/` or the marts** — the new source costs one adapter
  module, one settings key and one composition-root line, and nothing else
- Daily run still green, still under a few minutes
- Reconciliation reports divergence for at least one instrument and does not block on it
- The scale table in `PROJECT.md` §2 contains measured numbers, not estimates
- Every instrument carries a cost floor and a liquidity floor; no strategy can undercut either
- `tradeable_eu` resolves to instruments actually purchasable from a Polish brokerage account

---

## M6 — Orchestration, data quality and deployment

*Goal: replace the CLI script with a real asset graph, and refuse to publish bad data — narrowly.*

The deployment target moves from GitHub Actions to the VPS here (`PROJECT.md` §11.1), because
Dagster wants a long-lived daemon.

### Tasks
- [ ] Dagster project in `entrypoints/dagster_defs/` with `MultiPartitionsDefinition`:
      `instrument` (dynamic, from registry) × `date` (daily)
- [ ] **Every asset is a thin wrapper over an M2–M4 application use case.** No joins, no business
      logic, no vendor calls inside an asset body. `lint-imports` forbids `application` and `domain`
      from importing `dagster`, which makes the rule mechanical rather than a code-review habit
- [ ] Asset graph: `raw_ohlcv` → `bronze` → dbt assets (auto-loaded) → `features` → `decisions` →
      `alerts`
- [ ] **Registry sensor**: detects new or newly-enabled instruments and registers their dynamic
      partitions — the mechanism that makes step 3 of the add-instrument workflow real
- [ ] Asset checks: freshness (per calendar), volume range, nullness, single-day move beyond ±30%
      not explained by a `dq_restatements` entry, cross-source divergence, restatement magnitude
- [ ] **Failure domains** (`PROJECT.md` §4.4) — the isolation unit is the instrument partition:
  - one instrument failing blocks that instrument's features, decisions and alerts; the other 39
    complete
  - universe-level assets **degrade rather than fail**, computing over healthy members and recording
    which were excluded and why
  - **`min_quorum` enforcement** — below it, the rebalance is skipped, previous positions held, and
    the digest says so. Ranking a distorted universe is the failure mode that produces confident
    wrong output
  - alert suppression is per-instrument, never global
- [ ] `dq_results`: every check run, its threshold, outcome and partition
- [ ] Daily schedule plus a manual backfill job; retry policy on ingest assets
- [ ] **Image and release pipeline** (`PROJECT.md` §11.4): multi-stage Dockerfile, pinned base
      digest, non-root user, `uv sync --frozen`; PySpark in a **separate** image so the daily path
      does not carry a JVM; `trivy` scan failing on high severity; push to GHCR tagged by git SHA
- [ ] `make deploy SHA=…` and `make rollback` — ssh, `compose pull`, `up -d`, smoke test. **Exercise
      the rollback once, deliberately.** A rollback path that has never been run is not a rollback
      path
- [ ] `deploy/smoke.sh` — post-deploy synthetic dry run that must produce a Decision, with automatic
      rollback on failure. Catches a broken image at 22:00 rather than 05:30
- [ ] `deploy/cloud-init.yaml` — the whole box from nothing: user, Docker, compose files, secrets,
      start. **Time the rebuild once** so the recovery target in `PROJECT.md` §11.3 is a measurement
- [ ] `deploy/compose.prod.yml` — image tags, `mem_limit` per service sized so a runaway pipeline
      cannot OOM-kill the alert worker, `restart: unless-stopped`, healthchecks, and `json-file`
      logging with `max-size: 10m` / `max-file: 3`. Differs from the base file in operations only,
      never in behaviour
- [ ] Host hardening: key-only SSH, no root login, `unattended-upgrades`, fail2ban. Every published
      port bound to `127.0.0.1` — Docker's iptables rules **bypass UFW**, so a tidy firewall in front
      of a `ports: 8000:8000` is still open to the world
- [ ] Retention jobs: Dagster runs older than 90 days purged weekly, MLflow artifacts capped at the
      last 12 plus promoted versions, `docker image prune` keeping the last three SHAs
- [ ] Disk-usage check in the daily pipeline, warning into the digest at 75%. Filling the disk is the
      likeliest way this host dies (`PROJECT.md` §11.6)
- [ ] Dagster run-concurrency limit on the pipeline tag, completing the single-writer enforcement
      begun in M4
- [ ] Monthly CI job: pull the latest ops-store backup, restore into a scratch container, assert
      watermarks and outbox read back. An untested restore is a hope with a cron schedule
- [ ] Migrate the scheduled run off GitHub Actions; keep `daily.yml` as a fallback path
- [ ] `docs/DATA_QUALITY.md`; ADRs: Dagster versus Airflow; moving off GitHub Actions

### Acceptance
- `dagster dev` shows the full asset graph with lineage
- Adding an instrument to YAML causes its partitions to appear without any code change
- Injecting a bad row for one instrument blocks **only** that instrument, and no alert is sent for it
- A `sectors` universe with two failed members computes over nine and says so in the digest
- Dropping below `min_quorum` holds positions instead of rotating
- One backfill command materialises one instrument's full history
- The VPS has run the daily pipeline green for seven consecutive days
- A second, read-only connection queries the serving snapshot while a pipeline run is writing the
  warehouse — the property the API depends on in M9, proven before there is an API
- `make rollback` has been run once against a real previous SHA and the system came back
- The box has been rebuilt from `cloud-init.yaml` once, and the elapsed time is recorded in the
  runbook
- Starting a manual backfill during a scheduled run blocks or exits cleanly — it never half-writes
- A restored ops-store backup passes the monthly CI check
- `nmap` from outside the box shows only the SSH port

---

## M7 — Features, strategy compiler, engine and portfolio

*Goal: the user-facing simulation capability, and the invariant that makes "one definition, two
runtimes" true rather than aspirational.*

The largest milestone. Split it if it stalls: features (7a), compiler (7b), engine (7c).

### Tasks — features
- [ ] Per-instrument: returns (1/5/21/63/252d), realized volatility, ATR, RSI, MACD, distance from
      SMA(20/50/200), drawdown, **rolling** volume z-score
- [ ] Cross-sectional per universe: momentum rank, volatility rank, relative strength versus
      benchmark, breadth, rolling correlation to `cross_asset` members
- [ ] Macro: level, 5d and 20d deltas, joined on release date (`PROJECT.md` §6.3)
- [ ] Calendar effects: day of week, month, turn of month, days to and from month end — named in
      `PROJECT.md` §8.1's feature list and easy to forget because nothing else depends on them
- [ ] **Known-answer tests** for RSI, MACD and ATR against reference values on a fixed fixture
      series — cheap, and it catches the off-by-one a stability test will not
- [ ] `fct_features_daily` materialised via dbt, contract enforced

### Tasks — the compiler *(`PROJECT.md` §7.2)*
- [ ] Hand-written parser over a closed grammar → **typed AST**. No `eval`, ever. A test feeds it
      `__import__("os").system("...")` and asserts a parse error
- [ ] Typed function registry: `sma`, `ema`, `rsi`, `delta`, `momentum`, `rank`, `drawdown`,
      comparisons, boolean logic
- [ ] **Minimal type system** — `Scalar`, `Series[float]`, `Series[bool]`, `CrossSection[float]` —
      catching `rank_by: "close > 5"` and `filter: "sma(close, 20)"` at load time rather than at
      05:00 on a Tuesday
- [ ] Compile AST → Polars expressions over the feature frame
- [ ] `apiVersion: finflow/v1` mandatory; unknown version fails loudly; additive-only policy inside a
      major version, written up in `docs/CONTRACTS.md` (`PROJECT.md` §7.4)
- [ ] `on_missing_model: strict | skip | permissive`, `strict` the default. `skip` **changes the AST
      hash**, so a degraded run is a distinguishable variant (`PROJECT.md` §7.5)
- [ ] The AST — not the source string — is what the run hash keys on and what is persisted with the
      run

### Tasks — the engine
- [ ] `decide(features, ast, as_of) -> Decision` (already exists from M4; the compiler now feeds it)
- [ ] `simulate(decisions, bars, costs) -> fills, metrics` — the backtest-only half that needs the
      next bar. **Execution at next bar's open**, never the signal bar's close
- [ ] Sizing: fixed, vol-target (with `rebalance_band`), inverse-vol, equal-weight
- [ ] Costs: the registry's `commission_bps` and `spread_bps` floors, plus slippage scaled by
      realized volatility (`PROJECT.md` §5.7). A strategy may raise any of the three, never lower it
- [ ] Cross-sectional rebalancing with configurable frequency, using date-effective membership and
      `min_quorum`
- [ ] `vol_source: forecast | trailing` in vol targeting, so the volatility model feeds position
      sizing instead of being reported and ignored (`PROJECT.md` §8.2)
- [ ] **Portfolio netting** (`PROJECT.md` §7.6): `portfolio.yml` declares per-strategy capital
      allocation, maximum gross exposure, maximum single-instrument weight and a minimum trade size;
      the netted result is **one** `Decision` per day for the whole account, written at
      `scope: portfolio` with the contributing per-strategy decision ids on it (`PROJECT.md` §9.2).
      The per-strategy decisions are still persisted — they are the counterfactual §7.7 needs — but
      only the netted one reaches the outbox. Without this, four strategies emit four contradictory
      alerts and push portfolio construction onto the reader
- [ ] Benchmark may be a **portfolio** (`{SPY: 0.6, IEF: 0.4}`), not only a ticker, and metrics
      include beta-adjusted alpha and information ratio — beating SPY at 60% average exposure is not
      skill (`PROJECT.md` §7.1)
- [ ] `tradeable_only: true` filters live decisions to `tradeable_eu`; a strategy whose live universe
      differs from its research universe reports both
- [ ] Metrics: total and annualised return, Sharpe **with confidence interval**, Sortino, max
      drawdown, Calmar, hit rate, turnover, exposure, benchmark comparison, tracking error, and
      gross / net-of-cost / net-of-cost-and-tax return (`PROJECT.md` §8.4)
- [ ] Ex-ante portfolio risk from the covariance of held instruments, reported in PLN
- [ ] **Decision journal** (`PROJECT.md` §7.7): every instruction recorded as followed, overridden or
      ignored, with a one-line reason captured at the time. A quarterly review command prints the
      journal against what subsequently happened. One sentence a month, and it is the difference
      between believing you make good calls and knowing
- [ ] Persist to `fct_backtest_runs` / `_positions` / `_metrics`, keyed by
      `hash(strategy_ast + registry_commit + manifest + code_sha)`
- [ ] Purity contract, asserted: `decide` and `simulate` perform no IO, read no clock, touch no
      globals. This is what makes determinism testable and the compute backend swappable
- [ ] Re-express M4's hand-built AST as `strategies/sma_cross.yml` and assert the compiler produces
      an identical AST — the migration is a *diff of two ASTs*, not a rewrite
- [ ] Ship 4–5 example strategies, including one cross-sectional rotation and one relative-value
      strategy over `rates_credit` (where price return is defensible, `PROJECT.md` §6.4)
- [ ] ADR: the AST rather than the YAML source string as the strategy interface — what it buys
      (stable run identity across reformatting, a persisted artifact older than the surface syntax)

### Tasks — the invariant
- [ ] **Prefix-stability property test** (`PROJECT.md` §7.3): for random dates *D*,
      `decide(features[:D], ast, D) == decide(features_full, ast, D)`. Run under Hypothesis over
      sampled dates and strategies
- [ ] Assert it catches, by deliberate injection: a `shift(-1)`, a full-sample z-score, a global
      rank, a forward-filled macro join. A property test nobody has watched fail is not yet a test

### Acceptance
- The prefix-stability property and the known-answer tests pass
- Injecting each of the four lookahead bugs above makes the property fail
- Buy-and-hold reproduces the benchmark's price return within a basis point
- Sector rotation backtests with correct membership in 2010, 2016 and today
- Two identical runs produce the same hash; changing one cost parameter changes it; reformatting an
  expression string does not
- The daily alert now comes from a YAML strategy, and its Decision is byte-identical to M4's for the
  same rule and date

---

# Stage 3 — depth and confidence

## M8 — Models and deflated evaluation

*Goal: honestly evaluated models, as an optional enrichment behind a port.*

### Tasks
- [ ] `ports/model_provider.py` with `MlflowModelProvider` and `NullModelProvider`. The domain layer
      gains no compile-time dependency on LightGBM
- [ ] Targets: `y_direction_{1,5,21}d`, `y_realized_vol_{5,21}d`
- [ ] Walk-forward CV splitter with **purge** and **embargo**; unit-tested for no train/test overlap
      at fold boundaries
- [ ] **Baselines matched to the target** (`PROJECT.md` §8.1): direction against naive, drift and the
      unconditional base rate; volatility against EWMA (λ=0.94), GARCH(1,1) and **HAR-RV**, scored
      with QLIKE and MSE. Benchmarking a volatility model with AUC, or against ARIMA, measures
      nothing
- [ ] **Deflated Sharpe Ratio and PBO** (`PROJECT.md` §8.3). The trial count comes from
      `SELECT count(*) FROM fct_backtest_runs` — the input nobody usually has and this platform gets
      for free, and it is what stops `RESULTS.md` publishing a number that
      was selected for looking good
- [ ] Sharpe confidence intervals everywhere, via SE(SR) ≈ √((1 + SR²/2)/T). At ten years of daily
      data SE ≈ 0.32, so 0.65 versus a benchmark's 0.40 is noise — and the interval says so without
      an argument
- [ ] End-to-end test of the volatility model: same strategy with `vol_source: trailing` versus
      `forecast`, compared on realized-vs-target volatility and drawdown. This answers whether the
      model is worth anything far better than QLIKE does
- [ ] LightGBM **panel model** — stacked instrument-date rows, instrument and asset class as
      categoricals
- [ ] **Predictions land in `fct_features_daily` like any other column**, so the evaluator has no
      notion of "an ML column" and the prefix-stability property covers predictions for free
- [ ] MLflow: params, metrics, feature importance, artifact, registry
- [ ] Monthly refit as a Dagster asset; promotion only on beating the incumbent out-of-sample
- [ ] `docs/RESULTS.md`: AUC, Brier, per-asset-class breakdown, Sharpe net of costs versus benchmark,
      **and** the price-return bias per universe (§6.4) and the survivorship note (§6.5)
- [ ] ADRs: why two targets; ML as an optional port

### Acceptance
- Walk-forward evaluation runs across the full universe
- `docs/RESULTS.md` publishes real numbers — including direction ≈ 0.52 AUC, stated plainly, next to
  its baseline
- Every headline Sharpe appears with its confidence interval, its deflated value, and the trial count
  it was deflated by
- The volatility model beats HAR-RV on QLIKE, or the report says plainly that it does not
- Switching to `vol_source: forecast` measurably tightens realized volatility around target
- The rung-2 gate of `PROJECT.md` §15 can be evaluated: PBO below 0.5 and a deflated Sharpe interval
  excluding zero, or a clear statement that it cannot be met yet. Either is a successful outcome;
  only an unanswered question is a failure
- A newly added instrument gets features and predictions with no code change
- Swapping in `NullModelProvider` makes `strict` strategies refuse to run and says so in the digest,
  rather than quietly firing a different strategy

---

## M9 — Serving

*Goal: something to show, and a way to look at a backtest that is not a Telegram message.*

Three presentation adapters, **one application layer**. The bot must not re-implement backtest
invocation; it calls `RunBacktest` like everyone else.

### Tasks
- [ ] FastAPI over the serving snapshot, **read-only** (`PROJECT.md` §4.5):
  - `GET /instruments`, `GET /universes` — registry-driven
  - `GET /prices/{symbol}`, `GET /features/{symbol}`
  - `POST /backtests`, `GET /backtests/{run_id}`
  - `GET /decisions?strategy=` — current state, with `as_of`, `strategy_version`, `snapshot_id`
  - `GET /health` — freshness, last run status, snapshot age, quorum state per universe
  - OpenAPI docs, Pydantic response models, bearer-token auth, localhost-bound
- [ ] Streamlit consuming **the API**, never the database:
  - universe browser and price explorer
  - strategy editor — YAML in, compiler errors out, including type errors from §7.2
  - backtest results: equity curve, drawdown, trade list, metrics versus benchmark, with the
    price-return caveat rendered on every report
  - run comparison across saved backtests
- [ ] Telegram commands: `/status`, `/subscribe <strategy>`, `/unsubscribe`, `/backtest <name>`
- [ ] Tests: the API cannot acquire a write lock; the UI opens no DuckDB connection (asserted by
      import contract, not by inspection); all three entrypoints reach the same use case

### Acceptance
- `docker compose up` gives a working API, UI and bot
- The UI is usable while the pipeline is mid-run — no lock errors, which is the point of the
  snapshot design
- A user can define a strategy in the UI, see a type error, fix it, backtest, subscribe, and receive
  a real alert
- `lint-imports` shows no path from `entrypoints/ui` to any adapter

---

## M10 — Compute benchmark and documentation

*Goal: a system you can pick up again after six months away, and a measured answer to "why no
Spark?"*

### Tasks
- [ ] `compute/spark/` — a second implementation of the bronze→silver transforms behind the same
      internal interface as `compute/polars/`. That it can be written without touching
      `application/` is the milestone's real result
- [ ] `chispa` unit tests on each transform with small fixture DataFrames
- [ ] **`docs/SCALING.md` — the actual deliverable.** Measured wall time and peak memory for Polars
      versus Spark at 300k, 3M and 30M synthetic rows; the crossover point; and the conclusion that
      this dataset sits two orders of magnitude below it
- [ ] `README.md`: what it is, architecture diagram, quickstart, measured scale, results summary,
      add-an-instrument pointer, daily-run status badge, disclaimer
- [ ] `docs/architecture.png` — generated from source, showing the layers of §4.1
- [ ] `make demo`: seeds a small dataset, runs the whole pipeline, launches the UI — **no network
      required**
- [ ] Remaining ADRs; `docs/RUNBOOK.md` updated with everything learned since M4

### Acceptance
- A clean clone reaches a running system from the README alone, in under five minutes
- `make demo` works with networking disabled
- The Spark backend passes the same transform tests as the Polars backend
- Adding it required no change to `application/` or `domain/`
- `docs/SCALING.md` contains a crossover row count measured on this machine — or, if the Spark
  backend was cut, Polars-only measurements plus an explicitly labelled estimate of the crossover

---

# Stage 4 — Lakehouse extension *(optional)*

Each step ships independently. `PROJECT.md` §12 states that none of this is required at 300k rows.
Each is also a test of a specific seam, which is the honest reason to do them.

## A1 — Delta Lake on MinIO — *tests the `Warehouse` and `ObjectStore` ports*

- [ ] MinIO in Compose; `deltalake` writer replacing the raw Parquet landing
- [ ] Bronze and silver as Delta, partitioned by instrument
- [ ] dbt points at Delta via DuckDB's delta extension — **mart SQL unchanged**, which the M3
      dialect-neutrality check has been protecting all along
- [ ] Time travel replaces the M2 manifest: pin a version, re-run, identical results
- [ ] Record honestly whether the marts really were unchanged; if not, say what leaked and why
- [ ] ADR: Delta versus Iceberg

## A2 — PySpark transforms — *tests the compute backend seam*

- [ ] Promote bronze→silver from Polars to PySpark across the board, reusing M10's transforms
- [ ] Broadcast-join the registry as a dimension
- [ ] Partition pruning and predicate pushdown verified against query plans
- [ ] Honest ADR: this made the pipeline slower at this data size, and why it was still worth doing

## A3 — Streaming ingest — *the only step that changes what the product can do*

- [ ] Redpanda in Compose; topics `quotes.raw`, `quotes.normalized`, `decisions.emitted`
- [ ] Finnhub WebSocket producer subscribing to every enabled instrument from the registry
- [ ] Synthetic replayer producing to the same topics for offline work
- [ ] Spark Structured Streaming: 1-minute bars, windowed features, watermarks for late data
- [ ] Streaming sink to Delta bronze; intraday evaluation through the **same** `decide()`
- [ ] Consumer lag and end-to-end latency metrics

## A4 — Change data capture — *promotes the ops store*

- [ ] `ops.sqlite` → Postgres (the `OpsStore` port should absorb this)
- [ ] Debezium → Kafka on the strategy and subscription tables
- [ ] Live evaluator consumes strategy changes instead of polling
- [ ] Test: editing a strategy takes effect without a restart, and its `strategy_version` changes

## A5 — Infrastructure as code

- [ ] Terraform modules: S3, Glue catalog, EMR Serverless, IAM, Secrets Manager
- [ ] Remote state with locking; separate dev and prod workspaces
- [ ] `terraform plan` on every PR; **`apply` stays manual and unfunded** unless deliberately
      budgeted — this is the point where the infrastructure stops being free
- [ ] Container images built and pushed by CI

## A6 — Observability

- [ ] OpenTelemetry across ingest, transform and serving
- [ ] Prometheus and Grafana in Compose
- [ ] Dashboards: freshness by instrument, pipeline duration, quality score trend, consumer lag,
      alert delivery latency, restatement frequency, quorum breaches
- [ ] Alert rules on freshness SLA breach

## A7 — gRPC signal service

- [ ] Protobuf definitions for decisions and instruments
- [ ] `SignalService.StreamSignals` server-streaming RPC
- [ ] Generated client, integration test, docs
- [ ] ADR: gRPC alongside REST rather than replacing it

---

# Cross-cutting standards

## Testing strategy

| Layer | Tool | What it proves |
|---|---|---|
| **Architecture** | `import-linter` | The dependency rule holds; `domain` knows nothing of vendors, DBs or Dagster |
| **No ambient time** | pytest AST walk | `datetime.now` / `date.today` appear nowhere in `domain` or `application` |
| Registry | pytest | Invalid configurations are rejected before any run |
| **Port conformance** | pytest, parametrized | Every `SourceClient` honours the same contract — what makes "one interface" true |
| Ingest clients | pytest + `respx` | Parsing, error taxonomy, rate-limit detection — no live network in CI |
| Vendor errors | pytest | A captured Stooq rate-limit page raises, never parses |
| Convergence | pytest | Double-run yields one row per key, latest opinion wins |
| Rebuild | pytest | Deleting the warehouse and rebuilding from raw reproduces the marts |
| **Restore** | pytest | The ops store backup actually restores |
| Concurrency | pytest | One warehouse writer; the API cannot acquire a write lock; two ops writers succeed |
| Frame contracts | Patito | Dtypes, nullability and uniqueness at every stage boundary |
| dbt models | `dbt test`, `contract: enforced` | Constraints, relationships, gap detection, column types |
| Indicators | pytest | Known-answer tests against reference values |
| **Prefix stability** | Hypothesis | One property subsuming the whole lookahead family (§7.3) |
| Release lag | pytest | Monthly macro is null until its publication date |
| DSL safety and types | pytest | Malicious expressions fail to parse; type errors caught at load |
| Purity | pytest | `decide` and `simulate` do no IO and read no clock |
| Backtest | pytest | Determinism; buy-and-hold reproduces the benchmark |
| Alerts | pytest | Exactly-once **per decision** across simulated crashes; no half-rotations |
| Failure domains | pytest | One bad instrument blocks one instrument; quorum breach holds positions |
| End-to-end | pytest + Compose | `make demo` produces a decision from raw input |
| **Live sources** | pytest `-m integration` | **Nightly and manual only.** How you learn a vendor changed its format |

**Coverage target: 80% on `src/finflow/`, enforced in CI.** Note that coverage is the weakest signal
in this table; the property, conformance and architecture tests are the ones that would actually
catch a regression that matters.

## CI pipeline

```
on: [push, pull_request]

  lint         ruff check · ruff format --check
  typecheck    mypy --strict src/
  imports      lint-imports                      ← the dependency rule
  secrets      gitleaks                          ← fails fast, cannot be --no-verify'd here
  registry     validate every instruments/*.yml
  contracts    dialect-neutrality grep over dbt/models/marts
  deps         pip-audit
  unit         pytest -m "not integration" --cov --cov-fail-under=80
  dbt          dbt build against seeded slim dataset, contracts enforced
  integration  pytest -m integration  (compose services, mocked sources)
  build        docker build (buildx cache) · trivy scan · push ghcr:<sha>  (main only)
  docs         dbt docs generate  (main branch only)

on: schedule (monthly)
  restore      pull latest ops backup → restore → assert readable
  rebuild      delete warehouse → rebuild from raw → assert marts identical

on: schedule (nightly)
  live         pytest -m integration --live   (real vendors; failure opens an issue)

on: schedule (daily, 05:30 UTC)          # M4 → M6; the production run
  daily        finflow daily → Telegram → healthchecks.io ping
```

`imports` and `secrets` run before `unit` deliberately: an architecture violation or a leaked token
should fail in ten seconds, not after the test suite. `uv` and buildx caches are warmed on `main` so
a PR build stays under a couple of minutes — CI that takes ten minutes stops being run locally,
which is the point at which it stops preventing anything.

**Never `pull_request_target`.** It is the one Actions footgun that hands repository secrets to
untrusted code, and on a public portfolio repo it is how tokens leak.

## Daily-operations standards *(from M4 onward)*

1. Every run writes a row to `pipeline_runs` — status, duration, rows, snapshot id, manifest ref.
2. Every run either pings the dead-man's switch or does not; there is no partial success.
3. Every outbound message carries `as_of`, `strategy_version` and `snapshot_id`.
4. A failing check blocks alerts for the affected **instruments** and says so in the digest.
5. Any incident taking more than ten minutes to diagnose gets a `docs/RUNBOOK.md` entry the same day.
6. Secrets exist only in GitHub Actions secrets or the VPS `.env` (mode 600), and every credential is
   scoped to the least privilege that works — the pipeline's object-store token cannot delete.
7. Every deploy names a git SHA. `latest` is for humans, never for a rollback.
8. No service publishes a port on anything but `127.0.0.1`.

## Definition of done, per milestone

1. Code merged with tests, CI green — including `imports` and `secrets`
2. Acceptance criteria demonstrably met and shown working, not asserted
3. Documentation updated in the same PR
4. Any architectural choice recorded as an ADR
5. No TODO left without a linked issue
6. **The daily run is still green** (M4 onward)
7. The digest is still readable in ten seconds, and the share of no-action days has not fallen below
   95% (`PROJECT.md` §1.2). A milestone that makes the daily message noisier has not shipped

## Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| **Scope exceeds available time** | **High** | The cut list, decided in advance. M4 is the point past which stopping still leaves a working system |
| **The digest becomes noise and gets muted** | **High** | The quietest failure and the one that ends the product. ≥95% no-action days is a tracked target, not an aspiration; rebalance bands and minimum trade sizes exist to protect it; a milestone that makes the message noisier is not done |
| **The system is trusted faster than it has earned** | Medium | The trust ladder (`PROJECT.md` §15) with gates fixed in advance and an automatic drop on any instruction issued from bad data. Deciding this now avoids deciding it during a good month |
| **The user overrides silently and the record decays** | Medium | Explicit `/pause`, `/mute`, `/hold` and a decision journal, so an override is data rather than a gap (§7.7) |
| **The DSL becomes the project** | **High** | The grammar is closed and the function registry is a fixed whitelist. New functions are a backlog item, never a mid-milestone yes. If M7 slips twice, ship the hand-built AST form and defer the parser |
| Stooq rate-limits or blocks the runner IP | High | Typed error taxonomy, `deferred_until` resume, secondary source, recorded fixtures keep CI independent |
| Vendor changes format silently | High | Nightly live-source test that opens an issue on failure |
| Retroactive restatements corrupt history | High | Append-only raw zone; `dq_restatements`; runs pinned to a manifest |
| Macro revisions and release lags leak lookahead | High | ALFRED vintages, release-date joins, and the prefix-stability property over both |
| **Layering erodes under deadline pressure** | Medium | `import-linter` in CI. An architecture that is only in a document is a description of the past |
| **Premature abstraction** | Medium | `PROJECT.md` §4.6: a package appears at its second implementation or second consumer, not before. No DI framework, no repository pattern |
| Price-return bias misleads on credit and rates | Medium | Documented per universe in `RESULTS.md`; long-only `rates_credit` strategies carry a warning |
| DuckDB lock contention once the UI exists | Medium | Single-writer discipline, serving snapshot, ops store split, tests for all three |
| **Quorum breach rotates a distorted universe** | Medium | `min_quorum` enforced in M6, defaulting to 0.9; hold rather than rotate; digest says so |
| Scope creep into ML sophistication | Medium | Frozen at M8, and ML sits behind a port that can be cut entirely |
| **Selection bias across many backtests** | **High** | Deflated Sharpe and PBO with the trial count read from `fct_backtest_runs` (§8.3). Running fifty strategies and reporting the best one undeflated is the single most common way a portfolio backtest lies |
| **The universe is not buyable from an EU account** | **High** | PRIIPs blocks US-domiciled ETFs for EU retail. `ucits_equivalent` mapping and a `tradeable_eu` live filter in M5; research and live universes reported separately |
| **Costs understated on thin ETFs** | Medium | Per-instrument cost floors in the registry, slippage scaling in volatility, `min_adv_usd` gate. A flat 3 bps on GDXJ manufactures several percent a year of fake return |
| Turnover tax drag ignored | Medium | Net-of-tax reporting under a stated realization assumption (19% PIT-38); high-turnover strategies compared to buy-and-hold on that basis |
| Backtest overfitting | Medium | Walk-forward only, costs always on, benchmark always shown, results published honestly |
| Survivorship bias | Medium | Structurally handled going forward; residual quantified (`PROJECT.md` §6.5) |
| Silent failure while unattended | Medium | Dead-man's switch, daily digest, staleness stamped on every message |
| **Disk fills on the VPS** | **High** | The likeliest way a small host dies, and entirely preventable: log rotation, Dagster run retention, MLflow artifact caps, image pruning, and a 75% warning in the digest (`PROJECT.md` §11.6) |
| **Two writers collide** (manual backfill during the scheduled run) | Medium | Enforced, not documented: `flock`, Dagster concurrency limits, workflow concurrency group. This is the realistic collision because a backfill is what you start *when something looks wrong* |
| **A deploy breaks the daily run** | Medium | SHA-tagged images, smoke test gating the deploy with automatic rollback, `make rollback` exercised once deliberately in M6 |
| **A service crash-loops unnoticed** | Medium | `restart: unless-stopped` restarts a broken worker forever and the dead-man's switch does not cover it, because the pipeline still succeeds. Compose healthchecks plus a restart-count line in the digest |
| Ops-store migration fails mid-deploy | Low | Versioned migrations applied on start and asserted by the smoke test; nightly encrypted offsite backup restores in minutes |
| Ops store lost or corrupted | Low | Nightly `VACUUM INTO` backup to a separate bucket with a **CI-tested** restore; worst case is re-sending one day of alerts |
| Committed secret | Low | `gitleaks` in pre-commit and CI; least-privilege scoped tokens bound the blast radius; rotation documented in the runbook |
| VPS dies or credentials rotate | Low | Rebuildable from `cloud-init.yaml` in under an hour, timed once so the target is measured; warehouse rebuilt from raw; `daily.yml` kept as a fallback runner |
| Stage 4 destabilises a working MVP | Low | Each A-step ships independently; the MVP path stays runnable throughout |
| Telegram token leaks | Low | `.env` only, gitignored, `.env.example` documents without values |

## Backlog — explicitly out of scope for the MVP

Options and futures · intraday bars below 1 minute · non-USD instruments and FX normalisation ·
total-return reconstruction from a distributions feed · portfolio-level optimisation across
strategies · live broker execution · deep learning models · multi-user auth and tenancy · mobile app
· user-defined functions in the DSL · broker API integration · intraday execution · a correlation-based
gross-exposure overlay (worth doing, but only once the vol overlay of §8.2 is measured first, so the
two effects are separable).

Each is a real extension; none belongs on the critical path. The first two worth doing after M10,
for a non-US author, are **FX normalisation with a PLN reporting view** and **broker-availability
filtering via registry `tags`** — both are registry-schema changes the design already anticipates,
and neither should require a change outside `registry/`, `contracts/` and the dbt staging layer. If
either does, the seams were in the wrong place and that is worth knowing.
