# FinFlow — Implementation Plan

Milestone-by-milestone build plan. Stage 1 (M0–M8) delivers the MVP; Stage 2 (A1–A7) promotes it
onto lakehouse infrastructure. See `PROJECT.md` for architecture and rationale.

**Rule for every milestone:** it is not done until CI is green and the acceptance criteria pass.
Never start the next milestone on a red build.

---

## Sequencing overview

| # | Milestone | Effort | Blocks |
|---|---|---|---|
| M0 | Repository foundation | 1 evening | everything |
| M1 | Instrument registry | 1 evening | M2, M3 |
| M2 | Ingestion layer | 1 weekend | M3 |
| M3 | Warehouse and dbt marts | 1 weekend | M4, M5 |
| M4 | Orchestration and data quality | ½ weekend | M5, M7 |
| M5 | Features and models | 1 weekend | M6 |
| M6 | Strategy DSL and backtest engine | 1 weekend | M7 |
| M7 | Serving, UI and alerts | 1 weekend | — |
| M8 | Spark backfill and documentation | ½ weekend | — |
| | **MVP total** | **~4 weekends** | |
| A1–A7 | Lakehouse extension | +4–5 weekends | after M8 |

**Critical path:** M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7. M8 can be done in parallel with M6/M7.

---

# Stage 1 — MVP

## M0 — Repository foundation

*Goal: an empty but fully industrialised repo. Every later milestone inherits the guardrails.*

### Tasks
- [ ] `pyproject.toml` with `uv`, package under `src/finflow/`
- [ ] Tooling config: ruff (lint + format), mypy `strict = true`, pytest with coverage
- [ ] `.pre-commit-config.yaml`: ruff, ruff-format, mypy, trailing whitespace, YAML check
- [ ] `Makefile`: `install`, `test`, `lint`, `up`, `down`, `demo`, `backfill`, `docs`
- [ ] `.github/workflows/ci.yml`: lint → typecheck → test, on push and PR
- [ ] `.env.example` with every key documented; real `.env` gitignored
- [ ] `.gitignore`: `.env`, `data/`, `*.duckdb`, `mlruns/`, `__pycache__/`, `.venv/`
- [ ] `docs/adr/0001-record-architecture-decisions.md`
- [ ] Structured logging via `structlog`, JSON in non-TTY environments
- [ ] `src/finflow/config.py` — Pydantic `Settings` reading env, no scattered `os.getenv`

### Acceptance
- `make install && make test` passes on a clean clone
- CI green with zero mypy errors in strict mode
- No secret is readable anywhere in the repo

---

## M1 — Instrument registry

*Goal: the extensibility mechanism. Adding an ETF must be a one-file edit from this point forward.*

### Tasks
- [ ] Pydantic models: `Instrument`, `Universe`, `MacroSeries`, `Registry`
- [ ] Loader that merges every `instruments/*.yml` into one validated `Registry` object
- [ ] Validation rules:
  - unique symbols across all files
  - every universe member exists as an instrument
  - `backfill_start >= inception`
  - every declared source key maps to a registered client
  - valid `calendar` code recognised by `exchange_calendars`
- [ ] Seed the registry with a real starting universe:
  - `precious_metals` — GLD, IAU, SGOL, SLV, GDX, GDXJ, SIL
  - `equity_core` — SPY, QQQ, IWM, EFA, EEM
  - `sectors` — the eleven SPDR sector ETFs
  - `rates_credit` — TLT, IEF, SHY, LQD, HYG, TIP
  - `broad_commodities` — DBC, USO, UNG, DBA, PDBC
  - `cross_asset` — SPY, TLT, GLD, DBC, UUP, HYG
- [ ] `instruments/macro.yml` — DFII10, DTWEXBGS, VIXCLS, T10Y2Y, BAMLH0A0HYM2, CPIAUCSL
- [ ] Query helpers: `registry.enabled()`, `registry.universe("sectors")`, `registry.sources_for(sym)`
- [ ] Tests: valid registry loads; duplicate symbol fails; unknown universe member fails; bad
      calendar fails
- [ ] `docs/ADDING_AN_INSTRUMENT.md` — the five-step workflow
- [ ] ADR: registry as code versus database table

### Acceptance
- ~40 instruments and 6 universes load and validate in under 100 ms
- Every invalid-registry test fails for the *right* reason
- Adding an instrument requires touching exactly one YAML file

---

## M2 — Ingestion layer

*Goal: raw data on disk, idempotently, for any registered instrument.*

### Tasks
- [ ] `SourceClient` protocol: `fetch(symbol, start, end) -> pl.DataFrame`, plus `capabilities()`
- [ ] `StooqClient` — CSV over HTTPS, no key, symbol mapping from the registry
- [ ] `FredClient` — macro series, free API key
- [ ] `TwelveDataClient` — reconciliation source, quota-aware
- [ ] Cross-cutting concerns in one place, not per client:
  - retry with exponential backoff and jitter (`tenacity`)
  - rate limiting per source (token bucket)
  - request/response logging with timing
- [ ] Landing zone: `data/raw/source=<s>/symbol=<sym>/date=<yyyy-mm-dd>/data.parquet`
- [ ] Watermark table in DuckDB: `(source, symbol) -> last_loaded_date, last_run_at, row_count`
- [ ] Idempotency: re-running a date range overwrites its partitions, never appends duplicates
- [ ] Schema normalisation to the canonical contract: `symbol, date, open, high, low, close,
      adj_close, volume, source, ingested_at`
- [ ] Synthetic source that generates deterministic OHLCV — for CI and offline demos
- [ ] Tests: recorded HTTP fixtures (`respx`), no live network calls in CI; idempotency test that
      runs ingest twice and asserts identical row counts

### Acceptance
- `make backfill` populates the full universe from `backfill_start` to today
- Running it twice produces byte-identical partitions
- Whole test suite passes with networking disabled
- A source outage degrades gracefully: other instruments still complete, failure is recorded

---

## M3 — Warehouse and dbt marts

*Goal: a modeled star schema you can actually query.*

### Tasks
- [ ] DuckDB loader: raw Parquet → `bronze_ohlcv` / `bronze_macro`, with `_ingested_at` and `_source`
- [ ] `bronze_quarantine` for rows failing the contract, with a rejection reason
- [ ] dbt project (`dbt-duckdb`) in three layers:
  - **staging** — `stg_ohlcv`, `stg_macro`: typed, renamed, deduplicated on `(symbol, date, source)`
  - **intermediate** — `int_ohlcv_adjusted` (splits/distributions), `int_calendar_aligned`
    (per-instrument trading days), `int_source_reconciled` (primary source wins, divergence recorded)
  - **marts** — `dim_instrument` (SCD2 from registry), `dim_universe`, `bridge_universe_member`,
    `dim_date`, `fct_ohlcv_daily`, `fct_macro_daily`
- [ ] `dim_date` generated with per-calendar trading-day flags
- [ ] dbt tests: `unique`, `not_null`, `relationships`, `accepted_values`, plus dbt-expectations for
      `close > 0`, `high >= low`, row-count ranges
- [ ] Custom generic test: **no gaps on trading days** per instrument, calendar-aware
- [ ] Incremental materialisation on facts, partitioned by instrument
- [ ] `dbt docs generate` wired into the Makefile

### Acceptance
- `dbt build` completes green over the full universe
- `SELECT * FROM fct_ohlcv_daily WHERE symbol='GLD'` returns a clean continuous series
- Adding an instrument to the registry and re-running produces its rows with no model edits
- Deliberately corrupting a source row lands it in quarantine, not in the marts

---

## M4 — Orchestration and data quality

*Goal: the pipeline runs itself and refuses to publish bad data.*

### Tasks
- [ ] Dagster project with `MultiPartitionsDefinition`:
      `instrument` (dynamic, from registry) × `date` (daily)
- [ ] Asset graph: `raw_ohlcv` → `bronze` → dbt assets (auto-loaded) → `features` (M5)
- [ ] **Registry sensor**: detects new or newly-enabled instruments and registers their dynamic
      partitions automatically — this is the mechanism that makes step 3 of the add-instrument
      workflow real
- [ ] Asset checks:
  - freshness — latest date within N trading days of today, per calendar
  - volume — row count within expected range for the period
  - nullness — no nulls in required columns
  - sanity — no single-day move beyond ±30% without a corresponding corporate action
  - reconciliation — cross-source divergence under threshold
- [ ] **Circuit breaker**: a failed blocking check halts downstream assets, so no signal or alert is
      ever produced from unvalidated data
- [ ] `dq_results` table: every check run, its threshold, outcome and partition
- [ ] Daily schedule plus a manual backfill job
- [ ] Retry policy on ingest assets; failure notification to Telegram (reuses M7 sender)
- [ ] `docs/DATA_QUALITY.md` — every check, threshold, owner, and what happens when it fires

### Acceptance
- `dagster dev` shows the full asset graph with lineage
- Adding an instrument to YAML causes its partitions to appear without any code change
- Injecting a bad row fails the check and visibly blocks downstream assets
- One backfill command materialises one instrument's full history

---

## M5 — Features and models

*Goal: point-in-time correct features and honestly evaluated models.*

### Tasks
- [ ] Per-instrument features: returns (1/5/21/63/252d), realized volatility, ATR, RSI, MACD,
      distance from SMA(20/50/200), drawdown, volume z-score
- [ ] Cross-sectional features per universe: momentum rank, volatility rank, relative strength versus
      benchmark, breadth (% above SMA200), rolling correlation to `cross_asset` members
- [ ] Macro features: level, 5d and 20d deltas for every registered series
- [ ] **Point-in-time discipline** — every feature computed with backward-looking windows only, and a
      test that shifts input data forward and asserts historical features are unchanged
- [ ] `fct_features_daily` materialised via dbt or Dagster asset
- [ ] Targets: `y_direction_{1,5,21}d`, `y_realized_vol_{5,21}d`
- [ ] Walk-forward CV splitter with **purge** and **embargo**; unit-tested for no train/test overlap
- [ ] Baselines: naive, drift, ARIMA, always reported alongside
- [ ] LightGBM **panel model** — stacked instrument-date rows, instrument and asset class as
      categoricals
- [ ] MLflow: params, metrics, feature importance, model artifact, registry
- [ ] Evaluation report: AUC, Brier, and per-asset-class breakdown, written to `docs/RESULTS.md`

### Acceptance
- Lookahead test passes
- Walk-forward evaluation runs across the full universe
- `docs/RESULTS.md` publishes real numbers — including direction ≈ 0.52 AUC, stated plainly
- A newly added instrument gets features and predictions with no code change

---

## M6 — Strategy DSL and backtest engine

*Goal: the user-facing simulation capability, and one definition that runs in both places.*

### Tasks
- [ ] YAML strategy schema (Pydantic) for both `single` and `cross_sectional` types
- [ ] Safe expression evaluator — whitelisted functions only (`sma`, `ema`, `rsi`, `delta`,
      `momentum`, `rank`, `drawdown`, comparisons, boolean logic). **No `eval` of user input.**
- [ ] Expression compiler → Polars expressions over the feature frame
- [ ] Vectorized backtest engine:
  - signal → target position → trade generation
  - position sizing: fixed, vol-target, inverse-vol, equal-weight
  - cost model: commission bps, slippage bps, optional spread
  - cross-sectional rebalancing with configurable frequency
  - **execution at next bar's open** — never the signal bar's close
- [ ] Metrics: total and annualised return, Sharpe, Sortino, max drawdown, Calmar, hit rate,
      turnover, exposure, plus benchmark comparison and tracking error
- [ ] Persist to `fct_backtest_runs` / `_positions` / `_metrics`, keyed by
      `hash(strategy_yaml + registry_version + data_version + code_sha)`
- [ ] Determinism test: same inputs → identical run hash and identical metrics
- [ ] Ship 4–5 example strategies in `strategies/`, including one cross-sectional rotation

### Acceptance
- Buy-and-hold strategy reproduces the benchmark's return within a basis point
- Sector rotation backtest runs across the full `sectors` universe
- Two identical runs produce the same hash; changing one cost parameter changes it
- A strategy referencing a universe automatically includes a newly added member

---

## M7 — Serving, UI and alerts

*Goal: a person can use this.*

### Tasks
- [ ] FastAPI:
  - `GET /instruments`, `GET /universes` — registry-driven
  - `GET /prices/{symbol}`, `GET /features/{symbol}`
  - `POST /backtests` (submit), `GET /backtests/{run_id}`
  - `GET /signals?strategy=` — current signal state
  - OpenAPI docs, Pydantic response models, health endpoint
- [ ] Streamlit UI:
  - universe browser and price explorer
  - strategy editor (YAML in, validation feedback)
  - backtest results: equity curve, drawdown, trade list, metrics versus benchmark
  - run comparison across saved backtests
- [ ] Telegram bot:
  - `/subscribe <strategy>`, `/unsubscribe`, `/status`, `/backtest <strategy>`
  - signal alerts with instrument, direction, price, triggering rule
  - **transactional outbox**: signals written to `alerts_outbox` in the same transaction as the
    signal, a separate worker delivers and marks sent
  - **idempotency**: unique `(strategy_id, instrument, bar_ts, rule_id)`; a restart re-sends nothing
  - cooldown to prevent alert storms on choppy signals
- [ ] Dagster schedule: evaluate strategies after the daily pipeline, enqueue alerts
- [ ] Tests: outbox delivers exactly once under simulated crash-and-restart

### Acceptance
- `docker compose up` gives a working API, UI and bot
- Restarting the alert worker mid-delivery sends no duplicates and loses nothing
- A user can define a strategy in the UI, backtest it, subscribe, and receive a real alert

---

## M8 — Spark backfill and documentation

*Goal: the heavy-data path, and a repo a stranger can understand in five minutes.*

### Tasks
- [ ] PySpark local-mode job: full multi-instrument historical backfill and adjustment, reading the
      raw Parquet landing zone and writing the bronze layer
- [ ] Structured as testable pure transforms, not one monolithic `main()`
- [ ] `chispa` unit tests on each transform with small fixture DataFrames
- [ ] Benchmark note: Spark path versus Polars path, and the universe size where Spark wins
- [ ] `README.md`: what it is, architecture diagram, quickstart, results summary, add-an-instrument
      pointer, disclaimer
- [ ] `docs/architecture.png` — generated, not hand-drawn
- [ ] `docs/SCALING.md`: which jobs become Spark on EMR, where the broker slots in, how the dbt
      models port to a lakehouse
- [ ] Remaining ADRs
- [ ] `make demo`: seeds a small dataset, runs the whole pipeline, launches the UI — **no network
      required**

### Acceptance
- A stranger clones the repo and gets a running system from the README in under five minutes
- `make demo` works with networking disabled
- Spark transforms are unit-tested, not just executed

---

# Stage 2 — Lakehouse extension

Each step ships independently. The system stays working throughout; there is no big-bang rewrite.

## A1 — Delta Lake on MinIO *(1 weekend)*
- [ ] MinIO in Compose; `deltalake` writer replacing raw Parquet landing
- [ ] Bronze and silver as Delta tables, partitioned by instrument
- [ ] dbt points at Delta via DuckDB's delta extension — **mart SQL unchanged**
- [ ] Time travel wired into backtest reproducibility: pin a data version, re-run, get identical results
- [ ] ADR: Delta versus Iceberg

## A2 — PySpark transforms *(1 weekend)*
- [ ] Promote bronze→silver transforms from Polars to PySpark across the board
- [ ] Reuse the M8 transform functions; extend `chispa` coverage
- [ ] Broadcast-join the registry as a dimension
- [ ] Partition pruning and predicate pushdown verified against query plans

## A3 — Streaming ingest *(1–1.5 weekends)*
- [ ] Redpanda in Compose; topics `quotes.raw`, `quotes.normalized`, `signals.emitted`
- [ ] Finnhub WebSocket producer subscribing to every enabled instrument from the registry
- [ ] Synthetic replayer producing to the same topics for offline work
- [ ] Spark Structured Streaming: 1-minute bars, windowed features, watermarks for late data
- [ ] Streaming sink to Delta bronze; intraday signal evaluation
- [ ] Consumer lag and end-to-end latency metrics

## A4 — Change data capture *(½ weekend)*
- [ ] Move strategies and subscriptions from YAML/DuckDB into Postgres
- [ ] Debezium → Kafka on the strategy tables
- [ ] Live evaluator consumes strategy changes instead of polling
- [ ] Test: editing a strategy takes effect without a restart

## A5 — Infrastructure as code *(1 weekend)*
- [ ] Terraform modules: S3, Glue catalog, EMR Serverless, IAM, Secrets Manager
- [ ] Remote state with locking; separate dev and prod workspaces
- [ ] `terraform plan` on every PR, `apply` gated on tag
- [ ] Container images built and pushed by CI

## A6 — Observability *(½ weekend)*
- [ ] OpenTelemetry instrumentation across ingest, transform and serving
- [ ] Prometheus and Grafana in Compose
- [ ] Dashboards: freshness by instrument, pipeline duration, quality score trend, consumer lag,
      alert delivery latency
- [ ] Alert rules on freshness SLA breach

## A7 — gRPC signal service *(½ weekend)*
- [ ] Protobuf definitions for signals and instruments
- [ ] `SignalService.StreamSignals` server-streaming RPC
- [ ] Generated client, integration test, docs
- [ ] ADR: gRPC alongside REST rather than replacing it

---

# Cross-cutting standards

## Testing strategy

| Layer | Tool | What it proves |
|---|---|---|
| Registry | pytest | Invalid configurations are rejected before any run |
| Ingest clients | pytest + `respx` | Parsing, retry, rate limiting — no live network in CI |
| Idempotency | pytest | Double-run produces identical output |
| Transforms | `chispa`, Polars asserts | Correctness on small fixture frames |
| dbt models | `dbt test` on seeds | Constraints, relationships, gap detection |
| Point-in-time | pytest | Shifting future data leaves history unchanged |
| Backtest | pytest | Determinism; buy-and-hold reproduces the benchmark |
| Alerts | pytest | Exactly-once delivery across simulated crashes |
| End-to-end | pytest + Compose | `make demo` produces a signal from raw input |

**Coverage target: 80% on `src/finflow/`, enforced in CI.**

## CI pipeline

```
on: [push, pull_request]

  lint         ruff check · ruff format --check
  typecheck    mypy --strict src/
  registry     validate every instruments/*.yml
  unit         pytest -m "not integration" --cov --cov-fail-under=80
  dbt          dbt build against seeded slim dataset
  integration  pytest -m integration  (docker compose services)
  build        docker build
  docs         dbt docs generate  (main branch only)
```

The registry validation stage matters: a malformed instrument fails the PR that introduced it, not
tomorrow's 6 a.m. pipeline run.

## Definition of done, per milestone

1. Code merged with tests, CI green
2. Acceptance criteria demonstrably met
3. Documentation updated in the same PR
4. Any architectural choice recorded as an ADR
5. No TODO left without a linked issue

## Risk register

| Risk | Likelihood | Mitigation |
|---|---|---|
| Source API breaks or rate-limits | High | Multiple sources per instrument; graceful degradation; recorded fixtures keep CI independent |
| Scope creep into ML sophistication | High | Model scope frozen at M5. Improvements go in a backlog, not the critical path |
| Corporate action handling is wrong | Medium | Reconcile against a second source; sanity checks on large single-day moves |
| Backtest overfitting | Medium | Walk-forward only, costs always on, benchmark always shown, results published honestly |
| Multi-calendar bugs on non-US instruments | Medium | MVP stays USD/US-listed; `exchange_calendars` from day one so extension is mechanical |
| Stage 2 destabilises a working MVP | Medium | Each A-step ships independently; the MVP path stays runnable throughout |
| Telegram token leaks | Low | `.env` only, gitignored, `.env.example` documents without values |

## Backlog — explicitly out of scope for the MVP

Options and futures · intraday bars below 1 minute · non-USD instruments and FX normalisation ·
portfolio-level optimisation across strategies · live broker execution · deep learning models ·
multi-user auth and tenancy · mobile app.

Each is a real extension; none belongs on the critical path.
