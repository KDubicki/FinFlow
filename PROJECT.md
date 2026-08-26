# FinFlow — Multi-Asset ETF Data Platform

**Design document**
Status: Approved — MVP is the Lean Warehouse, evolving into the Lakehouse

---

## 1. What this is

FinFlow is a data platform for exchange-traded funds across asset classes. It ingests price and
macro data for an extensible universe of ETFs, models it into a queryable warehouse, computes
predictive features, and lets a user define a trading strategy, backtest it against history, run it
live, and receive an alert on Telegram when it fires.

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

## 2. Design principles

The interesting engineering problem is not the model — it is building a pipeline whose output you
can actually trust, and which grows to new instruments without growing new code.

| Principle | What it means in practice |
|---|---|
| **Instruments are configuration** | Adding, disabling or re-sourcing an instrument is a registry edit reviewed in version control. No code path is ever named after a ticker. |
| **Ship a working slice early** | A finished narrow pipeline beats an unfinished broad one. One end-to-end path green before widening. |
| **Everything runs locally** | `docker compose up` produces a working system, with a demo path needing no network access. |
| **No lookahead, ever** | Features joined as-of the decision timestamp. A test fails if lookahead is introduced. |
| **Bad data stops the line** | Failed quality checks block downstream assets. No signal is emitted on unvalidated data. |
| **Reproducible by construction** | Every backtest is keyed by strategy, data version and code revision. |
| **One definition, two runtimes** | The same strategy YAML drives the backtest and the live evaluator. What was tested is what runs. |
| **Report what is true** | Publish the metrics the models actually achieve, including the weak ones. |

---

## 3. The instrument registry

This is the core extensibility mechanism and the most important design decision in the project.

### 3.1 Registry as code

Instruments live in version-controlled YAML under `instruments/`, not in a database table. The
trade-off is deliberate:

- Changes are **reviewable** — adding an instrument is a pull request with a diff.
- Backfills are **reproducible** — the registry state at any commit is recoverable.
- The pipeline is **auditable** — you can answer "when did we start tracking this, and why".

A database-backed registry would allow runtime additions without a deploy, but loses all three
properties. Recorded as an ADR.

### 3.2 Instrument definition

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
    sources:
      stooq: spy.us          # primary
      twelvedata: SPY        # reconciliation
    total_return: false      # price series; dividends handled separately
    enabled: true
    tags: [core, benchmark]
```

Every field is validated by a Pydantic model at load time. A malformed or duplicate entry fails CI
before it can reach a pipeline run.

### 3.3 Universes

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
    members: [XLE, XLF, XLK, XLV, XLI, XLP, XLU, XLB, XLRE, XLY, XLC]
    benchmark: SPY

  rates_credit:
    members: [TLT, IEF, SHY, LQD, HYG, TIP]
    benchmark: IEF

  cross_asset:
    description: One representative per asset class, for regime detection
    members: [SPY, TLT, GLD, DBC, UUP, HYG]
    benchmark: SPY
```

### 3.4 Macro series

Macro drivers are registered separately, since they are levels rather than tradeable prices.

```yaml
# instruments/macro.yml
series:
  - id: us10y_real
    source: FRED
    source_id: DFII10
    unit: percent
    transform: [level, delta_5d, delta_20d]

  - id: dollar_index
    source: FRED
    source_id: DTWEXBGS
    unit: index
    transform: [level, pct_change_20d]
```

### 3.5 Adding a new instrument — the whole workflow

1. Add an entry to the appropriate `instruments/*.yml` file.
2. Open a PR. CI validates the schema, checks for duplicate symbols, and verifies each declared
   source actually returns data for that symbol.
3. On merge, a Dagster sensor detects the registry change and **registers a new dynamic partition**.
4. The backfill runs for that instrument's partitions only — no full recompute of anything.
5. Quality checks run against the new series before it is admitted to the marts.
6. Any universe containing it, and every strategy and model referencing that universe, picks it up
   on the next scheduled run.

No code changes. No migrations. That property is the point of the whole design.

### 3.6 What multi-asset makes possible

Generalising beyond a single instrument is not just scope — it unlocks a class of features that
cannot exist in a single-asset pipeline:

- **Cross-sectional ranking.** Momentum and value ranks computed across a universe, enabling
  rotation strategies (hold the top *n* sectors by 12-1 momentum).
- **Relative strength.** GDX/GLD (miners versus metal), SPY/TLT (stocks versus bonds), HYG/IEF
  (credit risk appetite) — each a genuine regime signal.
- **Rolling correlation and regime detection.** Cross-asset correlation matrices over the
  `cross_asset` universe identify risk-on and risk-off states.
- **Panel models.** One model trained on stacked instrument-date observations generalises far better
  than a per-instrument model fit on a few thousand rows.
- **Breadth.** Percentage of a universe above its 200-day moving average.

---

## 4. Data sources

All sources are free and require no brokerage account. Sources are declared per instrument in the
registry, so a new provider is added by implementing one client interface and referencing it there.

### 4.1 Batch — daily and historical

| Source | Coverage | Auth | Role |
|---|---|---|---|
| **Stooq** | US ETFs and equities, indices, FX — long daily history | none | **Primary.** Plain CSV over HTTPS, no key, wide symbol coverage. |
| **FRED** (St. Louis Fed) | Real yields, dollar index, CPI, credit spreads, Fed funds | free key | Macro series. Best-in-class and fully free. |
| **Twelve Data** | OHLCV, FX, intraday | free key | Secondary source for cross-checking. |
| **Alpha Vantage** | OHLCV, FX | free key | Tertiary. Tight daily request cap, unsuitable as a primary feed. |
| **Nasdaq Data Link** | Benchmark reference prices | free key | Reference data. |

Deliberately avoided as a primary source: `yfinance`. Its terms are ambiguous beyond personal use
and it breaks without warning. Acceptable only as an optional third cross-check.

### 4.2 Streaming — intraday (Phase A)

| Source | What it gives | Auth | Notes |
|---|---|---|---|
| **Finnhub WS** | Real-time US equity and ETF trades | free key | Covers the whole ETF universe, but only during US market hours. |
| **Binance WS** | 24/7 stream on gold-backed and crypto tokens | none | Useful precisely because it never closes — development and demos work at any hour. |
| **Synthetic replayer** | Replays historical bars as a live tick stream | none | Always included. Makes CI deterministic and lets the system demo fully offline. |

### 4.3 Cross-source reconciliation

Instruments declaring two sources are compared in a `dq_source_agreement` table. Divergence beyond a
threshold is surfaced as a quality metric rather than silently averaged away.

### 4.4 Multi-asset ingestion concerns

Broadening beyond one asset class introduces real complexity, handled explicitly:

- **Trading calendars.** Instruments carry a `calendar` code and are aligned using
  `exchange_calendars`. A missing bar on a US holiday is expected; a missing bar on a trading day is
  a quality incident. The distinction requires the calendar.
- **Corporate actions.** Splits and distributions differ enormously between equity, bond and
  commodity ETFs. Silver adjusts to a total-return series where dividends are material.
- **Currency.** MVP is USD-only. Non-USD instruments require an FX normalisation layer, deferred and
  noted in the registry schema via the `currency` field.
- **Inception dates.** Backfills start at `backfill_start`, never at a global start date, so
  instrument history is never fabricated before the fund existed.

---

## 5. Strategy definition

A single YAML document defines a strategy. The backtest engine and the live evaluator both consume
it, so there is no possible drift between what was validated and what runs.

**Single-instrument:**

```yaml
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
sizing: { type: vol_target, annual_vol: 0.10, max_leverage: 1.0 }
costs:  { commission_bps: 5, slippage_bps: 3 }
alerts: { telegram: true, cooldown_bars: 3 }
```

**Cross-sectional** — only possible because the platform is multi-asset:

```yaml
name: sector_rotation
type: cross_sectional
universe: sectors
rank_by: "momentum(close, 252, skip=21)"
select:  { top_n: 3, rebalance: monthly }
filter:  "close > sma(close, 200)"        # trend filter, applied per member
weights: { scheme: inverse_vol, lookback: 60 }
costs:   { commission_bps: 5, slippage_bps: 3 }
benchmark: SPY
alerts:  { telegram: true, on: rebalance }
```

---

## 6. Modeling approach

The models stay small and rigorously evaluated. Sophistication buys very little here; correct
validation buys everything.

- **Two targets.** Sign of the n-day forward return, and n-day realized volatility. Volatility is
  genuinely forecastable; direction is close to unforecastable. Modeling both makes that contrast
  explicit rather than hiding it.
- **Panel training.** Models train on stacked instrument-date observations across a universe, with
  instrument identity as a categorical feature. This yields far more training data than any single
  series provides and generalises to newly added instruments without retraining from scratch.
- **Baselines first.** Naive, drift and ARIMA benchmarks are computed before any gradient boosting,
  and every result is reported next to its baseline.
- **Walk-forward validation with purging and embargo** between train and test windows. A random
  train/test split on time series is silently wrong and produces flattering nonsense.
- **Features.** Per-instrument: multi-horizon returns, realized volatility, ATR, RSI, distance from
  moving averages. Cross-sectional: momentum rank, volatility rank, relative strength versus
  benchmark, breadth. Macro: real-yield and dollar deltas. Plus calendar effects.
- **Metrics.** Statistical (AUC, Brier) and financial (Sharpe, max drawdown, hit rate, turnover) —
  all net of costs, all against benchmark.

**Expected honest outcome:** direction prediction lands near 0.52 AUC, barely better than a coin
flip, while volatility forecasting is clearly useful. Both get published. A pipeline that can prove
its model is weak is more valuable than one that cannot tell.

---

## 7. Dimensional model

```
dim_instrument   (SCD2, sourced from registry)
  instrument_key · symbol · name · asset_class · sub_class · exchange
  currency · calendar · inception · enabled · valid_from · valid_to · is_current

dim_universe                     bridge_universe_member
  universe_key · name              universe_key · instrument_key
  description · benchmark_symbol   valid_from · valid_to

dim_date
  date_key · date · trading_day_xnys · month_end · quarter_end

fct_ohlcv_daily        grain: instrument × date
  open · high · low · close · adj_close · volume
  source · ingested_at · quality_flag

fct_macro_daily        grain: series × date
fct_features_daily     grain: instrument × date  (per-instrument + cross-sectional)
fct_signals            grain: strategy × instrument × date
fct_backtest_runs      grain: run_id   (+ fct_backtest_positions, fct_backtest_metrics)
dq_results             grain: check × asset × partition × run
dq_source_agreement    grain: instrument × date × source_pair
```

Every fact table is partitioned by instrument and date, which is what makes single-instrument
backfills cheap.

---

## 8. MVP — the Lean Warehouse

**Decision: this is what gets built first.** No cluster, no broker, no object store. Everything runs
in-process; the platform boots in seconds and the full test suite runs in CI in under two minutes.

Spark appears in exactly one job — a local-mode historical backfill across the full instrument
universe — so the heavy-data path exists and is unit-tested without imposing cluster operations on
every other component.

```
  Stooq · FRED · Twelve Data          instruments/*.yml  (registry)
            │                                  │
            ▼  Python ingest (httpx + Polars)  │ drives partitions,
     idempotent · watermarked · retried  ◀─────┘ sources, calendars
            │
     raw Parquet on local disk  ──── one PySpark local-mode job:
            │                        full multi-instrument backfill
            ▼
        DuckDB  ◀── dbt (staging → intermediate → marts)
            │        + dbt tests + docs + dbt-expectations
            ▼
   marts: dim_instrument · dim_universe · fct_ohlcv_daily
          fct_features_daily · fct_signals · fct_backtest_runs
            │
     ┌──────┴───────┬──────────────┬──────────────┐
     │ LightGBM     │ Backtest     │ FastAPI      │
     │ + MLflow     │ engine       │ + Streamlit  │
     └──────────────┴──────────────┴──────────────┘
                                          │
                              Telegram bot (scheduled evaluator)

  Dagster orchestrates, partitioned by instrument × date
  GitHub Actions runs the whole thing on every PR
```

| Layer | Choice |
|---|---|
| Language | Python 3.12, `uv`, ruff, mypy (strict), pytest |
| Registry | YAML + Pydantic validation, versioned in git |
| Ingestion | httpx + Polars, retry/backoff, watermark table, raw Parquet landing |
| Calendars | `exchange_calendars` for per-instrument trading-day alignment |
| Heavy backfill | PySpark local mode — one job, unit-tested with `chispa` |
| Warehouse | DuckDB, file-backed with versioned snapshots |
| Transform | dbt-duckdb — staging / intermediate / marts, with tests, docs, exposures |
| Data quality | dbt tests + `dbt-expectations` + freshness checks + reconciliation |
| ML | LightGBM, scikit-learn, MLflow (local file backend) |
| Serving | FastAPI + Streamlit |
| Alerts | python-telegram-bot, triggered by a Dagster schedule, outbox-backed |
| Infra | Single Dockerfile + Compose |
| CI/CD | GitHub Actions: lint → type → test → full `dbt build` on a slim seeded dataset → docker build |

**Trade-off accepted.** No Kafka, no CDC, no real-time path, and only a thin Spark surface. In
exchange the project is realistically finishable, and because everything runs in CI the pipeline is
genuinely tested rather than merely testable.

---

## 9. Extension — the Lakehouse

Once the MVP is complete and green, the same system is promoted onto lakehouse infrastructure. The
dbt models and Dagster assets survive the migration nearly unchanged — which is the strongest
validation that the layering was right.

```
  Stooq · FRED  ──▶ batch ingest ──┐
                                   ├──▶  BRONZE  raw, append-only, quarantine
  Finnhub WS ──▶ producer ──▶      │        Delta Lake on MinIO (S3 API)
                  Redpanda         │
  Postgres  ──▶ Debezium CDC ──────┘
  (app DB)      strategies, users
                                   ▼
                        PySpark batch jobs
                  SILVER  deduped · adjusted · calendar-aligned
                          typed · contract-validated
                                   ▼
                           dbt (star schema)
                  GOLD    same marts as MVP, same SQL
                                   ▼
        ┌──────────────┬────────────────┬─────────────────────┐
        │  MLflow      │ Backtest engine│ FastAPI + gRPC      │
        └──────────────┴────────────────┴─────────────────────┘

  Dagster · Great Expectations · Grafana · Terraform on AWS
```

**What each step adds**

| Step | Change | Why |
|---|---|---|
| A1 | DuckDB → Delta Lake on MinIO | Time travel, schema evolution, ACID on the lake; enables snapshot-exact backtest reproduction |
| A2 | Transforms → PySpark | Horizontal scale as the universe and history grow |
| A3 | Finnhub WS → Redpanda → Structured Streaming | Intraday signals instead of end-of-day only |
| A4 | Debezium CDC on the app Postgres | Strategy edits reach the live evaluator without polling |
| A5 | Terraform → AWS (S3, Glue, EMR Serverless) | Reproducible infrastructure |
| A6 | OpenTelemetry → Prometheus → Grafana | Freshness, lag and quality visible over time |
| A7 | gRPC `SignalService.StreamSignals` | Streaming interface for programmatic consumers |

**Migration is incremental.** Each step ships independently and the system stays working throughout.
There is no big-bang rewrite, and no step is a prerequisite for using the platform.

---

## 10. Comparison of the two stages

| Dimension | MVP — Lean Warehouse | Extension — Lakehouse |
|---|:---:|:---:|
| Time to complete | 3–4 weekends | +4–5 weekends |
| Storage | DuckDB file | Delta Lake on S3/MinIO |
| Compute | In-process Polars + one Spark job | PySpark throughout |
| Latency | End-of-day | Intraday streaming |
| Runs end-to-end in CI | Fully | Partly |
| Local RAM required | ~2 GB | ~12 GB |
| Cold-start setup | Seconds | Minutes |
| Adding an instrument | Registry edit | Registry edit (unchanged) |
| dbt marts | Same SQL | Same SQL |

The last two rows matter most: neither the extensibility mechanism nor the business logic changes
between stages. Only the infrastructure underneath does.

---

## 11. Engineering details worth building in

Cheap to add, and each addresses a failure mode a naive pipeline hits in production.

- **Quarantine and circuit breaker.** Malformed rows land in `bronze_quarantine` rather than being
  silently dropped. Failed quality checks block downstream signal assets, so no alert is ever
  emitted from unvalidated data.
- **Point-in-time correct features.** Every feature joined as-of its decision timestamp, with purged
  and embargoed walk-forward CV. A dedicated test fails if lookahead is introduced.
- **Per-instrument partitioning.** Backfills, quality checks and recomputes operate on one
  instrument's partitions. Adding the 40th ETF costs the same as adding the 4th.
- **Reproducible backtests.** Each run keyed by `hash(strategy_yaml + registry_version +
  data_version + code_sha)` and written to `fct_backtest_runs`.
- **Idempotent alerts.** Unique key `(strategy_id, instrument, bar_ts, rule_id)`. A consumer restart
  must not re-send yesterday's signals, and a test asserts it doesn't.
- **Transactional outbox** for Telegram delivery — no lost and no duplicated messages across
  restarts or failures.
- **Survivorship-bias awareness.** The registry records `enabled` and delisting dates rather than
  deleting instruments, so historical backtests include funds that no longer trade.
- **Data contracts.** Pydantic schemas at every stage boundary, explicitly versioned, with a
  documented policy for breaking changes.
- **Architecture decision records** in `docs/adr/` — registry as code versus database, DuckDB versus
  Postgres, Delta versus Iceberg, Dagster versus Airflow, why two model targets, why the outbox.
- **`docs/DATA_QUALITY.md`** — every check, its threshold, its owner, and what happens when it fires.

---

## 12. Repository layout

```
finflow/
├── README.md                  # architecture diagram, quickstart, results
├── PROJECT.md                 # this document
├── IMPLEMENTATION.md          # milestone-by-milestone build plan
├── Makefile                   # make up · make demo · make test · make backfill
├── docker-compose.yml
├── pyproject.toml             # uv, ruff, mypy, pytest config
├── .github/workflows/ci.yml
├── instruments/               # THE REGISTRY — add an ETF here
│   ├── equity_us.yml
│   ├── commodities.yml
│   ├── rates_credit.yml
│   ├── macro.yml
│   └── universes.yml
├── docs/
│   ├── adr/
│   ├── DATA_QUALITY.md
│   ├── ADDING_AN_INSTRUMENT.md
│   └── architecture.png
├── src/finflow/
│   ├── registry/              # registry loading, Pydantic models, validation
│   ├── ingest/                # source clients, watermarks, retry/backoff
│   ├── spark/                 # multi-instrument backfill job
│   ├── features/              # per-instrument + cross-sectional, point-in-time
│   ├── ml/                    # panel training, walk-forward CV, MLflow
│   ├── backtest/              # vectorized engine, cost model, metrics
│   ├── strategy/              # YAML DSL parser + evaluator (backtest ↔ live)
│   ├── api/                   # FastAPI REST
│   ├── alerts/                # Telegram bot, outbox, idempotency
│   └── quality/               # expectations, reconciliation, freshness
├── dbt/                       # staging → intermediate → marts, tests, docs
├── orchestration/             # Dagster assets, partitions, sensors, checks
├── strategies/                # example YAML strategies
└── tests/                     # unit · spark (chispa) · integration · dbt
```

---

## 13. Next steps

See `IMPLEMENTATION.md` for the milestone-by-milestone build plan.
