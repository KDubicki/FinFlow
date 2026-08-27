# Adding an instrument

Adding an ETF is an edit to one YAML file under `instruments/`. No code changes,
no migrations. If you find yourself editing anything under `src/`, something is
wrong with the design and that is worth stopping to investigate.

## The whole workflow

1. Add an entry to the appropriate `instruments/*.yml` file — `equity_us.yml`,
   `commodities.yml`, `rates_credit.yml`, or a new file if none fits.
2. Add it to any universe in `instruments/universes.yml` that should contain it.
3. Run `make registry` to validate locally.
4. Open a pull request. The `registry` job validates the schema; `make check`
   runs the same thing.
5. On merge the instrument is picked up by the next scheduled run.

## The fields

```yaml
instruments:
  - symbol: SPY                     # uppercase; unique across every file
    name: SPDR S&P 500 ETF Trust
    asset_class: equity             # equity | commodity | rates | credit | currency
    sub_class: us_large_cap         # free-form, for cost and baseline defaults
    exchange: ARCA
    currency: USD
    calendar: XNYS                  # must be known to exchange_calendars
    inception: 1993-01-22           # the fund's first day
    backfill_start: 1993-02-01      # never before inception
    delisted: null                  # a date if the fund ceased to exist
    sources:
      stooq: spy.us                 # primary; the vendor's own symbol
      twelvedata: SPY               # optional, reconciliation only
    return_basis: price             # price only, until a distributions source exists
    distribution_yield_hint: 0.013  # documentation only; flags price-return drag
    costs: { commission_bps: 2, spread_bps: 1 }
    min_adv_usd: 50_000_000         # below this, no signal is emitted that day
    ucits_equivalent: CSPX.UK       # what an EU retail account can actually buy
    enabled: true
    tags: [core, benchmark]
```

### The fields that are easy to get wrong

**`costs`** are a floor, not an estimate. A strategy may raise them, never lower
them. A flat assumption across the universe understates the round trip on thin
ETFs by five to ten times and manufactures alpha that cannot be earned
(`PROJECT.md` §5.7). If you do not know an instrument's typical spread, look it
up rather than copying SPY's.

**`min_adv_usd`** is the floor below which no signal is emitted, not the fund's
actual average volume. Set it conservatively; the current values are placeholders
to be revisited once real volume is measured.

**`ucits_equivalent`** is what a Polish brokerage account can actually buy. Under
PRIIPs an EU retail investor generally cannot buy US-domiciled ETFs, so an
instrument with no mapping is research-only. **Leave it `null` rather than
guessing** — a wrong mapping produces a target portfolio that cannot be executed,
which is worse than an honest gap.

**`enabled` and `delisted` are different things.** `enabled: false` is a choice
and stops future ingestion. `delisted` is a fact and records that the fund ceased
to exist. Neither ever removes the entry: the append-only history is the only
defence this project has against survivorship bias (`PROJECT.md` §6.5).

**`return_basis`** must be `price`. The field exists so that a distributions feed
is additive rather than a migration, but no free source supplies one, and
building a fake total-return series would be a wrong number that looks
authoritative (`PROJECT.md` §6.4).

## Universes

```yaml
universes:
  sectors:
    description: The eleven SPDR sector funds
    members:
      - XLE
      - XLF
      - { symbol: XLRE, from: 2015-10-08 }   # date-effective membership
      - { symbol: XLC,  from: 2018-06-19 }
    benchmark: SPY
```

A member may be a bare symbol or a mapping with `from` and `to`. Membership is
resolved **as of the evaluation date, never as of today**: a backtest of
`sectors` starting in 2010 holds nine members, not eleven. If you add an
instrument that listed partway through the history you care about, give it a
`from` date — otherwise every backtest before that date silently includes a fund
that did not exist.

Widening a universe should only ever **add** members. Changing what a name means
changes every strategy that references it, without those strategies changing.

## Macro series

```yaml
series:
  - id: cpi_headline          # lowercase, underscores
    source: fred
    source_id: CPIAUCSL       # the vendor's identifier
    unit: index
    frequency: monthly
    release_lag_days: 14      # published ~mid-month, for the prior month
    revised: true             # seasonal factors restated annually
    vintage_aware: true       # requires revised; read via ALFRED realtime params
    transform: [yoy, mom]
```

`release_lag_days` is not decoration. It is what stops the pipeline using
March's CPI print in a decision made on 1 March. Get it wrong and the backtest
reads a number that did not exist for six weeks, which looks like skill.

## What the validation will tell you

Every failure names the file and the value. The checks are:

| Check | Message you will see |
|---|---|
| Symbol defined twice | `duplicate symbol(s) across the registry: GLD` |
| Universe names an unregistered instrument | `universe 'metals' references unknown instrument(s): IAU` |
| Benchmark not registered | `universe 'metals' has unknown benchmark 'SPY'` |
| Calendar unknown to `exchange_calendars` | `calendar 'XMOON' is not recognised` |
| History claimed before the fund existed | `backfill_start ... precedes inception` |
| Delisting on or before inception | `delisted ... is not after inception` |
| Source with no client | `sources: Input should be 'stooq', 'fred', ...` |
| Lowercase symbol | `symbol: String should match pattern` |
| Misspelled field | `liquidity: Extra inputs are not permitted` |
| `vintage_aware` without `revised` | `vintage_aware requires revised` |

Validation is offline by design. Whether the vendor actually returns data for
your symbol is checked by the nightly live-source job, not on the pull request:
a PR check that depends on Stooq being up fails for reasons unrelated to the PR,
and a check that fails for unrelated reasons is a check that gets ignored.

So the first backfill is where a wrong vendor symbol surfaces. If a new
instrument ingests nothing, check `sources:` against the vendor's own listing
before looking anywhere else.
