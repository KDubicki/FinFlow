# Setup — what you need to do

This project runs **on-premise**: the raw data, the warehouse and the
operational store are files on a disk you own, and the daily run fires from a
timer on your own machine. Nothing it produces leaves the box.

It does still fetch prices from vendors over the internet, so it needs API keys.
That is the only thing here that needs a human.

Two steps take about ten minutes. **Step 3 is a decision, not a task**, and it is
the one that matters.

---

## 0. Do these three things now  ⬅ start here

Everything below this section is context. These three are the actions.

### 0.1 Install the git hooks (one command, 30 seconds)

```
make install
```

The hooks are configured but **not installed in your clone**, which means
`gitleaks` is not currently checking your commits for secrets, and nothing is
stopping a commit straight to `main`. Both matter more now that there are real
API keys in play.

### 0.2 See the whole pipeline run, with no network and no keys

```
make backfill-offline     # synthetic source -> append-only raw zone
make build                # raw -> bronze -> dbt -> marts -> serving snapshot
make docs                 # browsable model lineage
```

That exercises every part of the path except the vendors themselves: the raw
zone, the bronze resolution, the star schema with contracts enforced, and the
serving snapshot. If this works, the machine is ready.

To look at what came out:

```
uv run python -c "
from finflow.adapters.warehouse import DuckDBWarehouse
w = DuckDBWarehouse('data/serving.duckdb', read_only=True)
print(w.query('SELECT * FROM fct_ohlcv_daily ORDER BY date DESC LIMIT 5'))
"
```

### 0.3 Point the data and backup directories at real locations

Add to `.env`:

```
FINFLOW_DATA_DIR=/srv/finflow/data
FINFLOW_BACKUP_DIR=/mnt/backup/finflow
```

`FINFLOW_BACKUP_DIR` **must be on a different physical device** — an external
disk, a NAS mount, a second internal drive. Not a folder next to the data.

This is the one thing in the whole setup where getting it wrong is
unrecoverable. The raw zone cannot be rebuilt from anything; the warehouse and
every backtest are rebuilt *from it*. On a single machine, the failure that
actually happens is the disk dying and taking the data and its "backup"
together. Everything else in this project can be fixed after the fact. This
cannot.

The code side is done: `VACUUM INTO` snapshots of the ops store, verified before
they overwrite anything, with the restore path exercised by a test rather than
assumed. It just needs somewhere safe to write to.

---

## 1. FRED API key — required for macro data

The macro series (`DFII10`, `DTWEXBGS`, `VIXCLS`, and later CPI) come from the
St. Louis Fed. The key is free, instant, and has no meaningful rate limit.

1. Go to <https://fredaccount.stlouisfed.org/apikeys>
2. Create an account if you do not have one.
3. Request an API key — issued immediately, a 32-character lowercase hex string.
4. Put it in `.env` at the repository root:

   ```
   FINFLOW_FRED_API_KEY=your_key_here
   ```

`.env` is gitignored and `gitleaks` runs in both pre-commit and CI, so a key
cannot reach the repository by accident. Never put a real value in
`.env.example`.

---

## 2. Decide where the data lives, and where it is mirrored

By default everything goes under `./data` in the repository. That is fine while
you are developing. Before the pipeline starts running unattended, two things
need deciding.

**Where the data directory lives.** Set it explicitly rather than leaving it
relative to a checkout you might move:

```
FINFLOW_DATA_DIR=/srv/finflow/data
```

**Where it is mirrored — this is the important one.** The raw zone is the only
thing in this system that cannot be rebuilt. The warehouse is regenerated from it
in seconds; the features and backtests are derived from that. Lose the raw zone
and you lose the history.

On-premise, that protection is your responsibility and it comes down to one rule:
**a copy on the same disk is not a backup.** The failure that actually happens on
a single machine is that the disk dies and takes the data and its "backup" with
it.

So point the mirror at genuinely separate hardware — an external drive, a NAS
mount, a second internal disk:

```
FINFLOW_BACKUP_DIR=/mnt/backup/finflow
```

The nightly job will `rsync --archive --link-dest` the raw zone there, so each
night is a browsable snapshot that costs only the changed files, plus an
encrypted copy of `ops.sqlite`. That last file is under a megabyte and is the
only state a rebuild cannot recreate, so it is worth also keeping a copy
somewhere physically elsewhere.

Nothing in the code can delete a raw partition — the `ObjectStore` port has no
delete method and a test asserts it. Pruning is a deliberate manual act against
the filesystem. That is the on-premise stand-in for a delete-less bucket
credential, and it is enforced by the type system rather than by a policy you
have to remember.

---

## 3. Decide what the primary price source is  ⚠️

**This is a real problem and it needs your decision.**

`PROJECT.md` §6.1 names Stooq as the primary source for daily OHLCV. When I
tested it from this machine, Stooq did not return CSV. It returned an HTML page
with **HTTP 200** containing a JavaScript proof-of-work challenge:

```
This site requires JavaScript to verify your browser.
```

This happened for every request, with any User-Agent, including a browser one.
It is not a rate limit — it is an anti-bot gate in front of the whole endpoint.

I have **not** written anything to solve that challenge, and I would not: it is
an access control the site owner put there deliberately, and working around it is
not something to build into a system meant to run unattended for years.

### What I built anyway

The `StooqClient` validates the content type and the header row **before**
parsing, so this page raises `SourceRateLimited` and the symbol is deferred to
the next run. It never becomes a price bar. The captured page is in
`tests/fixtures/stooq_blocked.html` and the test asserts exactly that.

So the failure is safe and visible. It is just a failure.

### What you need to check

Running on-premise genuinely helps here: a residential Polish IP is treated very
differently from a datacentre range, and `PROJECT.md` §6.1 already notes Stooq
"blocks cloud egress ranges aggressively". **Please run this on the machine that
will host the pipeline** — type it with a leading `!` in the Claude Code prompt,
or in any terminal:

```
! curl -s "https://stooq.com/q/d/l/?s=gld.us&d1=20240102&d2=20240110&i=d" | head -3
```

- **`Date,Open,High,Low,Close,Volume`** — Stooq works from your network and the
  plan is unchanged. This is a realistic outcome now that the daily run is
  on-premise rather than on a cloud runner.
- **`<!DOCTYPE html>`** — Stooq is not usable as the primary source, and we pick
  a different one.

### The options if Stooq is out

| Option | Free tier | Enough for daily? | Enough for backfill? | Notes |
|---|---|---|---|---|
| **Twelve Data** | 800 calls/day, 8/min | Yes — 8 instruments needs 8 calls | Slow but possible over several days | Already planned as the reconciliation source; promoting it is a registry edit plus one settings key |
| **Alpha Vantage** | 25 calls/day | No | No | Manual repairs only, as `PROJECT.md` already says |
| **yfinance** | unofficial | Yes | Yes | `PROJECT.md` §6.1 excludes it as primary — ambiguous terms, breaks without warning. Pragmatic and the least defensible |
| **EODHD / Marketstack** | limited | Partly | No | Paid tiers are cheap (~$20/mo) if you want it to just work |

**My recommendation:** promote **Twelve Data** to primary for the eight-instrument
slice. 800 calls/day is comfortable for a daily run, the client is on the M5 task
list anyway so it is not wasted work, and the free tier is a documented allowance
rather than something that might be withdrawn. Backfill takes a few days of
patient running, which the `deferred_until` resume mechanism already handles —
that is exactly what it was built for.

To do that, get a key at <https://twelvedata.com/pricing> (Basic, free) and add:

```
FINFLOW_TWELVEDATA_API_KEY=...
```

Tell me which way you want to go and I will wire it up. Until then the pipeline
runs end-to-end on the synthetic source, which is deterministic, offline, and
proves every part of the path except the vendor itself.

---

## 4. Later — the machine itself

Not needed until the pipeline actually runs unattended, but decided in advance
so it is not decided at 06:00 on a Tuesday.

- **An always-on machine.** A mini PC, a home server, a spare box. Not a laptop:
  it will be shut at 05:00.
- **Nothing forwarded to it.** The pipeline makes outbound calls to vendors and
  to Telegram; nothing needs to reach it. Do not forward a port, and check UPnP
  is off — Docker publishes ports by inserting its own iptables rules, which
  **bypass UFW entirely**, so a tidy firewall in front of a published port is not
  the protection it looks like.
- **A dead-man's switch, running elsewhere.** The daily run pings
  <https://healthchecks.io> (free) on success, and it emails you if a ping does
  not arrive. This is the one external service the design keeps deliberately: a
  monitor running on the box cannot tell you the box is down, which is precisely
  the failure it exists to catch.

---

## What works right now without any of the above

```
make check     # lint, types, dependency rule, registry, tests
```

The synthetic source generates deterministic OHLCV with realistic volatility
clustering. It is what keeps CI hermetic and what will make `make demo` work
with the network off.
