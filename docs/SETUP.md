# Setup — what you need to do

Two things need a human: credentials, and one decision about the primary data
source. Everything else is automated.

Work through this in order. Steps 1 and 2 take about ten minutes. **Step 3 is a
decision, not a task**, and it is the one that matters.

---

## 1. FRED API key — required for macro data

The macro series (`DFII10`, `DTWEXBGS`, `VIXCLS`, and later CPI) come from the
St. Louis Fed. The key is free, instant, and has no meaningful rate limit.

1. Go to <https://fredaccount.stlouisfed.org/apikeys>
2. Create an account if you do not have one.
3. Request an API key. It is issued immediately — a 32-character lowercase
   hex string.
4. Put it in `.env` at the repository root:

   ```
   FINFLOW_FRED_API_KEY=your_key_here
   ```

`.env` is gitignored and `gitleaks` runs in both pre-commit and CI, so a key
cannot reach the repository by accident. Never put it in `.env.example`.

**Check it works:**

```
make check-sources
```

---

## 2. Cloudflare R2 bucket — not needed yet, but needed before the first real backfill

Until you set this up, everything writes to `./data/raw` on your machine via
`LocalObjectStore`, which is fine for development. R2 matters when the pipeline
starts running somewhere that is not your laptop.

The raw zone is the **only unrecoverable asset in this project**. Everything
else — the warehouse, the features, the backtests — is rebuilt from it in
seconds. So it gets three protections, and all three are worth the ten minutes.

1. Create a Cloudflare account and enable R2 (the free tier is several hundred
   times what this project needs).
2. Create a bucket, e.g. `finflow-raw`.
3. **Turn on object versioning for the bucket.** Settings → Versioning → Enable.
   This is the protection against a bug that writes the wrong bytes.
4. Create an API token — R2 → Manage API Tokens → Create:
   - Permission: **Object Read & Write**
   - **Not** Admin, and **not** anything including delete.
   - Scope it to the one bucket, not the whole account.
5. Create a **second, separate bucket** for backups, e.g. `finflow-backup`, with
   its own token. A backup in the same bucket as the data is not a backup.
6. Put the values in `.env`:

   ```
   FINFLOW_OBJECT_STORE=s3
   FINFLOW_S3_BUCKET=finflow-raw
   FINFLOW_S3_ENDPOINT_URL=https://<account-id>.r2.cloudflarestorage.com
   FINFLOW_S3_ACCESS_KEY_ID=...
   FINFLOW_S3_SECRET_ACCESS_KEY=...
   ```

**Why the token must not be able to delete:** the pipeline has no legitimate
reason to remove anything from the raw zone, and the `ObjectStore` port has no
delete method at all. Giving it a credential that *could* delete would make
"I accidentally wiped thirty years of history" merely unlikely rather than
impossible. Lifecycle rules, configured in the Cloudflare console, are how
anything ever gets removed.

---

## 3. Decide what the primary price source is  ⚠️

**This is a real problem and it needs your decision.**

`PROJECT.md` §6.1 names Stooq as the primary source for daily OHLCV. When I
tested it from this machine, Stooq did not return CSV. It returned an HTML page
with **HTTP 200** containing a JavaScript proof-of-work challenge:

```
This site requires JavaScript to verify your browser.
```

This happened for every request, with any User-Agent, including a plain browser
one. It is not a rate limit — it is an anti-bot gate in front of the whole
endpoint.

I have **not** written anything to solve that challenge, and I would not: it is
an access control the site owner put there deliberately, and working around it
is not something to build into a system that is supposed to run unattended for
years.

### What I built anyway

The `StooqClient` is implemented and correct. It validates the content type and
the header row **before** parsing, so this page raises `SourceRateLimited` and
the symbol is deferred to the next run. It never becomes a price bar. The
captured page is in `tests/fixtures/stooq_blocked.html` and the test asserts
exactly that.

So the failure is safe and visible. It is just a failure.

### What you need to check

The block may be specific to this machine's network. `PROJECT.md` §6.1 already
notes Stooq "blocks cloud egress ranges aggressively", so a residential Polish
IP may behave differently. **Please run this yourself** — type it with a leading
`!` in the Claude Code prompt, or in any terminal:

```
! curl -s "https://stooq.com/q/d/l/?s=gld.us&d1=20240102&d2=20240110&i=d" | head -3
```

- **If you see `Date,Open,High,Low,Close,Volume`** — Stooq works from your
  network, and the plan is unchanged. The daily run will need to happen from a
  network that also works, which rules out GitHub Actions runners and probably
  most VPS providers. Tell me and I will note it.
- **If you see `<!DOCTYPE html>`** — Stooq is not usable as the primary source
  and we need to pick a different one.

### The options if Stooq is out

| Option | Free tier | Enough for daily? | Enough for backfill? | Notes |
|---|---|---|---|---|
| **Twelve Data** | 800 calls/day, 8/min | Yes — 8 instruments needs 8 calls | Slow but possible over several days | Already planned as the reconciliation source; promoting it to primary is a registry edit plus one settings key |
| **Alpha Vantage** | 25 calls/day | No | No | Manual repairs only, as `PROJECT.md` already says |
| **yfinance** | unofficial | Yes | Yes | `PROJECT.md` §6.1 excludes it as primary — ambiguous terms, breaks without warning. It is the pragmatic choice and the least defensible one |
| **EODHD / Marketstack** | limited | Partly | No | Paid tiers are cheap (~$20/mo) if you want this to just work |

**My recommendation:** promote **Twelve Data** to primary for the eight-instrument
slice. 800 calls/day is comfortable for a daily run, the client is on the M5 task
list anyway so it is not wasted work, and the free tier is an honest, documented
allowance rather than something that might be withdrawn. Backfill takes a few
days of patient running, which the `deferred_until` resume mechanism already
handles — that is exactly what it was built for.

To do that you need a Twelve Data key:

1. <https://twelvedata.com/pricing> → Basic (free) → sign up.
2. Copy the API key.
3. Add to `.env`:

   ```
   FINFLOW_TWELVEDATA_API_KEY=...
   ```

Tell me which way you want to go and I will wire it up. Until then the pipeline
runs end-to-end on the synthetic source, which is deterministic, offline, and
proves every part of the path except the vendor itself.

---

## What works right now without any of the above

```
make demo-ingest      # synthetic source -> local raw zone, no network
make check            # lint, types, dependency rule, registry, tests
```

The synthetic client generates deterministic OHLCV with realistic volatility
clustering. It is what keeps CI hermetic and what will make `make demo` work on
a plane.
