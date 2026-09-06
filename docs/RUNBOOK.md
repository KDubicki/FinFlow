# Runbook

What each message means, and the first three things to check. Written for the
person reading a phone at 06:00, who is also the person who wrote it and will
not remember.

**The rule that governs everything below:** when in doubt, issue no
instruction. A missed trade costs an opportunity; an instruction from bad data
costs money and, worse, costs trust in the system — which is the thing that
takes months to rebuild (`PROJECT.md` §15).

Any incident that takes more than ten minutes to diagnose gets an entry here the
same day (daily-operations standard 5).

---

## The digest

One message, every morning, whether or not anything happened. It looks like
this:

```
FinFlow 2026-09-06 · ok

No action.

Data      412 rows · fresh to 2026-09-05
Checks    37 passed · 0 failed · 0 restatements
Rules     1 evaluated · 0 delivered

Held
  GLD 12 · 100% (target 100%)

run 3f2a91c4b0d7 · snapshot 3f2a91c4b0d7
```

The first word after the date is the only thing to read when in a hurry:

| Status | Meaning | What to do |
|---|---|---|
| `ok` | Everything ran, data is fresh, checks passed | Act on the action lines, if any. Most days there are none. |
| `degraded` | The run finished, but a check failed or the data is more than a day old | Read the `!` lines. Do not act on a target built from stale data. |
| `failed` | A step did not complete | **No instruction is issued.** Diagnose below. |

### No digest at all

This is the one that matters, and it is why the dead-man's switch exists: a
digest that does not arrive is indistinguishable from a box that is switched
off, so healthchecks.io emails when the ping does not come.

1. `systemctl list-timers 'finflow-*'` — did the timer fire?
2. `journalctl -u finflow-daily -n 200` — did the run start, and where did it stop?
3. `ss -tulpn` / `ping 1.1.1.1` — is the box on the network at all?

If the machine was off, `Persistent=true` fires the missed run on the next boot.
Let it: a late digest is worth more than a skipped one.

---

## Messages, and what they mean

### `! Data is N days old — treat any target as stale`

The freshest bar in the warehouse is behind today. The decision was still made,
because a paused pipeline that decides nothing is not more honest than one that
decides visibly-stale — but the message says so, and so does every decision
message (`PROJECT.md` §7.3).

1. `journalctl -u finflow-daily | grep ingestion_failed` — which source, which symbol?
2. `sqlite3 data/ops.sqlite 'select * from watermarks order by last_run_at desc limit 10'` —
   is a `(source, symbol)` pair deferred? A rate limit sets `deferred_until` and
   the next run resumes on its own.
3. Weekend or holiday? Friday's bar on a Monday morning is not an incident.

### `! ingest: ...` / `FAILED` on the ingest step

One instrument failing must not stop the others — the failure domain is the
instrument (`PROJECT.md` §4.4). The step only fails outright when *nothing*
landed.

- **`SourceRateLimited`** — Stooq serves a JavaScript interstitial with HTTP 200
  when it decides you are a robot. The client refuses to parse it and defers the
  whole source; the next run resumes. If it persists for two days, promote Twelve
  Data (see `docs/SETUP.md` §3).
- **`SymbolNotFound`** — an incident against the *registry entry*, not the run.
  The vendor symbol in `instruments/*.yml` is wrong or the fund was renamed.
  Retrying cannot help.
- **`AuthenticationFailed`** — the one failure no later instrument can work
  around, so the run stops. Rotate the key, put it in `/etc/finflow/finflow.env`,
  re-run by hand.

### `! build: dbt build failed (N checks failed)`

Evaluation is skipped and no instruction is issued. This is deliberate: a
decision from a warehouse that did not build is exactly the instruction-from-bad-
data the trust ladder drops a rung for.

1. `make build` locally against a copy, or `journalctl -u finflow-daily` — dbt's
   own output is in the log, and it names the model.
2. `dbt/target/run_results.json` lists every failed test by name.
3. `no_trading_day_gaps` failing usually means a missed session, not a broken
   model: cross-check `dq_restatements` and the watermarks before touching SQL.

The warehouse is disposable. If it is wedged, delete it and re-run — the raw
zone rebuilds it in seconds, and that path is exercised by a test rather than
hoped for.

```bash
rm data/warehouse.duckdb && sudo systemctl start finflow-daily.service
```

### `Withheld` lines

Nothing is ever suppressed silently (`PROJECT.md` §7.7). Every line here is
either a control you set or an instrument the rule could not evaluate:

| Line | Meaning |
|---|---|
| `mute GLD until 2026-10-01` | You muted it. `/unmute GLD` ends it early. |
| `pause <strategy>` | The strategy still computes and still records; it just does not instruct. `/resume <strategy>`. |
| `hold * until ...` | All rebalancing suspended. `/unhold`. |
| `SLV: insufficient history — the rule needs 50 bars` | Newly added instrument, still warming up. It resolves itself. |
| `IAU: no bars on or before this date` | The instrument is in the universe but has no data. Check ingestion for that symbol. |

A control you do not remember setting is not a bug in the digest — it is the
digest doing its job. `/status` lists everything in force.

### `! Disk 81% full on the box`

The likeliest way a small host dies, and entirely preventable (`PROJECT.md`
§11.6). In order of yield:

1. `du -sh data/raw data/*.duckdb dbt/target dbt/logs`
2. `journalctl --vacuum-size=200M`
3. `duckdb data/warehouse.duckdb 'checkpoint'` — the WAL can outgrow the file.

**Never** delete anything under `data/raw`. It is the one asset that cannot be
rebuilt, which is why the object-store port has no delete method at all.

### `! abandoned: <decision> after N attempts`

Delivery failed six times and the row was retired so it could not block the
queue behind it. The decision is still recorded; only the message was lost.

```sql
-- what it would have said
select payload from alerts_outbox where decision_id = '<id>';
```

Fix the transport (usually a revoked bot token), then decide by hand whether the
instruction is still current. Do not re-queue a stale rotation.

---

## The bot

Commands are applied at the **start of the next scheduled run** — this is a
timer, not a daemon, so nothing is listening between runs. The reply says when
each took effect.

| Command | Effect |
|---|---|
| `/position GLD 12 [180.20]` | Records what you actually hold. Nothing automated writes this. |
| `/position GLD 0` | Closes the position. |
| `/pause <strategy>` · `/resume <strategy>` | Stops and restarts instructions. Recording continues throughout, so the counterfactual survives. |
| `/mute GLD 14d` · `/unmute GLD` | Excludes one instrument from targets. A mute must state an end date. |
| `/hold 2w` · `/unhold` | Suspends all rebalancing. |
| `/status` | Last good run, holdings, controls, queued alerts. |

Only the configured chat is obeyed. A bot token is discoverable, and
`/position GLD 0` from a stranger would silently corrupt the holdings the digest
is computed from.

---

## Recovery

### Restore the ops store

The only state a rebuild cannot recreate. The archive is verified *before*
anything is overwritten, so a corrupt backup fails without destroying the
database it was meant to replace.

```bash
finflow-backup --check      # monthly drill: verify, change nothing
finflow-backup --restore    # newest backup over the live store
```

Worst case is re-sending one day of alerts.

### Rebuild the box

`docs/SETUP.md` and `deploy/README.md`. In order: provision, restore the ops
store from the second device, re-mirror the raw zone back, run
`finflow-daily --skip-ingest` to rebuild the warehouse, then let the timer take
over.

### Rotate the bot token

1. `/revoke` then `/token` to @BotFather.
2. Update `/etc/finflow/finflow.env` (root-owned, mode 600).
3. `sudo systemctl start finflow-daily.service` and check the digest arrives.

Any queued decisions are still on the outbox and are delivered on that run —
nothing is lost by a rotation.

---

## When to stop trusting it

Drop a rung on the trust ladder (`PROJECT.md` §15) — immediately, without
debating it — if **any instruction was issued from bad data**: a stale snapshot,
a failed check that did not block, or a decision whose `snapshot_id` does not
match a green run. Deciding this in advance is the point; deciding it during a
good month is how people talk themselves out of it.
