# 10. Decisions are content-addressed, and they live in the ops store

Date: 2026-09-06

## Status

Accepted

## Context

M4 delivers a decision to a phone once a day, unattended, from a single machine
that may be rebooted mid-run. Two questions had to be settled before any of that
could be written.

**Where does a decision live?** `PROJECT.md` §9.4 says decisions are written to
`fct_decisions` and referenced by `decision_id` from every outbox row. But §4.3
is equally clear that the warehouse is *derived and disposable* — deleted and
rebuilt from the raw zone whenever it is convenient, and a test asserts exactly
that. A decision is not derivable from the raw zone: it depends on the strategy
version that was running at the time and on the controls the user had set. A
rebuild would silently erase the record of what the system told its user.

**What identifies a decision?** The obvious key is the run that produced it. It
is also wrong in both directions:

- Re-running the pipeline after a crash produces a *new* run id, so the same
  instruction is delivered a second time.
- Deduplicating on `(strategy_id, as_of)` instead has the opposite defect: the
  mid-morning retry that finally gets the late vendor's bar computes a
  *different* portfolio and is suppressed as a duplicate. That is a silent miss,
  which is the worst failure this system has.

## Decision

**Decisions are authoritative in the operational store**, in a `decisions` table
alongside `alerts_outbox`, `positions_target` and the controls. The warehouse
gets a projection when the backtest engine needs one in M7 — as a *copy* of the
authoritative record, not the record itself.

**A decision's identity is its content.** `decision_id` is a SHA-256 over the
scope, strategy id, strategy version, `as_of`, `data_as_of`, the target
positions and what was withheld — truncated to 16 hex characters. The snapshot
id is deliberately excluded: it changes on every build, so including it would
make every run a new decision and defeat deduplication entirely.

The outbox is unique on `(strategy_id, strategy_version, decision_id)` and rows
are inserted with an ignore-on-conflict.

## Consequences

**Exactly-once delivery needs no coordination.** Re-running the pipeline over
unchanged data recomputes the same id, the insert is ignored, and nothing is
sent twice. There is no lock, no distributed anything, and no state held between
runs beyond a SQLite row.

**A genuinely changed instruction is always delivered.** New data, an edited
strategy, or a newly applied mute all change the content, so they get a new id
and a new row. The retry-after-a-late-vendor case works by construction rather
than by special case.

**A rebuild of the warehouse cannot lose the record.** Which also means the ops
store is now carrying the only copy of something a user might act on, and its
backup and restore path stops being a nicety. `finflow-backup --check` is the
monthly drill.

**One duplicate window remains open, and is documented rather than hidden.** If
the process dies between Telegram accepting a message and the store recording
that it did, the next run resends it. Telegram exposes no idempotency key, so
this cannot be closed — only made small, by marking immediately after the call
returns. One repeated message is the correct side of that trade against one
silently lost instruction.

**The portfolio scope uses a reserved id rather than NULL.** `strategy_id =
'portfolio'` for the netted account-level decision of §7.6, because two NULLs
are not equal in SQL and a nullable column in that key would silently stop
deduplicating exactly the decision that matters most.

**Weights are rounded to six places before hashing.** Otherwise a Polars upgrade
that changes the last bit of a division would produce a "new" decision and an
unnecessary message.
