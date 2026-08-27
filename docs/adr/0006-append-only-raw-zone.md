# 6. The raw zone is append-only, and idempotency is convergent

Date: 2026-08-27

## Status

Accepted

## Context

Vendors restate history. Stooq applies split adjustments retroactively, so
re-downloading GLD's 2019 bars after a split gives different numbers than the
download taken last year. FRED restates seasonally adjusted series annually.

This breaks two assumptions that a data pipeline usually gets to make:

- **"Re-running ingest produces identical output" is false.** It cannot be an
  acceptance criterion, because the upstream data genuinely changed.
- **"A backtest keyed by data version" is meaningless** if the data underneath
  was overwritten in place. The version would name bytes that no longer exist.

A pipeline that overwrites on re-fetch silently absorbs both. The old number is
gone, nobody knows it changed, and a backtest run last year cannot be
reproduced — nor can anyone tell whether a difference in results came from a
code change or a data restatement.

## Decision

**Raw partitions are never rewritten.** Each ingestion run writes a new
partition keyed by the instant it ran:

```
raw/source=stooq/symbol=GLD/ingested=20260827T051200000000Z/data.parquet
raw/source=stooq/symbol=GLD/ingested=20261104T051100000000Z/data.parquet
```

Both are kept. The second is a *later opinion*, not a correction.

Three things follow:

- **Idempotency is convergent, not identical.** Ingesting twice leaves two
  partitions on disk and resolves to one row per `(symbol, date)` — the latest
  opinion. The test asserts that, rather than asserting byte equality, which
  would be asserting something false.
- **Enforced, not intended.** The `ObjectStore` port's `put` raises
  `ObjectAlreadyExists` rather than overwriting, and the port has **no delete
  method at all**. On-premise, that type-level refusal is what a delete-less
  bucket credential would otherwise provide (ADR 0005).
- **Reproducibility is keyed by a manifest.** Each run records which ingestion
  partitions it admitted, per `(source, symbol)`, and `snapshot_id` is a hash of
  that. An earlier draft used `max(ingested_at)`, which is wrong: backfilling
  GLD's 1990s history today would bump the id and falsely imply every other
  instrument had changed. A test pins that property.

The last loaded day is deliberately re-fetched on each incremental run. The raw
zone is append-only, so an overlapping row costs a few bytes, and a missed
restatement costs correctness.

## Consequences

**Corporate actions become observable rather than assumed.** When a re-fetch
disagrees with stored history, both values exist, so the difference can be
recorded in `dq_restatements` with both run ids and a large restatement can
raise an alert. A pipeline that overwrote would have nothing to compare.

**A backtest can be pinned to the data as it was.** The manifest names exact
partitions, so "reproduce the run from March" is a real operation rather than an
aspiration — and it is precisely what Delta time travel would replace if the
lakehouse extension were ever built.

**The raw zone only grows.** At roughly 25 MB a year this is not a constraint
worth engineering around, and the alternative — pruning — is the thing that
destroys the property. Pruning is therefore a deliberate manual act against the
filesystem, not something the pipeline can do.

**It is also the archive.** An instrument that delists while the project is
running keeps its history, which is the only structural defence available
against survivorship bias — a live vendor will simply stop serving it.

## Consequences accepted

Storage grows without bound, and there is no automatic cleanup. Resolving bronze
requires reading every partition for a symbol and taking the latest per
`(symbol, date)`, which is more work than reading one file. At this data size
both are irrelevant, and both were chosen over losing the ability to answer
"what did this look like before?".
