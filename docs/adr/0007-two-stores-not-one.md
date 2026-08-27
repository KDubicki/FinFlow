# 7. Two stores, not one

Date: 2026-08-28

## Status

Accepted

## Context

An earlier revision put `alerts_outbox`, `pipeline_runs` and the watermarks in
the same DuckDB file as the marts, while also claiming "the warehouse is derived
and never backed up; it is rebuilt from raw."

Those two statements cannot both be true. You cannot rebuild "which alerts were
already sent" from price history. Either the warehouse is disposable and the
outbox is not in it, or the outbox is in it and the warehouse needs backing up
like anything else — at which point "derived, disposable" stops being a design
property and becomes a slogan.

There is a second, unrelated problem with one store. The workloads are opposite:

- The marts are a bulk-analytical workload: a few large writes, many columnar
  reads, one writer.
- The outbox and watermarks are a transactional workload: many tiny writes,
  claim-and-mark semantics, and **more than one writer** — the pipeline run and
  the alert worker both write.

DuckDB is very good at the first and structurally wrong for the second: it
allows exactly one writer, cross-process.

## Decision

Two stores with different guarantees.

| | `warehouse.duckdb` | `ops.sqlite` |
|---|---|---|
| Holds | dims, facts, features, backtests, dq results | watermarks, `pipeline_runs`, outbox, positions, controls |
| Authority | derived; the raw zone is the truth | **authoritative — nothing else knows this** |
| Rebuildable | yes, from raw, in seconds | no |
| Backup | none; the rebuild is the test | nightly, to separate hardware, restore-tested |
| Writers | exactly one | two, concurrently |

SQLite in WAL mode for the operational side is the whole point: it is a
row-store with real concurrent-writer handling, which is precisely the workload
DuckDB's single-writer model cannot serve.

## Consequences

**"The warehouse is disposable" becomes testable**, and is tested: deleting it
and rebuilding from raw reproduces the bronze layer. That assertion is only
meaningful because there is nothing irreplaceable in the file.

**The backup policy stops being a judgement call.** Exactly one file needs
backing up, it is under a megabyte, and its restore path is exercised by a test
rather than assumed. An untested restore is a hope with a cron schedule.

**Single-writer enforcement gets a real home.** `DuckDBWarehouse` refuses a
second writer with a message naming what to close. Worth being precise about
what that covers: DuckDB's lock is *cross-process*, which is the collision that
actually happens — a UI or a shell holding the file when the scheduled run
starts. Two connections inside one process share an instance and neither is
refused, so the in-process case is covered by the `flock` in the CLI entrypoint
instead. Both are needed; neither alone is enough.

**The Stage 4 migration gets a clean line.** The analytical store could become
Delta and the operational store Postgres, independently, because they are
already separate things with separate ports.

**The cost** is two connection objects, two backup stories to explain instead of
one, and a join across stores being impossible. None of that has bitten yet, and
the alternative was a contradiction at the centre of the design.
