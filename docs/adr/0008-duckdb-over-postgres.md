# 8. DuckDB for the analytical store

Date: 2026-08-28

## Status

Accepted

## Context

The analytical store holds roughly 300,000 fact rows, all history, growing by
about 40 rows a day (`PROJECT.md` §2). It is read by dbt during a build, by the
backtest engine, and eventually by a read-only API. It is written by exactly one
process.

Postgres is the default answer for "we need a database", and it would work. So
would SQLite. The question is what each costs at this size.

## Decision

DuckDB, file-backed, with a snapshot promoted for serving.

The reasoning is the row count, not a preference:

- **Columnar and vectorized**, which is what an analytical scan wants. Postgres
  is row-oriented and would need indexes and tuning to match it on the queries
  this system actually runs.
- **Zero operational surface.** No daemon, no port, no user management, no
  connection pool, no backup story — because the file is derived and rebuilt
  from raw in seconds. On a machine the user maintains themselves (ADR 0005),
  every service that does not exist is one that cannot break at 05:30.
- **Reads Parquet natively**, so the raw zone is queryable without a load step
  when debugging.
- **dbt-duckdb is a first-class adapter**, so the models are ordinary dbt.

What DuckDB is *not* good at is concurrent writers, and that is precisely why
the operational store is SQLite instead (ADR 0007). Choosing DuckDB here is only
defensible because the workload it is wrong for was moved somewhere else.

## Consequences

**One writer, ever.** Enforced in the adapter and by an `flock`, and the serving
snapshot exists so that readers never contend with the pipeline. This is a real
constraint that shapes the process table in `PROJECT.md` §4.5, not an
implementation detail.

**The rebuild is the backup.** A monthly job deletes the warehouse and rebuilds
it from raw, which both proves the property and removes the need for a backup.

**Scaling out would mean changing this.** At a hundred times the data, or with
several writers, Postgres or a lakehouse becomes the right answer — and the
`Warehouse` port is the seam that makes that an adapter swap rather than a
rewrite. `PROJECT.md` §12 already treats it as a testable hypothesis rather than
a promise.

**Dialect neutrality is enforced, not hoped for.** DuckDB-specific SQL is
confined to staging and macros; a CI check greps the marts for a deny-list. It
caught a `QUALIFY` in `fct_macro_daily` the first time it ran, which is the
entire argument for having it.
