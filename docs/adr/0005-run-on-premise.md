# 5. Run on-premise, on hardware the user owns

Date: 2026-08-27

## Status

Accepted. Supersedes the hosting arrangement described in earlier drafts of
`PROJECT.md` §11.1.

## Context

The design originally ran the daily pipeline on a GitHub Actions scheduled
workflow until M6, then on a rented VPS. Because an Actions runner is ephemeral,
that arrangement required somewhere durable to put the raw zone, which meant an
S3-compatible bucket — Cloudflare R2 — reached through the `ObjectStore` port,
plus a scoped credential, bucket versioning, a lifecycle policy, and code to
push and pull `ops.sqlite` around every run.

That is a considerable amount of machinery, and all of it exists to solve a
problem created by the choice of host rather than by the problem domain. The
system manages one person's savings, holds about 25 MB a year, and needs to run
once a day.

## Decision

The daily path runs on a machine the user owns, from M4 onward. Data and compute
are on-premise:

- The raw zone, `warehouse.duckdb` and `ops.sqlite` are files in one directory on
  local disk. `LocalObjectStore` is the only object-store implementation;
  `S3ObjectStore`, boto3 and moto are removed.
- The schedule is a `systemd` timer with `Persistent=true`, so a run missed while
  the machine was off fires on the next boot.
- GitHub Actions keeps running CI, which is not the daily path.
- The machine accepts no inbound connections. Every published port binds to
  `127.0.0.1`.

**Vendors are still called over the internet.** "On-premise" here means the
system's own data and computation stay on the user's hardware, not that it is
air-gapped. Fetching prices is the one thing that inherently cannot be local.

The `ObjectStore` port stays. It is what keeps the ingestion service ignorant of
where bytes go, and removing an implementation is not a reason to remove an
abstraction that has two remaining ones and a conformance suite.

## Consequences

**What gets simpler.** The state-synchronisation problem disappears entirely: no
bucket credentials in the daily path, no pushing and pulling `ops.sqlite`, no
migration from one host to another at M6 — the host stops changing, and only
gains services. Three of the four secrets the design previously required are
gone, and the two that remain (vendor keys, a bot token) cannot destroy
anything.

**What gets better.** The attack surface is smaller: nothing needs to reach the
machine, so nothing is exposed. Running from a residential IP also helps with the
data sources, several of which treat cloud egress ranges with suspicion — Stooq
currently serves a proof-of-work interstitial to this network, and a datacentre
address would not have improved that.

**What gets worse, and this is the real cost.** Availability becomes the user's
problem. A cloud runner is somebody else's job to keep alive; a box under a desk
is not. Two consequences follow and both are load-bearing:

- The dead-man's switch (§11.2) matters more than it would have, and it must run
  *off* the box. A monitor on the machine it is monitoring cannot report that the
  machine is down.
- Durability is now a single disk unless something is done about it. The raw zone
  is the one unrecoverable asset in the system, and a bucket with versioning gave
  it protection that a directory does not. So: a nightly `rsync --link-dest`
  mirror to separate physical hardware, and the `ObjectStore` port exposing no
  delete method at all, which is the on-premise equivalent of a delete-less
  credential and is enforced by the type system rather than by a policy document.

**Being honest about the trade.** A versioned bucket with a delete-less token is
genuinely stronger protection for the raw zone than a mirrored local disk. This
decision accepts weaker durability in exchange for a much smaller system, no
recurring cost, no vendor account, and a smaller exposed surface. That is a
reasonable trade for a solo project holding 25 MB a year — but it is a trade, and
the mirror to a second device is not optional dressing on it. It is the part that
makes the trade acceptable.

## Revisiting

If the raw zone ever holds history that would be expensive or impossible to
re-fetch — a delisted instrument, or a vendor that stops serving deep history —
the calculation changes, because at that point the data is genuinely
irreplaceable rather than merely inconvenient to rebuild. Adding a remote store
back is one adapter behind an unchanged port, which is precisely why the port
survived this decision.
