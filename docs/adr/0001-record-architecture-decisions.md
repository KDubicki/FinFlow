# 1. Record architecture decisions

Date: 2026-08-26

## Status

Accepted

## Context

This project makes a number of choices that are not self-evident from the code:
the storage engine, the orchestrator, whether the instrument registry lives in
version control or a database, how models are validated. Six months later the
reasoning behind any one of them is lost, and the temptation is to re-litigate
a decision whose original constraints are no longer visible.

## Decision

Every architecturally significant decision is recorded as a short markdown file
in `docs/adr/`, numbered sequentially, using the Context / Decision /
Consequences structure.

A decision is architecturally significant if reversing it would require changing
more than one module, or if a reasonable engineer would ask "why was this done
this way?".

Records are immutable once accepted. A decision that changes gets a new record
that supersedes the old one; the original stays, marked as superseded.

## Consequences

- The reasoning behind the design survives independently of whoever wrote it.
- Code review has a place to point when a change contradicts a prior decision.
- A small ongoing cost: each significant decision needs a few paragraphs written
  at the time it is made, when the context is still fresh.
