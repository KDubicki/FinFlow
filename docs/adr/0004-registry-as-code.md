# 4. The instrument registry lives in version control, not a database

Date: 2026-08-27

## Status

Accepted

## Context

The platform is instrument-agnostic by design: adding an ETF should be a
configuration change, not a code change (`PROJECT.md` §5). That leaves the
question of where the configuration lives.

A database table is the conventional answer. It allows adding an instrument at
runtime without a deploy, which is a real advantage for a system with operators
who are not the author.

This system has exactly one user, who is also its author, and the operations
that matter are not "add an instrument at 3am" but "explain why this backtest
saw nine sectors in 2010" and "reproduce the state the registry was in when this
run happened".

## Decision

Instruments, universes and macro series live in version-controlled YAML under
`instruments/`, loaded into an immutable `Registry` value object.

The registry's git SHA and commit date are resolved once at load time and
stamped into the object, so nothing downstream shells out to git in the middle
of a computation. The commit date — not the pipeline run date — is what
`dim_instrument` uses for SCD2 `valid_from`, so a backfill run in November does
not stamp an August change with November.

The `Registry` is constructed once at the composition root and injected. It is
explicitly not a module-level singleton imported from twenty places, which is
the shape it drifts into by default and which makes "evaluate against the
registry as it was in March" impossible.

Validation happens at load time and in CI on every pull request, offline.

## Consequences

Three properties follow directly, and each is the reason for the choice:

- **Reviewable.** Adding an instrument is a diff. The costs assumed for it, the
  sources declared for it and the universes it joins are all visible in one
  place, at the moment someone can still object.
- **Reproducible.** The registry state at any commit is recoverable, so a
  backtest can be pinned to the universe as it was defined rather than as it is
  now.
- **Auditable.** "When did we start tracking this, and why" is answerable from
  `git log`.

The cost is that adding an instrument requires a commit and a CI run rather than
an INSERT. For one user and roughly forty instruments this is not a constraint
worth engineering around, and the alternative loses all three properties above
to buy convenience nobody needs.

Resolution degrades rather than fails when git is absent — an unpacked tarball,
or a container built without `.git`. The registry still loads; it is simply not
reproducible, and `RegistryCommit.is_reproducible` reports that rather than
leaving a caller to guess from a null SHA.

If the registry ever does need runtime mutation, the `Registry` object is the
seam: a different loader behind the same value object, with the version-control
properties consciously traded away rather than lost by default.
