# 11. A message only when the instruction changes

Date: 2026-09-06

## Status

Accepted

## Context

The named risk that ends this product is not a wrong number. It is the digest
becoming noise and getting muted — after which the system still runs, still
decides, and no longer has a user (`PROJECT.md` §1.2, and the risk register's
one "quietest failure" entry).

The evaluator produces a decision every day. If every decision were delivered,
a strategy holding GLD through a six-week trend would send forty-two identical
"target: GLD 100%" messages. Every one of them is *true*, and their combined
effect is to train the reader to swipe the notification away — including on the
morning the target changes.

The opposite failure is just as real: suppressing a message because it looks
similar to yesterday's is how a rotation goes undelivered.

## Decision

**Every decision is recorded. A decision message is queued only when the
instruction changes** — when the strategy version or the set of target weights
differs from the last decision that was enqueued for that strategy.

**The digest is unconditional and carries the standing state.** It goes out
every morning whether or not anything happened, reports the current target
against actual holdings, and says "No action." when there is nothing to do.

**Drift inside the rebalance band is not an instruction.** Five percentage
points by default (`FINFLOW_REBALANCE_BAND_PP`), replaced in M5 by a band
derived from the per-instrument spreads already in the registry.

**Opening and closing are never suppressed by the band.** A target the user does
not hold at all, and a holding the strategy has exited, are always instructions.

## Consequences

**"No action" is the normal message**, which is what makes an action line worth
reading. The ≥95% no-action target is measurable from `alerts_outbox`, and a
milestone that pushes it below that has not shipped.

**The unchanged case is still visible**, in the digest's `Held` section and in
its `Withheld` line ("target unchanged since the last message"). Nothing is
suppressed silently — the reader can always see that the system decided, and
what it decided.

**A change is delivered immediately**, because the comparison is against the
last *enqueued* decision rather than a time window. There is no debouncing, no
cooldown, and no way for a genuine rotation to be smoothed away.

**The comparison is on weights, not on the decision id.** The id also moves when
the underlying bar date moves, which is every day. Comparing ids would have
produced exactly the daily message this decision exists to prevent — and it was
the first implementation, which is why it is written down here.

**A second consumer will need something else.** A UI showing "what does the
system think today" reads `decisions`, not the outbox: the outbox is a delivery
queue, and its emptiness means "nothing new to say", never "nothing to show".
