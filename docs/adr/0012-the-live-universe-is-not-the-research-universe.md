# 12. The live universe is not the research universe

Date: 2026-09-06

## Status

Accepted

## Context

The registry widened from 8 instruments to 42 in M5, and the widening exposed a
problem the smaller universe had hidden: **an EU retail account cannot buy most
of them.**

Under PRIIPs, a fund sold to EU retail investors must publish a Key Information
Document. US-domiciled ETFs do not, so a Polish brokerage account refuses the
order for SPY, GLD, TLT and every one of the eleven SPDR sectors — however
liquid they are, and regardless of what any backtest says about them.

That is not a footnote. A system whose daily message says "target: SPY 50%, TLT
50%" to a user who cannot buy either is not a research tool with a caveat; it is
a system whose output is unexecutable, which is the same thing as having no
output. The gap between "what the data supports" and "what can be bought" had to
become a first-class distinction rather than something the user rediscovers at
the broker every time.

The naive fix — filter the universe down to what is purchasable — was rejected.
The US lines have a decade or more of extra history, and throwing that away
makes every backtest worse in exchange for making it executable. The research
value and the execution constraint are genuinely different concerns.

## Decision

**Both universes exist, and they are labelled.**

- `ucits: true` marks a line an EU retail account can actually buy. It is an
  explicit registry field, not inferred from the symbol or the exchange, because
  the cost of getting it wrong is a target portfolio that cannot be executed.
- `ucits_equivalent` names the line to buy *instead of* a US original. It is
  validated: it must name a registered instrument that is itself `ucits: true`.
  A mapping to a fund the system does not ingest is advice nobody can check.
- `tradeable_eu` on an instrument is its own `ucits` flag — **not** "has an
  equivalent". Having a UCITS cousin does not make SPY purchasable.
- The `tradeable_eu` universe holds the purchasable lines and nothing else.
- A decision over a research universe carries the substitution **on the target
  position**, so the message reads `SPY 50% -> buy CSPX.UK` and is actionable
  without a lookup.
- `docs/RESULTS.md` reports what each mapping was measured to be worth, and
  `scripts/verify_ucits_mapping.py` produces the measurement.

## Consequences

**The mapping is a claim with evidence behind it, or it is absent.** Nine of the
forty-two instruments carry one. The sectors, the miners and the commodity funds
do not, because no honest mapping exists — and an unverified mapping is worse
than an absent one, since the digest would confidently name a fund nobody has
checked.

**The live universe has less history than the research universe**, often by a
decade. DTLA.UK starts in 2015 where TLT starts in 2002. Any result that is
going to be traded has to be re-stated over the live universe's window, and that
window is now a fact in the registry rather than a discovery in month three.

**Accumulating share classes change the return basis.** CSPX.UK, EIMI.UK and
DTLA.UK reinvest distributions internally, so their price series *is* total
return, while their US counterparts' is not (ADR 0009). Comparing them directly
manufactures a tracking difference roughly equal to the dividend yield. This is
recorded in the registry, in the verification script's output, and in
`RESULTS.md` — three places, because it is the mistake most likely to be made
twice.

**The previous derivation was backwards and shipped.** Before this decision,
`dim_instrument.tradeable_eu` was computed as `ucits_equivalent is not None`,
which marked SPY tradeable and CSPX.UK not. Nothing consumed the column yet, so
nothing broke — but it is a good illustration of why a field whose meaning is
"can I buy this" should be stated by the thing that knows, rather than inferred
from a neighbouring column.

**A `tags`-based broker-availability filter is the natural next step** and is
already on the backlog. It stays there: this decision covers the regulatory
constraint, which is the same for every EU broker, and not the per-broker
differences, which are not.
