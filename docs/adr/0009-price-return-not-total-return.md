# 9. The MVP is price return, and says so

Date: 2026-08-28

## Status

Accepted

## Context

Stooq's `.us` series are split-adjusted but not dividend-adjusted, and no free
source provides clean ETF distribution history. So the returns this system
computes are *price* returns, and they understate total return by the
distribution yield.

That matters very unevenly:

| Instrument | Approx. yield | Effect on a 10-year backtest |
|---|---|---|
| GLD, SLV, SGOL | 0% | none — price return is correct |
| SPY, QQQ | ~1.2% | modest drag on long-only results |
| TLT, IEF, LQD | ~3–4% | material |
| **HYG** | **~6–7%** | **a long-only price-return backtest is simply wrong** |

There are three options. Use price return and say so. Reconstruct total return
from an unreliable distributions feed. Or quietly report price return as if it
were total return.

## Decision

**The MVP is explicitly price return.** `return_basis: price` on every
instrument, validated at load time — the registry *rejects* `total` until a
distributions source exists, so the field cannot drift out of line with reality.
The caveat is stated on every backtest report and in the UI, and
`docs/RESULTS.md` reports the bias per universe.

Reconstructing total return from a feed we do not trust was rejected as worse
than the honest gap: a wrong number that looks authoritative is more dangerous
than a right number with a stated limitation, because nobody checks the former.

## Consequences

**`rates_credit` strategies ship as relative or long/short forms**, where the
carry largely cancels between legs and the bias mostly drops out. A long-only
HYG strategy is not shipped, because its backtest would be wrong by more than
any edge it could plausibly show.

**`return_basis: total` exists in the schema from day one**, so adding a
distributions source is additive — a new source, a registry flag, and the marts
already carry the column. It is a validation rule that blocks it today, not a
migration.

**Benchmark comparisons stay honest** as long as both sides use the same basis.
Comparing a price-return strategy to a total-return benchmark would manufacture
underperformance just as surely as the reverse manufactures alpha.

**This is a limitation, not a feature**, and the documentation says so in those
words. The failure mode being avoided is the one where a reader assumes the more
flattering interpretation because nothing told them not to.
