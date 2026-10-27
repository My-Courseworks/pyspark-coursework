# ADR 0007 · Labels: one global cutoff, label window = the tail of the dataset

**Status:** accepted · 2026-10-28

## Context
Churn labels need features computed strictly before a cutoff and the label from strictly after it (course M2). ShopCo's real definition is "no purchase in the next 30 days". The local rungs only have 2–3 days of data.

## Decision
- `cutoff = START_DATE + floor(days × 2/3)`, clamped so both sides have at least one day (`config.Scale.cutoff_date`).
- Features use `dt < cutoff` **and** are filtered on the partition column `dt` (not only on `ts`), so Spark prunes whole day directories.
- `churned = 1` if the customer has no purchase in `[cutoff, end)`.
- The training set is one snapshot per run and carries a `cutoff` column; a multi-snapshot (as-of join) design is noted in the course's M5 and deliberately not built here.

## Consequences
- At `cloud` scale the label window is 10 days, not 30; the mechanism is identical and the horizon is one constant.
- Churn rates at small rungs are high (most customers do not buy on one specific day); that is expected, not a bug.

## Alternatives rejected
- Per-user rolling cutoffs: realistic but multiplies rows by snapshots; out of scope for a lab whose point is the Spark mechanics.
