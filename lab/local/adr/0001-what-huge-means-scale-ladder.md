# ADR 0001 · What "huge" means: a scale ladder, not one size

**Status:** accepted · 2026-10-28

## Context
ShopCo (the course's running example) sees ~2B clickstream events a month, ~67M a day. That is far beyond what one laptop can generate or process in a reasonable sitting, and far beyond what pandas can hold. "Huge" therefore has to mean different things for a laptop and for the cloud, while the *shape* of the data stays the same so every lesson (laziness, shuffles, skew, broadcast joins) shows up at every size.

## Decision
One scale ladder in `src/shopco/config.py`, selected with `--scale`:

| scale | days | events | customers | products | where | why |
|---|---|---|---|---|---|---|
| `test` | 2 | 100K | 5K | 500 | pytest | seconds; proves the code |
| `smoke` | 2 | 2M | 100K | 5K | laptop | first run, ~15 s end to end |
| `local` | 10 | **500M** | 5M | 50K | laptop | "huge for a laptop": ~6 GB Parquet, ~30 GB if pandas held it; minutes not hours |
| `cloud` | 30 | **2.01B** | 20M | 50K | Dataproc Serverless | the real ShopCo month; never run locally |

**Measured on this laptop (12 cores, 24 GB):** 2M events end to end in 12 s; 60M events (an earlier `local` definition) in 17 s with 707 MB of Parquet. That was too small to be "huge", so `local` was raised to 500M; its timing is in `local/README.md`.

Every rung has the same quirks: 0.5% NULL users, one bot key with 3% of all events, quadratic user skew, cubic product skew, a products table that is small in rows but over 10 MB in bytes. The feature/label cutoff is the first two thirds of the days.

## Consequences
- Local runs finish in minutes, so you can iterate; the cloud run is a demonstration you run a few times, not a dev loop.
- Numbers in reports differ by rung; compare *ratios* (skew, shuffle/input) across rungs, not absolutes.
- The laptop has 12 cores / 24 GB; `local` is sized so the whole pipeline fits without spilling much (see ADR 0006). A bigger laptop could add a rung; the config is the only place to change.

## Alternatives rejected
- One fixed size: either too small to show anything or too big to run locally.
- Scale by a single multiplier: days and per-day volume matter separately (the label window needs whole days).
