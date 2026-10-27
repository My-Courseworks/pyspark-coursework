# ShopCo run `20261027T191629Z-local-all`

| | |
|---|---|
| status | `ok` |
| mode | `local` |
| scale | `local` |
| stage | `all` |
| spark_version | `3.5.3` |
| master | `local[*]` |
| default_parallelism | `12` |
| driver_memory | `12g` |
| shuffle_partitions | `48` |
| started_at | `2026-10-27T19:16:36+00:00` |
| finished_at | `2026-10-27T19:17:36+00:00` |
| total elapsed | **60.7s** |

## Steps

| # | step | status | elapsed | jobs | stages | tasks | input | output | shuffle read | shuffle write | spill (disk) | max/median task | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `gen_customers` | ✅ | 2.0s | 1 | 1 | 10 | 0 | 29.4 MB | 0 | 0 | 0 | 1.0× | rows=5,000,000 |
| 2 | `gen_products` | ✅ | 0.2s | 1 | 1 | 1 | 0 | 476.2 KB | 0 | 0 | 0 | 1.0× | rows=50,000 |
| 3 | `gen_events_2026-09-01` | ✅ | 3.9s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.5× | rows=50,000,000, day=2026-09-01 |
| 4 | `gen_events_2026-09-02` | ✅ | 3.6s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.2× | rows=50,000,000, day=2026-09-02 |
| 5 | `gen_events_2026-09-03` | ✅ | 3.7s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.2× | rows=50,000,000, day=2026-09-03 |
| 6 | `gen_events_2026-09-04` | ✅ | 3.8s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.2× | rows=50,000,000, day=2026-09-04 |
| 7 | `gen_events_2026-09-05` | ✅ | 4.0s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.1× | rows=50,000,000, day=2026-09-05 |
| 8 | `gen_events_2026-09-06` | ✅ | 3.7s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.3× | rows=50,000,000, day=2026-09-06 |
| 9 | `gen_events_2026-09-07` | ✅ | 3.8s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.2× | rows=50,000,000, day=2026-09-07 |
| 10 | `gen_events_2026-09-08` | ✅ | 3.7s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.2× | rows=50,000,000, day=2026-09-08 |
| 11 | `gen_events_2026-09-09` | ✅ | 4.4s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.2× | rows=50,000,000, day=2026-09-09 |
| 12 | `gen_events_2026-09-10` | ✅ | 4.0s | 1 | 1 | 48 | 0 | 534.2 MB | 0 | 0 | 0 | 1.1× | rows=50,000,000, day=2026-09-10 |
| 13 | `load` | ✅ | 0.2s | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 1.0× | cutoff=2026-09-07 |
| 14 | `junk_key_audit` | ✅ | 1.9s | 2 | 3 | 121 | 2.3 GB | 0 | 11.1 KB | 11.1 KB | 0 | 1.7× | total_events=500,000,000, null_user_rows=2,497,593, bot_rows=14,996,725, dropped=1 |
| 15 | `purchases` | ✅ | 4.9s | 3 | 4 | 109 | 3.6 GB | 0 | 2.1 KB | 2.1 KB | 0 | **12.1×** ⚠ | rows=14,470,542 |
| 16 | `agg_features` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 17 | `window_features` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 18 | `category_breadth` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 19 | `enrich_customers` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 20 | `write_features` | ✅ | 8.1s | 10 | 17 | 345 | 1.3 GB | 67.7 MB | 747.9 MB | 747.9 MB | 0 | 2.0× | rows=2,992,966 |
| 21 | `labels` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 22 | `write_training` | ✅ | 3.3s | 7 | 11 | 148 | 1.1 GB | 68.3 MB | 189.4 MB | 189.4 MB | 0 | 1.4× | rows=2,992,966 |
| 23 | `summary` | ✅ | 0.2s | 3 | 6 | 45 | 18.5 MB | 0 | 17.5 KB | 17.5 KB | 0 | 1.0× | rows=2,992,966, churn_rate=0.3499, avg_orders=4.835, avg_spend=1301, n_countries=13 |

**Totals:** 23 steps · 40 jobs · 1,262 tasks · input 8.4 GB · shuffle written 937.2 MB

## Slowest stages

| step | stage | tasks | executor time | shuffle read | shuffle write | spill (disk) | max/median task | name |
|---|---|---|---|---|---|---|---|---|
| `purchases` | 18 | 36 | 53.8s | 0 | 0 | 0 | 1.3× | count at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-09` | 10 | 48 | 49.8s | 0 | 0 | 0 | 1.2× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-10` | 11 | 48 | 45.0s | 0 | 0 | 0 | 1.1× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-05` | 6 | 48 | 44.5s | 0 | 0 | 0 | 1.1× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-01` | 2 | 48 | 42.7s | 0 | 0 | 0 | 1.5× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-04` | 5 | 48 | 42.0s | 0 | 0 | 0 | 1.2× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-07` | 8 | 48 | 41.7s | 0 | 0 | 0 | 1.2× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-06` | 7 | 48 | 41.7s | 0 | 0 | 0 | 1.3× | parquet at NativeMethodAccessorImpl.java:0 |

## How to read this

- **jobs = 0 (lazy):** the step only built a plan; its work ran inside a later step's action (course M1).
- **tasks:** one per partition per stage; `executor time` sums all task run time, so it exceeds wall time when tasks run in parallel.
- **shuffle write/read:** bytes moved between stages; the main cost in Spark (M1, M3).
- **spill (disk):** a partition did not fit in memory; slow but not fatal (M3).
- **max/median task ≥ 5× ⚠:** one task much slower than the rest = skew (M3). Look for junk keys first.
- Re-render: `python -m shopco report --runs-root <runs> --run-id <id>`; replay in the History Server from `<runs>/spark-events` (local).
