# ShopCo run `20261027T191737Z-local-pipeline`

| | |
|---|---|
| status | `ok` |
| mode | `local` |
| scale | `local` |
| stage | `pipeline` |
| spark_version | `3.5.3` |
| master | `local[*]` |
| default_parallelism | `12` |
| driver_memory | `12g` |
| shuffle_partitions | `48` |
| started_at | `2026-10-27T19:17:44+00:00` |
| finished_at | `2026-10-27T19:18:06+00:00` |
| total elapsed | **22.2s** |

## Steps

| # | step | status | elapsed | jobs | stages | tasks | input | output | shuffle read | shuffle write | spill (disk) | max/median task | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `load` | ✅ | 0.8s | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 1.0× | cutoff=2026-09-07 |
| 2 | `junk_key_audit` | ✅ | 2.2s | 2 | 3 | 121 | 2.3 GB | 0 | 11.1 KB | 11.1 KB | 0 | 3.2× | total_events=500,000,000, null_user_rows=2,497,593, bot_rows=14,996,725, dropped=0 |
| 3 | `purchases` | ✅ | 4.7s | 3 | 4 | 109 | 3.6 GB | 0 | 2.1 KB | 2.1 KB | 0 | **27.2×** ⚠ | rows=14,919,927 |
| 4 | `agg_features` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 5 | `window_features` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 6 | `category_breadth` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 7 | `enrich_customers` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 8 | `write_features` | ✅ | 9.6s | 10 | 17 | 340 | 1.4 GB | 67.7 MB | 751.6 MB | 751.6 MB | 0 | 2.2× | rows=2,992,967 |
| 9 | `labels` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 10 | `write_training` | ✅ | 3.3s | 7 | 11 | 140 | 1.1 GB | 68.3 MB | 189.4 MB | 189.4 MB | 0 | 2.4× | rows=2,992,967 |
| 11 | `summary` | ✅ | 0.3s | 3 | 6 | 45 | 18.5 MB | 0 | 17.6 KB | 17.6 KB | 0 | 1.3× | rows=2,992,967, churn_rate=0.3499, avg_orders=4.985, avg_spend=1341, n_countries=13 |

**Totals:** 11 steps · 28 jobs · 758 tasks · input 8.4 GB · shuffle written 941.0 MB

## Slowest stages

| step | stage | tasks | executor time | shuffle read | shuffle write | spill (disk) | max/median task | name |
|---|---|---|---|---|---|---|---|---|
| `write_features` | 22 | 13 | 55.2s | 623.2 MB | 0 | 0 | 1.0× | parquet at NativeMethodAccessorImpl.java:0 |
| `purchases` | 6 | 36 | 51.9s | 0 | 0 | 0 | 1.1× | count at NativeMethodAccessorImpl.java:0 |
| `junk_key_audit` | 3 | 60 | 17.8s | 0 | 11.1 KB | 0 | 3.2× | collect at /Users/aprabhak/Documents/my_github/pyspark-shopco-lab/src/shopco/pip |
| `write_features` | 12 | 36 | 16.8s | 0 | 361.2 MB | 0 | 2.2× | parquet at NativeMethodAccessorImpl.java:0 |
| `write_training` | 29 | 24 | 15.0s | 0 | 37.6 MB | 0 | 1.3× | parquet at NativeMethodAccessorImpl.java:0 |
| `write_features` | 14 | 36 | 11.6s | 0 | 128.3 MB | 0 | 2.1× | parquet at NativeMethodAccessorImpl.java:0 |
| `write_training` | 33 | 14 | 10.0s | 189.4 MB | 0 | 0 | 1.1× | parquet at NativeMethodAccessorImpl.java:0 |
| `write_features` | 16 | 14 | 9.7s | 128.3 MB | 33.9 MB | 0 | 1.0× | parquet at NativeMethodAccessorImpl.java:0 |

## How to read this

- **jobs = 0 (lazy):** the step only built a plan; its work ran inside a later step's action (course M1).
- **tasks:** one per partition per stage; `executor time` sums all task run time, so it exceeds wall time when tasks run in parallel.
- **shuffle write/read:** bytes moved between stages; the main cost in Spark (M1, M3).
- **spill (disk):** a partition did not fit in memory; slow but not fatal (M3).
- **max/median task ≥ 5× ⚠:** one task much slower than the rest = skew (M3). Look for junk keys first.
- Re-render: `python -m shopco report --runs-root <runs> --run-id <id>`; replay in the History Server from `<runs>/spark-events` (local).
