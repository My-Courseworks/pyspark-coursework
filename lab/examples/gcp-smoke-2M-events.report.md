# ShopCo run `shopco-smoke-all-20261027t191745`

| | |
|---|---|
| status | `ok` |
| mode | `cloud` |
| scale | `smoke` |
| stage | `all` |
| spark_version | `3.5.3` |
| master | `spark://gdpic-srvls-batch-16f30d0c-082d-4b90-9fb3-60f16ede0c8b-m:7077` |
| default_parallelism | `2` |
| driver_memory | `9600m` |
| shuffle_partitions | `64` |
| started_at | `2026-10-27T19:19:14+00:00` |
| finished_at | `2026-10-27T19:20:25+00:00` |
| total elapsed | **72.5s** |

## Steps

| # | step | status | elapsed | jobs | stages | tasks | input | output | shuffle read | shuffle write | spill (disk) | max/median task | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `gen_customers` | ✅ | 12.0s | 1 | 1 | 2 | 0 | 611.4 KB | 0 | 0 | 0 | 1.0× | rows=100,000 |
| 2 | `gen_products` | ✅ | 2.0s | 1 | 1 | 1 | 0 | 54.4 KB | 0 | 0 | 0 | 1.0× | rows=5,000 |
| 3 | `gen_events_2026-09-01` | ✅ | 4.2s | 1 | 1 | 8 | 0 | 12.6 MB | 0 | 0 | 0 | 1.1× | rows=1,000,000, day=2026-09-01 |
| 4 | `gen_events_2026-09-02` | ✅ | 3.2s | 1 | 1 | 8 | 0 | 12.6 MB | 0 | 0 | 0 | 1.0× | rows=1,000,000, day=2026-09-02 |
| 5 | `load` | ✅ | 1.9s | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 1.0× | cutoff=2026-09-02 |
| 6 | `junk_key_audit` | ✅ | 4.2s | 2 | 3 | 17 | 8.2 MB | 0 | 1.5 KB | 1.5 KB | 0 | 1.2× | total_events=2,000,000, null_user_rows=9,891, bot_rows=59,728, dropped=1 |
| 7 | `purchases` | ✅ | 2.0s | 3 | 4 | 25 | 14.0 MB | 0 | 472.0 B | 472.0 B | 0 | 1.1× | rows=48,082 |
| 8 | `agg_features` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 9 | `window_features` (lazy) | ✅ | 0.2s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 10 | `category_breadth` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 11 | `enrich_customers` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 12 | `write_features` | ✅ | 9.0s | 12 | 18 | 68 | 4.7 MB | 584.3 KB | 2.9 MB | 2.9 MB | 0 | 1.1× | rows=34,163 |
| 13 | `labels` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 14 | `write_training` | ✅ | 4.3s | 7 | 9 | 23 | 5.0 MB | 589.4 KB | 392.0 KB | 392.0 KB | 0 | 1.0× | rows=34,163 |
| 15 | `summary` | ✅ | 1.1s | 3 | 6 | 6 | 101.1 KB | 0 | 1.3 KB | 1.3 KB | 0 | 1.0× | rows=34,163, churn_rate=0.5962, avg_orders=1.407, avg_spend=403.7, n_countries=13 |
| 16 | `sink_bigquery` | ✅ | 18.7s | 2 | 2 | 9 | 588.3 KB | 0 | 0 | 0 | 0 | 1.0× | training_table=shopco.training_set, ledger_table=shopco.run_ledger, ledger_rows=15 |

**Totals:** 16 steps · 36 jobs · 170 tasks · input 32.6 MB · shuffle written 3.3 MB

## Slowest stages

| step | stage | tasks | executor time | shuffle read | shuffle write | spill (disk) | max/median task | name |
|---|---|---|---|---|---|---|---|---|
| `sink_bigquery` | 48 | 8 | 32.8s | 0 | 0 | 0 | 1.0× | collect at BigQueryDataSourceWriterInsertableRelation.java:89 |
| `gen_events_2026-09-01` | 2 | 8 | 21.2s | 0 | 0 | 0 | 1.1× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-02` | 3 | 8 | 14.9s | 0 | 0 | 0 | 1.0× | parquet at NativeMethodAccessorImpl.java:0 |
| `junk_key_audit` | 7 | 8 | 13.9s | 0 | 1.5 KB | 0 | 1.2× | collect at /var/dataproc/tmp/srvls-batch-16f30d0c-082d-4b90-9fb3-60f16ede0c8b/sh |
| `write_features` | 14 | 8 | 12.9s | 0 | 1.5 MB | 0 | 1.0× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_customers` | 0 | 2 | 9.6s | 0 | 0 | 0 | 1.0× | parquet at NativeMethodAccessorImpl.java:0 |
| `purchases` | 10 | 8 | 8.1s | 0 | 0 | 0 | 1.0× | count at NativeMethodAccessorImpl.java:0 |
| `sink_bigquery` | 47 | 1 | 6.0s | 0 | 0 | 0 | 1.0× | collect at BigQueryDataSourceWriterInsertableRelation.java:89 |

## How to read this

- **jobs = 0 (lazy):** the step only built a plan; its work ran inside a later step's action (course M1).
- **tasks:** one per partition per stage; `executor time` sums all task run time, so it exceeds wall time when tasks run in parallel.
- **shuffle write/read:** bytes moved between stages; the main cost in Spark (M1, M3).
- **spill (disk):** a partition did not fit in memory; slow but not fatal (M3).
- **max/median task ≥ 5× ⚠:** one task much slower than the rest = skew (M3). Look for junk keys first.
- Re-render: `python -m shopco report --runs-root <runs> --run-id <id>`; replay in the History Server from `<runs>/spark-events` (local).
