# ShopCo run `shopco-cloud-all-20261027t192144`

| | |
|---|---|
| status | `ok` |
| mode | `cloud` |
| scale | `cloud` |
| stage | `all` |
| spark_version | `3.5.3` |
| master | `spark://gdpic-srvls-batch-c411b307-746b-438c-8459-e6332e1de177-m:7077` |
| default_parallelism | `2` |
| driver_memory | `9600m` |
| shuffle_partitions | `1024` |
| started_at | `2026-10-27T19:23:22+00:00` |
| finished_at | `2026-10-27T19:41:06+00:00` |
| total elapsed | **17.8m** |

## Steps

| # | step | status | elapsed | jobs | stages | tasks | input | output | shuffle read | shuffle write | spill (disk) | max/median task | notes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | `gen_customers` | ✅ | 30.3s | 1 | 1 | 40 | 0 | 117.4 MB | 0 | 0 | 0 | 3.8× | rows=20,000,000 |
| 2 | `gen_products` | ✅ | 2.9s | 1 | 1 | 1 | 0 | 472.1 KB | 0 | 0 | 0 | 1.0× | rows=50,000 |
| 3 | `gen_events_2026-09-01` | ✅ | 60.2s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | 3.7× | rows=67,000,000, day=2026-09-01 |
| 4 | `gen_events_2026-09-02` | ✅ | 50.5s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.8× | rows=67,000,000, day=2026-09-02 |
| 5 | `gen_events_2026-09-03` | ✅ | 44.6s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.1× | rows=67,000,000, day=2026-09-03 |
| 6 | `gen_events_2026-09-04` | ✅ | 51.3s | 1 | 1 | 256 | 0 | 873.4 MB | 0 | 0 | 0 | **6.8×** ⚠ | rows=67,000,000, day=2026-09-04 |
| 7 | `gen_events_2026-09-05` | ✅ | 27.0s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | **6.1×** ⚠ | rows=67,000,000, day=2026-09-05 |
| 8 | `gen_events_2026-09-06` | ✅ | 16.8s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | **5.2×** ⚠ | rows=67,000,000, day=2026-09-06 |
| 9 | `gen_events_2026-09-07` | ✅ | 15.7s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.2× | rows=67,000,000, day=2026-09-07 |
| 10 | `gen_events_2026-09-08` | ✅ | 15.7s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | 1.7× | rows=67,000,000, day=2026-09-08 |
| 11 | `gen_events_2026-09-09` | ✅ | 18.4s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | **5.1×** ⚠ | rows=67,000,000, day=2026-09-09 |
| 12 | `gen_events_2026-09-10` | ✅ | 17.0s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | **5.9×** ⚠ | rows=67,000,000, day=2026-09-10 |
| 13 | `gen_events_2026-09-11` | ✅ | 14.7s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.4× | rows=67,000,000, day=2026-09-11 |
| 14 | `gen_events_2026-09-12` | ✅ | 15.6s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 3.0× | rows=67,000,000, day=2026-09-12 |
| 15 | `gen_events_2026-09-13` | ✅ | 16.2s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.7× | rows=67,000,000, day=2026-09-13 |
| 16 | `gen_events_2026-09-14` | ✅ | 17.6s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 4.5× | rows=67,000,000, day=2026-09-14 |
| 17 | `gen_events_2026-09-15` | ✅ | 16.7s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.9× | rows=67,000,000, day=2026-09-15 |
| 18 | `gen_events_2026-09-16` | ✅ | 17.2s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | **5.6×** ⚠ | rows=67,000,000, day=2026-09-16 |
| 19 | `gen_events_2026-09-17` | ✅ | 19.4s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | **5.2×** ⚠ | rows=67,000,000, day=2026-09-17 |
| 20 | `gen_events_2026-09-18` | ✅ | 22.1s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | **5.4×** ⚠ | rows=67,000,000, day=2026-09-18 |
| 21 | `gen_events_2026-09-19` | ✅ | 19.7s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | **5.6×** ⚠ | rows=67,000,000, day=2026-09-19 |
| 22 | `gen_events_2026-09-20` | ✅ | 15.8s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | **6.0×** ⚠ | rows=67,000,000, day=2026-09-20 |
| 23 | `gen_events_2026-09-21` | ✅ | 16.0s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 3.0× | rows=67,000,000, day=2026-09-21 |
| 24 | `gen_events_2026-09-22` | ✅ | 16.9s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.9× | rows=67,000,000, day=2026-09-22 |
| 25 | `gen_events_2026-09-23` | ✅ | 15.1s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 4.9× | rows=67,000,000, day=2026-09-23 |
| 26 | `gen_events_2026-09-24` | ✅ | 15.4s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | 4.3× | rows=67,000,000, day=2026-09-24 |
| 27 | `gen_events_2026-09-25` | ✅ | 15.8s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 2.6× | rows=67,000,000, day=2026-09-25 |
| 28 | `gen_events_2026-09-26` | ✅ | 15.1s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 3.4× | rows=67,000,000, day=2026-09-26 |
| 29 | `gen_events_2026-09-27` | ✅ | 15.3s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 3.5× | rows=67,000,000, day=2026-09-27 |
| 30 | `gen_events_2026-09-28` | ✅ | 21.5s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | **6.0×** ⚠ | rows=67,000,000, day=2026-09-28 |
| 31 | `gen_events_2026-09-29` | ✅ | 16.4s | 1 | 1 | 256 | 0 | 872.9 MB | 0 | 0 | 0 | **5.6×** ⚠ | rows=67,000,000, day=2026-09-29 |
| 32 | `gen_events_2026-09-30` | ✅ | 16.6s | 1 | 1 | 256 | 0 | 873.0 MB | 0 | 0 | 0 | 3.7× | rows=67,000,000, day=2026-09-30 |
| 33 | `load` | ✅ | 4.0s | 3 | 3 | 3 | 0 | 0 | 0 | 0 | 0 | 1.0× | cutoff=2026-09-21 |
| 34 | `junk_key_audit` | ✅ | 99.7s | 2 | 3 | 905 | 9.9 GB | 0 | 83.9 KB | 83.9 KB | 0 | 2.3× | total_events=2,010,000,000, null_user_rows=10,038,197, bot_rows=60,249,004, dropped=1 |
| 35 | `purchases` | ✅ | 95.5s | 3 | 4 | 907 | 19.0 GB | 0 | 17.4 KB | 17.4 KB | 0 | **25.2×** ⚠ | rows=64,616,102 |
| 36 | `agg_features` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 37 | `window_features` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 38 | `category_breadth` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 39 | `enrich_customers` (lazy) | ✅ | 0.0s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 40 | `write_features` | ✅ | 64.7s | 11 | 17 | 2,310 | 5.9 GB | 105.7 MB | 3.2 GB | 3.2 GB | 0 | **5.5×** ⚠ | rows=4,930,320 |
| 41 | `labels` (lazy) | ✅ | 0.1s | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | - |  |
| 42 | `write_training` | ✅ | 51.5s | 7 | 10 | 457 | 3.6 GB | 105.7 MB | 183.2 MB | 183.2 MB | 0 | 4.0× | rows=4,930,320 |
| 43 | `summary` | ✅ | 1.2s | 3 | 6 | 90 | 32.6 MB | 0 | 36.2 KB | 36.2 KB | 0 | 1.4× | rows=4,930,320, churn_rate=0.4189, avg_orders=13.11, avg_spend=3525, n_countries=13 |
| 44 | `sink_bigquery` | ✅ | 36.5s | 2 | 2 | 57 | 105.5 MB | 0 | 0 | 0 | 0 | 1.2× | training_table=shopco.training_set, ledger_table=shopco.run_ledger, ledger_rows=43 |

**Totals:** 44 steps · 63 jobs · 12,450 tasks · input 38.5 GB · shuffle written 3.3 GB

## Slowest stages

| step | stage | tasks | executor time | shuffle read | shuffle write | spill (disk) | max/median task | name |
|---|---|---|---|---|---|---|---|---|
| `junk_key_audit` | 35 | 452 | 44.5m | 0 | 83.9 KB | 0 | 2.3× | collect at /var/dataproc/tmp/srvls-batch-c411b307-746b-438c-8459-e6332e1de177/sh |
| `purchases` | 38 | 302 | 39.4m | 0 | 0 | 0 | 1.8× | count at NativeMethodAccessorImpl.java:0 |
| `write_training` | 61 | 151 | 16.8m | 0 | 157.8 MB | 0 | 1.9× | parquet at <unknown>:0 |
| `write_features` | 54 | 43 | 13.0m | 2.5 GB | 0 | 0 | 1.2× | parquet at <unknown>:0 |
| `gen_events_2026-09-05` | 6 | 256 | 8.1m | 0 | 0 | 0 | 6.1× | parquet at NativeMethodAccessorImpl.java:0 |
| `gen_events_2026-09-01` | 2 | 256 | 7.4m | 0 | 0 | 0 | 3.7× | parquet at NativeMethodAccessorImpl.java:0 |
| `sink_bigquery` | 75 | 29 | 7.1m | 0 | 0 | 0 | 1.1× | collect at BigQueryDataSourceWriterInsertableRelation.java:89 |
| `gen_events_2026-09-06` | 7 | 256 | 6.8m | 0 | 0 | 0 | 5.2× | parquet at NativeMethodAccessorImpl.java:0 |

## How to read this

- **jobs = 0 (lazy):** the step only built a plan; its work ran inside a later step's action (course M1).
- **tasks:** one per partition per stage; `executor time` sums all task run time, so it exceeds wall time when tasks run in parallel.
- **shuffle write/read:** bytes moved between stages; the main cost in Spark (M1, M3).
- **spill (disk):** a partition did not fit in memory; slow but not fatal (M3).
- **max/median task ≥ 5× ⚠:** one task much slower than the rest = skew (M3). Look for junk keys first.
- Re-render: `python -m shopco report --runs-root <runs> --run-id <id>`; replay in the History Server from `<runs>/spark-events` (local).
