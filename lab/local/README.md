# ShopCo on a laptop

Machine this was built on: Apple Silicon, 12 cores, 24 GB RAM, Java 17 from Homebrew, Python 3.12.

## Commands

| command | what |
|---|---|
| `local/scripts/setup.sh` | one-time: Java 17, `.venv`, package |
| `source local/scripts/env.sh` | per shell: `JAVA_HOME`, venv on `PATH` |
| `.venv/bin/pytest` | end-to-end test at 100K events |
| `local/scripts/run.sh <stage> <scale> [args]` | `stage` = generate / pipeline / all / report; `scale` = test / smoke / local |
| `local/scripts/history_server.sh` | Spark History Server over `runs/spark-events/` |

Useful `args`: `--keep-junk` (leave the bot key in, see skew), `--driver-memory 16g`, `--shuffle-partitions 96`, `--conf k=v`.

## Measured runs

| scale | events | wall time | Parquet on disk | notes |
|---|---|---|---|---|
| smoke | 2M | 12 s | 25 MB | first run after setup |
| 60M (old `local`) | 60M | 17 s | 707 MB | too small to count as huge; rung raised |
| local | 500M | **60.7 s** (generate 40 s + pipeline 20 s) | 5.3 GB events, 69 MB training set | 3.0M training rows, 14.5M purchases; peak shuffle 752 MB in `write_features` |
| local, `pipeline --keep-junk` | 500M | 22.2 s | same data | bot key kept: `write_features` skew 2.2× vs 1.4× without it |

Why the laptop is this fast: 12 cores read 5 GB of Parquet in parallel, the purchases subset (5%) is cached, and every join is either broadcast (`products`) or on already-aggregated per-user rows. The same pipeline on 2B rows needs the cluster because the shuffles grow with the data and 24 GB of RAM runs out.

On skew: the bot key is 3% of traffic, and a shuffle partition at this scale already holds ~300K rows, so one hot key adds ~750K rows to one task: a 2–3× long tail, below the report's 5× warning. The ⚠ you may see on the `purchases` step is the cache materialisation, where a few tasks read far more than the median. Raise `BOT_EVENT_FRACTION` in `config.py` to 0.2 to see a real long tail.

## What the report shows

- One row per step: jobs, stages, tasks, input/output bytes, shuffle read/write, spill, max/median task time.
- `(lazy)` steps ran 0 jobs: they only built a plan. Their work shows up in the next step that triggers an action (`write_features`, `write_training`).
- `max/median task ≥ 5× ⚠` is skew. Run `pipeline local --keep-junk` and compare `write_features` with the default run: the window over `user_id` is where the bot key lands in one task.
- Open `http://localhost:4040` during a run for the live Spark UI, or the History Server afterwards for the full DAG.

## Decisions

See [adr/](adr/).
