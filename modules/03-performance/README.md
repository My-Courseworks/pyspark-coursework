# M3 · Performance: shuffles, skew, joins

> **Module question:** Why is my job slow, and what do I check first?
> **Cross-cutting threads:** Failure modes · Tradeoff ledger
> **Domain spine:** ShopCo `events ⋈ products ⋈ customers` feature job

---

## Opening scene: 199 tasks done, 1 running

The Spark UI shows a stage with 200 tasks. 199 finished in 20 seconds, and one has been running for 25 minutes. It's a **left** join of events to customers. That one task holds every event where `user_id` is NULL (a left join keeps them, and they all hash to the same partition), plus ShopCo's internal load-test bot, which generated 300M events.

- **Why they all land on one task:**
  - A shuffle sends each row to partition `hash(key) % numPartitions` (the `hashpartitioning(user_id, 200)` in M1's "Reading the plan")
  - Same key → same hash → same partition, every time; that's how a join brings matching rows together
  - Every NULL `user_id` hashes to the same value, so all NULLs go to one partition
  - The bot is a single `user_id` with 300M rows, so it also goes to one partition (here, by bad luck, the same one)
  - NULL `user_id`s typically come from logged-out or anonymous sessions

Adding executors won't help, because one task can only use one core.

- A partition can't be split while its stage is running, so the stage finishes only when its slowest task does

> **Failure mode (the module in one line):** treating a slow job as "needs more cluster" instead of reading the plan and the stage/task timeline.

---

## 1. The debugging order (memorize this)

1. **Spark UI → Stages tab.** Which stage is slow? Is task duration *uniform* (too little parallelism, or too much data) or does it have a *long tail* (skew)?
   - Where the UI is: `http://localhost:4040` in local mode while the SparkSession is alive; on Databricks, EMR or Dataproc, use the cluster's "Spark UI" link
   - Click the slow stage and read "Summary Metrics": Min / 25th / Median / 75th / Max of Duration, Shuffle Read Size and Spill
   - Long tail: Max ≫ Median (25 min vs 20 s in the opening scene) → skew, see Skew
   - Uniform, and fewer tasks than total cores → too little parallelism, raise the partition count (see Partitioning knobs)
   - Uniform, with many tasks → too much data: read less first (see File formats), then use more partitions, and only then a bigger cluster
2. **Shuffle read/write sizes and "Spill (disk)".** Spill means partitions are too big for executor memory.
   - Spill: a task's sort or aggregation buffer outgrew its share of executor memory, so Spark wrote the overflow to local disk and read it back
   - Spill makes a task slow but doesn't kill it; the fatal version is an executor OOM (out of memory)
   - First fix: more, smaller partitions (raise `spark.sql.shuffle.partitions`), not more memory
3. **`df.explain("formatted")`.** Check the join strategy (`BroadcastHashJoin` vs `SortMergeJoin`), whether filters were pushed down, and the number of `Exchange` nodes.
   - `Exchange` = a shuffle (see M1 "Reading the plan"); `BroadcastExchange` is a broadcast, not a shuffle of the big side, so count it separately
   - `ReadSchema` on the scan node lists the columns actually read from storage
4. Then change code, and only last change cluster size.

## 2. Joins: pick the strategy

| Strategy | When | Shuffle? |
|---|---|---|
| **Broadcast hash join** | One side is small (fits in each executor's memory) | Only the small side is sent, to every executor; the big side never moves |
| **Sort-merge join** | Both sides are large | Both sides shuffled by key and sorted |
| Shuffle hash join | Medium side, opt-in or chosen by AQE | Both shuffled, no sort |

- **Hash join:** build a hash table (like a Python `dict`, key → rows) from one side, then stream the other side through it and look up each key
- **Broadcast hash join:** the driver collects the small table and sends a full copy to every executor; each task builds the dict once and streams its slice of the big side through it, so no big-side row crosses the network
- **Sort-merge join:** shuffle both sides so equal keys meet in the same partition (see Opening scene), sort each partition by key, then walk the two sorted lists in step; it scales to any size because sorting can spill to disk
- Shuffle hash join via AQE: only when `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` is set (default 0, which means off)
- **Which side can be broadcast:**
  - inner join: either side
  - left join: only the right side; right join: only the left side
  - full outer join: neither
  - a `F.broadcast()` hint on a side that can't be broadcast is silently ignored
  - so in `events` LEFT JOIN `customers`, only `customers` could be broadcast, and at 20M rows it's too big
- **"Small enough":** executors usually have a few GB to tens of GB of memory; Spark refuses broadcasts over 8 GB; in practice keep them to tens or low hundreds of MB

```python
products = spark.read.parquet("s3://shopco/products/")   # 50K rows → broadcast
enriched = events.join(F.broadcast(products), "product_id")
```

- Setup: `spark`, `F` and `events` come from M2's setup (see M2 "The core verbs")
- `customers` is loaded the same way: `customers = spark.read.parquet("s3://shopco/customers/")`
- No S3 access? Use the toy data in "Try it locally" under Skew
- Broadcast only the columns you need, since the copy goes to every executor: `F.broadcast(products.select("product_id", "category"))`

Spark auto-broadcasts tables whose *estimated* size is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), and AQE can switch to a broadcast at runtime once it sees real sizes. An explicit `F.broadcast()` is a hint you control.

- **AQE (Adaptive Query Execution):** Spark runs the plan stage by stage, and after each shuffle it looks at the *actual* data sizes and re-plans the rest (M1 introduces it for coalescing partitions)
- **"Estimated" size:** before anything runs, Spark guesses a table's size from file sizes and column types; after filters and joins the guess can be far off either way
- A runtime switch saves less than a planned broadcast: both sides have already been shuffle-written, so AQE saves only the sort and the network read

## 3. Skew

Skew means a few keys hold most of the rows, so one task does most of the work.

- **Find the hot keys** before choosing a fix:
  - `events.groupBy("user_id").count().orderBy(F.desc("count")).show(20)`
  - in the UI, the join stage's Max Shuffle Read Size ≫ Median
- **First, check for junk keys.** NULLs, `-1`, `"unknown"`, bots. Filter them or handle them separately. This is often the whole fix. (An inner join already drops NULL keys automatically; left/outer joins and `groupBy` don't.)
  - Drop them only if they're junk for the model: in a left join, dropping rows changes the output
  - To keep them, split: join the clean rows, and pass the junk rows through without joining

    ```python
    BOT_ID = 123456789                                     # the load-test bot's user_id
    is_junk = F.col("user_id").isNull() | (F.col("user_id") == BOT_ID)
    clean   = events.filter(~is_junk).join(customers, "user_id", "left")
    junk    = events.filter(is_junk)                       # never shuffled for the join
    joined  = clean.unionByName(junk, allowMissingColumns=True)   # customer columns are NULL for junk rows
    ```
  - `groupBy` skew hurts less: partial aggregation (see M1 "Reading the plan") shrinks a hot key to one row per task before the shuffle
  - Joins, windows and `collect_list` can't pre-aggregate, so they take the full hit
- **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled`, on by default with AQE) splits oversized partitions of a sort-merge join automatically.
  - A partition counts as skewed only if it's > 5 × the median partition **and** > 256 MB (`skewedPartitionFactor`, `skewedPartitionThresholdInBytes`)
  - It cuts the skewed partition into chunks and gives each chunk its own copy of the matching partition from the other side
  - When it fires, the SQL tab of the UI shows `SortMergeJoin(skew=true)`
  - Why the opening scene still had a 25-minute task, with AQE on by default:
    - AQE is off, or Spark is older than 3.2
    - the skewed stage isn't a shuffle join (a window or an aggregation)
    - splitting would force an extra shuffle downstream, so AQE skipped it
    - the skewed side is one the join type can't split (a left join can split only its left side)
- **Salting**, the manual fallback: spread a hot key across N sub-keys.
  - Why it works: the join key becomes `(user_id, salt)`, so the hot user's rows hash to N different partitions instead of one
  - Why the other side is copied N times: each salted event must still find its customer, so every salt value needs its own copy of that customer row

```python
N = 16
events_salted    = events.withColumn("salt", (F.rand() * N).cast("int"))      # skewed side: random salt per row
customers_salted = customers.withColumn(                                     # other side: copy each row N times, one per salt
    "salt", F.explode(F.array([F.lit(i) for i in range(N)])))
joined = events_salted.join(customers_salted, on=["user_id", "salt"], how="left").drop("salt")
```

Salting costs N× replication of the *non-skewed* side (here 20M customers become 320M rows), so you're trading data volume for parallelism. In practice, salt only the few hot keys.

- Hot-keys-only salting: other keys keep `salt = 0` and are not copied

  ```python
  HOT = [BOT_ID]                                          # from the hot-key check above
  is_hot = F.col("user_id").isin(HOT)
  events_salted = events.withColumn(
      "salt", F.when(is_hot, (F.rand() * N).cast("int")).otherwise(F.lit(0)))
  customers_salted = customers.withColumn(
      "salt", F.explode(F.when(is_hot, F.array([F.lit(i) for i in range(N)]))
                         .otherwise(F.array(F.lit(0)))))
  ```
- **Try it locally** (no S3, no cluster): build skewed toy data, run the join, then open `http://localhost:4040`

  ```python
  from pyspark.sql import SparkSession, functions as F

  spark = SparkSession.builder.master("local[4]").appName("m3-skew").getOrCreate()
  spark.conf.set("spark.sql.adaptive.enabled", "false")          # AQE off, so you see the raw skew
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # force a sort-merge join

  events = spark.range(5_000_000).select(                        # 60% of rows get a NULL user_id
      F.when(F.rand() < 0.6, F.lit(None).cast("long"))
       .otherwise((F.rand() * 100_000).cast("long")).alias("user_id"))
  customers = spark.range(100_000).withColumnRenamed("id", "user_id")

  events.join(customers, "user_id", "left").count()             # Stages tab: one task ≫ the rest
  ```
  - Re-run with the junk-key split and compare the stage's Max vs Median duration
  - To watch AQE's skew join fire on toy-sized data, turn AQE on and lower its size limits, which are tuned for real clusters:
    - `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` → `"1MB"`
    - `spark.sql.adaptive.advisoryPartitionSizeInBytes` → `"1MB"` (the chunk size it splits into)

## 4. Partitioning knobs

- **Two meanings of "partition"** (don't mix them up):
  - in-memory partition: a chunk of rows that one task processes (see M1 "Partitions"); `repartition`, `coalesce` and `shuffle.partitions` act on these
  - on-disk partition: one directory per column value (`dt=2026-10-01/`), created by `write.partitionBy`

| Call | Shuffle? | Use for |
|---|---|---|
| `repartition(n)` / `repartition(n, "col")` | Yes (full) | Increase parallelism, or co-locate by key before repeated joins |
| `coalesce(n)` | No | Reduce partitions cheaply, e.g. before writing fewer files |
| `spark.sql.shuffle.partitions` | n/a | Default 200; AQE coalesces at runtime, so set it high-ish and let AQE shrink it |

- `coalesce(n)` trap: with no shuffle, it also shrinks the work *before* it; `coalesce(1)` can make the whole preceding stage run as one task, so use `repartition(n)` when that work is heavy
- `repartition(n, "col")` for repeated joins pays off only if you `cache()` the result; otherwise each action redoes the shuffle
- Setting a config: `spark.conf.set("spark.sql.shuffle.partitions", 2000)` at runtime, or `.config(...)` on `SparkSession.builder`
- "High-ish": aim for roughly 100–200 MB per shuffle partition (e.g. 400 GB shuffled → 2,000–4,000 partitions)

**Write side:** `df.write.partitionBy("dt").parquet(...)` creates one directory per `dt` value. Readers filtering on `dt` then skip whole directories (**partition pruning**). Don't partition by a high-cardinality column like `user_id`, because you'll get millions of tiny files.

- Why tiny files hurt (the **small-files problem**): every file costs a storage request, a metadata entry and scheduling work, so listing and opening millions of 50 KB files takes longer than reading the data

## 5. Caching

```python
features.cache()          # lazy: materializes on the next action
features.count()          # materialize now
... reuse features in several models ...
features.unpersist()
```

Cache **only** what you reuse across multiple actions, and only after the expensive part. Caching the raw 2B-row table "just in case" evicts useful data and can be slower than re-reading Parquet. DataFrame `cache()` keeps data in memory and spills to disk when memory runs out. Use `persist(StorageLevel...)` to choose a different level.

- `StorageLevel` needs `from pyspark import StorageLevel`, e.g. `features.persist(StorageLevel.DISK_ONLY)`
- Check that the cache is used: the UI's Storage tab lists it with the fraction held in memory, and `explain()` shows `InMemoryTableScan`

## 6. File formats (the cheapest win)

Use **Parquet** (or Delta/Iceberg tables on top of Parquet) for its columnar layout:
- **Column pruning:** reading 3 of 40 columns reads about 3/40 of the bytes.
  - More exactly: in proportion to those columns' share of the file; a long text column costs more than an integer one
- **Predicate pushdown:** row-group min/max stats let Spark skip chunks.
  - Row group: a horizontal chunk of a Parquet file (often ~128 MB) that stores each column separately, with min/max stats per column
  - Skipping works only if the data is sorted or clustered on the filtered column; if values are spread randomly, every row group's min/max range overlaps the filter and nothing is skipped
- CSV/JSON are row-based, so every query reads every byte. Without an explicit schema, schema inference adds an extra pass over the data.

> **Tradeoff (the ledger entry):** a broadcast join costs driver and executor memory (the small table is copied everywhere, and an over-large broadcast can OOM). Accepted because it removes the shuffle of the large side, which is usually the single biggest cost in the job.

---

## Worked example: fixing the feature job

| Symptom | Diagnosis | Fix |
|---|---|---|
| 4 `Exchange` nodes, `SortMergeJoin` with `products` | `products` is only 50K rows but wide (long text columns), so its estimated size exceeds the 10 MB auto-broadcast threshold | Select only the needed columns, then `F.broadcast(products)` |
| 1 task ≫ others in the `customers` left join | `user_id IS NULL` + bot account | Filter NULLs and the bot ID before the join |
| 40K output files of ~50 KB | Each of the 200 post-shuffle tasks writes one file into *every* `dt` directory (200 tasks × 200 days) | `.repartition("dt")` before `.write.partitionBy("dt")`, so each day's rows end up in one task |
| Reads all 40 columns | A UDF is called on `F.struct("*")`, so every column counts as used | Pass the UDF only the columns it needs, or replace it with built-ins (M2) |

- Why 4 `Exchange` nodes: each sort-merge join shuffles both inputs (events + products by `product_id`, then that result + customers by `user_id`)
  - after the broadcast fix: 2 `Exchange` + 1 `BroadcastExchange`
- How you see "reads all 40 columns": the scan's `ReadSchema` in `explain("formatted")` lists every column
- `.repartition("dt")` trade-off: one task now writes a whole day
  - if a day is huge, that task becomes the new long tail
  - middle ground: repartition by `dt` plus a small salt (k values) → k files and k tasks per day

---

## Design exercise
> *Paper-based. Think, then write.*

**Task.** ShopCo's nightly feature job went from 20 min to 3 hours after Black Friday.
1. List the three things you check, in order, and what each would tell you.
2. For "one task runs 2.5 h", write two candidate fixes and the cost of each.
3. Write a short ADR: do you add AQE/salting to the code, or double the cluster? Justify it with cost.
   - ADR (Architecture Decision Record): a few lines of context, decision, alternatives considered, consequences
   - AQE is a config switch; salting is a code change
   - Cost hint: if one task is the bottleneck, doubling the cluster doubles the hourly bill without shortening that task

**Why this exercise matters.** "Tell me about a slow Spark job you fixed" is close to a guaranteed question. You need a diagnostic *order*, not a bag of tricks.

---

**Verification note:** Light mode. Steps 7–8 skipped at the user's request; config defaults stated from author knowledge of Spark 3.x. Re-check against your cluster's Spark version before quoting numbers.

**Next module:** [M4 — ML at scale](../04-ml-at-scale/README.md): training on what you just built.
