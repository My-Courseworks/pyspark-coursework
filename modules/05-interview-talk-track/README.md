# M5 · Interview talk-track

> **Module question:** What do I actually say?
> **Cross-cutting threads:** Failure modes · Tradeoff ledger
> **Domain spine:** ShopCo churn: your "story" project

---

## Opening scene: "So, how much Spark have you used?"

A weak answer lists APIs. A senior answer gives a mental model, one trade-off you made, and one time something broke and how you found it. That takes about 90 seconds and invites follow-ups you're ready for.

> **Failure mode (the module in one line):** answering with syntax instead of judgment.

---

## 1. The 90-second pitch (adapt the numbers to your real project)

> "I use PySpark mainly as the data-prep layer for ML. On a churn project we had about 2 billion events a month. I built the feature pipeline in the DataFrame API, mostly aggregations and window functions, with **point-in-time cutoffs** so features never saw data from after the cutoff. The biggest performance issue was **skew**: in a left join, null user IDs and a bot account put most of the rows on one task. Filtering those keys and letting **AQE** handle the rest took it from hours to about 20 minutes. For training, the aggregated feature table **fit on one node**, so we trained XGBoost single-node and only used Spark again for **batch scoring with a pandas UDF**. I'd only reach for MLlib or distributed XGBoost if the training data really didn't fit."

Every phrase in **bold** is a hook the interviewer can pull, and M1–M4 has the answer behind each one.

**Follow-up ladders (expect one "how?" per hook):**
- **Point-in-time cutoffs**
  - "How exactly?" → features use only events before the cutoff; labels use only `[cutoff, cutoff + 30 days)` (M2)
  - "What if you need many snapshot dates?" → as-of join (see Terms the table uses)
- **Skew**
  - "How did you find it?" → Spark UI Stages tab: 199 tasks done in seconds, 1 still running, with a huge shuffle read (M3)
  - "How did you confirm the keys?" → `groupBy("user_id").count().orderBy(F.desc("count"))` put NULL and the bot ID on top
  - "Why not add executors?" → one task runs on one core; more machines don't split it (M3)
- **AQE**
  - "What is it?" → Adaptive Query Execution: Spark re-plans at runtime from real shuffle sizes (M1, M3)
    - merges small shuffle partitions
    - switches a join to broadcast when one side turns out small
    - splits oversized partitions in sort-merge joins (skew join)
  - "What didn't it fix?" → the NULL/bot keys; filtering them was the real fix
- **Fit on one node**
  - "How did you know?" → 20M rows × 50 features × 8 bytes ≈ 8 GB (M4)
  - "How did it get there?" → Spark wrote Parquet; a large training node read it directly, not via `toPandas()` (M4)
- **Batch scoring with a pandas UDF**
  - "Why not a plain UDF?" → Arrow batches, vectorized predict, model loaded once per task (M2, M4)
  - "Why not MLlib?" → only scoring needed to be parallel, not training

**If you have no production Spark experience:**
- Don't present ShopCo as a job you did: one detail question ("how many executors?") exposes it
- Run the M1–M4 snippets in local mode (`pip install pyspark`) on a generated dataset, so your numbers and Spark UI observations are real
- Frame it honestly: "In a practice project I simulated a ShopCo-style clickstream…"
- Quote only the scale you ran; then say what you'd expect to change at 2B rows
- For something you haven't hit: "I haven't seen that in production; I'd check the Stages tab, then `explain`, then change code before cluster size"

## 2. Likely questions → crisp answers

| Question | Answer (one or two lines) | Module |
|---|---|---|
| Transformations vs actions? | Transformations build a lazy plan; an action triggers at least one job (broadcasts and AQE can add more). Laziness lets Catalyst optimize the whole query. | M1 |
| Why is a notebook with many `count()`s slow? | Each action recomputes the lineage from S3. Remove the counts, or `cache()` after the expensive step. | M1 |
| How does Spark survive a node failure? | It recomputes the lost partitions from lineage (the plan). | M1 |
| Narrow vs wide? | Narrow stays within a partition; wide needs a shuffle, which marks a stage boundary. Shuffles are the main cost. | M1 |
| What's a shuffle and why is it expensive? | Redistributing rows by key across executors: serialization, disk writes, network. | M1 |
| How many partitions? | Reads split at ~128 MB; shuffles produce `spark.sql.shuffle.partitions` (default 200). Set it high-ish and let AQE coalesce. | M1, M3 |
| `repartition` vs `coalesce`? | `repartition` does a full shuffle, can increase or decrease partitions, can partition by key, and balances them. `coalesce` only merges partitions (decrease only), with no shuffle. `coalesce(1)` can squeeze the whole upstream stage into 1 task, so use `repartition(1)` after heavy work. | M3 (caveats: here) |
| How do you debug a slow job? | Stages tab: uniform task times (too little parallelism or too much data) vs a long tail (skew). Then shuffle sizes and spill. Then `explain("formatted")`: join strategy, pushed filters, `Exchange` count. Change code first, cluster size last. | M3 |
| What is AQE? | Runtime re-optimization from real shuffle sizes: merges small partitions, switches to broadcast, splits skewed sort-merge-join partitions. On by default since 3.2. | M1, M3 |
| How do you handle skew? | Detect: one task ≫ the rest in the Stages tab; confirm hot keys with `groupBy(key).count()`. Fix: filter junk keys (NULL, bots) first, then AQE skew join (sort-merge joins only, not `groupBy` or windows), then salt as a last resort. | M3 |
| Join strategies? | Broadcast hash: small side copied everywhere. Sort-merge: both sides shuffled and sorted. Shuffle hash: both shuffled, no sort. | M3 |
| Broadcast join? | Copy the small table to every executor so the large side isn't shuffled. Auto-chosen below 10 MB *estimated* size. Watch driver and executor memory. | M3 |
| When do you cache? | When a DataFrame is reused across several actions, after the expensive step. Unpersist afterwards. | M3 |
| `cache` vs `persist` vs `checkpoint`? | `cache` = `persist` at the default level (memory, spilling to disk). `persist(level)` picks another level. `checkpoint` writes to reliable storage and cuts the lineage. | M3 (`checkpoint`: not in M1–M4) |
| Executor OOM or spill? | Oversized or skewed partitions; spill shows in the Stages tab. Use more partitions, fix skew, or prune columns before adding memory. | M1, M3 |
| How do you size executors? | Name the knobs: `spark.executor.memory`, `spark.executor.cores`, `spark.driver.memory`, dynamic allocation. Tune from spill and GC time in the UI, not upfront. | Not in M1–M4 |
| UDFs? | Built-ins first; pandas UDF (Arrow, vectorized) if needed; plain Python UDF last. UDFs are black boxes to the optimizer: Catalyst can't simplify or rewrite what's inside them, and they get no code generation; plain UDFs also pay JVM↔Python transfer per row. | M2 |
| Window function pitfalls? | Shuffles and sorts by the `partitionBy` key. No `partitionBy` puts all data on one task. | M2 |
| Exact vs approximate distinct? | `countDistinct` is expensive on billions of rows; `approx_count_distinct` (HyperLogLog) trades a small error for speed. | M2 |
| Why Parquet? | Columnar layout: column pruning, predicate pushdown, compression, schema stored in the file. | M3 |
| Small-files problem? | Many tiny output files (e.g. 200 tasks × every `dt` directory) slow every later read. `repartition("dt")` before `write.partitionBy("dt")`; never partition by `user_id`. | M3 |
| RDD vs DataFrame vs Dataset? | RDD: low-level collection, Python lambdas, no optimizer. DataFrame: schema + Catalyst. Dataset: typed, Scala/Java only. | Not in M1–M4 |
| `groupByKey` vs `reduceByKey`? (RDD-era question) | `groupByKey` shuffles every value for a key, then groups (big shuffle, OOM on hot keys). `reduceByKey` combines within each partition first, then shuffles the partial results. The DataFrame `groupBy().agg()` does this partial aggregation automatically (two `HashAggregate`s in `explain`). | M1 (DataFrame side only) |
| Driver OOM? | Usually `collect`/`toPandas`/a big broadcast (the broadcast table is first collected on the driver). Aggregate or sample first, or write to storage and read it on a separate node. | M4 |
| MLlib vs sklearn? | MLlib or distributed XGBoost/LightGBM when one model's training data doesn't fit; otherwise sklearn/XGBoost single-node, or `applyInPandas` for many models. | M4 |
| Is Spark always the answer? | No. Under tens of GB, DuckDB or Polars is often faster and simpler. | M4 |
| Data leakage in features? | Features use only data before each cutoff; labels only after it. Many snapshot dates: as-of join. Versioned tables (Delta) for reproducibility, if retention keeps old versions. | M2, M4 (as-of join, retention: here) |
| Streaming? | Structured Streaming runs micro-batches on the DataFrame API. If you haven't used it, say so. | M1 (name only) |

**Terms the table uses:**
- **Whole-stage code generation (codegen):** Spark compiles a chain of operators into one JVM function, avoiding per-row overhead
  - `explain` marks those operators with `*(n)`
  - a UDF breaks the chain
- **Apache Arrow:** a columnar in-memory format shared by the JVM and pandas
  - pandas UDFs move column batches through Arrow instead of pickling row by row
  - pandas/NumPy then process each batch vectorized
- **RDD (Resilient Distributed Dataset):** Spark's original low-level API, a distributed collection of Python objects transformed with lambdas
  - Catalyst can't see inside the lambdas: no pushdown, no codegen
  - this course uses DataFrames; RDD questions are legacy but still asked
- **As-of join (many snapshot dates):**
  - build a spine of `(user_id, snapshot_date)` rows
  - join events on `user_id` where `ts < snapshot_date`
  - aggregate per `(user_id, snapshot_date)`
  - label from `[snapshot_date, snapshot_date + 30 days)`
  - cost: rows multiply by the number of snapshots, so pre-aggregate events per user per day first
- **Delta retention:** `VACUUM` deletes files older than the retention period (default 7 days)
  - time travel to those versions then fails
  - to reproduce a training set months later, raise retention or save a copy of the training table

## 3. Tradeoff ledger (quotable lines)

- **Lazy evaluation** costs debuggability; accepted for whole-plan optimization. (M1)
- **Python UDFs** cost optimizer visibility and serialization; accepted only when no built-in exists. (M2)
- **Broadcast join** costs driver memory (it's collected there first) and memory on every executor; accepted to avoid shuffling the big side. (M3)
- **Salting** costs N× replication of the non-skewed side; accepted to parallelize a hot key. (M3)
- **Single-node training** costs a size ceiling; accepted for ecosystem and iteration speed. (M4)

## 4. Words that signal seniority

*Catalyst, physical plan, `explain`, Exchange, AQE, skew, broadcast hash join vs sort-merge join, partition pruning, predicate pushdown, Arrow, pandas UDF, `applyInPandas`, point-in-time correctness, Delta time travel, small-files problem, "it fit on one node so I didn't distribute training."*

Use them in a sentence with a reason, never as a list.

- What the less obvious ones mean:
  - **Physical plan:** the operators Catalyst actually chose (join type, `Exchange`s), printed by `explain` (M1)
  - **Exchange:** the shuffle node in that plan (M1)
  - **Partition pruning:** skipping whole `dt=` directories on read (M1, M3)
  - **Predicate pushdown:** skipping Parquet row groups using min/max stats (M1, M3)
  - **Small-files problem:** too many tiny output files, slowing every later read (M3)
  - **Delta time travel:** reading a table as of an earlier version (M4)
- Model sentences:
  - "I checked the physical plan, saw a `SortMergeJoin` against a 50K-row table, and broadcast it."
  - "The filter showed under `PartitionFilters`, so partition pruning skipped every day outside the window."
  - "It fit on one node, so I didn't distribute training; Spark did features and scoring."

---

## Design exercise
> *Paper-based. Think, then write.*

**Task.** Write your own version of the 90-second pitch from a real project, or from ShopCo if you have none.
- If you use ShopCo, frame it as a practice project and run the snippets locally first (see If you have no production Spark experience)

1. Include exactly one mental-model claim, one performance story (symptom → diagnosis → fix → result), and one "when I *didn't* use Spark" decision.
2. For each bolded hook, write the follow-up question you'd expect and your two-line answer.
3. Say it out loud once and time it.

**Why this exercise matters.** In the interview you'll be judged on whether you can explain trade-offs clearly, not on API recall.

---

**Verification note:** Light mode. Steps 7–8 (literature survey, memory-less verifier) skipped at the user's request.

**Next module:** End of course. Back to the [syllabus](../../syllabus.md).
