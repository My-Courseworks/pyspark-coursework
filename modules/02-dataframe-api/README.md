# M2 · DataFrame API for DS work

> **Module question:** How do I write feature engineering that Spark can optimize?
> **Cross-cutting threads:** Failure modes · Tradeoff ledger
> **Domain spine:** ShopCo churn features per customer

---

## Opening scene: the 40-minute lambda

You need "days since last purchase" per customer, so you write a Python UDF because it's quick. It's correct, and it takes 40 minutes. A teammate rewrites it with `F.datediff` and `F.max`, and it takes 90 seconds.

> **Failure mode (the module in one line):** reaching for Python UDFs and row-wise logic when a built-in column expression exists.

---

## 1. The core verbs

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("churn").getOrCreate()
events = spark.read.parquet("s3://shopco/events/")   # user_id, ts, event_type, product_id, price, dt
cutoff = F.lit("2026-10-01").cast("date")            # features use only data before this (see worked example)

purchases = (
    events
    .filter(F.col("event_type") == "purchase")
    .filter(F.col("dt") < "2026-10-01")               # partition column: skips whole day-directories
    .filter(F.to_date("ts") < cutoff)                 # exact guard on the event time
    .select("user_id", "ts", "product_id", "price")
    .withColumn("is_big", F.when(F.col("price") > 100, 1).otherwise(0))
)
```

**Running this without S3 (local mode)**
- Install with `pip install "pyspark[sql]"`: the `sql` extra pulls in pandas and PyArrow, which pandas UDFs need
- Spark runs on the JVM, so you also need a Java runtime (Java 17 works for Spark 3.3+ and 4.x)
- Replace the `spark.read.parquet(...)` line with a toy `events` table:

```python
from datetime import datetime
events = spark.createDataFrame(
    [("u1", datetime(2026, 9, 3, 10),  "purchase", "p1", 120.0, "2026-09-03"),
     ("u1", datetime(2026, 9, 20, 9),  "view",     "p2", None,  "2026-09-20"),
     ("u1", datetime(2026, 9, 25, 18), "purchase", "p2", 35.0,  "2026-09-25"),
     ("u2", datetime(2026, 8, 1, 12),  "purchase", "p1", 80.0,  "2026-08-01"),
     ("u2", datetime(2026, 10, 5, 12), "purchase", "p3", 15.0,  "2026-10-05")],
    "user_id string, ts timestamp, event_type string, product_id string, price double, dt string",
)
```

- Every snippet in this module is lazy (see [M1 · Lazy evaluation](../01-mental-model/README.md#2-lazy-evaluation-transformations-vs-actions)); nothing runs until an action
- To see results, call an action:
  - `purchases.show()`: prints the first 20 rows
  - `purchases.printSchema()`: column names and types (no job runs)
  - `purchases.limit(100).toPandas()`: a small pandas copy for inspection

**Schema assumed by this module**
- `ts`: `timestamp` (event time); `F.to_date("ts")` truncates it to a `date`
- `to_date` uses the session time zone (`spark.sql.session.timeZone`), so day boundaries depend on that setting
- `dt`: `string` like `"2026-09-03"`, the partition column (one S3 directory per day, as in [M1 · Reading the plan](../01-mental-model/README.md#worked-example-reading-the-plan))
- `price`: `double`; null on non-purchase events
- Type names such as `"date"`, `"double"`, `"int"` are Spark SQL type strings, used by `.cast(...)` and UDF return types

**Why filter on `dt` as well as `ts`**
- A filter on the bare partition column (`F.col("dt") < "..."`) becomes a `PartitionFilter`: whole days are never read
- A filter on a function of a column (`F.to_date("ts") < ...`) is not a partition filter, so on its own it scans all of history
- Keep the `ts` filter too: it is the exact cut, while `dt` does the cheap pruning

**Column expressions: the core idea of this module**
- In pandas, `df.x > 0` computes a boolean Series right away
- In Spark, `F.col("x") > 0` is an unevaluated **expression**: a formula that names a column and holds no data
- Think of a spreadsheet formula `=B2>0` before it is filled down
- `F.col("name")` refers to a column; `F.lit(value)` wraps a constant so it can sit inside an expression
- So `cutoff` above is a Column (a constant date expression), not a Python `date`
- `F.when`, `F.datediff`, `F.max` and the rest just build bigger expressions
- Catalyst can read, rewrite and compile these expressions; it cannot see inside a Python function (see UDF hierarchy)
- `functions as F` is the convention because `F.sum`/`F.max` would otherwise shadow Python's built-in `sum`/`max`
- Combine conditions with `&`, `|`, `~` and wrap each side in parentheses, exactly as in pandas (`and`/`or` raise an error)

| pandas habit | PySpark equivalent |
|---|---|
| `df[df.x > 0]` | `df.filter(F.col("x") > 0)` |
| `df["y"] = ...` | `df.withColumn("y", ...)` (returns a **new** DataFrame, since DataFrames are immutable) |
| `df.groupby("k").agg({...})` | `df.groupBy("k").agg(F.sum("x").alias("sx"), ...)` |
| `df.merge(o, on="k", how="left")` | `df.join(o, on="k", how="left")` |
| `np.where` | `F.when(cond, a).otherwise(b)` |
| `df.apply(f, axis=1)` | Usually wrong. Find the built-in in `pyspark.sql.functions` |
| `df.head()` | `df.show(5)` (prints) or `df.limit(5).toPandas()` |
| `df[["a", "b"]]` | `df.select("a", "b")` |
| `df.rename(columns={"a": "b"})` | `df.withColumnRenamed("a", "b")`, or `.alias("b")` on an expression |
| `df.drop(columns=["a"])` | `df.drop("a")` |
| `df.drop_duplicates()` / `df["a"].unique()` | `df.dropDuplicates()` / `df.select("a").distinct()` |
| `df.sort_values("a")` | `df.orderBy("a")` (a full shuffle + sort) |
| `df["a"].isna()` / `df.fillna(0)` | `F.col("a").isNull()` / `df.fillna(0)` |

SQL is equally valid: `df.createOrReplaceTempView("events")` then `spark.sql("SELECT ...")`. Both compile to the same Catalyst plan, so choose based on readability.
- Example: `spark.sql("SELECT user_id, COUNT(*) AS n FROM events GROUP BY user_id")` returns a DataFrame

## 2. Aggregations: the feature-table workhorse

```python
features = (
    purchases
    .groupBy("user_id")
    .agg(
        F.count("*").alias("n_orders"),
        F.sum("price").alias("total_spend"),
        F.avg("price").alias("avg_order"),
        F.countDistinct("product_id").alias("n_products"),
        F.datediff(cutoff, F.max(F.to_date("ts"))).alias("days_since_last"),
    )
)
```

- `days_since_last` is the opening scene's feature, written with built-ins (`F.max` + `F.datediff`) and no UDF
- `.alias("name")` names the output column, like `as name` in SQL
- A function of an aggregate (`F.datediff(cutoff, F.max(...))`) is allowed inside `.agg`

**Nulls (pandas `NaN` → Spark `null`)**
- Missing values are `null`; comparisons with `null` give `null`, and `filter` drops those rows
- `F.count("*")` counts rows; `F.count("col")` counts non-null values of `col`
- `sum`/`avg`/`max` skip nulls; a group with only nulls gets `null`, not 0
- After a left join, unmatched rows have `null` features: fill them explicitly (`fillna(0)` or `F.coalesce(col, F.lit(0))`)

On billions of rows, consider `F.approx_count_distinct` (HyperLogLog-based) instead of `countDistinct`. Exact distinct counts are expensive.
- Why exact is expensive: Spark must keep every distinct value per group, which means an extra shuffle on `(user_id, product_id)` before the final count
- **HyperLogLog (HLL):** a fixed-size sketch that estimates the number of distinct values from hashes, without storing the values
- Accuracy is set by `rsd` (relative standard deviation), default 0.05: `F.approx_count_distinct("product_id", rsd=0.05)`
- It pays off when groups have many distinct values (distinct users per product, distinct sessions per day, a global count)
- For a per-user count of a few dozen products, each HLL sketch can be larger than the exact set, so measure before switching

## 3. Window functions: sequence features without a self-join

**What a window function is**
- It computes a value for **each row** from a group of related rows, and keeps every row (unlike `groupBy`, which collapses each group to one row)
- pandas equivalents: `df.groupby("user_id")["ts"].shift()` → `F.lag`, `.cumcount() + 1` → `F.row_number`, `.rolling("30D").sum()` → a range frame
- The alternative without windows is a self-join (joining purchases to themselves on `user_id` to find "the previous one"), which multiplies rows and shuffles twice
- `Window.partitionBy("user_id")`: the group (like `groupby` keys)
- `.orderBy("ts")`: the order within the group (needed for `lag`, `row_number`, running sums)
- `F.something(...).over(w)`: applies the function over that window

**Three meanings of "partition" in this course**
- Data partition: a chunk of rows that one task processes ([M1 · Partitions](../01-mental-model/README.md#4-partitions))
- Window `partitionBy`: the logical group a window function looks at (this section)
- Write `partitionBy`: one output directory per column value ([M3 · Partitioning knobs](../03-performance/README.md#4-partitioning-knobs))

```python
w = Window.partitionBy("user_id").orderBy("ts")

gaps = (
    purchases
    .withColumn("prev_ts", F.lag("ts").over(w))
    .withColumn("gap_days", F.datediff(F.to_date("ts"), F.to_date("prev_ts")))
    .withColumn("order_rank", F.row_number().over(w))
)

# Rolling 30-day spend per user (range frame on epoch seconds)
w30 = (Window.partitionBy("user_id")
       .orderBy(F.col("ts").cast("long"))
       .rangeBetween(-30 * 86400, 0))
rolling = purchases.withColumn("spend_30d", F.sum("price").over(w30))
```

**Reading the code**
- `F.lag("ts")` is `null` on each user's first purchase, so `gap_days` is `null` there too
- `F.row_number()` breaks ties arbitrarily; if two purchases share a `ts`, add a tiebreaker column to `orderBy`
- `rank`/`dense_rank` give tied rows the same number instead
- `ts.cast("long")` turns a timestamp into epoch seconds, so `-30 * 86400` means "30 days back"

**Frames: which rows the window sums over**
- `rowsBetween(-2, 0)`: by row count (this row and the 2 before it)
- `rangeBetween(-30 * 86400, 0)`: by value of the `orderBy` column (every row whose time is within 30 days)
- With `orderBy` and no explicit frame, the default is "start of group to current row", so `F.sum("price").over(w)` is a running total

**From per-event windows to one row per user**
- `gaps` and `rolling` still have one row per purchase; a feature table needs one row per user
- Aggregate the window output, then join it onto `features`:

```python
gap_feats = gaps.groupBy("user_id").agg(F.avg("gap_days").alias("avg_gap_days"))
features = features.join(gap_feats, on="user_id", how="left")
```

- The rolling `spend_30d` is "spend in the 30 days before each purchase", not "spend in the 30 days before the cutoff"
- For the snapshot feature a conditional aggregate is simpler and needs no window: `F.sum(F.when(F.to_date("ts") >= F.date_sub(cutoff, 30), F.col("price")).otherwise(0))` inside the `.agg` of the Aggregations section
- Rolling windows earn their place when you need the value at many points in time (one training row per user per date)

Windows shuffle by `partitionBy` key, and they sort within each partition. A window with **no** `partitionBy` puts all the data on one task, which is a classic silent bottleneck (Spark logs a warning).

## 4. UDF hierarchy: fastest to slowest

| Option | How it runs | Use when |
|---|---|---|
| Built-in `F.*` / SQL expression | In the JVM, optimized by Catalyst, code-generated | Always try first |
| **pandas UDF** (`@F.pandas_udf`) | Batches moved via **Apache Arrow**, vectorized in pandas/NumPy | Custom math with no built-in |
| Plain Python UDF (`@F.udf`) | Rows serialized to a Python worker and processed one at a time | Last resort |

**Terms in the table**
- **Code-generated** (whole-stage codegen): Spark compiles a chain of operators into one Java function; `explain()` marks these with `*(n)`
- **Python worker:** a separate Python process that Spark starts on each executor; data must be serialized out of the JVM, sent to it, and the results sent back
- **Apache Arrow:** a columnar in-memory format; it ships whole column batches to Python cheaply, and pandas wraps them as Series without row-by-row conversion

**The opening scene, as a plain UDF (the slow version)**

```python
from datetime import date

@F.udf("int")                                   # declare the return type; it defaults to string
def days_since(last_ts):                        # called once per row, inside a Python worker
    return (date(2026, 10, 1) - last_ts.date()).days

slow = (purchases.groupBy("user_id").agg(F.max("ts").alias("last_ts"))
                 .withColumn("days_since_last", days_since("last_ts")))
```

- Compare with `days_since_last` in the Aggregations section: same answer, no Python
- `slow.explain()` shows a `BatchEvalPython` node (the Python round trip); a pandas UDF shows `ArrowEvalPython`; the built-in version shows neither
- If the function returns a value that doesn't match the declared type (e.g. a float for `"int"`), Spark silently returns `null` instead of raising an error

**Reading the pandas UDF below**
- The type hints `pd.Series -> pd.Series` tell Spark this is a Series-to-Series UDF: one column batch in, one out, same length
- `"double"` is the return type, as a Spark SQL type string
- It needs PyArrow installed (included in `pyspark[sql]`)

```python
import pandas as pd

@F.pandas_udf("double")
def log1p_spend(s: pd.Series) -> pd.Series:
    import numpy as np
    return np.log1p(s)

features = features.withColumn("log_spend", log1p_spend("total_spend"))
```

(This one is actually `F.log1p`, which proves the point: check the built-ins first.)

> **Tradeoff (the ledger entry):** a Python UDF is a black box to the optimizer: Catalyst can't simplify or rewrite what's inside it, and it gets no code generation. It also adds the cost of moving data between the JVM and Python. Accepted only when no built-in exists, and then as a pandas UDF.

---

## Worked example: point-in-time correct training set

Churn labels need features computed **only from data before the cutoff**. Otherwise you leak the future into training. That's why `purchases` (and therefore `features`) is filtered to `< cutoff` at the top of this module.

```python
label_window_end = F.lit("2026-10-31").cast("date")
past   = events.filter(F.to_date("ts") <  cutoff)
future = events.filter((F.to_date("ts") >= cutoff) & (F.to_date("ts") < label_window_end))

labels = (
    past.select("user_id").distinct()
    .join(future.filter(F.col("event_type") == "purchase")
                .select("user_id").distinct()
                .withColumn("bought", F.lit(1)),
          on="user_id", how="left")
    .withColumn("churned", F.when(F.col("bought").isNull(), 1).otherwise(0))
    .drop("bought")
)
train = features.join(labels, on="user_id", how="inner")
```

**What leakage looks like here**
- Without the cutoff, `days_since_last` for a customer who bought on Oct 5 would be small *because* they bought in the label window
- The model would learn "small `days_since_last` means not churned" from the answer itself: great offline AUC, useless in production

**Reading the code**
- Label window: Oct 1 to Oct 30, i.e. the 30 days after `cutoff`, matching "stop buying in the next 30 days" from the [syllabus](../../syllabus.md)
- `F.date_add(cutoff, 30)` expresses the same end date without hardcoding it
- Left join + `isNull` marks users with no match on the right: here, no purchase in the label window
- `how="left_anti"` is the built-in for "rows with no match" if you only need the churners
- As in The core verbs, add `F.col("dt")` filters to `past` and `future` so Spark skips the days outside each range
- The final inner join keeps only users who had at least one purchase before `cutoff`: that is the churn population
- Users who only browsed are dropped; if they should be scored, left-join from `labels` and fill their features with 0
- One `cutoff` gives one snapshot (one row per user); production training sets stack several cutoffs (e.g. one per month) so the model sees many points in time

Point-in-time correctness is the senior-DS talking point here, not Spark itself.

---

## Design exercise
> *Paper-based. Think, then write.*

**Task.** Design the schema of ShopCo's daily `customer_features` table.
1. List 8 features: at least 2 window-based and 1 that needs an approximate aggregate.
2. For each, write the PySpark expression family (`agg`, window, join) and whether it shuffles.
3. Decide the table's write partitioning (`dt`? `user_id`?) and justify it in one line.

**Before you start**
- **Grain:** what one row of the table represents; here, one row per `(user_id, dt)`, i.e. each customer's features as of each day
- Write partitioning is covered in [M3 · Partitioning knobs](../03-performance/README.md#4-partitioning-knobs) (write side); skim it first or treat step 3 as a preview
- Which operations shuffle: `groupBy`, `join` (unless broadcast), windows; see [M1 · Jobs, stages, tasks](../01-mental-model/README.md#3-jobs--stages--tasks-and-the-shuffle)

**Self-check**
- Every feature uses only data before that row's `dt`
- Each window-based feature says how it collapses to one row per user per day
- The approximate aggregate is one where groups have many distinct values
- The partition column has low cardinality (one directory per value)

**Why this exercise matters.** Feature tables are where DS work meets Spark. Choosing the grain and the partitioning is a design decision interviewers probe.

---

**Verification note:** Light mode. Steps 7–8 skipped at the user's request; API claims are from author knowledge of Spark 3.x.

**Next module:** [M3 — Performance](../03-performance/README.md): why the feature job is slow and what to check first.
