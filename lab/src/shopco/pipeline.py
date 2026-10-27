"""Churn feature pipeline: the course's M2/M3 lessons as one runnable job.

Steps (each is a monitored unit; lazy steps show 0 jobs on purpose):
  load              read the three tables
  junk_key_audit    count NULL/bot rows, then drop them (M3 skew fix)
  purchases         purchases strictly before the cutoff (point-in-time, M2)
  agg_features      groupBy aggregations
  window_features   lag/avg gap via a window (M2)
  category_breadth  broadcast join to a column-pruned products table (M3)
  enrich_customers  sort-merge join to the 20M-row customers table (M3)
  write_features    materialise customer_features (the feature table is a deliverable)
  labels            churned = no purchase in the label window
  write_training    features ⋈ labels -> training_set
  summary           headline stats
  sink_bigquery     (cloud only) training_set + run ledger -> BigQuery
"""

from __future__ import annotations

import json

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.window import Window

from shopco import config as C
from shopco.config import Paths, Scale


def run_pipeline(
    spark: SparkSession,
    scale: Scale,
    paths: Paths,
    monitor,
    *,
    sink: str | None = None,
    bq_dataset: str | None = None,
    drop_junk: bool = True,
) -> dict:
    cutoff = scale.cutoff_date
    cutoff_lit = F.lit(cutoff.isoformat()).cast("date")

    with monitor.step("load") as h:
        events = spark.read.parquet(paths.events)
        customers = spark.read.parquet(paths.customers)
        products = spark.read.parquet(paths.products)
        h.note(cutoff=cutoff.isoformat(), event_columns=events.columns)

    with monitor.step("junk_key_audit") as h:
        is_null = F.col("user_id").isNull()
        is_bot = F.col("user_id") == C.BOT_USER_ID
        audit = (
            events.groupBy(is_null.alias("is_null"), is_bot.alias("is_bot"))
            .count()
            .collect()
        )
        counts = {(r["is_null"], bool(r["is_bot"])): r["count"] for r in audit}
        null_rows = sum(v for (n, _b), v in counts.items() if n)
        bot_rows = sum(v for (n, b), v in counts.items() if b and not n)
        total = sum(counts.values())
        h.note(total_events=total, null_user_rows=null_rows, bot_rows=bot_rows, dropped=drop_junk)
        # --keep-junk leaves the bot and NULL keys in, so the window step's skew shows up in the report
        clean = events.filter(~(is_null | is_bot)) if drop_junk else events.filter(~is_null)

    with monitor.step("purchases") as h:
        purchases = (
            clean.filter((F.col("event_type") == "purchase") & (F.col("dt").cast("date") < cutoff_lit))
            .select("user_id", "ts", "product_id", "price")
            .cache()  # reused by three aggregations below; small (5% of clean events)
        )
        h.note(rows=purchases.count())

    with monitor.step("agg_features"):
        features = purchases.groupBy("user_id").agg(
            F.count("*").alias("n_orders"),
            F.sum("price").alias("total_spend"),
            F.avg("price").alias("avg_order"),
            F.approx_count_distinct("product_id").alias("n_products_approx"),
            F.datediff(cutoff_lit, F.max(F.to_date("ts"))).alias("days_since_last"),
            F.sum(F.when(F.col("price") > 100, 1).otherwise(0)).alias("n_big_orders"),
        )

    with monitor.step("window_features"):
        w = Window.partitionBy("user_id").orderBy("ts")
        gaps = (
            purchases.withColumn("prev_ts", F.lag("ts").over(w))
            .withColumn("gap_hours", (F.col("ts").cast("long") - F.col("prev_ts").cast("long")) / 3600.0)
            .groupBy("user_id")
            .agg(F.avg("gap_hours").alias("avg_gap_hours"), F.max("gap_hours").alias("max_gap_hours"))
        )
        features = features.join(gaps, "user_id", "left")

    with monitor.step("category_breadth"):
        prod_slim = products.select("product_id", "category")  # prune the wide text columns first
        n_cat = (
            purchases.join(F.broadcast(prod_slim), "product_id")
            .groupBy("user_id")
            .agg(F.countDistinct("category").alias("n_categories"))
        )
        features = features.join(n_cat, "user_id", "left")

    with monitor.step("enrich_customers"):
        cust_slim = customers.select("user_id", "country", "region", "signup_date")
        features = features.join(cust_slim, "user_id", "left").withColumn(
            "tenure_days", F.datediff(cutoff_lit, F.col("signup_date"))
        )

    with monitor.step("write_features") as h:
        features.write.mode("overwrite").parquet(paths.features)
        features = spark.read.parquet(paths.features)
        h.note(rows=features.count(), columns=features.columns)

    with monitor.step("labels"):
        future_buyers = (
            clean.filter((F.col("event_type") == "purchase") & (F.col("dt").cast("date") >= cutoff_lit))
            .select("user_id")
            .distinct()
            .withColumn("bought", F.lit(1))
        )
        labels = (
            features.select("user_id")
            .join(future_buyers, "user_id", "left")
            .withColumn("churned", F.when(F.col("bought").isNull(), 1).otherwise(0))
            .drop("bought")
        )

    with monitor.step("write_training") as h:
        train = features.join(labels, "user_id", "inner").withColumn("cutoff", cutoff_lit)
        train.write.mode("overwrite").parquet(paths.training_set)
        train = spark.read.parquet(paths.training_set)
        h.note(rows=train.count())

    with monitor.step("summary") as h:
        row = train.agg(
            F.count("*").alias("rows"),
            F.avg("churned").alias("churn_rate"),
            F.avg("n_orders").alias("avg_orders"),
            F.avg("total_spend").alias("avg_spend"),
            F.countDistinct("country").alias("n_countries"),
        ).first()
        summary = {k: (float(v) if isinstance(v, float) else v) for k, v in row.asDict().items()}
        h.note(**summary)

    purchases.unpersist()

    if sink == "bigquery":
        if not bq_dataset:
            raise ValueError("--bq-dataset is required with --sink bigquery")
        with monitor.step("sink_bigquery") as h:
            (
                train.write.format("bigquery")
                .option("table", f"{bq_dataset}.training_set")
                .option("writeMethod", "direct")
                .mode("overwrite")
                .save()
            )
            # the run ledger so far (every step before this one), one row per step
            ledger_rows = [
                {
                    "run_id": s["run_id"],
                    "step_id": s["step_id"],
                    "step": s["step"],
                    "status": s["status"],
                    "started_at": s["started_at"],
                    "elapsed_s": float(s["elapsed_s"]),
                    "n_jobs": int(s.get("n_jobs", 0)),
                    "n_stages": int(s.get("n_stages", 0)),
                    "tasks": int(s.get("tasks", 0)),
                    "input_bytes": int(s.get("input_bytes", 0) or 0),
                    "output_bytes": int(s.get("output_bytes", 0) or 0),
                    "shuffle_read_bytes": int(s.get("shuffle_read_bytes", 0) or 0),
                    "shuffle_write_bytes": int(s.get("shuffle_write_bytes", 0) or 0),
                    "spill_disk_bytes": int(s.get("spill_disk_bytes", 0) or 0),
                    "max_skew_ratio": float(s["max_skew_ratio"]) if s.get("max_skew_ratio") else None,
                    "notes_json": json.dumps(s.get("notes") or {}, default=str),
                }
                for s in monitor.steps
            ]
            ledger_schema = (
                "run_id string, step_id string, step string, status string, started_at string, elapsed_s double, "
                "n_jobs long, n_stages long, tasks long, input_bytes long, output_bytes long, shuffle_read_bytes long, "
                "shuffle_write_bytes long, spill_disk_bytes long, max_skew_ratio double, notes_json string"
            )
            (
                spark.createDataFrame(ledger_rows, schema=ledger_schema)
                .write.format("bigquery")
                .option("table", f"{bq_dataset}.run_ledger")
                .option("writeMethod", "direct")
                .mode("append")
                .save()
            )
            h.note(training_table=f"{bq_dataset}.training_set", ledger_table=f"{bq_dataset}.run_ledger", ledger_rows=len(ledger_rows))

    return summary


def read_training_set(spark: SparkSession, paths: Paths) -> DataFrame:
    return spark.read.parquet(paths.training_set)
