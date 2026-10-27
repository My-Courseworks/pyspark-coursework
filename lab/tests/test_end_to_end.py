"""End-to-end at the `test` scale (100K events): generate -> pipeline -> ledger -> report.

Runs in well under a minute on a laptop; needs Java 17 and the venv (local/scripts/setup.sh).
"""

from __future__ import annotations

import json
import os

import pytest
from pyspark.sql import functions as F

from shopco import config as C
from shopco.config import SCALES, Paths
from shopco.generate import generate_all
from shopco.monitor import RunMonitor
from shopco.pipeline import run_pipeline
from shopco.report import load_run, render_report
from shopco.session import build_session


@pytest.fixture(scope="module")
def spark():
    s = build_session("shopco-test", "local", driver_memory="2g", shuffle_partitions=4, extra_conf={"spark.master": "local[2]"})
    yield s
    s.stop()


@pytest.fixture(scope="module")
def run(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("shopco")
    paths = Paths(str(root / "data"), str(root / "runs"))
    scale = SCALES["test"]
    monitor = RunMonitor(spark, "test-run", paths.run_dir("test-run"), {"mode": "local", "scale": "test", "stage": "all"})
    generate_all(spark, scale, paths, monitor, seed=7)
    summary = run_pipeline(spark, scale, paths, monitor)
    report = monitor.finish()
    return paths, scale, monitor, summary, report


def test_generated_shape(spark, run):
    paths, scale, *_ = run
    events = spark.read.parquet(paths.events)
    assert events.count() == scale.events
    assert set(events.columns) == {"user_id", "ts", "event_type", "product_id", "price", "dt"}
    days = [r["dt"] for r in events.select("dt").distinct().collect()]
    assert len(days) == scale.days
    # junk keys exist at roughly the configured rates
    n = scale.events
    null_frac = events.filter(F.col("user_id").isNull()).count() / n
    bot_frac = events.filter(F.col("user_id") == C.BOT_USER_ID).count() / n
    assert abs(null_frac - C.NULL_USER_FRACTION) < 0.003
    assert abs(bot_frac - C.BOT_EVENT_FRACTION) < 0.01
    assert spark.read.parquet(paths.customers).count() == scale.customers
    assert spark.read.parquet(paths.products).count() == scale.products


def test_training_set_is_point_in_time_and_clean(spark, run):
    paths, scale, *_ = run
    train = spark.read.parquet(paths.training_set)
    assert train.filter(F.col("user_id").isNull() | (F.col("user_id") == C.BOT_USER_ID)).count() == 0
    assert train.select("churned").distinct().count() == 2
    assert train.select("user_id").distinct().count() == train.count()
    # every training row is a customer who purchased before the cutoff
    events = spark.read.parquet(paths.events)
    expected = (
        events.filter((F.col("event_type") == "purchase") & (F.col("dt").cast("date") < F.lit(scale.cutoff_date.isoformat()).cast("date")))
        .filter(F.col("user_id").isNotNull() & (F.col("user_id") != C.BOT_USER_ID))
        .select("user_id").distinct().count()
    )
    assert train.count() == expected
    # label is consistent with the raw data for a sample of users
    sample = train.select("user_id", "churned").limit(50).collect()
    future = events.filter((F.col("event_type") == "purchase") & (F.col("dt").cast("date") >= F.lit(scale.cutoff_date.isoformat()).cast("date")))
    future_users = {r["user_id"] for r in future.select("user_id").distinct().collect()}
    for r in sample:
        assert r["churned"] == (0 if r["user_id"] in future_users else 1)
    assert "country" in train.columns and "n_categories" in train.columns and "avg_gap_hours" in train.columns


def test_ledger_and_report(spark, run):
    paths, scale, monitor, summary, report = run
    run_dir = paths.run_dir("test-run")
    assert os.path.exists(f"{run_dir}/run.json") and os.path.exists(f"{run_dir}/report.md")
    meta, steps = load_run(spark, run_dir)
    assert meta["status"] == "ok"
    names = [s["step"] for s in steps]
    assert names[:2] == ["gen_customers", "gen_products"]
    assert "write_training" in names and "summary" in names
    assert all(s["status"] == "ok" for s in steps)
    # steps that write data must have run real Spark jobs with metrics
    wt = next(s for s in steps if s["step"] == "write_training")
    assert wt["n_jobs"] >= 1 and wt["tasks"] >= 1
    if wt.get("metrics_source") == "rest":
        assert wt["shuffle_write_bytes"] > 0 or wt["shuffle_read_bytes"] > 0
    # lazy steps are recorded as such
    assert next(s for s in steps if s["step"] == "agg_features")["n_jobs"] == 0
    assert summary["rows"] == wt["notes"]["rows"]
    assert "| `write_training`" in report and "## Steps" in report
    assert render_report(meta, steps) == report
    with open(f"{run_dir}/ledger/{wt['step_id']}.json") as f:
        assert json.load(f)["step"] == "write_training"
