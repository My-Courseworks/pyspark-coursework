"""SparkSession construction for the two runtimes.

local: master=local[*], explicit driver memory, event log on (for the History
Server), shuffle partitions sized for a laptop. See local/adr/0006.

cloud: Dataproc Serverless sets master, memory and autoscaling from the batch
submission; we only add the settings that are the same everywhere.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

COMMON_CONF = {
    "spark.sql.adaptive.enabled": "true",  # AQE: coalesce shuffle partitions, runtime broadcast, skew-join split
    "spark.sql.execution.arrow.pyspark.enabled": "true",  # Arrow for toPandas / pandas UDFs
    "spark.ui.showConsoleProgress": "false",  # the monitor prints its own progress lines
    "spark.sql.session.timeZone": "UTC",
}


def build_session(
    app_name: str,
    mode: str,
    *,
    runs_root: str | None = None,
    tmp_dir: str | None = None,
    driver_memory: str = "12g",
    shuffle_partitions: int | None = None,
    extra_conf: dict[str, str] | None = None,
) -> SparkSession:
    b = SparkSession.builder.appName(app_name)
    for k, v in COMMON_CONF.items():
        b = b.config(k, v)

    if mode == "local":
        b = b.master("local[*]")
        # The driver IS the executor in local mode, so this is the whole Spark heap.
        b = b.config("spark.driver.memory", driver_memory)
        b = b.config("spark.driver.maxResultSize", "2g")
        b = b.config("spark.sql.shuffle.partitions", str(shuffle_partitions or 4 * (os.cpu_count() or 4)))
        if tmp_dir:
            os.makedirs(tmp_dir, exist_ok=True)
            b = b.config("spark.local.dir", tmp_dir)  # shuffle spill files go here, not /tmp
        if runs_root:
            ev = os.path.join(runs_root, "spark-events")
            os.makedirs(ev, exist_ok=True)
            b = b.config("spark.eventLog.enabled", "true")
            b = b.config("spark.eventLog.dir", f"file://{os.path.abspath(ev)}")
    elif mode == "cloud":
        if shuffle_partitions:
            b = b.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
    else:
        raise ValueError(f"unknown mode {mode!r}; expected 'local' or 'cloud'")

    for k, v in (extra_conf or {}).items():
        b = b.config(k, v)
    return b.getOrCreate()
