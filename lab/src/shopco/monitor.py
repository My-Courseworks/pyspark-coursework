"""Run monitoring: every pipeline step becomes a ledger record with real Spark metrics.

How it works (local/adr/0005):
  1. Each step sets a Spark *job group* named after the step, so every job the
     step triggers is tagged with it.
  2. When the step ends, the monitor asks the Spark UI's REST API for the jobs in
     that group and their stages: tasks, bytes read/written, shuffle, spill, and
     the per-task run-time distribution (max/median = the skew ratio from M3).
  3. The record is written as JSON next to the data (local dir or gs://) and
     printed as one structured stdout line (SHOPCO_STEP {...}) so Cloud Logging
     can turn it into metrics.
  4. If the REST API is unreachable, the monitor falls back to Spark's
     StatusTracker (task counts only) and says so in the record.
"""

from __future__ import annotations

import datetime as dt
import json
import time
import urllib.request
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from shopco.fs import HadoopFS

_QUANTILES = "0.0,0.5,1.0"  # min, median, max


@dataclass
class StepHandle:
    step_id: str
    name: str
    notes: dict[str, Any] = field(default_factory=dict)

    def note(self, **kw: Any) -> None:
        self.notes.update(kw)


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


class RunMonitor:
    def __init__(self, spark: SparkSession, run_id: str, run_dir: str, meta: dict[str, Any]):
        self.spark = spark
        self.sc = spark.sparkContext
        self.run_id = run_id
        self.run_dir = run_dir.rstrip("/")
        self.meta = dict(meta)
        self.steps: list[dict[str, Any]] = []
        self.fs = HadoopFS(spark)
        self._t0 = time.monotonic()
        self.app_id = self.sc.applicationId
        self.ui_url = self.sc.uiWebUrl
        self.meta.update(
            run_id=run_id,
            app_id=self.app_id,
            spark_version=spark.version,
            master=self.sc.master,
            default_parallelism=self.sc.defaultParallelism,
            driver_memory=self.sc.getConf().get("spark.driver.memory", "default"),
            shuffle_partitions=spark.conf.get("spark.sql.shuffle.partitions"),
            ui_url=self.ui_url,
            started_at=_now(),
            status="running",
        )
        self.fs.mkdirs(f"{self.run_dir}/ledger")
        self._write_run()
        print("SHOPCO_RUN " + json.dumps(self.meta), flush=True)

    # ---- steps -------------------------------------------------------------
    @contextmanager
    def step(self, name: str):
        idx = len(self.steps) + 1
        step_id = f"{idx:02d}-{name}"
        handle = StepHandle(step_id, name)
        self.sc.setJobGroup(step_id, name, False)
        started = _now()
        t0 = time.monotonic()
        status, error = "ok", None
        print(f"SHOPCO_STEP_START {step_id}", flush=True)
        try:
            yield handle
        except BaseException as e:  # record, then re-raise
            status, error = "failed", f"{type(e).__name__}: {e}"
            raise
        finally:
            elapsed = round(time.monotonic() - t0, 3)
            self.sc.setJobGroup("idle", "idle", False)
            metrics = self._collect(step_id)
            rec: dict[str, Any] = {
                "run_id": self.run_id,
                "step_id": step_id,
                "step": name,
                "status": status,
                "started_at": started,
                "elapsed_s": elapsed,
                "error": error,
                "notes": handle.notes,
                **metrics,
            }
            self.steps.append(rec)
            self.fs.write_text(f"{self.run_dir}/ledger/{step_id}.json", json.dumps(rec))
            print("SHOPCO_STEP " + json.dumps(rec, default=str), flush=True)
            if status == "failed":
                self.finish(status="failed", error=error)

    # ---- metrics -----------------------------------------------------------
    def _rest(self, path: str) -> Any:
        url = f"{self.ui_url}/api/v1/applications/{self.app_id}{path}"
        with urllib.request.urlopen(url, timeout=10) as r:  # noqa: S310 (loopback UI)
            return json.loads(r.read().decode("utf-8"))

    def _collect(self, group: str) -> dict[str, Any]:
        try:
            return self._collect_rest(group)
        except Exception as e:  # UI off, port blocked, etc.
            fallback = self._collect_status_tracker(group)
            fallback["metrics_source"] = "statusTracker"
            fallback["metrics_warning"] = f"REST API unavailable: {type(e).__name__}: {e}"
            return fallback

    def _collect_rest(self, group: str) -> dict[str, Any]:
        jobs = [j for j in self._rest("/jobs") if j.get("jobGroup") == group]
        stage_ids = sorted({sid for j in jobs for sid in j.get("stageIds", [])})
        stages: list[dict[str, Any]] = []
        for sid in stage_ids:
            attempts = self._rest(f"/stages/{sid}?withSummaries=true&quantiles={_QUANTILES}")
            if not attempts:
                continue
            s = attempts[-1]  # latest attempt
            dist = s.get("taskMetricsDistributions") or {}
            run_q = dist.get("executorRunTime") or []
            median = run_q[1] if len(run_q) == 3 else None
            mx = run_q[2] if len(run_q) == 3 else None
            skew = round(mx / median, 2) if median and mx else None
            stages.append(
                {
                    "stage_id": sid,
                    "name": (s.get("name") or "")[:80],
                    "status": s.get("status"),
                    "tasks": s.get("numTasks", 0),
                    "failed_tasks": s.get("numFailedTasks", 0),
                    "executor_run_time_ms": s.get("executorRunTime", 0),
                    "input_bytes": s.get("inputBytes", 0),
                    "output_bytes": s.get("outputBytes", 0),
                    "shuffle_read_bytes": s.get("shuffleReadBytes", 0),
                    "shuffle_write_bytes": s.get("shuffleWriteBytes", 0),
                    "spill_memory_bytes": s.get("memoryBytesSpilled", 0),
                    "spill_disk_bytes": s.get("diskBytesSpilled", 0),
                    "task_time_median_ms": median,
                    "task_time_max_ms": mx,
                    "skew_ratio": skew,
                }
            )
        agg = {
            "metrics_source": "rest",
            "jobs": [j["jobId"] for j in jobs],
            "n_jobs": len(jobs),
            "n_stages": len(stages),
            "tasks": sum(s["tasks"] for s in stages),
            "failed_tasks": sum(s["failed_tasks"] for s in stages),
            "executor_run_time_ms": sum(s["executor_run_time_ms"] for s in stages),
            "input_bytes": sum(s["input_bytes"] for s in stages),
            "output_bytes": sum(s["output_bytes"] for s in stages),
            "shuffle_read_bytes": sum(s["shuffle_read_bytes"] for s in stages),
            "shuffle_write_bytes": sum(s["shuffle_write_bytes"] for s in stages),
            "spill_disk_bytes": sum(s["spill_disk_bytes"] for s in stages),
            "max_skew_ratio": max((s["skew_ratio"] for s in stages if s["skew_ratio"]), default=None),
            "stages": stages,
        }
        return agg

    def _collect_status_tracker(self, group: str) -> dict[str, Any]:
        tracker = self.sc.statusTracker()
        job_ids = list(tracker.getJobIdsForGroup(group))
        stages, tasks, failed = [], 0, 0
        for jid in job_ids:
            info = tracker.getJobInfo(jid)
            if info is None:
                continue
            for sid in info.stageIds:
                si = tracker.getStageInfo(sid)
                if si is None:
                    continue
                stages.append({"stage_id": sid, "name": si.name[:80], "tasks": si.numTasks, "failed_tasks": si.numFailedTasks})
                tasks += si.numTasks
                failed += si.numFailedTasks
        return {"jobs": job_ids, "n_jobs": len(job_ids), "n_stages": len(stages), "tasks": tasks, "failed_tasks": failed, "stages": stages}

    # ---- run lifecycle ------------------------------------------------------
    def _write_run(self) -> None:
        self.fs.write_text(f"{self.run_dir}/run.json", json.dumps(self.meta, indent=2, default=str))

    def finish(self, status: str = "ok", error: str | None = None) -> str:
        from shopco.report import render_report  # local import: report depends on nothing here

        self.meta.update(
            status=status,
            error=error,
            finished_at=_now(),
            elapsed_s=round(time.monotonic() - self._t0, 3),
            n_steps=len(self.steps),
            steps_failed=sum(1 for s in self.steps if s["status"] != "ok"),
        )
        self._write_run()
        report = render_report(self.meta, self.steps)
        self.fs.write_text(f"{self.run_dir}/report.md", report)
        print("SHOPCO_RUN " + json.dumps(self.meta, default=str), flush=True)
        return report
