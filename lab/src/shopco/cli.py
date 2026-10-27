"""Command line: generate | pipeline | all | report.

Same entry point on a laptop (python -m shopco ...) and on Dataproc Serverless
(gcp/jobs/main.py forwards its argv here).
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
import time

from shopco import __version__
from shopco.config import SCALES, Paths


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="shopco", description="ShopCo PySpark lab")
    p.add_argument("stage", choices=["generate", "pipeline", "all", "report"])
    p.add_argument("--mode", choices=["local", "cloud"], default="local")
    p.add_argument("--scale", choices=sorted(SCALES), default="smoke")
    p.add_argument("--data-root", default="data", help="local dir or gs://bucket/prefix")
    p.add_argument("--runs-root", default="runs", help="where run ledgers and reports go")
    p.add_argument("--run-id", default=None, help="defaults to <utc time>-<scale>-<stage>")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--driver-memory", default="12g", help="local mode only")
    p.add_argument("--shuffle-partitions", type=int, default=None)
    p.add_argument("--sink", choices=["bigquery"], default=None, help="cloud only: also write training_set to BigQuery")
    p.add_argument("--bq-dataset", default=None)
    p.add_argument("--conf", action="append", default=[], metavar="k=v", help="extra spark conf (repeatable)")
    p.add_argument("--keep-junk", action="store_true", help="do NOT drop the bot key before the pipeline (shows skew in the report)")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    paths = Paths(args.data_root, args.runs_root)
    scale = SCALES[args.scale]
    run_id = args.run_id or f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{args.scale}-{args.stage}"
    extra = dict(kv.split("=", 1) for kv in args.conf)

    # Imports after arg parsing so `--help` works without a JVM.
    from shopco.session import build_session

    t0 = time.monotonic()
    spark = build_session(
        f"shopco-{args.stage}-{args.scale}",
        args.mode,
        runs_root=args.runs_root if args.mode == "local" else None,
        tmp_dir=paths.tmp if args.mode == "local" else None,
        driver_memory=args.driver_memory,
        shuffle_partitions=args.shuffle_partitions,
        extra_conf=extra,
    )

    if args.stage == "report":
        from shopco.report import load_run, render_report

        if not args.run_id:
            print("report needs --run-id", file=sys.stderr)
            return 2
        meta, steps = load_run(spark, paths.run_dir(run_id))
        print(render_report(meta, steps))
        return 0

    from shopco.monitor import RunMonitor

    meta = {
        "mode": args.mode,
        "scale": scale.name,
        "stage": args.stage,
        "seed": args.seed,
        "keep_junk": args.keep_junk,
        "data_root": args.data_root,
        "shopco_version": __version__,
        "scale_spec": {"days": scale.days, "events_per_day": scale.events_per_day, "events": scale.events, "customers": scale.customers, "products": scale.products},
    }
    monitor = RunMonitor(spark, run_id, paths.run_dir(run_id), meta)
    try:
        if args.stage in ("generate", "all"):
            from shopco.generate import generate_all

            generate_all(spark, scale, paths, monitor, seed=args.seed)
        if args.stage in ("pipeline", "all"):
            from shopco.pipeline import run_pipeline

            run_pipeline(spark, scale, paths, monitor, sink=args.sink, bq_dataset=args.bq_dataset, drop_junk=not args.keep_junk)
    except BaseException:
        # the failing step already called monitor.finish(status="failed")
        raise
    report = monitor.finish(status="ok")
    print(report)
    print(f"run {run_id} finished in {time.monotonic() - t0:.1f}s; report: {paths.run_dir(run_id)}/report.md")
    spark.stop()
    return 0
