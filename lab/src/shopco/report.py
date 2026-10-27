"""Render a run ledger as a Markdown report, and re-render old runs on demand."""

from __future__ import annotations

import json
from typing import Any

from shopco.fs import HadoopFS


def _hb(n: int | float | None) -> str:
    if not n:
        return "0"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _secs(s: float | None) -> str:
    if s is None:
        return "-"
    return f"{s:.1f}s" if s < 120 else f"{s/60:.1f}m"


def render_report(meta: dict[str, Any], steps: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    lines.append(f"# ShopCo run `{meta.get('run_id')}`")
    lines.append("")
    lines.append("| | |")
    lines.append("|---|---|")
    for k in ("status", "mode", "scale", "stage", "spark_version", "master", "default_parallelism", "driver_memory", "shuffle_partitions", "started_at", "finished_at"):
        if k in meta:
            lines.append(f"| {k} | `{meta[k]}` |")
    if "elapsed_s" in meta:
        lines.append(f"| total elapsed | **{_secs(meta['elapsed_s'])}** |")
    if meta.get("error"):
        lines.append(f"| error | `{meta['error']}` |")
    lines.append("")

    lines.append("## Steps")
    lines.append("")
    lines.append("| # | step | status | elapsed | jobs | stages | tasks | input | output | shuffle read | shuffle write | spill (disk) | max/median task | notes |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, s in enumerate(steps, 1):
        notes = {k: v for k, v in (s.get("notes") or {}).items() if not isinstance(v, (list, dict))}
        note_str = ", ".join(f"{k}={v:,}" if isinstance(v, int) else f"{k}={v:.4g}" if isinstance(v, float) else f"{k}={v}" for k, v in notes.items())
        skew = s.get("max_skew_ratio")
        skew_str = f"{skew:.1f}×" if skew else "-"
        if skew and skew >= 5:
            skew_str = f"**{skew_str}** ⚠"
        lazy = " (lazy)" if s.get("n_jobs", 0) == 0 and s["status"] == "ok" else ""
        status = "✅" if s["status"] == "ok" else "❌"
        lines.append(
            f"| {i} | `{s['step']}`{lazy} | {status} | {_secs(s['elapsed_s'])} | {s.get('n_jobs', 0)} | {s.get('n_stages', 0)} | {s.get('tasks', 0):,} "
            f"| {_hb(s.get('input_bytes'))} | {_hb(s.get('output_bytes'))} | {_hb(s.get('shuffle_read_bytes'))} | {_hb(s.get('shuffle_write_bytes'))} "
            f"| {_hb(s.get('spill_disk_bytes'))} | {skew_str} | {note_str} |"
        )
    lines.append("")

    total_tasks = sum(s.get("tasks", 0) for s in steps)
    total_shuffle = sum(s.get("shuffle_write_bytes", 0) or 0 for s in steps)
    total_input = sum(s.get("input_bytes", 0) or 0 for s in steps)
    lines.append(f"**Totals:** {len(steps)} steps · {sum(s.get('n_jobs', 0) for s in steps)} jobs · {total_tasks:,} tasks · input {_hb(total_input)} · shuffle written {_hb(total_shuffle)}")
    lines.append("")

    all_stages = [(s["step"], st) for s in steps for st in (s.get("stages") or []) if "executor_run_time_ms" in st]
    if all_stages:
        lines.append("## Slowest stages")
        lines.append("")
        lines.append("| step | stage | tasks | executor time | shuffle read | shuffle write | spill (disk) | max/median task | name |")
        lines.append("|---|---|---|---|---|---|---|---|---|")
        for step, st in sorted(all_stages, key=lambda x: -x[1]["executor_run_time_ms"])[:8]:
            skew = st.get("skew_ratio")
            lines.append(
                f"| `{step}` | {st['stage_id']} | {st['tasks']:,} | {_secs(st['executor_run_time_ms']/1000)} | {_hb(st['shuffle_read_bytes'])} | {_hb(st['shuffle_write_bytes'])} "
                f"| {_hb(st['spill_disk_bytes'])} | {f'{skew:.1f}×' if skew else '-'} | {st['name']} |"
            )
        lines.append("")

    warnings = [s for s in steps if s.get("metrics_warning")]
    if warnings:
        lines.append("## Metric warnings")
        lines.append("")
        for s in warnings:
            lines.append(f"- `{s['step']}`: {s['metrics_warning']}")
        lines.append("")

    lines.append("## How to read this")
    lines.append("")
    lines.append("- **jobs = 0 (lazy):** the step only built a plan; its work ran inside a later step's action (course M1).")
    lines.append("- **tasks:** one per partition per stage; `executor time` sums all task run time, so it exceeds wall time when tasks run in parallel.")
    lines.append("- **shuffle write/read:** bytes moved between stages; the main cost in Spark (M1, M3).")
    lines.append("- **spill (disk):** a partition did not fit in memory; slow but not fatal (M3).")
    lines.append("- **max/median task ≥ 5× ⚠:** one task much slower than the rest = skew (M3). Look for junk keys first.")
    lines.append("- Re-render: `python -m shopco report --runs-root <runs> --run-id <id>`; replay in the History Server from `<runs>/spark-events` (local).")
    return "\n".join(lines) + "\n"


def load_run(spark, run_dir: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    fs = HadoopFS(spark)
    meta = json.loads(fs.read_text(f"{run_dir}/run.json"))
    steps = [json.loads(fs.read_text(p)) for p in fs.glob(f"{run_dir}/ledger/*.json")]
    steps.sort(key=lambda s: s["step_id"])
    return meta, steps
