"""Print compact statistics from parsed Xenna Stage metrics CSV files."""

from __future__ import annotations

import argparse
import csv
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path


def read_rows(path: Path) -> list[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            row["time"] = datetime.fromisoformat(row["timestamp"])
            row["stage_short"] = row["stage"].split(" - ", 1)[-1]
            for field in (
                "actors_ready", "actors_running", "actors_idle", "tasks_completed",
                "input_queue", "output_queue", "slots_used", "slots_empty",
            ):
                row[field] = float(row[field] or 0)
            row["tasks_per_actor_s"] = float(row["tasks_per_actor_s"] or 0)
            rows.append(row)
    return rows


def percentile(values: list[float], ratio: float) -> float:
    values = sorted(values)
    return values[round((len(values) - 1) * ratio)] if values else 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("csv_path", type=Path)
    args = parser.parse_args()
    rows = read_rows(args.csv_path)
    start, end = min(row["time"] for row in rows), max(row["time"] for row in rows)
    print("duration_min", round((end - start).total_seconds() / 60, 2), "snapshots", len(rows) // 7)
    for stage in sorted({row["stage_short"] for row in rows}):
        selected = [row for row in rows if row["stage_short"] == stage]
        ready = [row["actors_ready"] for row in selected]
        running = [row["actors_running"] for row in selected]
        speed = [row["tasks_per_actor_s"] for row in selected if row["tasks_per_actor_s"] > 0]
        transitions = []
        last = None
        for row in selected:
            value = int(row["actors_ready"])
            if value != last:
                transitions.append((round((row["time"] - start).total_seconds() / 60, 1), value))
                last = value
        print(
            stage,
            "ready_dist", dict(sorted(Counter(map(int, ready)).items())),
            "ready_mean", round(statistics.mean(ready), 2),
            "running_mean", round(statistics.mean(running), 2),
            "completed", int(max(row["tasks_completed"] for row in selected)),
            "speed_p50", round(percentile(speed, .5), 5),
            "speed_p90", round(percentile(speed, .9), 5),
            "max_input_q", int(max(row["input_queue"] for row in selected)),
            "max_output_q", int(max(row["output_queue"] for row in selected)),
            "transitions", transitions,
        )

    by_time = {}
    for row in rows:
        by_time.setdefault(row["timestamp"], {})[row["stage_short"]] = row
    for label, upstream, downstream in (
        ("transcoded_minus_captioned", "ClipTranscodingStage", "CaptionGenerationStage"),
        ("prepared_minus_captioned", "CaptionPreparationStage", "CaptionGenerationStage"),
        ("captioned_minus_written", "CaptionGenerationStage", "FullErrorClipWriterStage"),
    ):
        values = [
            max(0, snapshot[upstream]["tasks_completed"] - snapshot[downstream]["tasks_completed"])
            for snapshot in by_time.values()
            if upstream in snapshot and downstream in snapshot
        ]
        print(label, "mean", round(statistics.mean(values), 2), "p95", percentile(values, .95), "max", max(values))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
