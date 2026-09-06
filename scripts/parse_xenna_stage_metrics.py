"""Convert Xenna's periodic Stage state tables into tidy CSV rows."""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
TIMESTAMP = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)")
STAGE_ROW = re.compile(r"^│ (Stage \d+ - [^│]+?)\s*│(.*)│$")

FIELDS = [
    "timestamp",
    "stage",
    "actors_target",
    "actors_pending",
    "actors_ready",
    "actors_running",
    "actors_idle",
    "tasks_completed",
    "tasks_returned_none",
    "input_queue",
    "output_queue",
    "slots_used",
    "slots_empty",
    "tasks_per_actor_s",
]


def parse_stage_rows(log_path: Path) -> list[dict[str, str]]:
    text = ANSI.sub("", log_path.read_text(errors="replace")).replace("\r", "\n")
    timestamp = ""
    in_stage_state = False
    rows: list[dict[str, str]] = []
    for line in text.splitlines():
        match = TIMESTAMP.match(line)
        if match:
            timestamp = match.group(1)
        if line == "Stage state:":
            in_stage_state = True
            continue
        if in_stage_state and line.startswith("╘"):
            in_stage_state = False
            continue
        if not in_stage_state:
            continue
        match = STAGE_ROW.match(line)
        if not match:
            continue
        values = [value.strip() for value in match.group(2).split("│")]
        if len(values) != 12:
            continue
        row = {"timestamp": timestamp, "stage": match.group(1).strip()}
        row.update(dict(zip(FIELDS[2:], values, strict=True)))
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("log_path", type=Path)
    parser.add_argument("output_path", type=Path)
    args = parser.parse_args()
    rows = parse_stage_rows(args.log_path)
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    with args.output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {len(rows)} rows to {args.output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
