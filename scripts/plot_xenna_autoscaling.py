"""Render Xenna stage metrics CSV as a dependency-free SVG report figure."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime
from html import escape
from pathlib import Path

COLORS = {
    "VideoReaderStage": "#4C78A8",
    "FixedStrideExtractorStage": "#72B7B2",
    "ClipTranscodingStage": "#F58518",
    "CaptionPreparationStage": "#ECA82C",
    "CaptionGenerationStage": "#E45756",
    "FullErrorClipWriterStage": "#54A24B",
}


def read_csv(path: Path, phase: str) -> list[dict]:
    rows = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            row["phase"] = phase
            row["time"] = datetime.fromisoformat(row["timestamp"])
            for field in (
                "actors_ready", "actors_running", "actors_idle", "tasks_completed",
                "input_queue", "output_queue", "slots_used", "slots_empty",
            ):
                row[field] = float(row[field] or 0)
            row["tasks_per_actor_s"] = float(row["tasks_per_actor_s"] or 0)
            row["stage_short"] = row["stage"].split(" - ", 1)[-1]
            rows.append(row)
    return rows


def polyline(points, color, width=2, dash=None, opacity=1):
    attrs = f'fill="none" stroke="{color}" stroke-width="{width}" opacity="{opacity}"'
    if dash:
        attrs += f' stroke-dasharray="{dash}"'
    coords = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)
    return f'<polyline {attrs} points="{coords}" />'


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("main_csv", type=Path)
    parser.add_argument("resume_csv", type=Path)
    parser.add_argument("output_svg", type=Path)
    args = parser.parse_args()

    main_rows = read_csv(args.main_csv, "Main run")
    resume_rows = read_csv(args.resume_csv, "Resume")
    main_start = min(row["time"] for row in main_rows)
    resume_start = min(row["time"] for row in resume_rows)
    main_duration = max((row["time"] - main_start).total_seconds() / 60 for row in main_rows)
    gap = 4
    for row in main_rows:
        row["minute"] = (row["time"] - main_start).total_seconds() / 60
    for row in resume_rows:
        row["minute"] = main_duration + gap + (row["time"] - resume_start).total_seconds() / 60
    rows = main_rows + resume_rows
    x_max = max(row["minute"] for row in rows)

    width, height = 1500, 1275
    left, right = 95, 1440
    panels = [(205, 395), (480, 670), (755, 945), (1030, 1220)]
    plot_width = right - left
    svg = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#111318"/>',
        '<style>text{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,sans-serif;fill:#dfe5ef}.muted{fill:#8f99a8}.title{font-size:22px;font-weight:700}.axis{font-size:12px}.panel{font-size:15px;font-weight:650}</style>',
        '<text x="40" y="38" class="title">Xenna Streaming: Autoscaling and Backpressure Timeline</text>',
        '<text x="40" y="64" class="muted">Main run: 194 videos before Ray disk protection · Resume: remaining 38 videos, succeeded</text>',
    ]

    split_x = left + plot_width * main_duration / x_max
    for top, bottom in panels:
        svg.append(f'<rect x="{left}" y="{top}" width="{plot_width}" height="{bottom-top}" fill="#181c23" stroke="#343a46"/>')
        svg.append(f'<line x1="{split_x:.1f}" y1="{top}" x2="{split_x:.1f}" y2="{bottom}" stroke="#c46b73" stroke-dasharray="6 5"/>')
    svg.append(f'<text x="{split_x - 8:.1f}" y="174" text-anchor="end" class="axis">Main run · OutOfDisk stop →</text>')
    svg.append(f'<text x="{split_x + 8:.1f}" y="174" class="axis">← 38-video resume</text>')

    def x(value):
        return left + plot_width * value / x_max

    def draw_panel(index, title, field, stages, y_max, dashed_field=None):
        top, bottom = panels[index]
        svg.append(f'<text x="{left}" y="{top-12}" class="panel">{escape(title)}</text>')
        for tick in range(5):
            value = y_max * tick / 4
            yy = bottom - (bottom - top) * value / y_max
            svg.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{right}" y2="{yy:.1f}" stroke="#2b303a"/>')
            svg.append(f'<text x="{left-10}" y="{yy+4:.1f}" text-anchor="end" class="axis muted">{value:g}</text>')
        for stage in stages:
            color = COLORS[stage]
            for phase in ("Main run", "Resume"):
                selected = [row for row in rows if row["stage_short"] == stage and row["phase"] == phase]
                points = [
                    (x(row["minute"]), bottom - (bottom - top) * min(row[field], y_max) / y_max)
                    for row in selected
                ]
                svg.append(polyline(points, color))
                if dashed_field:
                    other = [
                        (x(row["minute"]), bottom - (bottom - top) * min(row[dashed_field], y_max) / y_max)
                        for row in selected
                    ]
                    svg.append(polyline(other, color, width=1.4, dash="5 4", opacity=.8))

    def draw_gap_panel(index):
        top, bottom = panels[index]
        svg.append(f'<text x="{left}" y="{top-12}" class="panel">In-flight work between adjacent Stages (completed-count gaps)</text>')
        gap_specs = [
            ("Transcoded − captioned", "ClipTranscodingStage", "CaptionGenerationStage", "#F58518"),
            ("Prepared − captioned", "CaptionPreparationStage", "CaptionGenerationStage", "#ECA82C"),
            ("Captioned − written", "CaptionGenerationStage", "FullErrorClipWriterStage", "#54A24B"),
        ]
        y_max = 12
        for tick in range(5):
            value = y_max * tick / 4
            yy = bottom - (bottom - top) * value / y_max
            svg.append(f'<line x1="{left}" y1="{yy:.1f}" x2="{right}" y2="{yy:.1f}" stroke="#2b303a"/>')
            svg.append(f'<text x="{left-10}" y="{yy+4:.1f}" text-anchor="end" class="axis muted">{value:g}</text>')
        for label, upstream, downstream, color in gap_specs:
            for phase in ("Main run", "Resume"):
                snapshots = {}
                for row in rows:
                    if row["phase"] == phase and row["stage_short"] in (upstream, downstream):
                        snapshots.setdefault(row["timestamp"], {})[row["stage_short"]] = row
                selected = []
                for snapshot in snapshots.values():
                    if upstream in snapshot and downstream in snapshot:
                        upstream_row, downstream_row = snapshot[upstream], snapshot[downstream]
                        selected.append((upstream_row["minute"], max(0, upstream_row["tasks_completed"] - downstream_row["tasks_completed"])))
                points = [(x(minute), bottom - (bottom - top) * min(value, y_max) / y_max) for minute, value in selected]
                svg.append(polyline(points, color))
            legend_index = gap_specs.index((label, upstream, downstream, color))
            lx = left + 20 + legend_index * 310
            svg.append(f'<line x1="{lx}" y1="{top+18}" x2="{lx+24}" y2="{top+18}" stroke="{color}" stroke-width="3"/>')
            svg.append(f'<text x="{lx+30}" y="{top+22}" class="axis">{escape(label)}</text>')

    draw_panel(0, "Actors: ready (solid) and running (dashed)", "actors_ready", list(COLORS), 12, "actors_running")
    draw_panel(1, "Cumulative completed video tasks", "tasks_completed", ["ClipTranscodingStage", "CaptionPreparationStage", "CaptionGenerationStage", "FullErrorClipWriterStage"], 240)
    draw_gap_panel(2)
    draw_panel(3, "Per-actor processing speed (tasks/s)", "tasks_per_actor_s", ["ClipTranscodingStage", "CaptionPreparationStage", "CaptionGenerationStage"], .30)

    for tick in range(0, int(x_max) + 1, 10):
        xx = x(tick)
        svg.append(f'<line x1="{xx:.1f}" y1="{panels[-1][1]}" x2="{xx:.1f}" y2="{panels[-1][1]+6}" stroke="#8f99a8"/>')
        svg.append(f'<text x="{xx:.1f}" y="{panels[-1][1]+23}" text-anchor="middle" class="axis muted">{tick}</text>')
    svg.append(f'<text x="{(left+right)/2}" y="1268" text-anchor="middle" class="axis">Elapsed minutes (4-minute visual gap separates runs)</text>')

    legend_x, legend_y = 70, 103
    for index, (stage, color) in enumerate(COLORS.items()):
        xx = legend_x + (index % 2) * 520
        yy = legend_y + (index // 2) * 24
        svg.append(f'<line x1="{xx}" y1="{yy}" x2="{xx+24}" y2="{yy}" stroke="{color}" stroke-width="3"/>')
        svg.append(f'<text x="{xx+31}" y="{yy+4}" class="axis">{escape(stage)}</text>')

    svg.append("</svg>")
    args.output_svg.parent.mkdir(parents=True, exist_ok=True)
    args.output_svg.write_text("\n".join(svg), encoding="utf-8")
    print(args.output_svg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
