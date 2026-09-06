import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

text = Path(sys.argv[1]).read_text(errors="replace")
text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text).replace("\r", "\n")
pattern = re.compile(
    r"CaptionGenerationStage pid=\d+, ip=([0-9.]+).*?"
    r"(2026-09-05 \d\d:\d\d:\d\d\.\d+).*?"
    r"Generated (\d+) captions for video (.+?) chunk-"
)
rows = pattern.findall(text)
print("visible_generation_events", len(rows), "by_ip", dict(Counter(row[0] for row in rows)))
if rows:
    print("first", rows[0][1], rows[0][0])
    print("last", rows[-1][1], rows[-1][0])
    timed = [(datetime.fromisoformat(row[1]), int(row[2])) for row in rows]
    latest = timed[-1][0]
    elapsed_minutes = (latest - timed[0][0]).total_seconds() / 60
    print("elapsed_minutes", round(elapsed_minutes, 2))
    for minutes in (5, 10, 20, 30):
        selected = [row for row in timed if (latest - row[0]).total_seconds() <= minutes * 60]
        print(
            f"last_{minutes}m",
            "videos_per_min",
            round(len(selected) / minutes, 2),
            "captions_per_min",
            round(sum(row[1] for row in selected) / minutes, 2),
        )

stages = [
    "FilePartitioningStage",
    "VideoReaderStage",
    "FixedStrideExtractorStage",
    "ClipTranscodingStage",
    "CaptionPreparationStage",
    "CaptionGenerationStage",
    "FullErrorClipWriterStage",
]
for stage in stages:
    lines = [line for line in text.splitlines() if stage in line and "│" in line]
    print(f"\n{stage}")
    for line in lines[-2:]:
        print(line[:300])
