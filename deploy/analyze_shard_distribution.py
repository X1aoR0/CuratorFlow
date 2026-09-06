import collections
import json
import statistics
import subprocess
from pathlib import Path

root = Path("/mnt/curator-flow/input")
output = Path("/mnt/curator-flow/output/caption-scale-4gpu-full-20260905-184128")
files = sorted(root.rglob("*.mp4"), key=str)
done = set()
for path in (output / "processed_videos").rglob("*.json"):
    try:
        video = json.loads(path.read_text()).get("video")
        if video:
            done.add(str(Path(video).resolve()))
    except (OSError, ValueError):
        pass

rows = []
for index, path in enumerate(files):
    raw = subprocess.check_output(
        [
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=width,height,avg_frame_rate,bit_rate",
            "-show_entries", "format=duration,bit_rate,size", "-of", "json", str(path),
        ],
        text=True,
    )
    data = json.loads(raw)
    stream = data.get("streams", [{}])[0]
    fmt = data.get("format", {})
    width, height = int(stream.get("width", 0)), int(stream.get("height", 0))
    rows.append(
        {
            "index": index,
            "path": str(path),
            "size": int(fmt.get("size") or path.stat().st_size),
            "bitrate": int(fmt.get("bit_rate") or stream.get("bit_rate") or 0),
            "pixels": width * height,
            "resolution": f"{width}x{height}",
            "done": str(path.resolve()) in done,
        }
    )


def percentile(values, ratio):
    values = sorted(values)
    return values[round((len(values) - 1) * ratio)]


def show(name, selected):
    print(
        name,
        "n", len(selected),
        "size_mean_MB", round(statistics.mean(x["size"] for x in selected) / 1e6, 2),
        "size_p50_MB", round(percentile([x["size"] for x in selected], 0.5) / 1e6, 2),
        "bitrate_mean_Mbps", round(statistics.mean(x["bitrate"] for x in selected) / 1e6, 2),
        "bitrate_p50_Mbps", round(percentile([x["bitrate"] for x in selected], 0.5) / 1e6, 2),
        "pixels_mean_M", round(statistics.mean(x["pixels"] for x in selected) / 1e6, 2),
        "top_res", collections.Counter(x["resolution"] for x in selected).most_common(5),
    )

quarters = [rows[index:index + 58] for index in range(0, len(rows), 58)]
for index, selected in enumerate(quarters, 1):
    show(f"quarter_{index}", selected)
show("done_now", [x for x in rows if x["done"]])
show("pending_now", [x for x in rows if not x["done"]])
print("largest_pending")
for row in sorted((x for x in rows if not x["done"]), key=lambda x: x["size"], reverse=True)[:8]:
    print(round(row["size"] / 1e6, 1), round(row["bitrate"] / 1e6, 2), row["resolution"], row["path"])
