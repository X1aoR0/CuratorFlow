#!/usr/bin/env bash
set -u

run_id=${1:-caption-scale-4gpu-full-20260905-184128}
output_id=${2:-$run_id}
output=/mnt/curator-flow/output/$output_id
log=/mnt/curator-flow/output/$run_id.log
report=/mnt/curator-flow/output/$run_id-report.json

printf 'time=%s alive=%s errors=%s\n' \
  "$(date +%H:%M:%S)" \
  "$(pgrep -f "[c]urator_flow.run_video_pipelines.*$run_id" | wc -l)" \
  "$(grep -cE 'Pipeline execution failed|Traceback|ERROR' "$log" || true)"

printf 'clips=%s metas=%s videos=%s chunks=%s caption_logs=%s generated_batches=%s\n' \
  "$(find "$output/clips" -name '*.mp4' 2>/dev/null | wc -l)" \
  "$(find "$output/metas/v0" -name '*.json' 2>/dev/null | wc -l)" \
  "$(find "$output/processed_videos" -name '*.json' 2>/dev/null | wc -l)" \
  "$(find "$output/processed_clip_chunks" -name '*.json' 2>/dev/null | wc -l)" \
  "$(grep -c 'Caption for clip' "$log" || true)" \
  "$(grep -c 'Generated .* captions for video' "$log" || true)"

if [[ -f $report ]]; then
  echo REPORT_EXISTS
  /data/curator-cluster/venv/bin/python - "$report" <<'PY'
import json
import sys
data = json.load(open(sys.argv[1]))
print({key: data.get(key) for key in ("status", "wall_time_s", "output_tasks", "throughput_videos_per_s", "throughput_clips_per_s")})
print(data.get("artifacts"))
PY
else
  echo REPORT_MISSING
fi

tail -12 "$log" | cut -c1-260
