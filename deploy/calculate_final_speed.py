from datetime import datetime

main_seconds = 4465.4376764297485
resume_seconds = 1837.217269897461
main_videos, resume_videos = 194, 38
main_clips, resume_clips = 1163, 228
main_windows, resume_windows = 1241, 252

for name, seconds, videos, clips, windows in (
    ("main", main_seconds, main_videos, main_clips, main_windows),
    ("resume", resume_seconds, resume_videos, resume_clips, resume_windows),
    ("active_total", main_seconds + resume_seconds, 232, 1391, 1493),
):
    minutes = seconds / 60
    print(
        name,
        "minutes", round(minutes, 2),
        "videos_per_min", round(videos / minutes, 3),
        "clips_per_min", round(clips / minutes, 3),
        "windows_per_min", round(windows / minutes, 3),
    )

start = datetime.fromisoformat("2026-09-05 18:41:28")
finish = datetime.fromisoformat("2026-09-05 20:56:11")
minutes = (finish - start).total_seconds() / 60
print(
    "wall_clock_total",
    "minutes", round(minutes, 2),
    "videos_per_min", round(232 / minutes, 3),
    "clips_per_min", round(1391 / minutes, 3),
    "windows_per_min", round(1493 / minutes, 3),
)
