# Video Output Contract

CuratorFlow 直接采用 NeMo Curator `ClipWriterStage` 的输出，不再定义一套平行的媒体数据契约。

## Output Layout

```text
output/
├── clips/                    # 通过处理的 MP4 clip
├── filtered_clips/           # 被过滤的 clip
├── previews/                 # caption preview
├── metas/v0/                 # 每个 clip 的 JSON metadata
├── ce1_embd/                 # Cosmos-Embed1 embedding
├── ce1_embd_parquet/         # 批量 embedding parquet
├── processed_videos/         # 原视频级 metadata
└── processed_clip_chunks/    # clip chunk 统计
```

## Clip Metadata

`metas/v0/<clip_uuid>.json` 由 Curator 写出，主要字段包括：

- `span_uuid`：确定性 clip UUID；
- `source_video`：输入视频路径；
- `duration_span`：clip 起止秒数；
- `width_source`、`height_source`、`framerate_source`：源视频属性；
- `clip_location`：输出 MP4 位置；
- `windows`：caption window 的帧范围与模型 caption；
- `valid`：clip 是否包含 buffer 和有效 window。
- `errors`：Curator 各 Stage 写入 `clip.errors` 的完整键值字典，例如
  `{"transcode": "ffmpeg exited with code 1"}`；没有错误时不写该字段。

未启用 caption 时，`windows` 为空且 `valid=false`，但 clip MP4 仍可用于 CPU 转码链路验证。启用 caption 后，window 内字段按 Curator 规则命名，例如 `qwen2.5_caption`。

## Experiment Report

CuratorFlow 额外写 `report.json`。它不替代 Curator metadata，只保存实验信息：参数和 Stage 顺序、环境和输入规模、wall time 与吞吐、Curator StagePerf 指标、产物数量，以及失败异常。

