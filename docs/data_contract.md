# Data Contract

本文档定义 CuratorFlow 的最小多模态数据闭环。目标不是先做大规模分布式，而是先用一小批图片和视频跑通从原始媒体到可供多模态大模型预训练读取的语料产物。

设计参考本地 `~/work/ai/Curator` 项目：

- 图片链路：`FilePartitioningStage` 读取 tar shard，`ImageReaderStage` 产出 `ImageBatch/ImageObject`，后续 embedding/filter，最后 `ImageWriterStage` 写 tar 和同名 Parquet 元数据。
- 视频链路：`VideoReader` 发现文件并产出 `VideoTask/Video`，后续切 clip、抽帧、过滤、embedding/caption，最后 `ClipWriterStage` 写 `clips/`、`metas/v0/`、`ce1_embd_parquet/` 等目录。
- 执行模型：Pipeline 由 Stage 串联，数据对象在 Ray object store 中流动，Writer 在末端持久化。

CuratorFlow 在此基础上增加一个统一的训练语料 Manifest，屏蔽图片和视频底层产物布局差异。

## 1. 闭环目标

M1 闭环必须回答四个问题：

1. 输入的图片、视频和文本描述如何被唯一标识。
2. 每个样本经过了哪些校验、清洗、过滤和派生产物生成。
3. 哪些样本进入预训练语料，哪些样本被拒绝，原因是什么。
4. 同一批输入重复运行时，输出路径、样本 ID、统计报告是否稳定。

最小闭环：

```text
raw media + dataset manifest
    -> ingest and task mapping
    -> image decode / video probe
    -> text normalization
    -> content hash and duplicate marking
    -> cheap quality filtering
    -> deterministic writer
    -> training manifest + rejected manifest + run report
```

成熟预训练数据处理链路必须包含 pHash、embedding、图文/文视频对齐、aesthetic、NSFW、视频 motion、caption 等质量算子。第一轮 smoke 可以先验证数据能进出这些 Stage；如果某个模型权重或 GPU 环境缺失，不能静默省略，必须在报告里把该 Stage 标记为 `blocked_missing_model` 或 `blocked_missing_gpu`。

## 2. 输入数据形态

### 2.1 推荐目录

```text
data/
├── samples/
│   ├── images/
│   │   ├── img_000001.jpg
│   │   ├── img_000002.jpg
│   │   └── corrupt.jpg
│   └── videos/
│       ├── vid_000001.mp4
│       ├── vid_000002.mp4
│       └── corrupt.mp4
└── manifests/
    └── dataset_manifest.jsonl
```

后续兼容 Curator 图片路径时，可以把图片目录转换为 WebDataset tar：

```text
data/raw/image_wds/
├── 00000.tar
└── 00001.tar
```

Curator 的 `ImageReaderStage` 只从 tar 中提取 JPEG 图片，tar 内的 `.txt`、`.json` 等非 JPEG 文件会被忽略。因此 CuratorFlow 的权威文本和血缘信息应放在外部 manifest 或 writer 产出的 Parquet/JSON 元数据中，而不是依赖 tar 内伴随文件被 reader 自动读取。

### 2.2 最小样本覆盖

M1 数据集规模保持很小，但要覆盖关键分支：

| 类型 | 建议数量 | 用途 |
| --- | ---: | --- |
| 正常图片 | 20-100 | 验证 decode、metadata、输出 |
| 重复图片 | 2-5 | 验证 hash 和 duplicate 标记 |
| 坏图片 | 1-3 | 验证 rejected manifest |
| 过小图片 | 1-3 | 验证质量过滤 |
| 正常短视频 | 3-10 | 验证 probe、clip、输出 |
| 重复视频 | 1-2 | 验证 content hash |
| 坏视频 | 1-2 | 验证错误记录 |
| 过短或异常比例视频 | 1-2 | 验证质量过滤 |

## 3. Seed Manifest

`data/manifests/dataset_manifest.jsonl` 是闭环输入的权威清单。每行是一个原始媒体样本。

### 3.1 必填字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `sample_id` | string | 输入样本稳定 ID，建议由来源 ID 或路径 hash 生成 |
| `dataset_name` | string | 数据集名，例如 `m1_smoke` |
| `dataset_version` | string | 输入数据版本，例如 `2026-08-31` |
| `media_type` | string | `image` 或 `video` |
| `uri` | string | 本地路径、OSS/S3 URI 或 HTTP URL |
| `uri_type` | string | `local_file`、`tar_member`、`s3`、`oss` 或 `http` |
| `text` | string 或 null | 图片 caption、视频 caption、alt text 或人工描述 |
| `source` | string | 来源系统、下载任务或构造脚本名 |
| `license` | string 或 null | 许可信息，不明确时填 `unknown` |

### 3.2 可选字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `shard_id` | string | 输入 shard ID，单文件输入可为空 |
| `member_name` | string | tar 内成员名，仅 `tar_member` 使用 |
| `checksum_sha256` | string | 原始文件 sha256，可在 ingest 阶段补齐 |
| `bytes` | int | 文件字节数，可在 ingest 阶段补齐 |
| `mime_type` | string | 例如 `image/jpeg`、`video/mp4` |
| `language` | string | 文本语言，不确定时填 `unknown` |
| `created_at` | string | 输入清单生成时间 |
| `metadata` | object | 来源特有字段，例如 URL、原始标题、作者、标签 |

### 3.3 示例

```json
{"sample_id":"img_000001","dataset_name":"m1_smoke","dataset_version":"2026-08-31","media_type":"image","uri":"data/samples/images/img_000001.jpg","uri_type":"local_file","text":"A red chair beside a window.","source":"local_seed","license":"unknown","metadata":{"split":"smoke"}}
{"sample_id":"vid_000001","dataset_name":"m1_smoke","dataset_version":"2026-08-31","media_type":"video","uri":"data/samples/videos/vid_000001.mp4","uri_type":"local_file","text":"A short clip of traffic moving through an intersection.","source":"local_seed","license":"unknown","metadata":{"split":"smoke"}}
```

## 4. 内部任务映射

CuratorFlow 的统一 manifest 在 pipeline 内部映射为不同任务对象。

### 4.1 图片任务

Curator 兼容视角：

```text
ImageBatch
└── ImageObject
    ├── image_path
    ├── image_id
    ├── metadata
    ├── image_data
    ├── embedding
    ├── aesthetic_score
    └── nsfw_score
```

CuratorFlow 约定：

- `image_id` 使用 `sample_id`。
- `image_path` 使用原始 `uri` 或 `tar/member` 展开路径。
- `metadata` 必须携带原始 manifest 字段中除大对象外的血缘信息。
- `image_data` 只在 decode 到 writer 的活跃窗口中存在，不落盘保存为中间文件。

### 4.2 视频任务

Curator 兼容视角：

```text
VideoTask
└── Video
    ├── input_video
    ├── source_bytes
    ├── metadata
    ├── clips
    ├── filtered_clips
    └── errors
```

视频经过切分后，训练样本粒度从原始 video 变成 clip：

```text
Clip
├── uuid
├── source_video
├── span
├── buffer
├── extracted_frames
├── motion_score_global_mean
├── motion_score_per_patch_min_256
├── aesthetic_score
├── cosmos_embed1_embedding
├── windows
└── errors
```

CuratorFlow 约定：

- 原始视频的 `sample_id` 保留为 `parent_sample_id`。
- clip 样本 ID 使用确定性规则生成：`clip_id = sha256(parent_sample_id + start_time + end_time + pipeline_version)[:16]`。
- 第一轮使用 Curator `FixedStrideExtractorStage` 做确定性切片，默认 `clip_len_s=5`、`clip_stride_s=5`、`min_clip_length_s=2`。
- clip 字节写出使用 Curator `ClipTranscodingStage` + `ClipWriterStage`，输出布局与 Curator 视频导出保持一致。

## 5. 处理阶段字段

### 5.1 Ingest

补齐并校验：

| 字段 | 说明 |
| --- | --- |
| `resolved_uri` | 绝对路径或可读 URI |
| `exists` | 文件是否存在 |
| `bytes` | 文件大小 |
| `checksum_sha256` | 原始文件 sha256 |
| `ingest_error` | 文件不存在、权限错误、URI 不支持等 |

### 5.2 Decode And Probe

图片字段：

| 字段 | 说明 |
| --- | --- |
| `image.width` | 像素宽 |
| `image.height` | 像素高 |
| `image.mode` | RGB、RGBA、L 等 |
| `image.format` | JPEG、PNG、WEBP 等 |
| `image.decode_ok` | 是否可解码 |
| `image.error` | 解码错误 |

视频字段：

| 字段 | 说明 |
| --- | --- |
| `video.width` | 源视频宽 |
| `video.height` | 源视频高 |
| `video.framerate` | fps |
| `video.num_frames` | 帧数 |
| `video.duration_s` | 时长 |
| `video.video_codec` | 视频编码 |
| `video.audio_codec` | 音频编码，可为空 |
| `video.probe_ok` | 是否可探测 |
| `video.error` | 探测错误 |

### 5.3 Text Normalize

字段：

| 字段 | 说明 |
| --- | --- |
| `text_normalized` | Unicode 规范化、去控制字符、压缩空白后的文本 |
| `text_length` | 字符数 |
| `text_language` | M1 可填 `unknown`，后续接语言识别 |
| `text_error` | 空文本、过短、异常字符等 |

### 5.4 Hash And Dedup Marking

字段：

| 字段 | 说明 |
| --- | --- |
| `content_sha256` | 原始媒体字节 hash |
| `text_sha256` | 归一化文本 hash |
| `phash` | 图片感知 hash，用于近重复候选 |
| `duplicate_of` | 若当前样本是重复样本，指向保留样本 ID |
| `dedup_scope` | `within_run`、`global`，M1 先做 `within_run` |

### 5.5 Quality Filter

统一判断结果：

| 字段 | 说明 |
| --- | --- |
| `keep` | 是否进入训练候选 |
| `reject_reasons` | 拒绝原因数组 |
| `quality_scores` | 分数字典 |
| `stage_status` | 各阶段状态字典 |

M1 最小规则：

- 文件必须存在且可读。
- 图片必须可解码，最短边不小于 128，宽高比在 `[0.2, 5.0]`。
- 视频必须可 probe，时长不小于 2 秒，最长边不超过本轮配置上限。
- 文本不能为空，归一化后长度不小于 3。
- 原始字节重复样本只保留第一个。
- 图片必须经过 pHash、CLIP embedding、aesthetic、NSFW 阶段；缺少模型或 GPU 时记录阻塞状态，不将该结果伪装为通过。
- 视频 clip 必须经过 motion、aesthetic、Cosmos embedding、caption 阶段；缺少模型或 GPU 时记录阻塞状态，不将该结果伪装为通过。

## 6. 输出布局

### 6.1 工作目录

```text
data/output/
└── pipeline_version=v1/
    └── run_id=<run_id>/
        ├── curated/
        │   ├── images/
        │   │   ├── images-<hash>-000000.tar
        │   │   └── images-<hash>-000000.parquet
        │   └── videos/
        │       ├── clips/
        │       ├── filtered_clips/
        │       ├── metas/v0/
        │       ├── ce1_embd_parquet/
        │       ├── processed_videos/
        │       └── processed_clip_chunks/
        ├── manifests/
        │   ├── training_manifest.jsonl
        │   ├── rejected_manifest.jsonl
        │   └── source_manifest.normalized.jsonl
        ├── reports/
        │   ├── run_report.json
        │   └── conservation_report.json
        └── commit.json
```

### 6.2 Training Manifest

`training_manifest.jsonl` 是下游多模态预训练读取的主入口。图片每个保留图片一行，视频每个保留 clip 一行。

通用字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `training_sample_id` | string | 训练样本稳定 ID |
| `parent_sample_id` | string | 原始输入样本 ID |
| `dataset_name` | string | 数据集名 |
| `dataset_version` | string | 输入数据版本 |
| `pipeline_version` | string | 处理版本 |
| `media_type` | string | `image` 或 `video_clip` |
| `media_uri` | string | 训练时读取的媒体路径 |
| `text` | string | 归一化后的 caption 或描述 |
| `source` | string | 来源 |
| `license` | string | 许可 |
| `quality_scores` | object | 质量分数 |
| `lineage` | object | 输入 URI、hash、shard、处理阶段 |

图片字段：

| 字段 | 说明 |
| --- | --- |
| `image.width` | 输出图片宽 |
| `image.height` | 输出图片高 |
| `image.format` | 输出格式，M1 建议统一 JPEG |
| `image.tar_file` | 图片 tar shard 路径 |
| `image.member_name` | tar 内文件名 |

视频 clip 字段：

| 字段 | 说明 |
| --- | --- |
| `video.source_uri` | 原始视频路径 |
| `video.clip_uri` | clip mp4 路径 |
| `video.start_time_s` | clip 起始秒 |
| `video.end_time_s` | clip 结束秒 |
| `video.duration_s` | clip 时长 |
| `video.width` | clip 宽 |
| `video.height` | clip 高 |
| `video.framerate` | fps |
| `video.metadata_uri` | clip JSON metadata 路径 |

示例：

```json
{"training_sample_id":"img_000001","parent_sample_id":"img_000001","dataset_name":"m1_smoke","dataset_version":"2026-08-31","pipeline_version":"v1","media_type":"image","media_uri":"data/output/pipeline_version=v1/run_id=local/curated/images/images-abcd-000000.tar#img_000001.jpg","text":"A red chair beside a window.","source":"local_seed","license":"unknown","quality_scores":{"basic":1.0},"lineage":{"input_uri":"data/samples/images/img_000001.jpg","content_sha256":"..."},"image":{"width":640,"height":480,"format":"JPEG","tar_file":"data/output/pipeline_version=v1/run_id=local/curated/images/images-abcd-000000.tar","member_name":"img_000001.jpg"}}
{"training_sample_id":"clip_a1b2c3d4e5f60708","parent_sample_id":"vid_000001","dataset_name":"m1_smoke","dataset_version":"2026-08-31","pipeline_version":"v1","media_type":"video_clip","media_uri":"data/output/pipeline_version=v1/run_id=local/curated/videos/clips/a1/clip_a1b2c3d4e5f60708.mp4","text":"A short clip of traffic moving through an intersection.","source":"local_seed","license":"unknown","quality_scores":{"basic":1.0},"lineage":{"input_uri":"data/samples/videos/vid_000001.mp4","content_sha256":"..."},"video":{"source_uri":"data/samples/videos/vid_000001.mp4","clip_uri":"data/output/pipeline_version=v1/run_id=local/curated/videos/clips/a1/clip_a1b2c3d4e5f60708.mp4","start_time_s":0.0,"end_time_s":5.0,"duration_s":5.0,"width":1280,"height":720,"framerate":30.0,"metadata_uri":"data/output/pipeline_version=v1/run_id=local/curated/videos/metas/v0/clip_a1b2c3d4e5f60708.json"}}
```

### 6.3 Rejected Manifest

`rejected_manifest.jsonl` 每行记录一个被拒绝的原始样本或派生 clip。

字段：

| 字段 | 说明 |
| --- | --- |
| `rejected_id` | 被拒绝对象 ID，图片为 `sample_id`，视频 clip 为 `clip_id` |
| `parent_sample_id` | 原始输入样本 ID |
| `media_type` | `image`、`video` 或 `video_clip` |
| `uri` | 原始或派生媒体路径 |
| `reject_reasons` | 拒绝原因数组 |
| `stage_status` | 阶段状态 |
| `error` | 具体错误信息，可为空 |

标准原因值：

```text
missing_file
read_error
decode_error
probe_error
empty_text
text_too_short
image_too_small
image_bad_aspect_ratio
video_too_short
duplicate_content
writer_error
```

### 6.4 Run Report

`run_report.json` 汇总本次运行：

```json
{
  "run_id": "local",
  "pipeline_version": "v1",
  "input": {
    "manifest": "data/manifests/dataset_manifest.jsonl",
    "records": 42
  },
  "output": {
    "training_records": 35,
    "rejected_records": 7,
    "image_records": 30,
    "video_clip_records": 5
  },
  "reject_reason_counts": {
    "duplicate_content": 2,
    "decode_error": 1,
    "probe_error": 1,
    "image_too_small": 2,
    "empty_text": 1
  }
}
```

`conservation_report.json` 必须证明：

- `input image samples = kept image records + rejected image records`，重复样本计入 rejected。
- `input video samples = probed videos + rejected source videos`。
- `generated clips = kept video_clip records + rejected video_clip records`。
- writer 输出的 tar/parquet/json/mp4 文件都能被 manifest 引用。

### 6.5 Commit

`commit.json` 是本次输出可用的唯一完成标记。没有 `commit.json` 的 run 目录视为未完成。

```json
{
  "run_id": "local",
  "dataset_name": "m1_smoke",
  "dataset_version": "2026-08-31",
  "pipeline_version": "v1",
  "status": "committed",
  "created_at": "2026-08-31T00:00:00Z",
  "manifests": {
    "training": "manifests/training_manifest.jsonl",
    "rejected": "manifests/rejected_manifest.jsonl"
  },
  "reports": {
    "run": "reports/run_report.json",
    "conservation": "reports/conservation_report.json"
  }
}
```

## 7. M1 验收标准

M1 闭环完成的判断标准：

1. 能从 `dataset_manifest.jsonl` 读取混合图片和视频输入。
2. 能产出 `training_manifest.jsonl`、`rejected_manifest.jsonl`、`run_report.json`、`conservation_report.json` 和 `commit.json`。
3. 图片保留样本被写入可复用的 tar + metadata 文件，字段对齐 Curator `ImageWriterStage` 的输出语义。
4. 视频保留样本以 clip 为训练粒度，输出路径和 metadata 布局对齐 Curator `ClipWriterStage` 的目录语义。
5. 坏图、坏视频、重复样本、空文本样本不会静默丢弃，必须进入 rejected manifest。
6. pHash、CLIP、aesthetic、NSFW、video motion、video embedding、caption 等正式算子必须出现在执行计划和报告中。
7. 同一输入重复运行两次，训练样本 ID、拒绝原因和输出 manifest 内容稳定。
8. pipeline 中不持久化全量解码图片或全量视频帧，只持久化最终媒体、metadata、manifest、report。

## 8. 下一步实现切分

建议按以下顺序实现：

1. `scripts/data/build_smoke_dataset.py`：生成最小图片/视频样本和 `dataset_manifest.jsonl`。
2. `src/curator_flow/contracts.py`：定义 manifest、quality metadata、report 的 dataclass 或 Pydantic model。
3. `src/curator_flow/pipelines/image_curator_smoke.py`：直接用 Curator 图片 Stage 跑 smoke 图片链路。
4. `src/curator_flow/pipelines/video_curator_smoke.py`：直接用 Curator 视频 Stage 跑 smoke 视频链路。
5. `src/curator_flow/operators/phash.py`：补 Curator 当前缺少的图片 pHash Stage。
6. `src/curator_flow/pipelines/build_training_manifest.py`：汇总 Curator 输出为训练主索引、拒绝清单、报告和 commit。
7. `tests/`：验证坏样本进入 rejected、重复样本稳定、二次运行输出一致。

