# Pipeline Plan

本文档定义 CuratorFlow 的端到端 Pipeline、每个 Stage 需要实现的算子，以及最终交付给多模态大模型预训练的数据形态。

原则：

- 第一阶段先搞清并验证每个 Stage 算子，不先上三节点集群。
- 能用 Curator 原生 Stage 的地方直接用 Curator，不另造同义抽象。
- Curator 没有覆盖、但成熟预训练数据系统必须具备的能力，才在 CuratorFlow 中补自定义 Stage。
- 输入输出以 manifest 和 shard 为边界，避免 Driver 或单个 Worker 持有全量样本。
- 最终产物必须能被训练数据加载器稳定读取，而不是只生成中间分析结果。

## 1. 总体 Pipeline

CuratorFlow 的核心不是替代 Curator，而是把 Curator 的图片、视频、interleaved、dedup 能力组合成一条面向多模态预训练语料的生产链路。

```text
DatasetManifestAdapter                       (CuratorFlow)
    -> Curator native inputs
        ├── image WebDataset tar shards
        ├── video file list / directory
        └── optional interleaved parquet or WDS

Image branch:
    FilePartitioningStage                 (Curator)
    -> ImageReaderStage                   (Curator)
    -> ImageMetadataEnrichmentStage       (CuratorFlow)
    -> ImagePerceptualHashStage           (CuratorFlow)
    -> ImageEmbeddingStage                (Curator)
    -> ImageAestheticFilterStage          (Curator)
    -> ImageNSFWFilterStage               (Curator)
    -> ImageDuplicateRemovalStage         (Curator + CuratorFlow glue)
    -> ImageWriterStage                   (Curator)

Video branch:
    VideoReader / VideoReaderStage        (Curator)
    -> FixedStrideExtractorStage          (Curator)
    -> ClipTranscodingStage               (Curator)
    -> MotionVectorDecodeStage            (Curator)
    -> MotionFilterStage                  (Curator)
    -> ClipFrameExtractionStage           (Curator)
    -> ClipAestheticFilterStage           (Curator)
    -> CosmosEmbed1FrameCreationStage     (Curator)
    -> CosmosEmbed1EmbeddingStage         (Curator)
    -> CaptionPreparationStage            (Curator)
    -> CaptionGenerationStage             (Curator)
    -> CaptionEnhancementStage            (Curator)
    -> ClipWriterStage                    (Curator)

Cross-modal and dataset-level:
    TextNormalizeStage                    (CuratorFlow, later Curator text filters where useful)
    -> PairAlignmentStage                 (Curator interleaved CLIP score or custom bridge)
    -> ExactDedupWorkflow                 (Curator where applicable)
    -> Fuzzy/SemanticDedupWorkflow        (Curator where applicable)
    -> TrainingManifestWriter             (CuratorFlow)
    -> RejectedManifestWriter             (CuratorFlow)
    -> ReportWriter                       (CuratorFlow)
    -> CommitWriter                       (CuratorFlow)
```

第一轮不是“省略关键质量算子”，而是分两步验证同一套算子链：

```text
M1a: smoke dataset + Curator native readers/writers + deterministic reports
M1b: pHash / CLIP / NSFW / aesthetic / motion / caption operators all wired, with small input
```

如果模型权重或 GPU 暂时不可用，Stage 不能假装完成；要把运行状态写为 `blocked_missing_model` 或 `blocked_missing_gpu`，并在验收报告中明确。成熟系统允许分阶段上线，但不把核心质量算子从设计里删掉。

## 1.1 Curator Stage 复用边界

| 能力 | 优先实现方式 | 说明 |
| --- | --- | --- |
| 图片 tar 分片 | Curator `FilePartitioningStage` | 输入目录下 `.tar` shard |
| 图片读取/解码 | Curator `ImageReaderStage` | 产出 `ImageBatch/ImageObject` |
| 图片 CLIP embedding | Curator `ImageEmbeddingStage` | 写入 `ImageObject.embedding` |
| 图片 aesthetic | Curator `ImageAestheticFilterStage` | 依赖 image embedding |
| 图片 NSFW | Curator `ImageNSFWFilterStage` | 依赖 image embedding |
| 图片写出 | Curator `ImageWriterStage` | tar + parquet metadata |
| 图片重复删除 | Curator `ImageDuplicatesRemovalStage` | 读取 removal parquet |
| 视频读取/probe | Curator `VideoReader` / `VideoReaderStage` | 产出 `VideoTask/Video` |
| 视频 fixed stride 切片 | Curator `FixedStrideExtractorStage` | 产出 `Clip` |
| 视频转码 | Curator `ClipTranscodingStage` | 给 clip 生成 mp4 buffer |
| 视频 motion | Curator `MotionVectorDecodeStage` + `MotionFilterStage` | 过滤静态片段 |
| 视频抽帧 | Curator `ClipFrameExtractionStage` | 给 aesthetic、caption、embedding 使用 |
| 视频 aesthetic | Curator `ClipAestheticFilterStage` | 过滤低质量 clip |
| 视频 embedding | Curator `CosmosEmbed1FrameCreationStage` + `CosmosEmbed1EmbeddingStage` | 生成 clip-level embedding |
| 视频 caption | Curator `CaptionPreparationStage` + `CaptionGenerationStage` + `CaptionEnhancementStage` | 生成或增强训练文本 |
| 视频写出 | Curator `ClipWriterStage` | `clips/`、`metas/v0/`、embedding parquet |
| 图文交错 IO | Curator `InterleavedParquetReader` / `InterleavedWebdatasetReader` / writers | MINT-1T 风格数据 |
| 图文对齐过滤 | Curator `InterleavedCLIPScoreFilterStage` 或 CuratorFlow bridge | 对 interleaved 数据直接用 Curator |
| 图片 pHash | CuratorFlow `ImagePerceptualHashStage` | Curator 当前未提供通用图片 pHash stage |
| 训练主索引 | CuratorFlow `TrainingManifestWriter` | 将图片/video/interleaved 产物统一成训练入口 |
| rejected/report/commit | CuratorFlow writer | 补齐工程闭环 |

## 2. 输入是什么

CuratorFlow 的权威输入是 `dataset_manifest.jsonl`，每行代表一个原始媒体样本。它可以指向单文件，也可以指向标准 shard 内成员。

```json
{"sample_id":"curator_image_000000","dataset_name":"m1_smoke","dataset_version":"2026-08-31","media_type":"image","uri":"data/smoke/raw/image_wds/00000.tar","uri_type":"tar_member","member_name":"000000.jpg","text":"A sample JPEG image from the NeMo Curator image reader fixture.","source":"nemo_curator/tests/image_data/00000.tar","license":"Apache-2.0"}
{"sample_id":"curator_video_test_video","dataset_name":"m1_smoke","dataset_version":"2026-08-31","media_type":"video","uri":"data/smoke/raw/videos/test_video.mp4","uri_type":"local_file","text":"A short MP4 sample video from the NeMo Curator video caption fixture.","source":"nemo_curator/tests/stages/video/caption/fixtures/test_video.mp4","license":"Apache-2.0"}
```

同时保留 Curator 原生输入格式：

- 图片：WebDataset tar shard，供 Curator `FilePartitioningStage + ImageReaderStage` 使用。
- 视频：目录或显式 JSON file list，供 Curator `VideoReader` / `ClientPartitioningStage` 使用。
- 图文交错数据：后续可落到 Curator `InterleavedBatch` schema，即 `sample_id`、`position`、`modality`、`content_type`、`text_content`、`binary_content`、`source_ref`、`materialize_error`。

## 3. Stage 与算子

### 3.1 DatasetManifestReader

职责：

- 流式读取 `dataset_manifest.jsonl`。
- 保持输入顺序。
- 给每行补充 `manifest_line` 和 `run_id`。

输入：

- `manifest_path`

输出：

- `SourceRecord` 记录流。

需要实现：

- JSONL 逐行解析。
- 空行跳过。
- JSON 解析错误进入 rejected，不中断全局任务。

### 3.2 ManifestValidator

职责：

- 校验字段完整性和类型。
- 校验 `sample_id` 在本次输入中唯一。
- 校验 `media_type` 和 `uri_type` 是否支持。

输入：

- `SourceRecord`

输出：

- 合法样本继续流转。
- 非法样本进入 `rejected_manifest`，原因如 `invalid_manifest`、`duplicate_sample_id`、`unsupported_media_type`。

需要实现：

- 必填字段检查。
- `media_type in {"image", "video"}`。
- `uri_type in {"local_file", "tar_member", "s3", "oss", "http"}`，M1 只真正支持 `local_file` 和 `tar_member`。

### 3.3 MediaResolver

职责：

- 把 manifest URI 解析成可读取的本地或远端 locator。
- 对本地路径执行存在性、权限和大小检查。
- 对 tar member 检查 tar 文件和成员是否存在。

输入：

- 合法 `SourceRecord`

输出：

- `ResolvedRecord`

关键字段：

- `resolved_uri`
- `exists`
- `bytes`
- `content_sha256`
- `container_sha256`
- `read_error`

需要实现：

- 本地文件 sha256。
- tar member sha256。
- 绝对路径和相对路径统一解析。

### 3.4 TaskRouter

职责：

- 按媒体类型分流。
- 图片进入 image branch。
- 视频进入 video branch。

输入：

- `ResolvedRecord`

输出：

- `ImageCandidate`
- `VideoCandidate`

Curator 对齐：

- 图片后续可映射成 `ImageObject(image_path, image_id, metadata, image_data)`。
- 视频后续可映射成 `VideoTask(Video(input_video, source_bytes, metadata))`。

### 3.5 ImageReadDecode

职责：

- 解码图片。
- 读取宽高、格式、模式。
- 将图片统一转为 RGB。
- 不持久化解码后的全量像素，只在当前处理窗口中保留。

输入：

- `ImageCandidate`

输出：

- `ImageDecodedSample`

关键字段：

- `image.width`
- `image.height`
- `image.format`
- `image.mode`
- `image.decode_ok`
- `image.error`

实现：

- 优先使用 Curator `ImageReaderStage` 读取 WebDataset tar。
- 本地单文件或 tar member 的 smoke 适配只作为输入准备层，不作为长期主路径。
- 输出对齐 `ImageBatch/ImageObject`。

### 3.6 ImageBasicQuality

职责：

- 执行无需模型的图片质量规则。
- 给样本追加质量分和拒绝原因。

输入：

- `ImageDecodedSample`

输出：

- `ImageQualitySample`

M1 规则：

- `decode_ok=true`。
- 最短边 `>=128`。
- 宽高比在 `[0.2, 5.0]`。
- 可选检测全黑、全白、极低方差。

该 Stage 只负责硬规则，不能替代模型质量过滤。NSFW、aesthetic、CLIP 对齐必须进入后续正式 Stage。

### 3.7 ImagePerceptualHashStage

职责：

- 计算图片去重和血缘用 hash。
- 同时覆盖精确重复和近重复候选。

输入：

- `ImageQualitySample`

输出：

- 带 hash 字段的图片样本。

实现：

- `content_sha256` 使用原始图片字节。
- `phash` 使用图像像素生成感知 hash。
- 可同时生成 `dhash` 或 `ahash` 作为调试辅助。

说明：

- Curator 当前没有通用图片 pHash Stage，因此这是 CuratorFlow 必须补的自定义 Stage。
- 自定义 Stage 的输入输出要贴近 `ImageBatch/ImageObject`，后续可直接放进 Curator Pipeline。
- `content_sha256` 用于完全相同文件去重，`phash` 用于视觉近重复候选。

### 3.8 ImageEmbeddingStage

职责：

- 使用 CLIP 生成图片 embedding。
- 为 aesthetic、NSFW、语义去重、图文对齐提供特征。

输入：

- `ImageBatch`，其中每个 `ImageObject.image_data` 已经存在。

输出：

- `ImageObject.embedding`

实现：

- 直接使用 Curator `ImageEmbeddingStage`。

关键配置：

- `model_dir`
- `num_gpus_per_worker`
- `model_inference_batch_size`
- `remove_image_data`

### 3.9 ImageAestheticFilterStage

职责：

- 给图片打 aesthetic score。
- 过滤低美学质量图片。

输入：

- 已有 `ImageObject.embedding` 的 `ImageBatch`。

输出：

- `ImageObject.aesthetic_score`
- 过滤后的 `ImageBatch`

实现：

- 直接使用 Curator `ImageAestheticFilterStage`。

### 3.10 ImageNSFWFilterStage

职责：

- 给图片打 NSFW probability。
- 过滤超过阈值的图片。

输入：

- 已有 `ImageObject.embedding` 的 `ImageBatch`。

输出：

- `ImageObject.nsfw_score`
- 过滤后的 `ImageBatch`

实现：

- 直接使用 Curator `ImageNSFWFilterStage`。

### 3.11 ImageWriterStage

职责：

- 写出保留图片。
- 保留图片 ID、原始路径、质量分、hash、血缘信息。

输入：

- 过滤后的 `ImageBatch`

输出：

- Curator 标准图片产物：tar + parquet metadata。

实现：

- 直接使用 Curator `ImageWriterStage`，`deterministic_name=True`。

### 3.12 VideoProbe

职责：

- 探测视频基本元信息。
- 识别损坏视频。

输入：

- `VideoCandidate`

输出：

- `VideoProbedSample`

关键字段：

- `video.width`
- `video.height`
- `video.framerate`
- `video.num_frames`
- `video.duration_s`
- `video.video_codec`
- `video.audio_codec`
- `video.probe_ok`
- `video.error`

实现：

- 优先使用 Curator `VideoReader` / `VideoReaderStage`。
- Curator 内部通过 `Video.populate_metadata()` 提取 `VideoMetadata`。
- 必要时用 `ffprobe` 做调试和外部验证。

### 3.13 VideoClipPlanner

职责：

- 将源视频规划成训练粒度的 clip。
- M1 使用确定性 fixed stride。

输入：

- `VideoProbedSample`

输出：

- `VideoClipCandidate` 列表。

实现：

- 直接使用 Curator `FixedStrideExtractorStage`。
- 更高质量切分再接 Curator `TransNetV2ClipExtractionStage`。

基础规则：

- `clip_len_s=5`。
- `clip_stride_s=5`。
- `min_clip_length_s=2`。
- 对 3.5 秒 smoke 视频生成一个 `[0.0, 3.5]` clip。

关键字段：

- `clip_id`
- `parent_sample_id`
- `start_time_s`
- `end_time_s`
- `duration_s`

### 3.14 VideoClipTranscodingStage

职责：

- 将 planned clip 物化为可训练读取的 mp4 bytes。
- 统一编码参数，降低训练 loader 侧解码异常。

输入：

- 带 `Clip` 列表的 `VideoTask`

输出：

- `Clip.buffer`
- clip 编码元数据
- clip 级错误

实现：

- 直接使用 Curator `ClipTranscodingStage`。

### 3.15 VideoMotionFilterStage

职责：

- 过滤静止、近静止或运动信息不足的视频片段。
- 避免预训练数据中充满无效监控画面、静态 PPT、冻结帧。

输入：

- 带 `Clip.buffer` 的 `VideoTask`

输出：

- `motion_score_global_mean`
- `motion_score_per_patch_min_256`
- 过滤后的 `video.clips` 和 `video.filtered_clips`

实现：

- 直接使用 Curator `MotionVectorDecodeStage`。
- 直接使用 Curator `MotionFilterStage`。

### 3.16 VideoFrameAndAestheticStage

职责：

- 从 clip 中抽帧。
- 对 clip 做 aesthetic 过滤。

输入：

- 带 `Clip.buffer` 的 `VideoTask`

输出：

- `clip.extracted_frames`
- `clip.aesthetic_score`
- 过滤后的 clips

实现：

- 直接使用 Curator `ClipFrameExtractionStage`。
- 直接使用 Curator `ClipAestheticFilterStage`。

### 3.17 VideoEmbeddingStage

职责：

- 为每个视频 clip 生成 Cosmos-Embed1 embedding。
- 支撑视频语义检索、去重、图文/文视频对齐分析。

输入：

- 有可抽帧 clip 的 `VideoTask`

输出：

- `clip.cosmos_embed1_frames`
- `clip.cosmos_embed1_embedding`

实现：

- 直接使用 Curator `CosmosEmbed1FrameCreationStage`。
- 直接使用 Curator `CosmosEmbed1EmbeddingStage`。

### 3.18 VideoCaptionStage

职责：

- 对视频 clip 生成 caption。
- 可基于已有 caption 做增强或规范化。
- 对缺 caption 的视频数据尤其关键，因为预训练通常需要 video-text pair。

输入：

- 带 clip/window/frame 信息的 `VideoTask`

输出：

- `window.caption`
- `window.enhanced_caption`

实现：

- 直接使用 Curator `CaptionPreparationStage`。
- 直接使用 Curator `CaptionGenerationStage`。
- 直接使用 Curator `CaptionEnhancementStage`。

### 3.19 VideoClipWriter

职责：

- 写出通过质量过滤的 clip 媒体、metadata、preview 和 embedding。

输入：

- `VideoTask`

输出：

- Curator 标准视频产物。

输出布局对齐 Curator `ClipWriterStage`：
```text
curated/videos/
├── clips/
├── filtered_clips/
├── metas/v0/
├── ce1_embd_parquet/
├── processed_videos/
└── processed_clip_chunks/
```

关键字段：

- `clip_uri`
- `metadata_uri`
- `writer_status`

实现：

- 直接使用 Curator `ClipWriterStage`。

### 3.20 TextNormalize

职责：

- 清洗图片 caption、视频 caption、alt text 或人工描述。
- 该阶段对 image 和 video clip 都生效。

输入：

- 带 `text` 的 image/video/clip 样本。

输出：

- `text_normalized`
- `text_length`
- `text_sha256`
- `text_language`
- `text_error`

M1 规则：

- Unicode NFKC。
- 去控制字符。
- 多空白折叠为一个空格。
- 长度小于 3 拒绝。

增强：

- 优先复用 Curator text filters/modifiers。
- 如果字段形态不匹配，再补 CuratorFlow adapter。

### 3.21 PairAlignmentScoring

职责：

- 计算图片-文本或视频-文本语义匹配。
- 对低对齐样本给出过滤理由。

输入：

- 带 media embedding 和 text embedding 的样本。

输出：

- `alignment_score`
- `alignment_model`
- `alignment_status`

实现：

- 对 interleaved 图文样本，直接使用 Curator `InterleavedCLIPScoreFilterStage`。
- 对 image/video pair 训练 manifest，先实现 CuratorFlow bridge，将样本转成 interleaved rows 或复用 CLIP/Cosmos embedding 后计算相似度。

### 3.22 RunDedup

职责：

- 在当前 run 内做精确重复检测。
- 后续扩展为全局去重。

输入：

- 所有已解析样本。

输出：

- `duplicate_of`
- `dedup_scope`
- `dedup_status`

实现：

- 原始媒体 `content_sha256` 相同则只保留输入顺序中第一条。
- 文本 exact/fuzzy dedup 优先使用 Curator exact/fuzzy dedup workflows。
- embedding semantic dedup 优先使用 Curator `SemanticDeduplicationWorkflow`。
- 图片 pHash 结果由 CuratorFlow 自定义 Stage 生成，作为候选去重特征。

### 3.23 QualityDecision

职责：

- 汇总所有阶段状态，做最终 keep/reject 判断。
- 保证 reject reason 可解释。

输入：

- 各分支处理后的样本。

输出：

- `AcceptedTrainingSample`
- `RejectedRecord`

keep 条件：

- manifest 合法。
- 文件可读取。
- 图片 decode 成功或视频 probe 成功。
- 文本有效。
- 不重复。
- 基础媒体质量通过。
- 图片通过 CLIP/aesthetic/NSFW 阈值。
- 视频 clip 通过 motion/aesthetic/embedding/caption 状态检查。

### 3.24 TrainingManifestWriter

职责：

- 写出下游训练主入口 `training_manifest.jsonl`。
- 图片每个样本一行。
- 视频每个 clip 一行。

输入：

- `AcceptedTrainingSample`

输出：

- `data/output/pipeline_version=<version>/run_id=<run_id>/manifests/training_manifest.jsonl`

### 3.25 RejectedManifestWriter

职责：

- 写出所有拒绝样本。
- 拒绝不是异常吞掉，而是数据治理产物。

输入：

- `RejectedRecord`

输出：

- `rejected_manifest.jsonl`

### 3.26 ReportWriter

职责：

- 写统计报告和守恒报告。

输出：

- `run_report.json`
- `conservation_report.json`

必须统计：

- 输入样本数。
- 图片输入、视频输入数。
- 生成 clip 数。
- keep/reject 数。
- 每种 reject reason 数。
- 输出文件数量和字节数。

### 3.27 CommitWriter

职责：

- 最后写 `commit.json`。
- `commit.json` 存在才表示本次 run 完成。

输入：

- manifest/report 校验结果。

输出：

- `commit.json`

## 4. 最终给多模态大模型的数据是什么

最终交付物分两层。

### 4.1 训练主索引

主索引是 `training_manifest.jsonl`。训练框架优先读取这个文件。

每行是一个训练样本：

- 图片样本：`media_type=image`，指向图片 tar 中的 member。
- 视频样本：`media_type=video_clip`，指向一个已经裁好的 mp4 clip。

统一字段：

```json
{
  "training_sample_id": "string",
  "parent_sample_id": "string",
  "dataset_name": "string",
  "dataset_version": "string",
  "pipeline_version": "v1",
  "media_type": "image|video_clip",
  "media_uri": "path-or-uri",
  "text": "normalized caption or description",
  "source": "string",
  "license": "string",
  "quality_scores": {},
  "lineage": {}
}
```

这是一种“样本级索引 + 外部媒体 shard”的训练格式，适合大规模数据：manifest 小、可 shuffle，媒体按 shard 存放。

### 4.2 媒体与元数据 shard

图片训练资产：

```text
curated/images/
├── images-<hash>-000000.tar
└── images-<hash>-000000.parquet
```

- tar 保存 JPEG 图片字节。
- Parquet 保存 `image_id`、`tar_file`、`member_name`、`original_path` 和质量/血缘 metadata。
- 对齐 Curator `ImageWriterStage`。

视频训练资产：

```text
curated/videos/
├── clips/<prefix>/<clip_id>.mp4
├── metas/v0/<clip_id>.json
├── ce1_embd_parquet/*.parquet
├── processed_videos/*.json
└── processed_clip_chunks/*.json
```

- `clips/` 保存训练实际读取的视频片段。
- `metas/v0/` 保存 clip 级元数据。
- `ce1_embd_parquet/` 保存 Cosmos-Embed1 clip embedding。
- 对齐 Curator `ClipWriterStage` 语义。

### 4.3 可选 Interleaved 格式

如果后续目标是 MINT-1T 风格图文交错预训练，CuratorFlow 还应输出 Parquet 或 WDS 形式的 `InterleavedBatch`：

```text
sample_id
position
modality
content_type
text_content
binary_content
source_ref
materialize_error
```

对于当前图片/视频 pair 闭环，`training_manifest.jsonl` 是主格式；interleaved 是后续扩展格式。

## 5. 代码实现计划

### M1: Curator Smoke And Dependency Check

新增模块：

```text
src/curator_flow/
├── contracts.py
├── io/
│   ├── manifest_reader.py
│   └── media.py
├── operators/
│   ├── adapters.py
│   ├── phash.py
│   ├── text.py
│   ├── alignment.py
│   └── writers.py
└── pipelines/
    ├── image_curator_smoke.py
    ├── video_curator_smoke.py
    └── build_training_manifest.py
```

M1 CLI：

```bash
python3 scripts/data/build_smoke_dataset.py

python3 -m curator_flow.pipelines.image_curator_smoke \
  --input data/smoke/raw/image_wds \
  --output data/output/pipeline_version=v1/run_id=smoke/curated/images

python3 -m curator_flow.pipelines.video_curator_smoke \
  --input data/smoke/raw/videos \
  --input-list data/smoke/manifests/video_file_list.json \
  --output data/output/pipeline_version=v1/run_id=smoke/curated/videos

python3 -m curator_flow.pipelines.build_training_manifest \
  --manifest data/smoke/manifests/dataset_manifest.jsonl \
  --output data/output \
  --run-id smoke
```

验收：

- `data/output/pipeline_version=v1/run_id=smoke/commit.json` 存在。
- `training_manifest.jsonl` 至少包含 1 条 image 和 1 条 video_clip。
- `rejected_manifest.jsonl` 能表达坏样本和重复样本。
- 图片分支实际调用 Curator `ImageReaderStage`、`ImageEmbeddingStage`、`ImageAestheticFilterStage`、`ImageNSFWFilterStage`、`ImageWriterStage`，除非环境缺少模型/GPU，此时报告必须明确阻塞原因。
- 视频分支实际调用 Curator `VideoReader`、`FixedStrideExtractorStage`、`ClipTranscodingStage`、`MotionFilterStage`、`ClipAestheticFilterStage`、`CosmosEmbed1EmbeddingStage`、`CaptionGenerationStage`、`ClipWriterStage`，除非环境缺少模型/GPU，此时报告必须明确阻塞原因。
- 连续运行两次，manifest 内容稳定。

### M2: Image Curation Chain

目标：

- 图片分支完整接入 Curator `FilePartitioningStage`、`ImageReaderStage`、`ImageEmbeddingStage`、`ImageAestheticFilterStage`、`ImageNSFWFilterStage`、`ImageWriterStage`。
- 实现 CuratorFlow `ImagePerceptualHashStage`，将 `content_sha256`、`phash`、`dhash` 写入 `ImageObject.metadata`。
- 生成 image-level training manifest 和 rejected manifest。
- 保持输出格式与 Curator 图片 tar + parquet metadata 兼容。

### M3: Video Curation Chain

目标：

- 视频分支完整接入 Curator `VideoReader`、`FixedStrideExtractorStage`、`ClipTranscodingStage`、`MotionVectorDecodeStage`、`MotionFilterStage`、`ClipFrameExtractionStage`、`ClipAestheticFilterStage`、`CosmosEmbed1FrameCreationStage`、`CosmosEmbed1EmbeddingStage`、`CaptionPreparationStage`、`CaptionGenerationStage`、`CaptionEnhancementStage`、`ClipWriterStage`。
- 生成 video-clip-level training manifest 和 rejected manifest。
- 保持输出格式与 Curator 视频 `clips/`、`metas/v0/`、`ce1_embd_parquet/` 兼容。

### M4: Cross-Modal Alignment And Global Dedup

目标：

- 精确 hash 去重扩展到跨 run。
- pHash bucket。
- embedding 语义去重。
- 对 interleaved 图文样本复用 Curator `InterleavedCLIPScoreFilterStage`。
- 对 image/video pair 样本生成统一 alignment score。
- 输出 duplicate removal manifest。

### M5: Ray/Xenna Distributed Execution

目标：

- 将前面已验证的 Curator Stage 和少量 CuratorFlow Stage 组合到 Xenna/Ray 执行。
- 按 manifest partition 和 shard 分配任务。
- 加 checkpoint、retry、commit 协议。
- 最后再上三台 ECS 跑同一个 pipeline。
