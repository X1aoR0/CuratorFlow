# CuratorFlow Pipeline Plan

CuratorFlow 不重新实现 NeMo Curator 的媒体处理算子。项目只负责 Pipeline 组装、实验配置、运行指标、产物校验和分布式验证。

单个物理 Video shard 的递进实验、指标与验收标准见 [video_shard_experiment.md](video_shard_experiment.md)。

## Video Pipeline

第一条主链路直接使用 Curator Stage：

```text
FilePartitioningStage
  -> VideoReaderStage
  -> FixedStrideExtractorStage
  -> ClipTranscodingStage
  -> CaptionPreparationStage
  -> CaptionGenerationStage
  -> ClipWriterStage
```

`VideoReader` 是 Curator 的 CompositeStage，内部正好分解为前两个 Stage。项目中显式展开它们，便于观察各 Stage 的耗时。

| Stage | 输入 | 输出 | 资源 |
| --- | --- | --- | --- |
| `FilePartitioningStage` | 视频目录 | 每个视频一个 `FileGroupTask` | CPU |
| `VideoReaderStage` | 视频路径 | `VideoTask`，含 `source_bytes` 和 metadata | CPU |
| `FixedStrideExtractorStage` | `VideoTask` | 确定性 clip spans | CPU |
| `ClipTranscodingStage` | source bytes + spans | 每个 `Clip.buffer` | CPU VP9 或 GPU NVENC |
| `CaptionPreparationStage` | clip buffer | windows + VLM inputs | 预处理 |
| `CaptionGenerationStage` | VLM inputs | `window.caption` | 1 GPU |
| `ClipWriterStage` | clips + captions + metadata | MP4、JSON、统计 | CPU |

三台 CPU ECS 负责读取、切片、VP9 转码、写出和观测，新增 GPU Worker 负责 caption。服务端环境配置完成后，在同一入口开启 `--generate-captions` 验证完整链路。无 CUDA GPU 时 preflight 会明确失败，不会把未运行 caption 当成成功。

## Dataset

实验数据参考 `mvp-lab/LLaVA-OneVision-2-Data`：

- `mid_training_video/60s_rest/` 包含 10,809 个约 60 秒视频 WebDataset shard；
- 单 shard 约 3.2 GB；
- Curator `VideoReaderStage` 读取单视频文件，因此准备脚本下载一个明确 shard，再有界解包若干视频。

```bash
python scripts/data/prepare_llava_video_shard.py \
  --download-root /data/llava-onevision/shards \
  --video-root /data/llava-onevision/videos \
  --report-path /data/llava-onevision/prepare-report.json \
  --max-videos 8
```

默认只下载 `mid_training_video/60s_rest/train_00480_of_10809.tar`。脚本启动前检查磁盘余量，并安全解包，避免路径穿越和无界展开。

## Observation

每次运行写一个 JSON report，记录：

- Pipeline 参数与 Stage 顺序；
- 主机、Python、CPU、ffmpeg/ffprobe；
- 输入视频数和字节数；
- wall time、视频吞吐、clip 吞吐；
- Curator `_stage_perf` 聚合指标；
- 输出 MP4、metadata、有效 clip 和 caption window 数量。

单机 CPU 运行：

```bash
python -m curator_flow.run_video_pipelines \
  --input-path /data/videos \
  --output-path /data/experiments/video-cpu/output \
  --report-path /data/experiments/video-cpu/report.json \
  --executor xenna \
  --execution-mode batch \
  --transcode-encoder libvpx-vp9
```

4 vCPU 单节点在 75% 配额下只有 3 CPU 可用，而 CPU 链路 streaming 同时请求 4.75 CPU，因此单机 smoke 使用 batch；三节点集群再验证 streaming。

## Image Pipeline

第二条链路暂不实施，边界固定为 Curator 原生 Stage：

```text
FilePartitioningStage
  -> ImageReaderStage
  -> ImageEmbeddingStage
  -> ImageAestheticFilterStage
  -> ImageNSFWFilterStage
  -> ImageWriterStage
```

Video Pipeline 的单机、GPU、集群和观测闭环稳定后，再按同一实验框架接入图片链路。
