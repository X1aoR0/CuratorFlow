# 1. 实验环境和数据准备

## 目标

本阶段目标是把 CuratorFlow 的视频预训练语料处理实验跑到可验证状态：

1. 使用 NeMo Curator 原生视频 pipeline，不重新实现已有成熟算子。
2. 在 GPU 节点完成最小闭环验证，包括视频读取、切片、转码、caption 准备、caption 生成、写出。
3. 形成后续 Ray 集群实验所需的基础环境、样本数据和模型权重。

## 机器角色

当前集群规划是 3 台 CPU ECS 加 1 台 GPU ECS：

| 节点 | 角色 | IP |
| --- | --- | --- |
| ECS1 | 公网下载中转、Ray CPU 节点 | `8.145.50.233` / `192.168.0.208` |
| ECS2 | Ray CPU 节点 | `8.130.134.116` |
| ECS3 | Ray CPU 节点 | `8.130.191.173` |
| GPU1 | Ray GPU 节点、caption 推理 | `192.168.0.211` |

GPU 节点没有公网下载能力，因此大文件按下面的方式处理：

```text
公网资源 -> ECS1 -> 内网 rsync -> GPU1
```

其中 ECS1 负责下载和中转，GPU1 负责 CUDA 相关安装、模型加载和推理。

## Curator 依赖准备

CuratorFlow 当前目标是直接使用 Curator 的 video pipeline：

```text
FilePartitioningStage
-> VideoReaderStage
-> FixedStrideExtractorStage
-> ClipTranscodingStage
-> CaptionPreparationStage
-> CaptionGenerationStage
-> ClipWriterStage
```

依赖来自 Curator 的 `video_cuda12` extra。它包含几类包：

| 依赖类别 | 代表包 | 用途 |
| --- | --- | --- |
| 视频处理 | `av`, `opencv-python-headless`, `cvcuda-cu12` | 读视频、帧处理、GPU/CPU 视频处理 |
| 转码/系统工具 | `ffmpeg`, `ffprobe` | Curator 视频读取和 clip 转码依赖 |
| GPU 推理 | `torch==2.11.0+cu129`, `vllm==0.22.0+cu129` | 运行 Qwen2.5-VL caption 模型 |
| CUDA 用户态库 | `nvidia-cudnn-cu12`, `nvidia-cublas-cu12`, `nvidia-nccl-cu12` 等 | PyTorch/vLLM 运行时依赖，不是 GPU 驱动 |
| JIT/kernel | `flashinfer`, `triton`, `tokenspeed-triton` | vLLM 推理加速 |
| CUDA Python | `pycuda` | Curator CUDA 相关阶段依赖，需要在有 CUDA headers 的 GPU 节点编译 |

关键结论：

- ECS1 不需要安装 CUDA driver，也不应该在 ECS1 编译 CUDA 扩展。
- ECS1 可以下载 Python 包和模型权重。
- `pycuda` 必须在 GPU1 上编译，因为它需要 `cuda.h` 和 CUDA Toolkit。

## PyPI 镜像处理

一开始 `uv.lock` 中 PyPI 相关包锁定到了：

```text
https://pypi.org/simple
https://files.pythonhosted.org/packages/...
```

ECS1 到 `files.pythonhosted.org` 下载大 wheel 很慢，`nvidia-cudnn-cu12` 的 range 测试只有约几十 KB/s。切换到阿里云镜像后，速度明显变好。

实际处理方式：

```text
/data/Curator/pyproject.toml
/data/Curator/uv.lock
```

这两个部署用文件里的 PyPI 源被直接替换为：

```text
https://mirrors.aliyun.com/pypi/simple
https://mirrors.aliyun.com/pypi/packages/...
```

同时保留原始备份：

```text
/data/Curator/pyproject.toml.orig
/data/Curator/uv.lock.orig
```

注意：PyTorch 和 vLLM 的 CUDA 专用 wheel 源没有替换：

```text
https://download.pytorch.org/whl/cu129
https://wheels.vllm.ai/0.22.0/cu129
```

因为 `torch==2.11.0+cu129` 和 `vllm==0.22.0+cu129` 需要这些专用索引。

## GPU 运行环境

GPU1 硬件和系统情况：

```text
GPU: NVIDIA L20
GPU memory: 46068 MiB
Driver: 580.126.09
CUDA Toolkit: /usr/local/cuda-12.8, /usr/local/cuda-13.0
OS: Ubuntu 22.04
Root disk: 148G, smoke 后约 79G 可用
```

Python/Curator 环境：

```text
/opt/curator-runtime/uv-python
/opt/curator-runtime/venv
/opt/Curator
```

GPU 环境验证结果：

```text
torch OK 2.11.0+cu129
vllm OK 0.22.0
nemo_curator OK 1.3.0
pycuda.driver OK
cuda_available True
cuda_count 1
cuda_name NVIDIA L20
pycuda_device_count 1
pycuda_device_name NVIDIA L20
```

GPU 节点额外安装了系统级依赖：

```text
ffmpeg
ffprobe
```

它们用于 Curator 的 `VideoReaderStage` 和 `ClipTranscodingStage`。

## 视频样本数据

实验数据来自：

```text
repo: mvp-lab/LLaVA-OneVision-2-Data
shard: mid_training_video/60s_rest/train_00480_of_10809.tar
size: 3,233,167,360 bytes
```

ECS1 通过 `hf-mirror.com` 下载该 shard，下载完成后校验字节数，并从 WebDataset tar 中安全抽取前 8 个视频。

准备结果：

```text
video_count: 8
video_bytes: 179,791,041
```

ECS1 路径：

```text
/data/datasets/llava-onevision-2/shard/mid_training_video/60s_rest/train_00480_of_10809.tar
/data/datasets/llava-onevision-2/videos
/data/datasets/llava-onevision-2/prepare-report.json
```

GPU1 路径：

```text
/data/datasets/llava-onevision-2/videos
```

## Qwen2.5-VL 模型

Curator 的 `qwen2.5` video caption 使用：

```text
model_id: Qwen/Qwen2.5-VL-7B-Instruct
revision: cc59489
```

模型总大小：

```text
16 files
16,595,981,281 bytes
```

主要权重文件：

```text
model-00001-of-00005.safetensors  3,900,233,256 bytes
model-00002-of-00005.safetensors  3,864,726,320 bytes
model-00003-of-00005.safetensors  3,864,726,424 bytes
model-00004-of-00005.safetensors  3,864,733,680 bytes
model-00005-of-00005.safetensors  1,089,994,880 bytes
```

下载和传输方式：

```text
ECS1: curl -L --fail --retry 5 从 hf-mirror.com 单文件下载
ECS1: rsync 单文件到 GPU1
ECS1: 校验 GPU 文件大小
ECS1: 删除本地临时文件
```

这样 ECS1 不需要一次性容纳完整 16.6GB 模型，只需要容纳当前正在下载的最大单文件。

GPU1 模型路径：

```text
/opt/curator-flow/models/Qwen/Qwen2.5-VL-7B-Instruct
```

模型完整性校验结果：

```text
file_count: 16
total_bytes: 16,595,981,281
missing: []
bad: []
```

## 已完成验证

### 1. 无 caption 视频 smoke

命令入口：

```bash
python -m curator_flow.run_video_pipelines \
  --input-path /data/datasets/llava-onevision-2/videos \
  --output-path /data/curator-flow-work/video-smoke-gpu-no-caption-20260904-123107 \
  --report-path /data/curator-flow-work/video-smoke-gpu-no-caption-report.json \
  --executor xenna \
  --execution-mode batch \
  --video-limit 1 \
  --clip-len-s 10 \
  --clip-stride-s 10 \
  --limit-clips 2 \
  --transcode-encoder libvpx-vp9 \
  --transcode-cpus-per-worker 2 \
  --transcode-batch-size 1 \
  --verbose
```

结果：

```text
status: succeeded
input_files: 8
input_bytes: 179,791,041
output_tasks: 1
clip_files: 6
metadata_files: 6
wall_time_s: 259.88
```

输出路径：

```text
/data/curator-flow-work/video-smoke-gpu-no-caption-20260904-123107
/data/curator-flow-work/video-smoke-gpu-no-caption-report.json
```

### 2. 带 caption 视频 smoke

命令入口：

```bash
cd /opt/curator-flow/models

PYTHONPATH=/opt/curator-flow/src \
PATH=/opt/curator-runtime/venv/bin:/usr/local/cuda-12.8/bin:$PATH \
CUDA_HOME=/usr/local/cuda-12.8 \
LD_LIBRARY_PATH=/usr/local/cuda-12.8/lib64:$LD_LIBRARY_PATH \
HF_HUB_OFFLINE=1 \
TRANSFORMERS_OFFLINE=1 \
/opt/curator-runtime/venv/bin/python -m curator_flow.run_video_pipelines \
  --input-path /data/curator-flow-work/video-smoke-gpu-no-caption-20260904-123107/clips \
  --output-path /data/curator-flow-work/video-smoke-gpu-caption-20260904-143140 \
  --report-path /data/curator-flow-work/video-smoke-gpu-caption-report.json \
  --model-dir /opt/curator-flow/models \
  --executor xenna \
  --execution-mode batch \
  --video-limit 1 \
  --clip-len-s 10 \
  --clip-stride-s 10 \
  --limit-clips 1 \
  --transcode-encoder libvpx-vp9 \
  --transcode-cpus-per-worker 2 \
  --transcode-batch-size 1 \
  --generate-captions \
  --caption-model qwen2.5 \
  --caption-batch-size 1 \
  --caption-max-output-tokens 64 \
  --verbose
```

结果：

```text
status: succeeded
wall_time_s: 180.33
caption_preparation_process_time_sum: 1.27s
caption_generation_process_time_sum: 113.36s
clip_transcoding_process_time_sum: 35.05s
```

生成示例 caption：

```text
The video begins with a close-up shot of a person's hands as they carefully place a piece of paper with the text "Carve Tools" written on it onto a wooden surface...
```

输出路径：

```text
/data/curator-flow-work/video-smoke-gpu-caption-20260904-143140
/data/curator-flow-work/video-smoke-gpu-caption-report.json
```

## 当前注意点

1. ECS1 的 `/data` 空间偏紧，后续跑更大实验前应该清理中转目录。
2. Curator 部署目录 `/data/Curator` 和 GPU 的 `/opt/Curator` 已改为阿里云 PyPI 镜像；这属于部署环境修改，不是 CuratorFlow 仓库代码修改。
3. `CaptionPreparationStage` 会用 `AutoProcessor.from_pretrained("Qwen/Qwen2.5-VL-7B-Instruct")`，因此运行 caption smoke 时需要从 `/opt/curator-flow/models` 作为当前目录启动，或保证 Hugging Face cache 中已有对应模型。
4. `PATH` 必须包含 `/opt/curator-runtime/venv/bin`，否则 vLLM/FlashInfer JIT 找不到 `ninja`。
5. 这次 smoke 还只是单 GPU 单机验证，下一步才是把 GPU 节点加入 3 台 CPU ECS 组成 Ray 集群，验证跨节点调度。
