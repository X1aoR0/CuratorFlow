# 4. Caption Scale-out：四 GPU 环境快速迁移准备

## 实际部署结果

四个 GPU 节点已经完成运行环境迁移并加入 Ray：

| 节点 | 内网 IP | CPU | GPU | 环境状态 |
| --- | --- | ---: | --- | --- |
| GPU1 | `192.168.0.211` | 8 | 1×NVIDIA L20 46GB | 黄金源 |
| GPU2 | `192.168.0.212` | 8 | 1×NVIDIA L20 46GB | 已从 GPU1 内网迁移并验收 |
| GPU3 | `192.168.0.213` | 8 | 1×NVIDIA L20 46GB | 已从 GPU1 内网迁移并验收 |
| GPU4 | `192.168.0.214` | 8 | 1×NVIDIA L20 46GB | 已从 GPU1 内网迁移并验收 |

三台新节点均已具备：

```text
Ubuntu 22.04.5
NVIDIA driver 580.126.09
CUDA /usr/local/cuda-12.8
Python 3.12.14
torch 2.11.0+cu129
Ray 2.57.0
vLLM 0.22.0
NeMo Curator 1.3.0
ffmpeg / ffprobe
Qwen2_5_VLProcessor 离线加载
NFS /mnt/curator-flow 持久挂载
```

Ray 集群实测：

```text
ALIVE nodes: 7
CPU: 44
GPU: 4
object store: 82.31 GiB
```

4 个 Ray `num_gpus=1` 探针任务分别调度到 `.211`、`.212`、`.213`、`.214`，每台均确认 `torch.cuda.is_available() == True` 且设备为 NVIDIA L20。

正式运行前仍需修改 Pipeline：当前 CaptionGeneration 配置固定 `num_workers=1`、`gpus=0.99`，不会自动使用四张卡。

## 目标

在 GPU ECS 开始按小时计费后，用尽可能短且可预测的时间完成下面几件事：

1. 以已经验证过的 GPU1 为黄金节点，将完全相同的运行环境复制到另外 3 台 GPU ECS。
2. 4 台 GPU 节点加入现有 Ray 集群，Ray 能看到恰好 4 张 GPU。
3. CaptionGeneration 启动 4 个 actor，每张 GPU 恰好承载一个 vLLM/Qwen2.5-VL 实例。
4. 先用很小的输入验证四卡并行，再对完整 232 个视频从头运行一轮全 Pipeline。
5. 全程记录 Stage 吞吐、actor 数、GPU 利用率和产物守恒；完成后立即停止或释放昂贵 GPU 实例。

本篇是开机前的操作手册和验收标准。当前不启动 GPU 实例，也不执行有计费影响的云资源操作。

## 目标拓扑

```text
                         Ray head / driver
                        ECS1 192.168.0.208
                                 │
       ┌─────────────────────────┼──────────────────────────┐
       │                         │                          │
   CPU workers               NFS server               GPU workers
   ECS1/ECS2/ECS3       ECS2 192.168.0.209       GPU1/GPU2/GPU3/GPU4
                                                     每机 1×L20
                                                     每机 1×vLLM actor
```

固定共享路径：

```text
/mnt/curator-flow/input          232 个原始视频
/mnt/curator-flow/output         运行产物和日志
/mnt/curator-flow/repo           Driver 使用的代码
/mnt/curator-flow/models         四个 GPU actor 共用的一份模型文件
/mnt/curator-flow/hf-cache       Processor 配置的共享离线 cache
```

每个 GPU 节点保持相同本地路径：

```text
/opt/curator-runtime             Python 3.12.14 + venv
/opt/Curator                     NeMo Curator 1.3.0 源码/可编辑安装目标
/usr/local/cuda-12.8             CUDA Toolkit
```

所有节点必须使用相同路径，因为 venv 脚本、editable install 和部分缓存可能包含绝对路径。

## 迁移策略：全部走内网传输

本次不制作自定义镜像。4 台 GPU ECS 创建时选择与 GPU1 相同的阿里云 GPU/CUDA 基础镜像，然后把应用层环境从 GPU1 通过 VPC 内网复制过去。

需要区分两类内容：

| 层次 | 来源 | 处理方式 |
| --- | --- | --- |
| NVIDIA 驱动、内核模块、基础 CUDA | 阿里云 GPU 基础镜像 | 创建实例时选相同镜像，不能直接复制 GPU1 的内核驱动 |
| Python、Curator、vLLM | GPU1 黄金节点 | 通过内网复制到相同绝对路径 |
| ffmpeg、ffprobe、ninja、rsync、zstd、NFS client | GPU1/ECS1 准备的离线系统包，或基础镜像自带 | 新节点联网前不现场探索依赖 |
| 模型、Processor cache、输入、输出、repo | ECS2 NFS | 所有节点共用 `/mnt/curator-flow` 中的一份文件 |

内网需要传输的主要内容：

```text
/opt/curator-runtime
/opt/Curator
```

runtime 的准确大小要在 GPU1 恢复后用 `du -sb` 记录。模型不复制到每台 GPU：四个 vLLM actor 都从 `/mnt/curator-flow/models/Qwen/Qwen2.5-VL-7B-Instruct` 读取同一份 16.6 GB 权重文件。它们仍会分别把权重加载进各自 GPU 显存，这是数据并行所必需的，但磁盘上只有一份。

四 actor 同时冷启动会并发读取 NFS，理论读取量约为 `4 × 16.6 GB`。这不会额外占用 NFS 磁盘容量，但可能形成启动 I/O 峰值。因此 scale smoke 必须记录四个 actor 的模型加载时间；必要时错开 actor 启动，但不把模型复制到本地盘。

### 二叉并行分发

为了避免 GPU1 同时向 3 台机器发送大量 runtime 小文件、把单机出口和 inode 扫描打满，使用两轮分发：

```text
第一轮：GPU1 ───────────────> GPU2

第二轮：GPU1 ───────────────> GPU3
        GPU2 ───────────────> GPU4
```

第二轮两条链路并行。这样每份环境只复制两轮，且源端压力分散。若实测 GPU1 单机并发出口足够高，也可以直接并行向 GPU2–GPU4 分发；以 1 GiB 实际文件测试结果决定，不凭标称带宽猜测。

所有 GPU 节点之间需要提前具备 SSH key 信任，只允许内网 IP 访问；不要复制私钥、密码或云 API 凭据。

## 开 GPU 前必须完成的工作

这些工作都可以只用现有 CPU ECS 和共享 NFS 完成，不应占用 GPU 计费时间。

### 1. 冻结完整输入和新输出目录

这次 scale-out 的目标是观察完整 Pipeline 在四 GPU 下的调度、扩缩和吞吐，因此输入使用原始全部 232 个视频，而不是只补剩余 141 个：

```text
input videos              232
expected clips            1392
expected caption windows  approximately 1494
```

上一轮的 91 个完成结果保留作单卡基线，但不与新实验混写。四卡实验必须使用全新输出目录，否则 Writer 可能跳过已有文件，破坏吞吐和完成性观测：

```text
/mnt/curator-flow/output/caption-scale-4gpu-full-<timestamp>
```

开机前校验 `/mnt/curator-flow/input` 仍有 232 个 MP4，并记录文件数、总字节数和路径清单 checksum。不要删除或移动上一轮产物。

### 2. 固化黄金环境清单

GPU1 恢复后，开始分发前将下面信息写入共享 NFS：

```text
/mnt/curator-flow/bootstrap/gpu-golden/environment.txt
/mnt/curator-flow/bootstrap/gpu-golden/pip-freeze.txt
/mnt/curator-flow/bootstrap/gpu-golden/model-sha256.txt
/mnt/curator-flow/bootstrap/gpu-golden/smoke-result.json
```

环境清单至少记录：

```bash
uname -a
cat /etc/os-release
nvidia-smi
/usr/local/cuda-12.8/bin/nvcc --version
/opt/curator-runtime/venv/bin/python --version
/opt/curator-runtime/venv/bin/python -m pip freeze
/opt/curator-runtime/venv/bin/ray --version
ffmpeg -version
```

Python 验收值：

```text
Python       3.12.14
Ray          2.57.0
nemo_curator 1.3.0
torch        2.11.0+cu129
vllm         0.22.0
GPU          NVIDIA L20，约 46 GiB
```

模型至少校验 5 个 safetensors 分片和 `model.safetensors.index.json`。已有模型总大小为 16,595,981,281 bytes。

### 3. 提前修正四卡调度配置

当前代码是单卡恢复方案，不能直接用于四卡：

```text
CaptionPreparation  num_workers=1, GPU=0.01
CaptionGeneration   num_workers=1, GPU=0.99
```

如果不改，即使 Ray 有 4 张 GPU，也只会运行一个 CaptionGeneration actor。四卡实验前需要：

1. CaptionPreparation 恢复为纯 CPU Stage。共享 `HF_HOME` 已经能离线加载 Processor，不再需要用 0.01 GPU 强制放到 GPU1。
2. CaptionGeneration 每个 worker 请求完整 `1 GPU`。
3. CaptionGeneration 不再固定 `num_workers=1`：
   - 观测 Xenna autoscaling 时设置 `num_workers=None`；
   - 若目标是确定地使用全部四卡，可显式设置 `num_workers=4`。
4. CaptionPreparation 可让 Xenna自动扩缩，或显式使用 2–4 个 CPU workers。
5. 将 `caption_num_workers`、`logging_interval` 和 `autoscale_interval_s` 暴露为 CLI 参数，避免临时改代码。

建议四卡 scale-out 实验配置：

```text
CaptionPreparation: CPU=1, GPU=0, workers=auto
CaptionGeneration:  CPU=1, GPU=1, workers=auto（观察扩缩）或 4（最快完成）
logging_interval:   10s（scale smoke）/ 30s（正式长跑）
autoscale_interval: 30s（scale smoke）/ 60s（正式长跑）
```

### 4. 将 driver 从 GPU 节点移到 ECS1

上一次 driver 运行在 GPU1，GPU ECS 下线后 driver 也立即消失，最终 report 没有写出。四卡运行应从 ECS1/head 启动 driver：

- driver 不占用昂贵 GPU；
- 单个 GPU 节点故障时，driver 可以记录完整失败报告；
- 日志和 report 持续写入 NFS。

当前 `preflight()` 要求 driver 本地 `torch.cuda.is_available()`，因此需要在开 GPU 前改为检查 Ray 集群资源，例如确认 `ray.cluster_resources()["GPU"] >= 1`，而不是要求 ECS1 本机有 CUDA。

### 5. 提前准备网络和安全组

4 台 GPU ECS 必须在与现有集群相同的 VPC/可达网段，优先选择与 NFS 同地域、同可用区。安全组至少允许集群节点之间：

```text
TCP 22                SSH
TCP 6379              Ray GCS
TCP 8265              Ray Dashboard/API
TCP 10001             Ray Client（若使用）
TCP 10002-19999       Ray workers
TCP 2049              NFSv4
ICMP                   内网连通检查（建议）
```

最稳妥的规则是只允许同一个安全组或 `192.168.0.0/24` 内部互通，不向公网开放 Ray/NFS 端口。

提前准备 ECS1 公钥，通过实例初始化脚本或控制台密钥对加入新节点。不要在节点之间复制私钥和密码。

## 内网迁移执行流程

### 阶段 A：创建并验收四台基础 GPU ECS

从同一个阿里云 GPU/CUDA 基础镜像创建 GPU1–GPU4：

- 同地域、优先同可用区；
- 同一 VPC/安全组；
- 每台 1×L20；
- 系统盘至少能容纳 runtime、Curator、系统日志和 20 GB 余量；模型统一放在 NFS；
- 使用同一 SSH key；
- 配置费用告警、自动释放时间或至少设置人工计时器；
- 不设置 Ray 自动启动，避免节点在 head 尚未清理时加入旧 session。

创建后只做基础层验收：OS 版本、驱动、CUDA Toolkit、磁盘、内网连通和 SSH。GPU1 若恢复原实例，可直接作为黄金源；GPU2–GPU4 必须和它使用兼容的驱动/CUDA 基础环境。

### 阶段 B：冻结 GPU1 黄金源

GPU1 恢复后立即执行：

1. 确认没有遗留的 Ray、vLLM 和 Pipeline 进程。
2. 验证 `nvidia-smi`、CUDA、Python、torch、vLLM、Curator、ffmpeg。
3. 挂载 NFS，确认 input、已有 output 与共享 HF cache 可读。
4. 确认共享模型位于 `/mnt/curator-flow/models/Qwen/Qwen2.5-VL-7B-Instruct`，并验证 5 个权重分片。
5. 确认共享 `/mnt/curator-flow/hf-cache` 能按 Qwen ID 完全离线加载 Processor。
6. 修复并验证 venv 中所有 shebang 都指向 `/opt/curator-runtime/venv/bin/python`。
7. 用共享模型运行 Processor 加载和最小 vLLM/Curator smoke。
8. 记录环境清单、runtime/Curator 目录字节数与文件数，以及共享模型 checksum。

冻结后不再改 GPU1 环境，避免一边复制、一边安装导致四台内容不一致。

### 阶段 C：GPU1 → GPU2 首轮复制

先从 runtime 中选择一个较大的文件或准备一个临时测速文件：

```bash
rsync -a --info=progress2 --partial --inplace \
  /opt/curator-runtime/<test-file> \
  root@<GPU2_IP>:/opt/curator-runtime/
```

根据实际 MiB/s 估算全部复制时间。随后从 GPU1 复制：

```bash
rsync -aH --numeric-ids --delete-delay --partial \
  /opt/curator-runtime/ root@<GPU2_IP>:/opt/curator-runtime/
rsync -aH --numeric-ids --delete-delay --partial \
  /opt/Curator/ root@<GPU2_IP>:/opt/Curator/
```

说明：

- 不使用 `-z`，避免压缩 safetensors 浪费 CPU。
- `--partial` 允许网络中断后续传。
- `--delete-delay` 只对明确的四个目标目录使用；执行前必须确认目标路径，不能对 `/opt` 整体执行。
- 首轮完成后，GPU2 必须先通过完整验收，才能作为第二个分发源。

### 阶段 D：二叉并行复制 GPU3/GPU4

并行执行：

```text
GPU1 → GPU3
GPU2 → GPU4
```

使用与阶段 C 相同的 2 组 rsync 命令。完成后四台分别生成校验清单并比对：

```bash
find /opt/curator-runtime /opt/Curator \
  -type f -printf '%p %s\n' | sort | sha256sum
```

这是文件路径/大小清单 checksum，用于快速发现缺文件。runtime 内大量小文件逐个做内容 hash 会很慢，只在文件清单不一致时进一步排查。共享模型只需在 NFS 侧做一次内容 checksum。

## 新节点启动后的并行验收

拿到四个内网 IP 后，先更新：

```text
deploy/ecs_hosts.yaml
deploy/ssh_config
```

建议命名：`gpu1`、`gpu2`、`gpu3`、`gpu4`。下面所有检查应并行执行，而不是逐台串行。

### 1. 基础验收

每台必须同时满足：

```bash
nvidia-smi --query-gpu=name,memory.total,driver_version --format=csv,noheader
/opt/curator-runtime/venv/bin/python --version
/opt/curator-runtime/venv/bin/python -c \
  'import torch, ray, vllm, nemo_curator; print(torch.cuda.is_available(), torch.cuda.get_device_name(0), ray.__version__)'
ffmpeg -version
test -f /mnt/curator-flow/models/Qwen/Qwen2.5-VL-7B-Instruct/model.safetensors.index.json
```

验收标准：

- 每台只有且恰好 1 张 L20；
- torch CUDA 可用；
- Python/Ray/Curator/vLLM 版本完全一致；
- 模型 checksum 与黄金节点一致；
- GPU 系统时间差不超过数秒。

### 2. NFS 验收

推荐 fstab 使用网络安全选项：

```text
192.168.0.209:/data/curator-share /mnt/curator-flow nfs4 rw,_netdev,nofail,x-systemd.automount 0 0
```

每台执行：

```bash
mountpoint /mnt/curator-flow
test -r /mnt/curator-flow/input
test -w /mnt/curator-flow/output
test -r /mnt/curator-flow/hf-cache
```

必须验证四台能同时创建并删除各自命名的测试文件，避免只读挂载、UID、锁或 NFS 可用性问题。

### 3. 共享模型/Processor 验收

每台分别运行：

```bash
export HF_HOME=/mnt/curator-flow/hf-cache
export HF_HUB_OFFLINE=1
export TRANSFORMERS_OFFLINE=1

/opt/curator-runtime/venv/bin/python -c \
  'from nemo_curator.models.prompt_formatter import PromptFormatter; p=PromptFormatter("qwen2.5"); print(type(p.processor).__name__)'
```

期望输出：

```text
Qwen2_5_VLProcessor
```

这一步必须完全离线，日志不应出现 `huggingface.co` 请求。

## 重新建立干净 Ray 集群

旧 Ray 状态中已经存在失效 GPU node ID。正式 scale-out 前应重启集群，避免 dashboard 和 State API 混入旧节点。

### 1. CPU 节点停止旧 Ray

在 ecs1、ecs2、ecs3 分别执行：

```bash
source /data/curator-cluster/venv/bin/activate
ray stop
```

### 2. ECS1 启动 head

```bash
export RAY_MAX_LIMIT_FROM_API_SERVER=40000
export RAY_MAX_LIMIT_FROM_DATA_SOURCE=40000

ray start --head \
  --node-ip-address=192.168.0.208 \
  --port=6379 \
  --num-cpus=4 \
  --num-gpus=0 \
  --dashboard-host=0.0.0.0 \
  --disable-usage-stats
```

### 3. CPU workers 加入

ecs2、ecs3 使用各自的 `--node-ip-address`，并带相同的两个 Ray API limit 环境变量：

```bash
ray start \
  --address=192.168.0.208:6379 \
  --node-ip-address=<THIS_CPU_PRIVATE_IP> \
  --num-cpus=4 \
  --num-gpus=0
```

### 4. 四个 GPU workers 并行加入

每个 GPU 节点执行：

```bash
source /opt/curator-runtime/venv/bin/activate
export PATH=/opt/curator-runtime/venv/bin:/usr/local/cuda-12.8/bin:$PATH
export CUDA_HOME=/usr/local/cuda-12.8
export LD_LIBRARY_PATH=/usr/local/cuda-12.8/lib64:$LD_LIBRARY_PATH
export RAY_MAX_LIMIT_FROM_API_SERVER=40000
export RAY_MAX_LIMIT_FROM_DATA_SOURCE=40000
export RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1
export VLLM_USE_FLASHINFER_SAMPLER=0

ray start \
  --address=192.168.0.208:6379 \
  --node-ip-address=<THIS_GPU_PRIVATE_IP> \
  --num-cpus=<THIS_GPU_CPU_COUNT> \
  --num-gpus=1
```

`THIS_GPU_CPU_COUNT` 必须来自 `nproc`，不要盲目复制 GPU1 的 8 CPU，除非四台规格完全相同。

### 5. 集群验收

ECS1 上验收：

```bash
source /data/curator-cluster/venv/bin/activate
ray status
ray list nodes --address=http://127.0.0.1:8265 --detail
```

必须满足：

```text
ALIVE nodes = 7（3 CPU + 4 GPU）
GPU resources = 4
没有 DEAD/PENDING 节点
每个 GPU 节点资源中 GPU=1
```

## 四卡 scale smoke

正式处理完整数据前，只跑能够同时喂满 4 张卡的最小集合：建议 8 个视频，每张卡约两个任务。

验收项：

1. Ray `ALIVE` 的 CaptionGeneration actors 达到 4。
2. 四台 `nvidia-smi` 都看到一个 vLLM EngineCore。
3. 四张 GPU 都出现持续利用率，而不是只有 GPU1 工作。
4. Xenna `Stage state` 中 CaptionGeneration `Ready=4`。
5. 8 个视频应产出 48 clips、48 metadata、8 processed_videos 和 8 processed_clip_chunks；caption window 数可能因高帧率大于 48。
6. 日志没有联网下载、CUDA OOM、NFS 错误和 actor pending。

如果是验证 autoscaling，而非最快完成，应使用 `workers=auto`，并观察 CaptionGeneration：

```text
Ready 1 → 2 → 3 → 4
Pending 最终归零
Input Queue 在扩容后下降
```

当前 Xenna 版本的 `Actors: Target` 字段被硬编码为 0，不能作为扩缩容依据。以 `Ready/Pending/Running/Idle` 和 Ray ALIVE actors 为准。

## 完整 232 个视频正式运行

Driver 从 ECS1 启动，输入使用完整原始数据集，并创建全新的输出目录：

```text
input:  /mnt/curator-flow/input
model:  /mnt/curator-flow/models
output: /mnt/curator-flow/output/caption-scale-4gpu-full-<timestamp>
log:    /mnt/curator-flow/output/caption-scale-4gpu-full-<timestamp>.log
report: /mnt/curator-flow/output/caption-scale-4gpu-full-<timestamp>-report.json
```

为了和单卡基线可比较，完整四卡运行继续使用相同 Pipeline 参数：

```text
caption_max_output_tokens=256
caption_batch_size=1
sampling_fps=2
clip_len=10
stride=10
CPU libvpx-vp9
```

本轮从 FilePartitioning 到 ClipWriter 全部重新执行，用于观察四 GPU 下完整 Pipeline 的 Stage 吞吐、队列、autoscaling 和端到端 wall time。上一轮产物只作为单卡对照，不参与本轮 Writer 去重。caption 截断问题在后续独立质量实验中处理。

### 理论时间预算

单卡阶段实测约 8.3 captions/min。完整数据预计约 1494 windows：

```text
理想四卡 caption 时间 ≈ 1494 / (8.3 × 4) ≈ 45 分钟
```

考虑模型冷启动、autoscaling、转码、长输出波动和尾部排空，建议按下面的计费窗口规划：

| 阶段 | 目标时间 |
| --- | ---: |
| 四机启动、SSH、NFS、版本验收 | 5–10 分钟 |
| Ray 加入与 8 视频 scale smoke | 5–8 分钟 |
| 完整 232 视频处理 | 48–60 分钟 |
| 守恒检查、日志归档、停机 | 5 分钟 |
| 合计 | 63–83 分钟 |

这只是按上一轮吞吐的工程预算，不是 SLA。若 4 卡不能同时达到高利用率，应立即停止正式运行并排查，不要让实例空转计费。

## 运行时必须观察的指标

Xenna 日志每 10 或 30 秒采集：

- 各 Stage `Ready/Pending/Running/Idle`；
- `Tasks Completed`；
- `Input/Output Queue`；
- `Tasks/actor/s`；
- CaptionGeneration actor 数是否为 4；
- 上游转码是否能够持续供给 4 个 GPU actors。

Ray/Dashboard 观察：

- 7 个节点是否全部 ALIVE；
- 4 个 GPU actor 分别位于 4 个 node ID；
- object store 是否持续增长或 spill；
- 是否有 pending actor、lost node 或 worker restart。

每台 GPU 每 10 秒采集：

```bash
nvidia-smi --query-gpu=timestamp,index,utilization.gpu,memory.used,memory.total,power.draw \
  --format=csv,noheader
```

预期稳态：

- 4 张卡都接近持续忙碌；
- 每张卡只有一个 EngineCore；
- 显存不超过 L20 容量并在 warm-up 后趋于稳定；
- CaptionGeneration 输入队列不无限增长；
- Writer 和 NFS 没有形成新瓶颈。

## 完成后的守恒检查

本轮全新输出目录预计：

```text
processed_videos            232
processed_clip_chunks       232
clips                       1392
metas/v0                    1392
caption windows             约 1494
```

还要检查：

- 所有 metadata 都有至少一个非空 `qwen2.5_caption`；
- 完整输入的 232 个 source_video 全部能在新产物中找到；
- 本轮结果不依赖上一轮 91 个已完成标记；
- report 为 `succeeded`；
- Stage 没有 `Returned None` 或静默缺口；
- 记录 4 卡总 wall time、平均 captions/s、每卡利用率和 NFS/object store 峰值。

## 结束与费用止损

完成守恒校验后立即：

1. 将 log、report、环境清单和监控结果保存在 NFS。
2. 确认 Writer 已落盘、NFS 已同步。
3. `ray stop` 只负责停止 Ray，不会停止 ECS 计费。
4. 在阿里云控制台停止或释放 GPU2–GPU4；是否保留 GPU1 取决于后续实验计划。
5. 二次确认实例计费状态，而不是只确认 SSH 断开。

建议设置硬止损：

- 开机后预设迁移窗口内仍未形成 4 GPU ALIVE：停止实例排查网络、基础镜像或环境复制。
- scale smoke 后只有一张卡工作：停止正式运行，检查 `num_workers` 和资源声明。
- 任一 GPU OOM 或 actor 连续重启：停止正式运行，不反复自动重试烧钱。
- 正式运行超过预计上界且吞吐显著低于单卡×4：先保存状态，再决定是否继续。

## GPU 点对点复制不可用时的备用方案

如果安全组或网络策略暂时不允许 GPU 节点之间互相 SSH，则在 GPU1 上制作不含模型的 runtime bundle，并暂存到 NFS：

```text
/mnt/curator-flow/bootstrap/gpu-runtime-py312-cu128-v1.tar.zst
```

内容：

```text
/opt/curator-runtime
/opt/Curator
```

新节点并行解压到完全相同的绝对路径。模型仍由所有节点从 NFS 共用，不复制到本地。随后补装/校验系统包和 CUDA Toolkit。

这个方案必须满足：

- 相同 Ubuntu 版本和 x86_64 架构；
- 相同 NVIDIA driver/CUDA 兼容关系；
- 相同绝对路径；
- 修复并检查所有 venv shebang；
- 重新执行完整 import、CUDA、Processor 和 vLLM smoke。

runtime bundle 传输完并校验后应从 NFS 删除，释放共享空间。该方案只作为点对点复制失败时的兜底。

## 开机前最终 Go/No-Go 清单

只有下面全部满足才开始创建 4 台 GPU ECS：

- [ ] 完整 `/mnt/curator-flow/input` 已冻结并校验为 232 个视频。
- [ ] 四卡 worker 配置已实现，CaptionGeneration 不再固定 1 worker。
- [ ] ECS1 可作为 driver，不要求本地 CUDA。
- [ ] logging/autoscale interval 已可配置。
- [ ] GPU1 黄金目录已冻结并生成环境、大小和 checksum 清单。
- [ ] GPU1→GPU2 的内网测速已经达到可接受速率。
- [ ] NFS 上的共享模型和 HF Processor cache 均可完全离线加载。
- [ ] 新节点 VPC、安全组、SSH key、NFS 路径已确定。
- [ ] 四台实例规格、地域、可用区和系统盘容量一致。
- [ ] 8 视频四卡 smoke 输入已准备。
- [ ] 正式输出目录、日志、report 名称已预先确定。
- [ ] 费用告警、自动释放或人工止损时间已设置。
- [ ] 一键并行验收、Ray join、监控和守恒检查命令已准备。

完成这份清单后，GPU 计费窗口内不再做依赖探索，只执行基础实例启动、内网复制、验收、join、smoke、正式运行和停机。
