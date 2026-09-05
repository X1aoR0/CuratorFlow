# 单 Video Shard 实验手册

## 1. 实验目标

用一个固定的物理 Video shard 跑通可复现的端到端闭环，并能准确解释三层组件各自做了什么：

- **NeMo Curator**：定义 Video Task、原生 Stage、阶段指标和输出格式。
- **Xenna**：根据 Stage 的 CPU/GPU 声明分配 Worker，并以 batch 或 streaming 模式执行。
- **Ray**：提供多节点集群、Actor 放置、对象传输、资源统计和故障可见性。

第一轮只验证这条 Curator 原生链路：

```text
FilePartitioningStage
  -> VideoReaderStage
  -> FixedStrideExtractorStage
  -> ClipTranscodingStage
  -> CaptionPreparationStage
  -> CaptionGenerationStage
  -> ClipWriterStage
```

暂不加入 motion filter、aesthetic filter 和 embedding，避免同时引入过多模型和变量。实验成功不等于“生成了几个 MP4”，而是输入身份、环境身份、执行过程、输出守恒、caption 质量和重跑行为都有证据。

## 2. 固定实验范围

### 2.1 数据集

- 数据集：`mvp-lab/LLaVA-OneVision-2-Data`
- Shard：`mid_training_video/60s_rest/train_00480_of_10809.tar`
- 最终验收必须处理该 shard 内的全部视频。
- 前置实验使用同一 shard 中确定性的前 1 条和前 8 条视频，只算 smoke，不算最终闭环。

解包前固定记录：

- shard 字节数和 SHA-256；
- tar 内普通视频成员数；
- 解包后视频数和总字节数；
- `ffprobe` 得到的每条视频时长、编码、分辨率和 FPS；
- 全部视频总时长。

### 2.2 固定 Pipeline 参数

除非某一轮明确只改变一个变量，否则保持以下参数不变：

| 参数 | 固定值 | 原因 |
| --- | ---: | --- |
| `clip_len_s` | 10 | 典型 60 秒视频约生成 6 个训练 clip |
| `clip_stride_s` | 10 | 不重叠，便于做数量守恒 |
| `min_clip_length_s` | 2 | 只丢弃不足 2 秒的尾片段 |
| encoder | `libvpx-vp9` | 把唯一 GPU 完整留给 caption |
| transcode CPUs | 2 | 能放进 4 vCPU Worker，并给系统留余量 |
| encoder threads | 1 | 避免 FFmpeg 隐式超卖 CPU |
| transcode batch | 2 | 控制内存和中间对象体积 |
| clips per chunk | 32 | 一条约 60 秒视频通常保持为一个下游 Task |
| caption model | `qwen2.5` | 对应 Curator 的 Qwen2.5-VL-7B |
| sampling FPS | 2 | Curator 默认值，VLM 输入规模可控 |
| caption batch | 1 起步 | 先建立 L20 不 OOM 的基线 |
| max output tokens | 256 | 当前项目默认值 |
| CPU allocation | 75% | 为 Ray、系统、FFmpeg 和共享存储保留资源 |

对于时长为 `D` 秒的视频，预期 fixed-stride clip 数是：

```text
0                         D < 2
floor((D - 2) / 10) + 1  D >= 2
```

完整 streaming 实验不使用 `h264_nvenc`。`ClipTranscodingStage(h264_nvenc)` 和 `CaptionGenerationStage` 各声明 1 GPU，单 L20 集群同时运行时需要 2 GPU，Xenna 应当拒绝资源不足的计划。NVENC 只留作后续 batch 对照，因为 batch 模式下两个 GPU Stage 不同时驻留。

## 3. 当前基础设施事实

2026-09-04 实测：

| 节点 | 角色 | CPU | GPU | 与实验相关的可用空间 |
| --- | --- | ---: | --- | --- |
| `ecs1` | Ray head / CPU | 4 vCPU | 无 | `/data` 6.5 GiB，根盘 16 GiB |
| `ecs2` | CPU Worker | 4 vCPU | 无 | `/data` 43 GiB |
| `ecs3` | CPU Worker | 4 vCPU | 无 | 根盘 21 GiB |
| `gpu1` | GPU Worker | 8 vCPU | NVIDIA L20 46 GiB | 根盘 94 GiB |

当前四台机器没有共享文件系统。Ray 会在 Stage 之间传递 Task/Object，但不会让各节点上的同名本地路径变成同一份文件。当前 Curator `ClipWriterStage` 也只通过 `pathlib.Path` 写本地 POSIX 路径，因此分布式实验前必须解决输入和输出的共享可见性。

建议将 `ecs2:/data/curator-flow` 导出为 NFS，或者使用现有 NAS，并在四台机器统一挂载为 `/mnt/curator-flow`。至少预留 25 GiB 给 shard、解包视频、输出、报告和 checkpoint。模型权重可以只放 GPU 节点本地盘，但所有可能执行 `CaptionPreparationStage` 的节点都要提前准备好 processor/tokenizer 缓存。

目录约定：

```text
/mnt/curator-flow/
├── input/
│   ├── shards/train_00480_of_10809.tar
│   ├── videos-1/
│   ├── videos-8/
│   └── videos-all/
├── runs/<run-id>/
│   ├── output/
│   ├── report.json
│   ├── driver.log
│   ├── ray/
│   └── gpu.csv
└── checkpoints/<run-id>/
```

Driver 固定在 Linux 的 `ecs1` 上运行，并通过 `RAY_ADDRESS=auto` 连接现有集群。不从 macOS 开发机启动：NeMo Curator 会拒绝 Darwin，而且 laptop 作为远程 Driver 会额外引入网络和打包变量。

## 4. 开跑前硬门禁

以下任一项不通过，就不开始计时实验。

### 4.1 环境一致性

- 四台节点使用相同 Python minor 版本。当前系统 Python 是 CPU 节点 `3.12`、GPU 节点 `3.10`，必须通过实验虚拟环境统一。
- 四台节点使用同一个项目 commit。
- `nemo-curator`、`cosmos-xenna`、`ray`、`torch`、`vllm` 版本一致。
- 所有可能执行 CPU Stage 的节点都有 `ffmpeg` 和 `ffprobe`。
- 启动所有 Ray 进程时设置 `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1`，让 Xenna 管理 GPU 可见性。
- `ray status` 显示 4 个 Alive Node、正确的 CPU 总数以及恰好 1 GPU。

第一次实验保存以下证据：

```bash
git rev-parse HEAD
python --version
pip freeze
ffmpeg -version
ray status
ray list nodes --detail
nvidia-smi -q
```

### 4.2 共享存储可见性

从每个节点执行：

1. 读取 `/mnt/curator-flow` 下同一个 sentinel 文件。
2. 记录该路径的 mount/device 信息。
3. 创建一个以节点名命名的临时文件，并从其他三台节点确认可见。
4. 验证后只删除这些 sentinel 文件。

如果四台机器的同一路径实际落到独立本地盘，分布式实验直接判定为 blocked。

### 4.3 模型门禁

在 `gpu1` 上先用 Curator 加载一次 Qwen2.5-VL，确认：

- 模型 revision 固定；
- vLLM 能启动；
- 峰值显存低于 90%；
- 一个 prepared video window 能生成非空 caption。

模型下载和首次编译属于 warm-up 成本，单独记录，不混入稳态吞吐。

## 5. 实验矩阵

| Run | 输入 | 运行位置 | Xenna 模式 | Caption | 要回答的问题 |
| --- | ---: | --- | --- | --- | --- |
| `R0-cpu-1` | 1 条视频 | `ecs2` 单机 | batch | 关 | Curator 的读取、切片、转码、写出语义是否正确？ |
| `R1-gpu-1` | 1 条视频 | `gpu1` 单机 | batch | 开 | 原生 caption 链路能否加载模型并写出 caption？ |
| `R2-cluster-batch-8` | 8 条视频 | 4 节点集群 | batch | 开 | 共享路径和异构 Ray 放置是否正确？ |
| `R3-cluster-stream-8` | 8 条视频 | 4 节点集群 | streaming | 开 | Xenna 重叠执行和背压带来什么变化？ |
| `R4-cluster-stream-full` | 完整 shard | 4 节点集群 | streaming | 开 | 完整 shard 是否满足守恒、稳定性和质量门槛？ |
| `R5-rerun` | 完整 shard | 4 节点集群 | streaming | 开 | 同目录重跑会跳过什么，又会重复计算什么？ |

除 `R5` 故意复用 `R4` 输出目录外，每轮必须使用全新的 output。不能拿包含旧产物或失败残留的目录做吞吐比较。

## 6. 详细步骤

### A. 盘点并准备 Shard

1. 只下载一次 shard 到共享存储。
2. 计算 SHA-256。
3. 统计 tar 中视频成员数并记为 `N`。
4. 确定性解包前三套输入：前 1 条、前 8 条、全部 `N` 条。
5. 对 `videos-all` 执行 `ffprobe`，保存逐视频清单。
6. 确认最终 `prepare-report.json.video_count == N`。

当前准备脚本要求 `--max-videos` 为正整数，完整解包时传入 tar inventory 得到的精确 `N`。以后可以增加显式 `--all`，但不是本轮实验的必要条件。

### B. 保存 Curator 执行计划

执行前保存 `pipeline.describe()`，并核对 Stage 资源声明：

| Stage | 每个 Worker 的最低资源 | 关键行为 |
| --- | --- | --- |
| file partitioning | 0.5 CPU，固定 1 Worker | 每条视频产生一个 `FileGroupTask` |
| video reader | 1 CPU | 把整条视频读入 `Video.source_bytes` |
| fixed stride | 1 CPU | 按时间区间生成确定性 UUID5 clip |
| transcoding | 2 CPU | 生成 VP9 bytes，随后释放 source bytes |
| caption preparation | 1 CPU | 解码采样帧并构造 VLM 输入 |
| caption generation | 1 CPU + 1 GPU | 通过 vLLM 运行 Qwen |
| clip writer | 0.25 CPU | 将 MP4 和 JSON 写到共享路径 |

因此完整 streaming 至少需要同时容纳 6.75 CPU 和 1 GPU。集群按 75% 分配后约有 15 CPU，资源总量足够；batch 模式则不要求所有 Stage Worker 同时存在。

### C. `R0-cpu-1`：验证 Curator 数据语义

在 `ecs2` 上不设置 `RAY_ADDRESS`，以 batch 模式运行，不开启 caption：

```bash
make video ARGS="
  --input-path /mnt/curator-flow/input/videos-1
  --output-path /mnt/curator-flow/runs/R0-cpu-1/output
  --report-path /mnt/curator-flow/runs/R0-cpu-1/report.json
  --executor xenna
  --execution-mode batch
  --transcode-encoder libvpx-vp9
  --verbose"
```

预期：

- 产生 1 个输入 `FileGroupTask` 和 1 个 `VideoTask`；
- 典型 60 秒视频约产生 6 个 clip；
- 每个 clip 有一个 MP4 和一个 metadata JSON；
- 此时没有 caption preparation，因此 `windows=[]`、`valid=false` 是预期语义；
- 五个 CPU Stage 都有 `_stage_perf` 指标。

至少抽一对 MP4/JSON，用 `ffprobe` 对比实际时长和 `duration_span`。

### D. `R1-gpu-1`：验证原生 GPU Caption

在 `gpu1` 上不设置 `RAY_ADDRESS`，用 batch 模式和 `--generate-captions` 运行。模型目录使用 GPU 本地盘，例如 `/opt/curator/models`。

```bash
make video ARGS="
  --input-path /mnt/curator-flow/input/videos-1
  --output-path /mnt/curator-flow/runs/R1-gpu-1/output
  --report-path /mnt/curator-flow/runs/R1-gpu-1/report.json
  --model-dir /opt/curator/models
  --executor xenna
  --execution-mode batch
  --generate-captions
  --caption-batch-size 1
  --verbose"
```

预期：

- Pipeline 比 R0 多出 caption preparation 和 generation；
- caption generation 声明并实际占用 1 GPU；
- 每个有效 clip 至少有一个 window；
- 每个 window 有非空 `qwen2.5_caption`；
- caption 后 `valid=true`；
- caption 阶段能观察到 GPU 显存和利用率上升。

人工观看 clip 并阅读 caption，记录幻觉、时序错误、过度泛化和空文本。进程退出码为 0 不代表 caption 质量合格。

### E. `R2-cluster-batch-8`：验证 Ray 异构集群接线

在 `ecs1` 上设置 `RAY_ADDRESS=auto`，使用共享输入和输出。必须确认连接的是已有四节点集群，没有静默启动私有本地 Ray。

```bash
RAY_ADDRESS=auto make video ARGS="
  --input-path /mnt/curator-flow/input/videos-8
  --output-path /mnt/curator-flow/runs/R2-cluster-batch-8/output
  --report-path /mnt/curator-flow/runs/R2-cluster-batch-8/report.json
  --model-dir /opt/curator/models
  --executor xenna
  --execution-mode batch
  --generate-captions
  --caption-batch-size 1
  --verbose"
```

这一轮必须证明：

- Ray 看见 4 个节点；
- batch 模式逐 Stage 完成后再进入下一 Stage；
- caption Actor 被放置在 `gpu1`；
- CPU Actor 可以使用三个 CPU 节点或 `gpu1` 的空闲 CPU；
- Driver 能从共享目录看到全部输出。

保存 Xenna Worker allocation layout 和 `ray list actors --detail`。现有 `_stage_perf` 聚合值本身不能证明节点放置。

### F. `R3-cluster-stream-8`：验证 Xenna Streaming

输入和 Pipeline 参数与 R2 完全相同，只把 execution mode 改为 streaming，并换新输出目录。

```bash
RAY_ADDRESS=auto make video ARGS="
  --input-path /mnt/curator-flow/input/videos-8
  --output-path /mnt/curator-flow/runs/R3-cluster-stream-8/output
  --report-path /mnt/curator-flow/runs/R3-cluster-stream-8/report.json
  --model-dir /opt/curator/models
  --executor xenna
  --execution-mode streaming
  --generate-captions
  --caption-batch-size 1
  --verbose"
```

观察并记录：

- 每个 Stage 的 Worker 数和所在节点；
- Queue 的增长与回落；
- 各 Stage 的 actor idle time；
- Ray object store 占用和 spill；
- 每个节点的 CPU、内存和网络；
- GPU 利用率和显存；
- 与 R2 的总 wall time 差异。

8 条视频可能短于 Xenna 默认 180 秒 autoscale interval。诊断轮需要把它暴露为 CLI 参数并改成 10 至 30 秒；完整 shard 再恢复保守值。

结果解释：

- 8 条视频上 streaming 比 batch 慢不算失败，Actor 启动和模型 setup 可能占主导。
- 上游 idle 高且 GPU 持续繁忙，说明 caption 是瓶颈。
- object store/spill 持续增长，说明活跃窗口过大，或者 Writer/GPU 排空速度不足。
- 小输入下某些 CPU 节点没有任务，可能是 Xenna 的合理分配，但必须如实记录。

### G. 调 Caption Batch

在同一份 8 视频输入上依次测试 `caption_batch_size=1,2,4`，每次只改变这一项。记录 wall time、captions/s、GPU 峰值显存、利用率和 OOM。

选择满足以下条件的最大值：

- 连续运行两次不 OOM；
- 峰值显存低于 90%；
- captions/s 有提升或至少不下降；
- Ray object store 不持续增长。

在 R4 前冻结该值。调参结果属于配置依据，不混入 batch/streaming 公平对比。

### H. `R4-cluster-stream-full`：跑完整 Shard

使用冻结参数处理 shard 内全部视频。运行期间持续保存：

- Driver/Xenna 日志；
- `ray status` 和 Ray Actor 快照；
- 各节点 CPU、内存、磁盘和网络采样；
- Ray object store 与 spill 采样；
- 一秒粒度的 GPU utilization、memory 和 power。

计时不包含 shard 下载、模型下载和环境安装；包含正常冷启动会发生的 Curator Stage setup。若同时测 cold/warm，必须分别报告。

### I. 输出守恒校验

定义：

- `N`：解包输入视频数；
- `M_expected`：由每条视频的 `ffprobe` 时长计算出的预期 clip 总数；
- `M_written`：`clips/` 下实际 MP4 数；
- `M_meta`：`metas/v0/` 下实际 JSON 数。

验收条件：

```text
processed video metadata 数 == N
processed clip chunk 数      == sum(ceil(clips_i / 32))
M_written                    == M_expected - failed_or_filtered_clips
M_meta                       == M_written
每个 clip_location 都存在
每个 accepted clip 至少有一个 caption window
每个 window 都有非空 qwen2.5_caption
所有 error 都被计数且能关联到 source video
```

另外抽检至少 10 对 MP4/JSON，人工核对 UUID、源路径、span、实际时长、codec 和 caption。

### J. `R5-rerun`：区分幂等写与断点恢复

用完全相同的参数复用 R4 的 output path，运行前后分别计算所有产物 checksum。

当前代码的预期行为：

- Writer 遇到已有文件会跳过，已有产物 checksum 不应变化；
- CLI 尚未给 `pipeline.run()` 传 `checkpoint_path`，所以上游 Stage 仍会重新读取和计算；
- 因此 R5 只能证明 Writer 幂等，不能证明 checkpoint recovery。

后续把共享 checkpoint 暴露到 CLI 后，再增加“运行中止 -> 重启 -> 只处理未完成 Task”的独立实验。没做该实验前，不宣称已经验证断点恢复。

## 7. 每轮必须保存的指标

### 7.1 身份

- Run ID、UTC 起止时间；
- Git commit；
- shard 路径、SHA-256、字节数；
- Python、Curator、Xenna、Ray、Torch、vLLM、FFmpeg 版本；
- Ray Node ID、IP 和资源；
- 完整 Pipeline 参数与 Stage 顺序。

### 7.2 吞吐

- videos/s 和 input MiB/s；
- source video seconds / wall second；
- clips/s 和 captions/s；
- 总 wall time；
- 各 Stage process time 的 sum/mean/std；
- 各 Stage actor idle time 的 sum/mean/std。

### 7.3 资源

- 各节点 CPU 和内存峰值；
- Ray object store 峰值与 spill bytes；
- 各节点网络 RX/TX；
- GPU utilization、显存和功耗时序；
- Xenna Worker 分配变化。

### 7.4 正确性与质量

- 输入、预期 clip、实际 clip、metadata 和 caption 数；
- 按 Stage 和 source video 聚合的 error；
- 缺失输出引用；
- 重复 UUID；
- 至少 10 个 caption 人工样例。

## 8. 预先推演的假设

1. 完整 caption Pipeline 的瓶颈会是 Qwen caption generation；GPU 饱和后，上游 Stage 的 idle time 会升高。
2. Streaming 会通过重叠 reader、transcode、preparation、generation 和 writer 缩短完整 shard 的 wall time；小样本可能因为启动成本反而更慢。
3. GPU 饱和后继续增加 CPU Worker，不会明显提高端到端吞吐。
4. Xenna 背压正常时，Ray object store 应稳定在一个区间，而不是随 shard 总大小持续增长。
5. 单 shard 下共享存储写入不应成为首要瓶颈；若成为瓶颈，会表现为 Writer backlog、网络饱和或上游 idle。
6. 同 output path 重跑应保持已有产物 checksum 不变，但没有 checkpoint 时仍会重复上游计算。

最终报告对每条假设只能给出 `supported`、`rejected` 或 `inconclusive`，并附测量证据。

## 9. 当前代码缺口

在对应计时实验前补齐：

1. CPU Pipeline 构造仍会无条件 import caption 模块，需要改成只在 `generate_captions=true` 时加载。
2. Caption preflight 目前检查 Driver 本机 CUDA；异构集群应检查 Ray 集群 GPU 容量和节点环境，不能要求 CPU head 有 CUDA。
3. CLI 需要暴露 Xenna logging/autoscale interval 和 Curator `checkpoint_path`。
4. Report 需要补 Git/package/Ray node 身份、视频总时长、预期 clip 数、错误统计和节点放置证据。
5. 输出校验必须面向共享存储；分布式本地路径下只扫描 Driver 磁盘是无效的。

这些改动属于组装、运行和观测，不重新实现 Curator 的媒体算子。

## 10. 闭环退出条件

只有同时满足以下条件，才算完成一个 Video shard 的实验闭环：

- R0 至 R4 全部通过对应门禁；
- 处理了完整 shard，而不只是 1/8 条 smoke；
- 守恒检查闭合，或者每个差异都有可定位原因；
- 有证据证明 GPU caption Actor 实际运行在 `gpu1`；
- 保存了 Ray/Xenna 分配和 object store 证据；
- 输出 clip 与 caption 通过人工抽检；
- 使用相同输入比较了 batch/streaming 和 caption batch；
- 最终报告严格区分实测事实和推算；
- R5 被准确描述为 Writer 幂等，除非另行完成 checkpoint 恢复实验。
