# CuratorFlow 实施计划

## 1. 项目定位

CuratorFlow 是一个基于 NeMo Curator、Xenna 和 Ray 的多模态预训练数据治理项目。项目以有限实验资源完成真实分布式验证，并通过规模不变量、横向扩展实验、故障注入和容量模型，证明系统具备扩展到 PB 级数据处理的工程设计能力。

项目不会宣称在当前环境中真实处理过 1 PB 唯一数据。最终对外表述应为：

> 在数百 GB 真实图文数据上完成多节点实测，验证分片调度、有界背压、断点恢复、幂等输出和横向扩展，并据实测吞吐与扩展效率完成 1 PB 容量规划。

## 2. 当前资源与约束

### 2.1 已有资源

- 3 台 ECS。
- 每台 4 vCPU、16 GB 内存。
- 总计 12 vCPU、48 GB 内存。
- 总本地存储约 1 TB。

### 2.2 可选扩展资源

- 增加 1 台单 GPU 机器。
- 最低建议：T4 16 GB。
- 更合适：L4 24 GB 或 A10 24 GB。
- GPU 仅承担 CLIP embedding、图文相似度和可选美学评分，不承担训练。

### 2.3 存储使用红线

- 总磁盘使用率不超过 80%。
- 为 Ray spill、临时文件、失败重试和日志预留至少 20%。
- 不落盘保存全量解码图片或其他可重建中间对象。
- 输入、输出、临时目录和 Ray spill 使用独立目录并分别监控。

建议容量分配：

| 用途 | 预算 |
|---|---:|
| 原始压缩图文 Shard | 250～350 GB |
| 清洗后输出 Shard | 150～250 GB |
| Embedding、pHash、质量元数据 | 30～80 GB |
| Ray Object Spill | 100～150 GB |
| 临时文件与下载缓存 | 50～100 GB |
| Checkpoint、Manifest、日志 | 10～30 GB |
| 安全余量 | 150～250 GB |

## 3. 成功标准

项目完成时必须提供可复现证据，而不只是代码。

### 3.1 功能证据

- 处理真实图文配对数据。
- 完成图片解码、格式校验、文本清洗、pHash、CLIP embedding、图文相似度过滤和确定性写出。
- 每个样本保留来源、处理版本、质量分数、过滤原因和输出去向。
- 生成输入、过滤、失败、重试和最终输出的守恒报告。

### 3.2 分布式证据

- 3 台 CPU ECS 共同参与处理。
- 可选 GPU 节点只运行 GPU Stage，形成真实异构 Pipeline。
- 大对象通过 Ray ObjectRef/对象存储传递，不经过 Driver 反序列化中转。
- 完成 1、2、3 个 CPU Worker 节点的横向扩展测试。

### 3.3 有界性证据

- Driver 不一次性持有样本级全量 Task。
- 全局调度以 Manifest Partition 和物理 Shard 为单位。
- Worker 内部再拆为 ImageBatch。
- 人为限制 Writer 后，Queue 和 Object Store 使用量达到稳定区间，而不是随总输入线性增长。

### 3.4 可靠性证据

- StageWorker/Actor 故障后，未完成 Task 能重新入队。
- Job 重启后通过 Curator checkpoint 只恢复未完成 Source Shard。
- Sink 使用确定性路径和 commit 协议。
- 验证无丢失、无重复提交；允许底层 at-least-once 重算。

### 3.5 PB 容量证据

- 真实处理至少 200 GB，目标 300～400 GB。
- 进行至少 6～12 小时稳定性测试，理想目标 24 小时。
- 明确区分 unique physical data 与 cumulative replay I/O。
- 根据实测 MB/s、samples/s、扩展效率、失败重试率和安全余量，计算 1 PB 在 7 天、30 天 SLA 下的资源需求。

## 4. 总体架构

```text
                    Control Plane
              ┌─────────────────────┐
              │ Ray Head / Driver   │
              │ Manifest Coordinator│
              │ Checkpoint / Metrics│
              └──────────┬──────────┘
                         │
          ┌──────────────┼──────────────┐
          │              │              │
          ▼              ▼              ▼
   CPU Worker 1    CPU Worker 2    Optional GPU Worker
   Read/Decode     pHash/Write      CLIP/Similarity
          │              │              │
          └──────────────┼──────────────┘
                         ▼
             S3/OSS/MinIO or shared data layer
```

执行层次：

```text
Dataset
└── Run Partition
    └── Manifest Partition
        └── FileGroupTask (若干物理 Shard)
            └── Worker 内部 ImageBatch
                └── ImageObject
```

目标是让 Driver 内存和全局调度元数据只与“活跃窗口”相关，而不与全数据样本数相关。

## 5. Pipeline 设计

### Job A：流式多模态治理

```text
ManifestPartitionSource
    -> ShardReader
    -> DecodeAndValidate
    -> TextNormalize
    -> PerceptualHash
    -> ClipEmbedding (有GPU时启用)
    -> QualityFilter
    -> DeterministicWriter
```

Stage 职责：

1. `ManifestPartitionSourceStage`
   - 只读取当前 Manifest Partition。
   - 按 Shard 数和字节数生成 `FileGroupTask`。
   - 生成确定性 `task_id` 和 `_source_id`。

2. `ShardReaderStage`
   - 流式读取 WebDataset/Parquet。
   - 校验文件 checksum 和样本 schema。
   - 产生有界 `ImageBatch`，不把完整 FileGroup 全部展开到内存。

3. `DecodeAndValidateStage`
   - 图片解码、EXIF 方向修正。
   - 宽高、像素数、长宽比、全黑/全白和损坏检测。
   - 记录失败原因，不静默丢弃。

4. `TextNormalizeStage`
   - Unicode 规范化。
   - 控制字符清理。
   - 文本长度与空文本检查。
   - 可选轻量语言识别。

5. `PerceptualHashStage`
   - 计算内容 hash、pHash/dHash。
   - 产生后续全局去重所需的紧凑特征。

6. `ClipEmbeddingStage`
   - GPU 可用后启用。
   - 计算 image embedding、text embedding、图文相似度。
   - Embedding 使用 float16，避免元数据体积失控。

7. `QualityFilterStage`
   - 综合解码状态、尺寸、文本、相似度和可选美学/安全分数。
   - 保留 `keep`、`reject_reasons[]` 和各维度 score。

8. `DeterministicWriterStage`
   - 写 WebDataset/Parquet Shard。
   - 临时文件、校验、原子提交、最终 `commit.json`。
   - 重试时能够识别已提交输出并跳过。

### Job B：全局去重

全局去重单独运行，不强塞进 Xenna 线性 Pipeline：

```text
读取紧凑pHash/embedding特征
    -> 按bucket重分区
    -> 候选对生成
    -> 精确相似度验证
    -> Connected Components
    -> 生成保留/删除清单
```

优先复用 Curator dedup workflow 或 Ray Data，因为该阶段需要全局 Shuffle。

### Job C：最终物化

```text
治理后的Shard + 去重保留清单
    -> 最终预训练WebDataset/Parquet
    -> Dataset manifest
    -> 质量与血缘报告
```

## 6. 数据布局与幂等协议

建议目录/对象键布局：

```text
data/
├── manifests/
│   ├── dataset.json
│   └── partitions/part-000000.jsonl
├── raw/
│   └── shards/shard-000000.tar
├── work/
│   ├── cache/
│   ├── tmp/
│   └── ray-spill/
├── checkpoints/
└── output/
    └── pipeline_version=v1/
        └── partition=000000/
            └── shard=000000/
                ├── data.tar
                ├── metadata.parquet
                └── commit.json
```

确定性 Task ID：

```text
task_id = sha256(
    dataset_version
    + manifest_partition_id
    + ordered_input_shard_ids
    + pipeline_version
)
```

提交协议：

1. 写入 `*.tmp.<attempt_id>`。
2. flush/close 并计算 checksum。
3. 校验记录数、字节数和 schema。
4. 原子 rename/copy 到最终路径。
5. 最后写入 `commit.json`。
6. 只有合法 `commit.json` 才表示 Shard 完成。

## 7. 分阶段实施路线

### M0：开发环境与集群基线（0.5～1 天）

交付物：

- Python 虚拟环境和锁定依赖。
- 3 节点 Ray 集群启动/停止脚本。
- 节点、CPU、内存、磁盘和网络基线报告。
- `make smoke` 或等价的一键冒烟命令。

验收：

- Driver 能发现 3 个存活节点。
- 跨节点 Ray Task、Actor、ObjectRef 均可运行。
- 每台节点的工作目录和 spill 目录明确。

### M1：CPU 单机 MVP（1～2 天）

交付物：

- 公开小型图文样本集。
- Manifest 生成器。
- `ManifestPartitionSource -> ShardReader -> DecodeAndValidate -> Writer`。
- 确定性输出和基础质量报告。

验收：

- 处理 5～20 GB 数据。
- 输入数 = 成功输出 + 过滤 + 失败。
- 重跑不会生成重复最终 Shard。

### M2：3 节点 CPU 分布式（1～2 天）

交付物：

- 3 节点部署配置。
- 1、2、3 Worker 节点 benchmark。
- Driver 内存、网络、磁盘和 Object Store 指标。

验收：

- 至少处理 50～100 GB。
- 输出与单机结果一致。
- 给出扩展效率并解释非线性原因。

### M3：GPU 多模态 Stage（1～2 天，GPU 到位后）

交付物：

- 单 GPU CLIP embedding Stage。
- batch size 8/16/32/64 基准测试。
- 图文相似度分布和过滤阈值分析。

验收：

- GPU Stage 只落到 GPU 节点。
- 记录 images/s、显存、GPU 利用率、P95 latency。
- CPU Reader 能持续供给，或明确指出 GPU 饥饿瓶颈。

### M4：背压与有界性（1 天）

交付物：

- Writer 限速开关。
- Queue、slot、Object Store、spill 随时间变化图。
- 背压实验报告。

验收：

- Writer 限速后压力能逐级传回 Source。
- Queue 和 Object Store 使用不随输入总量线性增长。
- 能从源码解释 `num_tasks_in_progress + num_tasks_completed >= max_queued`。

### M5：恢复与故障注入（1～2 天）

交付物：

- Curator checkpoint/resumability 集成。
- Actor kill、Worker 节点退出、Job 重启脚本。
- 幂等输出校验器。
- 故障实验报告。

验收：

- 未完成 Task 能重试。
- 已完成 Shard 不重复提交。
- 最终无样本丢失；失败样本均有原因或待重试记录。

### M6：规模测试与 PB 容量规划（2～3 天）

交付物：

- 200～400 GB 真实数据测试。
- 6～12 小时稳定性测试。
- 吞吐、扩展效率、长尾、失败率和资源曲线。
- `PB_CAPACITY_REPORT.md`。

验收：

- Driver RSS 无持续无界增长。
- 临时文件和 ObjectRef 无明显泄漏。
- 明确给出 1 PB 在 7/30 天 SLA 下所需 CPU、GPU、磁盘、网络和成本区间。

### M7：全局去重与最终物化（可选增强，2～4 天）

交付物：

- pHash/embedding bucket。
- 候选对、相似度验证、Connected Components。
- 最终保留清单与最终训练 Shard。

验收：

- 去重前后数量守恒。
- 重复率、误判抽检和去重耗时可量化。

### M8：面试材料（1～2 天）

交付物：

- 架构图。
- 数据生命周期图。
- 背压时序图。
- 故障恢复时序图。
- Benchmark 与容量报告。
- 5 分钟项目讲稿和追问清单。

## 8. 实验矩阵

### 8.1 CPU 横向扩展

| 变量 | 取值 |
|---|---|
| CPU Worker 节点数 | 1 / 2 / 3 |
| Shard 大小 | 256 MB / 512 MB / 1 GB |
| FileGroupTask Shard 数 | 1 / 4 / 8 / 16 |
| ImageBatch | 32 / 64 / 128 |

输出指标：MB/s、samples/s、扩展效率、P50/P95/P99、CPU、RSS、磁盘和网络。

### 8.2 GPU 批大小

| 变量 | 取值 |
|---|---|
| CLIP batch size | 8 / 16 / 32 / 64 |
| 输入分辨率 | 固定模型要求 |
| embedding dtype | float16 / float32 对照 |

输出指标：images/s、显存、GPU utilization、P95、OOM 和 CPU 等待占比。

### 8.3 背压

- 基线 Writer。
- Writer 睡眠/限速 2 倍。
- Writer 睡眠/限速 4 倍。
- 观察上游 Queue、used slots、Object Store 和 spill。

### 8.4 故障

- kill CPU Stage Actor。
- kill GPU Stage Actor。
- 停止一个 Worker 节点。
- 中止并重启整个 Job。
- 注入损坏 Shard 和临时写失败。

## 9. 指标与报告

必须采集：

```text
Pipeline
├── input_samples_total / input_bytes_total
├── output_samples_total / output_bytes_total
├── filtered_samples_total
├── failed_samples_total
└── retried_tasks_total

Stage
├── capacity_samples_per_second
├── actual_samples_per_second
├── task_duration_p50/p95/p99
├── input/output queue length
├── worker group count
└── used/empty slots

System
├── CPU / RSS
├── GPU utilization / memory
├── network RX/TX
├── disk read/write/free
├── Ray Object Store memory
└── spill bytes

Quality
├── decode failure rate
├── resolution/aspect distributions
├── language distribution
├── CLIP similarity distribution
├── exact/near duplicate rate
└── final retention rate
```

## 10. PB 容量模型

所有容量结论必须来自真实实测。

```text
1 PB = 1,000,000,000 MB

days_to_process =
    1,000,000,000
    / effective_MB_per_second
    / 86,400
```

目标带宽：

| SLA | 所需持续有效吞吐 |
|---|---:|
| 30 天 | 约 386 MB/s |
| 7 天 | 约 1.65 GB/s |
| 3 天 | 约 3.86 GB/s |

资源外推必须加入：

- 实测横向扩展效率。
- 失败重试比例。
- 长尾和数据倾斜。
- 25%～40% 安全余量。
- 对象存储带宽与请求限额。
- 过滤后写放大/缩小比例。

## 11. 面试证据包

最终仓库至少包含：

```text
README.md
PLAN.md
docs/architecture.md
docs/data_contract.md
docs/failure_model.md
docs/benchmark_report.md
docs/pb_capacity_report.md
docs/interview_notes.md
configs/local.yaml
configs/cluster_cpu.yaml
configs/cluster_gpu.yaml
scripts/cluster/
scripts/fault_injection/
benchmarks/results/
```

简历措辞模板：

> 基于 NeMo Curator、Xenna 和 Ray 构建多模态预训练数据治理流水线，在 3 个 4C16G CPU 节点与单 GPU 节点上完成数百 GB 真实图文数据实测；采用分层 Manifest、Shard 级全局调度和 Worker 内 ImageBatch 控制 Driver 元数据规模，通过 Stage 级背压限制中间对象物化，并实现确定性输出、Checkpoint 恢复和故障重试。基于多节点扩展效率与长期吞吐完成 1 PB 数据在不同 SLA 下的资源和成本规划。

## 12. 当前第一步

第一阶段只做 M0，不提前实现复杂 Stage：

1. 确认三台 ECS 的操作系统、Python、网络和磁盘挂载。
2. 建立 Python 虚拟环境并安装与本地 Curator/Xenna 源码兼容的依赖。
3. 启动 3 节点 Ray 集群。
4. 运行跨节点 Task、Actor、ObjectRef 冒烟测试。
5. 记录基线 CPU、内存、磁盘和网络数据。
6. 再进入 M1 的 Manifest 与 CPU Pipeline。

