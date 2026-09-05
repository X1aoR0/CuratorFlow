# CuratorFlow

CuratorFlow 是一个 NeMo Curator 实验与观测项目。媒体处理直接使用 Curator 原生 Stage；本项目只负责 Pipeline 组装、实验配置、运行指标、产物校验和分布式验证。

当前 Pipeline 与执行约束见 [docs/pipeline_plan.md](docs/pipeline_plan.md)，单 Video shard 的完整实验步骤见 [docs/video_shard_experiment.md](docs/video_shard_experiment.md)。

## 资源目标

- 3 台 4C16G CPU ECS 和 1 台 GPU Worker。
- 总本地存储约 1 TB。
- 真实数据目标 200～400 GB。
- 不虚构真实处理 1 PB；通过真实 Benchmark 和扩展效率完成 PB 容量规划。

## 当前主链路

```text
FilePartitioningStage
  -> VideoReaderStage
  -> FixedStrideExtractorStage
  -> ClipTranscodingStage
  -> CaptionPreparationStage
  -> CaptionGenerationStage
  -> ClipWriterStage
```

CPU 节点负责读取、切片、转码、写出与观测；新增 GPU Worker 承载 Curator `CaptionGenerationStage`。服务端环境配置完成后即可验证完整链路。

本地命令统一通过 `Makefile` 设置 `PYTHONPYCACHEPREFIX=.venv/pycache`，避免在源码树里生成散落的 `__pycache__`。

运行入口：

```bash
make video ARGS="\
  --input-path /data/videos \
  --output-path /data/output/video \
  --report-path /data/output/video-report.json \
  --executor xenna"
```

如果直接调用 Python，需要在命令前显式设置同一个环境变量。

## 当前状态

- Curator 原生视频 Pipeline、实验入口和输出观测已实现。
- 3 个 CPU 节点与 1 个 GPU Worker 的连接信息已登记。
- 服务端 Curator/CUDA 环境和完整分布式运行仍待验证。

## 仓库结构

```text
curatorFlow/
├── src/curator_flow/       # Curator Pipeline 组装与实验入口
├── scripts/data/           # 有界数据准备脚本
├── deploy/                 # 集群主机与 SSH 配置
├── docs/                   # Pipeline、输出契约与实验图表
└── tests/                  # 单元与集成测试
```

