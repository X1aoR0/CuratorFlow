# CuratorFlow

CuratorFlow 是一个基于 NeMo Curator、Xenna 和 Ray 的多模态预训练数据治理实践项目。项目在有限实验资源上完成真实分布式验证，并通过分片调度、有界背压、幂等输出、断点恢复、故障注入和容量模型，论证系统扩展到 PB 级数据处理时的工程边界。

详细实施路线见 [PLAN.md](PLAN.md)。

## 资源目标

- 3 台 4C16G CPU ECS。
- 总本地存储约 1 TB。
- 可选增加 1 台 T4/L4/A10 单 GPU 节点。
- 真实数据目标 200～400 GB。
- 不虚构真实处理 1 PB；通过真实 Benchmark 和扩展效率完成 PB 容量规划。

## 计划中的数据链路

```text
Manifest Partition
  -> FileGroupTask / ShardReader
  -> DecodeAndValidate
  -> TextNormalize / pHash
  -> CLIP Embedding
  -> QualityFilter
  -> DeterministicWriter
  -> Global Dedup (separate job)
  -> Final Materialization
```

## 当前状态

- [ ] M0：开发环境与三节点 Ray 集群基线
- [ ] M1：CPU 单机 MVP
- [ ] M2：三节点 CPU 分布式
- [ ] M3：单 GPU 多模态 Stage
- [ ] M4：背压与有界性验证
- [ ] M5：恢复与故障注入
- [ ] M6：规模测试与 PB 容量规划
- [ ] M7：全局去重与最终物化（可选增强）
- [ ] M8：面试证据包

## 仓库结构

```text
curatorFlow/
├── src/curator_flow/       # Pipeline、Task、Stage 与运行入口
├── configs/                # 本地、CPU集群、GPU集群配置
├── scripts/                # 集群、数据准备、故障注入脚本
├── deploy/                 # Ray/监控/存储部署资料
├── benchmarks/             # Benchmark工具与结果
├── docs/                   # 架构、数据契约、故障和容量报告
└── tests/                  # 单元与集成测试
```

