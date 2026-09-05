# 3. Streaming 实验阶段分析

## 结论摘要

本次实验已经证明四节点 Curator/Xenna streaming 链路能够真实工作：CPU 节点并行读取、切片与 VP9 转码，GPU 节点完成 Qwen2.5-VL caption，Writer 将 MP4、caption metadata 和处理标记写入共享 NFS。已完成的 91 个视频没有静默丢失，四类落盘产物严格守恒。

但是，本次不能记为“全量成功”：GPU ECS 因欠费下线，Ray 收到该节点的 `SIGTERM`，Pipeline 在 91/232 个视频处中断，最终 report JSON 未生成。中断属于基础设施终止，不是 Pipeline 异常，也不是 CUDA OOM。

本次最重要的实验发现不是吞吐，而是 caption 参数存在明显质量风险：564 条 caption 中 267 条（47.3%）达到 `max_output_tokens=256`，其中 255 条没有句末标点，强烈表明文本被硬截断。正式生产预训练语料前必须调整 caption 长度或提示词，并补充自动质量检查。

## 实验身份与状态

| 项目 | 实测值 |
| --- | --- |
| 执行模式 | Xenna streaming |
| 输入 | LLaVA-OneVision shard，232 个约 60 秒 MP4 |
| Pipeline | FilePartitioning → VideoReader → FixedStrideExtractor → ClipTranscoding → CaptionPreparation → CaptionGeneration → ClipWriter |
| 主运行开始 | 2026-09-04 18:52:22 |
| 最后一份完整输出 | 2026-09-04 20:00:19 |
| 有效运行时间 | 约 67 分 57 秒 |
| 最终状态 | **外部中断，非 succeeded** |
| 中断原因 | GPU ECS `192.168.0.211` 离线；Ray 记录 `Expected termination: received SIGTERM` |
| 最终 report JSON | 未生成（driver 随 GPU 节点终止） |
| 主日志 | `/mnt/curator-flow/output/full-streaming-20260904-185217.log` |
| 产物目录 | `/mnt/curator-flow/output/full-streaming-20260904-18521720260904-173156` |

产物目录名称包含两段时间戳，是启动脚本替换旧时间戳时发生的命名错误，只影响目录可读性，不影响数据内容。

## 为使本轮运行成立所做的环境修正

开跑前后实际排除了四个环境问题：

1. ecs2、ecs3 缺少 `ffmpeg`/`ffprobe`，导致 `ClipTranscodingStage` 无法启动；补装后 CPU 转码正常。
2. Xenna 使用 40000 的 Ray State API limit，而 Ray 默认最大值较小；Ray 集群需带 `RAY_MAX_LIMIT_FROM_API_SERVER=40000` 和 `RAY_MAX_LIMIT_FROM_DATA_SOURCE=40000` 启动。
3. vLLM 的 FlashInfer sampler 首次运行需要 JIT 工具链并因环境缺失失败；设置 `VLLM_USE_FLASHINFER_SAMPLER=0` 后使用兼容 sampler。
4. Curator 的 `PromptFormatter` 使用硬编码 Hugging Face ID 加载 Qwen Processor，不读取 CaptionGeneration 的 `model_dir`。通过共享 HF cache 将该 ID 映射到现有本地模型目录，实现全节点离线运行。

`CaptionPreparationStage.setup_on_node()` 会实例化 `PromptFormatter`，因此它不能在没有 Processor cache 的 CPU 节点上任意预热。本轮用 Xenna 的 fractional GPU 资源约束完成放置：

- CaptionPreparation：`1 CPU + 0.01 GPU`，1 worker；只借极小 GPU 资源作为“必须落在 GPU 节点”的调度约束，实际工作仍是 CPU 解码、window 切分和 prompt 格式化。
- CaptionGeneration：`1 CPU + 0.99 GPU`，1 worker；在同一张 L20 上运行 vLLM。

两者资源之和为 1 GPU，因此可以同时驻留同一 GPU 节点。本次实测二者分别以独立 actor 正常运行。

## 完成度与产物守恒

| 产物 | 实测 | 相对全量目标 | 校验结果 |
| --- | ---: | ---: | --- |
| 完成源视频 | 91 | 91/232 = 39.2% | 每个源视频恰好 6 clips |
| `clips/*.mp4` | 546 | 546/1392 = 39.2% | 与 91×6 一致 |
| `metas/v0/*.json` | 546 | 546/1392 = 39.2% | 与 MP4 一一对应 |
| `processed_videos/**/*.json` | 91 | 91/232 | 与完成视频一致 |
| `processed_clip_chunks/**/*.json` | 91 | 91/232 | 6 clips < 32，每视频 1 chunk |
| caption windows | 564 | 见下一节 | 无空 caption |

已完成的数据来源分布：

- `30_60_s_howto100m`：59/59，全部完成。
- `30_60_s_pandas70m`：32/173，因 GPU 节点下线中断。

因此 91 个完成项不是随机抽样：它包含全部 howto100m 子集和 pandas70m 的前 32 个调度结果。不能直接把这 91 个视频的语义分布当作整个 shard 的无偏估计。

## 修正 caption window 规模预测

原实验设计假设“每个 10 秒 clip 恰好一个 caption window”，实测并不总成立。`window_size=256` 是按解码帧切分，而输入视频保留原帧率：

- 528 个 clip 产生 1 个 window。
- 18 个 clip 产生 2 个 windows。
- 这 18 个 clip 来自 3 个高帧率源视频（50 或 59.94 FPS）。
- 因而 546 clips 实际产生 564 caption windows。

完整 232 个视频的帧率扫描发现 17 个视频为 50/59.94/60 FPS。按每视频 6 clips、每个高帧率 clip 2 windows 估算：

```text
基础 windows = 232 × 6 = 1392
高帧率额外 windows = 17 × 6 = 102
全量预计 windows = 1494
```

所以最终 clip 数仍应是 1392，但 caption 数预计约 1494。训练索引设计不能假定“一条 clip 永远只有一条 caption”，需要明确 window 的帧范围。

## 吞吐分析

从 Pipeline 启动到最后一个完整视频写出约 4077 秒：

```text
视频吞吐 = 91 / 4077 ≈ 0.0223 视频/秒 ≈ 1.34 视频/分钟
clip 吞吐 = 546 / 4077 ≈ 0.134 clip/秒 ≈ 8.0 clips/分钟
caption 吞吐 = 564 / 4077 ≈ 0.138 caption/秒 ≈ 8.3 captions/分钟
平均完成一个 60 秒视频 ≈ 44.8 秒
```

若保持本轮平均吞吐，从零完整处理 232 个视频预计约 173 分钟；从 91 个完成项继续补齐剩余 141 个，粗略还需约 105 分钟。实际时间会随 caption 输出长度变化。

本轮早期观察到每分钟 2–3 个视频，后期下降到每分钟约 1 个，原因不是 CPU 或 NFS 停滞，而是许多 caption 接近 256-token 上限，单次 autoregressive decode 更长。用短时间窗口外推全量时间会显著乐观。

## Streaming、背压与资源观测

中断前最后一份完整 Stage 状态：

| Stage | 已完成任务 | 队列/状态 | 解读 |
| --- | ---: | --- | --- |
| FilePartitioning | 1 | 输出队列 111 | 已列出全部文件，受下游背压约束 |
| VideoReader | 121 | 下游队列 8 | 比最终写出领先 30 个视频 |
| FixedStrideExtractor | 113 | 下游队列 8 | 轻量 Stage，不是瓶颈 |
| ClipTranscoding | 97 | 4 actors running | CPU 节点持续并行转码 |
| CaptionPreparation | 97 | 1 actor idle | 明显快于生成阶段 |
| CaptionGeneration | 91 | 1 actor running，输入队列 4 | 端到端瓶颈 |
| ClipWriter | 91 | 1 actor idle | 写盘远快于 caption |

关键观测：

- L20 在稳态 caption 期间大部分时间为 100% utilization。
- vLLM 显存由约 35.1 GiB 增长到约 42.3 GiB 后稳定，L20 总显存约 46.1 GiB；没有 CUDA OOM。
- Ray object store 在一次观测中为 1.08/28.39 GiB，日志中没有 spill 或 `ObjectStoreFullError`。
- 上游队列始终有界，没有随 232 个输入线性增长，说明 Xenna streaming 背压有效。
- NFS 在中断时仍有约 22 GiB 空间，不是中断原因。
- GPU 下线后日志仅出现一次 Ray lost-object reconstruction；此前 `Traceback=0`、`Pipeline execution failed=0`、`ERROR=0`。

结论：原设计中的“单 GPU caption 是瓶颈”和“增加 CPU 不会提高稳态端到端吞吐”得到验证；streaming 的重叠和背压正常，但单卡生成速度决定总时长。

## 输出体积

91 个视频的产物总计约 653 MiB，其中：

- clips：648 MiB；
- metas：2.2 MiB；
- processed_videos：1.1 MiB；
- processed_clip_chunks：1.8 MiB。

546 个 MP4 的精确总字节数为 677,921,203，单 clip：

- 平均约 1.24 MB；
- 中位数约 0.49 MB；
- 最小约 23 KB；
- 最大约 14.67 MB。

按实测线性外推，全量 232 个视频产物约 1.6 GiB，低于原先约 2.5 GB 的估计。分布长尾很明显，均值和中位数差距较大，因此大规模容量规划应保留额外余量。

## Caption 自动质量分析

### 结构完整性

| 指标 | 实测 |
| --- | ---: |
| caption windows | 564 |
| 空 caption | 0 |
| 完全重复 caption | 0 |
| metadata `valid=true` | 546/546 |
| caption token 均值 | 227.7 |
| token P50 | 251 |
| token P75/P90/P95 | 256/256/256 |
| 达到 256-token 上限 | 267/564 = 47.3% |

### 截断风险

267 条达到上限的 caption 中，只有 12 条以句号、问号或叹号结束；其余 255 条大概率在生成中途被截断。低于 256 token 的 297 条中，296 条有正常句末标点。这个对照强烈说明问题来自 `max_output_tokens=256`，而不是普遍的模型写作风格。

因此可以区分：

- **形式上有效**：caption 非空、JSON 正常、可以被读取。
- **训练质量合格**：当前尚不能判定；约 45.2%（255/564）明显疑似截断，不宜不加处理直接进入正式预训练主索引。

### 风格风险

自动统计还发现：

- 32 条包含 `The video appears ...`，存在不必要的弱断言/套话。
- 24 条包含 Markdown `###` 标题，描述格式偏长、偏结构化。
- 没有发现 `I cannot`、道歉或拒绝式输出。

这些文本可用于实验，但若目标是稳定的视觉语义预训练语料，应收紧 prompt，使输出更客观、简洁、直接描述可见内容，并避免 Markdown 模板。

### 尚未验证的质量维度

自动统计只能确认“有文本、结构正确、长度与重复情况”。它不能证明 caption 与画面事实一致。日志中的若干描述语言流畅，但仍可能存在人物、动作、数字或场景幻觉。下一步质量评估至少应对一组分层样本抽帧并人工打分：

1. 低/中/高帧率各取样；
2. 短 caption 与 256-token 截断 caption 各取样；
3. howto100m 与 pandas70m 分开取样；
4. 评分维度包括事实一致性、覆盖度、时序描述、幻觉、文本完整性。

## 本轮能够下的结论

1. **链路可行**：四节点 Ray + NFS + Curator/Xenna streaming 可以完成真实视频转码、Qwen caption 和共享写盘。
2. **守恒成立**：已完成的 91 个视频全部满足 1 视频 → 6 clips，MP4、metadata、视频标记和 chunk 标记数量一致。
3. **瓶颈明确**：单 L20 上的 CaptionGeneration 是吞吐上限；CPU reader/transcode 和 NFS writer 不是稳态瓶颈。
4. **背压正常**：对象存储低占用、无 spill，上游领先量有限。
5. **全量未完成**：只能报告 39.2% 完成，不能报告 shard 成功。
6. **caption 参数需改进**：256-token 上限导致近半输出撞限，正式实验前需要处理。
7. **window 预测需按帧率修正**：预计全量约 1494 captions，而不是 1392。

## 下一轮建议

优先级从高到低：

1. 恢复 GPU ECS 后先验证节点、NFS、模型 cache 和 Ray 加入状态，再决定续跑方式。现有 CLI 没有 checkpoint，不能把“Writer 跳过已存在文件”表述为严格断点恢复。
2. 为了完成当前同参数实验，可保留 `max_output_tokens=256` 补齐剩余数据，保证一次实验内部参数一致；但产物应标记为实验数据，不直接视为成熟预训练语料。
3. 下一轮质量实验将 `max_output_tokens` 提高到 384 或 512，或收紧 prompt 要求 100–150 词客观描述，再比较截断率、吞吐与幻觉。更推荐先收紧 prompt，因为单纯提高上限会进一步降低吞吐。
4. 在最终训练索引生成前加入质量门槛：caption 非空、token 未撞上限、句末完整、无拒答/模板污染；高帧率多 window 需保留 start/end frame。
5. 增加独立的运行监控与报告落盘，使 driver 所在 GPU ECS 下线时仍能从 head 节点获得最终失败报告和精确中断原因。

## 证据边界

本文只对已经落盘的 91 个视频和运行日志作结论。由于 GPU ECS 被外部终止：

- 没有成功态 report JSON；
- 没有完成剩余 141 个视频；
- 没有验证当前实现的严格 checkpoint/恢复能力；
- 没有完成全量 caption 语义人工评审。

因此本轮的准确表述是：**streaming Pipeline 的功能、异构调度、背压和部分数据质量已得到实测验证；全量完成性尚未验证。**
