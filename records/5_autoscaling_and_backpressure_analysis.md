# 5. Autoscaling 与背压分析

## 数据来源与图表

本节完全基于两次 Xenna streaming 日志中的周期状态表：

- 主运行：`caption-scale-4gpu-full-20260905-184128.log`，147 个采样点，间隔约 30 秒，覆盖 73.92 分钟；
- 恢复运行：`caption-scale-4gpu-resume-20260905-202530.log`，121 个采样点，间隔约 15 秒，覆盖 30.29 分钟；
- 每个采样点包含全部 7 个 Stage，共 1876 条 Stage 时序记录。

原始大表通过 `scripts/parse_xenna_stage_metrics.py` 转换为 CSV，再由 `scripts/plot_xenna_autoscaling.py` 绘图。

![Xenna Autoscaling and Backpressure](../docs/xenna-autoscaling-backpressure.svg)

图中红色虚线左侧是主运行；主运行处理 194 个视频后因 ECS1 本地磁盘保护而停止。右侧是剩余 38 个视频的恢复运行。两段之间的 4 分钟只是视觉分隔，不代表真实停机间隔。

图的四个面板依次表示：

1. 各 Stage 的 Ready actor（实线）与 Running actor（虚线）；
2. 各 Stage 累计完成任务数；
3. 相邻 Stage 累计完成数的差，即仍在两个 Stage 之间流动的任务；
4. 每个 actor 的处理速度 `Tasks/actor/s`。

## Autoscaling 配置与解释边界

本次 Xenna 配置：

```text
execution_mode = streaming
cpu_allocation_percentage = 0.75
logging_interval = 30s（主运行）/ 15s（恢复运行）
autoscale_interval_s = 60s（主运行）/ 30s（恢复运行）
autoscale_speed_estimation_window_duration_s = 180s
autoscale_speed_estimation_min_data_points = 5
```

CaptionGeneration 显式固定为 4 workers，每个 worker 请求 1 GPU。因此它是固定四卡数据并行，不参与 Xenna worker 数量的自动扩缩；其他 `num_workers=None` 的 Stage 由 Xenna 调整。

当前 Xenna 版本中 `Actors: Target` 被硬编码成 0，不能作为 autoscaling 目标值。本文只使用 `Pending/Ready/Running/Idle` 的实际观测值。

Ray Dashboard 中的 CPU/GPU available 也不能直接当作 Xenna 可分配资源：Xenna 自己维护资源分配器，并以 `num_cpus=0` 创建 Ray actors；同时只允许 Pipeline 使用总 CPU 的 75%。

## 主运行的扩缩行为

### Reader 与 FixedStride：启动扩容后迅速收缩

VideoReader 和 FixedStrideExtractor 在约 0.5 分钟时都短暂达到 6 actors，约 1 分钟后收缩到 1：

```text
VideoReader:          0 → 6 → 1 → 0（输入枚举结束）
FixedStrideExtractor: 0 → 6 → 1
```

两者单任务速度远快于后续转码，因此不需要长期占用大量 actor。VideoReader 最终读取全部 232 个输入，FixedStride 在故障时完成 228 个任务；其余数据仍在流动窗口中。

### ClipTranscoding：成为 CPU 瓶颈并稳定扩到 9

ClipTranscoding 的 actor 变化：

```text
0.0 min: 0
0.5 min: 4
1.0 min: 9
73.4 min: 10
```

9 actors 状态维持了 143/147 个采样点，占 97.3%。其平均 Ready 为 8.92，平均 Running 为 8.76，说明绝大多数转码 actor 持续满负载。

```text
每 actor Tasks/s：P50 = 0.00400，P90 = 0.02779
```

P90 明显高于 P50，反映了输入分布差异：前段 256p 低码率视频转码很快，后段 1080p 高码率视频显著变慢。主运行接近结束时才从 9 扩到 10，说明 60 秒控制周期和 180 秒速度窗口使扩容响应较保守。

### CaptionPreparation：在 1 与 2 actors 之间振荡

Preparation 初始短暂扩到 5，随后主要在 1 与 2 之间切换：

```text
Ready=1：86 个采样点
Ready=2：57 个采样点
平均 Ready：1.44
平均 Running：1.02
```

该 Stage 的速度随输入分辨率和 window 数变化。Xenna 多次观察到短期积压后增加第二个 actor，积压消失后又缩回 1。这是本次最清晰的细粒度 autoscaling 行为。

### CaptionGeneration：四卡固定，平均约 70% actor 占用

模型 warm-up 完成后，CaptionGeneration 在 145/147 个采样点保持 Ready=4：

```text
平均 Ready：3.95
平均 Running：2.79
Running / Ready ≈ 70.6%
Tasks/actor/s P50：0.01349
Tasks/actor/s P90：0.03364
```

四卡并非长期全部 100% 忙碌。前期低分辨率数据和转码缓存能让四卡同时运行；后期 CPU VP9 转码变慢，CaptionGeneration 经常只有 1–3 actors Running。P90 与 P50 的差距还反映了 caption 输出长度变化：许多后段文本生成到 256-token 上限，单任务 decode 更久。

### Writer：始终不是瓶颈

主运行中 Writer 基本维持 1 actor：

```text
Tasks/actor/s P50：2.339
Tasks/actor/s P90：3.219
```

CaptionGeneration 的 P50 只有 0.01349 task/actor/s。即使考虑 4 个 GPU actor，Writer 仍快两个数量级，因此共享 NFS 的最终文件写入不是稳态瓶颈。

## 背压是否有效

Xenna 表中的单个 `Input Queue` 并不能完整反映在途数据，因为任务还可能已分配到 actor slots，或者停留在上一个 Stage 的输出侧。因此图中使用相邻 Stage 的累计完成差：

```text
已转码但尚未 caption = Transcoding completed − CaptionGeneration completed
已准备但尚未 caption = Preparation completed − CaptionGeneration completed
已 caption 但尚未写出 = CaptionGeneration completed − Writer completed
```

主运行的统计：

| 在途区间 | 均值 | P95 | 最大值 |
| --- | ---: | ---: | ---: |
| Transcoding → CaptionGeneration | 6.90 | 21 | 31 |
| Preparation → CaptionGeneration | 4.12 | 16 | 16 |
| CaptionGeneration → Writer | 0.01 | 0 | 1 |

这说明：

- GPU 前最多累计约 31 个视频任务，没有随 232 个输入线性增长；
- Writer 几乎即时消费 caption 结果，最多只落后 1 个任务；
- 绝大多数缓冲发生在 CPU 转码与 GPU caption 之间，符合系统瓶颈位置；
- 主运行观察到的 Ray object store 通常约 0.7–3.3 GiB，仅占 82.31 GiB 的少部分，没有长期线性增长。

因此 streaming 背压有效：系统没有一次性读取、转码并堆积全部 232 个视频，而是保持有限的活跃窗口。

## 为什么有效背压仍发生 OutOfDisk

主运行最终不是因队列无限增长而失败，而是 Ray 的本地磁盘保护：

```text
/tmp/ray/... is over 95% full
available space: 1.9426 GB
capacity: 39.0065 GB
ray.exceptions.OutOfDiskError
```

背压控制的是 Pipeline 中的在途任务和 object store 增长，不负责保证宿主机根盘始终有足够空间。ECS1 的 `/tmp/ray` 位于 40 GB 根盘；运行时临时对象/回退空间使可用空间跌破 Ray 的 5% 安全阈值。Pipeline 退出后临时对象释放，根盘恢复到约 15 GB 空闲，也验证了这是运行时峰值而非 NFS 产物永久占满。

恢复运行只处理剩余 38 个视频，ECS1 根盘始终保持 62–69% 使用率，未再次触发保护。

## 恢复运行中的 Autoscaling

恢复运行输入规模更小、且集中为后段高码率视频，扩缩形态与主运行不同。

### 转码快速扩到 11 actors

```text
0.0 min: 0
0.3 min: 4
0.5 min: 6
3.5 min: 10
9.3 min: 11
29.0 min: 0（转码全部完成）
```

11 actors 保持了 78/121 个采样点。平均 Ready=9.62，平均 Running=6.44。尾部任务分辨率更高，单视频转码时间更长；即使 actor 更多，GPU 仍多次等待上游。

### 恢复运行的在途任务更少

| 在途区间 | 均值 | P95 | 最大值 |
| --- | ---: | ---: | ---: |
| Transcoding → CaptionGeneration | 3.32 | 7 | 9 |
| Preparation → CaptionGeneration | 1.94 | 5 | 6 |
| CaptionGeneration → Writer | 0 | 0 | 0 |

38 个输入形成的活跃窗口明显小于主运行。Writer 始终同步跟上 CaptionGeneration。

### 尾部过度扩容

补跑最后约 1 分钟出现：

```text
CaptionPreparation Ready：短暂升到 26
Writer Ready：1 → 104 → 111 → 116
```

这些 actors 几乎全部 Idle，未带来实际吞吐收益。其原因是 autoscaling 决策异步执行，并使用最近 180 秒的历史速度；当决策结果应用时，上游任务已经基本排空。同时 Writer 每 actor 只声明 0.25 CPU，在其他 Stage 释放资源后，分配器可以一次创建大量廉价 actor。

这是一个明确的控制器滞后/尾部 overshoot：Autoscaling 在长稳态阶段有效，但小规模恢复任务接近结束时会过度扩容。后续可以通过以下方式降低无效 actor 启动：

- 为 Writer 固定 1–2 workers；
- 为 Preparation 设置合理最大 workers；
- 缩短速度估计窗口或增加尾部任务阈值；
- 避免对只有几十个任务的恢复运行使用过于激进的 autoscaling 周期。

## 结论

1. Xenna Autoscaling 确实生效：转码由 4 扩到 9/10，恢复运行进一步扩到 11；Preparation 按积压在 1/2 之间调整。
2. CaptionGeneration 的 4 actors 是显式固定的，不属于自动扩容，但四卡都实际执行了任务。
3. 背压有效：主运行 GPU 前在途任务最大 31，Writer 最大只落后 1，object store 没有随输入规模线性增长。
4. 稳态瓶颈是 CPU VP9 转码，尤其是后段 1080p 高码率视频；四 GPU 经常因上游供给不足而部分 Idle。
5. Autoscaling 具有控制延迟：主运行扩容偏保守，恢复运行尾部又出现 Preparation 26 / Writer 116 的过度扩容。
6. OutOfDisk 不否定背压有效性；它暴露的是 ECS1 根盘和 Ray 临时目录容量规划问题，需要为 `/tmp/ray`/spill 配置独立大容量目录或更充足的根盘。
