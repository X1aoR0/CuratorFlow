# 2. 实验设计

## 目标

在选定的整份 shard 上，用 Curator 原生 video pipeline **完整跑通一轮**，同时做到：

1. 全量处理该 shard 的全部视频（不是 8 个样本，是完整 232 个）。
2. 观测中间过程：各 Stage 吞吐、GPU 利用率、排队/背压、对象存储占用。
3. 校验输出正常：产物数量、结构、caption 内容与守恒关系符合预期。

不做的事（本轮明确排除）：

- 不做幂等/断点重跑验证。当前 CLI（`src/curator_flow.run_video_pipelines.py`）没有 `checkpoint_path` 参数，重跑只能证明 Writer 的“已存在跳过”，无法证明断点恢复，价值有限，本轮不纳入。
- 不追求逐步放大规模。目标就是一整份 shard 一次跑完。

## 环境实况（开跑前的差距）

通过 SSH 只读核对四台机器，实际状态与“已就绪”有出入，必须先补齐才能谈“集群全量”：

| 项 | 实测结果 |
| --- | --- |
| 目标 shard | `/data/datasets/llava-onevision-2/shard/.../train_00480_of_10809.tar`，3.01 GiB，232 个 mp4 成员，时长均 ~60s |
| 已解包视频 | 只有 8 个（172M），不是全部 232 个 |
| Ray 集群 | 未运行（ecs1 无 raylet/gcs_server，6379 空，`ray status` 连不上） |
| ecs1(head) | repo `/data/curator-flow`，venv py3.12.3，ray/curator/xenna/torch 齐全，**无 vllm**；有 shard + 8 视频 |
| ecs2 / ecs3 | 无 repo、无 venv、无 datasets，只有空 `/data` |
| gpu1 | 另一套布局：repo `/opt/curator-flow` + 运行时 venv `/opt/curator-runtime/venv`（py3.12.14），有 vllm、有 Qwen2.5-VL-7B（16G）、有 8 视频；GPU caption smoke 已成功，产出 `qwen2.5_caption` 内容真实可用 |
| 共享存储 | 无。数据分别躺在 ecs1 和 gpu1 本地盘，ecs2/ecs3 什么都没有 |

结论：GPU caption 链路已验证可跑通（最大风险已排除），但四节点分布式全量目前起不来——没有共享盘、workers 未装环境、Ray 未起、shard 只解包 8/232、ecs1 与 gpu1 的 venv/python 布局不一致。在这些补齐前，任何“集群全量”结论都无法产生。

## 固定参数（与已验证 smoke 一致，不改动）

```text
clip_len_s = 10
clip_stride_s = 10
min_clip_length_s = 2
transcode_encoder = libvpx-vp9   # CPU 转码
caption_model = qwen2.5
sampling_fps = 2
caption_window_size = 256
caption_max_output_tokens = 256
caption_batch_size = 1
cpu_allocation_percentage = 0.75
```

GPU 只分配给 caption，转码留在 CPU：单张 L20 无法同时承载“转码 1-GPU Stage + caption 1-GPU Stage”，而 vp9 CPU 转码已在 smoke 中验证可用。

## 产物规模预测（用于事后守恒校验）

| 量 | 预测值 | 依据 |
| --- | --- | --- |
| 输入视频 | 232 | tar 成员计数 |
| 每视频 clip 数 | 6 | `floor((60-2)/10)+1`，实测时长 60.0–60.14s |
| 总 clip / caption window | ≈1392 | 232×6；10s clip≈300 帧，window_size=256 → 每 clip 恰 1 window |
| metas/v0 JSON | 1392 | 每 clip 一个 |
| processed_videos / processed_clip_chunks | 232 / 232 | 6<32，每视频 1 chunk |
| 输出 VP9 clip 体积 | ~2.5 GB | 采样 clip 1.78MB × 1392 |

## 实验设计（单一目标 shard）

本轮只跑主流的 streaming 模式，不做 batch 对照：

- **完整全量运行**：`--executor xenna --execution-mode streaming --generate-captions`，输入 = 全部 232 视频。这是本轮唯一运行，也是过程观测对象（重叠执行 / 背压 / 自动扩缩）。选 streaming 是因为它能重叠 reader/transcode/caption/writer，更贴近真实吞吐形态。

## 节点放置推演（单 GPU 决定拓扑）

| Stage | 资源/worker | 落点 |
| --- | --- | --- |
| FilePartitioning | 0.5 CPU，1 worker | 任一节点，列全量文件 |
| VideoReader / FixedStride | 1 CPU | CPU 节点 + gpu1 富余 CPU |
| ClipTranscoding(vp9) | 2 CPU | CPU 节点为主 |
| CaptionPreparation | 1 CPU | CPU 节点 |
| CaptionGeneration | 1 CPU + 1 GPU | 只能钉在 gpu1 |
| ClipWriter | 0.25 CPU | 写共享输出路径 |

## 推演（先立预测，跑完后用观测验证）

1. **瓶颈**：单张 L20 上 1 个 caption actor 处理 ~1392 个 window，是全链路吞吐上限。预测现象：GPU 队列一旦饱和，上游各 Stage 的 `actor_idle_time` 抬升、GPU 利用率持续高位。事后用 `captions/s` 反推总时长 ≈ `1392 / captions_per_s`。
2. **加 CPU 无用假设**：GPU 饱和后，ecs2/ecs3 再多 CPU worker 也不会提升端到端吞吐，只会降低 CPU Stage 的排队。
3. **背压 / 对象存储**：Xenna 背压正常时 Ray object store 占用应稳定在某区间，而非随 232 视频线性增长；若持续增长 + spill，说明活跃窗口过大或 writer/GPU 排空不及。
4. **守恒**：按“产物规模预测”逐项核对 1392/232；任何缺口都要能定位到具体 source video 的 error，不允许静默丢失。

## 过程观测清单

- Ray：`ray status`（节点数、CPU/GPU 资源），dashboard 8265。
- GPU：`nvidia-smi` 利用率与显存（caption 阶段应持续高位）。
- Stage 指标：报告 JSON 里各 Stage 的 `process_time_sum`、`actor_idle_time`、`num_items_processed`。
- 吞吐：观察 caption 生成速率，反推端到端时长。
- 对象存储：Ray object store 使用量是否稳定，是否发生 spill。

## 输出校验清单

跑完后对输出目录做守恒核对：

```text
clips/**/*.mp4                     期望 1392
metas/v0/**/*.json                 期望 1392
processed_videos/**/*.json         期望 232
processed_clip_chunks/**/*.json    期望 232
```

- 抽样打开若干 `metas/v0/*.json`，确认 `windows` 内含 `qwen2.5_caption` 且 caption 文本非空、语义合理。
- 报告 JSON `status: succeeded`，`output_tasks` 与视频数一致。
- 若数量对不上，逐条定位到具体 source video 的 error 字段，不接受静默丢失。

## 失败模式（提前预判并堵住）

| 风险 | 后果 | 处置 |
| --- | --- | --- |
| 无共享盘 | reader 在没数据的节点找不到文件 / writer 输出散落各机 | 建共享 FS，或把所有 Stage 钉到持有数据的节点 |
| ecs1 与 gpu1 venv/python 不一致（3.12.3 vs 3.12.14，vllm 只在 gpu1） | Ray worker 反序列化/依赖不一致，caption 起不来 | 统一到同一 runtime venv；caption worker 必须用带 vllm 的那套 |
| ecs2/ecs3 空 | 声称 4 节点实为 1–2 节点 | 要么补齐 workers，要么诚实按实际节点数报告 |

## 开跑前必须为真的前置条件

1. 起 Ray 集群，`ray status` 显示预期节点数 + 恰好 1 GPU，启动时带 `RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1`。
2. 建统一共享路径（如 NFS 导出 `ecs2:/data` 挂到四机 `/mnt/curator-flow`），input/output/report 都放这里。
3. 四节点同一 venv/python + 同一 commit，caption 节点带 vllm 与本地 Qwen 模型。
4. 把 shard 全量解包成 232 个视频（现在只有 8）。
5. workers（ecs2/ecs3）装好 repo/venv；否则如实降级为 ecs1+gpu1 的 2 节点异构集群并注明。

## 备注

本篇为只读核对得出的设计与推演，未改动任何机器状态、未启动集群、未跑 pipeline。前置条件补齐后，再执行“完整全量运行”并回填实测数据。
