# Smoke Dataset

M1 使用一个很小的 smoke 数据集先验证多模态预训练语料闭环。数据来源优先复用本地 NeMo Curator 仓库 `~/work/ai/Curator` 的测试 fixture，而不是随机生成媒体文件。

## Build

```bash
python3 scripts/data/build_smoke_dataset.py
```

默认输出到 `data/smoke/`。该目录被 `.gitignore` 忽略，属于本地运行态数据。

## Sources

| Modality | Curator fixture | Local output | Format |
| --- | --- | --- | --- |
| Image | `~/work/ai/Curator/tests/image_data/00000.tar` | `data/smoke/raw/image_wds/00000.tar` | WebDataset tar shard with JPEG member |
| Video | `~/work/ai/Curator/tests/stages/video/caption/fixtures/test_video.mp4` | `data/smoke/raw/videos/test_video.mp4` | Local MP4 file |

## Generated Manifests

| File | Purpose |
| --- | --- |
| `data/smoke/manifests/dataset_manifest.jsonl` | CuratorFlow unified multimodal input manifest |
| `data/smoke/manifests/video_file_list.json` | Curator-compatible explicit video file list for `VideoReader` / `ClientPartitioningStage` |
| `data/smoke/manifests/curator_inputs.json` | Pointers to the Curator-compatible image and video inputs |

## Format Notes

Curator image curation commonly starts from WebDataset tar shards and reads JPEG members with `ImageReaderStage`. The smoke image fixture follows that shape:

```text
data/smoke/raw/image_wds/00000.tar
└── 000000.jpg
```

Curator video curation accepts local video directories or explicit JSON file lists. The smoke dataset includes both a local video directory and an absolute-path list:

```json
[
  "/absolute/path/to/data/smoke/raw/videos/test_video.mp4"
]
```

CuratorFlow's closed-loop pipeline should use `dataset_manifest.jsonl` as its canonical input because it carries text, source, license, checksum, and lineage fields needed for pretraining data governance.
