from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from curator_flow.experiments.video import _artifact_metrics, preflight
from curator_flow.pipelines.video import VideoPipelineConfig, build_video_pipeline


class _FakeStage:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.name = self.__class__.__name__


class _FakePipeline:
    def __init__(self, name, description):
        self.name = name
        self.description = description
        self.stages = []

    def add_stage(self, stage):
        self.stages.append(stage)
        return self


def _components():
    names = (
        "FilePartitioningStage",
        "VideoReaderStage",
        "FixedStrideExtractorStage",
        "ClipTranscodingStage",
        "CaptionPreparationStage",
        "CaptionGenerationStage",
        "ClipWriterStage",
    )
    components = {name: type(name, (_FakeStage,), {}) for name in names}
    components["Pipeline"] = _FakePipeline
    return components


def _config(tmp_path: Path, *, captions: bool = False) -> VideoPipelineConfig:
    input_path = tmp_path / "videos"
    input_path.mkdir()
    (input_path / "sample.mp4").write_bytes(b"video")
    return VideoPipelineConfig(
        input_path=input_path,
        output_path=tmp_path / "output",
        model_dir=tmp_path / "models",
        generate_captions=captions,
    )


def test_cpu_pipeline_uses_only_curator_stages(tmp_path: Path) -> None:
    with patch("curator_flow.pipelines.video._load_curator_components", return_value=_components()):
        pipeline = build_video_pipeline(_config(tmp_path))

    assert [stage.__class__.__name__ for stage in pipeline.stages] == [
        "FilePartitioningStage",
        "VideoReaderStage",
        "FixedStrideExtractorStage",
        "ClipTranscodingStage",
        "ClipWriterStage",
    ]
    assert pipeline.stages[0].kwargs["files_per_partition"] == 1
    assert pipeline.stages[3].kwargs["encoder"] == "libvpx-vp9"
    assert pipeline.stages[-1].kwargs["generate_captions"] is False


def test_caption_pipeline_inserts_curator_caption_stages(tmp_path: Path) -> None:
    with patch("curator_flow.pipelines.video._load_curator_components", return_value=_components()):
        pipeline = build_video_pipeline(_config(tmp_path, captions=True))

    assert [stage.__class__.__name__ for stage in pipeline.stages] == [
        "FilePartitioningStage",
        "VideoReaderStage",
        "FixedStrideExtractorStage",
        "ClipTranscodingStage",
        "CaptionPreparationStage",
        "CaptionGenerationStage",
        "ClipWriterStage",
    ]
    assert pipeline.stages[-1].kwargs["caption_models"] == ["qwen2.5"]


def test_preflight_rejects_captioning_without_cuda(tmp_path: Path) -> None:
    fake_torch = SimpleNamespace(cuda=SimpleNamespace(is_available=lambda: False))
    with (
        patch("curator_flow.experiments.video.shutil.which", return_value="/usr/bin/tool"),
        patch.dict("sys.modules", {"torch": fake_torch}),
        pytest.raises(RuntimeError, match="requires a CUDA GPU"),
    ):
        preflight(_config(tmp_path, captions=True))


def test_artifact_metrics_count_curator_outputs(tmp_path: Path) -> None:
    output = tmp_path / "output"
    (output / "clips").mkdir(parents=True)
    (output / "metas" / "v0").mkdir(parents=True)
    (output / "clips" / "a.mp4").write_bytes(b"clip")
    (output / "metas" / "v0" / "a.json").write_text("{}", encoding="utf-8")

    metrics = _artifact_metrics(output)

    assert metrics["clip_files"] == 1
    assert metrics["metadata_files"] == 1
    assert metrics["file_count"] == 2
    assert metrics["total_bytes"] == 6
    assert metrics["valid_clips"] == 0
    assert metrics["captioned_windows"] == 0

