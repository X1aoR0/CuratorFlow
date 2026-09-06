from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from curator_flow.build_video_pipelines import VideoPipelineConfig, build_video_pipeline
from curator_flow.run_video_pipelines import _artifact_metrics, preflight


class _FakeStage:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.name = self.__class__.__name__
        self.overrides = {}

    def with_(self, **kwargs):
        self.overrides.update(kwargs)
        return self


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
    components["Resources"] = lambda **kwargs: kwargs
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
    with patch("curator_flow.build_video_pipelines._load_curator_components", return_value=_components()):
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
    with patch("curator_flow.build_video_pipelines._load_curator_components", return_value=_components()):
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


def test_preflight_rejects_captioning_without_enough_cluster_gpus(tmp_path: Path) -> None:
    fake_ray = SimpleNamespace(
        is_initialized=lambda: False,
        init=lambda **kwargs: None,
        cluster_resources=lambda: {"GPU": 0},
        shutdown=lambda: None,
    )
    with (
        patch("curator_flow.run_video_pipelines.shutil.which", return_value="/usr/bin/tool"),
        patch.dict("sys.modules", {"ray": fake_ray}),
        pytest.raises(RuntimeError, match="requires 1 Ray GPUs"),
    ):
        preflight(_config(tmp_path, captions=True))


def test_caption_pipeline_uses_configured_gpu_workers(tmp_path: Path) -> None:
    config = _config(tmp_path, captions=True)
    config = VideoPipelineConfig(**{**config.__dict__, "caption_num_workers": 4})
    with patch("curator_flow.build_video_pipelines._load_curator_components", return_value=_components()):
        pipeline = build_video_pipeline(config)

    preparation = pipeline.stages[4]
    generation = pipeline.stages[5]
    assert preparation.overrides["resources"] == {"cpus": 1.0}
    assert preparation.overrides["num_workers"] is None
    assert generation.overrides["resources"] == {"cpus": 1.0, "gpus": 1.0}
    assert generation.overrides["num_workers"] == 4


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


def test_config_can_select_exact_input_files(tmp_path: Path) -> None:
    config = _config(tmp_path)
    selected = config.input_path / "sample.mp4"
    file_list = tmp_path / "pending.txt"
    file_list.write_text(f"{selected}\n", encoding="utf-8")
    config = VideoPipelineConfig(**{**config.__dict__, "input_file_list": file_list})

    assert config.file_paths() == [str(selected)]

