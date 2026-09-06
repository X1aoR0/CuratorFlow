"""Build the Curator-native video curation pipeline.

CuratorFlow deliberately owns only pipeline composition and experiment configuration.
Every media transformation in this module is implemented by a NeMo Curator stage.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class VideoPipelineConfig:
    """Configuration for one Curator video pipeline experiment."""

    input_path: Path
    output_path: Path
    model_dir: Path
    input_file_list: Path | None = None
    video_limit: int | None = None
    clip_len_s: float = 10.0
    clip_stride_s: float = 10.0
    min_clip_length_s: float = 2.0
    limit_clips: int = 0
    transcode_encoder: str = "libvpx-vp9"
    transcode_cpus_per_worker: float = 2.0
    transcode_encoder_threads: int = 1
    transcode_batch_size: int = 2
    clips_per_chunk: int = 32
    generate_captions: bool = False
    caption_model: str = "qwen2.5"
    caption_prompt_variant: str = "default"
    caption_sampling_fps: float = 2.0
    caption_window_size: int = 256
    caption_remainder_threshold: int = 128
    caption_batch_size: int = 1
    caption_max_output_tokens: int = 256
    caption_num_workers: int | None = 1
    upload_clips: bool = True
    dry_run: bool = False
    verbose: bool = False
    # 简单的校验，输入目录得有
    # clip_len得是正的
    # 编码器得是支持的
    def validate(self) -> None:
        if not self.input_path.exists():
            raise FileNotFoundError(f"Video input does not exist: {self.input_path}")
        if self.input_file_list is not None and not self.input_file_list.is_file():
            raise FileNotFoundError(f"Video input file list does not exist: {self.input_file_list}")
        if self.clip_len_s <= 0 or self.clip_stride_s <= 0:
            raise ValueError("clip_len_s and clip_stride_s must be positive.")
        if self.min_clip_length_s <= 0:
            raise ValueError("min_clip_length_s must be positive.")
        if self.transcode_encoder not in {"libvpx-vp9", "libopenh264", "h264_nvenc"}:
            raise ValueError(f"Unsupported transcode encoder: {self.transcode_encoder}")
        if self.caption_num_workers is not None and self.caption_num_workers <= 0:
            raise ValueError("caption_num_workers must be positive or None.")

    def to_dict(self) -> dict[str, Any]:
        values = asdict(self)
        for key in ("input_path", "output_path", "model_dir", "input_file_list"):
            if values[key] is not None:
                values[key] = str(values[key])
        return values

    def file_paths(self) -> str | list[str]:
        if self.input_file_list is None:
            return str(self.input_path)
        return [line.strip() for line in self.input_file_list.read_text(encoding="utf-8").splitlines() if line.strip()]

# 惰性导入nemo_curator
def _load_curator_components() -> dict[str, Any]:
    """Import Curator lazily so config/report tests run without Curator installed."""

    from nemo_curator.pipeline import Pipeline
    from nemo_curator.stages.file_partitioning import FilePartitioningStage
    from nemo_curator.stages.resources import Resources
    from nemo_curator.stages.video.caption.caption_generation import CaptionGenerationStage
    from nemo_curator.stages.video.caption.caption_preparation import CaptionPreparationStage
    from nemo_curator.stages.video.clipping.clip_extraction_stages import (
        ClipTranscodingStage,
        FixedStrideExtractorStage,
    )
    from nemo_curator.stages.video.io.video_reader import VideoReaderStage

    from curator_flow.clip_writer import FullErrorClipWriterStage

    return {
        "Pipeline": Pipeline,
        "Resources": Resources,
        "FilePartitioningStage": FilePartitioningStage,
        "VideoReaderStage": VideoReaderStage,
        "FixedStrideExtractorStage": FixedStrideExtractorStage,
        "ClipTranscodingStage": ClipTranscodingStage,
        "CaptionPreparationStage": CaptionPreparationStage,
        "CaptionGenerationStage": CaptionGenerationStage,
        "ClipWriterStage": FullErrorClipWriterStage,
    }


def build_video_pipeline(config: VideoPipelineConfig) -> Any:
    """Compose the requested pipeline exclusively from Curator stages."""

    config.validate()
    c = _load_curator_components()
    pipeline = c["Pipeline"](
        name="curator_flow_video",
        description="Curator-native video clipping and captioning experiment",
    )

    pipeline.add_stage(
        c["FilePartitioningStage"](
            file_paths=config.file_paths(),
            files_per_partition=1,
            file_extensions=[".mp4", ".mov", ".avi", ".mkv", ".webm"],
            limit=config.video_limit,
        )
    )
    pipeline.add_stage(c["VideoReaderStage"](input_path=str(config.input_path), verbose=config.verbose))
    pipeline.add_stage(
        c["FixedStrideExtractorStage"](
            clip_len_s=config.clip_len_s,
            clip_stride_s=config.clip_stride_s,
            min_clip_length_s=config.min_clip_length_s,
            limit_clips=config.limit_clips,
            verbose=config.verbose,
        )
    )
    pipeline.add_stage(
        c["ClipTranscodingStage"](
            num_cpus_per_worker=config.transcode_cpus_per_worker,
            encoder=config.transcode_encoder,
            encoder_threads=config.transcode_encoder_threads,
            encode_batch_size=config.transcode_batch_size,
            use_hwaccel=config.transcode_encoder == "h264_nvenc",
            use_input_bit_rate=False,
            num_clips_per_chunk=config.clips_per_chunk,
            verbose=config.verbose,
        )
    )

    if config.generate_captions:
        # Processor files live in the shared offline HF cache, so preparation can
        # scale across CPU nodes and leave all GPUs to caption generation.
        caption_preparation = c["CaptionPreparationStage"](
            model_variant=config.caption_model,
            prompt_variant=config.caption_prompt_variant,
            sampling_fps=config.caption_sampling_fps,
            window_size=config.caption_window_size,
            remainder_threshold=config.caption_remainder_threshold,
            generate_previews=False,
            verbose=config.verbose,
        ).with_(
            resources=c["Resources"](cpus=1.0),
            runtime_env={
                "env_vars": {
                    "HF_HOME": "/mnt/curator-flow/hf-cache",
                    "HF_HUB_OFFLINE": "1",
                    "TRANSFORMERS_OFFLINE": "1",
                }
            },
            num_workers=None,
        )
        pipeline.add_stage(caption_preparation)

        caption_generation = c["CaptionGenerationStage"](
            model_dir=str(config.model_dir),
            model_variant=config.caption_model,
            caption_batch_size=config.caption_batch_size,
            max_output_tokens=config.caption_max_output_tokens,
            disable_mmcache=True,
            verbose=config.verbose,
        ).with_(
            resources=c["Resources"](cpus=1.0, gpus=1.0),
            runtime_env={
                "env_vars": {
                    "HF_HOME": "/mnt/curator-flow/hf-cache",
                    "HF_HUB_OFFLINE": "1",
                    "TRANSFORMERS_OFFLINE": "1",
                    "VLLM_USE_FLASHINFER_SAMPLER": "0",
                }
            },
            num_workers=config.caption_num_workers,
        )
        pipeline.add_stage(caption_generation)

    pipeline.add_stage(
        c["ClipWriterStage"](
            output_path=str(config.output_path),
            input_path=str(config.input_path),
            upload_clips=config.upload_clips,
            dry_run=config.dry_run,
            generate_embeddings=False,
            generate_previews=False,
            generate_captions=config.generate_captions,
            caption_models=[config.caption_model] if config.generate_captions else [],
            enhanced_caption_models=[],
            verbose=config.verbose,
        )
    )
    return pipeline

