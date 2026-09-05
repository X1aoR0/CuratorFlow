"""Run and observe the Curator-native video pipeline."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import socket
import time
from collections import Counter
from pathlib import Path
from typing import Any

from curator_flow.build_video_pipelines import VideoPipelineConfig, build_video_pipeline


def preflight(config: VideoPipelineConfig) -> dict[str, Any]:
    """Validate host capabilities before starting an expensive Curator run."""

    config.validate()
    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        raise RuntimeError("ffmpeg and ffprobe are required by Curator video stages.")
    if config.generate_captions:
        try:
            import torch
        except ImportError as exc:
            raise RuntimeError("CaptionGenerationStage requires PyTorch and one GPU.") from exc
        if not torch.cuda.is_available():
            raise RuntimeError("CaptionGenerationStage requires a CUDA GPU; none is visible on this host.")

    input_files = sorted(
        path
        for path in config.input_path.rglob("*")
        if path.is_file() and path.suffix.lower() in {".mp4", ".mov", ".avi", ".mkv", ".webm"}
    )
    if not input_files:
        raise RuntimeError(f"No supported videos found under {config.input_path}.")
    return {
        "hostname": socket.gethostname(),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "input_files": len(input_files),
        "input_bytes": sum(path.stat().st_size for path in input_files),
        "ffmpeg": shutil.which("ffmpeg"),
        "ffprobe": shutil.which("ffprobe"),
    }


def _build_executor(name: str, execution_mode: str) -> Any:
    if name == "xenna":
        from nemo_curator.backends.xenna import XennaExecutor

        return XennaExecutor(config={"execution_mode": execution_mode, "cpu_allocation_percentage": 0.75})
    if name == "ray_data":
        from nemo_curator.backends.ray_data import RayDataExecutor

        return RayDataExecutor()
    raise ValueError(f"Unsupported executor: {name}")


def _stage_metrics(tasks: list[Any]) -> dict[str, Any]:
    from nemo_curator.tasks.utils import TaskPerfUtils

    return TaskPerfUtils.aggregate_task_metrics(tasks)


def _artifact_metrics(output_path: Path) -> dict[str, Any]:
    files = [path for path in output_path.rglob("*") if path.is_file()] if output_path.exists() else []
    suffix_counts = Counter(path.suffix or "<none>" for path in files)
    metadata_paths = list((output_path / "metas" / "v0").rglob("*.json")) if output_path.exists() else []
    metadata = [json.loads(path.read_text(encoding="utf-8")) for path in metadata_paths]
    return {
        "file_count": len(files),
        "total_bytes": sum(path.stat().st_size for path in files),
        "files_by_suffix": dict(sorted(suffix_counts.items())),
        "clip_files": len(list((output_path / "clips").rglob("*.mp4"))) if output_path.exists() else 0,
        "metadata_files": len(metadata_paths),
        "valid_clips": sum(bool(item.get("valid")) for item in metadata),
        "captioned_windows": sum(
            any(key.endswith("_caption") for key in window)
            for item in metadata
            for window in item.get("windows", [])
        ),
    }


def run_video_experiment(
    config: VideoPipelineConfig,
    *,
    executor_name: str,
    execution_mode: str,
    report_path: Path,
) -> dict[str, Any]:
    started_at = time.time()
    report: dict[str, Any] = {
        "status": "running",
        "config": config.to_dict(),
        "executor": executor_name,
        "execution_mode": execution_mode,
    }
    try:
        environment = preflight(config)
        pipeline = build_video_pipeline(config)
        report.update({"environment": environment, "stages": [stage.name for stage in pipeline.stages]})
        tasks = pipeline.run(_build_executor(executor_name, execution_mode)) or []
        elapsed = time.time() - started_at
        artifacts = _artifact_metrics(config.output_path)
        report.update(
            {
                "status": "succeeded",
                "wall_time_s": elapsed,
                "output_tasks": len(tasks),
                "throughput_videos_per_s": environment["input_files"] / elapsed if elapsed else 0.0,
                "throughput_clips_per_s": artifacts["clip_files"] / elapsed if elapsed else 0.0,
                "stage_metrics": _stage_metrics(tasks),
                "artifacts": artifacts,
            }
        )
    except Exception as exc:
        report.update({"status": "failed", "wall_time_s": time.time() - started_at, "error": repr(exc)})
        raise
    finally:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a Curator-native video curation experiment.")
    parser.add_argument("--input-path", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--model-dir", type=Path, default=Path("models"))
    parser.add_argument("--executor", choices=("xenna", "ray_data"), default="xenna")
    parser.add_argument("--execution-mode", choices=("batch", "streaming"), default="batch")
    parser.add_argument("--video-limit", type=int)
    parser.add_argument("--clip-len-s", type=float, default=10.0)
    parser.add_argument("--clip-stride-s", type=float, default=10.0)
    parser.add_argument("--min-clip-length-s", type=float, default=2.0)
    parser.add_argument("--limit-clips", type=int, default=0)
    parser.add_argument("--transcode-encoder", choices=("libvpx-vp9", "libopenh264", "h264_nvenc"), default="libvpx-vp9")
    parser.add_argument("--transcode-cpus-per-worker", type=float, default=2.0)
    parser.add_argument("--transcode-batch-size", type=int, default=2)
    parser.add_argument("--generate-captions", action="store_true")
    parser.add_argument("--caption-model", choices=("qwen2.5", "qwen3"), default="qwen2.5")
    parser.add_argument("--caption-batch-size", type=int, default=1)
    parser.add_argument("--caption-max-output-tokens", type=int, default=256)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()

# pipeline启动
def main() -> int:
    # 把命令行参数解析出来
    args = parse_args()
    # Pipeline的启动参数
    config = VideoPipelineConfig(
        input_path=args.input_path.resolve(),
        output_path=args.output_path.resolve(),
        model_dir=args.model_dir.resolve(),
        video_limit=args.video_limit,
        clip_len_s=args.clip_len_s,
        clip_stride_s=args.clip_stride_s,
        min_clip_length_s=args.min_clip_length_s,
        limit_clips=args.limit_clips,
        transcode_encoder=args.transcode_encoder,
        transcode_cpus_per_worker=args.transcode_cpus_per_worker,
        transcode_batch_size=args.transcode_batch_size,
        generate_captions=args.generate_captions,
        caption_model=args.caption_model,
        caption_batch_size=args.caption_batch_size,
        caption_max_output_tokens=args.caption_max_output_tokens,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )
    run_video_experiment(
        config,
        executor_name=args.executor,
        execution_mode=args.execution_mode,
        report_path=args.report_path.resolve(),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

