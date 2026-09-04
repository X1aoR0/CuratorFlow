"""Download and extract a bounded LLaVA-OneVision-2 video shard sample.

The source shard is a WebDataset tar archive. Curator's VideoReaderStage expects
individual video files, so this script performs only dataset preparation: download
one explicit shard and extract a bounded number of video members safely.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import tarfile
from pathlib import Path, PurePosixPath

DEFAULT_REPO_ID = "mvp-lab/LLaVA-OneVision-2-Data"
DEFAULT_SHARD = "mid_training_video/60s_rest/train_00480_of_10809.tar"
DEFAULT_ENDPOINT = "https://hf-mirror.com"
VIDEO_SUFFIXES = {".mp4", ".mov", ".avi", ".mkv", ".webm"}


def download_shard(repo_id: str, filename: str, download_root: Path, endpoint: str) -> Path:
    """Download exactly one dataset file and return its local path."""

    os.environ.setdefault("HF_HUB_DISABLE_XET", "1")
    from huggingface_hub import hf_hub_download

    return Path(
        hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            repo_type="dataset",
            local_dir=download_root,
            endpoint=endpoint,
        )
    )


def _safe_relative_path(member_name: str) -> Path:
    path = PurePosixPath(member_name)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"Unsafe tar member path: {member_name}")
    return Path(*path.parts)


def extract_videos(shard_path: Path, video_root: Path, max_videos: int) -> list[Path]:
    """Extract up to max_videos regular video members without using tar.extract."""

    extracted: list[Path] = []
    video_root.mkdir(parents=True, exist_ok=True)
    with tarfile.open(shard_path, "r:*") as archive:
        for member in archive:
            if len(extracted) >= max_videos:
                break
            if not member.isfile() or PurePosixPath(member.name).suffix.lower() not in VIDEO_SUFFIXES:
                continue
            source = archive.extractfile(member)
            if source is None:
                continue
            destination = video_root / _safe_relative_path(member.name)
            destination.parent.mkdir(parents=True, exist_ok=True)
            with source, destination.open("wb") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)
            extracted.append(destination)
    return extracted


def ensure_disk_headroom(path: Path, minimum_free_gb: float) -> None:
    path.mkdir(parents=True, exist_ok=True)
    free_gb = shutil.disk_usage(path).free / (1024**3)
    if free_gb < minimum_free_gb:
        raise RuntimeError(f"Insufficient disk space under {path}: {free_gb:.1f} GiB free, need {minimum_free_gb:.1f} GiB.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare one LLaVA-OneVision-2 video shard for Curator.")
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID)
    parser.add_argument("--filename", default=DEFAULT_SHARD)
    parser.add_argument("--download-root", type=Path, required=True)
    parser.add_argument("--video-root", type=Path, required=True)
    parser.add_argument("--report-path", type=Path, required=True)
    parser.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    parser.add_argument("--max-videos", type=int, default=8)
    parser.add_argument("--minimum-free-gb", type=float, default=8.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.max_videos <= 0:
        raise ValueError("max_videos must be positive.")
    ensure_disk_headroom(args.download_root, args.minimum_free_gb)
    shard_path = download_shard(args.repo_id, args.filename, args.download_root, args.endpoint)
    videos = extract_videos(shard_path, args.video_root, args.max_videos)
    if not videos:
        raise RuntimeError(f"No video members found in {shard_path}.")

    report = {
        "repo_id": args.repo_id,
        "filename": args.filename,
        "shard_path": str(shard_path.resolve()),
        "shard_bytes": shard_path.stat().st_size,
        "video_root": str(args.video_root.resolve()),
        "video_count": len(videos),
        "video_bytes": sum(path.stat().st_size for path in videos),
        "videos": [str(path.resolve()) for path in videos],
    }
    args.report_path.parent.mkdir(parents=True, exist_ok=True)
    args.report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

