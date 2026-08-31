"""Build the M1 smoke dataset from local NeMo Curator fixtures.

The script intentionally reuses media assets from a local Curator checkout
instead of generating synthetic images or videos. It creates:

- Curator-compatible image WebDataset tar shard.
- Curator-compatible video explicit file list JSON.
- CuratorFlow dataset manifest JSONL used by the local closed-loop pipeline.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tarfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_CURATOR_ROOT = Path("~/work/ai/Curator").expanduser()
DEFAULT_OUTPUT_ROOT = Path("data/smoke")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def rel(path: Path, repo_root: Path) -> str:
    return path.resolve().relative_to(repo_root.resolve()).as_posix()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def inspect_single_image_member(tar_path: Path) -> tuple[str, int, str]:
    with tarfile.open(tar_path, "r") as tf:
        image_members = [member for member in tf.getmembers() if member.isfile() and member.name.lower().endswith(".jpg")]
        if len(image_members) != 1:
            names = ", ".join(member.name for member in image_members)
            raise ValueError(f"Expected exactly one jpg member in {tar_path}, got {len(image_members)}: {names}")

        member = image_members[0]
        extracted = tf.extractfile(member)
        if extracted is None:
            raise ValueError(f"Cannot read tar member {member.name} from {tar_path}")
        payload = extracted.read()

    return member.name, len(payload), sha256_bytes(payload)


def build_dataset(curator_root: Path, output_root: Path, dataset_version: str, repo_root: Path) -> dict[str, Path]:
    source_image_tar = curator_root / "tests" / "image_data" / "00000.tar"
    source_video = curator_root / "tests" / "stages" / "video" / "caption" / "fixtures" / "test_video.mp4"

    if not source_image_tar.exists():
        raise FileNotFoundError(f"Missing Curator image fixture: {source_image_tar}")
    if not source_video.exists():
        raise FileNotFoundError(f"Missing Curator video fixture: {source_video}")

    image_dir = output_root / "raw" / "image_wds"
    video_dir = output_root / "raw" / "videos"
    manifest_dir = output_root / "manifests"
    image_dir.mkdir(parents=True, exist_ok=True)
    video_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    image_tar = image_dir / "00000.tar"
    video_file = video_dir / "test_video.mp4"
    shutil.copy2(source_image_tar, image_tar)
    shutil.copy2(source_video, video_file)

    image_member, image_member_bytes, image_member_sha256 = inspect_single_image_member(image_tar)
    created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    seed_rows = [
        {
            "sample_id": "curator_image_000000",
            "dataset_name": "m1_smoke",
            "dataset_version": dataset_version,
            "media_type": "image",
            "uri": rel(image_tar, repo_root),
            "uri_type": "tar_member",
            "shard_id": "image_wds_00000",
            "member_name": image_member,
            "text": "A sample JPEG image from the NeMo Curator image reader fixture.",
            "source": "nemo_curator/tests/image_data/00000.tar",
            "license": "Apache-2.0",
            "checksum_sha256": image_member_sha256,
            "bytes": image_member_bytes,
            "mime_type": "image/jpeg",
            "language": "en",
            "created_at": created_at,
            "metadata": {
                "curator_format": "image_webdataset_tar",
                "container_uri": rel(image_tar, repo_root),
                "container_checksum_sha256": sha256_file(image_tar),
                "source_repo": str(source_image_tar),
            },
        },
        {
            "sample_id": "curator_video_test_video",
            "dataset_name": "m1_smoke",
            "dataset_version": dataset_version,
            "media_type": "video",
            "uri": rel(video_file, repo_root),
            "uri_type": "local_file",
            "text": "A short MP4 sample video from the NeMo Curator video caption fixture.",
            "source": "nemo_curator/tests/stages/video/caption/fixtures/test_video.mp4",
            "license": "Apache-2.0",
            "checksum_sha256": sha256_file(video_file),
            "bytes": video_file.stat().st_size,
            "mime_type": "video/mp4",
            "language": "en",
            "created_at": created_at,
            "metadata": {
                "curator_format": "video_file",
                "source_repo": str(source_video),
            },
        },
    ]

    dataset_manifest = manifest_dir / "dataset_manifest.jsonl"
    video_file_list = manifest_dir / "video_file_list.json"
    curator_inputs = manifest_dir / "curator_inputs.json"
    readme = output_root / "README.md"

    write_jsonl(dataset_manifest, seed_rows)
    write_json(video_file_list, [str(video_file.resolve())])
    write_json(
        curator_inputs,
        {
            "image": {
                "format": "webdataset_tar",
                "file_paths": rel(image_dir, repo_root),
                "file_extensions": [".tar"],
                "members": [image_member],
            },
            "video": {
                "format": "explicit_file_list_json",
                "input_video_path": str(video_dir.resolve()),
                "input_list_json_path": str(video_file_list.resolve()),
                "file_extensions": [".mp4", ".mov", ".avi", ".mkv", ".webm"],
            },
            "unified_manifest": rel(dataset_manifest, repo_root),
        },
    )
    readme.write_text(
        "\n".join(
            [
                "# CuratorFlow Smoke Dataset",
                "",
                "This local dataset is assembled from media fixtures in `~/work/ai/Curator`.",
                "",
                "Standard inputs:",
                "",
                "- `raw/image_wds/00000.tar`: Curator-compatible image WebDataset tar shard.",
                "- `raw/videos/test_video.mp4`: Curator-compatible local video file.",
                "- `manifests/video_file_list.json`: explicit absolute-path list for Curator `VideoReader` / `ClientPartitioningStage`.",
                "- `manifests/dataset_manifest.jsonl`: CuratorFlow's unified multimodal dataset manifest.",
                "",
                "The repository ignores `data/` so this directory is local runtime state.",
                "",
            ]
        ),
        encoding="utf-8",
    )

    return {
        "output_root": output_root,
        "dataset_manifest": dataset_manifest,
        "video_file_list": video_file_list,
        "curator_inputs": curator_inputs,
        "image_tar": image_tar,
        "video_file": video_file,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the CuratorFlow M1 smoke dataset from Curator fixtures.")
    parser.add_argument("--curator-root", type=Path, default=DEFAULT_CURATOR_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--dataset-version", default="2026-08-31")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo_root = Path.cwd()
    paths = build_dataset(args.curator_root.expanduser(), args.output_root, args.dataset_version, repo_root)

    print("Smoke dataset created:")
    for key, value in paths.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
