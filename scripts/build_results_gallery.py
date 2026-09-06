"""Build a static, paginated browser for Curator clip outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def build_index(output_root: Path, run_dir: Path, gallery_dir: Path) -> dict:
    metadata_dir = run_dir / "metas" / "v0"
    items = []
    for metadata_path in metadata_dir.glob("*.json"):
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        clip_path = Path(metadata["clip_location"])
        if not clip_path.is_file():
            continue
        items.append(
            {
                "id": metadata["span_uuid"],
                "source_video": metadata["source_video"],
                "duration_span": metadata["duration_span"],
                "width": metadata.get("width"),
                "height": metadata.get("height"),
                "framerate": metadata.get("framerate"),
                "window_count": len(metadata.get("windows", [])),
                "valid": metadata.get("valid", False),
                "video_url": "/assets/" + clip_path.relative_to(run_dir).as_posix(),
                "metadata_url": "/assets/" + metadata_path.relative_to(run_dir).as_posix(),
            }
        )
    items.sort(key=lambda item: (item["source_video"], item["duration_span"], item["id"]))
    result = {
        "run_id": run_dir.name,
        "item_count": len(items),
        "page_size": 10,
        "items": items,
    }
    gallery_dir.mkdir(parents=True, exist_ok=True)
    (gallery_dir / "results.json").write_text(
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Build the Curator results gallery index.")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--gallery-dir", type=Path, required=True)
    args = parser.parse_args()
    result = build_index(args.output_root.resolve(), args.run_dir.resolve(), args.gallery_dir.resolve())
    print(json.dumps({key: result[key] for key in ("run_id", "item_count", "page_size")}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
