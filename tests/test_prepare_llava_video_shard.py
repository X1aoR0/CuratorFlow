import io
import tarfile
from pathlib import Path

import pytest

from scripts.data.prepare_llava_video_shard import _safe_relative_path, extract_videos


def test_extract_videos_is_bounded_and_skips_non_video_members(tmp_path: Path) -> None:
    shard = tmp_path / "sample.tar"
    with tarfile.open(shard, "w") as archive:
        for name, payload in [("60s/a.mp4", b"a"), ("60s/b.webm", b"bb"), ("60s/meta.json", b"{}")]:
            info = tarfile.TarInfo(name=name)
            info.size = len(payload)
            archive.addfile(info, io.BytesIO(payload))

    videos = extract_videos(shard, tmp_path / "videos", max_videos=1)

    assert len(videos) == 1
    assert videos[0].relative_to(tmp_path / "videos").as_posix() == "60s/a.mp4"
    assert videos[0].read_bytes() == b"a"


def test_safe_relative_path_rejects_traversal() -> None:
    with pytest.raises(ValueError, match="Unsafe tar member"):
        _safe_relative_path("../escape.mp4")

