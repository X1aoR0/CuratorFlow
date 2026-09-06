"""Small extensions to Curator's native clip writer."""

from __future__ import annotations

import json
import os
from pathlib import Path

from nemo_curator.stages.video.io.clip_writer import ClipWriterStage
from nemo_curator.tasks.video import Clip, ClipStats, VideoMetadata


class FullErrorClipWriterStage(ClipWriterStage):
    """Write the complete ``clip.errors`` mapping into clip metadata.

    Curator currently serializes ``list(clip.errors)``, which keeps only the
    error keys. This subclass delegates the entire write to Curator, then
    replaces that lossy list with the original error dictionary.
    """

    def _write_clip_metadata(
        self,
        clip: Clip,
        video_metadata: VideoMetadata,
        *,
        filtered: bool = False,
    ) -> ClipStats:
        stats = super()._write_clip_metadata(clip, video_metadata, filtered=filtered)
        if not clip.errors or self.dry_run:
            return stats

        destination = self._get_clip_uri(
            clip.uuid,
            self.get_output_path_metas(self.output_path, "v0"),
            "json",
        )
        destination = Path(destination)
        metadata = json.loads(destination.read_text(encoding="utf-8"))
        metadata["errors"] = dict(clip.errors)
        temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
        temporary.replace(destination)
        return stats
