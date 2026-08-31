"""Shared data contracts for CuratorFlow pipeline stages."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

MediaType = Literal["image", "video"]
UriType = Literal["local_file", "tar_member", "s3", "oss", "http"]

SUPPORTED_MEDIA_TYPES: set[str] = {"image", "video"}
SUPPORTED_URI_TYPES: set[str] = {"local_file", "tar_member", "s3", "oss", "http"}


@dataclass(frozen=True)
class SourceRecord:
    """A validated row from the dataset manifest."""

    sample_id: str
    dataset_name: str
    dataset_version: str
    media_type: MediaType
    uri: str
    uri_type: UriType
    text: str | None
    source: str
    license: str | None
    manifest_path: Path
    manifest_line: int
    shard_id: str | None = None
    member_name: str | None = None
    checksum_sha256: str | None = None
    bytes: int | None = None
    mime_type: str | None = None
    language: str | None = None
    created_at: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedRecord:
    """A source record whose media URI has been resolved and verified on disk.

    这是 MediaResolver 的输出：在把记录交给后续 Curator 图片/视频 Stage 之前，
    先确认媒体真的存在、能读、并算出内容 hash 用于去重和血缘追踪。
    """

    # 原始已校验记录，保留全部 manifest 字段
    source: SourceRecord
    # 解析后的可读绝对路径（tar_member 时指向 tar 容器本身）
    resolved_uri: str
    # 媒体是否成功定位并可读
    exists: bool
    # 记录媒体的字节数（tar_member 时是成员字节数，不是整个 tar）
    bytes_size: int | None
    # 记录媒体内容 sha256（tar_member 时是成员内容，不是整个 tar）
    content_sha256: str | None
    # 容器文件 sha256，仅 tar_member 时有值，用于校验 tar 本身
    container_sha256: str | None
    # tar 内成员名，仅 tar_member 时有值
    member_name: str | None
    # 读取或定位失败时的错误说明
    read_error: str | None = None

    @property
    def sample_id(self) -> str:
        return self.source.sample_id

    @property
    def media_type(self) -> str:
        return self.source.media_type


@dataclass(frozen=True)
class RejectedRecord:
    """A manifest-level rejection emitted before media processing starts."""

    rejected_id: str
    parent_sample_id: str | None
    media_type: str | None
    uri: str | None
    reject_reasons: list[str]
    stage_status: dict[str, str]
    error: str | None
    manifest_path: Path
    manifest_line: int
    raw: dict[str, Any] | None = None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "rejected_id": self.rejected_id,
            "parent_sample_id": self.parent_sample_id,
            "media_type": self.media_type,
            "uri": self.uri,
            "reject_reasons": self.reject_reasons,
            "stage_status": self.stage_status,
            "error": self.error,
            "manifest_path": str(self.manifest_path),
            "manifest_line": self.manifest_line,
            "raw": self.raw,
        }



