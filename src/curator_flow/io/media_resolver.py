"""Media resolver: turn a validated manifest record into a verified on-disk locator.

这是 pipeline 的第二个算子。DatasetManifestReader 只保证 manifest 这一行本身
字段合法，但并不检查媒体文件是否真的存在、能不能读。MediaResolver 负责：

- 把 manifest 里的 uri 解析成可读的绝对路径。
- 对 local_file / tar_member 做存在性和可读性检查。
- 计算内容 sha256（用于去重和血缘），tar 情况下再单独记录容器 sha256。
- 无法定位或读取的记录降级成 RejectedRecord，而不是抛异常中断整批。

它不做图片解码、视频 probe、模型推理——那些是后续 Curator Stage 的职责。
"""

from __future__ import annotations

import hashlib
import tarfile
from dataclasses import dataclass, field
from pathlib import Path

from curator_flow.contracts import RejectedRecord, ResolvedRecord, SourceRecord

# 一次读多少字节做流式 sha256，避免把大文件整体读进内存
_HASH_CHUNK_SIZE = 1024 * 1024

# MediaResolver 当前真正支持读取的 uri 类型；其余（s3/oss/http）先允许出现在
# manifest 里，但在解析阶段明确拒绝，避免假装成功。
SUPPORTED_RESOLVE_URI_TYPES: set[str] = {"local_file", "tar_member"}


@dataclass(frozen=True)
class MediaResolveResult:
    """Result of resolving a batch of source records."""

    resolved: list[ResolvedRecord] = field(default_factory=list)
    rejected: list[RejectedRecord] = field(default_factory=list)


def resolve_media(records: list[SourceRecord], base_dir: str | Path | None = None) -> MediaResolveResult:
    """Resolve and verify media for each source record.

    Args:
        records: DatasetManifestReader 产出的合法记录列表。
        base_dir: 相对路径的解析根目录，默认用每条记录的 manifest 所在目录的仓库根。
            为了让 manifest 里写的相对路径（如 data/smoke/...）在任意 cwd 下都能解析，
            传入仓库根目录即可。
    """

    resolved: list[ResolvedRecord] = []
    rejected: list[RejectedRecord] = []

    for record in records:
        outcome = resolve_one(record, base_dir)
        if isinstance(outcome, ResolvedRecord):
            resolved.append(outcome)
        else:
            rejected.append(outcome)

    return MediaResolveResult(resolved=resolved, rejected=rejected)

# resolve 一个sample
def resolve_one(record: SourceRecord, base_dir: str | Path | None = None) -> ResolvedRecord | RejectedRecord:
    """Resolve a single record; return ResolvedRecord on success or RejectedRecord on failure."""
    # 现在只支持local_file，或者tar_member
    if record.uri_type not in SUPPORTED_RESOLVE_URI_TYPES:
        return _reject(record, ["unsupported_uri_type"], f"uri_type '{record.uri_type}' is not resolvable yet.")

    resolved_path = _resolve_path(record.uri, base_dir)
    
    # 处理两种类型
    if record.uri_type == "local_file":
        return _resolve_local_file(record, resolved_path)

    # tar_member
    return _resolve_tar_member(record, resolved_path)


def _resolve_local_file(record: SourceRecord, path: Path) -> ResolvedRecord | RejectedRecord:
    # 文件不存在
    if not path.exists():
        return _reject(record, ["missing_file"], f"File not found: {path}")
    # 是目录/或者软连接
    if not path.is_file():
        return _reject(record, ["read_error"], f"Not a regular file: {path}")
    
    # 得到文件大小和sha256
    try:
        content_sha256, size = _sha256_and_size_of_file(path)
    except OSError as exc:
        return _reject(record, ["read_error"], f"Failed to read {path}: {exc}")

    return ResolvedRecord(
        source=record,
        resolved_uri=str(path),
        exists=True,
        bytes_size=size,
        content_sha256=content_sha256,
        container_sha256=None,
        member_name=None,
    )


def _resolve_tar_member(record: SourceRecord, tar_path: Path) -> ResolvedRecord | RejectedRecord:
    # manifest 校验阶段已保证 tar_member 一定带 member_name，这里再兜底一次
    if not record.member_name:
        return _reject(record, ["missing_member_name"], "tar_member record has no member_name.")

    if not tar_path.exists():
        return _reject(record, ["missing_file"], f"Tar container not found: {tar_path}")
    if not tarfile.is_tarfile(tar_path):
        return _reject(record, ["read_error"], f"Not a valid tar archive: {tar_path}")

    try:
        with tarfile.open(tar_path, "r") as tar:
            try:
                member = tar.getmember(record.member_name)
            except KeyError:
                return _reject(
                    record,
                    ["missing_tar_member"],
                    f"Member '{record.member_name}' not found in {tar_path}",
                )

            if not member.isfile():
                return _reject(
                    record,
                    ["read_error"],
                    f"Member '{record.member_name}' in {tar_path} is not a regular file.",
                )

            extracted = tar.extractfile(member)
            if extracted is None:
                return _reject(
                    record,
                    ["read_error"],
                    f"Cannot read member '{record.member_name}' from {tar_path}",
                )
            content_sha256, size = _sha256_and_size_of_stream(extracted)

        container_sha256, _ = _sha256_and_size_of_file(tar_path)
    except OSError as exc:
        return _reject(record, ["read_error"], f"Failed to read {tar_path}: {exc}")

    return ResolvedRecord(
        source=record,
        resolved_uri=str(tar_path),
        exists=True,
        bytes_size=size,
        content_sha256=content_sha256,
        container_sha256=container_sha256,
        member_name=record.member_name,
    )


def _resolve_path(uri: str, base_dir: str | Path | None) -> Path:
    """Resolve a manifest uri into an absolute path.

    绝对路径直接用；相对路径优先相对 base_dir，否则相对当前工作目录。
    """

    path = Path(uri)
    if path.is_absolute():
        return path
    if base_dir is not None:
        return (Path(base_dir) / path).resolve()
    return path.resolve()

# 这里是流式hash
def _sha256_and_size_of_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as fp:
        for chunk in iter(lambda: fp.read(_HASH_CHUNK_SIZE), b""):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _sha256_and_size_of_stream(stream) -> tuple[str, int]:  # noqa: ANN001 - tarfile stream 无稳定类型
    digest = hashlib.sha256()
    size = 0
    for chunk in iter(lambda: stream.read(_HASH_CHUNK_SIZE), b""):
        digest.update(chunk)
        size += len(chunk)
    return digest.hexdigest(), size


def _reject(record: SourceRecord, reasons: list[str], error: str) -> RejectedRecord:
    return RejectedRecord(
        rejected_id=record.sample_id,
        parent_sample_id=record.sample_id,
        media_type=record.media_type,
        uri=record.uri,
        reject_reasons=reasons,
        stage_status={"media_resolver": "rejected"},
        error=error,
        manifest_path=record.manifest_path,
        manifest_line=record.manifest_line,
        raw=record.raw,
    )
