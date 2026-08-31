"""Dataset manifest reader and validator."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from curator_flow.contracts import RejectedRecord, SourceRecord, SUPPORTED_MEDIA_TYPES, SUPPORTED_URI_TYPES


REQUIRED_FIELDS: tuple[str, ...] = (
    "sample_id",
    "dataset_name",
    "dataset_version",
    "media_type",
    "uri",
    "uri_type",
    "source",
)


@dataclass(frozen=True)
class ManifestLoadResult:
    """Result of loading and validating a dataset manifest."""

    samples: list[SourceRecord] = field(default_factory=list)
    rejected: list[RejectedRecord] = field(default_factory=list)


def load_dataset_manifest(manifest_path: str | Path) -> ManifestLoadResult:
    """Load a dataset manifest JSONL file and split valid rows from rejected rows."""

    path = Path(manifest_path)
    samples: list[SourceRecord] = []
    rejected: list[RejectedRecord] = []
    seen_sample_ids: dict[str, int] = {}
    # 打开manifest.jsonl
    with path.open("r", encoding="utf-8") as fp:
        for line_number, line in enumerate(fp, start=1):
            raw_line = line.strip()
            if not raw_line:
                continue

            try:
                # load一行 json
                row = json.loads(raw_line)
            except json.JSONDecodeError as exc:
                rejected.append(
                    _reject(
                        manifest_path=path,
                        manifest_line=line_number,
                        raw=None,
                        reasons=["invalid_json"],
                        error=str(exc),
                    )
                )
                continue
            
            if not isinstance(row, dict):
                rejected.append(
                    _reject(
                        manifest_path=path,
                        manifest_line=line_number,
                        raw={"value": row},
                        reasons=["invalid_manifest"],
                        error="Manifest row must be a JSON object.",
                    )
                )
                continue
            # 验证json manifest
            row_errors = validate_dataset_manifest_row(row, seen_sample_ids)
            sample_id = _string_or_none(row.get("sample_id"))
            if row_errors:
                rejected.append(
                    _reject(
                        manifest_path=path,
                        manifest_line=line_number,
                        raw=row,
                        reasons=row_errors,
                        error="; ".join(row_errors),
                    )
                )
                if sample_id and "duplicate_sample_id" not in row_errors:
                    seen_sample_ids[sample_id] = line_number
                continue

            seen_sample_ids[sample_id or ""] = line_number
            samples.append(_to_source_sample(row, path, line_number))

    return ManifestLoadResult(samples=samples, rejected=rejected)

# 一些必须有的字段
def validate_dataset_manifest_row(row: dict[str, Any], seen_sample_ids: dict[str, int]) -> list[str]:
    """Return stable rejection reasons for one manifest row."""

    reasons: list[str] = []

    for field_name in REQUIRED_FIELDS:
        value = row.get(field_name)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            reasons.append(f"missing_{field_name}")

    sample_id = _string_or_none(row.get("sample_id"))
    if sample_id and sample_id in seen_sample_ids:
        reasons.append("duplicate_sample_id")

    media_type = _string_or_none(row.get("media_type"))
    if media_type and media_type not in SUPPORTED_MEDIA_TYPES:
        reasons.append("unsupported_media_type")

    uri_type = _string_or_none(row.get("uri_type"))
    if uri_type and uri_type not in SUPPORTED_URI_TYPES:
        reasons.append("unsupported_uri_type")

    if uri_type == "tar_member":
        member_name = _string_or_none(row.get("member_name"))
        if not member_name:
            reasons.append("missing_member_name")

    metadata = row.get("metadata", {})
    if metadata is not None and not isinstance(metadata, dict):
        reasons.append("invalid_metadata")

    bytes_value = row.get("bytes")
    if bytes_value is not None and (not isinstance(bytes_value, int) or bytes_value < 0):
        reasons.append("invalid_bytes")

    return reasons


def _to_source_sample(row: dict[str, Any], manifest_path: Path, manifest_line: int) -> SourceRecord:
    return SourceRecord(
        sample_id=str(row["sample_id"]),
        dataset_name=str(row["dataset_name"]),
        dataset_version=str(row["dataset_version"]),
        media_type=row["media_type"],
        uri=str(row["uri"]),
        uri_type=row["uri_type"],
        text=_optional_string(row.get("text")),
        source=str(row["source"]),
        license=_optional_string(row.get("license")),
        manifest_path=manifest_path,
        manifest_line=manifest_line,
        shard_id=_optional_string(row.get("shard_id")),
        member_name=_optional_string(row.get("member_name")),
        checksum_sha256=_optional_string(row.get("checksum_sha256")),
        bytes=row.get("bytes"),
        mime_type=_optional_string(row.get("mime_type")),
        language=_optional_string(row.get("language")),
        created_at=_optional_string(row.get("created_at")),
        metadata=row.get("metadata") or {},
        raw=dict(row),
    )


def _reject(
    manifest_path: Path,
    manifest_line: int,
    raw: dict[str, Any] | None,
    reasons: list[str],
    error: str | None,
) -> RejectedRecord:
    sample_id = _string_or_none(raw.get("sample_id")) if raw else None
    media_type = _string_or_none(raw.get("media_type")) if raw else None
    uri = _string_or_none(raw.get("uri")) if raw else None

    return RejectedRecord(
        rejected_id=sample_id or f"{manifest_path.name}:{manifest_line}",
        parent_sample_id=sample_id,
        media_type=media_type,
        uri=uri,
        reject_reasons=reasons,
        stage_status={"manifest_validator": "rejected"},
        error=error,
        manifest_path=manifest_path,
        manifest_line=manifest_line,
        raw=raw,
    )


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None
