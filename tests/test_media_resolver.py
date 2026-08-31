import hashlib
import io
import tarfile
from pathlib import Path

from curator_flow.contracts import RejectedRecord, ResolvedRecord, SourceRecord
from curator_flow.io.media_resolver import resolve_media, resolve_one


def _make_record(
    tmp_path: Path,
    *,
    sample_id: str,
    media_type: str,
    uri: str,
    uri_type: str,
    member_name: str | None = None,
) -> SourceRecord:
    return SourceRecord(
        sample_id=sample_id,
        dataset_name="smoke",
        dataset_version="v1",
        media_type=media_type,
        uri=uri,
        uri_type=uri_type,
        text="caption",
        source="test",
        license="unknown",
        manifest_path=tmp_path / "dataset_manifest.jsonl",
        manifest_line=1,
        member_name=member_name,
    )


def test_resolve_local_file_computes_content_hash(tmp_path: Path) -> None:
    video = tmp_path / "clip.mp4"
    payload = b"fake video bytes"
    video.write_bytes(payload)

    record = _make_record(
        tmp_path,
        sample_id="vid_1",
        media_type="video",
        uri="clip.mp4",
        uri_type="local_file",
    )

    outcome = resolve_one(record, base_dir=tmp_path)

    assert isinstance(outcome, ResolvedRecord)
    assert outcome.exists is True
    assert outcome.bytes_size == len(payload)
    assert outcome.content_sha256 == hashlib.sha256(payload).hexdigest()
    assert outcome.container_sha256 is None
    assert outcome.member_name is None


def test_resolve_tar_member_hashes_member_and_container(tmp_path: Path) -> None:
    member_bytes = b"jpeg-bytes-000000"
    tar_path = tmp_path / "00000.tar"
    with tarfile.open(tar_path, "w") as tar:
        info = tarfile.TarInfo(name="000000.jpg")
        info.size = len(member_bytes)
        tar.addfile(info, io.BytesIO(member_bytes))

    record = _make_record(
        tmp_path,
        sample_id="img_1",
        media_type="image",
        uri="00000.tar",
        uri_type="tar_member",
        member_name="000000.jpg",
    )

    outcome = resolve_one(record, base_dir=tmp_path)

    assert isinstance(outcome, ResolvedRecord)
    assert outcome.bytes_size == len(member_bytes)
    assert outcome.content_sha256 == hashlib.sha256(member_bytes).hexdigest()
    # 容器 hash 是整个 tar 文件的 hash，和成员内容 hash 不同
    assert outcome.container_sha256 == hashlib.sha256(tar_path.read_bytes()).hexdigest()
    assert outcome.content_sha256 != outcome.container_sha256
    assert outcome.member_name == "000000.jpg"


def test_resolve_missing_local_file_is_rejected(tmp_path: Path) -> None:
    record = _make_record(
        tmp_path,
        sample_id="vid_missing",
        media_type="video",
        uri="nope.mp4",
        uri_type="local_file",
    )

    outcome = resolve_one(record, base_dir=tmp_path)

    assert isinstance(outcome, RejectedRecord)
    assert outcome.reject_reasons == ["missing_file"]
    assert outcome.stage_status == {"media_resolver": "rejected"}


def test_resolve_missing_tar_member_is_rejected(tmp_path: Path) -> None:
    tar_path = tmp_path / "00000.tar"
    with tarfile.open(tar_path, "w") as tar:
        info = tarfile.TarInfo(name="000000.jpg")
        info.size = 3
        tar.addfile(info, io.BytesIO(b"abc"))

    record = _make_record(
        tmp_path,
        sample_id="img_wrong_member",
        media_type="image",
        uri="00000.tar",
        uri_type="tar_member",
        member_name="999999.jpg",
    )

    outcome = resolve_one(record, base_dir=tmp_path)

    assert isinstance(outcome, RejectedRecord)
    assert outcome.reject_reasons == ["missing_tar_member"]


def test_resolve_unsupported_uri_type_is_rejected(tmp_path: Path) -> None:
    record = _make_record(
        tmp_path,
        sample_id="remote_1",
        media_type="image",
        uri="s3://bucket/img.jpg",
        uri_type="s3",
    )

    outcome = resolve_one(record, base_dir=tmp_path)

    assert isinstance(outcome, RejectedRecord)
    assert outcome.reject_reasons == ["unsupported_uri_type"]


def test_resolve_media_splits_resolved_and_rejected(tmp_path: Path) -> None:
    good = tmp_path / "a.mp4"
    good.write_bytes(b"data")
    records = [
        _make_record(tmp_path, sample_id="ok", media_type="video", uri="a.mp4", uri_type="local_file"),
        _make_record(tmp_path, sample_id="bad", media_type="video", uri="missing.mp4", uri_type="local_file"),
    ]

    result = resolve_media(records, base_dir=tmp_path)

    assert [r.sample_id for r in result.resolved] == ["ok"]
    assert [r.rejected_id for r in result.rejected] == ["bad"]
