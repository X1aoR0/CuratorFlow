from pathlib import Path

from curator_flow.io.manifest_reader import load_dataset_manifest, validate_dataset_manifest_row


def test_load_dataset_manifest_accepts_image_and_video_rows(tmp_path: Path) -> None:
    manifest = tmp_path / "dataset_manifest.jsonl"
    manifest.write_text(
        "\n".join(
            [
                '{"sample_id":"img_1","dataset_name":"smoke","dataset_version":"v1","media_type":"image",'
                '"uri":"data/images/00000.tar","uri_type":"tar_member","member_name":"000000.jpg",'
                '"text":"A caption.","source":"test","license":"unknown","metadata":{"k":"v"}}',
                '{"sample_id":"vid_1","dataset_name":"smoke","dataset_version":"v1","media_type":"video",'
                '"uri":"data/videos/test.mp4","uri_type":"local_file","text":"A video caption.",'
                '"source":"test","license":"unknown"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = load_dataset_manifest(manifest)

    assert result.rejected == []
    assert [sample.sample_id for sample in result.samples] == ["img_1", "vid_1"]
    assert result.samples[0].media_type == "image"
    assert result.samples[0].uri_type == "tar_member"
    assert result.samples[0].member_name == "000000.jpg"
    assert result.samples[0].metadata == {"k": "v"}
    assert result.samples[1].media_type == "video"
    assert result.samples[1].manifest_line == 2


def test_load_dataset_manifest_rejects_invalid_rows_without_stopping(tmp_path: Path) -> None:
    manifest = tmp_path / "dataset_manifest.jsonl"
    manifest.write_text(
        "\n".join(
            [
                '{"sample_id":"ok","dataset_name":"smoke","dataset_version":"v1","media_type":"image",'
                '"uri":"data/images/00000.tar","uri_type":"tar_member","member_name":"000000.jpg",'
                '"source":"test"}',
                '{"sample_id":"bad_json",',
                '{"sample_id":"ok","dataset_name":"smoke","dataset_version":"v1","media_type":"video",'
                '"uri":"data/videos/test.mp4","uri_type":"local_file","source":"test"}',
                '{"sample_id":"no_member","dataset_name":"smoke","dataset_version":"v1","media_type":"image",'
                '"uri":"data/images/00000.tar","uri_type":"tar_member","source":"test"}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    result = load_dataset_manifest(manifest)

    assert [sample.sample_id for sample in result.samples] == ["ok"]
    assert len(result.rejected) == 3
    assert result.rejected[0].reject_reasons == ["invalid_json"]
    assert result.rejected[1].reject_reasons == ["duplicate_sample_id"]
    assert result.rejected[2].reject_reasons == ["missing_member_name"]


def test_validate_dataset_manifest_row_reports_stable_reasons() -> None:
    reasons = validate_dataset_manifest_row(
        {
            "sample_id": "",
            "dataset_name": "smoke",
            "dataset_version": "v1",
            "media_type": "audio",
            "uri": "data/audio.wav",
            "uri_type": "local_file",
            "source": "test",
            "metadata": "not-an-object",
            "bytes": -1,
        },
        seen_sample_ids={},
    )

    assert reasons == [
        "missing_sample_id",
        "unsupported_media_type",
        "invalid_metadata",
        "invalid_bytes",
    ]
