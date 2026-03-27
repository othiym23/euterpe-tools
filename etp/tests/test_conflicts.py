"""Tests for CRC32 verification and conflict resolution."""

from __future__ import annotations

from pathlib import Path

from etp_lib.conflicts import (
    _extract_key_metadata,
    _format_media_summary,
    _format_size,
    check_destination_conflict,
    compute_crc32,
    verify_hash,
)
from etp_lib.types import AudioTrack, MediaInfo, SourceFile


def _mock_media():  # type: ignore[no-untyped-def]
    return MediaInfo(
        video_codec="HEVC",
        resolution="1080p",
        width=1920,
        height=1080,
        bit_depth=8,
        hdr_type="",
    )


class TestCrc32Verification:
    """Tests for CRC32 hash computation and verification."""

    def test_compute_crc32(self, tmp_path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"hello world")
        import zlib

        expected = f"{zlib.crc32(b'hello world') & 0xFFFFFFFF:08X}"
        assert compute_crc32(f) == expected

    def test_verify_hash_match(self, tmp_path):
        f = tmp_path / "test.mkv"
        f.write_bytes(b"test data")
        import zlib

        crc = f"{zlib.crc32(b'test data') & 0xFFFFFFFF:08X}"
        sf = SourceFile(path=f, hash_code=crc)
        result = verify_hash(sf)
        assert result is not None
        ok, actual = result
        assert ok is True
        assert actual == crc

    def test_verify_hash_mismatch(self, tmp_path):
        f = tmp_path / "test.mkv"
        f.write_bytes(b"test data")
        sf = SourceFile(path=f, hash_code="00000000")
        result = verify_hash(sf)
        assert result is not None
        ok, actual = result
        assert ok is False
        assert len(actual) == 8

    def test_verify_hash_no_hash(self):
        sf = SourceFile(path=Path("/tmp/test.mkv"), hash_code="")
        assert verify_hash(sf) is None

    def test_verify_hash_case_insensitive(self, tmp_path):
        f = tmp_path / "test.mkv"
        f.write_bytes(b"test data")
        import zlib

        crc = f"{zlib.crc32(b'test data') & 0xFFFFFFFF:08x}"  # lowercase
        sf = SourceFile(path=f, hash_code=crc)
        result = verify_hash(sf)
        assert result is not None
        assert result[0] is True


class TestConflictResolution:
    """Tests for destination conflict checking."""

    def test_no_conflict_when_dest_missing(self, tmp_path):
        sf = SourceFile(path=tmp_path / "src.mkv")
        dest = tmp_path / "nonexistent.mkv"
        assert check_destination_conflict(sf, dest) is None

    def test_conflict_detected(self, tmp_path):
        src = tmp_path / "src.mkv"
        src.write_bytes(b"source")
        dst = tmp_path / "dst.mkv"
        dst.write_bytes(b"existing")

        sf = SourceFile(path=src, release_group="FLE", source_type="BD")
        sf.media = _mock_media()

        conflict = check_destination_conflict(sf, dst)
        assert conflict is not None
        assert conflict.existing_path == dst

    def test_metadata_matches_same_group_codec(self, tmp_path):
        src = tmp_path / "[FLE] Show - 01.mkv"
        src.write_bytes(b"source")
        dst = tmp_path / "Show - s1e01 [FLE BD,1080p,HEVC].mkv"
        dst.write_bytes(b"existing")

        sf = SourceFile(path=src, release_group="FLE", source_type="BD")
        sf.media = _mock_media()

        conflict = check_destination_conflict(sf, dst)
        assert conflict is not None

    def test_extract_key_metadata(self):
        sf = SourceFile(path=Path("test.mkv"), release_group="FLE", source_type="BD")
        sf.media = MediaInfo(
            video_codec="HEVC",
            resolution="1080p",
            width=1920,
            height=1080,
            bit_depth=10,
            hdr_type="",
            audio_tracks=[
                AudioTrack("flac", "ja", "Japanese", False),
                AudioTrack("aac", "en", "English", False),
            ],
        )
        group, source, codec, audio = _extract_key_metadata(sf)
        assert group == "FLE"
        assert source == "BD"
        assert codec == "HEVC"
        assert audio == "flac+aac"

    def test_format_size(self):
        assert "GB" in _format_size(2_500_000_000)
        assert "MB" in _format_size(50_000_000)
        assert "KB" in _format_size(1_024)

    def test_format_media_summary_none(self):
        assert "unavailable" in _format_media_summary(None)

    def test_format_media_summary(self):
        media = MediaInfo(
            video_codec="HEVC",
            resolution="1080p",
            width=1920,
            height=1080,
            bit_depth=10,
            hdr_type="HDR",
            audio_tracks=[
                AudioTrack("flac", "ja", "Japanese", False),
            ],
        )
        summary = _format_media_summary(media)
        assert "HEVC" in summary
        assert "1080p" in summary
        assert "10bit" in summary
        assert "HDR" in summary
        assert "flac" in summary
