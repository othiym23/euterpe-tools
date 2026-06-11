"""Tests for recursive media-file discovery (etp_lib.media_scanner)."""

from pathlib import Path

from etp_lib.media_scanner import iter_media_files


def _touch(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x")
    return path


class TestIterMediaFiles:
    def test_video_only_by_default(self, tmp_path):
        wanted = {
            _touch(tmp_path / "a.mkv"),
            _touch(tmp_path / "b.mp4"),
            _touch(tmp_path / "c.avi"),
        }
        _touch(tmp_path / "d.flac")
        _touch(tmp_path / "e.srt")
        _touch(tmp_path / "f.txt")
        assert set(iter_media_files([tmp_path])) == wanted

    def test_include_audio(self, tmp_path):
        video = _touch(tmp_path / "a.mkv")
        audio = _touch(tmp_path / "b.flac")
        _touch(tmp_path / "c.srt")
        assert set(iter_media_files([tmp_path], include_audio=True)) == {video, audio}

    def test_extension_case_insensitive(self, tmp_path):
        upper = _touch(tmp_path / "a.MKV")
        assert iter_media_files([tmp_path]) == [upper]

    def test_recurses_fully(self, tmp_path):
        deep = _touch(tmp_path / "a" / "b" / "c" / "d.mkv")
        assert iter_media_files([tmp_path]) == [deep]

    def test_skips_download_working_dirs(self, tmp_path):
        kept = _touch(tmp_path / "done" / "a.mkv")
        _touch(tmp_path / "temp" / "b.mkv")
        _touch(tmp_path / "Incomplete" / "c.mkv")
        _touch(tmp_path / ".tmp" / "d.mkv")
        _touch(tmp_path / ".incomplete" / "e.mkv")
        assert iter_media_files([tmp_path]) == [kept]

    def test_missing_source_dir(self, tmp_path):
        assert iter_media_files([tmp_path / "nope"]) == []

    def test_multiple_source_dirs(self, tmp_path):
        a = _touch(tmp_path / "one" / "a.mkv")
        b = _touch(tmp_path / "two" / "b.mkv")
        assert set(iter_media_files([tmp_path / "one", tmp_path / "two"])) == {a, b}
