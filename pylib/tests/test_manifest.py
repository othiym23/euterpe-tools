"""Tests for KDL manifest writing, parsing, and execution."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from etp_lib import manifest as _manifest_mod
from etp_lib.manifest import (
    build_manifest_entries,
    execute_manifest,
    open_editor,
    parse_manifest,
    write_manifest,
)
from etp_lib.manifest import _match_bonus_to_anidb_special
from etp_lib.types import (
    AnimeInfo,
    Episode,
    EpisodeType,
    ManifestEntry,
    MediaInfo,
    ParsedMetadata,
    SourceFile,
)


def _mock_media():  # type: ignore[no-untyped-def]
    return MediaInfo(
        video_codec="HEVC",
        resolution="1080p",
        width=1920,
        height=1080,
        bit_depth=8,
        hdr_type="",
    )


def _parse_files(files: list[Path]) -> list[SourceFile]:
    """Parse a list of file paths into SourceFile objects (minimal)."""
    from etp_commands.anime import parse_source_filename

    parsed: list[SourceFile] = []
    for f in files:
        sf = parse_source_filename(f.name)
        sf.path = f
        parsed.append(sf)
    return parsed


class TestBuildManifestEntries:
    """Tests for batch manifest entry building."""

    def test_basic_entries(self, tmp_path, monkeypatch):
        f1 = tmp_path / "[Cyan] Show - 01 [1080p][AAAA1111].mkv"
        f1.write_bytes(b"file1")
        f2 = tmp_path / "[Cyan] Show - 02 [1080p][BBBB2222].mkv"
        f2.write_bytes(b"file2")

        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[
                Episode(1, EpisodeType.REGULAR, "Pilot", "", ""),
                Episode(2, EpisodeType.REGULAR, "Second", "", ""),
            ],
        )
        entries = build_manifest_entries(
            _parse_files([f1, f2]), info, "Show", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 2
        assert not entries[0].is_todo
        assert not entries[1].is_todo
        assert "s1e01" in str(entries[0].dest_path)
        assert "s1e02" in str(entries[1].dest_path)

    def test_unmatched_episode_todo(self, tmp_path, monkeypatch):
        f = tmp_path / "[Group] Movie [1080p].mkv"
        f.write_bytes(b"data")

        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        entries = build_manifest_entries(
            _parse_files([f]), info, "Show", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        assert entries[0].is_todo

    def test_hash_mismatch_strips_hash(self, tmp_path, monkeypatch):
        f = tmp_path / "[Group] Show - 01 [DEADBEEF].mkv"
        f.write_bytes(b"data")

        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: (False, "00000000"))

        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        entries = build_manifest_entries(
            _parse_files([f]), info, "Show", tmp_path / "dest", verbose=False
        )
        assert entries[0].hash_failed
        assert entries[0].source.parsed.hash_code == ""
        assert "DEADBEEF" not in str(entries[0].dest_path)

    def test_default_release_group_applied(self, tmp_path, monkeypatch):
        f1 = tmp_path / "[MTBB] Show - 01.mkv"
        f1.write_bytes(b"a")
        f2 = tmp_path / "Show - 02.mkv"  # no group
        f2.write_bytes(b"b")

        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        entries = build_manifest_entries(
            _parse_files([f1, f2]), info, "Show", tmp_path / "dest", verbose=False
        )
        # No sticky defaults -- f2 has no group, stays empty
        assert entries[1].source.parsed.release_group == ""


class TestWriteManifest:
    """Tests for KDL manifest file writing."""

    def test_basic_format(self, tmp_path):
        sf = SourceFile(
            path=tmp_path / "src.mkv", parsed=ParsedMetadata(episode=1, season=1)
        )
        dest_path = tmp_path / "series" / "Season 01" / "dst.mkv"
        entry = ManifestEntry(source=sf, dest_path=dest_path)
        info = AnimeInfo(
            anidb_id=42,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "etp-anime triage manifest" in content
            assert "AniDB: 42" in content
            assert "season 1 {" in content
            assert "source" in content and "src.mkv" in content
            assert 'dest "dst.mkv"' in content
        finally:
            path.unlink(missing_ok=True)

    def test_todo_tag(self, tmp_path):
        sf = SourceFile(
            path=tmp_path / "src.mkv", parsed=ParsedMetadata(episode=0, season=1)
        )
        dest_path = tmp_path / "series" / "Season 01" / "dst.mkv"
        entry = ManifestEntry(source=sf, dest_path=dest_path, is_todo=True)
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "(todo)episode" in content
        finally:
            path.unlink(missing_ok=True)

    def test_hash_mismatch_comment(self, tmp_path):
        sf = SourceFile(
            path=tmp_path / "src.mkv", parsed=ParsedMetadata(episode=1, season=1)
        )
        dest_path = tmp_path / "series" / "Season 01" / "dst.mkv"
        entry = ManifestEntry(source=sf, dest_path=dest_path, hash_failed=True)
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "CRC32 MISMATCH" in content
        finally:
            path.unlink(missing_ok=True)

    def test_grouped_by_season(self, tmp_path):
        sf1 = SourceFile(
            path=tmp_path / "s1e01.mkv", parsed=ParsedMetadata(episode=1, season=1)
        )
        sf2 = SourceFile(
            path=tmp_path / "s2e01.mkv", parsed=ParsedMetadata(episode=1, season=2)
        )
        entries = [
            ManifestEntry(
                source=sf1, dest_path=tmp_path / "series" / "Season 01" / "ep1.mkv"
            ),
            ManifestEntry(
                source=sf2, dest_path=tmp_path / "series" / "Season 02" / "ep1.mkv"
            ),
        ]
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = write_manifest(entries, info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "season 1 {" in content
            assert "season 2 {" in content
        finally:
            path.unlink(missing_ok=True)

    def test_entries_sorted_by_episode(self, tmp_path):
        """Episodes within a season are sorted by episode number."""
        sf9 = SourceFile(
            path=tmp_path / "ep09.mkv", parsed=ParsedMetadata(episode=9, season=1)
        )
        sf2 = SourceFile(
            path=tmp_path / "ep02.mkv", parsed=ParsedMetadata(episode=2, season=1)
        )
        sf5 = SourceFile(
            path=tmp_path / "ep05.mkv", parsed=ParsedMetadata(episode=5, season=1)
        )
        # Deliberately out of order
        entries = [
            ManifestEntry(
                source=sf9, dest_path=tmp_path / "series" / "Season 01" / "e09.mkv"
            ),
            ManifestEntry(
                source=sf2, dest_path=tmp_path / "series" / "Season 01" / "e02.mkv"
            ),
            ManifestEntry(
                source=sf5, dest_path=tmp_path / "series" / "Season 01" / "e05.mkv"
            ),
        ]
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = write_manifest(entries, info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            # Find all episode lines and check order
            import re

            ep_nums = re.findall(r"episode (\d+) \{", content)
            assert ep_nums == ["2", "5", "9"]
        finally:
            path.unlink(missing_ok=True)

    def test_quotes_in_episode_title(self, tmp_path):
        """Regression: quotes in episode titles must be escaped in KDL output."""
        sf = SourceFile(
            path=tmp_path / "src.mkv", parsed=ParsedMetadata(episode=8, season=2)
        )
        # Dest filename contains double quotes (from TVDB episode title)
        dest_path = (
            tmp_path
            / "series"
            / "Season 02"
            / 'Show - s2e08 - "Okonomiyaki" Means "I Love You" [G Web].mkv'
        )
        entry = ManifestEntry(source=sf, dest_path=dest_path)
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2024,
            episodes=[],
        )
        path = write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            # Must be valid KDL -- parsing should not raise
            import kdl

            kdl.parse(content)
            # Escaped quotes should be present
            assert '\\"Okonomiyaki\\"' in content
        finally:
            path.unlink(missing_ok=True)

    def test_quotes_roundtrip(self, tmp_path):
        """Manifest with quoted episode titles survives write->parse roundtrip."""
        sf = SourceFile(
            path=tmp_path / "src.mkv", parsed=ParsedMetadata(episode=1, season=1)
        )
        dest_name = 'Show - s1e01 - "Hello" World [G Web].mkv'
        dest_path = tmp_path / "series" / "Season 01" / dest_name
        entry = ManifestEntry(source=sf, dest_path=dest_path)
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2024,
            episodes=[],
        )
        manifest_path = write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            series_dir = tmp_path / "series"
            entries, errors, _extras, _renames = parse_manifest(
                manifest_path,
                {str(sf.path): sf},
                series_dir,
            )
            assert len(errors) == 0
            assert len(entries) == 1
            # The parsed dest filename should have the quotes restored
            assert '"Hello"' in entries[0][1].name
        finally:
            manifest_path.unlink(missing_ok=True)


class TestParseManifest:
    """Tests for KDL manifest parsing."""

    def _make_kdl(self, season: int, source: str, dest: str) -> str:
        return (
            f"season {season} {{\n"
            f'  episode 1 {{\n    source "{source}"\n    dest "{dest}"\n  }}\n'
            f"}}\n"
        )

    def test_valid_entry(self, tmp_path):
        sf = SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(self._make_kdl(1, "a.mkv", "dst.mkv"), encoding="utf-8")
        series_dir = tmp_path / "series"
        entries, errors, _extras, _renames = parse_manifest(
            manifest, {"a.mkv": sf}, series_dir
        )
        assert len(entries) == 1
        assert len(errors) == 0
        assert entries[0][0] is sf
        assert entries[0][1] == series_dir / "Season 01" / "dst.mkv"

    def test_todo_rejected(self, tmp_path):
        sf = SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season 1 {\n  (todo)episode 0 {\n    source "a.mkv"\n'
            '    dest "dst.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors, _extras, _renames = parse_manifest(
            manifest, {"a.mkv": sf}, tmp_path
        )
        assert len(entries) == 0
        assert any("todo" in e for e in errors)

    def test_unknown_source(self, tmp_path):
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            self._make_kdl(1, "unknown.mkv", "dst.mkv"), encoding="utf-8"
        )
        entries, errors, _extras, _renames = parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert any("unknown source" in e for e in errors)

    def test_empty_manifest(self, tmp_path):
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text("// all entries deleted\n", encoding="utf-8")
        entries, errors, _extras, _renames = parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert len(errors) == 0

    def test_slashdash_skipped(self, tmp_path):
        """KDL /- commented entries are excluded by the parser."""
        sf = SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season 1 {\n  /- episode 1 {\n    source "a.mkv"\n'
            '    dest "dst.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors, _extras, _renames = parse_manifest(
            manifest, {"a.mkv": sf}, tmp_path
        )
        assert len(entries) == 0
        assert len(errors) == 0

    def test_specials_group(self, tmp_path):
        sf = SourceFile(path=Path("/src/s.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'specials {\n  episode 1 {\n    source "s.mkv"\n'
            '    dest "special.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        series_dir = tmp_path / "series"
        entries, errors, _extras, _renames = parse_manifest(
            manifest, {"s.mkv": sf}, series_dir
        )
        assert len(entries) == 1
        assert entries[0][1] == series_dir / "Specials" / "special.mkv"

    def test_invalid_season_number(self, tmp_path):
        """Non-integer season number should produce an error, not crash."""
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season "abc" {\n  episode 1 {\n    source "a.mkv"\n'
            '    dest "dst.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors, _extras, _renames = parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert any("invalid season" in e for e in errors)

    def test_missing_dest_reports_episode(self, tmp_path):
        """Missing dest field should report which episode has the issue."""
        sf = SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season 1 {\n  episode 5 {\n    source "a.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors, _extras, _renames = parse_manifest(
            manifest, {"a.mkv": sf}, tmp_path
        )
        assert len(entries) == 0
        assert any("episode 5" in e and "missing dest" in e for e in errors)

    def test_missing_source_reports_error(self, tmp_path):
        """Missing source field should report an error."""
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season 1 {\n  episode 1 {\n    dest "dst.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors, _extras, _renames = parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert any("missing source" in e for e in errors)

    def test_unknown_source_shows_available(self, tmp_path):
        """Unknown source error should hint at available sources."""
        sf = SourceFile(path=Path("/src/real.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(self._make_kdl(1, "wrong.mkv", "dst.mkv"), encoding="utf-8")
        entries, errors, _extras, _renames = parse_manifest(
            manifest, {"real.mkv": sf}, tmp_path
        )
        assert len(entries) == 0
        assert any("unknown source" in e and "real.mkv" in e for e in errors)

    def test_extras_missing_dest_reports_error(self, tmp_path):
        """Extras with missing dest should report an error."""
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'extras {\n  file "/src/art.zip" {\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors, extras, _renames = parse_manifest(manifest, {}, tmp_path)
        assert len(extras) == 0
        assert any("extras" in e and "missing dest" in e for e in errors)

    def test_malformed_kdl_reports_parse_error(self, tmp_path):
        """Malformed KDL should return a parse error, not crash."""
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text("season 1 { unclosed", encoding="utf-8")
        entries, errors, _extras, _renames = parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert any("parse error" in e.lower() for e in errors)


class TestRenamesInManifest:
    """Tests for rename entries in manifest KDL."""

    def test_renames_roundtrip(self, tmp_path):
        """Renames written to KDL should be parsed back correctly."""
        old_path = tmp_path / "Season 01" / "Old Name - s1e01 [BD].mkv"
        new_path = tmp_path / "Season 01" / "New Name - s1e01 [BD].mkv"

        renames = [(old_path, new_path)]
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
        )
        manifest_path = write_manifest([], info, "Test", tmp_path, renames=renames)
        try:
            _entries, errors, _extras, parsed_renames = parse_manifest(
                manifest_path, {}, tmp_path
            )
            assert not errors
            assert len(parsed_renames) == 1
            assert parsed_renames[0][0] == old_path
            assert parsed_renames[0][1].name == "New Name - s1e01 [BD].mkv"
        finally:
            manifest_path.unlink()

    def test_renames_not_written_when_empty(self, tmp_path):
        """No renames section when the list is empty."""
        info = AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
        )
        manifest_path = write_manifest([], info, "Test", tmp_path)
        try:
            text = manifest_path.read_text(encoding="utf-8")
            assert "renames {" not in text
        finally:
            manifest_path.unlink()


class TestOpenEditor:
    """Tests for editor invocation."""

    @pytest.mark.parametrize(
        "env_visual,env_editor,expected_cmd",
        [
            ("code", "nano", ["code"]),  # VISUAL preferred
            (None, "nano", ["nano"]),  # EDITOR fallback
            (None, None, ["vi"]),  # vi default
        ],
    )
    def test_editor_selection(self, monkeypatch, env_visual, env_editor, expected_cmd):
        called_with: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
            called_with.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(subprocess, "run", fake_run)
        if env_visual is not None:
            monkeypatch.setenv("VISUAL", env_visual)
        else:
            monkeypatch.delenv("VISUAL", raising=False)
        if env_editor is not None:
            monkeypatch.setenv("EDITOR", env_editor)
        else:
            monkeypatch.delenv("EDITOR", raising=False)

        open_editor(Path("/tmp/test.tsv"))
        assert called_with[0][0] == expected_cmd[0]

    def test_editor_with_arguments(self, monkeypatch):
        called_with: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
            called_with.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setenv("VISUAL", "code --wait")
        monkeypatch.delenv("EDITOR", raising=False)

        open_editor(Path("/tmp/test.kdl"))
        assert called_with[0] == ["code", "--wait", "/tmp/test.kdl"]


class TestExecuteManifest:
    """Tests for manifest execution."""

    def test_success_counting(self, monkeypatch):
        monkeypatch.setattr(_manifest_mod, "copy_reflink", lambda *a, **kw: True)

        sf1 = SourceFile(path=Path("/src/a.mkv"))
        sf2 = SourceFile(path=Path("/src/b.mkv"))
        entries = [
            (sf1, Path("/dst/a.mkv")),
            (sf2, Path("/dst/b.mkv")),
        ]

        success, failed, copied = execute_manifest(entries, dry_run=True, verbose=False)
        assert success == 2
        assert failed == 0
        assert len(copied) == 2

    def test_failure_counting(self, monkeypatch):
        call_count = 0

        def mock_copy(*_a: object, **_kw: object) -> bool:
            nonlocal call_count
            call_count += 1
            return call_count != 2  # second call fails

        monkeypatch.setattr(_manifest_mod, "copy_reflink", mock_copy)

        entries = [
            (SourceFile(path=Path("/src/a.mkv")), Path("/dst/a.mkv")),
            (SourceFile(path=Path("/src/b.mkv")), Path("/dst/b.mkv")),
            (SourceFile(path=Path("/src/c.mkv")), Path("/dst/c.mkv")),
        ]
        success, failed, copied = execute_manifest(
            entries, dry_run=False, verbose=False
        )
        assert success == 2
        assert failed == 1

    def test_both_action_crc_disambiguation(self, tmp_path, monkeypatch):
        """'both' action with existing dest appends CRC32 to filename."""
        src = tmp_path / "src" / "ep01.mkv"
        src.parent.mkdir()
        src.write_bytes(b"source content here")

        dest_dir = tmp_path / "dst"
        dest_dir.mkdir()
        existing = dest_dir / "Show - s1e01 [Group BD,1080p,HEVC].mkv"
        existing.write_bytes(b"existing content")

        dest_path = dest_dir / "Show - s1e01 [Group BD,1080p,HEVC].mkv"

        sf = SourceFile(path=src, parsed=ParsedMetadata(episode=1, season=1))

        # Mock handle_conflict to return "both"
        monkeypatch.setattr(_manifest_mod, "handle_conflict", lambda *a, **kw: "both")
        monkeypatch.setattr(_manifest_mod, "copy_reflink", lambda *a, **kw: True)

        entries = [(sf, dest_path)]
        success, failed, copied = execute_manifest(
            entries, dry_run=False, verbose=False
        )
        assert success == 1
        # CRC32 should have been computed and stashed
        assert sf.parsed.hash_code != ""
        assert len(sf.parsed.hash_code) == 8

    def test_both_action_no_conflict_when_dest_missing(self, tmp_path, monkeypatch):
        """'both' with non-existent dest copies without disambiguation."""
        src = tmp_path / "src" / "ep01.mkv"
        src.parent.mkdir()
        src.write_bytes(b"source content")

        dest_path = tmp_path / "dst" / "Show - s1e01 [Group].mkv"
        dest_path.parent.mkdir()

        sf = SourceFile(path=src, parsed=ParsedMetadata(episode=1, season=1))

        monkeypatch.setattr(_manifest_mod, "handle_conflict", lambda *a, **kw: "both")

        copied_to: list[Path] = []

        def track_copy(src_p, dst_p, **kw):
            copied_to.append(dst_p)
            return True

        monkeypatch.setattr(_manifest_mod, "copy_reflink", track_copy)

        execute_manifest([(sf, dest_path)], dry_run=False, verbose=False)
        # Should copy to original path (no CRC suffix)
        assert copied_to[0] == dest_path


class TestBonusToAnidbMatching:
    """Tests for matching bonus files against AniDB specials."""

    @pytest.mark.parametrize(
        "bonus_type,expected_tag",
        [
            ("NCOP", "C1"),
            ("NCED", "C2"),
        ],
    )
    def test_credit_matching(self, bonus_type, expected_tag):
        specials = [
            Episode(1, EpisodeType.CREDIT, "Opening 1", "", "C1"),
            Episode(1, EpisodeType.CREDIT, "Ending 1", "", "C2"),
            Episode(1, EpisodeType.SPECIAL, "Special 1", "", "S1"),
        ]
        ep = _match_bonus_to_anidb_special(bonus_type, "", specials)
        assert ep is not None
        assert ep.special_tag == expected_tag

    def test_no_match_returns_none(self):
        specials = [Episode(1, EpisodeType.SPECIAL, "Special 1", "", "S1")]
        assert _match_bonus_to_anidb_special("PV", "", specials) is None

    def test_empty_specials(self):
        assert _match_bonus_to_anidb_special("NCOP", "", []) is None

    def test_romaji_title_match(self):
        """Bonus matches against romaji episode title when en/ja don't match."""
        specials = [
            Episode(
                1, EpisodeType.SPECIAL, "", "", "S1", title_romaji="Youjo Senki Special"
            ),
        ]
        ep = _match_bonus_to_anidb_special("Bonus", "Youjo Senki Special", specials)
        assert ep is not None
        assert ep.special_tag == "S1"

    def test_title_match(self):
        specials = [Episode(1, EpisodeType.SPECIAL, "Preview", "", "S1")]
        ep = _match_bonus_to_anidb_special("Preview", "Preview", specials)
        assert ep is not None


class TestNcopNcedManifestOutput:
    """Regression: NCOP/NCED should use bonus type as tag and file's song title."""

    # Other BD rip creators may use different naming conventions for
    # creditless OP/ED. These tests cover the [アニメ BD] pattern; add
    # cases here as new conventions are encountered.

    @pytest.mark.parametrize(
        "bonus_type,song_title,expected_tag,anidb_tag",
        [
            ("NCOP", "Song Title", "NCOP1", "C1"),
            ("NCED", "Song", "NCED1", "C2"),
        ],
    )
    def test_uses_bonus_tag_and_song_title(
        self, tmp_path, monkeypatch, bonus_type, song_title, expected_tag, anidb_tag
    ):
        sf = SourceFile(
            path=tmp_path
            / f"[アニメ BD] Show(第1期) 映像特典「{bonus_type}「{song_title}」(specs).mkv",
            parsed=ParsedMetadata(
                season=1, episode=None, bonus_type=bonus_type, episode_title=song_title
            ),
        )
        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="Show",
            title_en="Show",
            year=2020,
            episodes=[
                Episode(1, EpisodeType.REGULAR, "Ep 1", "", ""),
                Episode(1, EpisodeType.CREDIT, "Opening 1", "", "C1"),
                Episode(1, EpisodeType.CREDIT, "Ending 1", "", "C2"),
            ],
        )
        entries = build_manifest_entries(
            [sf], info, "Show", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        dest = str(entries[0].dest_path)
        assert expected_tag in dest
        assert anidb_tag not in dest
        assert song_title in dest


class TestNcopNcedWithEpisodeNumber:
    """Regression: NCOP/NCED with episode numbers must not be filed as regular episodes."""

    def test_ncop_with_episode_number_is_special(self, tmp_path, monkeypatch):
        """NCOP 01v2 should be a special, not regular s1e01."""
        sf = SourceFile(
            path=tmp_path
            / "[sam] Kusuriya no Hitorigoto - NCOP 01v2 [BD 1080p FLAC] [93B031B8].mkv",
            parsed=ParsedMetadata(
                episode=1,
                bonus_type="NCOP",
                is_special=True,
                version=2,
                hash_code="93B031B8",
            ),
        )
        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=100,
            tvdb_id=431162,
            title_ja="薬屋のひとりごと",
            title_en="The Apothecary Diaries",
            year=2023,
            episodes=[
                Episode(1, EpisodeType.REGULAR, "Maomao", "", ""),
                Episode(1, EpisodeType.CREDIT, "Opening 1", "", "C1"),
                Episode(1, EpisodeType.CREDIT, "Ending 1", "", "C2"),
            ],
        )
        entries = build_manifest_entries(
            [sf], info, "The Apothecary Diaries", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        dest = str(entries[0].dest_path)
        # Must be a special, not a regular episode (s1e01)
        assert "Specials" in dest
        assert "Season 01" not in dest

    def test_ncop_gets_tag_when_no_credits_available(self, tmp_path, monkeypatch):
        """NCOP 01v2 with TVDB-only data (no credits) should get NCOP1 tag."""
        sf = SourceFile(
            path=tmp_path
            / "[sam] Kusuriya no Hitorigoto - NCOP 01v2 [BD 1080p FLAC] [93B031B8].mkv",
            parsed=ParsedMetadata(
                episode=1,
                bonus_type="NCOP",
                is_special=True,
                version=2,
                hash_code="93B031B8",
            ),
        )
        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        # TVDB-only: no credit episodes, only regulars and specials
        info = AnimeInfo(
            anidb_id=None,
            tvdb_id=431162,
            title_ja="薬屋のひとりごと",
            title_en="The Apothecary Diaries",
            year=2023,
            episodes=[
                Episode(1, EpisodeType.REGULAR, "Maomao", "", ""),
            ],
        )
        entries = build_manifest_entries(
            [sf], info, "The Apothecary Diaries", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        dest = str(entries[0].dest_path)
        assert "Specials" in dest
        assert "NCOP1" in dest

    def test_nced_with_episode_number_is_special(self, tmp_path, monkeypatch):
        """NCED 02 should be a special, not regular s1e02."""
        sf = SourceFile(
            path=tmp_path
            / "[sam] Kusuriya no Hitorigoto - NCED 02 [BD 1080p FLAC] [BE75EF1C].mkv",
            parsed=ParsedMetadata(
                episode=2, bonus_type="NCED", is_special=True, hash_code="BE75EF1C"
            ),
        )
        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=100,
            tvdb_id=431162,
            title_ja="薬屋のひとりごと",
            title_en="The Apothecary Diaries",
            year=2023,
            episodes=[
                Episode(1, EpisodeType.REGULAR, "Maomao", "", ""),
                Episode(2, EpisodeType.REGULAR, "Chilly Apothecary", "", ""),
                Episode(1, EpisodeType.CREDIT, "Opening 1", "", "C1"),
                Episode(2, EpisodeType.CREDIT, "Ending 1", "", "C2"),
                Episode(3, EpisodeType.CREDIT, "Opening 2", "", "C3"),
                Episode(4, EpisodeType.CREDIT, "Ending 2", "", "C4"),
            ],
        )
        entries = build_manifest_entries(
            [sf], info, "The Apothecary Diaries", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        dest = str(entries[0].dest_path)
        # Must be a special, not regular s1e02
        assert "Specials" in dest
        assert "Season 01" not in dest

    def test_multiple_ncop_nced_unique_episode_numbers_and_sorted(
        self, tmp_path, monkeypatch
    ):
        """Multiple NCOP+NCED must get unique s0e numbers, sorted OP-ED pairs."""
        files = [
            SourceFile(
                path=tmp_path / "[sam] Show - NCOP 01v2 [BD 1080p FLAC] [AAAA1111].mkv",
                parsed=ParsedMetadata(
                    episode=1,
                    bonus_type="NCOP",
                    is_special=True,
                    version=2,
                    hash_code="AAAA1111",
                ),
            ),
            SourceFile(
                path=tmp_path / "[sam] Show - NCED 01v2 [BD 1080p FLAC] [BBBB2222].mkv",
                parsed=ParsedMetadata(
                    episode=1,
                    bonus_type="NCED",
                    is_special=True,
                    version=2,
                    hash_code="BBBB2222",
                ),
            ),
            SourceFile(
                path=tmp_path / "[sam] Show - NCOP 02 [BD 1080p FLAC] [CCCC3333].mkv",
                parsed=ParsedMetadata(
                    episode=2, bonus_type="NCOP", is_special=True, hash_code="CCCC3333"
                ),
            ),
            SourceFile(
                path=tmp_path / "[sam] Show - NCED 02 [BD 1080p FLAC] [DDDD4444].mkv",
                parsed=ParsedMetadata(
                    episode=2, bonus_type="NCED", is_special=True, hash_code="DDDD4444"
                ),
            ),
        ]
        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        # TVDB-only: no credit episodes
        info = AnimeInfo(
            anidb_id=None,
            tvdb_id=431162,
            title_ja="薬屋のひとりごと",
            title_en="The Apothecary Diaries",
            year=2023,
            episodes=[
                Episode(1, EpisodeType.REGULAR, "Maomao", "", ""),
            ],
        )
        entries = build_manifest_entries(
            files, info, "The Apothecary Diaries", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 4

        # All must be in Specials
        for e in entries:
            assert "Specials" in str(e.dest_path)

        # All episode numbers must be unique
        ep_nums = [e.source.parsed.episode for e in entries]
        assert len(set(ep_nums)) == 4, f"Duplicate episode numbers: {ep_nums}"

        # Extract tags from dest filenames for sort order check
        tags = []
        for e in entries:
            name = e.dest_path.name
            for tag in ("NCOP1", "NCED1", "NCOP2", "NCED2"):
                if tag in name:
                    tags.append(tag)
                    break

        # Must be sorted: NCOP1, NCED1, NCOP2, NCED2
        assert tags == ["NCOP1", "NCED1", "NCOP2", "NCED2"], f"Wrong sort order: {tags}"


class TestHamatvNumbering:
    """Tests for HamaTV-compatible special episode numbering."""

    def test_bonus_gets_hamatv_number(self, tmp_path):
        """Bonus files without AniDB match get s0e numbering."""
        sf = SourceFile(
            path=tmp_path
            / "[アニメ BD] Show(第1期) 映像特典「PV1」(1920x1080 HEVC 10bit FLAC).mkv",
            parsed=ParsedMetadata(season=1, bonus_type="PV", episode_title="PV1"),
        )

        info = AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="Show",
            title_en="Show",
            year=2020,
            episodes=[Episode(1, EpisodeType.REGULAR, "Ep 1", "", "")],
        )
        entries = build_manifest_entries(
            [sf], info, "Show", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        # Should be in Specials dir with s0e numbering
        assert "Specials" in str(entries[0].dest_path)
        assert entries[0].is_todo  # tagged as todo
        # Episode number should be written back to SourceFile
        assert entries[0].source.parsed.episode == 171  # PV/Trailers range start
        assert entries[0].source.parsed.season == 0

    def test_hamatv_numbers_written_to_sourcefile(self, tmp_path):
        """Regression: HamaTV numbers must be written back to sf.parsed.episode
        so write_manifest produces 'episode 321' instead of 'episode 0'."""
        files = []
        for label in ["PV1", "PV2"]:
            sf = SourceFile(
                path=tmp_path / f"[G] Show(第1期) 映像特典「{label}」(specs).mkv",
                parsed=ParsedMetadata(season=1, bonus_type="PV", episode_title=label),
            )
            files.append(sf)

        info = AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="Show",
            title_en="Show",
            year=2020,
            episodes=[Episode(1, EpisodeType.REGULAR, "Ep 1", "", "")],
        )
        entries = build_manifest_entries(
            files, info, "Show", tmp_path / "dest", verbose=False
        )
        ep_nums = sorted(e.source.parsed.episode or 0 for e in entries)
        assert ep_nums == [171, 172]  # sequential HamaTV PV/Trailers numbers
        assert all(e.source.parsed.season == 0 for e in entries)

    def test_anidb_matched_special_writes_episode_number(self, tmp_path, monkeypatch):
        """AniDB-matched specials should also write ep number back."""
        sf = SourceFile(
            path=tmp_path
            / "[G] Show(第1期) 映像特典「ノンテロップOP「Song」(specs).mkv",
            parsed=ParsedMetadata(
                season=1, episode=None, bonus_type="NCOP", episode_title="Song"
            ),
        )
        monkeypatch.setattr(_manifest_mod, "verify_hash", lambda _: None)

        info = AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="Show",
            title_en="Show",
            year=2020,
            episodes=[Episode(1, EpisodeType.CREDIT, "Opening 1", "", "C1")],
        )
        entries = build_manifest_entries(
            [sf], info, "Show", tmp_path / "dest", verbose=False
        )
        assert entries[0].source.parsed.episode == 1
        assert entries[0].source.parsed.season == 0
