"""Tests for episode filename formatting and series directory naming."""

from __future__ import annotations

from pathlib import Path

from etp_lib.naming import (
    build_metadata_block,
    format_episode_filename,
    format_series_dirname,
)
from etp_lib.types import AudioTrack, MediaInfo, ParsedMetadata, SourceFile


class TestMetadataBlock:
    """Tests for metadata block construction."""

    def _make_source(self, **overrides: object) -> SourceFile:
        sf = SourceFile(
            path=Path("test.mkv"),
            parsed=ParsedMetadata(
                release_group="SubGroup",
                source_type="BD",
                episode=1,
            ),
            media=MediaInfo(
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
                encoding_lib="x265",
            ),
        )
        for key, value in overrides.items():
            if hasattr(sf.parsed, key):
                setattr(sf.parsed, key, value)
            else:
                setattr(sf, key, value)
        return sf

    def test_full_metadata_block(self):
        sf = self._make_source()
        block = build_metadata_block(sf)
        assert block == "SubGroup BD,1080p,HEVC,10bit,x265,flac+aac,dual-audio"

    def test_remux_included(self):
        sf = self._make_source(is_remux=True)
        block = build_metadata_block(sf)
        assert "REMUX" in block
        parts = block.split(",")
        assert parts.index("REMUX") < parts.index("1080p")

    def test_hdr_included(self):
        sf = self._make_source()
        assert sf.media is not None
        sf.media.hdr_type = "HDR"
        block = build_metadata_block(sf)
        assert "HDR" in block

    def test_dovi_included(self):
        sf = self._make_source()
        assert sf.media is not None
        sf.media.hdr_type = "DoVi"
        sf.media.resolution = "4K"
        block = build_metadata_block(sf)
        assert "DoVi" in block

    def test_no_encoding_lib(self):
        sf = self._make_source()
        assert sf.media is not None
        sf.media.encoding_lib = ""
        block = build_metadata_block(sf)
        assert "x264" not in block
        assert "x265" not in block

    def test_dual_audio_detection(self):
        sf = self._make_source()
        block = build_metadata_block(sf)
        assert "dual-audio" in block

    def test_multi_audio_detection(self):
        sf = self._make_source()
        assert sf.media is not None
        sf.media.audio_tracks.append(AudioTrack("aac", "de", "German", False))
        block = build_metadata_block(sf)
        assert "multi-audio" in block
        assert "dual-audio" not in block

    def test_commentary_excluded_from_dual_audio(self):
        """Commentary tracks don't count toward dual-audio."""
        sf = self._make_source()
        assert sf.media is not None
        sf.media.audio_tracks = [
            AudioTrack("flac", "ja", "Japanese", False),
            AudioTrack("flac", "ja", "Commentary", True),
        ]
        block = build_metadata_block(sf)
        assert "dual-audio" not in block
        assert "multi-audio" not in block

    def test_audio_codecs_joined_with_plus(self):
        sf = self._make_source()
        assert sf.media is not None
        sf.media.audio_tracks = [
            AudioTrack("DTS", "ja", "Japanese", False),
            AudioTrack("flac", "en", "English", False),
        ]
        block = build_metadata_block(sf)
        assert "DTS+flac" in block

    def test_no_media_returns_empty(self):
        sf = self._make_source(media=None)
        assert build_metadata_block(sf) == ""

    def test_web_source_type(self):
        sf = self._make_source(source_type="Web")
        block = build_metadata_block(sf)
        assert block.startswith("SubGroup Web,")


class TestFormatEpisodeFilename:
    """Tests for episode filename formatting."""

    def _make_source(self):  # type: ignore[no-untyped-def]
        return SourceFile(
            path=Path("test.mkv"),
            parsed=ParsedMetadata(
                release_group="NH",
                source_type="BD",
                episode=1,
            ),
            media=MediaInfo(
                video_codec="AVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=10,
                hdr_type="",
                audio_tracks=[
                    AudioTrack("aac", "ja", "Japanese", False),
                ],
                encoding_lib="x264",
            ),
        )

    def test_regular_episode_with_name(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="BEASTARS",
            season=1,
            episode=1,
            episode_name="The Moon and the Beast",
            source=sf,
        )
        assert result == (
            "BEASTARS - s1e01 - The Moon and the Beast "
            "[NH BD,1080p,AVC,10bit,x264,aac].mkv"
        )

    def test_regular_episode_without_name(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="Eden of the East",
            season=1,
            episode=5,
            episode_name="",
            source=sf,
        )
        assert result == (
            "Eden of the East - s1e05 [NH BD,1080p,AVC,10bit,x264,aac].mkv"
        )

    def test_special_episode(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="City Hunter",
            season=0,
            episode=0,
            episode_name="",
            source=sf,
            is_special=True,
            special_tag="NCOP1",
        )
        assert result == ("City Hunter - NCOP1 [NH BD,1080p,AVC,10bit,x264,aac].mkv")

    def test_special_with_name(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="Akira",
            season=0,
            episode=0,
            episode_name="Making of Akira",
            source=sf,
            is_special=True,
            special_tag="S1",
        )
        assert result == (
            "Akira - S1 - Making of Akira [NH BD,1080p,AVC,10bit,x264,aac].mkv"
        )

    def test_movie_single_file(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="",
            season=1,
            episode=1,
            episode_name="",
            source=sf,
            is_movie=True,
            movie_dir_name="アキラ [Akira] (1988)",
        )
        assert result == (
            "アキラ [Akira] (1988) - complete movie "
            "[NH BD,1080p,AVC,10bit,x264,aac].mkv"
        )

    def test_hash_preserved(self):
        sf = self._make_source()
        sf.parsed.hash_code = "ABCD1234"
        result = format_episode_filename(
            concise_name="Show",
            season=1,
            episode=1,
            episode_name="Ep",
            source=sf,
        )
        assert "[ABCD1234]" in result

    def test_episode_number_zero_padded(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="Show",
            season=1,
            episode=3,
            episode_name="",
            source=sf,
        )
        assert "s1e03" in result

    def test_file_extension_preserved(self):
        sf = self._make_source()
        sf.path = Path("test.mp4")
        result = format_episode_filename(
            concise_name="Show",
            season=1,
            episode=1,
            episode_name="",
            source=sf,
        )
        assert result.endswith(".mp4")

    def test_colon_sanitized_in_episode_name(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="FMA",
            season=1,
            episode=1,
            episode_name="Those Who Challenge the Sun: Part 1",
            source=sf,
        )
        assert "Those Who Challenge the Sun- Part 1" in result
        assert ":" not in result

    def test_slash_sanitized_in_concise_name(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="Fate/Zero",
            season=1,
            episode=1,
            episode_name="",
            source=sf,
        )
        assert "Fate - Zero" in result
        assert "/" not in result


class TestDirectoryNaming:
    """Tests for directory naming."""

    def test_format_series_dirname(self):
        result = format_series_dirname("アキラ", "Akira", 1988)
        assert result == "アキラ [Akira] (1988)"

    def test_format_series_dirname_complex(self):
        result = format_series_dirname("東のエデン", "Eden of the East", 2009)
        assert result == "東のエデン [Eden of the East] (2009)"

    def test_colon_sanitized(self):
        result = format_series_dirname(
            "鋼の錬金術師 (2009)", "Fullmetal Alchemist: Brotherhood", 2009
        )
        assert result == "鋼の錬金術師 [Fullmetal Alchemist- Brotherhood] (2009)"

    def test_slash_sanitized(self):
        result = format_series_dirname("Fate/Zero", "Fate/Zero", 2011)
        assert result == "Fate - Zero (2011)"

    def test_redundant_year_stripped(self):
        result = format_series_dirname(
            "鋼の錬金術師 (2009)", "Fullmetal Alchemist (2009)", 2009
        )
        assert result == "鋼の錬金術師 [Fullmetal Alchemist] (2009)"

    def test_non_matching_year_kept(self):
        result = format_series_dirname(
            "鋼の錬金術師 (2003)", "Fullmetal Alchemist", 2009
        )
        assert result == "鋼の錬金術師 (2003) [Fullmetal Alchemist] (2009)"

    def test_romaji_ja_uses_single_title(self):
        """Romaji-only Japanese title falls back to single-title format."""
        result = format_series_dirname("BEASTARS", "BEASTARS", 2019)
        assert result == "BEASTARS (2019)"

    def test_romaji_ja_prefers_english(self):
        """When ja is romaji and en differs, use en as the single title."""
        result = format_series_dirname(
            "Hagane no Renkinjutsushi", "Fullmetal Alchemist", 2003
        )
        assert result == "Fullmetal Alchemist (2003)"

    def test_empty_en_uses_ja(self):
        result = format_series_dirname("アキラ", "", 1988)
        assert result == "アキラ (1988)"

    def test_empty_ja_uses_en(self):
        result = format_series_dirname("", "Akira", 1988)
        assert result == "Akira (1988)"

    def test_identical_titles_single(self):
        """Identical ja and en after sanitization -> single title."""
        result = format_series_dirname("アキラ", "アキラ", 1988)
        assert result == "アキラ (1988)"

    def test_no_empty_brackets(self):
        """Never produce 'TITLE [] (YYYY)'."""
        result = format_series_dirname("BEASTARS", "", 2019)
        assert "[]" not in result
        assert result == "BEASTARS (2019)"
