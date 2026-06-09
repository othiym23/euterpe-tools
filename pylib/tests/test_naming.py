"""Tests for episode filename formatting and series directory naming."""

from __future__ import annotations

from pathlib import Path

from etp_lib.naming import (
    build_metadata_block,
    extras_relpath,
    format_display_title,
    format_episode_filename,
    format_movie_dirname,
    format_movie_filename,
    format_series_dirname,
    format_tv_episode_filename,
    format_tv_series_dirname,
    subtitle_sidecars,
)
from etp_lib.types import AudioTrack, MediaInfo, ParsedMetadata, SourceFile


def make_western_source(**parsed_overrides: object) -> SourceFile:
    """A typical western-release source file for movie/TV naming tests."""
    parsed = ParsedMetadata(release_group="NTb", source_type="Web")
    for key, value in parsed_overrides.items():
        setattr(parsed, key, value)
    return SourceFile(
        path=Path("source.mkv"),
        parsed=parsed,
        media=MediaInfo(
            video_codec="AVC",
            resolution="1080p",
            width=1920,
            height=1080,
            bit_depth=8,
            hdr_type="",
            audio_tracks=[AudioTrack("EAC3", "en", "English", False)],
            encoding_lib="x264",
        ),
    )


class TestSubtitleSidecars:
    """`subtitle_sidecars` maps co-located subs onto the dest video's name."""

    @staticmethod
    def _touch(path: Path) -> Path:
        path.write_bytes(b"")
        return path

    def test_untagged_defaults_to_en(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        self._touch(tmp_path / "Show - 01.srt")
        dest = tmp_path / "dest" / "Frieren - s1e01 - Title [MTBB].mkv"
        assert subtitle_sidecars(video, dest) == [
            (
                tmp_path / "Show - 01.srt",
                tmp_path / "dest" / "Frieren - s1e01 - Title [MTBB].en.srt",
            )
        ]

    def test_custom_default_lang(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        self._touch(tmp_path / "Show - 01.srt")
        dest = tmp_path / "Frieren - s1e01.mkv"
        pairs = subtitle_sidecars(video, dest, default_lang="ja")
        assert pairs[0][1].name == "Frieren - s1e01.ja.srt"

    def test_tagged_tokens_preserved(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        self._touch(tmp_path / "Show - 01.en.forced.srt")
        dest = tmp_path / "Frieren - s1e01.mkv"
        pairs = subtitle_sidecars(video, dest, default_lang="ja")
        # An explicit token wins over the default language.
        assert pairs[0][1].name == "Frieren - s1e01.en.forced.srt"

    def test_prefix_boundary_not_matched(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        self._touch(tmp_path / "Show - 011.srt")  # episode 11, not 1
        dest = tmp_path / "Frieren - s1e01.mkv"
        assert subtitle_sidecars(video, dest) == []

    def test_non_subtitle_siblings_ignored(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        self._touch(tmp_path / "Show - 01.nfo")
        self._touch(tmp_path / "Show - 01.txt")
        dest = tmp_path / "Frieren - s1e01.mkv"
        assert subtitle_sidecars(video, dest) == []

    def test_multiple_extensions(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        self._touch(tmp_path / "Show - 01.ass")
        self._touch(tmp_path / "Show - 01.sub")
        self._touch(tmp_path / "Show - 01.idx")
        dest = tmp_path / "Frieren - s1e01.mkv"
        names = sorted(d.name for _s, d in subtitle_sidecars(video, dest))
        assert names == [
            "Frieren - s1e01.en.ass",
            "Frieren - s1e01.en.idx",
            "Frieren - s1e01.en.sub",
        ]

    def test_no_sidecars_returns_empty(self, tmp_path):
        video = self._touch(tmp_path / "Show - 01.mkv")
        assert subtitle_sidecars(video, tmp_path / "Frieren - s1e01.mkv") == []


class TestExtrasRelpath:
    """`extras_relpath` identifies Extras subtrees and their relative path."""

    def test_not_under_extras_returns_none(self):
        assert extras_relpath(Path("/dl/Batch/ep01.mkv")) is None

    def test_under_extras_preserves_substructure(self):
        rel = extras_relpath(Path("/dl/Batch/Extras/OST/Disc 1/track01.flac"))
        assert rel == Path("OST/Disc 1/track01.flac")

    def test_directly_in_extras(self):
        assert extras_relpath(Path("/dl/Batch/Extras/NCOP.mkv")) == Path("NCOP.mkv")

    def test_case_insensitive(self):
        assert extras_relpath(Path("/dl/Batch/extras/scan.png")) == Path("scan.png")


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

    def test_mediainfo_interlaced_overrides_filename(self):
        """Mediainfo detecting interlaced should produce 1080i in metadata."""
        sf = self._make_source()
        # Filename says 1080p but mediainfo detects interlaced
        sf.media.resolution = "1080i"  # type: ignore[union-attr]  # ty: ignore[invalid-assignment]
        block = build_metadata_block(sf)
        assert "1080i" in block
        assert "1080p" not in block

    def test_resolution_normalization_in_metadata(self):
        """Resolution from mediainfo should be normalized (e.g., 480p not 720x480)."""
        sf = self._make_source()
        sf.media.resolution = "480p"  # type: ignore[union-attr]  # ty: ignore[invalid-assignment]
        block = build_metadata_block(sf)
        assert "480p" in block

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

    def test_multi_episode_range(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="Panty and Stocking",
            season=1,
            episode=2,
            episode_name="A + B + C",
            source=sf,
            episodes=[2, 3, 4],
        )
        assert result == (
            "Panty and Stocking - s1e02-e04 - A + B + C "
            "[NH BD,1080p,AVC,10bit,x264,aac].mkv"
        )

    def test_multi_episode_single_element_uses_scalar(self):
        sf = self._make_source()
        result = format_episode_filename(
            concise_name="Show",
            season=1,
            episode=5,
            episode_name="Foo",
            source=sf,
            episodes=[5],
        )
        assert " - s1e05 - " in result

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


class TestFormatMovieDirname:
    """Tests for Plex-convention movie directory naming."""

    def test_basic(self):
        assert format_movie_dirname("Heat", 1995, 949) == "Heat (1995) {tmdb-949}"

    def test_no_id(self):
        assert format_movie_dirname("Heat", 1995, None) == "Heat (1995)"

    def test_edition(self):
        result = format_movie_dirname("Blade Runner", 1982, 78, edition="Final Cut")
        assert result == "Blade Runner (1982) {tmdb-78} {edition-Final Cut}"

    def test_edition_without_id(self):
        result = format_movie_dirname("Blade Runner", 1982, None, edition="Final Cut")
        assert result == "Blade Runner (1982) {edition-Final Cut}"

    def test_unknown_year_omitted(self):
        assert format_movie_dirname("Mystery", 0, 123) == "Mystery {tmdb-123}"

    def test_sanitizes_title(self):
        result = format_movie_dirname("Face/Off: Redux", 1997, 754)
        assert result == "Face - Off- Redux (1997) {tmdb-754}"

    def test_redundant_year_stripped(self):
        assert format_movie_dirname("Heat (1995)", 1995, 949) == (
            "Heat (1995) {tmdb-949}"
        )


class TestFormatMovieFilename:
    """Tests for movie filenames (named exactly after the folder)."""

    def test_dirname_plus_block(self):
        source = make_western_source()
        result = format_movie_filename("Heat (1995) {tmdb-949}", source)
        assert result == "Heat (1995) {tmdb-949} [NTb Web,1080p,AVC,x264,EAC3].mkv"

    def test_no_complete_movie_marker(self):
        source = make_western_source()
        result = format_movie_filename("Heat (1995) {tmdb-949}", source)
        assert "complete movie" not in result

    def test_hash_appended(self):
        source = make_western_source(hash_code="ABCD1234")
        result = format_movie_filename("Heat (1995) {tmdb-949}", source)
        assert result.endswith("[ABCD1234].mkv")

    def test_extension_from_source(self):
        source = make_western_source()
        source.path = Path("source.mp4")
        assert format_movie_filename("Heat (1995)", source).endswith(".mp4")

    def test_no_media_info(self):
        source = make_western_source()
        source.media = None
        assert format_movie_filename("Heat (1995)", source) == "Heat (1995).mkv"


class TestFormatTvSeriesDirname:
    """Tests for Plex-convention series directory naming."""

    def test_basic(self):
        result = format_tv_series_dirname("Severance", 2022, 371980)
        assert result == "Severance (2022) {tvdb-371980}"

    def test_no_id(self):
        assert format_tv_series_dirname("Severance", 2022, None) == "Severance (2022)"

    def test_unknown_year_omitted(self):
        assert format_tv_series_dirname("Mystery", 0, 5) == "Mystery {tvdb-5}"

    def test_redundant_year_stripped(self):
        result = format_tv_series_dirname("ONE PIECE (2023)", 2023, 393190)
        assert result == "ONE PIECE (2023) {tvdb-393190}"


class TestFormatTvEpisodeFilename:
    """Tests for TV episode filenames (zero-padded season tags)."""

    def test_basic(self):
        source = make_western_source()
        result = format_tv_episode_filename(
            "Severance", 2022, 1, 1, "Good News About Hell", source
        )
        assert result == (
            "Severance (2022) - s01e01 - Good News About Hell "
            "[NTb Web,1080p,AVC,x264,EAC3].mkv"
        )

    def test_season_zero_padded(self):
        source = make_western_source()
        result = format_tv_episode_filename("Show", 2020, 12, 3, "Ep", source)
        assert " - s12e03 - " in result

    def test_specials_use_season_zero(self):
        source = make_western_source()
        result = format_tv_episode_filename("Show", 2020, 0, 5, "Bonus", source)
        assert " - s00e05 - " in result

    def test_multi_episode_range(self):
        source = make_western_source()
        result = format_tv_episode_filename(
            "Show", 2020, 1, 5, "Two-Parter", source, episodes=[5, 6]
        )
        assert " - s01e05-e06 - " in result

    def test_no_episode_title(self):
        source = make_western_source()
        result = format_tv_episode_filename("Show", 2020, 1, 1, "", source)
        assert result == "Show (2020) - s01e01 [NTb Web,1080p,AVC,x264,EAC3].mkv"

    def test_sanitizes_episode_title(self):
        source = make_western_source()
        result = format_tv_episode_filename(
            "Show", 2020, 1, 1, "Either/Or: Part 1", source
        )
        assert " - Either - Or- Part 1 " in result


class TestFormatDisplayTitle:
    """Directory names lead with the original-language title."""

    def test_native_with_english_bracketed(self):
        assert format_display_title("올드보이", "Oldboy") == "올드보이 [Oldboy]"

    def test_identical_titles_no_brackets(self):
        assert format_display_title("Heat", "Heat") == "Heat"

    def test_punctuation_only_difference_no_brackets(self):
        assert format_display_title("WALL·E", "WALL-E") == "WALL-E"

    def test_case_only_difference_no_brackets(self):
        assert format_display_title("ONE PIECE", "One Piece") == "One Piece"

    def test_missing_original_falls_back(self):
        assert format_display_title("", "Severance") == "Severance"

    def test_missing_english_falls_back(self):
        assert format_display_title("血は渇いてる", "") == "血は渇いてる"

    def test_latin_original_differs(self):
        assert format_display_title("La Haine", "Hate") == "La Haine [Hate]"


class TestDualTitleDirnames:
    def test_movie_dirname_with_original(self):
        result = format_movie_dirname("Oldboy", 2003, 670, original_title="올드보이")
        assert result == "올드보이 [Oldboy] (2003) {tmdb-670}"

    def test_movie_dirname_with_original_and_edition(self):
        result = format_movie_dirname(
            "Blood Is Dry",
            1960,
            99,
            edition="4K Remaster",
            original_title="血は渇いてる",
        )
        assert result == (
            "血は渇いてる [Blood Is Dry] (1960) {tmdb-99} {edition-4K Remaster}"
        )

    def test_tv_dirname_with_original(self):
        result = format_tv_series_dirname(
            "Ayaka is in Love with Hiroko!",
            2024,
            443158,
            original_title="彩香ちゃんは弘子先輩に恋してる",
        )
        assert result == (
            "彩香ちゃんは弘子先輩に恋してる [Ayaka is in Love with Hiroko!]"
            " (2024) {tvdb-443158}"
        )

    def test_identical_original_unchanged(self):
        assert (
            format_movie_dirname("Heat", 1995, 949, original_title="Heat")
            == "Heat (1995) {tmdb-949}"
        )
