"""Tests for etp-anime."""

from __future__ import annotations

import importlib.machinery
import importlib.util
import subprocess
import sys as _sys
import types
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Load etp-anime as a module (no .py extension)
# ---------------------------------------------------------------------------

_anime_path = Path(__file__).resolve().parent / "etp-anime"
_loader = importlib.machinery.SourceFileLoader("etp_anime", str(_anime_path))
_spec = importlib.util.spec_from_loader("etp_anime", _loader)
assert _spec is not None
anime = types.ModuleType(_spec.name)
_spec.loader = _loader  # type: ignore[union-attr]
# Register in sys.modules so dataclasses can resolve the module
_sys.modules["etp_anime"] = anime
anime.__file__ = str(_anime_path)
_loader.exec_module(anime)


# ---------------------------------------------------------------------------
# Fixtures: sample mediainfo JSON
# ---------------------------------------------------------------------------

MEDIAINFO_HEVC_DUAL_AUDIO = {
    "media": {
        "track": [
            {"@type": "General", "VideoCount": "1", "AudioCount": "2"},
            {
                "@type": "Video",
                "Format": "HEVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "10",
                "Encoded_Library_Name": "x265",
                "Encoded_Library": "x265 - 2.6+22",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "en",
                "Title": "English 5.1",
            },
            {
                "@type": "Audio",
                "Format": "FLAC",
                "Language": "ja",
                "Title": "Japanese 2.0",
            },
        ]
    }
}

MEDIAINFO_AVC_X264 = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "AVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "10",
                "Encoded_Library_Name": "x264",
                "Encoded_Library": "x264 - core 161 r3018",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "en",
                "Title": "English 2.0 AAC",
            },
            {
                "@type": "Audio",
                "Format": "FLAC",
                "Language": "ja",
                "Title": "Japanese 2.0",
            },
        ]
    }
}

MEDIAINFO_HDR_DOLBY_VISION = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "HEVC",
                "Width": "3840",
                "Height": "2160",
                "BitDepth": "10",
                "HDR_Format": "Dolby Vision, Version 1.0, dvhe.08.06, BL+RPU",
                "HDR_Format_Compatibility": "HDR10",
            },
            {
                "@type": "Audio",
                "Format": "DTS",
                "Language": "ja",
                "Title": "Japanese 5.1",
            },
        ]
    }
}

MEDIAINFO_COMMENTARY = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "AVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "8",
            },
            {
                "@type": "Audio",
                "Format": "AC-3",
                "Language": "ja",
                "Title": "Japanese 2.0",
            },
            {
                "@type": "Audio",
                "Format": "AC-3",
                "Language": "ja",
                "Title": "Director's Commentary",
            },
        ]
    }
}

MEDIAINFO_MULTI_AUDIO = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "HEVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "10",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "ja",
                "Title": "Japanese",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "en",
                "Title": "English",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "de",
                "Title": "German",
            },
        ]
    }
}

# ---------------------------------------------------------------------------
# Fixtures: AniDB XML
# ---------------------------------------------------------------------------

ANIDB_XML_SAMPLE = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="28" restricted="false">
  <type>Movie</type>
  <episodecount>1</episodecount>
  <startdate>1988-07-16</startdate>
  <enddate>1988-07-16</enddate>
  <titles>
    <title xml:lang="ja" type="official">アキラ</title>
    <title xml:lang="en" type="official">Akira</title>
    <title xml:lang="x-jat" type="main">Akira</title>
  </titles>
  <episodes>
    <episode id="1" update="2023-01-15">
      <epno type="1">1</epno>
      <length>124</length>
      <rating votes="5">8.50</rating>
      <title xml:lang="en">Akira</title>
      <title xml:lang="ja">アキラ</title>
    </episode>
    <episode id="2" update="2023-01-15">
      <epno type="3">C1</epno>
      <length>2</length>
      <title xml:lang="en">Opening</title>
    </episode>
    <episode id="3" update="2023-01-15">
      <epno type="3">C2</epno>
      <length>2</length>
      <title xml:lang="en">Ending</title>
    </episode>
    <episode id="4" update="2023-01-15">
      <epno type="2">S1</epno>
      <length>30</length>
      <title xml:lang="en">Making of Akira</title>
      <title xml:lang="ja">アキラのメイキング</title>
    </episode>
    <episode id="5" update="2023-01-15">
      <epno type="4">T1</epno>
      <length>1</length>
      <title xml:lang="en">Trailer</title>
    </episode>
  </episodes>
</anime>
"""

ANIDB_XML_SERIES = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="1234" restricted="false">
  <type>TV Series</type>
  <episodecount>12</episodecount>
  <startdate>2020-01-01</startdate>
  <titles>
    <title xml:lang="ja" type="official">テストアニメ</title>
    <title xml:lang="en" type="official">Test Anime</title>
    <title xml:lang="x-jat" type="main">Test Anime</title>
  </titles>
  <episodes>
    <episode id="100"><epno type="1">1</epno>
      <title xml:lang="en">The Beginning</title>
      <title xml:lang="ja">始まり</title>
    </episode>
    <episode id="101"><epno type="1">2</epno>
      <title xml:lang="en">The Journey</title>
    </episode>
    <episode id="102"><epno type="1">3</epno>
      <title xml:lang="en">The End</title>
    </episode>
  </episodes>
</anime>
"""

ANIDB_XML_ERROR = "<error>Anime not found</error>"

# ---------------------------------------------------------------------------
# Fixtures: TheTVDB JSON
# ---------------------------------------------------------------------------

TVDB_SERIES_DATA = {
    "name": "テストアニメ",
    "year": "2020",
    "firstAired": "2020-01-01",
    "aliases": [
        {"language": "eng", "name": "Test Anime"},
        {"language": "fra", "name": "Anime de Test"},
    ],
}

TVDB_EPISODES_DATA = [
    {"seasonNumber": 1, "number": 1, "name": "Pilot"},
    {"seasonNumber": 1, "number": 2, "name": "Second Episode"},
    {"seasonNumber": 0, "number": 1, "name": "Special 1"},
]


# ===================================================================
# Test classes
# ===================================================================


class TestParseSourceFilename:
    """Tests for source filename parsing."""

    def test_bracketed_group_with_dash_episode(self):
        sf = anime.parse_source_filename(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        assert sf.release_group == "Cyan"
        assert sf.parsed_episode == 8
        assert sf.hash_code == "D98B31F3"

    def test_scene_format_with_s_e(self):
        sf = anime.parse_source_filename("BEASTARS.S01E05.1080p.BluRay.x264-GROUP.mkv")
        assert sf.parsed_season == 1
        assert sf.parsed_episode == 5
        assert sf.source_type == "BD"

    def test_erai_raws_format(self):
        sf = anime.parse_source_filename(
            "[Erai-raws] Champignon no Majo - 11 "
            "[1080p CR WEB-DL AVC AAC][MultiSub][0A021911].mkv"
        )
        assert sf.release_group == "Erai-raws"
        assert sf.parsed_episode == 11
        assert sf.source_type == "Web"
        assert sf.hash_code == "0A021911"

    def test_no_group_no_hash(self):
        sf = anime.parse_source_filename("My Anime - 03 (1080p).mkv")
        assert sf.release_group == ""
        assert sf.parsed_episode == 3
        assert sf.hash_code == ""

    def test_bd_remux_detection(self):
        sf = anime.parse_source_filename("[Group] Anime - 01 [BDREMUX 1080p HEVC].mkv")
        assert sf.source_type == "BD"
        assert sf.is_remux is True

    def test_web_sources(self):
        sf = anime.parse_source_filename(
            "[Erai-raws] Show - 01 [1080p CR WEB-DL AVC].mkv"
        )
        assert sf.source_type == "Web"

    def test_sonarr_format(self):
        sf = anime.parse_source_filename(
            "BEASTARS - s01e01 - The Moon and the Beast "
            "[NH Bluray-1080p,10bit,x264,AAC].mkv"
        )
        assert sf.parsed_season == 1
        assert sf.parsed_episode == 1
        assert sf.source_type == "BD"

    def test_no_episode_number(self):
        sf = anime.parse_source_filename("[Group] Movie Title [BD 1080p].mkv")
        assert sf.parsed_episode is None

    def test_scene_trailing_group(self):
        sf = anime.parse_source_filename(
            "Re.ZERO.Starting.Life.in.Another.World.S03E09.1080p.CR.WEB-DL.AAC2.0.H.264.DUAL-VARYG.mkv"
        )
        assert sf.release_group == "VARYG"
        assert sf.parsed_season == 3
        assert sf.parsed_episode == 9

    def test_scene_group_not_overridden_by_bracket(self):
        """Bracket group takes priority over scene trailing group."""
        sf = anime.parse_source_filename("[FLE] Show - 01 [1080p]-GROUP.mkv")
        assert sf.release_group == "FLE"

    def test_bracket_group_fallback(self):
        """Short bracketed tag like [PMR] at end is picked up as release group."""
        sf = anime.parse_source_filename(
            "Re ZERO Starting Life in Another World - S03E01v2 "
            "(BD Remux 1080p AVC FLAC E-AC-3) [Dual Audio] [PMR].mkv"
        )
        assert sf.release_group == "PMR"
        assert sf.parsed_season == 3
        assert sf.parsed_episode == 1
        assert sf.version == 2

    def test_bracket_group_not_crc32(self):
        """8-char hex in brackets is a CRC32 hash, not a release group."""
        sf = anime.parse_source_filename("[FLE] Show - 01 [4CC4766E].mkv")
        assert sf.release_group == "FLE"
        assert sf.hash_code == "4CC4766E"

    def test_version_dash_format(self):
        sf = anime.parse_source_filename("[MTBB] Title - 05v2 [hash1234].mkv")
        assert sf.release_group == "MTBB"
        assert sf.parsed_episode == 5
        assert sf.version == 2

    def test_version_s_e_format(self):
        sf = anime.parse_source_filename("Show.S01E05v3.1080p.BluRay.mkv")
        assert sf.parsed_season == 1
        assert sf.parsed_episode == 5
        assert sf.version == 3

    def test_no_version(self):
        sf = anime.parse_source_filename("[Group] Title - 05 [hash1234].mkv")
        assert sf.parsed_episode == 5
        assert sf.version is None

    def test_version_in_metadata_block(self):
        sf = anime.SourceFile(
            path=Path("test.mkv"),
            release_group="MTBB",
            version=2,
            media=anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )
        block = anime.build_metadata_block(sf)
        assert block.startswith("MTBB(v2)")

    def test_no_version_in_metadata_block(self):
        sf = anime.SourceFile(
            path=Path("test.mkv"),
            release_group="MTBB",
            media=anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )
        block = anime.build_metadata_block(sf)
        assert block.startswith("MTBB,")


class TestMediaInfoParsing:
    """Tests for mediainfo JSON parsing."""

    def test_hevc_dual_audio(self):
        mi = anime.parse_mediainfo_json(MEDIAINFO_HEVC_DUAL_AUDIO)
        assert mi.video_codec == "HEVC"
        assert mi.resolution == "1080p"
        assert mi.bit_depth == 10
        assert mi.encoding_lib == "x265"
        assert len(mi.audio_tracks) == 2
        assert mi.audio_tracks[0].codec == "aac"
        assert mi.audio_tracks[0].language == "en"
        assert mi.audio_tracks[1].codec == "flac"
        assert mi.audio_tracks[1].language == "ja"

    def test_avc_x264(self):
        mi = anime.parse_mediainfo_json(MEDIAINFO_AVC_X264)
        assert mi.video_codec == "AVC"
        assert mi.encoding_lib == "x264"

    def test_hdr_dolby_vision_with_compatibility(self):
        mi = anime.parse_mediainfo_json(MEDIAINFO_HDR_DOLBY_VISION)
        assert mi.video_codec == "HEVC"
        assert mi.resolution == "4K"
        assert mi.hdr_type == "DoVi,HDR"

    def test_dolby_vision_without_compatibility(self):
        data = {
            "media": {
                "track": [
                    {"@type": "General"},
                    {
                        "@type": "Video",
                        "Format": "HEVC",
                        "Width": "3840",
                        "Height": "2160",
                        "BitDepth": "10",
                        "HDR_Format": "Dolby Vision, Version 1.0, dvhe.05.06, BL+RPU",
                    },
                ]
            }
        }
        mi = anime.parse_mediainfo_json(data)
        assert mi.hdr_type == "DoVi"

    def test_commentary_detection(self):
        mi = anime.parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert len(mi.audio_tracks) == 2
        assert mi.audio_tracks[0].is_commentary is False
        assert mi.audio_tracks[1].is_commentary is True

    def test_codec_case_conventions(self):
        """Open-source codecs lowercase, proprietary uppercase."""
        mi = anime.parse_mediainfo_json(MEDIAINFO_HEVC_DUAL_AUDIO)
        assert mi.audio_tracks[0].codec == "aac"  # lowercase
        assert mi.audio_tracks[1].codec == "flac"  # lowercase

        mi2 = anime.parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert mi2.audio_tracks[0].codec == "AC3"  # uppercase

    def test_resolution_shorthands(self):
        assert anime._resolution_shorthand(1920, 1080) == "1080p"
        assert anime._resolution_shorthand(1280, 720) == "720p"
        assert anime._resolution_shorthand(3840, 2160) == "4K"
        assert anime._resolution_shorthand(960, 540) == "540p"
        assert anime._resolution_shorthand(720, 480) == "480p"

    def test_multi_audio(self):
        mi = anime.parse_mediainfo_json(MEDIAINFO_MULTI_AUDIO)
        assert len(mi.audio_tracks) == 3
        languages = {t.language for t in mi.audio_tracks}
        assert languages == {"ja", "en", "de"}

    def test_no_encoding_lib(self):
        mi = anime.parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert mi.encoding_lib == ""


class TestMetadataBlock:
    """Tests for metadata block construction."""

    def _make_source(self, **overrides):  # type: ignore[no-untyped-def]
        defaults = {
            "path": Path("test.mkv"),
            "release_group": "SubGroup",
            "source_type": "BD",
            "is_remux": False,
            "hash_code": "",
            "parsed_episode": 1,
            "media": anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=10,
                hdr_type="",
                audio_tracks=[
                    anime.AudioTrack("flac", "ja", "Japanese", False),
                    anime.AudioTrack("aac", "en", "English", False),
                ],
                encoding_lib="x265",
            ),
        }
        defaults.update(overrides)
        return anime.SourceFile(**defaults)

    def test_full_metadata_block(self):
        sf = self._make_source()
        block = anime.build_metadata_block(sf)
        assert block == "SubGroup BD,1080p,HEVC,10bit,x265,flac+aac,dual-audio"  # noqa: E501

    def test_remux_included(self):
        sf = self._make_source(is_remux=True)
        block = anime.build_metadata_block(sf)
        assert "REMUX" in block
        parts = block.split(",")
        assert parts.index("REMUX") < parts.index("1080p")

    def test_hdr_included(self):
        sf = self._make_source()
        sf.media.hdr_type = "HDR"
        block = anime.build_metadata_block(sf)
        assert "HDR" in block

    def test_dovi_included(self):
        sf = self._make_source()
        sf.media.hdr_type = "DoVi"
        sf.media.resolution = "4K"
        block = anime.build_metadata_block(sf)
        assert "DoVi" in block

    def test_no_encoding_lib(self):
        sf = self._make_source()
        sf.media.encoding_lib = ""
        block = anime.build_metadata_block(sf)
        assert "x264" not in block
        assert "x265" not in block

    def test_dual_audio_detection(self):
        sf = self._make_source()
        block = anime.build_metadata_block(sf)
        assert "dual-audio" in block

    def test_multi_audio_detection(self):
        sf = self._make_source()
        sf.media.audio_tracks.append(anime.AudioTrack("aac", "de", "German", False))
        block = anime.build_metadata_block(sf)
        assert "multi-audio" in block
        assert "dual-audio" not in block

    def test_commentary_excluded_from_dual_audio(self):
        """Commentary tracks don't count toward dual-audio."""
        sf = self._make_source()
        sf.media.audio_tracks = [
            anime.AudioTrack("flac", "ja", "Japanese", False),
            anime.AudioTrack("flac", "ja", "Commentary", True),
        ]
        block = anime.build_metadata_block(sf)
        assert "dual-audio" not in block
        assert "multi-audio" not in block

    def test_audio_codecs_joined_with_plus(self):
        sf = self._make_source()
        sf.media.audio_tracks = [
            anime.AudioTrack("DTS", "ja", "Japanese", False),
            anime.AudioTrack("flac", "en", "English", False),
        ]
        block = anime.build_metadata_block(sf)
        assert "DTS+flac" in block

    def test_no_media_returns_empty(self):
        sf = self._make_source(media=None)
        assert anime.build_metadata_block(sf) == ""

    def test_web_source_type(self):
        sf = self._make_source(source_type="Web")
        block = anime.build_metadata_block(sf)
        assert block.startswith("SubGroup Web,")


class TestFormatEpisodeFilename:
    """Tests for episode filename formatting."""

    def _make_source(self):  # type: ignore[no-untyped-def]
        return anime.SourceFile(
            path=Path("test.mkv"),
            release_group="NH",
            source_type="BD",
            hash_code="",
            parsed_episode=1,
            media=anime.MediaInfo(
                video_codec="AVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=10,
                hdr_type="",
                audio_tracks=[
                    anime.AudioTrack("aac", "ja", "Japanese", False),
                ],
                encoding_lib="x264",
            ),
        )

    def test_regular_episode_with_name(self):
        sf = self._make_source()
        result = anime.format_episode_filename(
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
        result = anime.format_episode_filename(
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
        result = anime.format_episode_filename(
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
        result = anime.format_episode_filename(
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
        result = anime.format_episode_filename(
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
        sf.hash_code = "ABCD1234"
        result = anime.format_episode_filename(
            concise_name="Show",
            season=1,
            episode=1,
            episode_name="Ep",
            source=sf,
        )
        assert "[ABCD1234]" in result

    def test_episode_number_zero_padded(self):
        sf = self._make_source()
        result = anime.format_episode_filename(
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
        result = anime.format_episode_filename(
            concise_name="Show",
            season=1,
            episode=1,
            episode_name="",
            source=sf,
        )
        assert result.endswith(".mp4")

    def test_colon_sanitized_in_episode_name(self):
        sf = self._make_source()
        result = anime.format_episode_filename(
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
        result = anime.format_episode_filename(
            concise_name="Fate/Zero",
            season=1,
            episode=1,
            episode_name="",
            source=sf,
        )
        assert "Fate - Zero" in result
        assert "/" not in result


class TestAnidbParsing:
    """Tests for AniDB XML parsing."""

    def test_parse_movie(self):
        info = anime._parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        assert info.anidb_id == 28
        assert info.title_ja == "アキラ"
        assert info.title_en == "Akira"
        assert info.year == 1988

    def test_episode_types(self):
        info = anime._parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        types = {e.ep_type for e in info.episodes}
        assert "regular" in types
        assert "credit" in types
        assert "special" in types
        assert "trailer" in types

    def test_episode_count(self):
        info = anime._parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        assert len(info.episodes) == 5

    def test_special_tags(self):
        info = anime._parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        specials = [e for e in info.episodes if e.ep_type != "regular"]
        tags = {e.special_tag for e in specials}
        assert "C1" in tags
        assert "C2" in tags
        assert "S1" in tags
        assert "T1" in tags

    def test_episode_titles(self):
        info = anime._parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        regular = [e for e in info.episodes if e.ep_type == "regular"]
        assert regular[0].title_en == "Akira"
        assert regular[0].title_ja == "アキラ"

    def test_backtick_replaced_in_en_episode_title(self):
        """Backticks in English episode titles are replaced with apostrophes."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="99" restricted="false">
  <type>TV Series</type>
  <episodecount>1</episodecount>
  <startdate>2020-01-01</startdate>
  <titles>
    <title xml:lang="ja" type="official">テスト</title>
    <title xml:lang="en" type="official">Test</title>
  </titles>
  <episodes>
    <episode id="1"><epno type="1">1</epno>
      <title xml:lang="en">The King`s Gambit</title>
      <title xml:lang="ja">王の`策略</title>
    </episode>
  </episodes>
</anime>
"""
        info = anime._parse_anidb_xml(xml, 99)
        assert info.episodes[0].title_en == "The King's Gambit"
        assert info.episodes[0].title_ja == "王の`策略"

    def test_series_with_multiple_episodes(self):
        info = anime._parse_anidb_xml(ANIDB_XML_SERIES, 1234)
        regular = [e for e in info.episodes if e.ep_type == "regular"]
        assert len(regular) == 3
        assert regular[0].title_en == "The Beginning"
        assert regular[1].title_en == "The Journey"
        assert regular[2].title_en == "The End"

    def test_ja_official_preferred_over_romaji_main(self):
        """ja official title is preferred even when x-jat main appears first in XML."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="6107" restricted="false">
  <type>TV Series</type>
  <episodecount>64</episodecount>
  <startdate>2009-04-05</startdate>
  <titles>
    <title xml:lang="x-jat" type="main">Hagane no Renkinjutsushi (2009)</title>
    <title xml:lang="en" type="official">Fullmetal Alchemist: Brotherhood</title>
    <title xml:lang="ja" type="official">鋼の錬金術師 (2009)</title>
  </titles>
  <episodes/>
</anime>
"""
        info = anime._parse_anidb_xml(xml, 6107)
        assert info.title_ja == "鋼の錬金術師 (2009)"
        assert info.title_en == "Fullmetal Alchemist: Brotherhood"

    def test_romaji_fallback_when_no_ja(self):
        """Falls back to x-jat main when no ja title exists."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="99" restricted="false">
  <type>TV Series</type>
  <episodecount>12</episodecount>
  <startdate>2020-01-01</startdate>
  <titles>
    <title xml:lang="x-jat" type="main">Romaji Title</title>
    <title xml:lang="en" type="official">English Title</title>
  </titles>
  <episodes/>
</anime>
"""
        info = anime._parse_anidb_xml(xml, 99)
        assert info.title_ja == "Romaji Title"
        assert info.title_en == "English Title"

    def test_error_response(self):
        with pytest.raises(ValueError, match="Anime not found"):
            anime._parse_anidb_xml(ANIDB_XML_ERROR, 99999)


class TestTvdbParsing:
    """Tests for TheTVDB JSON parsing."""

    def test_parse_series(self):
        info = anime._parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        assert info.tvdb_id == 12345
        assert info.title_ja == "テストアニメ"
        assert info.title_en == "Test Anime"
        assert info.year == 2020

    def test_episode_count(self):
        info = anime._parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        assert len(info.episodes) == 3

    def test_special_detection(self):
        info = anime._parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        specials = [e for e in info.episodes if e.ep_type == "special"]
        assert len(specials) == 1
        assert specials[0].special_tag == "S1"

    def test_episode_names(self):
        info = anime._parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        regular = [e for e in info.episodes if e.ep_type == "regular"]
        assert regular[0].title_en == "Pilot"

    def test_japanese_title_from_primary_name(self):
        """TheTVDB primary name is the original-language (Japanese) title."""
        info = anime._parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        assert info.title_ja == "テストアニメ"

    def test_no_english_alias(self):
        """When no English alias exists, title_en is empty."""
        data = {"name": "テストアニメ", "year": "2020", "aliases": []}
        info = anime._parse_tvdb_json(data, TVDB_EPISODES_DATA, 12345)
        assert info.title_ja == "テストアニメ"
        assert info.title_en == ""

    def test_translations_override_aliases(self):
        """Canonical translations take priority over primary name and aliases."""
        translations = {"jpn": "公式日本語名", "eng": "Official English Name"}
        info = anime._parse_tvdb_json(
            TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345, translations=translations
        )
        assert info.title_ja == "公式日本語名"
        assert info.title_en == "Official English Name"

    def test_translations_partial_eng_only(self):
        """When only eng translation exists, ja falls back to primary name."""
        translations = {"eng": "Official English Name"}
        info = anime._parse_tvdb_json(
            TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345, translations=translations
        )
        assert info.title_ja == "テストアニメ"
        assert info.title_en == "Official English Name"

    def test_translations_partial_jpn_only(self):
        """When only jpn translation exists, eng falls back to alias."""
        translations = {"jpn": "公式日本語名"}
        info = anime._parse_tvdb_json(
            TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345, translations=translations
        )
        assert info.title_ja == "公式日本語名"
        assert info.title_en == "Test Anime"  # from eng alias


class TestDirectoryNaming:
    """Tests for directory and ID file naming."""

    def test_format_series_dirname(self):
        result = anime.format_series_dirname("アキラ", "Akira", 1988)
        assert result == "アキラ [Akira] (1988)"

    def test_format_series_dirname_complex(self):
        result = anime.format_series_dirname("東のエデン", "Eden of the East", 2009)
        assert result == "東のエデン [Eden of the East] (2009)"

    def test_colon_sanitized(self):
        result = anime.format_series_dirname(
            "鋼の錬金術師 (2009)", "Fullmetal Alchemist: Brotherhood", 2009
        )
        assert result == "鋼の錬金術師 [Fullmetal Alchemist- Brotherhood] (2009)"

    def test_slash_sanitized(self):
        # Fate/Zero has no Japanese chars, so it uses the single-title format
        result = anime.format_series_dirname("Fate/Zero", "Fate/Zero", 2011)
        assert result == "Fate - Zero (2011)"

    def test_redundant_year_stripped(self):
        result = anime.format_series_dirname(
            "鋼の錬金術師 (2009)", "Fullmetal Alchemist (2009)", 2009
        )
        assert result == "鋼の錬金術師 [Fullmetal Alchemist] (2009)"

    def test_non_matching_year_kept(self):
        result = anime.format_series_dirname(
            "鋼の錬金術師 (2003)", "Fullmetal Alchemist", 2009
        )
        assert result == "鋼の錬金術師 (2003) [Fullmetal Alchemist] (2009)"

    def test_romaji_ja_uses_single_title(self):
        """Romaji-only Japanese title falls back to single-title format."""
        result = anime.format_series_dirname("BEASTARS", "BEASTARS", 2019)
        assert result == "BEASTARS (2019)"

    def test_romaji_ja_prefers_english(self):
        """When ja is romaji and en differs, use en as the single title."""
        result = anime.format_series_dirname(
            "Hagane no Renkinjutsushi", "Fullmetal Alchemist", 2003
        )
        assert result == "Fullmetal Alchemist (2003)"

    def test_empty_en_uses_ja(self):
        result = anime.format_series_dirname("アキラ", "", 1988)
        assert result == "アキラ (1988)"

    def test_empty_ja_uses_en(self):
        result = anime.format_series_dirname("", "Akira", 1988)
        assert result == "Akira (1988)"

    def test_identical_titles_single(self):
        """Identical ja and en after sanitization → single title."""
        result = anime.format_series_dirname("アキラ", "アキラ", 1988)
        assert result == "アキラ (1988)"

    def test_no_empty_brackets(self):
        """Never produce 'TITLE [] (YYYY)'."""
        result = anime.format_series_dirname("BEASTARS", "", 2019)
        assert "[]" not in result
        assert result == "BEASTARS (2019)"

    def test_create_series_directory(self, tmp_path):
        info = anime.AnimeInfo(
            anidb_id=28,
            tvdb_id=None,
            title_ja="アキラ",
            title_en="Akira",
            year=1988,
            episodes=[],
        )
        series_dir = anime.create_series_directory(
            tmp_path, info, seasons=[1], has_specials=True
        )
        assert series_dir.is_dir()
        assert (series_dir / "Season 01").is_dir()
        assert (series_dir / "Specials").is_dir()
        assert (series_dir / "anidb.id").read_text().strip() == "28"

    def test_create_series_directory_tvdb(self, tmp_path):
        info = anime.AnimeInfo(
            anidb_id=None,
            tvdb_id=79604,
            title_ja="ブラックラグーン",
            title_en="BLACK LAGOON",
            year=2006,
            episodes=[],
        )
        series_dir = anime.create_series_directory(tmp_path, info, seasons=[1, 2])
        assert (series_dir / "Season 01").is_dir()
        assert (series_dir / "Season 02").is_dir()
        assert (series_dir / "tvdb.id").read_text().strip() == "79604"

    def test_create_series_directory_dry_run(self, tmp_path, capsys):
        info = anime.AnimeInfo(
            anidb_id=28,
            tvdb_id=None,
            title_ja="アキラ",
            title_en="Akira",
            year=1988,
            episodes=[],
        )
        anime.create_series_directory(tmp_path, info, seasons=[1], dry_run=True)
        captured = capsys.readouterr()
        assert "[dry-run]" in captured.out


class TestScanDestIds:
    """Tests for scanning destination directory for ID files."""

    def test_finds_anidb_id(self, tmp_path):
        series = tmp_path / "アキラ [Akira] (1988)"
        series.mkdir()
        (series / "anidb.id").write_text("28\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result[("anidb", 28)] == series

    def test_finds_tvdb_id(self, tmp_path):
        series = tmp_path / "BLACK LAGOON (2006)"
        series.mkdir()
        (series / "tvdb.id").write_text("79604\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result[("tvdb", 79604)] == series

    def test_finds_both_ids(self, tmp_path):
        series = tmp_path / "Some Anime (2020)"
        series.mkdir()
        (series / "anidb.id").write_text("1234\n")
        (series / "tvdb.id").write_text("5678\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result[("anidb", 1234)] == series
        assert result[("tvdb", 5678)] == series

    def test_multiple_series(self, tmp_path):
        s1 = tmp_path / "Series One"
        s2 = tmp_path / "Series Two"
        s1.mkdir()
        s2.mkdir()
        (s1 / "anidb.id").write_text("100\n")
        (s2 / "anidb.id").write_text("200\n")

        result = anime.scan_dest_ids(tmp_path)
        assert len(result) == 2
        assert result[("anidb", 100)] == s1
        assert result[("anidb", 200)] == s2

    def test_empty_directory(self, tmp_path):
        assert anime.scan_dest_ids(tmp_path) == {}

    def test_nonexistent_directory(self):
        assert anime.scan_dest_ids(Path("/nonexistent")) == {}

    def test_ignores_invalid_id(self, tmp_path):
        series = tmp_path / "Bad ID"
        series.mkdir()
        (series / "anidb.id").write_text("not_a_number\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result == {}

    def test_ignores_files_in_dest(self, tmp_path):
        (tmp_path / "somefile.txt").write_text("hello")
        assert anime.scan_dest_ids(tmp_path) == {}


class TestResolveSeriesDirectory:
    """Tests for the 3-step directory resolution."""

    def _make_info(self, anidb_id: int | None = 28, tvdb_id: int | None = None):  # type: ignore[no-untyped-def]
        return anime.AnimeInfo(
            anidb_id=anidb_id,
            tvdb_id=tvdb_id,
            title_ja="アキラ",
            title_en="Akira",
            year=1988,
            episodes=[],
        )

    def test_finds_by_anidb_id(self, tmp_path):
        existing = tmp_path / "Akira (old name)"
        existing.mkdir()
        id_map = {("anidb", 28): existing}

        result = anime.resolve_series_directory(
            tmp_path, self._make_info(), id_map=id_map
        )
        assert result == existing

    def test_finds_by_tvdb_id(self, tmp_path):
        existing = tmp_path / "Akira (different name)"
        existing.mkdir()
        id_map = {("tvdb", 5678): existing}

        result = anime.resolve_series_directory(
            tmp_path, self._make_info(anidb_id=None, tvdb_id=5678), id_map=id_map
        )
        assert result == existing

    def test_finds_by_conventional_name(self, tmp_path):
        conventional = tmp_path / "アキラ [Akira] (1988)"
        conventional.mkdir()

        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result == conventional

    def test_creates_new_when_no_match(self, tmp_path, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result.name == "アキラ [Akira] (1988)"
        assert result.is_dir()
        assert (result / "anidb.id").read_text().strip() == "28"

    def test_id_match_takes_priority_over_name(self, tmp_path):
        """If both ID and conventional name exist, ID wins."""
        id_dir = tmp_path / "Akira (custom name)"
        id_dir.mkdir()
        conventional = tmp_path / "アキラ [Akira] (1988)"
        conventional.mkdir()
        id_map = {("anidb", 28): id_dir}

        result = anime.resolve_series_directory(
            tmp_path, self._make_info(), id_map=id_map
        )
        assert result == id_dir

    def test_creates_season_subdirs_on_id_match(self, tmp_path):
        existing = tmp_path / "Akira (old)"
        existing.mkdir()
        id_map = {("anidb", 28): existing}

        anime.resolve_series_directory(
            tmp_path,
            self._make_info(),
            id_map=id_map,
            seasons=[1, 2],
            has_specials=True,
        )
        assert (existing / "Season 01").is_dir()
        assert (existing / "Season 02").is_dir()
        assert (existing / "Specials").is_dir()

    def test_writes_id_file_on_name_match(self, tmp_path):
        conventional = tmp_path / "アキラ [Akira] (1988)"
        conventional.mkdir()

        anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert (conventional / "anidb.id").read_text().strip() == "28"

    def test_manual_path_absolute(self, tmp_path, monkeypatch):
        manual = tmp_path / "My Custom Akira Dir"
        manual.mkdir()
        monkeypatch.setattr("builtins.input", lambda _: str(manual))

        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result == manual

    def test_manual_path_relative(self, tmp_path, monkeypatch):
        manual = tmp_path / "My Custom Akira Dir"
        manual.mkdir()
        monkeypatch.setattr("builtins.input", lambda _: "My Custom Akira Dir")

        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result == manual


class TestCLI:
    """Tests for CLI argument parsing."""

    def test_anidb_and_tvdb_mutually_exclusive(self):
        parser = anime.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--anidb", "28", "--tvdb", "12345"])

    def test_requires_anidb_or_tvdb_or_triage(self):
        parser = anime.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_anidb_mode(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--anidb", "28"])
        assert args.anidb == 28
        assert args.tvdb is None
        assert args.triage is False

    def test_tvdb_mode(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--tvdb", "12345"])
        assert args.tvdb == 12345
        assert args.anidb is None
        assert args.triage is False

    def test_triage_mode(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--triage"])
        assert args.triage is True
        assert args.anidb is None
        assert args.tvdb is None

    def test_triage_and_anidb_mutually_exclusive(self):
        parser = anime.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--triage", "--anidb", "28"])

    def test_triage_and_tvdb_mutually_exclusive(self):
        parser = anime.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--triage", "--tvdb", "12345"])

    def test_triage_with_dry_run(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--triage", "--dry-run"])
        assert args.triage is True
        assert args.dry_run is True

    def test_single_file_mode(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--anidb", "28", "--file", "/tmp/test.mkv"])
        assert args.file == Path("/tmp/test.mkv")

    def test_dry_run(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--anidb", "28", "--dry-run"])
        assert args.dry_run is True

    def test_default_dest(self):
        parser = anime.build_parser()
        args = parser.parse_args(["--anidb", "28"])
        assert args.dest == anime.DEFAULT_DEST_DIR


class TestExtractSeriesName:
    """Tests for per-file series name extraction."""

    def test_bracketed_group_stripped(self):
        name = anime._extract_series_name(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        assert name == "Champignon no Majo"

    def test_sonarr_format(self):
        name = anime._extract_series_name(
            "BEASTARS - s01e01 - The Moon and the Beast "
            "[NH Bluray-1080p,10bit,x264,AAC].mkv"
        )
        assert name == "BEASTARS"

    def test_scene_format(self):
        name = anime._extract_series_name(
            "Girls.und.Panzer.S01E05.1080p.BluRay.x264-GROUP.mkv"
        )
        assert name == "Girls.und.Panzer"

    def test_no_group_no_hash(self):
        name = anime._extract_series_name("My Anime - 03 (1080p).mkv")
        assert name == "My Anime"

    def test_movie_no_episode(self):
        name = anime._extract_series_name("[Group] Movie Title [BD 1080p].mkv")
        assert name == "Movie Title [BD 1080p]"

    def test_empty_filename(self):
        name = anime._extract_series_name("")
        assert name == ""


class TestNormalizeForGrouping:
    """Tests for grouping normalization."""

    def test_lowercase(self):
        assert anime._normalize_for_grouping("Champignon no Majo") == "champignonnomajo"

    def test_strip_punctuation(self):
        assert anime._normalize_for_grouping("Girls & Panzer!") == "girlspanzer"

    def test_strip_spaces_and_dashes(self):
        assert anime._normalize_for_grouping("My-Anime Name") == "myanimename"

    def test_identical_after_normalization(self):
        a = anime._normalize_for_grouping("Champignon no Majo")
        b = anime._normalize_for_grouping("champignon no majo")
        assert a == b

    def test_empty(self):
        assert anime._normalize_for_grouping("") == ""


class TestScanAndGroup:
    """Tests for scanning and grouping source files."""

    def test_groups_by_series_name(self, tmp_path):
        # Create some fake media files
        (tmp_path / "[Cyan] Show A - 01 [1080p][AAAA1111].mkv").touch()
        (tmp_path / "[Cyan] Show A - 02 [1080p][BBBB2222].mkv").touch()
        (tmp_path / "[Cyan] Show B - 01 [1080p][CCCC3333].mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        assert len(groups) == 2
        # Show A should have 2 files
        assert any(len(files) == 2 for files in groups.values())
        # Show B should have 1 file
        assert any(len(files) == 1 for files in groups.values())

    def test_ordered_by_count_descending(self, tmp_path):
        (tmp_path / "[G] Big - 01.mkv").touch()
        (tmp_path / "[G] Big - 02.mkv").touch()
        (tmp_path / "[G] Big - 03.mkv").touch()
        (tmp_path / "[G] Small - 01.mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        counts = [len(files) for files in groups.values()]
        assert counts == sorted(counts, reverse=True)

    def test_subdirectory_scanning(self, tmp_path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "[G] SubShow - 01.mkv").touch()
        (sub / "[G] SubShow - 02.mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        assert len(groups) == 1
        assert len(list(groups.values())[0]) == 2

    def test_non_media_files_ignored(self, tmp_path):
        (tmp_path / "readme.txt").touch()
        (tmp_path / "subtitle.srt").touch()
        (tmp_path / "[G] Show - 01.mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        assert len(groups) == 1

    def test_empty_directory(self, tmp_path):
        groups = anime._scan_and_group([tmp_path])
        assert groups == {}

    def test_nonexistent_directory(self):
        groups = anime._scan_and_group([Path("/nonexistent/path")])
        assert groups == {}

    def test_multiple_source_dirs(self, tmp_path):
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "[G] Show - 01.mkv").touch()
        (dir_b / "[G] Show - 02.mkv").touch()

        groups = anime._scan_and_group([dir_a, dir_b])
        assert len(groups) == 1
        assert len(list(groups.values())[0]) == 2


class TestExtractConciseName:
    """Tests for concise name extraction from filenames."""

    def test_bracketed_group_stripped(self):
        sf = anime.SourceFile(
            path=Path("[Cyan] Champignon no Majo - 08 [1080p][ABCD1234].mkv")
        )
        name = anime._extract_concise_name([sf])
        assert name == "Champignon no Majo"

    def test_sonarr_format(self):
        sf = anime.SourceFile(
            path=Path(
                "BEASTARS - s01e01 - The Moon and the Beast "
                "[NH Bluray-1080p,10bit,x264,AAC].mkv"
            )
        )
        name = anime._extract_concise_name([sf])
        assert name == "BEASTARS"

    def test_empty_list(self):
        assert anime._extract_concise_name([]) == ""


class TestGroupDefaults:
    """Tests for sticky group defaults across files."""

    def test_defaults_initial_state(self):
        defaults = anime.GroupDefaults()
        assert defaults.release_group == ""
        assert defaults.source_type == ""

    def test_process_file_prompts_for_missing_group(self, monkeypatch):
        """When release_group is empty, _process_file prompts for it."""
        inputs = iter(["MTBB", "s1e01", "n"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        monkeypatch.setattr(
            anime,
            "analyze_file",
            lambda _: anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )

        sf = anime.SourceFile(path=Path("/tmp/test.mkv"))
        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="Test",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        defaults = anime.GroupDefaults()
        anime._process_file(
            sf,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=defaults,
        )
        assert sf.release_group == "MTBB"
        assert defaults.release_group == "MTBB"

    def test_defaults_carry_to_next_file(self, monkeypatch):
        """Defaults set for one file are offered for the next."""
        # First file: user types "MTBB" at release group prompt
        # Second file: user accepts default (empty input)
        prompts_seen: list[str] = []
        call_count = 0

        def fake_input(prompt: str) -> str:
            nonlocal call_count
            prompts_seen.append(prompt)
            call_count += 1
            # File 1: release group prompt → "MTBB", episode confirm → accept,
            #          copy confirm → no
            # File 2: release group prompt → accept default, episode → accept,
            #          copy confirm → no
            if "Release group" in prompt:
                return "MTBB" if call_count <= 3 else ""
            if "Episode" in prompt:
                return ""
            if "Copy" in prompt:
                return "n"
            return ""

        monkeypatch.setattr("builtins.input", fake_input)
        monkeypatch.setattr(
            anime,
            "analyze_file",
            lambda _: anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="Test",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        defaults = anime.GroupDefaults()

        sf1 = anime.SourceFile(path=Path("/tmp/ep01.mkv"), parsed_episode=1)
        anime._process_file(
            sf1,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=defaults,
        )
        assert defaults.release_group == "MTBB"

        sf2 = anime.SourceFile(path=Path("/tmp/ep02.mkv"), parsed_episode=2)
        anime._process_file(
            sf2,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=defaults,
        )
        # Second file should have picked up the default
        assert sf2.release_group == "MTBB"

    def test_no_prompt_when_group_present(self, monkeypatch):
        """No release group prompt when filename already has one."""
        prompts_seen: list[str] = []

        def fake_input(prompt: str) -> str:
            prompts_seen.append(prompt)
            if "Copy" in prompt:
                return "n"
            return ""

        monkeypatch.setattr("builtins.input", fake_input)
        monkeypatch.setattr(
            anime,
            "analyze_file",
            lambda _: anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="Test",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        sf = anime.SourceFile(
            path=Path("/tmp/test.mkv"),
            release_group="Cyan",
            parsed_episode=1,
        )
        anime._process_file(
            sf,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=anime.GroupDefaults(),
        )
        assert not any("Release group" in p for p in prompts_seen)


class TestTriageManifest:
    """Tests for triage copy-tracking manifest."""

    def test_roundtrip(self, monkeypatch, tmp_path):
        cache_dir = tmp_path / "triage"
        cache_dir.mkdir()
        monkeypatch.setattr(
            anime, "_triage_manifest_path", lambda: cache_dir / "copied.json"
        )

        assert anime._load_triage_manifest() == set()

        paths = {"/vol/a.mkv", "/vol/b.mkv"}
        anime._save_triage_manifest(paths)
        assert anime._load_triage_manifest() == paths

    def test_corrupt_manifest(self, monkeypatch, tmp_path):
        manifest = tmp_path / "copied.json"
        manifest.write_text("not json!!!", encoding="utf-8")
        monkeypatch.setattr(anime, "_triage_manifest_path", lambda: manifest)
        assert anime._load_triage_manifest() == set()

    def test_manifest_accumulates(self, monkeypatch, tmp_path):
        cache_dir = tmp_path / "triage"
        cache_dir.mkdir()
        monkeypatch.setattr(
            anime, "_triage_manifest_path", lambda: cache_dir / "copied.json"
        )

        anime._save_triage_manifest({"/vol/a.mkv"})
        loaded = anime._load_triage_manifest()
        loaded.add("/vol/b.mkv")
        anime._save_triage_manifest(loaded)
        assert anime._load_triage_manifest() == {"/vol/a.mkv", "/vol/b.mkv"}


class TestCrc32Verification:
    """Tests for CRC32 hash computation and verification."""

    def test_compute_crc32(self, tmp_path):
        f = tmp_path / "test.bin"
        f.write_bytes(b"hello world")
        # Known CRC32 of b"hello world"
        import zlib

        expected = f"{zlib.crc32(b'hello world') & 0xFFFFFFFF:08X}"
        assert anime.compute_crc32(f) == expected

    def test_verify_hash_match(self, tmp_path):
        f = tmp_path / "test.mkv"
        f.write_bytes(b"test data")
        import zlib

        crc = f"{zlib.crc32(b'test data') & 0xFFFFFFFF:08X}"
        sf = anime.SourceFile(path=f, hash_code=crc)
        result = anime.verify_hash(sf)
        assert result is not None
        ok, actual = result
        assert ok is True
        assert actual == crc

    def test_verify_hash_mismatch(self, tmp_path):
        f = tmp_path / "test.mkv"
        f.write_bytes(b"test data")
        sf = anime.SourceFile(path=f, hash_code="00000000")
        result = anime.verify_hash(sf)
        assert result is not None
        ok, actual = result
        assert ok is False
        assert len(actual) == 8

    def test_verify_hash_no_hash(self):
        sf = anime.SourceFile(path=Path("/tmp/test.mkv"), hash_code="")
        assert anime.verify_hash(sf) is None

    def test_verify_hash_case_insensitive(self, tmp_path):
        f = tmp_path / "test.mkv"
        f.write_bytes(b"test data")
        import zlib

        crc = f"{zlib.crc32(b'test data') & 0xFFFFFFFF:08x}"  # lowercase
        sf = anime.SourceFile(path=f, hash_code=crc)
        result = anime.verify_hash(sf)
        assert result is not None
        assert result[0] is True


# Helper to create a mock MediaInfo for batch tests
def _mock_media():  # type: ignore[no-untyped-def]
    return anime.MediaInfo(
        video_codec="HEVC",
        resolution="1080p",
        width=1920,
        height=1080,
        bit_depth=8,
        hdr_type="",
    )


class TestBuildManifestEntries:
    """Tests for batch manifest entry building."""

    def test_basic_entries(self, tmp_path, monkeypatch):
        f1 = tmp_path / "[Cyan] Show - 01 [1080p][AAAA1111].mkv"
        f1.write_bytes(b"file1")
        f2 = tmp_path / "[Cyan] Show - 02 [1080p][BBBB2222].mkv"
        f2.write_bytes(b"file2")

        monkeypatch.setattr(anime, "analyze_file", lambda _: _mock_media())
        monkeypatch.setattr(anime, "verify_hash", lambda _: None)

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[
                anime.Episode(1, "regular", "Pilot", "", ""),
                anime.Episode(2, "regular", "Second", "", ""),
            ],
        )
        entries = anime._build_manifest_entries(
            anime._parse_files([f1, f2]), info, "Show", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 2
        assert not entries[0].is_todo
        assert not entries[1].is_todo
        assert "s1e01" in str(entries[0].dest_path)
        assert "s1e02" in str(entries[1].dest_path)

    def test_unmatched_episode_todo(self, tmp_path, monkeypatch):
        f = tmp_path / "[Group] Movie [1080p].mkv"
        f.write_bytes(b"data")

        monkeypatch.setattr(anime, "analyze_file", lambda _: _mock_media())
        monkeypatch.setattr(anime, "verify_hash", lambda _: None)

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        entries = anime._build_manifest_entries(
            anime._parse_files([f]), info, "Show", tmp_path / "dest", verbose=False
        )
        assert len(entries) == 1
        assert entries[0].is_todo

    def test_hash_mismatch_strips_hash(self, tmp_path, monkeypatch):
        f = tmp_path / "[Group] Show - 01 [DEADBEEF].mkv"
        f.write_bytes(b"data")

        monkeypatch.setattr(anime, "analyze_file", lambda _: _mock_media())
        monkeypatch.setattr(anime, "verify_hash", lambda _: (False, "00000000"))

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        entries = anime._build_manifest_entries(
            anime._parse_files([f]), info, "Show", tmp_path / "dest", verbose=False
        )
        assert entries[0].hash_failed
        assert entries[0].source.hash_code == ""
        assert "DEADBEEF" not in str(entries[0].dest_path)

    def test_default_release_group_applied(self, tmp_path, monkeypatch):
        f1 = tmp_path / "[MTBB] Show - 01.mkv"
        f1.write_bytes(b"a")
        f2 = tmp_path / "Show - 02.mkv"  # no group
        f2.write_bytes(b"b")

        monkeypatch.setattr(anime, "analyze_file", lambda _: _mock_media())
        monkeypatch.setattr(anime, "verify_hash", lambda _: None)

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        entries = anime._build_manifest_entries(
            anime._parse_files([f1, f2]), info, "Show", tmp_path / "dest", verbose=False
        )
        # No sticky defaults — f2 has no group, stays empty
        assert entries[1].source.release_group == ""


class TestWriteManifest:
    """Tests for KDL manifest file writing."""

    def test_basic_format(self, tmp_path):
        sf = anime.SourceFile(
            path=tmp_path / "src.mkv", parsed_episode=1, parsed_season=1
        )
        dest_path = tmp_path / "series" / "Season 01" / "dst.mkv"
        entry = anime.ManifestEntry(source=sf, dest_path=dest_path)
        info = anime.AnimeInfo(
            anidb_id=42,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = anime._write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "etp-anime triage manifest" in content
            assert "AniDB: 42" in content
            assert "season 1 {" in content
            assert 'source "src.mkv"' in content
            assert 'dest "dst.mkv"' in content
        finally:
            path.unlink(missing_ok=True)

    def test_todo_tag(self, tmp_path):
        sf = anime.SourceFile(
            path=tmp_path / "src.mkv", parsed_episode=0, parsed_season=1
        )
        dest_path = tmp_path / "series" / "Season 01" / "dst.mkv"
        entry = anime.ManifestEntry(source=sf, dest_path=dest_path, is_todo=True)
        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = anime._write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "(todo)episode" in content
        finally:
            path.unlink(missing_ok=True)

    def test_hash_mismatch_comment(self, tmp_path):
        sf = anime.SourceFile(
            path=tmp_path / "src.mkv", parsed_episode=1, parsed_season=1
        )
        dest_path = tmp_path / "series" / "Season 01" / "dst.mkv"
        entry = anime.ManifestEntry(source=sf, dest_path=dest_path, hash_failed=True)
        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = anime._write_manifest([entry], info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "CRC32 MISMATCH" in content
        finally:
            path.unlink(missing_ok=True)

    def test_grouped_by_season(self, tmp_path):
        sf1 = anime.SourceFile(
            path=tmp_path / "s1e01.mkv", parsed_episode=1, parsed_season=1
        )
        sf2 = anime.SourceFile(
            path=tmp_path / "s2e01.mkv", parsed_episode=1, parsed_season=2
        )
        entries = [
            anime.ManifestEntry(
                source=sf1, dest_path=tmp_path / "series" / "Season 01" / "ep1.mkv"
            ),
            anime.ManifestEntry(
                source=sf2, dest_path=tmp_path / "series" / "Season 02" / "ep1.mkv"
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = anime._write_manifest(entries, info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            assert "season 1 {" in content
            assert "season 2 {" in content
        finally:
            path.unlink(missing_ok=True)

    def test_entries_sorted_by_episode(self, tmp_path):
        """Episodes within a season are sorted by episode number."""
        sf9 = anime.SourceFile(
            path=tmp_path / "ep09.mkv", parsed_episode=9, parsed_season=1
        )
        sf2 = anime.SourceFile(
            path=tmp_path / "ep02.mkv", parsed_episode=2, parsed_season=1
        )
        sf5 = anime.SourceFile(
            path=tmp_path / "ep05.mkv", parsed_episode=5, parsed_season=1
        )
        # Deliberately out of order
        entries = [
            anime.ManifestEntry(
                source=sf9, dest_path=tmp_path / "series" / "Season 01" / "e09.mkv"
            ),
            anime.ManifestEntry(
                source=sf2, dest_path=tmp_path / "series" / "Season 01" / "e02.mkv"
            ),
            anime.ManifestEntry(
                source=sf5, dest_path=tmp_path / "series" / "Season 01" / "e05.mkv"
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        path = anime._write_manifest(entries, info, "Test", tmp_path / "series")
        try:
            content = path.read_text(encoding="utf-8")
            # Find all episode lines and check order
            import re

            ep_nums = re.findall(r"episode (\d+) \{", content)
            assert ep_nums == ["2", "5", "9"]
        finally:
            path.unlink(missing_ok=True)


class TestParseManifest:
    """Tests for KDL manifest parsing."""

    def _make_kdl(self, season: int, source: str, dest: str) -> str:
        return (
            f"season {season} {{\n"
            f'  episode 1 {{\n    source "{source}"\n    dest "{dest}"\n  }}\n'
            f"}}\n"
        )

    def test_valid_entry(self, tmp_path):
        sf = anime.SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(self._make_kdl(1, "a.mkv", "dst.mkv"), encoding="utf-8")
        series_dir = tmp_path / "series"
        entries, errors = anime._parse_manifest(manifest, {"a.mkv": sf}, series_dir)
        assert len(entries) == 1
        assert len(errors) == 0
        assert entries[0][0] is sf
        assert entries[0][1] == series_dir / "Season 01" / "dst.mkv"

    def test_todo_rejected(self, tmp_path):
        sf = anime.SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season 1 {\n  (todo)episode 0 {\n    source "a.mkv"\n'
            '    dest "dst.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors = anime._parse_manifest(manifest, {"a.mkv": sf}, tmp_path)
        assert len(entries) == 0
        assert any("todo" in e for e in errors)

    def test_unknown_source(self, tmp_path):
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            self._make_kdl(1, "unknown.mkv", "dst.mkv"), encoding="utf-8"
        )
        entries, errors = anime._parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert any("unknown source" in e for e in errors)

    def test_empty_manifest(self, tmp_path):
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text("// all entries deleted\n", encoding="utf-8")
        entries, errors = anime._parse_manifest(manifest, {}, tmp_path)
        assert len(entries) == 0
        assert len(errors) == 0

    def test_slashdash_skipped(self, tmp_path):
        """KDL /- commented entries are excluded by the parser."""
        sf = anime.SourceFile(path=Path("/src/a.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'season 1 {\n  /- episode 1 {\n    source "a.mkv"\n'
            '    dest "dst.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        entries, errors = anime._parse_manifest(manifest, {"a.mkv": sf}, tmp_path)
        assert len(entries) == 0
        assert len(errors) == 0

    def test_specials_group(self, tmp_path):
        sf = anime.SourceFile(path=Path("/src/s.mkv"))
        manifest = tmp_path / "manifest.kdl"
        manifest.write_text(
            'specials {\n  episode 1 {\n    source "s.mkv"\n'
            '    dest "special.mkv"\n  }\n}\n',
            encoding="utf-8",
        )
        series_dir = tmp_path / "series"
        entries, errors = anime._parse_manifest(manifest, {"s.mkv": sf}, series_dir)
        assert len(entries) == 1
        assert entries[0][1] == series_dir / "Specials" / "special.mkv"


class TestOpenEditor:
    """Tests for editor invocation."""

    def test_visual_preferred(self, monkeypatch):
        called_with: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
            called_with.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.setenv("VISUAL", "code")
        monkeypatch.setenv("EDITOR", "nano")

        anime._open_editor(Path("/tmp/test.tsv"))
        assert called_with[0][0] == "code"

    def test_editor_fallback(self, monkeypatch):
        called_with: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
            called_with.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.delenv("VISUAL", raising=False)
        monkeypatch.setenv("EDITOR", "nano")

        anime._open_editor(Path("/tmp/test.tsv"))
        assert called_with[0][0] == "nano"

    def test_vi_default(self, monkeypatch):
        called_with: list[list[str]] = []

        def fake_run(cmd: list[str], **_kw: object) -> subprocess.CompletedProcess[str]:
            called_with.append(cmd)
            return subprocess.CompletedProcess(cmd, 0)

        monkeypatch.setattr(subprocess, "run", fake_run)
        monkeypatch.delenv("VISUAL", raising=False)
        monkeypatch.delenv("EDITOR", raising=False)

        anime._open_editor(Path("/tmp/test.tsv"))
        assert called_with[0][0] == "vi"


class TestExecuteManifest:
    """Tests for manifest execution."""

    def test_success_counting(self, monkeypatch):
        monkeypatch.setattr(anime, "copy_reflink", lambda *a, **kw: True)

        sf1 = anime.SourceFile(path=Path("/src/a.mkv"))
        sf2 = anime.SourceFile(path=Path("/src/b.mkv"))
        entries = [
            (sf1, Path("/dst/a.mkv")),
            (sf2, Path("/dst/b.mkv")),
        ]

        success, failed, copied = anime._execute_manifest(
            entries, dry_run=True, verbose=False
        )
        assert success == 2
        assert failed == 0
        assert len(copied) == 2

    def test_failure_counting(self, monkeypatch):
        call_count = 0

        def mock_copy(*_a: object, **_kw: object) -> bool:
            nonlocal call_count
            call_count += 1
            return call_count != 2  # second call fails

        monkeypatch.setattr(anime, "copy_reflink", mock_copy)

        entries = [
            (anime.SourceFile(path=Path("/src/a.mkv")), Path("/dst/a.mkv")),
            (anime.SourceFile(path=Path("/src/b.mkv")), Path("/dst/b.mkv")),
            (anime.SourceFile(path=Path("/src/c.mkv")), Path("/dst/c.mkv")),
        ]
        success, failed, copied = anime._execute_manifest(
            entries, dry_run=False, verbose=False
        )
        assert success == 2
        assert failed == 1
