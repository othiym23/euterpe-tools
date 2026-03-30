"""Tests for parsy-based media parser primitives.

Validates that each primitive parser recognizes the same vocabulary as
the existing parser's keyword sets and regex patterns.
"""

from __future__ import annotations

import pytest

from etp_lib import media_parser_parsy as pp


class TestResolution:
    @pytest.mark.parametrize(
        "input,expected",
        [
            ("1080p", "1080p"),
            ("720p", "720p"),
            ("480p", "480p"),
            ("2160p", "2160p"),
            ("1080i", "1080i"),
            ("4K", "4K"),
            ("4k", "4k"),
            ("576p", "576p"),
            ("540p", "540p"),
        ],
    )
    def test_standard_resolutions(self, input, expected):
        result = pp.resolution.parse(input)
        assert isinstance(result, pp.Resolution)
        assert result.value == expected

    @pytest.mark.parametrize(
        "input,expected",
        [
            ("1920x1080", "1920x1080"),
            ("1280x720", "1280x720"),
            ("3840x2160", "3840x2160"),
        ],
    )
    def test_dimension_resolutions(self, input, expected):
        result = pp.resolution.parse(input)
        assert isinstance(result, pp.Resolution)
        assert result.value == expected

    def test_rejects_non_resolution(self):
        with pytest.raises(Exception):
            pp.resolution.parse("hello")


class TestVideoCodec:
    @pytest.mark.parametrize(
        "input",
        ["HEVC", "hevc", "AVC", "avc", "x265", "x264", "H.264", "H.265", "AV1", "XviD"],
    )
    def test_known_codecs(self, input):
        result = pp.video_codec.parse(input)
        assert isinstance(result, pp.VideoCodec)
        assert result.value == input

    def test_rejects_unknown(self):
        with pytest.raises(Exception):
            pp.video_codec.parse("VP10")


class TestAudioCodec:
    @pytest.mark.parametrize(
        "input",
        [
            "AAC",
            "FLAC",
            "opus",
            "DTS",
            "DTS-HD",
            "AC3",
            "EAC3",
            "TrueHD",
            "PCM",
            "LPCM",
        ],
    )
    def test_simple_codecs(self, input):
        result = pp.audio_codec.parse(input)
        assert isinstance(result, pp.AudioCodec)
        assert result.value == input

    @pytest.mark.parametrize(
        "input",
        ["AAC2.0", "DDP5.1", "FLAC2.0", "DTS-HD MA", "DTS-HDMA"],
    )
    def test_compound_codecs(self, input):
        result = pp.audio_codec.parse(input)
        assert isinstance(result, pp.AudioCodec)
        assert result.value == input


class TestSource:
    @pytest.mark.parametrize(
        "input,expected_type",
        [
            ("BD", "BD"),
            ("BluRay", "BD"),
            ("Blu-Ray", "BD"),
            ("WEB", "Web"),
            ("WEB-DL", "Web"),
            ("CR", "Web"),
            ("AMZN", "Web"),
            ("NF", "Web"),
            ("DVD", "DVD"),
            ("DVDRip", "DVD"),
            ("DVD-R", "DVD-R"),
            ("HDTV", "HDTV"),
            ("SDTV", "SDTV"),
            ("VCD", "VCD"),
            ("CD-R", "CD-R"),
        ],
    )
    def test_source_types(self, input, expected_type):
        result = pp.source.parse(input)
        assert isinstance(result, pp.Source)
        assert result.value == input
        assert result.source_type == expected_type

    def test_raw_has_no_type(self):
        result = pp.source.parse("raw")
        assert isinstance(result, pp.Source)
        assert result.source_type == ""


class TestRemux:
    @pytest.mark.parametrize("input", ["REMUX", "remux", "Remux"])
    def test_remux(self, input):
        result = pp.remux.parse(input)
        assert isinstance(result, pp.Remux)


class TestEpisodeSE:
    @pytest.mark.parametrize(
        "input,season,episode,version",
        [
            ("S01E05", 1, 5, None),
            ("s1e1", 1, 1, None),
            ("S03E13", 3, 13, None),
            ("S01E01v2", 1, 1, 2),
            ("s02e24", 2, 24, None),
        ],
    )
    def test_season_episode(self, input, season, episode, version):
        result = pp.episode_se.parse(input)
        assert isinstance(result, pp.EpisodeSE)
        assert result.season == season
        assert result.episode == episode
        assert result.version == version


class TestEpisodeBare:
    @pytest.mark.parametrize(
        "input,episode,version",
        [
            ("01", 1, None),
            ("12", 12, None),
            ("999", 999, None),
            ("05v2", 5, 2),
        ],
    )
    def test_bare_episodes(self, input, episode, version):
        result = pp.episode_bare.parse(input)
        assert isinstance(result, pp.EpisodeBare)
        assert result.episode == episode
        assert result.version == version

    def test_rejects_year(self):
        with pytest.raises(Exception):
            pp.episode_bare.parse("2019")


class TestEpisodeJP:
    def test_japanese_episode(self):
        result = pp.episode_jp.parse("第01話")
        assert isinstance(result, pp.EpisodeJP)
        assert result.episode == 1

    def test_high_number(self):
        result = pp.episode_jp.parse("第100話")
        assert isinstance(result, pp.EpisodeJP)
        assert result.episode == 100


class TestSeasonJP:
    def test_japanese_season(self):
        result = pp.season_jp.parse("第1期")
        assert isinstance(result, pp.SeasonJP)
        assert result.season == 1

    def test_double_digit(self):
        result = pp.season_jp.parse("第12期")
        assert isinstance(result, pp.SeasonJP)
        assert result.season == 12


class TestSeasonWord:
    @pytest.mark.parametrize(
        "input,season",
        [
            ("1st Season", 1),
            ("2nd Season", 2),
            ("3rd Season", 3),
            ("4th Season", 4),
        ],
    )
    def test_ordinal_seasons(self, input, season):
        result = pp.season_word.parse(input)
        assert isinstance(result, pp.SeasonWord)
        assert result.season == season


class TestSeasonOnly:
    @pytest.mark.parametrize("input,season", [("S01", 1), ("s3", 3), ("S12", 12)])
    def test_bare_season(self, input, season):
        result = pp.season_only.parse(input)
        assert isinstance(result, pp.SeasonOnly)
        assert result.season == season


class TestSpecial:
    @pytest.mark.parametrize(
        "input,tag,number",
        [
            ("SP1", "SP", 1),
            ("OVA", "OVA", None),
            ("OVA2", "OVA", 2),
            ("OAD", "OAD", None),
            ("ONA", "ONA", None),
        ],
    )
    def test_specials(self, input, tag, number):
        result = pp.special.parse(input)
        assert isinstance(result, pp.Special)
        assert result.tag == tag
        assert result.number == number


class TestBatchRange:
    def test_tilde_range(self):
        result = pp.batch_range.parse("01~26")
        assert isinstance(result, pp.BatchRange)
        assert result.start == 1
        assert result.end == 26

    def test_spaced_range(self):
        result = pp.batch_range.parse("01 ~ 13")
        assert isinstance(result, pp.BatchRange)
        assert result.start == 1
        assert result.end == 13


class TestVersion:
    @pytest.mark.parametrize("input,num", [("v2", 2), ("v3", 3), ("V5", 5)])
    def test_versions(self, input, num):
        result = pp.version.parse(input)
        assert isinstance(result, pp.Version)
        assert result.number == num


class TestYear:
    @pytest.mark.parametrize("input", ["2019", "1999", "2024"])
    def test_valid_years(self, input):
        result = pp.year.parse(input)
        assert isinstance(result, pp.Year)
        assert result.value == int(input)

    def test_rejects_non_year(self):
        with pytest.raises(Exception):
            pp.year.parse("1080")


class TestCRC32:
    @pytest.mark.parametrize("input", ["ABCD1234", "abcd1234", "0DE08879"])
    def test_valid_hashes(self, input):
        result = pp.crc32.parse(input)
        assert isinstance(result, pp.CRC32)
        assert result.value == input.upper()

    def test_rejects_short(self):
        with pytest.raises(Exception):
            pp.crc32.parse("ABCD")


class TestLanguage:
    @pytest.mark.parametrize("input", ["jpn", "eng", "dual", "multi"])
    def test_languages(self, input):
        result = pp.language.parse(input)
        assert isinstance(result, pp.Language)
        assert result.value == input


class TestBonusEN:
    @pytest.mark.parametrize(
        "input,bonus_type",
        [
            ("NCOP", "NCOP"),
            ("NCOP1", "NCOP"),
            ("NCED", "NCED"),
            ("NCED2", "NCED"),
            ("NC OP1", "NCOP"),
            ("NC ED1", "NCED"),
            ("Creditless OP", "NCOP"),
            ("Creditless ED2", "NCED"),
        ],
    )
    def test_english_bonus(self, input, bonus_type):
        result = pp.bonus_en.parse(input)
        assert isinstance(result, pp.BonusKeyword)
        assert result.bonus_type == bonus_type


class TestMetadataWord:
    """metadata_word should match any resolution, codec, source, remux, or language."""

    @pytest.mark.parametrize("input", ["1080p", "HEVC", "FLAC", "BD", "REMUX", "jpn"])
    def test_recognizes_metadata(self, input):
        result = pp.metadata_word.parse(input)
        assert result is not None
