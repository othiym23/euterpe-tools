"""Parity tests: compare parsy-based parser output against the existing parser.

For every test input, both parsers must produce identical ParsedMedia output
on the fields that the parsy parser implements. Gaps are tracked and reported.
"""

from __future__ import annotations

import pytest

from etp_lib import media_parser as mp
from etp_lib import media_parser_parsy as pp

# ---------------------------------------------------------------------------
# Test corpus: representative filenames from each convention
# ---------------------------------------------------------------------------

FANSUB_FILENAMES = [
    "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv",
    "[Erai-raws] Champignon no Majo - 11 [1080p CR WEB-DL AVC AAC][MultiSub][0A021911].mkv",
    "[Erai-raws] Honzuki no Gekokujou - Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen - 04v2 [1080p][Multiple Subtitle].mkv",
    "[ak-Submarines] Girls und Panzer - MLLSD - SP1 [WEB 1080p][D227DE6D].mkv",
    "[Erai-raws] Honzuki no Gekokujou S3 - 10 END [1080p][Multiple Subtitle][E91FC872].mkv",
    "[pspspsps] Nights with a Cat - 61v2 [4398ABA3].mkv",
    "[Vodes] Youjo Senki - S01E01.mkv",
    "[Vodes] Youjo Senki NC ED1.mkv",
    "[Vodes] Youjo Senki NC OP1.mkv",
]

SCENE_FILENAMES = [
    "You.and.I.Are.Polar.Opposites.S01E01.You.My.Polar.Opposite.1080p.CR.WEB-DL.DUAL.AAC2.0.H.264-VARYG.mkv",
    "TO.BE.HERO.X.S01E01.NICE.1080p.CR.WEB-DL.DUAL.AAC2.0.H.264.MSubs-ToonsHub.mkv",
    "Movie.2005.WEB-DL.2160p.mkv",
    "Golden.Kamuy.S01.1080p.BluRay.Remux.AVC.TrueHD.5.1-Hinna.mkv",
]

JAPANESE_FILENAMES = [
    "[アニメ BD] 探偵オペラミルキィホームズ(第1期) 第01話「屋根裏の入居者」(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv",
    "[アニメ BD] 探偵オペラミルキィホームズ 第2幕(第2期) 第05話「コソコソと支度」(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv",
]

SONARR_FILENAMES = [
    "The Magnificent KOTOBUKI (2019) - S01E01 - Moonlit Guns for Hire - 1080p BluRay REMUX AVC Japanese [LPCM 2.0 + DTS-HDMA 2.1] - B00BA.mkv",
    "Re ZERO Starting Life in Another World S03E13 The Warriors Commendation 1080p FLAC 2 0 AVC REMUX-FraMeSToR.mkv",
    "Golden Kamuy - s01e01 - Wenkamuy [Hinna Bluray-1080p Remux,8bit,AVC,FLAC].mkv",
]

ALL_FILENAMES = (
    FANSUB_FILENAMES + SCENE_FILENAMES + JAPANESE_FILENAMES + SONARR_FILENAMES
)

# Fields to compare — core identification fields
CORE_FIELDS = ["series_name", "episode", "season", "release_group"]

# Metadata fields
META_FIELDS = ["source_type", "is_remux", "resolution", "video_codec"]

# Filenames where the old parser has known bugs that the new parser fixes.
_OLD_PARSER_BUGS = {
    # Old parser misidentifies release group as [LPCM 2.0 + DTS-HDMA 2.1] and
    # includes metadata in episode_title. New parser correctly uses B00BA.
    "The Magnificent KOTOBUKI (2019) - S01E01 - Moonlit Guns for Hire - 1080p BluRay REMUX AVC Japanese [LPCM 2.0 + DTS-HDMA 2.1] - B00BA.mkv",
    # Old parser gets episode=5 from TrueHD.5.1 suffix (false positive).
    "Golden.Kamuy.S01.1080p.BluRay.Remux.AVC.TrueHD.5.1-Hinna.mkv",
}

# All fields we compare
COMPARE_FIELDS = (
    CORE_FIELDS
    + META_FIELDS
    + [
        "episode_title",
        "hash_code",
        "year",
        "is_special",
        "version",
        "bonus_type",
    ]
)


def _compare(
    old: mp.ParsedMedia, new: mp.ParsedMedia, fields: list[str]
) -> dict[str, tuple]:
    """Compare two ParsedMedia on given fields. Returns {field: (old_val, new_val)} for mismatches."""
    mismatches = {}
    for field in fields:
        old_val = getattr(old, field)
        new_val = getattr(new, field)
        if old_val != new_val:
            mismatches[field] = (old_val, new_val)
    return mismatches


class TestCoreParity:
    """Core identification fields must match between old and new parser."""

    @pytest.mark.parametrize(
        "filename", [f for f in ALL_FILENAMES if f not in _OLD_PARSER_BUGS]
    )
    def test_episode_detected(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        if old.episode is not None:
            assert new.episode == old.episode, (
                f"episode mismatch for {filename!r}: "
                f"old={old.episode}, new={new.episode}"
            )

    @pytest.mark.parametrize("filename", ALL_FILENAMES)
    def test_season_detected(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        if old.season is not None:
            assert new.season == old.season, (
                f"season mismatch for {filename!r}: old={old.season}, new={new.season}"
            )

    @pytest.mark.parametrize("filename", ALL_FILENAMES)
    def test_series_name(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        if old.series_name:
            assert new.series_name == old.series_name, (
                f"series_name mismatch for {filename!r}: "
                f"old={old.series_name!r}, new={new.series_name!r}"
            )


class TestMetaParity:
    """Metadata fields should match between old and new parser."""

    @pytest.mark.parametrize("filename", ALL_FILENAMES)
    def test_source_type(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        if old.source_type:
            assert new.source_type == old.source_type, (
                f"source_type mismatch for {filename!r}: "
                f"old={old.source_type!r}, new={new.source_type!r}"
            )

    @pytest.mark.parametrize("filename", ALL_FILENAMES)
    def test_is_remux(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        if old.is_remux:
            assert new.is_remux == old.is_remux, (
                f"is_remux mismatch for {filename!r}: "
                f"old={old.is_remux}, new={new.is_remux}"
            )

    @pytest.mark.parametrize("filename", ALL_FILENAMES)
    def test_resolution(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        if old.resolution:
            assert new.resolution == old.resolution, (
                f"resolution mismatch for {filename!r}: "
                f"old={old.resolution!r}, new={new.resolution!r}"
            )


class TestFullParity:
    """Full field-by-field comparison — reports all mismatches."""

    @pytest.mark.parametrize(
        "filename", [f for f in ALL_FILENAMES if f not in _OLD_PARSER_BUGS]
    )
    def test_all_fields(self, filename):
        old = mp.parse_component(filename)
        new = pp.parse_component_parsy(filename)
        mismatches = _compare(old, new, COMPARE_FIELDS)
        # Filter out fields where old parser produced empty/None
        significant = {
            k: v for k, v in mismatches.items() if v[0] not in ("", None, False, 0, [])
        }
        assert not significant, f"Mismatches for {filename!r}:\n" + "\n".join(
            f"  {k}: old={v[0]!r}, new={v[1]!r}" for k, v in significant.items()
        )
