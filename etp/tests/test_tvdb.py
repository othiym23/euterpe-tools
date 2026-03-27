"""Tests for TheTVDB JSON parsing."""

from __future__ import annotations

from etp_lib.tvdb import _parse_tvdb_json

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


class TestTvdbParsing:
    """Tests for TheTVDB JSON parsing."""

    def test_parse_series(self):
        info = _parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        assert info.tvdb_id == 12345
        assert info.title_ja == "テストアニメ"
        assert info.title_en == "Test Anime"
        assert info.year == 2020

    def test_episode_count(self):
        info = _parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        assert len(info.episodes) == 3

    def test_special_detection(self):
        info = _parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        specials = [e for e in info.episodes if e.ep_type == "special"]
        assert len(specials) == 1
        assert specials[0].special_tag == "s0e01"

    def test_episode_names(self):
        info = _parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        regular = [e for e in info.episodes if e.ep_type == "regular"]
        assert regular[0].title_en == "Pilot"

    def test_japanese_title_from_primary_name(self):
        """TheTVDB primary name is the original-language (Japanese) title."""
        info = _parse_tvdb_json(TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345)
        assert info.title_ja == "テストアニメ"

    def test_no_english_alias(self):
        """When no English alias exists, title_en is empty."""
        data = {"name": "テストアニメ", "year": "2020", "aliases": []}
        info = _parse_tvdb_json(data, TVDB_EPISODES_DATA, 12345)
        assert info.title_ja == "テストアニメ"
        assert info.title_en == ""

    def test_translations_override_aliases(self):
        """Canonical translations take priority over primary name and aliases."""
        translations = {"jpn": "公式日本語名", "eng": "Official English Name"}
        info = _parse_tvdb_json(
            TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345, translations=translations
        )
        assert info.title_ja == "公式日本語名"
        assert info.title_en == "Official English Name"

    def test_translations_partial_eng_only(self):
        """When only eng translation exists, ja falls back to primary name."""
        translations = {"eng": "Official English Name"}
        info = _parse_tvdb_json(
            TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345, translations=translations
        )
        assert info.title_ja == "テストアニメ"
        assert info.title_en == "Official English Name"

    def test_translations_partial_jpn_only(self):
        """When only jpn translation exists, eng falls back to alias."""
        translations = {"jpn": "公式日本語名"}
        info = _parse_tvdb_json(
            TVDB_SERIES_DATA, TVDB_EPISODES_DATA, 12345, translations=translations
        )
        assert info.title_ja == "公式日本語名"
        assert info.title_en == "Test Anime"  # from eng alias
