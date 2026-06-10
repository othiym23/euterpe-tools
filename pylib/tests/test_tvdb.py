"""Tests for TheTVDB JSON parsing."""

from __future__ import annotations

from etp_lib.tvdb import _parse_tvdb_json, _parse_tvdb_search
from etp_lib.types import MetadataProvider

# ---------------------------------------------------------------------------
# Fixtures: TheTVDB JSON
# ---------------------------------------------------------------------------

TVDB_SERIES_DATA = {
    "name": "テストアニメ",
    "year": "2020",
    "firstAired": "2020-01-01",
    "originalLanguage": "jpn",
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

    def test_english_original_ignores_japanese_translation(self):
        """A Japanese translation of an English-original show is not its
        original title (regression: Murderbot's directory led with
        マーダーボット)."""
        data = {
            "name": "Murderbot",
            "year": "2025",
            "originalLanguage": "eng",
            "aliases": [],
        }
        translations = {"eng": "Murderbot", "jpn": "マーダーボット"}
        info = _parse_tvdb_json(
            data, TVDB_EPISODES_DATA, 443396, translations=translations
        )
        assert info.title_en == "Murderbot"
        assert info.title_ja == "Murderbot"  # no distinct original title

    def test_korean_original_uses_korean_translation(self):
        data = {
            "name": "Oldboy Series",
            "year": "2003",
            "originalLanguage": "kor",
            "aliases": [],
        }
        translations = {"eng": "Oldboy Series", "kor": "올드보이"}
        info = _parse_tvdb_json(
            data, TVDB_EPISODES_DATA, 99999, translations=translations
        )
        assert info.title_ja == "올드보이"
        assert info.title_en == "Oldboy Series"

    def test_null_episode_name(self):
        """TVDB returns null for episode name (TBA) — should not crash."""
        episodes = [
            {"seasonNumber": 0, "number": 6, "name": None},
            {"seasonNumber": 1, "number": 1, "name": "Pilot"},
        ]
        info = _parse_tvdb_json(TVDB_SERIES_DATA, episodes, 12345)
        special = [e for e in info.episodes if e.season == 0][0]
        assert special.title_en == ""
        assert special.number == 6

    def test_null_season_and_number(self):
        """TVDB returns null for seasonNumber/number — should default safely."""
        episodes = [
            {"seasonNumber": None, "number": None, "name": "Mystery"},
        ]
        info = _parse_tvdb_json(TVDB_SERIES_DATA, episodes, 12345)
        assert info.episodes[0].season == 1
        assert info.episodes[0].number == 0
        assert info.episodes[0].title_en == "Mystery"

    def test_null_year_and_aliases(self):
        """TVDB returns null for year and alias name — should not crash."""
        data = {
            "name": "Test",
            "year": None,
            "firstAired": None,
            "aliases": [{"language": "eng", "name": None}],
        }
        info = _parse_tvdb_json(data, [], 12345)
        assert info.year == 0
        assert info.title_en == ""


# ---------------------------------------------------------------------------
# Fixtures: TheTVDB /search JSON
# ---------------------------------------------------------------------------

TVDB_SEARCH_DATA = [
    {
        "tvdb_id": "371980",
        "name": "Severance",
        "year": "2022",
        "translations": {"eng": "Severance"},
    },
    {
        "tvdb_id": "puppies",  # malformed ID: dropped
        "name": "Severed",
        "year": "2005",
    },
    {
        "tvdb_id": "81189",
        "name": "ブレイキング・バッド",
        "year": None,
        "translations": {"eng": "Breaking Bad"},
    },
]


class TestTvdbSearchParsing:
    """Tests for TheTVDB /search result parsing."""

    def test_parses_candidates(self):
        candidates = _parse_tvdb_search(TVDB_SEARCH_DATA)
        assert len(candidates) == 2
        first = candidates[0]
        assert first.provider == MetadataProvider.TVDB
        assert first.id == 371980
        assert first.title == "Severance"
        assert first.year == 2022

    def test_malformed_id_dropped(self):
        candidates = _parse_tvdb_search(TVDB_SEARCH_DATA)
        assert all(c.title != "Severed" for c in candidates)

    def test_english_translation_as_original_title(self):
        candidates = _parse_tvdb_search(TVDB_SEARCH_DATA)
        bb = candidates[1]
        assert bb.title == "ブレイキング・バッド"
        assert bb.original_title == "Breaking Bad"
        assert bb.year == 0

    def test_empty(self):
        assert _parse_tvdb_search([]) == []
