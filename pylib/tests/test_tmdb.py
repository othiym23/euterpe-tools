"""Tests for TMDB JSON parsing."""

from __future__ import annotations

from etp_lib.tmdb import (
    _parse_movie_results,
    _parse_tmdb_movie,
    _parse_tmdb_tv,
    _parse_tv_results,
    _year_of,
)
from etp_lib.types import MetadataProvider

# ---------------------------------------------------------------------------
# Fixtures: TMDB JSON
# ---------------------------------------------------------------------------

MOVIE_SEARCH_DATA = {
    "results": [
        {
            "id": 949,
            "title": "Heat",
            "original_title": "Heat",
            "release_date": "1995-12-15",
        },
        {
            "id": 63696,
            "title": "Heat",
            "original_title": "Heat",
            "release_date": "1986-08-15",
        },
    ]
}

TV_SEARCH_DATA = {
    "results": [
        {
            "id": 95396,
            "name": "Severance",
            "original_name": "Severance",
            "first_air_date": "2022-02-17",
        },
    ]
}

MOVIE_DATA = {
    "id": 670,
    "title": "Oldboy",
    "original_title": "올드보이",
    "release_date": "2003-11-21",
    "imdb_id": "tt0364569",
    "alternative_titles": {
        "titles": [
            {"iso_3166_1": "KR", "title": "Old Boy", "type": ""},
            {"iso_3166_1": "RU", "title": "Олдбой", "type": ""},
        ]
    },
}

TV_DATA = {
    "id": 95396,
    "name": "Severance",
    "original_name": "Severance",
    "first_air_date": "2022-02-17",
    "external_ids": {"tvdb_id": 371980, "imdb_id": "tt11280740"},
}


class TestYearOf:
    def test_full_date(self):
        assert _year_of("1995-12-15") == 1995

    def test_none(self):
        assert _year_of(None) == 0

    def test_empty(self):
        assert _year_of("") == 0

    def test_garbage(self):
        assert _year_of("soon") == 0


class TestSearchParsing:
    def test_movie_results(self):
        candidates = _parse_movie_results(MOVIE_SEARCH_DATA)
        assert len(candidates) == 2
        assert candidates[0].provider == MetadataProvider.TMDB
        assert candidates[0].id == 949
        assert candidates[0].title == "Heat"
        assert candidates[0].year == 1995
        assert candidates[1].year == 1986

    def test_tv_results(self):
        candidates = _parse_tv_results(TV_SEARCH_DATA)
        assert len(candidates) == 1
        assert candidates[0].id == 95396
        assert candidates[0].title == "Severance"
        assert candidates[0].year == 2022

    def test_empty_results(self):
        assert _parse_movie_results({"results": []}) == []
        assert _parse_tv_results({}) == []

    def test_result_without_id_dropped(self):
        data = {"results": [{"title": "No ID", "release_date": "2020-01-01"}]}
        assert _parse_movie_results(data) == []


class TestMovieParsing:
    def test_basic_fields(self):
        info = _parse_tmdb_movie(MOVIE_DATA, 670)
        assert info.tmdb_id == 670
        assert info.title == "Oldboy"
        assert info.original_title == "올드보이"
        assert info.year == 2003
        assert info.imdb_id == "tt0364569"

    def test_aliases_include_alternative_titles(self):
        info = _parse_tmdb_movie(MOVIE_DATA, 670)
        assert "Old Boy" in info.aliases
        assert "Олдбой" in info.aliases

    def test_all_titles_deduped(self):
        info = _parse_tmdb_movie(MOVIE_DATA, 670)
        titles = info.all_titles()
        assert titles[0] == "Oldboy"
        assert len(titles) == len(set(titles))

    def test_missing_optional_fields(self):
        info = _parse_tmdb_movie({"id": 1, "title": "Bare"}, 1)
        assert info.title == "Bare"
        assert info.year == 0
        assert info.imdb_id == ""
        assert info.aliases == ["Bare"]

    def test_null_alternative_titles(self):
        info = _parse_tmdb_movie(
            {"id": 1, "title": "Bare", "alternative_titles": None}, 1
        )
        assert info.aliases == ["Bare"]


class TestTvParsing:
    def test_external_ids(self):
        info = _parse_tmdb_tv(TV_DATA, 95396)
        assert info.tmdb_id == 95396
        assert info.title == "Severance"
        assert info.year == 2022
        assert info.tvdb_id == 371980
        assert info.imdb_id == "tt11280740"

    def test_missing_external_ids(self):
        info = _parse_tmdb_tv({"id": 1, "name": "Bare"}, 1)
        assert info.tvdb_id is None
        assert info.imdb_id == ""

    def test_tvdb_id_as_string(self):
        data = {"id": 1, "name": "X", "external_ids": {"tvdb_id": "371980"}}
        assert _parse_tmdb_tv(data, 1).tvdb_id == 371980
