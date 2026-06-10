"""Tests for media-ingestion.kdl loading (etp_lib.media_config)."""

from pathlib import Path

import pytest

from etp_lib.media_config import (
    MediaConfigError,
    load_media_config,
    lookup_mapping,
    save_title_mapping,
)
from etp_lib.types import (
    DEFAULT_MOVIES_DEST_DIR,
    DEFAULT_TELEVISION_SOURCE_DIR,
)

SAMPLE = """
paths {
  downloads-dir "/Volumes/docker/pvr/data/downloads"
  movies-source-dir "/Volumes/docker/pvr/data/movies"
  movies-dest-dir "/Volumes/video/movies"
  television-source-dir "/Volumes/docker/pvr/data/television"
  television-dest-dir "/Volumes/video/television"
}

movie "Blade Runner (1982)" {
  tmdb 78
  edition "Final Cut"
}

series "Severance (2022)" {
  tvdb 371980
  tmdb 95396
}
"""


class TestLoadMediaConfig:
    def test_defaults_when_missing(self, tmp_path):
        config = load_media_config(tmp_path / "nope.kdl")
        assert config.movies_dest_dir == DEFAULT_MOVIES_DEST_DIR
        assert config.television_source_dir == DEFAULT_TELEVISION_SOURCE_DIR
        assert config.movie_mappings == {}

    def test_paths_block(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        path.write_text(SAMPLE, encoding="utf-8")
        config = load_media_config(path)
        assert config.downloads_dir == Path("/Volumes/docker/pvr/data/downloads")
        assert config.movies_source_dir == Path("/Volumes/docker/pvr/data/movies")
        assert config.movies_dest_dir == Path("/Volumes/video/movies")
        assert config.television_source_dir == Path(
            "/Volumes/docker/pvr/data/television"
        )
        assert config.television_dest_dir == Path("/Volumes/video/television")

    def test_mappings(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        path.write_text(SAMPLE, encoding="utf-8")
        config = load_media_config(path)
        movie = config.movie_mappings["blade runner (1982)"]
        assert movie.tmdb_id == 78
        assert movie.edition == "Final Cut"
        series = config.series_mappings["severance (2022)"]
        assert series.tvdb_id == 371980
        assert series.tmdb_id == 95396

    def test_later_block_overrides_earlier(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        path.write_text(
            'series "X (2020)" {\n  tvdb 1\n}\nseries "X (2020)" {\n  tvdb 2\n}\n',
            encoding="utf-8",
        )
        config = load_media_config(path)
        assert config.series_mappings["x (2020)"].tvdb_id == 2


class TestLookupMapping:
    def test_case_insensitive(self, tmp_path):
        path = tmp_path / "c.kdl"
        path.write_text('movie "Heat (1995)" {\n  tmdb 949\n}\n', encoding="utf-8")
        config = load_media_config(path)
        found = lookup_mapping(config.movie_mappings, "HEAT (1995)")
        assert found is not None and found.tmdb_id == 949

    def test_first_name_wins(self, tmp_path):
        path = tmp_path / "c.kdl"
        path.write_text(
            'movie "Heat (1995)" {\n  tmdb 949\n}\nmovie "Heat" {\n  tmdb 1\n}\n',
            encoding="utf-8",
        )
        config = load_media_config(path)
        found = lookup_mapping(config.movie_mappings, "Heat (1995)", "Heat")
        assert found is not None and found.tmdb_id == 949

    def test_missing(self):
        assert lookup_mapping({}, "Nope") is None


class TestSaveTitleMapping:
    def test_append_and_reload(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        save_title_mapping("movie", "Heat (1995)", tmdb_id=949, path=path)
        save_title_mapping(
            "series", "Severance (2022)", tvdb_id=371980, tmdb_id=95396, path=path
        )
        config = load_media_config(path)
        assert config.movie_mappings["heat (1995)"].tmdb_id == 949
        assert config.series_mappings["severance (2022)"].tvdb_id == 371980

    def test_edition_escaped(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        save_title_mapping(
            "movie", 'The "Best" Movie (2000)', tmdb_id=1, edition='2" Cut', path=path
        )
        config = load_media_config(path)
        mapping = config.movie_mappings['the "best" movie (2000)']
        assert mapping.edition == '2" Cut'


class TestMediaConfigErrors:
    def test_non_integer_provider_id(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        path.write_text('movie "X (2020)" {\n  tmdb "abc"\n}\n', encoding="utf-8")
        with pytest.raises(MediaConfigError, match="must be an integer"):
            load_media_config(path)

    def test_invalid_kdl(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        path.write_text("paths { unterminated", encoding="utf-8")
        with pytest.raises(MediaConfigError, match="invalid KDL"):
            load_media_config(path)


class TestArrEndpoints:
    def test_urls_parsed(self, tmp_path):
        path = tmp_path / "media-ingestion.kdl"
        path.write_text(
            'radarr {\n  url "http://radarr:7878"\n}\n'
            'sonarr {\n  url "http://sonarr:8989"\n}\n',
            encoding="utf-8",
        )
        config = load_media_config(path)
        assert config.radarr_url == "http://radarr:7878"
        assert config.sonarr_url == "http://sonarr:8989"

    def test_default_empty(self, tmp_path):
        config = load_media_config(tmp_path / "nope.kdl")
        assert config.radarr_url == ""
        assert config.sonarr_url == ""
