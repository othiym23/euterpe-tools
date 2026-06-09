"""TMDB (The Movie Database) v3 API client.

Primary metadata provider for movies; cross-check provider for television
(TheTVDB stays primary for episode numbering and titles). Responses are
cached for 24 hours alongside the AniDB/TheTVDB caches.
"""

from __future__ import annotations

import hashlib
import json
import urllib.parse
import urllib.request

from etp_lib.paths import cache_dir
from etp_lib.provider_cache import load_cached_json, store_cached_json
from etp_lib.types import (
    MetadataProvider,
    MovieInfo,
    SearchCandidate,
    TmdbTvInfo,
    dedup_titles,
)

_TMDB_API_BASE = "https://api.themoviedb.org/3"


def _tmdb_request(
    endpoint: str, api_key: str, params: dict[str, str] | None = None
) -> dict:
    """Make an authenticated GET request to the TMDB v3 API.

    *api_key* may be either a v3 API key (sent as the ``api_key`` query
    parameter) or a v4 read-access token (a JWT, recognizable by its ``ey``
    prefix, sent as a Bearer header). Both work against all v3 endpoints.
    """
    params = dict(params or {})
    headers = {"Accept": "application/json"}
    if api_key.startswith("ey"):
        headers["Authorization"] = f"Bearer {api_key}"
    else:
        params["api_key"] = api_key

    url = f"{_TMDB_API_BASE}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _cached_request(
    cache_key: str,
    endpoint: str,
    api_key: str,
    params: dict[str, str] | None = None,
    no_cache: bool = False,
) -> dict:
    """GET *endpoint* with a 24-hour file cache keyed by *cache_key*."""
    cache_file = cache_dir("tmdb") / f"{cache_key}.json"
    cached = load_cached_json(cache_file, no_cache)
    if isinstance(cached, dict):
        return cached

    data = _tmdb_request(endpoint, api_key, params)
    store_cached_json(cache_file, data)
    return data


def _search_cache_key(kind: str, query: str, year: int | None) -> str:
    digest = hashlib.sha1(f"{query}|{year or ''}".encode()).hexdigest()[:16]
    return f"search-{kind}-{digest}"


def _year_of(date_str: str | None) -> int:
    """Extract the year from a TMDB ``YYYY-MM-DD`` date string."""
    if date_str and len(date_str) >= 4:
        try:
            return int(date_str[:4])
        except ValueError:
            pass
    return 0


# ---------------------------------------------------------------------------
# JSON parsing (pure, unit-tested)
# ---------------------------------------------------------------------------


def _parse_movie_results(data: dict) -> list[SearchCandidate]:
    """Parse ``/search/movie`` results into candidates, TMDB rank order."""
    return [
        SearchCandidate(
            provider=MetadataProvider.TMDB,
            id=r.get("id") or 0,
            title=r.get("title") or "",
            year=_year_of(r.get("release_date")),
            original_title=r.get("original_title") or "",
        )
        for r in data.get("results", [])
        if r.get("id")
    ]


def _parse_tv_results(data: dict) -> list[SearchCandidate]:
    """Parse ``/search/tv`` results into candidates, TMDB rank order."""
    return [
        SearchCandidate(
            provider=MetadataProvider.TMDB,
            id=r.get("id") or 0,
            title=r.get("name") or "",
            year=_year_of(r.get("first_air_date")),
            original_title=r.get("original_name") or "",
        )
        for r in data.get("results", [])
        if r.get("id")
    ]


def _parse_tmdb_movie(data: dict, movie_id: int) -> MovieInfo:
    """Parse a ``/movie/{id}`` response (with alternative_titles appended)."""
    title = data.get("title") or ""
    original_title = data.get("original_title") or ""
    alt_titles = (
        t.get("title") or ""
        for t in (data.get("alternative_titles") or {}).get("titles", [])
    )
    return MovieInfo(
        tmdb_id=movie_id,
        title=title,
        year=_year_of(data.get("release_date")),
        original_title=original_title,
        imdb_id=data.get("imdb_id") or "",
        aliases=dedup_titles((title, original_title, *alt_titles)),
    )


def _parse_tmdb_tv(data: dict, tv_id: int) -> TmdbTvInfo:
    """Parse a ``/tv/{id}`` response (with external_ids appended)."""
    external = data.get("external_ids") or {}
    raw_tvdb = external.get("tvdb_id")
    return TmdbTvInfo(
        tmdb_id=tv_id,
        title=data.get("name") or "",
        year=_year_of(data.get("first_air_date")),
        original_title=data.get("original_name") or "",
        tvdb_id=int(raw_tvdb) if raw_tvdb else None,
        imdb_id=external.get("imdb_id") or "",
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def search_movie(
    query: str, year: int | None, api_key: str, no_cache: bool = False
) -> list[SearchCandidate]:
    """Search TMDB movies, optionally filtered by release year."""
    params = {"query": query}
    if year:
        params["year"] = str(year)
    data = _cached_request(
        _search_cache_key("movie", query, year),
        "/search/movie",
        api_key,
        params,
        no_cache,
    )
    return _parse_movie_results(data)


def search_tv(
    query: str, year: int | None, api_key: str, no_cache: bool = False
) -> list[SearchCandidate]:
    """Search TMDB TV series, optionally filtered by first-air year."""
    params = {"query": query}
    if year:
        params["first_air_date_year"] = str(year)
    data = _cached_request(
        _search_cache_key("tv", query, year),
        "/search/tv",
        api_key,
        params,
        no_cache,
    )
    return _parse_tv_results(data)


def fetch_tmdb_movie(movie_id: int, api_key: str, no_cache: bool = False) -> MovieInfo:
    """Fetch full movie metadata including alternative titles."""
    data = _cached_request(
        f"movie-{movie_id}",
        f"/movie/{movie_id}",
        api_key,
        {"append_to_response": "alternative_titles"},
        no_cache,
    )
    return _parse_tmdb_movie(data, movie_id)


def fetch_tmdb_tv(tv_id: int, api_key: str, no_cache: bool = False) -> TmdbTvInfo:
    """Fetch TV series metadata including external (TheTVDB/IMDb) IDs."""
    data = _cached_request(
        f"tv-{tv_id}",
        f"/tv/{tv_id}",
        api_key,
        {"append_to_response": "external_ids"},
        no_cache,
    )
    return _parse_tmdb_tv(data, tv_id)
