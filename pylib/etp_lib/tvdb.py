"""TheTVDB v4 API client for fetching series metadata."""

from __future__ import annotations

import hashlib
import json
import urllib.error
import urllib.parse
import urllib.request

from etp_lib.paths import cache_dir
from etp_lib.provider_cache import load_cached_json, store_cached_json
from etp_lib.types import (
    TVDB_MAX_PAGES,
    AnimeInfo,
    Episode,
    EpisodeType,
    MetadataProvider,
    SearchCandidate,
    dedup_titles,
)

_TVDB_API_BASE = "https://api4.thetvdb.com/v4"


def _tvdb_request(endpoint: str, token: str) -> dict:
    """Make an authenticated GET request to TheTVDB v4 API."""
    url = f"{_TVDB_API_BASE}{endpoint}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


# Bearer tokens last weeks; cache them per key for the process lifetime so
# a plan run logs in once instead of once per search/fetch call.
_TOKEN_CACHE: dict[str, str] = {}


def tvdb_login(api_key: str) -> str:
    """Authenticate with TheTVDB and return a bearer token (cached)."""
    token = _TOKEN_CACHE.get(api_key)
    if token is not None:
        return token

    url = f"{_TVDB_API_BASE}/login"
    payload = json.dumps({"apikey": api_key}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    token = data["data"]["token"]
    _TOKEN_CACHE[api_key] = token
    return token


def _parse_tvdb_json(
    series_data: dict,
    episodes_data: list[dict],
    series_id: int,
    translations: dict[str, str] | None = None,
) -> AnimeInfo:
    """Parse TheTVDB series + episodes JSON into AnimeInfo.

    *translations* is an optional ``{lang: name}`` dict from the
    ``/series/{id}/translations/{lang}`` endpoint.  When present these
    canonical names take priority over the primary name and alias list.
    """
    name = series_data.get("name", "")
    raw_aliases = series_data.get("aliases", [])
    translations = translations or {}

    # Canonical translations are preferred; fall back to primary name /
    # aliases. Only the series' original-language translation counts as
    # the original title — a Japanese translation of an American show is
    # just a translation (マーダーボット is not Murderbot's original name).
    original_language = series_data.get("originalLanguage") or ""
    title_ja = translations.get(original_language) or name
    title_en = translations.get("eng") or ""
    if not title_en:
        for alias in raw_aliases:
            if alias.get("language") == "eng":
                title_en = alias.get("name") or ""
                break

    # Collect all alias names (primary name + every alias from any language)
    # so the matcher can recognize alternate transliterations like
    # "Sousou no Frieren" vs the canonical "Frieren: After the Funeral".
    alias_set = dedup_titles(
        (
            name,
            title_ja,
            title_en,
            *(alias.get("name") or "" for alias in raw_aliases),
        )
    )

    year_str = series_data.get("year") or ""
    year = int(year_str) if year_str else 0

    # First aired date as fallback for year
    if not year:
        first_aired = series_data.get("firstAired") or ""
        if first_aired and len(first_aired) >= 4:
            try:
                year = int(first_aired[:4])
            except ValueError:
                pass

    episodes: list[Episode] = []
    for ep in episodes_data:
        season_num = ep.get("seasonNumber")
        if season_num is None:
            season_num = 1
        ep_num = ep.get("number")
        if ep_num is None:
            ep_num = 0
        ep_name = ep.get("name") or ""

        is_special = season_num == 0
        ep_type = EpisodeType.SPECIAL if is_special else EpisodeType.REGULAR
        special_tag = f"s0e{ep_num:02d}" if is_special else ""

        episodes.append(
            Episode(
                number=ep_num,
                ep_type=ep_type,
                title_en=ep_name,
                title_ja="",
                special_tag=special_tag,
                season=season_num,
            )
        )

    episodes.sort(key=lambda e: (e.ep_type != EpisodeType.REGULAR, e.number))

    return AnimeInfo(
        anidb_id=None,
        tvdb_id=series_id,
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        aliases=alias_set,
        episodes=episodes,
    )


def _fetch_tvdb_translations(
    series_id: int, token: str, languages: list[str]
) -> dict[str, str]:
    """Fetch canonical translated names for a series.

    Returns ``{lang: name}`` for each language that has a translation.
    Silently skips languages that 404 or have no name.
    """
    result: dict[str, str] = {}
    for lang in languages:
        try:
            resp = _tvdb_request(f"/series/{series_id}/translations/{lang}", token)
            name = resp.get("data", {}).get("name", "")
            if name:
                result[lang] = name
        except urllib.error.HTTPError:
            pass
    return result


def _parse_tvdb_search(results: list[dict]) -> list[SearchCandidate]:
    """Parse ``/search`` results into candidates, TheTVDB rank order.

    TheTVDB returns ``tvdb_id`` and ``year`` as strings; entries whose ID
    doesn't parse are dropped.
    """
    candidates: list[SearchCandidate] = []
    for r in results:
        try:
            tvdb_id = int(r.get("tvdb_id") or "")
        except ValueError:
            continue
        year_str = r.get("year") or ""
        try:
            year = int(year_str)
        except ValueError:
            year = 0
        translations = r.get("translations") or {}
        candidates.append(
            SearchCandidate(
                provider=MetadataProvider.TVDB,
                id=tvdb_id,
                title=r.get("name") or "",
                year=year,
                original_title=translations.get("eng") or "",
            )
        )
    return candidates


def _search_tvdb(
    query: str, kind: str, api_key: str, no_cache: bool = False
) -> list[SearchCandidate]:
    """Search TheTVDB for *kind* (``series`` or ``movie``), with caching."""
    digest = hashlib.sha1(f"{kind}|{query}".encode()).hexdigest()[:16]
    cache_file = cache_dir("tvdb") / f"search-{digest}.json"

    cached = load_cached_json(cache_file, no_cache)
    if isinstance(cached, list):
        return _parse_tvdb_search(cached)

    token = tvdb_login(api_key)
    params = urllib.parse.urlencode({"query": query, "type": kind})
    resp = _tvdb_request(f"/search?{params}", token)
    results = resp.get("data", [])

    store_cached_json(cache_file, results)
    return _parse_tvdb_search(results)


def search_tvdb_series(
    query: str, api_key: str, no_cache: bool = False
) -> list[SearchCandidate]:
    """Search TheTVDB series by name."""
    return _search_tvdb(query, "series", api_key, no_cache)


def search_tvdb_movies(
    query: str, api_key: str, no_cache: bool = False
) -> list[SearchCandidate]:
    """Search TheTVDB movies by name (cross-check provider for movies)."""
    return _search_tvdb(query, "movie", api_key, no_cache)


def fetch_tvdb_series(
    series_id: int,
    api_key: str,
    no_cache: bool = False,
) -> AnimeInfo:
    """Fetch series info from TheTVDB with caching."""
    cache_file = cache_dir("tvdb") / f"{series_id}.json"

    # Check cache (24h validity).  Re-fetch if the cached result has no
    # episodes — the entry may have been fetched before episodes were added.
    cached = load_cached_json(cache_file, no_cache)
    if isinstance(cached, dict):
        info = _parse_tvdb_json(
            cached["series"],
            cached["episodes"],
            series_id,
            translations=cached.get("translations"),
        )
        if info.episodes:
            return info

    # Login and fetch
    token = tvdb_login(api_key)

    series_resp = _tvdb_request(f"/series/{series_id}", token)
    series_data = series_resp.get("data", {})

    # Fetch canonical translations for English and the series' original
    # language (library convention: the original-language title leads
    # directory names, whatever that language is).
    available = series_data.get("nameTranslations", [])
    original_language = series_data.get("originalLanguage") or ""
    want = [
        lang
        for lang in dict.fromkeys(("eng", original_language))
        if lang and lang in available
    ]
    translations = _fetch_tvdb_translations(series_id, token, want)

    # Fetch episodes with English translations when available
    all_episodes: list[dict] = []
    page = 0
    while page < TVDB_MAX_PAGES:
        ep_resp = _tvdb_request(
            f"/series/{series_id}/episodes/default/eng?page={page}", token
        )
        ep_data = ep_resp.get("data", {})
        ep_list = ep_data.get("episodes", [])
        if not ep_list:
            break
        all_episodes.extend(ep_list)
        page += 1

    # Cache the response (including translations)
    cache_data = {
        "series": series_data,
        "episodes": all_episodes,
        "translations": translations,
    }
    store_cached_json(cache_file, cache_data)

    return _parse_tvdb_json(series_data, all_episodes, series_id, translations)
