"""TheTVDB v4 API client for fetching series metadata."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request

from etp_lib.paths import cache_dir
from etp_lib.types import CACHE_MAX_AGE_SECONDS, TVDB_MAX_PAGES, AnimeInfo, Episode

_TVDB_API_BASE = "https://api4.thetvdb.com/v4"


def _tvdb_request(endpoint: str, token: str) -> dict:
    """Make an authenticated GET request to TheTVDB v4 API."""
    url = f"{_TVDB_API_BASE}{endpoint}"
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def tvdb_login(api_key: str) -> str:
    """Authenticate with TheTVDB and return a bearer token."""
    url = f"{_TVDB_API_BASE}/login"
    payload = json.dumps({"apikey": api_key}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")

    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data["data"]["token"]


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
    aliases = series_data.get("aliases", [])
    translations = translations or {}

    # Canonical translations are preferred; fall back to primary name / aliases.
    title_ja = translations.get("jpn") or name
    title_en = translations.get("eng", "")
    if not title_en:
        for alias in aliases:
            if alias.get("language") == "eng":
                title_en = alias.get("name", "")
                break

    year_str = series_data.get("year", "")
    year = int(year_str) if year_str else 0

    # First aired date as fallback for year
    if not year:
        first_aired = series_data.get("firstAired", "")
        if first_aired and len(first_aired) >= 4:
            try:
                year = int(first_aired[:4])
            except ValueError:
                pass

    episodes: list[Episode] = []
    for ep in episodes_data:
        season_num = ep.get("seasonNumber", 1)
        ep_num = ep.get("number", 0)
        ep_name = ep.get("name", "")

        is_special = season_num == 0
        ep_type = "special" if is_special else "regular"
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

    episodes.sort(key=lambda e: (e.ep_type != "regular", e.number))

    return AnimeInfo(
        anidb_id=None,
        tvdb_id=series_id,
        title_ja=title_ja,
        title_en=title_en,
        year=year,
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


def fetch_tvdb_series(
    series_id: int,
    api_key: str,
    no_cache: bool = False,
) -> AnimeInfo:
    """Fetch series info from TheTVDB with caching."""
    cache_file = cache_dir("tvdb") / f"{series_id}.json"

    # Check cache (24h validity)
    if not no_cache and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_MAX_AGE_SECONDS:
            cached = json.loads(cache_file.read_text(encoding="utf-8"))
            return _parse_tvdb_json(
                cached["series"],
                cached["episodes"],
                series_id,
                translations=cached.get("translations"),
            )

    # Login and fetch
    token = tvdb_login(api_key)

    series_resp = _tvdb_request(f"/series/{series_id}", token)
    series_data = series_resp.get("data", {})

    # Fetch canonical translations for English and Japanese titles
    available = series_data.get("nameTranslations", [])
    want = [lang for lang in ("eng", "jpn") if lang in available]
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
    cache_file.write_text(json.dumps(cache_data, ensure_ascii=False), encoding="utf-8")

    return _parse_tvdb_json(series_data, all_episodes, series_id, translations)
