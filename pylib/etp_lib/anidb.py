"""AniDB HTTP API client for fetching anime metadata."""

from __future__ import annotations

import gzip
import re
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

from etp_lib.paths import cache_dir
from etp_lib.types import CACHE_MAX_AGE_SECONDS, AnimeInfo, Episode, EpisodeType

# AniDB's HTTP API only supports plaintext HTTP on port 9001 — there is no
# HTTPS endpoint.  The "client" and "clientver" params are a registered app
# identifier, not a secret, so the risk is limited to response snooping.
_ANIDB_API_URL = "http://api.anidb.net:9001/httpapi"
_anidb_last_request: float = 0.0

# AniDB episode type constants
_ANIDB_EP_REGULAR = "1"
_ANIDB_EP_SPECIAL = "2"
_ANIDB_EP_CREDIT = "3"
_ANIDB_EP_TRAILER = "4"
_ANIDB_EP_PARODY = "5"
_ANIDB_EP_OTHER = "6"

_ANIDB_EP_TYPE_MAP: dict[str, EpisodeType] = {
    _ANIDB_EP_REGULAR: EpisodeType.REGULAR,
    _ANIDB_EP_SPECIAL: EpisodeType.SPECIAL,
    _ANIDB_EP_CREDIT: EpisodeType.CREDIT,
    _ANIDB_EP_TRAILER: EpisodeType.TRAILER,
    _ANIDB_EP_PARODY: EpisodeType.PARODY,
    _ANIDB_EP_OTHER: EpisodeType.OTHER,
}


def _anidb_rate_limit() -> None:
    """Enforce minimum 2-second gap between AniDB requests."""
    global _anidb_last_request
    now = time.monotonic()
    elapsed = now - _anidb_last_request
    if _anidb_last_request > 0 and elapsed < 2.0:
        time.sleep(2.0 - elapsed)
    _anidb_last_request = time.monotonic()


def _parse_anidb_xml(xml_text: str, aid: int) -> AnimeInfo:
    """Parse AniDB anime XML into an AnimeInfo."""
    root = ET.fromstring(xml_text)

    # Check for error
    if root.tag == "error":
        raise ValueError(f"AniDB API error: {root.text}")

    # Titles -- collect candidates in a single pass, then pick by priority.
    # Japanese: ja official > ja main > x-jat main > main fallback
    # English:  en official > en main > en synonym (first)
    ja_official = ""
    ja_main = ""
    jat_main = ""
    en_official = ""
    en_main = ""
    en_synonym = ""
    main_title_fallback = ""
    all_aliases: list[str] = []
    seen_aliases: set[str] = set()
    for title_elem in root.findall("titles/title"):
        lang = title_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
        ttype = title_elem.get("type", "")
        text = (title_elem.text or "").strip()

        if not text:
            continue

        # Collect main/official/synonym title variants in any language so
        # the matcher can recognize romaji and alternate transliterations.
        # "short" titles (e.g. "FMA") are excluded — too prone to collision.
        if ttype in ("main", "official", "synonym") and text not in seen_aliases:
            seen_aliases.add(text)
            all_aliases.append(text)

        if ttype == "main" and not main_title_fallback:
            main_title_fallback = text

        if lang == "ja" and ttype == "official" and not ja_official:
            ja_official = text
        elif lang == "ja" and ttype == "main" and not ja_main:
            ja_main = text
        elif lang == "x-jat" and ttype == "main" and not jat_main:
            jat_main = text

        if lang == "en" and ttype == "official" and not en_official:
            en_official = text
        elif lang == "en" and ttype == "main" and not en_main:
            en_main = text
        elif lang == "en" and ttype == "synonym" and not en_synonym:
            en_synonym = text

    title_ja = ja_official or ja_main or jat_main or main_title_fallback
    title_en = en_official or en_main or en_synonym

    # Year from startdate
    year = 0
    startdate = root.findtext("startdate", "")
    if startdate and len(startdate) >= 4:
        try:
            year = int(startdate[:4])
        except ValueError:
            pass

    # Episodes
    episodes: list[Episode] = []
    for ep_elem in root.findall("episodes/episode"):
        epno_elem = ep_elem.find("epno")
        if epno_elem is None:
            continue

        ep_type_str = epno_elem.get("type", _ANIDB_EP_REGULAR)
        ep_type = _ANIDB_EP_TYPE_MAP.get(ep_type_str, EpisodeType.OTHER)
        epno_text = (epno_elem.text or "").strip()

        # Parse episode number
        ep_number = 0
        # Regular episodes are just numbers; specials have letter prefixes
        num_match = re.search(r"(\d+)", epno_text)
        if num_match:
            ep_number = int(num_match.group(1))

        # Build special tag
        special_tag = ""
        if ep_type_str != _ANIDB_EP_REGULAR:
            # Use the raw epno text as the tag (e.g., "S1", "C1", "T1")
            special_tag = epno_text

        # Episode titles
        title_en_ep = ""
        title_ja_ep = ""
        title_jat_ep = ""
        for title_elem in ep_elem.findall("title"):
            lang = title_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
            text = (title_elem.text or "").strip()
            if lang == "en" and not title_en_ep:
                title_en_ep = text.replace("`", "'")
            elif lang == "ja" and not title_ja_ep:
                title_ja_ep = text
            elif lang == "x-jat" and not title_jat_ep:
                title_jat_ep = text

        episodes.append(
            Episode(
                number=ep_number,
                ep_type=ep_type,
                title_en=title_en_ep,
                title_ja=title_ja_ep,
                special_tag=special_tag,
                title_romaji=title_jat_ep,
            )
        )

    # Sort episodes: regulars by number, then specials by tag
    episodes.sort(key=lambda e: (e.ep_type != EpisodeType.REGULAR, e.number))

    return AnimeInfo(
        anidb_id=aid,
        tvdb_id=None,
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        title_romaji=jat_main,
        aliases=all_aliases,
        episodes=episodes,
    )


def fetch_anidb_anime(
    aid: int,
    client: str,
    clientver: int,
    no_cache: bool = False,
) -> AnimeInfo:
    """Fetch anime info from AniDB HTTP API with caching."""
    cache_file = cache_dir("anidb") / f"{aid}.xml"

    # Check cache (24h validity).  Re-fetch if the cached result has no
    # episodes — the entry may have been fetched before episodes were added.
    if not no_cache and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < CACHE_MAX_AGE_SECONDS:
            xml_text = cache_file.read_text(encoding="utf-8")
            info = _parse_anidb_xml(xml_text, aid)
            if info.episodes:
                return info

    # Fetch from API
    _anidb_rate_limit()

    params = urllib.parse.urlencode(
        {
            "request": "anime",
            "client": client,
            "clientver": clientver,
            "protover": 1,
            "aid": aid,
        }
    )
    url = f"{_ANIDB_API_URL}?{params}"

    req = urllib.request.Request(url)
    req.add_header("Accept-Encoding", "gzip")

    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        # Decompress if gzip
        if resp.headers.get("Content-Encoding") == "gzip":
            raw = gzip.decompress(raw)
        xml_text = raw.decode("utf-8")

    # Cache the response
    cache_file.write_text(xml_text, encoding="utf-8")

    return _parse_anidb_xml(xml_text, aid)
