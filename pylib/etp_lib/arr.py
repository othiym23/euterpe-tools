"""Radarr/Sonarr API clients for authoritative provider ID resolution.

Radarr knows the TMDB ID of every movie folder it manages, and Sonarr
knows the TheTVDB ID of every series folder, so for managed-tree
ingestion these APIs beat title searches: no ambiguity, no wrong-year
remakes. The planner indexes the managed items once per run and matches
scanned titles by folder name (with a normalized title+year fallback).
"""

from __future__ import annotations

import json
import urllib.request
from dataclasses import dataclass
from pathlib import PurePosixPath

from etp_lib.naming import normalize_title


@dataclass
class ArrEntry:
    """One movie/series as Radarr/Sonarr knows it."""

    title: str
    year: int
    folder: str  # basename of the managed directory
    tmdb_id: int | None = None
    tvdb_id: int | None = None
    imdb_id: str = ""


def _arr_request(base_url: str, endpoint: str, api_key: str) -> list[dict]:
    url = f"{base_url.rstrip('/')}{endpoint}"
    req = urllib.request.Request(
        url, headers={"X-Api-Key": api_key, "Accept": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _index(entries: dict[str, ArrEntry], entry: ArrEntry) -> None:
    """Index an entry by folder name and by normalized ``Title (Year)``."""
    if entry.folder:
        entries.setdefault(entry.folder.casefold(), entry)
    if entry.title and entry.year:
        entries.setdefault(normalize_title(f"{entry.title} ({entry.year})"), entry)


def fetch_radarr_index(url: str, api_key: str) -> dict[str, ArrEntry]:
    """Index every Radarr-managed movie by folder name and title+year."""
    entries: dict[str, ArrEntry] = {}
    for item in _arr_request(url, "/api/v3/movie", api_key):
        raw_tmdb = item.get("tmdbId")
        _index(
            entries,
            ArrEntry(
                title=item.get("title") or "",
                year=int(item.get("year") or 0),
                folder=PurePosixPath(str(item.get("path") or "")).name,
                tmdb_id=int(raw_tmdb) if raw_tmdb else None,
                imdb_id=item.get("imdbId") or "",
            ),
        )
    return entries


def fetch_sonarr_index(url: str, api_key: str) -> dict[str, ArrEntry]:
    """Index every Sonarr-managed series by folder name and title+year."""
    entries: dict[str, ArrEntry] = {}
    for item in _arr_request(url, "/api/v3/series", api_key):
        raw_tvdb = item.get("tvdbId")
        _index(
            entries,
            ArrEntry(
                title=item.get("title") or "",
                year=int(item.get("year") or 0),
                folder=PurePosixPath(str(item.get("path") or "")).name,
                tvdb_id=int(raw_tvdb) if raw_tvdb else None,
                imdb_id=item.get("imdbId") or "",
            ),
        )
    return entries


def lookup(index: dict[str, ArrEntry], raw_title: str, title: str, year: int):
    """Find a scanned title in an arr index, or None.

    Managed-tree folder names match exactly; downloads-mode groups fall
    back to the normalized ``Title (Year)`` key.
    """
    entry = index.get(raw_title.casefold())
    if entry is None and title and year:
        entry = index.get(normalize_title(f"{title} ({year})"))
    return entry
