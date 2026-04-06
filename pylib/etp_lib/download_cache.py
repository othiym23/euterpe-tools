"""Msgpack-based cache for download directory scanning and grouping.

Caches the output of ``_scan_and_group`` and ``_build_download_index`` to
avoid re-walking the downloads directory on repeat runs. Invalidation is
directory-level: only subdirectories whose mtime changed are re-walked
and re-parsed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import msgpack

from etp_lib.paths import cache_dir
from etp_lib.types import DownloadIndex

_CACHE_VERSION = 4  # bumped: dir_mtimes preservation fix


@dataclass
class DownloadCache:
    """Cached download directory state."""

    groups: dict[str, list[Path]] = field(default_factory=dict)
    download_index: DownloadIndex = field(default_factory=DownloadIndex)
    dir_mtimes: dict[str, int] = field(default_factory=dict)
    """``{dir_path: mtime_ns}`` for each walked directory."""

    dest_ids: dict[tuple[str, int], str] = field(default_factory=dict)
    """``{(provider, id): dir_path}`` from scan_dest_ids."""
    dest_mtimes: dict[str, int] = field(default_factory=dict)
    """``{dir_path: mtime_ns}`` for the destination directory (depth 1)."""


def cache_path() -> Path:
    """Return the path to the downloads cache file."""
    return cache_dir("triage") / "downloads-cache.msgpack"


def _serialize(cache: DownloadCache) -> bytes:
    groups = {name: [str(p) for p in paths] for name, paths in cache.groups.items()}
    dl_index = {
        key: [(s, e, str(p), sz) for s, e, p, sz in entries]
        for key, entries in cache.download_index.by_series.items()
    }
    # dest_ids keys are (provider, id) tuples — serialize as "provider:id" strings
    dest_ids = {f"{p}:{i}": str(d) for (p, i), d in cache.dest_ids.items()}
    data = {
        "v": _CACHE_VERSION,
        "dirs": cache.dir_mtimes,
        "groups": groups,
        "dl_index": dl_index,
        "dl_count": cache.download_index.file_count,
        "dest_ids": dest_ids,
        "dest_dirs": cache.dest_mtimes,
    }
    result: bytes = msgpack.packb(data, use_bin_type=True)  # type: ignore[assignment]
    return result


def _deserialize(raw: bytes) -> DownloadCache | None:
    try:
        data = msgpack.unpackb(raw, raw=False)
    except msgpack.UnpackException, ValueError:
        return None

    if not isinstance(data, dict) or data.get("v") != _CACHE_VERSION:
        return None

    groups = {
        name: [Path(p) for p in paths] for name, paths in data.get("groups", {}).items()
    }
    dl_entries = {
        key: [(s, e, Path(p), sz) for s, e, p, sz in entries]
        for key, entries in data.get("dl_index", {}).items()
    }
    dl_index = DownloadIndex(by_series=dl_entries, file_count=data.get("dl_count", 0))

    # Deserialize dest_ids: "provider:id" -> (provider, int(id))
    raw_dest_ids = data.get("dest_ids", {})
    dest_ids: dict[tuple[str, int], str] = {}
    for key, dir_path in raw_dest_ids.items():
        parts = key.rsplit(":", 1)
        if len(parts) == 2:
            try:
                dest_ids[(parts[0], int(parts[1]))] = dir_path
            except ValueError:
                pass

    return DownloadCache(
        groups=groups,
        download_index=dl_index,
        dir_mtimes=data.get("dirs", {}),
        dest_ids=dest_ids,
        dest_mtimes=data.get("dest_dirs", {}),
    )


def load_cache() -> DownloadCache | None:
    """Load the download cache from disk, or None if missing/corrupt."""
    path = cache_path()
    if not path.exists():
        return None
    try:
        return _deserialize(path.read_bytes())
    except OSError:
        return None


def save_cache(cache: DownloadCache) -> None:
    """Write the download cache to disk, merging with existing data.

    Each subcommand and helper sets only the fields it owns
    (groups, download_index, dest_ids, dir_mtimes, dest_mtimes); the
    merge preserves fields from the existing cache that the caller
    didn't populate.
    """
    path = cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_cache()
    if existing is not None:
        if not cache.groups and existing.groups:
            cache.groups = existing.groups
        if not cache.download_index.by_series and existing.download_index.by_series:
            cache.download_index = existing.download_index
        if not cache.dir_mtimes and existing.dir_mtimes:
            cache.dir_mtimes = existing.dir_mtimes
        if not cache.dest_ids and existing.dest_ids:
            cache.dest_ids = existing.dest_ids
        if not cache.dest_mtimes and existing.dest_mtimes:
            cache.dest_mtimes = existing.dest_mtimes
    path.write_bytes(_serialize(cache))


def check_cache_freshness(
    source_dirs: list[Path], no_cache: bool = False
) -> tuple[DownloadCache | None, dict[str, int], bool]:
    """Check if the download cache is fresh.

    Returns ``(cache, current_mtimes, is_fresh)`` where:
    - *cache* is the loaded cache (or None if unavailable)
    - *current_mtimes* is the current directory mtime snapshot
    - *is_fresh* is True if the cache matches the current state
    """
    if no_cache:
        return None, scan_dir_mtimes(source_dirs), False

    cached = load_cache()
    current_mtimes = scan_dir_mtimes(source_dirs)

    if cached is not None:
        changed, removed = find_stale_dirs(cached.dir_mtimes, current_mtimes)
        if not changed and not removed:
            return cached, current_mtimes, True

    return cached, current_mtimes, False


def scan_dir_mtimes(source_dirs: list[Path]) -> dict[str, int]:
    """Collect mtime_ns for each directory in source_dirs (depth 1 + root).

    This is cheap even on Btrfs RAID 6 — just stat calls on ~100 dirs,
    no readdir of file contents.
    """
    mtimes: dict[str, int] = {}
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        mtimes[str(source_dir)] = source_dir.stat().st_mtime_ns
        try:
            for entry in os.scandir(source_dir):
                if entry.is_dir(follow_symlinks=False):
                    mtimes[str(entry.path)] = entry.stat().st_mtime_ns
        except OSError:
            pass
    return mtimes


def find_stale_dirs(
    cached_mtimes: dict[str, int], current_mtimes: dict[str, int]
) -> tuple[set[str], set[str]]:
    """Compare cached vs current directory mtimes.

    Returns ``(changed_dirs, removed_dirs)`` where:
    - *changed_dirs*: directories that are new or have a different mtime
    - *removed_dirs*: directories in the cache that no longer exist
    """
    changed = set()
    removed = set()

    for dir_path, mtime in current_mtimes.items():
        if dir_path not in cached_mtimes or cached_mtimes[dir_path] != mtime:
            changed.add(dir_path)

    for dir_path in cached_mtimes:
        if dir_path not in current_mtimes:
            removed.add(dir_path)

    return changed, removed
