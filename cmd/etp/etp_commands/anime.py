"""Interactive anime collection manager.

Manages an anime collection on a Synology NAS: fetches metadata from AniDB
or TheTVDB, analyzes source files with mediainfo, constructs properly named
episode files, and copies them using Btrfs COW reflinks.

Subcommands:
    etp anime triage [PATTERN]  — bulk import from downloads directory
    etp anime series [PATTERN]  — sync from Sonarr-managed anime directory
    etp anime episode FILE      — import a single episode or movie

Configuration: ~/.config/euterpe-tools/anime-ingestion.kdl (paths + series IDs)
Environment:   ~/.config/euterpe-tools/anime.env (API credentials)
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
import readline  # noqa: F401 — enables line editing in input()
import shutil
import subprocess
import sys
from collections import Counter
from pathlib import Path

import kdl

from etp_lib import media_parser
from etp_lib.anidb import fetch_anidb_anime
from etp_lib.conflicts import (
    copy_reflink,
    handle_conflict,
    prompt_confirm,
    prompt_value,
    verify_hash,
)
from etp_lib.colorize import colorize_path
from etp_lib.download_cache import (
    DownloadCache,
    check_cache_freshness,
    find_stale_dirs,
    load_cache,
    save_cache,
    scan_dir_mtimes,
)
from etp_lib.manifest import ManifestWorkflow, escape_kdl
from etp_lib.mediainfo import analyze_file
from etp_lib.naming import (
    build_metadata_block,  # noqa: F401 (re-export for tests)
    format_episode_filename,
    format_series_dirname,
    season_subdir,
)
from etp_lib.paths import cache_dir as _cache_dir
from etp_lib.tvdb import fetch_tvdb_series
from etp_lib.types import (
    AnimeConfig,
    AnimeInfo,
    BatchResult,
    ConflictAction,
    DEFAULT_ANIME_SOURCE_DIR,  # noqa: F401 (re-export for tests)
    DEFAULT_DEST_DIR,  # noqa: F401 (re-export for tests)
    DEFAULT_DOWNLOADS_DIR,  # noqa: F401 (re-export for tests)
    DownloadIndex,
    Episode,  # noqa: F401 (re-export for tests)
    EpisodeType,
    GroupDefaults,
    MatchedFile,
    MediaInfo,  # noqa: F401 (re-export for tests)
    MetadataProvider,
    ParsedMetadata,
    SourceFile,
)

VERSION = "0.1.0"


def _load_env_file() -> None:
    """Load environment variables from the anime.env config file if it exists.

    Uses paths.py for platform-aware config directory resolution.
    Supports simple KEY=VALUE lines (no quoting, no interpolation).
    Lines starting with # are ignored. Existing env vars are not overwritten.
    """
    from etp_lib import paths as etp_paths

    env_path = etp_paths.anime_env()
    if not env_path.is_file():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


# ---------------------------------------------------------------------------
# Configuration (anime-ingestion.kdl)
# ---------------------------------------------------------------------------


def load_anime_config(path: Path | None = None) -> AnimeConfig:
    """Load anime ingestion config from a KDL file.

    Falls back to defaults if the file doesn't exist.
    """
    if path is None:
        from etp_lib import paths as etp_paths

        path = etp_paths.anime_config()

    config = AnimeConfig()
    if not path.exists():
        return config

    doc = kdl.parse(path.read_text(encoding="utf-8"))

    # Parse paths block
    paths_node = doc.get("paths")
    if paths_node is not None:
        for child in paths_node.nodes:
            key = child.name.replace("-", "_")
            if child.args:
                val = str(child.args[0])
                if key == "downloads_dir":
                    config.downloads_dir = Path(val)
                elif key == "anime_source_dir":
                    config.anime_source_dir = Path(val)
                elif key == "anime_dest_dir":
                    config.anime_dest_dir = Path(val)

    # Parse series mappings (multiple IDs per series for multi-season AniDB)
    for node in doc.getAll("series"):
        name = str(node.args[0]) if node.args else ""
        if not name:
            continue
        ids: list[tuple[str, int]] = config.series_mappings.setdefault(name, [])
        for child in node.nodes:
            if child.name == "anidb" and child.args:
                entry = (MetadataProvider.ANIDB, int(child.args[0]))
                if entry not in ids:
                    ids.append(entry)
            elif child.name == "tvdb" and child.args:
                entry = (MetadataProvider.TVDB, int(child.args[0]))
                if entry not in ids:
                    ids.append(entry)
            elif child.name == "concise" and child.args:
                config.concise_names[name] = str(child.args[0])

    return config


def save_series_mapping(
    name: str,
    provider: str,
    provider_id: int,
    *,
    concise_name: str = "",
    path: Path | None = None,
) -> None:
    """Append a series->ID mapping to the config file.

    *concise_name* is the parser-extracted series name (without year,
    quality tags, etc.) stored as a ``concise`` property so that future
    title matching can use it alongside the directory name.
    """
    if path is None:
        from etp_lib import paths as etp_paths

        path = etp_paths.anime_config()

    path.parent.mkdir(parents=True, exist_ok=True)

    concise_line = ""
    if concise_name and concise_name != name:
        concise_line = f'\n  concise "{escape_kdl(concise_name)}"'

    line = (
        f'\nseries "{escape_kdl(name)}" {{\n'
        f"  {provider} {provider_id}{concise_line}\n"
        f"}}\n"
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def lookup_series_ids(name: str, config: AnimeConfig) -> list[tuple[str, int]]:
    """Look up series IDs from config mappings by name (case-insensitive).

    Returns a list of ``(provider, id)`` tuples -- multiple entries for
    multi-season AniDB series.
    """
    name_lower = name.lower()
    for series_name, mappings in config.series_mappings.items():
        if series_name.lower() == name_lower:
            return mappings
    return []


def _parse_id_input(raw: str) -> tuple[int | None, int | None]:
    """Parse a user-entered ID string into (anidb_id, tvdb_id).

    Returns ``(None, None)`` on invalid input.
    """
    try:
        if raw.lower().startswith("t"):
            return None, int(raw[1:])
        return int(raw), None
    except ValueError:
        return None, None


def _maybe_save_mapping(
    name: str,
    provider: str,
    pid: int,
    config: AnimeConfig,
    dry_run: bool,
    concise_name: str = "",
) -> None:
    """Save a series->ID mapping to config if this specific ID is not already saved."""
    if dry_run:
        return
    entry = (provider, pid)
    existing = lookup_series_ids(name, config)
    if entry in existing:
        return
    save_series_mapping(name, provider, pid, concise_name=concise_name)
    config.series_mappings.setdefault(name, []).append(entry)


# ---------------------------------------------------------------------------
# Source filename parsing
# ---------------------------------------------------------------------------

# Media file extensions (canonical set in media_parser)
_MEDIA_EXTENSIONS = media_parser._MEDIA_EXTENSIONS
_VIDEO_EXTENSIONS = media_parser._VIDEO_EXTENSIONS
_AUDIO_EXTENSIONS = media_parser._AUDIO_EXTENSIONS
_EXTRAS_EXTENSIONS = frozenset({".rar", ".zip", ".7z", ".flac", ".wav", ".ape", ".txt"})


def parse_source_filename(filename: str) -> SourceFile:
    """Parse an anime release filename into a SourceFile.

    Delegates to media_parser for tokenization and classification.
    """
    pm = media_parser.parse_component(filename)
    return SourceFile(
        path=Path(filename),
        parsed=ParsedMetadata(
            series_name=pm.series_name,
            release_group=pm.release_group,
            source_type=pm.source_type,
            is_remux=pm.is_remux,
            hash_code=pm.hash_code,
            episode=pm.episode,
            season=pm.season,
            version=pm.version,
            bonus_type=pm.bonus_type,
            is_special=pm.is_special,
            special_tag=pm.special_tag,
            episode_title=pm.episode_title,
            is_dual_audio=pm.is_dual_audio,
            is_uncensored=pm.is_uncensored,
            series_name_alt=pm.series_name_alt,
            episodes=pm.episodes,
            streaming_service=pm.streaming_service,
        ),
    )


# ---------------------------------------------------------------------------
# Directory management
# ---------------------------------------------------------------------------


def build_id_queue(
    series_name: str,
    config: AnimeConfig,
    source_dir: Path | None = None,
) -> list[tuple[str, int]]:
    """Build an ordered ID queue from source directory .id files and config.

    Reads ``anidb.id`` / ``tvdb.id`` from *source_dir* (if provided), then
    appends any config mappings not already in the queue.
    """
    queue: list[tuple[str, int]] = []
    if source_dir is not None:
        for id_filename, provider in (
            ("anidb.id", MetadataProvider.ANIDB),
            ("tvdb.id", MetadataProvider.TVDB),
        ):
            id_file = source_dir / id_filename
            if id_file.is_file():
                try:
                    raw = id_file.read_text(encoding="utf-8").strip()
                    if raw:
                        queue.append((provider, int(raw)))
                except ValueError, OSError:
                    pass
    for entry in lookup_series_ids(series_name, config):
        if entry not in queue:
            queue.append(entry)
    return queue


def scan_dest_ids(dest: Path) -> dict[tuple[str, int], Path]:
    """Scan destination directory for existing series with ID files.

    Returns a map of ``("anidb", id)`` or ``("tvdb", id)`` to the series
    directory path.
    """
    result: dict[tuple[str, int], Path] = {}
    if not dest.is_dir():
        return result

    for entry in dest.iterdir():
        if not entry.is_dir():
            continue
        for id_filename, provider in (
            ("anidb.id", MetadataProvider.ANIDB),
            ("tvdb.id", MetadataProvider.TVDB),
        ):
            id_file = entry / id_filename
            if id_file.is_file():
                try:
                    raw = id_file.read_text(encoding="utf-8").strip()
                    if raw:
                        result[(provider, int(raw))] = entry
                except ValueError, OSError:
                    pass
    return result


def _cached_scan_dest_ids(
    dest: Path, no_cache: bool = False
) -> dict[tuple[str, int], Path]:
    """Scan dest IDs with caching — avoids walking 1500+ dirs on repeat runs."""
    if not no_cache:
        cached = load_cache()
        if cached is not None and cached.dest_ids:
            dest_mtimes = scan_dir_mtimes([dest])
            changed, removed = find_stale_dirs(cached.dest_mtimes, dest_mtimes)
            if not changed and not removed:
                print(f"Using cached dest ID map ({len(cached.dest_ids)} entries).")
                return {k: Path(v) for k, v in cached.dest_ids.items()}

    id_map = scan_dest_ids(dest)
    save_cache(
        DownloadCache(
            dest_ids={k: str(v) for k, v in id_map.items()},
            dest_mtimes=scan_dir_mtimes([dest]),
        )
    )
    return id_map


def _ensure_subdirs(
    series_dir: Path,
    seasons: list[int] | None = None,
    dry_run: bool = False,
) -> None:
    """Create season subdirectories if they don't already exist.

    The Specials directory is not created here -- it is created on demand
    by ``copy_reflink`` when a file is actually destined for it.
    """
    if seasons is None:
        seasons = [1]
    for s in seasons:
        season_dir = series_dir / f"Season {s:02d}"
        if dry_run:
            print(f"  [dry-run] mkdir -p {season_dir}")
        else:
            season_dir.mkdir(exist_ok=True)


def _write_id_file(series_dir: Path, info: AnimeInfo, dry_run: bool = False) -> None:
    """Write anidb.id / tvdb.id into the series directory."""
    for filename, id_value in [("anidb.id", info.anidb_id), ("tvdb.id", info.tvdb_id)]:
        if id_value is None:
            continue
        id_file = series_dir / filename
        if dry_run:
            print(f"  [dry-run] write {id_file} <- {id_value}")
        else:
            try:
                with id_file.open("x", encoding="utf-8") as f:
                    f.write(str(id_value) + "\n")
            except FileExistsError:
                pass


def create_series_directory(
    base: Path,
    info: AnimeInfo,
    seasons: list[int] | None = None,
    dry_run: bool = False,
) -> Path:
    """Create the series directory structure and ID file."""
    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)
    series_dir = base / dirname
    create_series_directory_structure(series_dir, info, seasons, dry_run)
    return series_dir


def propose_series_directory(
    dest: Path,
    info: AnimeInfo,
    id_map: dict[tuple[str, int], Path] | None = None,
) -> tuple[Path, bool, bool]:
    """Find the best series directory path without creating anything.

    Returns ``(path, already_exists, found_via_id)`` using a 2-step lookup:
    1. Check *id_map* for a matching AniDB/TheTVDB ID
    2. Check if the conventionally-named directory already exists
    Falls back to the conventional path with ``already_exists=False``.
    """
    if id_map:
        if info.anidb_id is not None:
            match = id_map.get((MetadataProvider.ANIDB, info.anidb_id))
            if match is not None:
                return match, True, True
        if info.tvdb_id is not None:
            match = id_map.get((MetadataProvider.TVDB, info.tvdb_id))
            if match is not None:
                return match, True, True

    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)
    conventional = dest / dirname
    if conventional.is_dir():
        return conventional, True, False

    return conventional, False, False


def create_series_directory_structure(
    series_dir: Path,
    info: AnimeInfo,
    seasons: list[int] | None = None,
    dry_run: bool = False,
) -> None:
    """Create the directory tree and ID file for a series."""
    if dry_run:
        print(f"  [dry-run] mkdir -p {series_dir}")
    else:
        series_dir.mkdir(parents=True, exist_ok=True)
    _ensure_subdirs(series_dir, seasons, dry_run)
    _write_id_file(series_dir, info, dry_run)


def resolve_series_directory(
    dest: Path,
    info: AnimeInfo,
    id_map: dict[tuple[str, int], Path] | None = None,
    seasons: list[int] | None = None,
    dry_run: bool = False,
) -> Path:
    """Find or create the series directory using a 3-step lookup.

    1. Check *id_map* for a matching AniDB/TheTVDB ID -> use that directory
    2. Check if the conventionally-named directory already exists
    3. Prompt the user to pick an existing directory or create a new one
    """
    proposed, exists, found_via_id = propose_series_directory(dest, info, id_map)

    if exists:
        print(f"  Found existing directory: {proposed.name}")
        _ensure_subdirs(proposed, seasons, dry_run)
        if not found_via_id:
            _write_id_file(proposed, info, dry_run)
        return proposed

    # Step 3: prompt user -- can enter an existing directory name or a new name
    dirname = proposed.name
    print(f"\n  No existing directory found for: {dirname}")
    raw = prompt_value(
        "  Directory name, path to existing, or Enter to create default", ""
    )
    if raw:
        manual_dir = Path(raw)
        if not manual_dir.is_absolute():
            manual_dir = dest / manual_dir
        if manual_dir.is_dir():
            print(f"  Using existing: {manual_dir.name}")
        else:
            print(f"  Creating: {manual_dir.name}")
            if not dry_run:
                manual_dir.mkdir(parents=True, exist_ok=True)
        _ensure_subdirs(manual_dir, seasons, dry_run)
        _write_id_file(manual_dir, info, dry_run)
        return manual_dir

    create_series_directory_structure(proposed, info, seasons, dry_run)
    return proposed


# ---------------------------------------------------------------------------
# Triage tracking manifest
# ---------------------------------------------------------------------------


def _triage_manifest_path() -> Path:
    """Path to the triage manifest tracking copied files."""
    return _cache_dir("triage") / "copied.json"


def _load_triage_manifest() -> set[str]:
    """Load the set of previously copied file paths."""
    path = _triage_manifest_path()
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError, TypeError:
            return set()
    return set()


def _save_triage_manifest(copied: set[str]) -> None:
    """Persist the set of copied file paths."""
    path = _triage_manifest_path()
    path.write_text(json.dumps(sorted(copied), ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# Source file discovery
# ---------------------------------------------------------------------------


def _iter_media_files(
    source_dirs: list[Path], include_audio: bool = False
) -> list[Path]:
    """Walk source directories recursively for media files.

    By default only video files are returned.  Set *include_audio* to
    also collect audio files (for the QA tool or extras detection).
    """
    extensions = _MEDIA_EXTENSIONS if include_audio else _VIDEO_EXTENSIONS
    results: list[Path] = []
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        for root, _dirs, files in os.walk(source_dir):
            for name in files:
                if Path(name).suffix.lower() in extensions:
                    results.append(Path(root) / name)
    return results


def _extract_series_name(text: str) -> str:
    """Extract a series name from a filename or directory name.

    Delegates to media_parser for tokenization-based extraction.
    """
    return media_parser.parse_component(text).series_name


def _extract_concise_name(source_files: list[SourceFile]) -> str:
    """Try to extract a concise series name from source filenames."""
    if not source_files:
        return ""
    pm = media_parser.parse_component(source_files[0].path.name)
    return pm.series_name


@dataclass
class DestScan:
    """Cached scan of all media files in a series destination directory.

    Built once per batch by :func:`scan_dest_directory` and threaded through
    concise-name resolution, existing-file display, and rename detection.
    """

    files_by_subdir: dict[Path, list[Path]]
    """``{subdir_path: [media_file_paths]}`` for every subdirectory."""

    names_by_subdir: dict[Path, dict[str, list[Path]]]
    """``{subdir_path: {concise_name: [file_paths]}}``."""


def scan_dest_directory(series_dir: Path) -> DestScan:
    """Scan all subdirectories of *series_dir* for media files.

    Returns a :class:`DestScan` with both the raw file listing (for display)
    and the concise-name grouping (for name resolution and rename detection).
    """
    files_by_subdir: dict[Path, list[Path]] = {}
    names_by_subdir: dict[Path, dict[str, list[Path]]] = {}

    if not series_dir.is_dir():
        return DestScan(files_by_subdir, names_by_subdir)

    for subdir in sorted(series_dir.iterdir()):
        if not subdir.is_dir():
            continue
        media_files: list[Path] = []
        names: dict[str, list[Path]] = {}
        for f in sorted(subdir.iterdir()):
            if not f.is_file() or f.suffix.lower() not in _MEDIA_EXTENSIONS:
                continue
            media_files.append(f)
            stem = f.stem
            sep_idx = stem.find(" - ")
            if sep_idx > 0:
                name = stem[:sep_idx].strip()
            else:
                pm = media_parser.parse_component(f.name)
                name = pm.series_name.strip()
            if name:
                names.setdefault(name, []).append(f)
        if media_files:
            files_by_subdir[subdir] = media_files
        if names:
            names_by_subdir[subdir] = names

    return DestScan(files_by_subdir, names_by_subdir)


def _most_common_concise_name(
    names_by_subdir: dict[Path, dict[str, list[Path]]],
) -> str:
    """Return the most common concise name across all subdirectories."""
    counts: Counter[str] = Counter()
    for names_in_dir in names_by_subdir.values():
        for name, files in names_in_dir.items():
            counts[name] += len(files)
    return counts.most_common(1)[0][0] if counts else ""


def _resolve_concise_name_from_existing(
    existing_names: dict[Path, dict[str, list[Path]]],
    series_dir: Path,
) -> tuple[str, list[tuple[Path, Path]]]:
    """Determine the default concise name from existing dest files.

    If all existing files use the same name, returns it silently.  If
    multiple names are found per season dir, warns the user and offers to
    normalize by renaming existing files to use the most common name.

    Returns ``(default_name, renames)`` where *renames* is a list of
    ``(old_path, new_path)`` for files that need renaming.
    """
    most_common = _most_common_concise_name(existing_names)
    if not most_common:
        return "", []
    renames: list[tuple[Path, Path]] = []

    for season_dir, names_in_dir in existing_names.items():
        if len(names_in_dir) <= 1:
            continue

        # Multiple names in this season dir — warn and ask
        rel_dir = season_dir.relative_to(series_dir)
        print(f"\n  Warning: multiple naming conventions in {rel_dir}/:")
        for name, files in sorted(names_in_dir.items(), key=lambda kv: -len(kv[1])):
            print(f"    {name!r} ({len(files)} file(s))")

        if not prompt_confirm(
            f"  Rename all files in {rel_dir}/ to use {most_common!r}?"
        ):
            continue

        # Collect renames for files that don't match the target name
        for name, files in names_in_dir.items():
            if name == most_common:
                continue
            for old_path in files:
                new_name = old_path.name.replace(name, most_common, 1)
                if new_name != old_path.name:
                    renames.append((old_path, old_path.parent / new_name))

    return most_common, renames


def _build_download_index(downloads_dir: Path) -> DownloadIndex:
    """Build an index of download files for matching.

    Indexes files by normalized series name AND by (season, episode) for
    series-aware matching with a global fallback.  Uses media_parser for
    full-path parsing (extracts series name from directory structure).
    """
    index = DownloadIndex()
    # Cache per-directory keys to avoid re-parsing the same directory
    # name for every file it contains.
    dir_keys_cache: dict[tuple[str, str], set[str]] = {}

    for f in _iter_media_files([downloads_dir]):
        try:
            rel = str(f.relative_to(downloads_dir))
        except ValueError:
            rel = f.name
        pm = media_parser.parse_media_path(rel)
        if pm.episode is None:
            continue
        season = pm.season or 1
        ep = pm.episode
        try:
            size = f.stat().st_size
        except OSError:
            continue

        raw_name = pm.path_series_name or pm.series_name
        entry = (season, ep, f, size)

        # Compute index keys, cached by (raw_name, directory) to avoid
        # re-parsing the same series name + directory components.
        dir_part = "/".join(rel.split("/")[:-1])
        cache_key = (raw_name, dir_part)
        if cache_key not in dir_keys_cache:
            keys = media_parser.name_variants(raw_name)
            for part in rel.split("/")[:-1]:
                cleaned = media_parser.clean_series_title(part)
                if cleaned != part:
                    k = media_parser.normalize_for_matching(cleaned)
                    if k:
                        keys.add(k)
            dir_keys_cache[cache_key] = keys
        keys = dir_keys_cache[cache_key]

        for k in keys:
            index.by_series.setdefault(k, []).append(entry)

        index.file_count += 1

    return index


def _best_size_match(
    candidates: list[tuple[Path, int]], target_size: int
) -> Path | None:
    """Pick the file closest in size to *target_size* from *candidates*.

    Each candidate is a ``(path, cached_size)`` tuple.
    """
    best: Path | None = None
    best_diff = float("inf")
    for dl_path, dl_size in candidates:
        diff = abs(dl_size - target_size)
        if diff < best_diff:
            best_diff = diff
            best = dl_path
    return best


def _match_to_downloads(
    source_files: list[SourceFile],
    download_index: DownloadIndex,
    series_name: str = "",
    title_index: media_parser.TitleAliasIndex | None = None,
) -> list[SourceFile]:
    """Enrich source files with metadata from matching download files.

    Matches by series name first, using the title alias index to bridge
    English/Japanese title differences when available.  Picks the closest
    file size when multiple candidates exist.  The download file's parsed
    metadata (release group, hash, version, source type) replaces the
    source file's, but the path is kept.
    """
    # Collect download entries from all matching keys.
    dl_keys = set(download_index.by_series.keys())
    if title_index is not None:
        candidate_keys = title_index.matching_keys(series_name, index_keys=dl_keys)
    else:
        candidate_keys = media_parser.name_variants(series_name)

    series_entries: list[tuple[int, int, Path, int]] = []
    for key in candidate_keys:
        series_entries.extend(download_index.by_series.get(key, []))

    # Index series entries by (season, ep) for exact matching and
    # by file size for fallback matching when episode numbering
    # differs (DVD vs aired order renumbering).
    series_by_ep: dict[tuple[int, int], list[tuple[Path, int]]] = {}
    series_by_size: dict[int, list[tuple[Path, int, int]]] = {}
    for season, ep, path, size in series_entries:
        series_by_ep.setdefault((season, ep), []).append((path, size))
        series_by_size.setdefault(size, []).append((path, season, ep))

    enriched: list[SourceFile] = []
    for sf in source_files:
        if sf.matched_download is not None:
            enriched.append(sf)
            continue
        if sf.parsed.season is None or sf.parsed.episode is None:
            enriched.append(sf)
            continue

        try:
            src_size = sf.path.stat().st_size
        except OSError:
            enriched.append(sf)
            continue

        src_group = (
            sf.parsed.release_group.split()[0] if sf.parsed.release_group else ""
        )
        best: Path | None = None
        best_sf: SourceFile | None = None

        # Pass 1: exact (season, episode) match.
        key = (sf.parsed.season, sf.parsed.episode)
        candidates = series_by_ep.get(key, [])
        ep_best = _best_size_match(candidates, src_size) if candidates else None
        if ep_best is not None:
            dl_sf = parse_source_filename(ep_best.name)
            dl_group = (
                dl_sf.parsed.release_group.split()[0]
                if dl_sf.parsed.release_group
                else ""
            )
            if not src_group or not dl_group or src_group == dl_group:
                best = ep_best
                best_sf = dl_sf

        # Pass 2: exact-size + matching release group across all entries.
        if best is None and src_group and sf.parsed.season != 0:
            size_candidates = series_by_size.get(src_size, [])
            for dl_path, _dl_season, _dl_ep in size_candidates:
                dl_sf = parse_source_filename(dl_path.name)
                dl_group = (
                    dl_sf.parsed.release_group.split()[0]
                    if dl_sf.parsed.release_group
                    else ""
                )
                if dl_group == src_group:
                    best = dl_path
                    best_sf = dl_sf
                    break

        if best is None or best_sf is None:
            enriched.append(sf)
            continue

        # Enrich: use download's metadata but keep source path
        sf.matched_download = best
        if best_sf.parsed.release_group:
            sf.parsed.release_group = best_sf.parsed.release_group
        if best_sf.parsed.hash_code:
            sf.parsed.hash_code = best_sf.parsed.hash_code
        if best_sf.parsed.version is not None:
            sf.parsed.version = best_sf.parsed.version
        if best_sf.parsed.source_type:
            sf.parsed.source_type = best_sf.parsed.source_type
        if best_sf.parsed.is_dual_audio:
            sf.parsed.is_dual_audio = True
        if best_sf.parsed.is_uncensored:
            sf.parsed.is_uncensored = True
        if best_sf.parsed.streaming_service and not sf.parsed.streaming_service:
            sf.parsed.streaming_service = best_sf.parsed.streaming_service

        enriched.append(sf)

    return enriched


def _extract_group_name(f: Path, source_dirs: list[Path]) -> str:
    """Extract a series name for grouping, using the parent directory name
    when the file is in a subdirectory of a source dir.

    Files directly in a source dir use the filename. Files in a subdirectory
    use the subdirectory name (which is typically the batch/release name),
    stripped of release metadata.
    """
    # Check if the file is in a subdirectory of a source dir
    for src in source_dirs:
        try:
            rel = f.relative_to(src)
        except ValueError:
            continue
        if len(rel.parts) > 1:
            # In a subdirectory -- use the immediate subdirectory name
            return _extract_series_name(rel.parts[0])
    # Directly in source dir -- use the filename
    return _extract_series_name(f.name)


def _scan_and_group(source_dirs: list[Path]) -> dict[str, list[Path]]:
    """Scan source dirs for media files and group by detected series name.

    Files in subdirectories use the subdirectory name for grouping (batch
    releases typically share a directory). Files directly in a source
    directory use the filename.

    Uses series_name_alt to merge groups that refer to the same series
    under different language names (e.g. CJK vs Latin).

    Returns ``{display_name: [file_paths]}`` ordered by count descending.
    """
    key_to_paths: dict[str, list[Path]] = {}
    key_to_names: dict[str, list[str]] = {}
    alt_to_primary: dict[str, str] = {}

    for f in _iter_media_files(source_dirs):
        raw_name = _extract_group_name(f, source_dirs)
        pm = media_parser.parse_component(raw_name)
        key = media_parser.normalize_for_matching(pm.series_name or raw_name)

        if key in alt_to_primary:
            key = alt_to_primary[key]

        key_to_paths.setdefault(key, []).append(f)
        key_to_names.setdefault(key, []).append(raw_name)

        if pm.series_name_alt:
            alt_key = media_parser.normalize_for_matching(pm.series_name_alt)
            if alt_key and alt_key != key:
                alt_to_primary[alt_key] = key

    # Post-merge: coalesce groups created under alt keys before mappings existed
    for alt_key, primary_key in alt_to_primary.items():
        if alt_key in key_to_paths and alt_key != primary_key:
            key_to_paths.setdefault(primary_key, []).extend(key_to_paths.pop(alt_key))
            key_to_names.setdefault(primary_key, []).extend(
                key_to_names.pop(alt_key, [])
            )

    # Pick the most common display name per group
    result: dict[str, list[Path]] = {}
    for key, paths in sorted(key_to_paths.items()):
        names = key_to_names[key]
        display_name = Counter(names).most_common(1)[0][0] if names else key
        if not display_name:
            display_name = "[ungrouped]"
        result[display_name] = sorted(paths)

    # Sort by count descending
    return dict(sorted(result.items(), key=lambda kv: -len(kv[1])))


# ---------------------------------------------------------------------------
# CLI and main workflow
# ---------------------------------------------------------------------------


def _add_common_flags(p: argparse.ArgumentParser) -> None:
    """Add flags shared by all subcommands."""
    p.add_argument("--dest", type=Path, metavar="DIR", help="Destination directory")
    p.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without copying"
    )
    p.add_argument("--no-cache", action="store_true", help="Bypass API response cache")
    p.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    p.add_argument("--config", type=Path, metavar="FILE", help="Config file path")


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser with triage/series/episode subcommands."""
    p = argparse.ArgumentParser(
        prog="etp-anime",
        description="Interactive anime collection manager",
    )
    p.add_argument("--version", "-V", action="version", version=VERSION)

    sub = p.add_subparsers(dest="command")

    # triage: bulk import from downloads
    t = sub.add_parser(
        "triage",
        help="Bulk import from downloads directory",
        description="Scan downloads directory, group files by series, and "
        "process groups via editable KDL manifests.",
    )
    t.add_argument("pattern", nargs="?", help="Filter files by name pattern")
    t.add_argument(
        "--source",
        type=Path,
        action="append",
        metavar="DIR",
        help="Source directory (repeatable; overrides config)",
    )
    t.add_argument(
        "--force",
        action="store_true",
        help="Re-process previously triaged files and auto-replace conflicts",
    )
    _add_common_flags(t)

    # series: sync from Sonarr-managed directory
    s = sub.add_parser(
        "series",
        help="Sync from Sonarr-managed anime directory",
        description="Process series directories from a Sonarr-managed source, "
        "keeping the curated collection in sync via editable KDL manifests.",
    )
    s.add_argument("pattern", nargs="?", help="Filter series by name pattern")
    s.add_argument(
        "--source",
        type=Path,
        metavar="DIR",
        help="Source directory (overrides config)",
    )
    s.add_argument(
        "--force",
        action="store_true",
        help="Re-process previously synced files and auto-replace conflicts",
    )
    _add_common_flags(s)

    # ingest: unified import (replaces triage + series)
    i = sub.add_parser(
        "ingest",
        help="Import anime from Sonarr and/or downloads",
        description="Unified anime ingestion. Requires at least one of "
        "--sonarr or --downloads to specify which sources to process.",
    )
    i.add_argument("pattern", nargs="?", help="Filter by name pattern")
    i.add_argument(
        "--sonarr",
        action="store_true",
        help="Process Sonarr-managed anime directory",
    )
    i.add_argument(
        "--downloads",
        action="store_true",
        help="Triage files from downloads directory",
    )
    i.add_argument(
        "--source",
        type=Path,
        action="append",
        metavar="DIR",
        help="Override source directory (repeatable for --downloads)",
    )
    i.add_argument(
        "--force",
        action="store_true",
        help="Re-process previously ingested files",
    )
    _add_common_flags(i)

    # episode: single-file import
    e = sub.add_parser(
        "episode",
        help="Import a single episode or movie",
        description="Import a single file into the anime collection. "
        "Requires an AniDB or TheTVDB ID.",
    )
    e.add_argument("file", type=Path, help="File to import")
    id_group = e.add_mutually_exclusive_group(required=True)
    id_group.add_argument("--anidb", type=int, metavar="ID", help="AniDB anime ID")
    id_group.add_argument("--tvdb", type=int, metavar="ID", help="TheTVDB series ID")
    _add_common_flags(e)

    return p


def fetch_anime_info(
    anidb_id: int | None = None,
    tvdb_id: int | None = None,
    no_cache: bool = False,
) -> AnimeInfo:
    """Fetch anime info by AniDB or TheTVDB ID.

    Exactly one of *anidb_id* or *tvdb_id* must be provided.
    """
    if anidb_id is not None:
        client = os.environ.get("ANIDB_CLIENT", "")
        clientver_str = os.environ.get("ANIDB_CLIENTVER", "")
        if not client or not clientver_str:
            print(
                "error: ANIDB_CLIENT and ANIDB_CLIENTVER environment "
                "variables are required for AniDB lookup",
                file=sys.stderr,
            )
            sys.exit(1)
        clientver = int(clientver_str)
        print(f"Fetching AniDB anime {anidb_id}...")
        return fetch_anidb_anime(anidb_id, client, clientver, no_cache=no_cache)
    elif tvdb_id is not None:
        api_key = os.environ.get("TVDB_API_KEY", "")
        if not api_key:
            print(
                "error: TVDB_API_KEY environment variable is required for TheTVDB lookup",
                file=sys.stderr,
            )
            sys.exit(1)
        print(f"Fetching TheTVDB series {tvdb_id}...")
        return fetch_tvdb_series(tvdb_id, api_key, no_cache=no_cache)
    else:
        raise ValueError("Either anidb_id or tvdb_id must be provided")


def _confirm_anime_info(info: AnimeInfo) -> AnimeInfo:
    """Interactively confirm and edit anime metadata."""
    print("\nAnime metadata:")
    print(f"  Japanese title: {info.title_ja}")
    print(f"  English title:  {info.title_en}")
    print(f"  Year:           {info.year}")

    regular_eps = [e for e in info.episodes if e.ep_type == EpisodeType.REGULAR]
    special_eps = [e for e in info.episodes if e.ep_type != EpisodeType.REGULAR]
    print(f"  Episodes:       {len(regular_eps)} regular, {len(special_eps)} special")

    if not prompt_confirm("\nUse these values?"):
        info.title_ja = prompt_value("Japanese title", info.title_ja)
        info.title_en = prompt_value("English title", info.title_en)
        year_str = prompt_value("Year", str(info.year))
        info.year = int(year_str) if year_str.isdigit() else info.year

    return info


def _parse_files(files: list[Path]) -> list[SourceFile]:
    """Parse a list of file paths into SourceFile objects."""
    parsed: list[SourceFile] = []
    for f in files:
        sf = parse_source_filename(f.name)
        sf.path = f
        parsed.append(sf)
    return parsed


def _process_file(
    source: SourceFile,
    info: AnimeInfo,
    concise_name: str,
    series_dir: Path,
    dry_run: bool,
    verbose: bool,
    defaults: GroupDefaults | None = None,
) -> bool:
    """Process a single source file: analyze, name, copy."""
    print(f"\n--- {source.path.name} ---")

    # Prompt for release group if not parsed from filename
    if not source.parsed.release_group:
        default_group = defaults.release_group if defaults else ""
        source.parsed.release_group = prompt_value("Release group", default_group)

    # Update sticky defaults
    if defaults is not None and source.parsed.release_group:
        defaults.release_group = source.parsed.release_group

    # Analyze with mediainfo
    try:
        source.media = analyze_file(source.path)
        if verbose:
            m = source.media
            print(
                f"  Video: {m.video_codec} {m.resolution} "
                f"{m.bit_depth}bit {m.hdr_type or 'SDR'}"
            )
            print(f"  Encoding lib: {m.encoding_lib or '(none)'}")
            for t in m.audio_tracks:
                commentary = " (commentary)" if t.is_commentary else ""
                print(f"  Audio: {t.codec} [{t.language}] {t.title}{commentary}")
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        print(f"  warning: mediainfo failed: {e}")
        print("  Skipping this file.")
        return False

    # Determine episode type and number
    ep_number = source.parsed.episode
    season = source.parsed.season or 1
    is_special = False
    special_tag = ""
    episode_name = ""

    if ep_number is not None:
        episode_name = info.find_episode_title(ep_number, season)
    else:
        # Ask for episode number
        ep_str = prompt_value("Episode number (or special tag like S1, NCOP1)")
        # Check if it's a special tag
        if re.match(r"^[A-Z]", ep_str):
            is_special = True
            special_tag = ep_str
            # Try to find matching episode
            for ep in info.episodes:
                if ep.special_tag == ep_str:
                    episode_name = ep.title_en
                    break
        else:
            try:
                ep_number = int(ep_str)
            except ValueError:
                print("  Invalid episode number, skipping.")
                return False

    # Confirm episode assignment
    if ep_number is not None and not is_special:
        ep_display = f"s{season}e{ep_number:02d}"
        if episode_name:
            ep_display += f" - {episode_name}"
        confirmed = prompt_value("Episode", ep_display)
        if confirmed != ep_display:
            # Re-parse if edited
            m = re.match(r"s(\d+)e(\d+)(?:\s*-\s*(.*))?", confirmed, re.IGNORECASE)
            if m:
                season = int(m.group(1))
                ep_number = int(m.group(2))
                episode_name = (m.group(3) or "").strip()

    # Verify CRC32 hash before building filename -- on mismatch the hash
    # is stripped from the destination filename
    hash_result = verify_hash(source)
    if hash_result is not None:
        ok, actual = hash_result
        if ok:
            print(f"  CRC32 verified: {source.parsed.hash_code}")
        else:
            print(
                f"  WARNING: CRC32 mismatch! expected {source.parsed.hash_code}, got {actual}"
            )
            if not prompt_confirm("  Hash mismatch — copy anyway?", default=False):
                return False
            source.parsed.hash_code = ""

    # Build filename
    filename = format_episode_filename(
        concise_name=concise_name,
        season=season,
        episode=ep_number or 0,
        episode_name=episode_name,
        source=source,
        is_special=is_special,
        special_tag=special_tag,
    )

    dest_path = season_subdir(series_dir, season, is_special) / filename
    print(f"\n  -> {dest_path}")

    # Check for existing file at destination
    if not dry_run:
        action = handle_conflict(
            source,
            dest_path,
            parse_source_filename_fn=parse_source_filename,
            analyze_file_fn=analyze_file,
        )
        if action == ConflictAction.SKIP:
            return False
        if action == ConflictAction.KEEP:
            return True

    if not prompt_confirm("  Copy this file?"):
        return False

    return copy_reflink(source.path, dest_path, dry_run=dry_run)


# ---------------------------------------------------------------------------
# Batch triage (vidir-style manifest editing)
# ---------------------------------------------------------------------------


def _match_files_to_season(
    pool: list[SourceFile],
    info: AnimeInfo,
    *,
    more_ids_queued: bool = False,
) -> tuple[list[MatchedFile], list[SourceFile]]:
    """Match files from the pool against an AniDB season.

    Groups pool files by their parsed season number, then asks the user
    which detected season corresponds to this AniDB entry.  When a season
    has more files than the AniDB entry has regular episodes (e.g., a
    multi-cour season split across two AniDB IDs), only the first N files
    by episode number are matched.

    Returns ``(matched, remaining)`` where matched files are wrapped in
    MatchedFile (with renumbered episodes if needed) and remaining files
    keep their original SourceFile state intact.
    """
    regular_count = sum(1 for ep in info.episodes if ep.ep_type == EpisodeType.REGULAR)

    # Filter pool by sub-series title similarity against AniDB entry.
    # This prevents files from other sub-series in a batch from being
    # mixed in (e.g. 探偵オペラ vs ふたりは vs 探偵歌劇TD).
    # Try English first, then Japanese, then romaji — files may use any.
    known_titles: list[str] = []
    for t in (info.title_en, info.title_ja, info.title_romaji):
        norm = media_parser.normalize_for_matching(t)
        if norm and norm not in known_titles:
            known_titles.append(norm)
    title_matched: list[SourceFile] = []
    title_unmatched: list[SourceFile] = []
    if known_titles:
        for sf in pool:
            sf_title_norm = media_parser.normalize_for_matching(sf.parsed.series_name)
            if not sf_title_norm:
                # Can't determine series name — include by default
                title_matched.append(sf)
            elif sf_title_norm in known_titles or any(
                sf_title_norm.startswith(t) or t.startswith(sf_title_norm)
                for t in known_titles
            ):
                title_matched.append(sf)
            else:
                title_unmatched.append(sf)
    else:
        title_matched = list(pool)

    def _group_by_season(
        files: list[SourceFile],
    ) -> tuple[dict[int, list[SourceFile]], list[SourceFile]]:
        by: dict[int, list[SourceFile]] = {}
        unseasoned: list[SourceFile] = []
        for sf in files:
            if sf.parsed.season is not None:
                by.setdefault(sf.parsed.season, []).append(sf)
            elif sf.parsed.episode is not None:
                by.setdefault(1, []).append(sf)
            else:
                unseasoned.append(sf)
        return by, unseasoned

    by_season, no_season = _group_by_season(title_matched)

    if not by_season:
        if title_unmatched:
            print(
                f"\n  No files matched title '{info.title_en or info.title_ja}'."
                f" ({len(title_unmatched)} files excluded by title filter)"
            )
            if prompt_confirm("  Include all files anyway?", default=False):
                title_matched = list(pool)
                title_unmatched = []
                by_season, no_season = _group_by_season(title_matched)
        if not by_season and no_season:
            # All matched files are unseasoned (e.g. OVA specials with no
            # episode numbers) — treat them as season 1 so the user can
            # proceed
            by_season[1] = no_season
            no_season = []
        if not by_season:
            return [], pool

    # Show candidates
    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)
    print(f"\n  {dirname} ({regular_count} regular episodes)")
    if title_unmatched:
        print(f"  ({len(title_unmatched)} files from other sub-series excluded)")
    print("  Candidate season matches:")
    season_keys = sorted(by_season.keys())
    for s in season_keys:
        count = len(by_season[s])
        print(f"    Season {s}: {count} files")
    if no_season:
        print(f"    (unseasoned): {len(no_season)} files")

    # Auto-select if there's only one non-specials season candidate.
    # When more AniDB IDs are queued, prefer the lowest season — higher
    # seasons will be matched by subsequent IDs.
    non_zero_keys = [s for s in season_keys if s != 0]
    if more_ids_queued and len(non_zero_keys) == 1 and non_zero_keys[0] > 1:
        # Only higher-season files remain, but more IDs are queued.
        # Skip this ID so the next one can pick up the right season.
        print(f"  Only season {non_zero_keys[0]} files remain; deferring to next ID.")
        return [], pool
    if len(non_zero_keys) == 1:
        chosen = non_zero_keys[0]
        print(f"  Auto-selected season {chosen}.")
    elif len(season_keys) == 1:
        chosen = season_keys[0]
        print(f"  Auto-selected season {chosen}.")
    else:
        chosen_str = prompt_value("  Which season maps to this AniDB ID?", "")
        try:
            chosen = int(chosen_str)
        except ValueError:
            print("  Invalid season number.")
            return [], pool

    if chosen not in by_season:
        print(f"  No files found for season {chosen}.")
        return [], pool

    # When only specials/season 0 matched and AniDB has no specials,
    # confirm the user wants to proceed (they may be hand-editing unmapped
    # specials into the series).
    if chosen == 0:
        special_count = sum(
            1 for ep in info.episodes if ep.ep_type != EpisodeType.REGULAR
        )
        if special_count == 0:
            print("  AniDB has no specials for this entry.")
            if not prompt_confirm("  Proceed with hand-editing?", default=False):
                return [], pool

    # Separate regular episodes from special/bonus files.
    # Parser-detected specials (S01OVA, S03OP, NCOP) and files without
    # episode numbers are treated as bonus — they stay with the matched
    # set but don't count against the AniDB regular episode limit.
    # Season 0 files (specials) are always included as bonus regardless
    # of which season was chosen — AniDB specials belong to the entry.
    chosen_files = by_season[chosen]
    season0_files = by_season.get(0, []) if chosen != 0 else []

    episode_files = sorted(
        [
            sf
            for sf in chosen_files
            if sf.parsed.episode is not None
            and not sf.parsed.is_special
            and not sf.parsed.bonus_type
        ],
        key=lambda sf: sf.parsed.episode or 0,
    )
    bonus_files = [
        sf
        for sf in chosen_files
        if sf.parsed.episode is None or sf.parsed.is_special or sf.parsed.bonus_type
    ] + season0_files

    if len(episode_files) > regular_count > 0:
        matched_eps = episode_files[:regular_count]
        leftover = episode_files[regular_count:]
        print(
            f"  AniDB entry has {regular_count} episodes but season has"
            f" {len(episode_files)} episode files — taking first {regular_count},"
            f" {len(leftover)} remaining for next ID."
        )
    else:
        matched_eps = episode_files
        leftover = []

    consumed = matched_eps + bonus_files

    # Determine renumbering offset for multi-cour splits where
    # e.g. S01E13 needs to become ep 1 of the second AniDB entry.
    renumber_offset = 0
    if consumed:
        first_ep = consumed[0].parsed.episode or 1
        last_ep = consumed[-1].parsed.episode or first_ep
        needs_renumber = first_ep != 1 and (
            regular_count > 0 and last_ep > regular_count
        )
        if needs_renumber:
            renumber_offset = first_ep - 1
            print(f"  Renumbering: ep {first_ep}+ → ep 1+")

    # Wrap matched files in MatchedFile (non-mutating)
    matched: list[MatchedFile] = []
    for sf in consumed:
        ep = sf.parsed.episode
        if ep is not None and renumber_offset:
            ep = ep - renumber_offset
        matched.append(MatchedFile(source=sf, episode=ep, season=sf.parsed.season))

    # Include unseasoned files as (todo) entries
    for sf in no_season:
        matched.append(MatchedFile(source=sf))

    # Build remaining pool (everything not matched, leftover already included)
    consumed_with_extras = consumed + no_season
    matched_set = set(id(sf) for sf in consumed_with_extras)
    remaining = [sf for sf in pool if id(sf) not in matched_set]

    return matched, remaining


_RE_TRAILING_YEAR = re.compile(r"\s*\(\d{4}\)\s*$")


def _strip_year(name: str) -> str:
    """Strip a trailing ``(YYYY)`` from a series name."""
    return _RE_TRAILING_YEAR.sub("", name)


def _auto_resolve_concise_name(
    matched: list[MatchedFile],
    dest_scan: DestScan,
    config: AnimeConfig | None,
    group_key: str,
) -> str:
    """Determine the concise name without prompting or triggering renames.

    Priority: existing files in dest (most common name) > config > parsed.
    Existing files take priority because the user has already established
    a naming convention in the destination directory.
    """
    default = group_key
    saved_concise = (
        config.concise_names.get(group_key, "")
        if config is not None and group_key
        else ""
    )
    if saved_concise:
        default = saved_concise
    elif not default:
        sources = [mf.source for mf in matched]
        default = _extract_concise_name(sources)

    # Existing files in dest override — they reflect the established naming
    if dest_scan.names_by_subdir:
        from_existing = _most_common_concise_name(dest_scan.names_by_subdir)
        if from_existing:
            default = from_existing

    return _strip_year(default)


def _auto_detect_release_group(matched: list[MatchedFile]) -> str:
    """Return the first non-empty release group from matched files."""
    return next(
        (
            mf.source.parsed.release_group
            for mf in matched
            if mf.source.parsed.release_group
        ),
        "",
    )


def _apply_batch_traits(
    matched: list[MatchedFile], release_group: str, detected_group: str
) -> None:
    """Apply release group overrides and majority-vote batch traits."""
    for mf in matched:
        existing = mf.source.parsed.release_group
        if not existing or existing == detected_group:
            mf.release_group = release_group

    dual_count = sum(1 for mf in matched if mf.source.parsed.is_dual_audio)
    if dual_count > len(matched) // 2:
        for mf in matched:
            mf.is_dual_audio = True

    uncensored_count = sum(1 for mf in matched if mf.source.parsed.is_uncensored)
    if uncensored_count > len(matched) // 2:
        for mf in matched:
            mf.is_uncensored = True


def _print_existing_dest_files(series_dir: Path, dest_scan: DestScan) -> None:
    """Print colorized existing media files from a pre-scanned directory."""
    if not dest_scan.files_by_subdir:
        return
    print(f"\n  Existing files in {series_dir.name}:")
    for subdir, files in dest_scan.files_by_subdir.items():
        for f in files:
            print(f"    {subdir.name}/{colorize_path(f.name)}")


def _prompt_batch_confirmation(
    proposed_dir: Path,
    dir_exists: bool,
    concise_name: str,
    release_group: str,
) -> str:
    """Show batch settings and get confirmation.

    Returns ``"y"`` to proceed, ``"e"`` to edit settings, ``"s"`` to skip.
    """
    status = "exists" if dir_exists else "new"
    print(f"\n  Series dir:     {proposed_dir.name}  [{status}]")
    print(f"  Concise name:   {concise_name}")
    print(f"  Release group:  {release_group}")
    while True:
        raw = input("\n  [Y]es / [e]dit / [s]kip: ").strip().lower()
        if raw in ("", "y", "yes"):
            return "y"
        if raw in ("e", "edit"):
            return "e"
        if raw in ("s", "skip"):
            return "s"


def _process_group_batch(
    files: list[Path],
    info: AnimeInfo,
    id_map: dict[tuple[str, int], Path],
    dest: Path,
    dry_run: bool,
    verbose: bool,
    default_concise_name: str = "",
    pre_matched: list[MatchedFile] | None = None,
    pre_parsed: list[SourceFile] | None = None,
    season_override: int | None = None,
    extras: list[Path] | None = None,
    config: AnimeConfig | None = None,
) -> BatchResult:
    """Batch-process a group of files via an editable manifest.

    Uses a two-phase workflow: build a lightweight preview (episode matching
    only, no mediainfo/CRC), show it for confirmation, then enrich with
    mediainfo/CRC and execute.

    If *pre_matched* is provided (from _match_files_to_season), uses those
    MatchedFile objects directly. If *pre_parsed* is provided, wraps them
    in MatchedFile. Otherwise parses *files* from scratch.

    If *season_override* is set, all files get that season value
    (e.g., ``season_override=1`` forces ``s1eYY`` for AniDB).
    """
    # Build MatchedFile list from whichever input was provided
    if pre_matched is not None:
        matched = pre_matched
    elif pre_parsed is not None:
        matched = [MatchedFile(source=sf) for sf in pre_parsed]
    else:
        matched = [MatchedFile(source=sf) for sf in _parse_files(files)]

    if season_override is not None:
        for mf in matched:
            mf.season = season_override

    seasons_needed = {mf.effective_season or 1 for mf in matched}
    seasons_list = sorted(seasons_needed)
    group_key = default_concise_name

    # Phase 1: Gather settings (no side effects)
    proposed_dir, dir_exists, _found_via_id = propose_series_directory(
        dest, info, id_map
    )
    dest_scan = scan_dest_directory(proposed_dir)
    concise_name = _auto_resolve_concise_name(matched, dest_scan, config, group_key)
    detected_group = _auto_detect_release_group(matched)
    release_group = detected_group
    _apply_batch_traits(matched, release_group, detected_group)
    snapshot_parsed = [mf.to_source_snapshot() for mf in matched]

    # Phase 2: Lightweight build (no display yet — wait for enrichment)
    workflow = ManifestWorkflow(
        snapshot_parsed,
        info,
        concise_name,
        proposed_dir,
        verbose=verbose,
        analyze_file_fn=analyze_file,
    )
    workflow.build_lightweight()

    # Phase 3: Confirmation gate for new directories
    while not dir_exists:
        choice = _prompt_batch_confirmation(
            proposed_dir, dir_exists, concise_name, release_group
        )
        if choice == "s":
            return BatchResult(skipped=len(matched))
        if choice == "e":
            concise_name = prompt_value(
                "Concise series name for filenames", concise_name
            )
            release_group = prompt_value("Release group", release_group)
            raw_dir = prompt_value("Series directory name", proposed_dir.name)
            if raw_dir != proposed_dir.name:
                new_dir = Path(raw_dir)
                if not new_dir.is_absolute():
                    new_dir = dest / new_dir
                proposed_dir = new_dir
                dir_exists = proposed_dir.is_dir()

            # Rebuild with new settings
            _apply_batch_traits(matched, release_group, detected_group)
            snapshot_parsed = [mf.to_source_snapshot() for mf in matched]
            workflow = ManifestWorkflow(
                snapshot_parsed,
                info,
                concise_name,
                proposed_dir,
                verbose=verbose,
                analyze_file_fn=analyze_file,
            )
            workflow.build_lightweight()
            continue
        break  # "y"

    # Phase 4: Commit side effects — create directory structure
    create_series_directory_structure(proposed_dir, info, seasons_list, dry_run)
    series_dir = proposed_dir

    if info.anidb_id is not None:
        id_map[(MetadataProvider.ANIDB, info.anidb_id)] = series_dir
    if info.tvdb_id is not None:
        id_map[(MetadataProvider.TVDB, info.tvdb_id)] = series_dir

    # Phase 5: Enrich with mediainfo + CRC32
    workflow.enrich(extras=extras, renames=[])
    file_count = len(workflow.parsed)

    # Phase 6: Show existing files + enriched manifest + edit prompt
    _print_existing_dest_files(series_dir, dest_scan)
    print()
    workflow.print_colorized_manifest()

    result = workflow.confirm_and_parse()
    if result is None:
        return BatchResult(skipped=file_count)
    parsed_entries, extra_entries, rename_entries = result

    # Files removed during manifest editing count as skipped
    edited_out = file_count - len(parsed_entries)

    # Check for naming inconsistencies — reuses the scan from phase 1
    if dest_scan.names_by_subdir:
        _resolved_name, extra_renames = _resolve_concise_name_from_existing(
            dest_scan.names_by_subdir, series_dir
        )
        rename_entries.extend(extra_renames)

    batch = workflow.execute(
        parsed_entries,
        extra_entries,
        rename_entries,
        dry_run,
        parse_source_filename_fn=parse_source_filename,
    )
    batch.skipped += edited_out
    return batch


# ---------------------------------------------------------------------------
# Subcommand entry points
#
# Three subcommands (see ADR 2026-03-26-01):
#   run_triage  -- bulk import from downloads directory via KDL manifests
#   run_series  -- sync from Sonarr-managed anime directory via KDL manifests
#   run_episode -- import a single file interactively
#
# All three share: filename construction (format_episode_filename),
# directory resolution (resolve_series_directory), conflict handling
# (handle_conflict), file copying (copy_reflink), and the anime-ingestion.kdl
# config for paths and per-series ID mappings.
# ---------------------------------------------------------------------------


def _process_pool(
    pool: list[SourceFile],
    group_name: str,
    id_queue: list[tuple[str, int]],
    id_map: dict[tuple[str, int], Path],
    dest: Path,
    config: AnimeConfig,
    dry_run: bool,
    verbose: bool,
    no_cache: bool,
    already_copied: set[str],
    extras: list[Path] | None = None,
    download_index: DownloadIndex | None = None,
    title_index: media_parser.TitleAliasIndex | None = None,
    resolved_paths: dict[Path, str] | None = None,
    auto_accept_ids: bool = False,
) -> tuple[BatchResult, bool]:
    """Process a file pool against metadata IDs interactively.

    Prompts the user for AniDB/TVDB IDs (using id_queue for pre-populated
    suggestions), fetches metadata, matches files to seasons, and delegates
    to ``_process_group_batch()`` for manifest editing and execution.

    Returns ``(BatchResult, quit_requested)``.
    """
    totals = BatchResult()

    def _resolve(p: Path) -> str:
        if resolved_paths:
            return resolved_paths.get(p, str(p.resolve()))
        return str(p.resolve())

    def _mark_pool_done() -> None:
        if not dry_run:
            for sf in pool:
                already_copied.add(_resolve(sf.path))
            _save_triage_manifest(already_copied)
        print(f"  Marked {len(pool)} file(s) as done.")
        pool.clear()

    while pool:
        anidb_id: int | None = None
        tvdb_id: int | None = None
        id_from_config = False

        # Use pre-populated ID queue. In series mode (auto_accept_ids),
        # accept without prompting. In triage mode, prompt for confirmation.
        if id_queue:
            provider, sid = id_queue[0]
            if auto_accept_ids:
                id_queue.pop(0)
                id_from_config = True
                print(f"\n  {len(pool)} file(s) remaining, using {provider} {sid}.")
                if provider == MetadataProvider.ANIDB:
                    anidb_id = sid
                else:
                    tvdb_id = sid
            else:
                print(f"\n  {len(pool)} file(s) remaining in pool.")
                raw = (
                    input(
                        f"\n  Use {provider} {sid} from config?"
                        f" [Y]es / [n]o / [s]kip / [d]one / [q]uit: "
                    )
                    .strip()
                    .lower()
                )
                if raw in ("", "y", "yes"):
                    id_queue.pop(0)
                    id_from_config = True
                    if provider == MetadataProvider.ANIDB:
                        anidb_id = sid
                    else:
                        tvdb_id = sid
                elif raw == "n":
                    id_queue.pop(0)
                elif raw == "s":
                    break
                elif raw == "d":
                    _mark_pool_done()
                    break
                elif raw == "q":
                    return totals, True

        # If no ID yet, prompt for one
        if anidb_id is None and tvdb_id is None:
            print(f"\n  {len(pool)} file(s) remaining in pool.")

            saved = lookup_series_ids(group_name, config)
            hint = ""
            if saved:
                hint = " [config: " + ", ".join(f"{p} {i}" for p, i in saved) + "]"

            raw = input(
                f"\nAniDB ID, TheTVDB ID (prefix with 't'),"
                f" 's' to skip, 'd' to mark done, 'q' to quit{hint}: "
            ).strip()
            if not raw or raw.lower() == "s":
                break
            if raw.lower() == "d":
                _mark_pool_done()
                break
            if raw.lower() == "q":
                return totals, True
            anidb_id, tvdb_id = _parse_id_input(raw)
            if anidb_id is None and tvdb_id is None:
                print(f"  Invalid ID '{raw}', try again.")
                continue
            provider = (
                MetadataProvider.ANIDB
                if anidb_id is not None
                else MetadataProvider.TVDB
            )
            pid = anidb_id if anidb_id is not None else tvdb_id
            assert pid is not None

        # Fetch metadata
        try:
            info = fetch_anime_info(
                anidb_id=anidb_id, tvdb_id=tvdb_id, no_cache=no_cache
            )
        except Exception as e:
            print(f"  Error fetching metadata: {e}")
            continue

        # Update title alias index and re-match downloads
        if title_index is not None:
            new_titles = [t for t in (info.title_ja, info.title_en) if t]
            if new_titles:
                title_index.add_series(new_titles)
        if download_index is not None and download_index.by_series:
            unmatched = [sf for sf in pool if sf.matched_download is None]
            if unmatched:
                pool = _match_to_downloads(
                    pool,
                    download_index,
                    series_name=group_name,
                    title_index=title_index,
                )

        # Skip interactive confirmation when ID came from config (read-through
        # cache — the user already approved this ID mapping).
        if not id_from_config:
            info = _confirm_anime_info(info)

        if anidb_id is not None:
            matched, pool = _match_files_to_season(
                pool, info, more_ids_queued=len(id_queue) > 0
            )
            if not matched:
                print("  No files matched. Try another ID.")
                continue
            batch = _process_group_batch(
                [],
                info,
                id_map,
                dest,
                dry_run,
                verbose,
                default_concise_name=group_name,
                pre_matched=matched,
                season_override=1,
                extras=extras,
                config=config,
            )
            extras = None  # extras go with the first AniDB season only
        else:
            batch = _process_group_batch(
                [],
                info,
                id_map,
                dest,
                dry_run,
                verbose,
                default_concise_name=group_name,
                pre_parsed=pool,
                extras=extras,
                config=config,
            )
            pool = []

        print(
            f"\n  Done: {batch.success} copied,"
            f" {batch.skipped} skipped, {batch.failed} failed"
        )
        totals.success += batch.success
        totals.skipped += batch.skipped
        totals.failed += batch.failed

        # Save config mapping only after successful copy
        if batch.success > 0:
            pid = anidb_id if anidb_id is not None else tvdb_id
            if pid is not None:
                _maybe_save_mapping(
                    group_name,
                    MetadataProvider.ANIDB
                    if anidb_id is not None
                    else MetadataProvider.TVDB,
                    pid,
                    config,
                    dry_run,
                    concise_name=_strip_year(group_name),
                )

        if batch.triaged and not dry_run:
            for p in batch.triaged:
                already_copied.add(_resolve(p))
            _save_triage_manifest(already_copied)

    # Count remaining pool files as skipped
    totals.skipped += len(pool)

    return totals, False


def run_series(args: argparse.Namespace, config: AnimeConfig) -> int:
    """Sync series from a Sonarr-managed anime directory.

    Iterates series directories in the source, looks up AniDB/TVDB IDs
    from source directory ID files or config mappings, and processes files
    via the batch KDL manifest flow. Uses the same triage manifest for
    tracking processed files.

    With --force: bypasses the triage manifest and auto-replaces conflicts.
    """
    source_dir = args.source or config.anime_source_dir

    if not source_dir.is_dir():
        print(f"error: source directory not found: {source_dir}", file=sys.stderr)
        return 1

    # List series directories
    series_dirs = sorted(
        d for d in source_dir.iterdir() if d.is_dir() and not d.name.startswith(".")
    )

    # Filter by pattern
    pattern = args.pattern
    if pattern:
        pattern_lower = pattern.lower()
        series_dirs = [d for d in series_dirs if pattern_lower in d.name.lower()]

    if not series_dirs:
        print("No series directories found.")
        return 0

    # Build download index — use cache if available
    downloads_dir = config.downloads_dir
    if downloads_dir.is_dir():
        cached, mtimes, fresh = check_cache_freshness(
            [downloads_dir], no_cache=args.no_cache
        )
        if fresh and cached is not None and cached.download_index.file_count > 0:
            print(
                f"Using cached download index ({cached.download_index.file_count} files)."
            )
            download_index = cached.download_index
        else:
            print(f"Building download index from {downloads_dir}...")
            download_index = _build_download_index(downloads_dir)
            print(f"  Indexed {download_index.file_count} files.")
            save_cache(DownloadCache(download_index=download_index, dir_mtimes=mtimes))
    else:
        download_index = DownloadIndex()

    # Build title alias index from cached AniDB/TVDB metadata
    title_index = media_parser.build_title_index(str(_cache_dir("anidb").parent))

    # Feed concise names from config into the alias index so that
    # directory names and parser-extracted names are linked
    for dir_name, concise in config.concise_names.items():
        title_index.add_series([dir_name, concise])

    if title_index.title_count:
        print(
            f"  Title alias index: {title_index.series_count} series, "
            f"{title_index.title_count} titles."
        )

    # Load triage manifest for tracking
    already_copied = _load_triage_manifest()
    force = args.force
    id_map = _cached_scan_dest_ids(args.dest, no_cache=args.no_cache)

    total_success = 0
    total_skipped = 0
    total_failed = 0

    for series_path in series_dirs:
        print(f"\n{'=' * 60}")
        print(f"Series: {series_path.name}")
        print(f"{'=' * 60}")

        # Collect media files in this series directory
        media_files = _iter_media_files([series_path])
        if not media_files:
            print("  No media files found, skipping.")
            continue

        # Filter already-processed files
        if not force and already_copied:
            media_files = [
                f for f in media_files if str(f.resolve()) not in already_copied
            ]
            if not media_files:
                print("  All files previously processed, skipping.")
                continue

        print(f"  {len(media_files)} file(s) to process.")

        id_queue = build_id_queue(series_path.name, config, source_dir=series_path)

        if id_queue:
            print(f"  Known IDs: {', '.join(f'{p} {i}' for p, i in id_queue)}")

        # Parse and enrich files
        pool = _parse_files(media_files)

        # Feed parser-detected alt titles into the title alias index
        # to improve download matching for bilingual releases
        alt_titles = {
            sf.parsed.series_name_alt for sf in pool if sf.parsed.series_name_alt
        }
        for alt in alt_titles:
            title_index.add_series([series_path.name, alt])

        if download_index.by_series:
            pool = _match_to_downloads(
                pool,
                download_index,
                series_name=series_path.name,
                title_index=title_index,
            )

        pool_result, quit_all = _process_pool(
            pool,
            group_name=series_path.name,
            id_queue=id_queue,
            id_map=id_map,
            dest=args.dest,
            config=config,
            dry_run=args.dry_run,
            verbose=args.verbose,
            no_cache=args.no_cache,
            already_copied=already_copied,
            download_index=download_index,
            title_index=title_index,
            auto_accept_ids=True,
        )
        total_success += pool_result.success
        total_skipped += pool_result.skipped
        total_failed += pool_result.failed

        if quit_all:
            break

    print(
        f"\nSeries sync complete: {total_success} copied,"
        f" {total_skipped} skipped, {total_failed} failed"
    )
    return 0 if total_failed == 0 else 1


def run_episode(args: argparse.Namespace, config: AnimeConfig) -> int:
    """Import a single episode or movie file.

    Simplified interactive workflow for one-off imports. Reluctant to create
    new series directories -- requires explicit confirmation (default no).
    """
    if not args.file.is_file():
        print(f"error: {args.file} is not a file", file=sys.stderr)
        return 1

    info = fetch_anime_info(
        anidb_id=args.anidb,
        tvdb_id=args.tvdb,
        no_cache=args.no_cache,
    )
    info = _confirm_anime_info(info)

    id_map = scan_dest_ids(args.dest)

    sf = parse_source_filename(args.file.name)
    sf.path = args.file

    default_name = _extract_concise_name([sf])
    concise_name = prompt_value("Concise series name for filename", default_name)

    # Reluctant directory creation: check if directory exists first
    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)
    conventional = args.dest / dirname

    # Check ID map and conventional name
    series_dir: Path | None = None
    if id_map:
        if info.anidb_id is not None:
            series_dir = id_map.get((MetadataProvider.ANIDB, info.anidb_id))
        if series_dir is None and info.tvdb_id is not None:
            series_dir = id_map.get((MetadataProvider.TVDB, info.tvdb_id))
    if series_dir is None and conventional.is_dir():
        series_dir = conventional

    if series_dir is None:
        print(f"\n  No existing series directory found for: {dirname}")
        if not prompt_confirm("  Create new series directory?", default=False):
            print("  Aborted.")
            return 1
        series_dir = resolve_series_directory(
            args.dest, info, id_map=id_map, dry_run=args.dry_run
        )
    else:
        print(f"  Using: {series_dir.name}")

    print(f"\nSeries directory: {series_dir}")

    if _process_file(sf, info, concise_name, series_dir, args.dry_run, args.verbose):
        print("\nDone: 1 file copied")
        return 0
    else:
        print("\nDone: file skipped/failed")
        return 1


def run_triage(args: argparse.Namespace, config: AnimeConfig) -> int:
    """Scan source directories, group files by series, and process in batch.

    For each group, the user provides AniDB/TVDB IDs one at a time. Files
    are matched to the metadata, a KDL manifest is generated for editing
    in $EDITOR, and the edited manifest is executed.

    AniDB IDs are processed per-season (each ID gets its own directory with
    s1eYY numbering). TVDB IDs process all remaining files at once. The
    user can enter 'd' to mark remaining files as done without copying, or
    's' to skip.

    Previously copied files are tracked in a JSON manifest and filtered
    out on subsequent runs (use --force to re-process).
    """
    source_dirs = args.source or [config.downloads_dir]

    # Check cached grouping first — avoids re-walking 5K+ files
    cached, mtimes, fresh = check_cache_freshness(source_dirs, no_cache=args.no_cache)
    if fresh and cached is not None and cached.groups:
        print("Using cached download index.")
        groups = cached.groups
    else:
        groups = _scan_and_group(source_dirs)
        save_cache(DownloadCache(groups=groups, dir_mtimes=mtimes))

    # Filter by pattern if provided
    pattern = args.pattern
    if pattern:
        pattern_lower = pattern.lower()
        groups = {
            name: files
            for name, files in groups.items()
            if pattern_lower in name.lower()
        }

    if not groups:
        print("No media files found in source directories.")
        return 0

    total_files = sum(len(files) for files in groups.values())

    # Load manifest of previously copied files
    already_copied = _load_triage_manifest()
    manifest_size_at_start = len(already_copied)
    force = args.force

    # Resolve all paths once upfront for manifest lookups
    resolved_paths: dict[Path, str] = {}
    for files in groups.values():
        for f in files:
            resolved_paths[f] = str(f.resolve())

    # Filter out already-copied files unless --force
    if not force and already_copied:
        filtered_groups: dict[str, list[Path]] = {}
        skipped_files = 0
        for name, files in groups.items():
            remaining = [f for f in files if resolved_paths[f] not in already_copied]
            skipped_files += len(files) - len(remaining)
            if remaining:
                filtered_groups[name] = remaining
        skipped_groups = len(groups) - len(filtered_groups)
        if skipped_files:
            parts = [f"Skipping {skipped_files} previously copied file(s)"]
            if skipped_groups:
                parts.append(f"{skipped_groups} fully-processed group(s)")
            print(f"{', '.join(parts)} (use --force to re-process).")
        groups = filtered_groups

    if not groups:
        print("All files have been previously copied. Nothing to do.")
        return 0

    remaining_files = sum(len(files) for files in groups.values())
    # Scan destination for existing series (once, reused across groups)
    id_map = _cached_scan_dest_ids(args.dest, no_cache=args.no_cache)

    remaining_files = sum(len(files) for files in groups.values())
    print(
        f"\n{len(groups)} group(s), {remaining_files} file(s) to process"
        f" (of {total_files} total):"
    )
    group_list = list(groups.items())
    for i, (name, files) in enumerate(group_list, 1):
        count = len(files)
        print(f"  {i}) {name}  ({count} file{'s' if count != 1 else ''})")

    # Process each group
    total_success = 0
    total_skipped = 0
    total_failed = 0
    groups_processed = 0

    for name, files in group_list:
        print(f"\n{'=' * 60}")
        print(f"Group: {name}  ({len(files)} files)")
        print(f"{'=' * 60}")
        for f in files:
            print(f"  {colorize_path(f.name)}")

        # Parse all files upfront for the group
        pool = _parse_files(files)

        # Collect extras: walk Extras/ subdirectories recursively,
        # plus non-video files from the immediate group directory.
        group_dir = files[0].parent if files else None
        group_extras: list[Path] = []
        extras_video: list[Path] = []
        if group_dir and group_dir.is_dir():
            # Non-video files in the immediate directory
            group_extras = [
                f
                for f in group_dir.iterdir()
                if f.is_file() and f.suffix.lower() in _EXTRAS_EXTENSIONS
            ]
            # Recursively walk Extras/ subdirectories
            extras_dir = group_dir / "Extras"
            if extras_dir.is_dir():
                for root, _dirs, fnames in os.walk(extras_dir):
                    for name in fnames:
                        p = Path(root) / name
                        ext = p.suffix.lower()
                        if ext in _VIDEO_EXTENSIONS:
                            extras_video.append(p)
                        else:
                            group_extras.append(p)

        # Prompt for video files found in Extras/
        if extras_video:
            print(f"\n  Found {len(extras_video)} video file(s) in Extras/:")
            for v in extras_video[:5]:
                print(f"    {colorize_path(v.name)}")
            if len(extras_video) > 5:
                print(f"    ... and {len(extras_video) - 5} more")
            raw = (
                input(
                    "\n  [e]xtras (copy as-is)  [s]pecials (edit in manifest)  [S]kip: "
                )
                .strip()
                .lower()
            )
            if raw in ("e", "extras"):
                group_extras.extend(extras_video)
            elif raw in ("s", "specials"):
                # Add video files to the main pool as specials
                for v in extras_video:
                    sf = parse_source_filename(v.name)
                    sf.path = v
                    sf.parsed.is_special = True
                    pool.append(sf)
            # else: skip — don't copy them

        id_queue = build_id_queue(name, config)
        if id_queue:
            print(f"  Known IDs: {', '.join(f'{p} {i}' for p, i in id_queue)}")

        pool_result, quit_all = _process_pool(
            pool,
            group_name=name,
            id_queue=id_queue,
            id_map=id_map,
            dest=args.dest,
            config=config,
            dry_run=args.dry_run,
            verbose=args.verbose,
            no_cache=args.no_cache,
            already_copied=already_copied,
            extras=group_extras,
            resolved_paths=resolved_paths,
        )
        total_success += pool_result.success
        total_skipped += pool_result.skipped
        total_failed += pool_result.failed

        if quit_all:
            if not args.dry_run and len(already_copied) > manifest_size_at_start:
                _save_triage_manifest(already_copied)
            return 0

        groups_processed += 1

    # Write manifest once at end (skip for dry-run).
    # Save if anything was processed -- including files marked 'd' for done.
    if not args.dry_run and len(already_copied) > manifest_size_at_start:
        _save_triage_manifest(already_copied)

    print(f"\n{'=' * 60}")
    print(
        f"Triage complete: {groups_processed} groups processed, "
        f"{total_success} copied, {total_skipped} skipped, {total_failed} failed"
    )
    return 0 if total_failed == 0 else 1


def run_ingest(args: argparse.Namespace, config: AnimeConfig) -> int:
    """Unified anime ingestion from Sonarr and/or downloads.

    Requires at least one of ``--sonarr`` or ``--downloads``. When both
    are specified, Sonarr sync runs first, then downloads triage handles
    whatever Sonarr didn't cover.
    """
    if not args.sonarr and not args.downloads:
        print(
            "error: specify --sonarr, --downloads, or both.",
            file=sys.stderr,
        )
        return 1

    total_result = 0

    if args.sonarr:
        print("=" * 60)
        print("Sonarr sync")
        print("=" * 60)
        # run_series expects args.source as a single Path
        series_args = argparse.Namespace(**vars(args))
        series_args.source = args.source[0] if args.source else None
        result = run_series(series_args, config)
        if result > total_result:
            total_result = result

    if args.downloads:
        if args.sonarr:
            print(f"\n{'=' * 60}")
            print("Downloads triage")
            print("=" * 60)
        result = run_triage(args, config)
        if result > total_result:
            total_result = result

    return total_result


def main() -> int:
    """Entry point: parse args, load config, dispatch to subcommand."""
    _load_env_file()

    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0

    if not shutil.which("mediainfo"):
        print(
            "error: mediainfo not found on $PATH. Install it to analyze media files.",
            file=sys.stderr,
        )
        return 1

    config = load_anime_config(args.config)

    if not args.dest:
        args.dest = config.anime_dest_dir

    if args.command == "ingest":
        return run_ingest(args, config)
    elif args.command == "triage":
        print(
            "warning: 'triage' is deprecated, use 'ingest --downloads' instead.",
            file=sys.stderr,
        )
        return run_triage(args, config)
    elif args.command == "series":
        print(
            "warning: 'series' is deprecated, use 'ingest --sonarr' instead.",
            file=sys.stderr,
        )
        return run_series(args, config)
    elif args.command == "episode":
        return run_episode(args, config)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
