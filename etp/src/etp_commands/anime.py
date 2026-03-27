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
from etp_lib.manifest import (
    build_manifest_entries,
    escape_kdl,
    execute_manifest,
    open_editor,
    parse_manifest,
    write_manifest,
)
from etp_lib.mediainfo import analyze_file
from etp_lib.naming import (
    build_metadata_block,  # noqa: F401 (re-export for tests)
    format_episode_filename,
    format_series_dirname,
)
from etp_lib.paths import cache_dir as _cache_dir
from etp_lib.tvdb import fetch_tvdb_series
from etp_lib.types import (
    AnimeConfig,
    AnimeInfo,
    DEFAULT_ANIME_SOURCE_DIR,  # noqa: F401 (re-export for tests)
    DEFAULT_DEST_DIR,  # noqa: F401 (re-export for tests)
    DEFAULT_DOWNLOADS_DIR,  # noqa: F401 (re-export for tests)
    DownloadIndex,
    Episode,  # noqa: F401 (re-export for tests)
    GroupDefaults,
    MediaInfo,  # noqa: F401 (re-export for tests)
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
                entry = ("anidb", int(child.args[0]))
                if entry not in ids:
                    ids.append(entry)
            elif child.name == "tvdb" and child.args:
                entry = ("tvdb", int(child.args[0]))
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
_EXTRAS_EXTENSIONS = frozenset({".rar", ".zip", ".7z", ".flac", ".wav", ".ape", ".txt"})


def parse_source_filename(filename: str) -> SourceFile:
    """Parse an anime release filename into a SourceFile.

    Delegates to media_parser for tokenization and classification.
    """
    pm = media_parser.parse_component(filename)
    sf = SourceFile(path=Path(filename))
    sf.release_group = pm.release_group
    sf.source_type = pm.source_type
    sf.is_remux = pm.is_remux
    sf.hash_code = pm.hash_code
    sf.parsed_episode = pm.episode
    sf.parsed_season = pm.season
    sf.version = pm.version
    return sf


# ---------------------------------------------------------------------------
# Directory management
# ---------------------------------------------------------------------------


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
        for id_filename, provider in (("anidb.id", "anidb"), ("tvdb.id", "tvdb")):
            id_file = entry / id_filename
            if id_file.is_file():
                try:
                    raw = id_file.read_text(encoding="utf-8").strip()
                    if raw:
                        result[(provider, int(raw))] = entry
                except (ValueError, OSError):
                    pass
    return result


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

    if dry_run:
        print(f"  [dry-run] mkdir -p {series_dir}")
    else:
        series_dir.mkdir(parents=True, exist_ok=True)

    _ensure_subdirs(series_dir, seasons, dry_run)
    _write_id_file(series_dir, info, dry_run)
    return series_dir


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
    # Step 1: ID match
    if id_map:
        if info.anidb_id is not None:
            match = id_map.get(("anidb", info.anidb_id))
            if match is not None:
                print(f"  Found existing directory by AniDB ID: {match.name}")
                _ensure_subdirs(match, seasons, dry_run)
                return match
        if info.tvdb_id is not None:
            match = id_map.get(("tvdb", info.tvdb_id))
            if match is not None:
                print(f"  Found existing directory by TheTVDB ID: {match.name}")
                _ensure_subdirs(match, seasons, dry_run)
                return match

    # Step 2: conventional name match
    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)
    conventional = dest / dirname
    if conventional.is_dir():
        print(f"  Found existing directory: {conventional.name}")
        _ensure_subdirs(conventional, seasons, dry_run)
        _write_id_file(conventional, info, dry_run)
        return conventional

    # Step 3: prompt user -- can enter an existing directory name or a new name
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

    return create_series_directory(dest, info, seasons, dry_run)


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
        except (json.JSONDecodeError, TypeError):
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
    source_dirs: list[Path], *, recursive: bool = False
) -> list[Path]:
    """Walk source directories for media files.

    By default scans one level of subdirectories.  With *recursive=True*
    walks the full tree (used for the download index where batch releases
    may nest several directories deep).
    """
    results: list[Path] = []
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        if recursive:
            for root, _dirs, files in os.walk(source_dir):
                for name in files:
                    if Path(name).suffix.lower() in _MEDIA_EXTENSIONS:
                        results.append(Path(root) / name)
        else:
            for entry in source_dir.iterdir():
                if entry.is_file() and entry.suffix.lower() in _MEDIA_EXTENSIONS:
                    results.append(entry)
                elif entry.is_dir():
                    for f in entry.iterdir():
                        if f.is_file() and f.suffix.lower() in _MEDIA_EXTENSIONS:
                            results.append(f)
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

    for f in _iter_media_files([downloads_dir], recursive=True):
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
        if sf.parsed_season is None or sf.parsed_episode is None:
            enriched.append(sf)
            continue

        try:
            src_size = sf.path.stat().st_size
        except OSError:
            enriched.append(sf)
            continue

        src_group = sf.release_group.split()[0] if sf.release_group else ""
        best: Path | None = None
        best_sf: SourceFile | None = None

        # Pass 1: exact (season, episode) match.
        key = (sf.parsed_season, sf.parsed_episode)
        candidates = series_by_ep.get(key, [])
        ep_best = _best_size_match(candidates, src_size) if candidates else None
        if ep_best is not None:
            dl_sf = parse_source_filename(ep_best.name)
            dl_group = dl_sf.release_group.split()[0] if dl_sf.release_group else ""
            if not src_group or not dl_group or src_group == dl_group:
                best = ep_best
                best_sf = dl_sf

        # Pass 2: exact-size + matching release group across all entries.
        if best is None and src_group and sf.parsed_season != 0:
            size_candidates = series_by_size.get(src_size, [])
            for dl_path, _dl_season, _dl_ep in size_candidates:
                dl_sf = parse_source_filename(dl_path.name)
                dl_group = dl_sf.release_group.split()[0] if dl_sf.release_group else ""
                if dl_group == src_group:
                    best = dl_path
                    best_sf = dl_sf
                    break

        if best is None or best_sf is None:
            enriched.append(sf)
            continue

        # Enrich: use download's metadata but keep source path
        sf.matched_download = best
        if best_sf.release_group:
            sf.release_group = best_sf.release_group
        if best_sf.hash_code:
            sf.hash_code = best_sf.hash_code
        if best_sf.version is not None:
            sf.version = best_sf.version
        if best_sf.source_type:
            sf.source_type = best_sf.source_type

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

    Returns ``{display_name: [file_paths]}`` ordered by count descending.
    """
    key_to_paths: dict[str, list[Path]] = {}
    key_to_names: dict[str, list[str]] = {}

    for f in _iter_media_files(source_dirs):
        raw_name = _extract_group_name(f, source_dirs)
        key = media_parser.normalize_for_matching(raw_name)
        key_to_paths.setdefault(key, []).append(f)
        key_to_names.setdefault(key, []).append(raw_name)

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

    regular_eps = [e for e in info.episodes if e.ep_type == "regular"]
    special_eps = [e for e in info.episodes if e.ep_type != "regular"]
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
    if not source.release_group:
        default_group = defaults.release_group if defaults else ""
        source.release_group = prompt_value("Release group", default_group)

    # Update sticky defaults
    if defaults is not None and source.release_group:
        defaults.release_group = source.release_group

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
    ep_number = source.parsed_episode
    season = source.parsed_season or 1
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
            print(f"  CRC32 verified: {source.hash_code}")
        else:
            print(
                f"  WARNING: CRC32 mismatch! expected {source.hash_code}, got {actual}"
            )
            if not prompt_confirm("  Hash mismatch — copy anyway?", default=False):
                return False
            source.hash_code = ""

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

    # Determine destination
    if is_special:
        dest_dir = series_dir / "Specials"
    else:
        dest_dir = series_dir / f"Season {season:02d}"

    dest_path = dest_dir / filename
    print(f"\n  -> {dest_path}")

    # Check for existing file at destination
    if not dry_run:
        action = handle_conflict(
            source,
            dest_path,
            parse_source_filename_fn=parse_source_filename,
            analyze_file_fn=analyze_file,
        )
        if action == "skip":
            return False
        if action == "keep":
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
) -> tuple[list[SourceFile], list[SourceFile]]:
    """Match files from the pool against an AniDB season.

    Groups pool files by their parsed season number, then asks the user
    which detected season corresponds to this AniDB entry.  When a season
    has more files than the AniDB entry has regular episodes (e.g., a
    multi-cour season split across two AniDB IDs), only the first N files
    by episode number are matched.  Their episode numbers are renumbered
    to start at 1 so they map correctly to the AniDB episode list.

    Returns ``(matched, remaining)`` where matched files are removed from
    the pool.
    """
    regular_count = sum(1 for ep in info.episodes if ep.ep_type == "regular")

    # Filter pool by sub-series title similarity against AniDB entry.
    # This prevents files from other sub-series in a batch from being
    # mixed in (e.g. 探偵オペラ vs ふたりは vs 探偵歌劇TD).
    # Try English first, then Japanese, then romaji — files may use any.
    known_titles: list[str] = []
    for t in (info.title_en, info.title_ja):
        norm = media_parser.normalize_for_matching(t)
        if norm and norm not in known_titles:
            known_titles.append(norm)
    title_matched: list[SourceFile] = []
    title_unmatched: list[SourceFile] = []
    if known_titles:
        for sf in pool:
            sf_pm = media_parser.parse_component(sf.path.name)
            sf_title_norm = media_parser.normalize_for_matching(sf_pm.series_name)
            if not sf_title_norm:
                # Can't determine series name — include by default
                title_matched.append(sf)
            elif sf_title_norm in known_titles:
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
            if sf.parsed_season is not None:
                by.setdefault(sf.parsed_season, []).append(sf)
            elif sf.parsed_episode is not None:
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

    # Prompt for which season
    default = str(season_keys[0]) if len(season_keys) == 1 else ""
    chosen_str = prompt_value("  Which season maps to this AniDB ID?", default)
    try:
        chosen = int(chosen_str)
    except ValueError:
        print("  Invalid season number.")
        return [], pool

    if chosen not in by_season:
        print(f"  No files found for season {chosen}.")
        return [], pool

    # Separate regular episodes from bonus files (映像特典 etc.)
    # Bonus files always stay with the matched set; only regular episodes
    # are counted against the AniDB episode limit.
    episode_files = sorted(
        [sf for sf in by_season[chosen] if sf.parsed_episode is not None],
        key=lambda sf: sf.parsed_episode or 0,
    )
    bonus_files = [sf for sf in by_season[chosen] if sf.parsed_episode is None]

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

    matched = matched_eps + bonus_files

    # Renumber episodes to start at 1 only for multi-cour splits where
    # e.g. S01E13 needs to become ep 1 of the second AniDB entry.
    # Skip renumbering when the episode range already fits within the
    # AniDB entry (e.g. a single ep 12 of a 12-episode season).
    if matched:
        first_ep = matched[0].parsed_episode or 1
        last_ep = matched[-1].parsed_episode or first_ep
        needs_renumber = first_ep != 1 and (
            regular_count > 0 and last_ep > regular_count
        )
        if needs_renumber:
            print(f"  Renumbering: ep {first_ep}+ → ep 1+")
            for sf in matched:
                if sf.parsed_episode is not None:
                    sf.parsed_episode = sf.parsed_episode - first_ep + 1

    # Include unseasoned files as (todo) entries
    matched_with_extras = matched + no_season

    # Build remaining pool (everything not matched, leftover already included)
    matched_set = set(id(sf) for sf in matched_with_extras)
    remaining = [sf for sf in pool if id(sf) not in matched_set]

    return matched_with_extras, remaining


_RE_TRAILING_YEAR = re.compile(r"\s*\(\d{4}\)\s*$")


def _strip_year(name: str) -> str:
    """Strip a trailing ``(YYYY)`` from a series name."""
    return _RE_TRAILING_YEAR.sub("", name)


def _process_group_batch(
    files: list[Path],
    info: AnimeInfo,
    id_map: dict[tuple[str, int], Path],
    dest: Path,
    dry_run: bool,
    verbose: bool,
    default_concise_name: str = "",
    pre_parsed: list[SourceFile] | None = None,
    season_override: int | None = None,
    extras: list[Path] | None = None,
    config: AnimeConfig | None = None,
) -> tuple[int, int, list[Path]]:
    """Batch-process a group of files via an editable manifest.

    Same interface as ``_process_group`` but uses a vidir-style workflow:
    build all source->destination mappings, open in $EDITOR, then execute.

    If *pre_parsed* is provided, uses those SourceFiles instead of parsing
    *files*. If *season_override* is set, all episodes are renumbered as
    that season (e.g., ``season_override=1`` forces ``s1eYY``).
    """
    parsed = pre_parsed if pre_parsed is not None else _parse_files(files)

    # Apply season override (for AniDB per-season processing)
    if season_override is not None:
        for sf in parsed:
            sf.parsed_season = season_override

    # Resolve concise name default: saved config > extracted > directory name
    saved_concise = ""
    if config is not None and default_concise_name:
        saved_concise = config.concise_names.get(default_concise_name, "")
    if saved_concise:
        default_concise_name = saved_concise
    elif not default_concise_name:
        default_concise_name = _extract_concise_name(parsed)
    default_concise_name = _strip_year(default_concise_name)
    concise_name = prompt_value(
        "Concise series name for filenames", default_concise_name
    )

    seasons_needed = {sf.parsed_season or 1 for sf in parsed}
    seasons_list = sorted(seasons_needed)

    series_dir = resolve_series_directory(
        dest,
        info,
        id_map=id_map,
        seasons=seasons_list,
        dry_run=dry_run,
    )
    print(f"\nSeries directory: {series_dir}")

    if info.anidb_id is not None:
        id_map[("anidb", info.anidb_id)] = series_dir
    if info.tvdb_id is not None:
        id_map[("tvdb", info.tvdb_id)] = series_dir

    # Always prompt for release group so the user can override
    # auto-detected values (e.g. "アニメ BD" is a content description,
    # not a release group name)
    detected = next((sf.release_group for sf in parsed if sf.release_group), "")
    group = prompt_value("Release group", detected)
    if group != detected:
        for sf in parsed:
            sf.release_group = group
    elif not detected:
        pass  # no group detected, user left it empty
    else:
        # Fill in any files that were missing a group
        for sf in parsed:
            if not sf.release_group:
                sf.release_group = group

    # Build manifest entries (mediainfo + CRC32 verification)
    print()
    entries = build_manifest_entries(
        parsed,
        info,
        concise_name,
        series_dir,
        verbose,
        analyze_file_fn=analyze_file,
    )

    # Write manifest to temp file
    manifest_path = write_manifest(
        entries, info, concise_name, series_dir, extras=extras or []
    )

    # Build source lookup by full path for parsing back
    known_sources: dict[str, SourceFile] = {
        str(e.source.path): e.source for e in entries
    }

    file_count = len(parsed)

    # Edit -> parse -> re-edit loop
    try:
        while True:
            if not open_editor(manifest_path):
                print("  Editor failed. Skipping group.")
                return 0, file_count, []

            parsed_entries, errors, extra_entries = parse_manifest(
                manifest_path, known_sources, series_dir
            )

            if errors:
                print(f"\n  Manifest has {len(errors)} error(s):")
                for err in errors:
                    print(err)
                if prompt_confirm("\n  Re-open editor to fix?"):
                    continue
                else:
                    print("  Skipping group.")
                    return 0, file_count, []

            if not parsed_entries:
                print(
                    "  Manifest is empty (all lines deleted or commented). Skipping group."
                )
                return 0, file_count, []

            break

        # Execute the manifest
        print(f"\n  Copying {len(parsed_entries)} file(s)...")
        result = execute_manifest(
            parsed_entries,
            dry_run,
            verbose,
            parse_source_filename_fn=parse_source_filename,
            analyze_file_fn=analyze_file,
        )

        # Copy extras (non-video files) if any remained in the manifest
        if extra_entries:
            print(f"  Copying {len(extra_entries)} extra(s)...")
            for src, dst in extra_entries:
                dst.parent.mkdir(parents=True, exist_ok=True)
                copy_reflink(src, dst, dry_run=dry_run)

        return result

    finally:
        try:
            manifest_path.unlink()
        except OSError:
            pass


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

    # Build download index for enriching metadata from original filenames
    downloads_dir = config.downloads_dir
    if downloads_dir.is_dir():
        print(f"Building download index from {downloads_dir}...")
        download_index = _build_download_index(downloads_dir)
        print(f"  Indexed {download_index.file_count} files.")
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
    manifest_size_at_start = len(already_copied)
    force = args.force
    id_map = scan_dest_ids(args.dest)

    total_success = 0
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

        # Build a queue of IDs from source dir files + config mappings
        id_queue: list[tuple[str, int]] = []

        # Check for ID files in the source series directory
        anidb_file = series_path / "anidb.id"
        tvdb_file = series_path / "tvdb.id"
        if anidb_file.is_file():
            try:
                id_queue.append(("anidb", int(anidb_file.read_text().strip())))
            except ValueError:
                pass
        if tvdb_file.is_file():
            try:
                id_queue.append(("tvdb", int(tvdb_file.read_text().strip())))
            except ValueError:
                pass

        # Add config mappings (may have multiple AniDB IDs for multi-season)
        for entry in lookup_series_ids(series_path.name, config):
            if entry not in id_queue:
                id_queue.append(entry)

        if id_queue:
            print(f"  Known IDs: {', '.join(f'{p} {i}' for p, i in id_queue)}")

        # Parse and enrich files
        pool = _parse_files(media_files)
        if download_index.by_series:
            pool = _match_to_downloads(
                pool,
                download_index,
                series_name=series_path.name,
                title_index=title_index,
            )

        # Process IDs from queue, then prompt for more if files remain
        quit_all = False
        while pool:
            anidb_id: int | None = None
            tvdb_id: int | None = None

            if id_queue:
                provider, sid = id_queue[0]
                print(f"\n  {len(pool)} file(s) remaining in pool.")
                raw = (
                    input(
                        f"\n  Use {provider} {sid} from config?"
                        f" [Y]es / [n]o / [s]kip series / [d]one / [q]uit: "
                    )
                    .strip()
                    .lower()
                )
                if raw in ("", "y", "yes"):
                    id_queue.pop(0)
                    if provider == "anidb":
                        anidb_id = sid
                    else:
                        tvdb_id = sid
                elif raw == "n":
                    id_queue.pop(0)  # discard this ID, fall through to prompt
                elif raw == "s":
                    break
                elif raw == "d":
                    if not args.dry_run:
                        for sf in pool:
                            already_copied.add(str(sf.path.resolve()))
                    print(f"  Marked {len(pool)} file(s) as done.")
                    pool = []
                    break
                elif raw == "q":
                    quit_all = True
                    break

            if anidb_id is None and tvdb_id is None:
                print(f"\n  {len(pool)} file(s) remaining in pool.")
                raw = input(
                    "\n  AniDB ID, TheTVDB ID (prefix with 't'),"
                    " 's' to skip, 'd' to mark done, 'q' to quit: "
                ).strip()
                if not raw or raw.lower() == "s":
                    break
                if raw.lower() == "d":
                    if not args.dry_run:
                        for sf in pool:
                            already_copied.add(str(sf.path.resolve()))
                    print(f"  Marked {len(pool)} file(s) as done.")
                    pool = []
                    break
                if raw.lower() == "q":
                    quit_all = True
                    break
                anidb_id, tvdb_id = _parse_id_input(raw)
                if anidb_id is None and tvdb_id is None:
                    print(f"  Invalid ID '{raw}', try again.")
                    continue
                provider = "anidb" if anidb_id is not None else "tvdb"
                pid = anidb_id if anidb_id is not None else tvdb_id
                assert pid is not None  # guaranteed by the continue above
                _maybe_save_mapping(
                    series_path.name,
                    provider,
                    pid,
                    config,
                    args.dry_run,
                    concise_name=_extract_concise_name(pool),
                )

            try:
                info = fetch_anime_info(
                    anidb_id=anidb_id,
                    tvdb_id=tvdb_id,
                    no_cache=args.no_cache,
                )
            except Exception as e:
                print(f"  Error fetching metadata: {e}")
                continue

            # Update title alias index with newly fetched titles and
            # re-match any pool files that weren't matched to downloads
            new_titles = [t for t in (info.title_ja, info.title_en) if t]
            if new_titles:
                title_index.add_series(new_titles)
            unmatched = [sf for sf in pool if sf.matched_download is None]
            if unmatched and download_index.by_series:
                pool = _match_to_downloads(
                    pool,
                    download_index,
                    series_name=series_path.name,
                    title_index=title_index,
                )

            info = _confirm_anime_info(info)

            if anidb_id is not None:
                # AniDB: match files to this season
                matched, pool = _match_files_to_season(pool, info)
                if not matched:
                    print("  No files matched.")
                    continue
                success, failed, triaged = _process_group_batch(
                    [],
                    info,
                    id_map,
                    args.dest,
                    args.dry_run,
                    args.verbose,
                    default_concise_name=series_path.name,
                    pre_parsed=matched,
                    season_override=1,
                    config=config,
                )
            else:
                # TVDB: process all remaining files
                success, failed, triaged = _process_group_batch(
                    [],
                    info,
                    id_map,
                    args.dest,
                    args.dry_run,
                    args.verbose,
                    default_concise_name=series_path.name,
                    pre_parsed=pool,
                    config=config,
                )
                pool = []

            print(f"\n  Done: {success} copied, {failed} skipped/failed")
            total_success += success
            total_failed += failed
            if triaged and not args.dry_run:
                for p in triaged:
                    already_copied.add(str(p.resolve()))

        if quit_all:
            break

    if not args.dry_run and len(already_copied) > manifest_size_at_start:
        _save_triage_manifest(already_copied)

    print(
        f"\nSeries sync complete: {total_success} copied, {total_failed} skipped/failed"
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
            series_dir = id_map.get(("anidb", info.anidb_id))
        if series_dir is None and info.tvdb_id is not None:
            series_dir = id_map.get(("tvdb", info.tvdb_id))
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
    groups = _scan_and_group(source_dirs)

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
        skipped_total = 0
        for name, files in groups.items():
            remaining = [f for f in files if resolved_paths[f] not in already_copied]
            skipped = len(files) - len(remaining)
            skipped_total += skipped
            if remaining:
                filtered_groups[name] = remaining
        if skipped_total:
            print(
                f"Skipping {skipped_total} previously copied file(s) (use --force to re-process)."
            )
        groups = filtered_groups

    if not groups:
        print("All files have been previously copied. Nothing to do.")
        return 0

    # Scan destination for existing series (once, reused across groups)
    id_map = scan_dest_ids(args.dest)

    print(f"\nFound {len(groups)} group(s):")
    group_list = list(groups.items())
    for i, (name, files) in enumerate(group_list, 1):
        count = len(files)
        print(f"  {i}) {name}  ({count} file{'s' if count != 1 else ''})")

    # Process each group
    total_success = 0
    total_failed = 0
    groups_processed = 0

    for name, files in group_list:
        print(f"\n{'=' * 60}")
        print(f"Group: {name}  ({len(files)} files)")
        print(f"{'=' * 60}")
        for f in files:
            print(f"  {f.name}")

        # Parse all files upfront for the group
        pool = _parse_files(files)

        # Collect non-video extras from the same directory
        group_dir = files[0].parent if files else None
        group_extras: list[Path] = []
        if group_dir and group_dir.is_dir():
            group_extras = [
                f
                for f in group_dir.iterdir()
                if f.is_file() and f.suffix.lower() in _EXTRAS_EXTENSIONS
            ]

        # Loop: process one metadata ID at a time against the remaining pool
        while pool:
            remaining_count = len(pool)
            print(f"\n  {remaining_count} file(s) remaining in pool.")

            # Check config for saved series mappings
            saved = lookup_series_ids(name, config)
            if saved:
                hint = " [config: " + ", ".join(f"{p} {i}" for p, i in saved) + "]"
            else:
                hint = ""

            raw = input(
                f"\nAniDB ID, TheTVDB ID (prefix with 't'),"
                f" 's' to skip, 'd' to mark done, 'q' to quit{hint}: "
            ).strip()
            if not raw or raw.lower() == "s":
                print("  Skipping remaining files.")
                break
            if raw.lower() == "q":
                # Save manifest and exit triage
                if not args.dry_run and len(already_copied) > manifest_size_at_start:
                    _save_triage_manifest(already_copied)
                return 0
            if raw.lower() == "d":
                # Mark all remaining files as done without copying
                if not args.dry_run:
                    for sf in pool:
                        already_copied.add(
                            resolved_paths.get(sf.path, str(sf.path.resolve()))
                        )
                print(f"  Marked {len(pool)} file(s) as done.")
                pool = []
                break

            anidb_id, tvdb_id = _parse_id_input(raw)
            if anidb_id is None and tvdb_id is None:
                print(f"  Invalid ID '{raw}', try again.")
                continue

            try:
                info = fetch_anime_info(
                    anidb_id=anidb_id, tvdb_id=tvdb_id, no_cache=args.no_cache
                )
            except Exception as e:
                print(f"  Error fetching metadata: {e}")
                continue

            info = _confirm_anime_info(info)

            provider = "anidb" if anidb_id is not None else "tvdb"
            pid = anidb_id if anidb_id is not None else tvdb_id
            if pid is not None:
                _maybe_save_mapping(
                    name,
                    provider,
                    pid,
                    config,
                    args.dry_run,
                    concise_name=_extract_concise_name(pool),
                )

            if anidb_id is not None:
                # AniDB per-season: match files to this season, force s1eYY
                matched, pool = _match_files_to_season(pool, info)
                if not matched:
                    print("  No files matched. Try another ID.")
                    continue
                success, failed, copied_paths = _process_group_batch(
                    [],
                    info,
                    id_map,
                    args.dest,
                    args.dry_run,
                    args.verbose,
                    default_concise_name=name,
                    pre_parsed=matched,
                    season_override=1,
                    extras=group_extras,
                    config=config,
                )
            else:
                # TVDB: process all remaining files at once (multi-season)
                success, failed, copied_paths = _process_group_batch(
                    [],
                    info,
                    id_map,
                    args.dest,
                    args.dry_run,
                    args.verbose,
                    default_concise_name=name,
                    pre_parsed=pool,
                    extras=group_extras,  # TVDB gets all remaining extras
                    config=config,
                )
                group_extras = []
                pool = []  # TVDB consumes entire pool

            print(f"\n  Season done: {success} copied, {failed} skipped/failed")
            total_success += success
            total_failed += failed

            if copied_paths and not args.dry_run:
                for p in copied_paths:
                    already_copied.add(resolved_paths.get(p, str(p.resolve())))

        groups_processed += 1

    # Write manifest once at end (skip for dry-run).
    # Save if anything was processed -- including files marked 'd' for done.
    if not args.dry_run and len(already_copied) > manifest_size_at_start:
        _save_triage_manifest(already_copied)

    print(f"\n{'=' * 60}")
    print(
        f"Triage complete: {groups_processed} groups processed, "
        f"{total_success} files copied, {total_failed} skipped/failed"
    )
    return 0 if total_failed == 0 else 1


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

    if args.command == "triage":
        return run_triage(args, config)
    elif args.command == "series":
        return run_series(args, config)
    elif args.command == "episode":
        return run_episode(args, config)
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
