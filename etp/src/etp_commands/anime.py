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
import errno
import gzip
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
import zlib
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import kdl

from etp_lib import media_parser

VERSION = "0.1.0"

_IS_LINUX = platform.system() == "Linux"


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


_CACHE_MAX_AGE_SECONDS = 86400  # 24 hours
_TVDB_MAX_PAGES = 100  # safety limit for paginated fetches

# ---------------------------------------------------------------------------
# Default paths (NAS layout)
# ---------------------------------------------------------------------------

DEFAULT_DOWNLOADS_DIR = Path("/volume1/docker/pvr/data/downloads")
DEFAULT_ANIME_SOURCE_DIR = Path("/volume1/docker/pvr/data/anime")
DEFAULT_DEST_DIR = Path("/volume1/video/anime")

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class Episode:
    number: int
    ep_type: str  # "regular", "special", "credit", "trailer", "parody", "other"
    title_en: str
    title_ja: str
    special_tag: str  # "S1", "CM01", "NCOP1", "NCED3", "T1", etc.
    season: int = 1  # TVDB season number (AniDB is always 1)


@dataclass
class AnimeInfo:
    anidb_id: int | None
    tvdb_id: int | None
    title_ja: str
    title_en: str
    year: int
    episodes: list[Episode] = field(default_factory=list)


@dataclass
class AudioTrack:
    codec: str  # Normalized: "aac", "flac", "opus", "AC3", "DTS", etc.
    language: str  # ISO 639: "ja", "en", etc.
    title: str
    is_commentary: bool


@dataclass
class MediaInfo:
    video_codec: str  # "HEVC", "AVC", "AV1", "XviD"
    resolution: str  # "1080p", "720p", "2160p", etc.
    width: int
    height: int
    bit_depth: int
    hdr_type: str  # "", "HDR", "UHD", "DoVi"
    audio_tracks: list[AudioTrack] = field(default_factory=list)
    encoding_lib: str = ""  # "x264", "x265", or ""


@dataclass
class SourceFile:
    path: Path
    release_group: str = ""
    source_type: str = ""  # "BD", "Web"
    is_remux: bool = False
    hash_code: str = ""  # e.g. "ABCD1234"
    parsed_episode: int | None = None
    parsed_season: int | None = None
    version: int | None = None  # e.g. 2 for "v2" releases
    media: MediaInfo | None = None
    matched_download: Path | None = None  # download file that enriched this entry


@dataclass
class GroupDefaults:
    """Sticky defaults that carry across files within a group.

    When processing multiple files together (e.g. triage mode), values
    confirmed for one file become the defaults offered for the next.
    """

    release_group: str = ""
    source_type: str = ""


@dataclass
class ManifestEntry:
    """One line in the triage manifest: source file to destination path."""

    source: SourceFile
    dest_path: Path
    is_todo: bool = False
    hash_failed: bool = False


@dataclass
class AnimeConfig:
    """Configuration loaded from anime-ingestion.kdl."""

    downloads_dir: Path = field(default_factory=lambda: DEFAULT_DOWNLOADS_DIR)
    anime_source_dir: Path = field(default_factory=lambda: DEFAULT_ANIME_SOURCE_DIR)
    anime_dest_dir: Path = field(default_factory=lambda: DEFAULT_DEST_DIR)
    series_mappings: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    # series directory name → concise name from parser (for title matching)
    concise_names: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Configuration (anime-ingestion.kdl)
# ---------------------------------------------------------------------------


def _escape_kdl(s: str) -> str:
    """Escape a string for use inside a KDL quoted value."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


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
    """Append a series→ID mapping to the config file.

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
        concise_line = f'\n  concise "{_escape_kdl(concise_name)}"'

    line = (
        f'\nseries "{_escape_kdl(name)}" {{\n'
        f"  {provider} {provider_id}{concise_line}\n"
        f"}}\n"
    )
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def lookup_series_ids(name: str, config: AnimeConfig) -> list[tuple[str, int]]:
    """Look up series IDs from config mappings by name (case-insensitive).

    Returns a list of ``(provider, id)`` tuples — multiple entries for
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
    """Save a series→ID mapping to config if this specific ID is not already saved."""
    if dry_run:
        return
    entry = (provider, pid)
    existing = lookup_series_ids(name, config)
    if entry in existing:
        return
    save_series_mapping(name, provider, pid, concise_name=concise_name)
    config.series_mappings.setdefault(name, []).append(entry)


# ---------------------------------------------------------------------------
# Mediainfo parsing
# ---------------------------------------------------------------------------

# Map mediainfo Format values to our normalized codec names
_VIDEO_CODEC_MAP: dict[str, str] = {
    "HEVC": "HEVC",
    "AVC": "AVC",
    "AV1": "AV1",
    "MPEG-4 Visual": "XviD",  # Usually XviD for anime
    "VP9": "VP9",
    "MPEG Video": "MPEG2",
}

# Audio codec normalization: open-source lowercase, proprietary uppercase
_AUDIO_CODEC_MAP: dict[str, str] = {
    "AAC": "aac",
    "FLAC": "flac",
    "Opus": "opus",
    "Vorbis": "vorbis",
    "PCM": "pcm",
    "AC-3": "AC3",
    "E-AC-3": "AC3",
    "DTS": "DTS",
    "DTS-HD": "DTS",
    "DTS-HD MA": "DTS",
    "DTS-HD Master Audio": "DTS",
    "MLP FBA": "DTS",  # TrueHD/Atmos shows as MLP FBA in some cases
    "TrueHD": "AC3",  # Dolby TrueHD → treat as AC3 family
    "MP3": "mp3",
    "MPEG Audio": "mp3",
    "mp2": "mp2",
}


def _resolution_shorthand(width: int, height: int) -> str:
    """Convert width x height to shorthand like '1080p', '720p', '4K'."""
    if height >= 2160 or width >= 3840:
        return "4K"
    if height >= 1080 or width >= 1920:
        return "1080p"
    if height >= 720 or width >= 1280:
        return "720p"
    if height >= 540 or width >= 960:
        return "540p"
    if height >= 480 or width >= 720:
        return "480p"
    return f"{height}p"


def _detect_hdr(video_track: dict) -> str:
    """Detect HDR type from mediainfo video track."""
    hdr_format = video_track.get("HDR_Format", "")
    hdr_compat = video_track.get("HDR_Format_Compatibility", "")
    transfer = video_track.get("transfer_characteristics", "")

    if "Dolby Vision" in hdr_format:
        if "HDR10" in hdr_compat:
            return "DoVi,HDR"
        return "DoVi"

    if "HDR10+" in hdr_format or "HDR10" in hdr_format:
        return "HDR"

    if "SMPTE ST 2084" in transfer or "PQ" in transfer:
        return "HDR"

    if "HLG" in hdr_format or "HLG" in transfer:
        return "HDR"

    # For 4K content without explicit HDR metadata
    return ""


def _normalize_audio_codec(format_str: str) -> str:
    """Normalize mediainfo audio Format to our naming convention."""
    # Try exact match first
    if format_str in _AUDIO_CODEC_MAP:
        return _AUDIO_CODEC_MAP[format_str]

    # Try prefix matching for variants like "DTS XLL" etc.
    for prefix in ("DTS", "AC-3", "E-AC-3", "AAC", "MLP"):
        if format_str.startswith(prefix):
            return _AUDIO_CODEC_MAP.get(prefix, format_str)

    return format_str.lower()


def _detect_encoding_lib(video_track: dict) -> str:
    """Detect x264/x265 from encoding library metadata."""
    lib_name = video_track.get("Encoded_Library_Name", "")
    lib_full = video_track.get("Encoded_Library", "")
    writing_lib = video_track.get("Writing_library", "")

    for field_val in (lib_name, lib_full, writing_lib):
        val = field_val.lower()
        if "x264" in val or "libx264" in val:
            return "x264"
        if "x265" in val or "libx265" in val:
            return "x265"

    return ""


def parse_mediainfo_json(data: dict) -> MediaInfo:
    """Parse mediainfo JSON output into a MediaInfo dataclass."""
    tracks = data.get("media", {}).get("track", [])

    video_codec = ""
    resolution = ""
    width = 0
    height = 0
    bit_depth = 8
    hdr_type = ""
    encoding_lib = ""
    audio_tracks: list[AudioTrack] = []

    for track in tracks:
        track_type = track.get("@type", "")

        if track_type == "Video":
            raw_format: str = track.get("Format", "")
            video_codec = _VIDEO_CODEC_MAP.get(raw_format, raw_format)
            width = int(track.get("Width", 0))
            height = int(track.get("Height", 0))
            bit_depth = int(track.get("BitDepth", 8))
            resolution = _resolution_shorthand(width, height)
            hdr_type = _detect_hdr(track)
            encoding_lib = _detect_encoding_lib(track)

        elif track_type == "Audio":
            raw_format = track.get("Format", "")
            codec = _normalize_audio_codec(raw_format)
            language = track.get("Language", "")
            title = track.get("Title", "")
            is_commentary = bool(_RE_COMMENTARY.search(title))
            audio_tracks.append(
                AudioTrack(
                    codec=codec,
                    language=language,
                    title=title,
                    is_commentary=is_commentary,
                )
            )

    return MediaInfo(
        video_codec=video_codec,
        resolution=resolution,
        width=width,
        height=height,
        bit_depth=bit_depth,
        hdr_type=hdr_type,
        audio_tracks=audio_tracks,
        encoding_lib=encoding_lib,
    )


def analyze_file(path: Path) -> MediaInfo:
    """Run mediainfo on a file and return parsed MediaInfo."""
    result = subprocess.run(
        ["mediainfo", "--Output=JSON", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    return parse_mediainfo_json(data)


# ---------------------------------------------------------------------------
# Source filename parsing
# ---------------------------------------------------------------------------

# Match: "commentary" as a whole word in audio track titles → no groups
_RE_COMMENTARY = re.compile(r"\bcommentary\b", re.IGNORECASE)

# Media file extensions (canonical set in media_parser)
_MEDIA_EXTENSIONS = media_parser._MEDIA_EXTENSIONS


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
# Metadata block and episode filename formatting
# ---------------------------------------------------------------------------


def _unique_audio_codecs(tracks: list[AudioTrack]) -> list[str]:
    """Return deduplicated non-commentary audio codec names in order."""
    seen: set[str] = set()
    codecs: list[str] = []
    for t in tracks:
        if not t.is_commentary and t.codec not in seen:
            seen.add(t.codec)
            codecs.append(t.codec)
    return codecs


def build_metadata_block(source: SourceFile) -> str:
    """Build the [...] metadata block for an episode filename.

    Format: ``release-group source,(REMUX,)res,codec,...``
    The release group and source type are space-separated; all subsequent
    fields are comma-separated.
    """
    if source.media is None:
        return ""

    media = source.media

    # Prefix part: "group source" (space-separated)
    # Append version to release group: "MTBB" + v2 → "MTBB(v2)"
    prefix_parts: list[str] = []
    if source.release_group:
        group = source.release_group
        if source.version is not None:
            group = f"{group}(v{source.version})"
        prefix_parts.append(group)
    # Default to "Web" when no source type detected — ensures the space
    # separator between group and tech fields is always present
    source_type = source.source_type or "Web"
    prefix_parts.append(source_type)
    prefix = " ".join(prefix_parts)

    # Comma-separated technical metadata
    tech: list[str] = []

    # REMUX
    if source.is_remux:
        tech.append("REMUX")

    # Resolution
    if media.resolution:
        tech.append(media.resolution)

    # Video codec
    if media.video_codec:
        tech.append(media.video_codec)

    # HDR/UHD/DoVi
    if media.hdr_type:
        tech.append(media.hdr_type)

    # 10bit (always for HEVC, also for other codecs with 10-bit)
    if media.bit_depth >= 10:
        tech.append("10bit")

    # Encoding library (x264/x265) — only when detected
    if media.encoding_lib:
        tech.append(media.encoding_lib)

    # Audio codecs and language detection
    non_commentary = [t for t in media.audio_tracks if not t.is_commentary]
    if non_commentary:
        codecs = _unique_audio_codecs(media.audio_tracks)
        languages: set[str] = set()
        for t in non_commentary:
            if t.language:
                languages.add(t.language)
        tech.append("+".join(codecs))

        has_ja = "ja" in languages or "jpn" in languages
        has_en = "en" in languages or "eng" in languages
        other_langs = languages - {"ja", "jpn", "en", "eng"}

        if has_ja and has_en and other_langs:
            tech.append("multi-audio")
        elif has_ja and has_en:
            tech.append("dual-audio")

    tech_str = ",".join(tech)

    if prefix and tech_str:
        return f"{prefix},{tech_str}"
    return prefix or tech_str


def _sanitize_path(name: str) -> str:
    """Sanitize a string for use in file/directory names.

    Replaces ``/`` with `` - `` and ``:`` with ``-``.
    """
    return name.replace("/", " - ").replace(":", "-")


def format_episode_filename(
    concise_name: str,
    season: int,
    episode: int,
    episode_name: str,
    source: SourceFile,
    is_movie: bool = False,
    movie_dir_name: str = "",
    is_special: bool = False,
    special_tag: str = "",
) -> str:
    """Build the full episode filename."""
    ext = source.path.suffix or ".mkv"
    metadata = build_metadata_block(source)
    meta_str = f" [{metadata}]" if metadata else ""
    hash_str = f" [{source.hash_code}]" if source.hash_code else ""

    concise_name = _sanitize_path(concise_name)
    episode_name = _sanitize_path(episode_name)
    movie_dir_name = _sanitize_path(movie_dir_name)

    if is_movie and not is_special:
        # Single-file movie: `DirName - complete movie [metadata] [hash].ext`
        return f"{movie_dir_name} - complete movie{meta_str}{hash_str}{ext}"

    if is_special:
        # Special: `Name - TAG - Episode Name [metadata] [hash].ext`
        if episode_name:
            return (
                f"{concise_name} - {special_tag} - "
                f"{episode_name}{meta_str}{hash_str}{ext}"
            )
        return f"{concise_name} - {special_tag}{meta_str}{hash_str}{ext}"

    # Regular episode: `Name - sXeYY - Episode Name [metadata] [hash].ext`
    ep_tag = f"s{season}e{episode:02d}"
    if episode_name:
        return f"{concise_name} - {ep_tag} - {episode_name}{meta_str}{hash_str}{ext}"
    return f"{concise_name} - {ep_tag}{meta_str}{hash_str}{ext}"


# ---------------------------------------------------------------------------
# Interactive prompts
# ---------------------------------------------------------------------------


def prompt_value(label: str, default: str = "") -> str:
    """Prompt for a value with an optional default."""
    if default:
        raw = input(f"{label} [{default}]: ").strip()
        return raw if raw else default
    return input(f"{label}: ").strip()


def prompt_confirm(message: str, default: bool = True) -> bool:
    """Prompt for yes/no confirmation."""
    suffix = "[Y/n]" if default else "[y/N]"
    raw = input(f"{message} {suffix}: ").strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes")


# ---------------------------------------------------------------------------
# Directory naming
# ---------------------------------------------------------------------------


def _strip_redundant_year(title: str, year: int) -> str:
    """Strip a trailing `` (YYYY)`` suffix if it matches the series year."""
    suffix = f" ({year})"
    if title.endswith(suffix):
        return title[: -len(suffix)]
    return title


# Hiragana, Katakana, CJK Unified Ideographs, CJK Extension A
_RE_JAPANESE = re.compile(r"[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\u3400-\u4DBF]")


def _has_japanese(text: str) -> bool:
    """Return True if *text* contains any CJK, hiragana, or katakana characters."""
    return bool(_RE_JAPANESE.search(text))


def format_series_dirname(title_ja: str, title_en: str, year: int) -> str:
    """Build the series directory name.

    Format is ``JA [EN] (YYYY)`` when both a native Japanese title and a
    distinct English title exist.  Falls back to ``TITLE (YYYY)`` when:
    - The Japanese title is empty or romaji (no kanji/kana)
    - The English title is empty
    - Both titles are identical
    """
    ja = _sanitize_path(_strip_redundant_year(title_ja, year))
    en = _sanitize_path(_strip_redundant_year(title_en, year))

    ja_is_native = bool(ja) and _has_japanese(ja)

    if ja_is_native and en and ja != en:
        return f"{ja} [{en}] ({year})"

    # Use whichever title is available; prefer English for readability
    title = en or ja
    return f"{title} ({year})"


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

    The Specials directory is not created here — it is created on demand
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
                id_file.open("x").write(str(id_value) + "\n")
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

    1. Check *id_map* for a matching AniDB/TheTVDB ID → use that directory
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

    # Step 3: prompt user — can enter an existing directory name or a new name
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
# AniDB HTTP API client
# ---------------------------------------------------------------------------

_ANIDB_API_URL = "http://api.anidb.net:9001/httpapi"
_anidb_last_request: float = 0.0

# AniDB episode type constants
_ANIDB_EP_REGULAR = "1"
_ANIDB_EP_SPECIAL = "2"
_ANIDB_EP_CREDIT = "3"
_ANIDB_EP_TRAILER = "4"
_ANIDB_EP_PARODY = "5"
_ANIDB_EP_OTHER = "6"

_ANIDB_EP_TYPE_MAP = {
    _ANIDB_EP_REGULAR: "regular",
    _ANIDB_EP_SPECIAL: "special",
    _ANIDB_EP_CREDIT: "credit",
    _ANIDB_EP_TRAILER: "trailer",
    _ANIDB_EP_PARODY: "parody",
    _ANIDB_EP_OTHER: "other",
}


def _cache_dir(provider: str) -> Path:
    """Return a cache directory under $XDG_CACHE_HOME/etp/<provider>."""
    base = Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
    cache = base / "etp" / provider
    cache.mkdir(parents=True, exist_ok=True)
    return cache


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

    # Titles — collect candidates in a single pass, then pick by priority.
    # Japanese: ja official > ja main > x-jat main > main fallback
    # English:  en official > en main
    ja_official = ""
    ja_main = ""
    jat_main = ""
    en_official = ""
    en_main = ""
    main_title_fallback = ""
    for title_elem in root.findall("titles/title"):
        lang = title_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
        ttype = title_elem.get("type", "")
        text = (title_elem.text or "").strip()

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

    title_ja = ja_official or ja_main or jat_main or main_title_fallback
    title_en = en_official or en_main

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
        ep_type = _ANIDB_EP_TYPE_MAP.get(ep_type_str, "other")
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
        for title_elem in ep_elem.findall("title"):
            lang = title_elem.get("{http://www.w3.org/XML/1998/namespace}lang", "")
            text = (title_elem.text or "").strip()
            if lang == "en" and not title_en_ep:
                title_en_ep = text.replace("`", "'")
            elif lang == "ja" and not title_ja_ep:
                title_ja_ep = text

        episodes.append(
            Episode(
                number=ep_number,
                ep_type=ep_type,
                title_en=title_en_ep,
                title_ja=title_ja_ep,
                special_tag=special_tag,
            )
        )

    # Sort episodes: regulars by number, then specials by tag
    episodes.sort(key=lambda e: (e.ep_type != "regular", e.number))

    return AnimeInfo(
        anidb_id=aid,
        tvdb_id=None,
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        episodes=episodes,
    )


def fetch_anidb_anime(
    aid: int,
    client: str,
    clientver: int,
    no_cache: bool = False,
) -> AnimeInfo:
    """Fetch anime info from AniDB HTTP API with caching."""
    cache_file = _cache_dir("anidb") / f"{aid}.xml"

    # Check cache (24h validity)
    if not no_cache and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < _CACHE_MAX_AGE_SECONDS:
            xml_text = cache_file.read_text(encoding="utf-8")
            return _parse_anidb_xml(xml_text, aid)

    # Fetch from API
    _anidb_rate_limit()

    params = f"request=anime&client={client}&clientver={clientver}&protover=1&aid={aid}"
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


# ---------------------------------------------------------------------------
# TheTVDB v4 API client
# ---------------------------------------------------------------------------

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
        special_tag = f"S{ep_num}" if is_special else ""

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
    cache_file = _cache_dir("tvdb") / f"{series_id}.json"

    # Check cache (24h validity)
    if not no_cache and cache_file.exists():
        age = time.time() - cache_file.stat().st_mtime
        if age < _CACHE_MAX_AGE_SECONDS:
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
    while page < _TVDB_MAX_PAGES:
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


# ---------------------------------------------------------------------------
# File operations
# ---------------------------------------------------------------------------


def compute_crc32(path: Path) -> str:
    """Compute the CRC32 hash of a file, returned as uppercase hex."""
    crc = 0
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):  # 1 MiB chunks
            crc = zlib.crc32(chunk, crc)
    return f"{crc & 0xFFFFFFFF:08X}"


def verify_hash(source: SourceFile) -> tuple[bool, str] | None:
    """Verify the CRC32 hash embedded in the filename against the file.

    Returns ``(True, actual_hash)`` if the hash matches,
    ``(False, actual_hash)`` if it mismatches, or ``None`` if no hash is
    present in the filename.
    """
    if not source.hash_code:
        return None
    actual = compute_crc32(source.path)
    return (actual.upper() == source.hash_code.upper(), actual)


@dataclass
class ConflictInfo:
    """Describes a conflict between an incoming file and an existing destination."""

    existing_path: Path
    existing_size: int
    existing_media: MediaInfo | None
    incoming_source: SourceFile
    incoming_dest: Path  # the intended destination filename
    metadata_matches: bool


def _extract_key_metadata(sf: SourceFile) -> tuple[str, str, str, str]:
    """Extract key metadata elements for comparison: (group, source, codec, audio)."""
    audio = ""
    if sf.media and sf.media.audio_tracks:
        audio = "+".join(_unique_audio_codecs(sf.media.audio_tracks))
    codec = sf.media.video_codec if sf.media else ""
    return (sf.release_group, sf.source_type, codec, audio)


def check_destination_conflict(
    source: SourceFile, dest_path: Path, intended_dest: Path | None = None
) -> ConflictInfo | None:
    """Check if destination already exists and return conflict info.

    *intended_dest* is the filename the user intends to write (may differ
    from *dest_path* when fuzzy-matching found an existing file with a
    different naming convention).

    Runs mediainfo on the existing file to ensure symmetric metadata
    comparison with the incoming file.
    """
    try:
        existing_size = dest_path.stat().st_size
    except FileNotFoundError:
        return None

    # Parse existing filename and analyze with mediainfo for comparison
    existing_sf = parse_source_filename(dest_path.name)
    existing_sf.path = dest_path
    existing_media: MediaInfo | None = None
    try:
        existing_media = analyze_file(dest_path)
        existing_sf.media = existing_media
    except (subprocess.CalledProcessError, FileNotFoundError):
        pass

    src_meta = _extract_key_metadata(source)
    dst_meta = _extract_key_metadata(existing_sf)
    metadata_matches = src_meta == dst_meta

    return ConflictInfo(
        existing_path=dest_path,
        existing_size=existing_size,
        existing_media=existing_media,
        incoming_source=source,
        incoming_dest=intended_dest or dest_path,
        metadata_matches=metadata_matches,
    )


def _format_size(size: int) -> str:
    """Format a file size in human-readable form."""
    if size >= 1 << 30:
        return f"{size / (1 << 30):.1f} GB"
    if size >= 1 << 20:
        return f"{size / (1 << 20):.1f} MB"
    return f"{size / (1 << 10):.1f} KB"


def _format_media_summary(media: MediaInfo | None) -> str:
    """Format a one-line mediainfo summary."""
    if media is None:
        return "(mediainfo unavailable)"
    parts: list[str] = []
    if media.video_codec:
        parts.append(media.video_codec)
    if media.resolution:
        parts.append(media.resolution)
    if media.bit_depth >= 10:
        parts.append(f"{media.bit_depth}bit")
    if media.hdr_type:
        parts.append(media.hdr_type)
    if media.audio_tracks:
        codecs = "+".join(_unique_audio_codecs(media.audio_tracks))
        if codecs:
            parts.append(codecs)
    return ", ".join(parts)


def resolve_conflict(conflict: ConflictInfo) -> str:
    """Handle a destination conflict. Returns 'replace', 'keep', or 'skip'.

    For matching metadata with matching CRC32, auto-replaces silently.
    """
    if conflict.metadata_matches:
        # Short-circuit: if file sizes differ, CRC32 can't match
        incoming_size = conflict.incoming_source.path.stat().st_size
        if incoming_size == conflict.existing_size:
            src_crc = compute_crc32(conflict.incoming_source.path)
            dst_crc = compute_crc32(conflict.existing_path)
            if src_crc == dst_crc:
                print(
                    "  Destination exists with matching encode"
                    " (CRC32 match) — replacing to fix naming."
                )
                return "replace"
            print("  WARNING: Same encode metadata but CRC32 differs!")
        else:
            print("  WARNING: Same encode metadata but file sizes differ!")
        print(f"    existing: {conflict.existing_path.name}")
        print(f"         new: {conflict.incoming_dest.name}")

    else:
        print("  Conflict: destination exists")
        print(f"    existing: {conflict.existing_path.name}")
        print(f"         new: {conflict.incoming_dest.name}")
        print()

        existing_size = _format_size(conflict.existing_size)
        incoming_size = _format_size(conflict.incoming_source.path.stat().st_size)
        existing_summary = _format_media_summary(conflict.existing_media)
        incoming_summary = _format_media_summary(conflict.incoming_source.media)
        print(f"    Existing: {existing_summary}, {existing_size}")
        print(f"         New: {incoming_summary}, {incoming_size}")

    print()
    while True:
        choice = input("  [k]eep existing  [r]eplace  [s]kip: ").strip().lower()
        if choice in ("k", "keep"):
            return "keep"
        if choice in ("r", "replace"):
            return "replace"
        if choice in ("s", "skip"):
            return "skip"
        print("  Please enter k, r, or s.")


# Matches episode tags like "s1e01", "s01e01", "s1e1" in filenames
_RE_EP_TAG = re.compile(r"[Ss](\d+)[Ee](\d+)")


def _find_existing_episode(dest_path: Path) -> Path | None:
    """Find an existing file for the same episode in the destination directory.

    Matches by episode tag (sXeYY) with fuzzy season/episode zero-padding,
    so ``s1e01`` matches ``s01e01`` and vice versa.
    """
    dest_dir = dest_path.parent
    if not dest_dir.is_dir():
        return None

    # Extract the episode tag from the target filename
    m = _RE_EP_TAG.search(dest_path.name)
    if not m:
        return None
    target_season = int(m.group(1))
    target_episode = int(m.group(2))

    # Scan the directory for a file with the same episode
    for existing in dest_dir.iterdir():
        if existing == dest_path or not existing.is_file():
            continue
        em = _RE_EP_TAG.search(existing.name)
        if (
            em
            and int(em.group(1)) == target_season
            and int(em.group(2)) == target_episode
        ):
            return existing

    return None


def handle_conflict(source: SourceFile, dest_path: Path) -> str | None:
    """Check for and resolve a destination conflict.

    First checks for an exact path match, then does a fuzzy search for
    an existing file with the same episode tag (handles different
    zero-padding conventions like s1e01 vs s01e01).

    Returns ``None`` if no conflict, or 'replace'/'keep'/'skip'.
    When 'replace' is returned, the existing file has already been removed.
    """
    # Exact path match
    conflict = check_destination_conflict(source, dest_path, intended_dest=dest_path)

    # Fuzzy match: same episode, different filename formatting
    if conflict is None:
        existing = _find_existing_episode(dest_path)
        if existing is not None:
            conflict = check_destination_conflict(
                source, existing, intended_dest=dest_path
            )

    if conflict is None:
        return None
    action = resolve_conflict(conflict)
    if action == "replace":
        conflict.existing_path.unlink()
    return action


def copy_reflink(src: Path, dst: Path, dry_run: bool = False) -> bool:
    """Copy a file using Btrfs COW reflink."""
    if dry_run:
        print(f"  [dry-run] cp --reflink=always {src} -> {dst}")
        return True

    # Ensure destination directory exists
    dst.parent.mkdir(parents=True, exist_ok=True)

    if _IS_LINUX:
        cmd = ["cp", "--reflink=always", str(src), str(dst)]
    else:
        print(
            f"  warning: reflinks not supported on {platform.system()}, "
            f"using regular copy"
        )
        cmd = ["cp", str(src), str(dst)]

    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  error: copy failed: {e}", file=sys.stderr)
        return False


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


@dataclass
class DownloadIndex:
    """Index of download files for matching against source files.

    Files are indexed by both a normalized series name key and by
    (season, episode) within each series, enabling series-aware matching.
    """

    # series_key → [(season, episode, path, size)]
    by_series: dict[str, list[tuple[int, int, Path, int]]] = field(default_factory=dict)
    file_count: int = 0


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
    # Collect download entries from all matching keys.  A single series
    # may be split across multiple download index keys (e.g. different
    # release groups use different romanizations of the same title).
    if title_index is not None:
        candidate_keys = title_index.matching_keys(series_name)
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
        # Exact tuple match naturally prevents season 0 (TVDB specials)
        # from matching regular seasons.
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
        # Handles DVD→aired order renumbering where episode numbers differ
        # but the file contents (and therefore size) are identical.
        if best is None and src_group:
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
            # In a subdirectory — use the immediate subdirectory name
            return _extract_series_name(rel.parts[0])
    # Directly in source dir — use the filename
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


def _find_episode_title(info: AnimeInfo, ep_number: int, season: int = 1) -> str:
    """Find the English title for a regular episode by number and season."""
    for ep in info.episodes:
        if ep.ep_type == "regular" and ep.number == ep_number and ep.season == season:
            return ep.title_en
    return ""


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
        episode_name = _find_episode_title(info, ep_number, season)
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

    # Verify CRC32 hash before building filename — on mismatch the hash
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
        action = handle_conflict(source, dest_path)
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


def _build_manifest_entries(
    parsed: list[SourceFile],
    info: AnimeInfo,
    concise_name: str,
    series_dir: Path,
    verbose: bool,
) -> list[ManifestEntry]:
    """Build manifest entries for all files without per-file prompts.

    Runs mediainfo, verifies CRC32 hashes, matches episodes, and constructs
    destination paths using defaults.
    """
    entries: list[ManifestEntry] = []
    total = len(parsed)
    for i, sf in enumerate(parsed, 1):
        print(f"  Analyzing {i}/{total}: {sf.path.name}")

        # Analyze with mediainfo
        try:
            sf.media = analyze_file(sf.path)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            if verbose:
                print(f"    warning: mediainfo failed: {e}")

        # Verify CRC32 hash — on mismatch, clear hash so it's stripped from dest
        hash_failed = False
        hash_result = verify_hash(sf)
        if hash_result is not None:
            ok, actual = hash_result
            if not ok:
                print(f"    CRC32 MISMATCH: expected {sf.hash_code}, got {actual}")
                sf.hash_code = ""
                hash_failed = True
            elif verbose:
                print(f"    CRC32 verified: {sf.hash_code}")

        # Match episode
        ep_number = sf.parsed_episode
        season = sf.parsed_season or 1
        is_special = False
        special_tag = ""
        episode_name = ""

        if ep_number is not None:
            episode_name = _find_episode_title(info, ep_number, season)

        # Build destination path
        if ep_number is None:
            # Can't auto-match — mark as TODO
            placeholder = format_episode_filename(
                concise_name=concise_name,
                season=season,
                episode=0,
                episode_name="EPISODE_NAME",
                source=sf,
                is_special=is_special,
                special_tag=special_tag,
            )
            placeholder = placeholder.replace("s1e00", "s1eXX")
            dest_dir = series_dir / f"Season {season:02d}"
            entries.append(
                ManifestEntry(
                    source=sf,
                    dest_path=dest_dir / placeholder,
                    is_todo=True,
                    hash_failed=hash_failed,
                )
            )
        else:
            filename = format_episode_filename(
                concise_name=concise_name,
                season=season,
                episode=ep_number,
                episode_name=episode_name,
                source=sf,
                is_special=is_special,
                special_tag=special_tag,
            )
            if is_special:
                dest_dir = series_dir / "Specials"
            else:
                dest_dir = series_dir / f"Season {season:02d}"
            entries.append(
                ManifestEntry(
                    source=sf,
                    dest_path=dest_dir / filename,
                    hash_failed=hash_failed,
                )
            )

    return entries


def _write_manifest(
    entries: list[ManifestEntry],
    info: AnimeInfo,
    concise_name: str,
    series_dir: Path,
) -> Path:
    """Write manifest entries to a KDL file for editing."""
    provider = ""
    if info.anidb_id is not None:
        provider = f"AniDB: {info.anidb_id}"
    elif info.tvdb_id is not None:
        provider = f"TheTVDB: {info.tvdb_id}"

    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)

    # Group entries by season/specials
    groups: dict[str, list[ManifestEntry]] = {}
    for entry in entries:
        # Determine group key from the destination path
        dest_parent = entry.dest_path.parent.name
        if dest_parent == "Specials":
            key = "specials"
        else:
            # "Season 01" → season number
            key = dest_parent
        groups.setdefault(key, []).append(entry)

    # Build KDL document as text (easier than constructing Node objects
    # for the header comments)
    lines: list[str] = []
    lines.append("// etp-anime triage manifest")
    lines.append(f"// Series: {dirname}")
    if provider:
        lines.append(f"// {provider}")
    lines.append(f"// Series dir: {series_dir}")
    lines.append("//")
    lines.append(
        "// Edit destination filenames. Delete or /- comment out entries to skip."
    )
    lines.append("// Source filenames are for reference only — only dest is used.")
    lines.append("")

    for group_key in sorted(groups.keys()):
        group_entries = sorted(
            groups[group_key], key=lambda e: e.source.parsed_episode or 0
        )
        if group_key == "specials":
            lines.append("specials {")
        else:
            # "Season 01" → season 1
            season_num = group_key.replace("Season ", "").lstrip("0") or "0"
            lines.append(f"season {season_num} {{")

        for entry in group_entries:
            ep_num = entry.source.parsed_episode or 0
            tag = "(todo)" if entry.is_todo else ""
            if entry.hash_failed:
                lines.append("  // CRC32 MISMATCH — hash stripped from destination")
            lines.append(f"  {tag}episode {ep_num} {{")
            lines.append(f'    source "{_escape_kdl(str(entry.source.path))}"')
            if entry.source.matched_download is not None:
                lines.append(
                    f'    downloaded "{_escape_kdl(str(entry.source.matched_download))}"'
                )
            dest_name = entry.dest_path.name
            if len(dest_name.encode("utf-8")) > _MAX_FILENAME_BYTES:
                lines.append(
                    f"    // WARNING: filename is"
                    f" {len(dest_name.encode('utf-8'))} bytes"
                    f" (max {_MAX_FILENAME_BYTES}) — shorten before saving"
                )
            lines.append(f'    dest "{_escape_kdl(dest_name)}"')
            lines.append("  }")

        lines.append("}")
        lines.append("")

    fd, path = tempfile.mkstemp(suffix=".kdl", prefix="etp-triage-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")

    return Path(path)


def _open_editor(manifest_path: Path) -> bool:
    """Open the manifest in the user's editor. Returns True on success."""
    editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi"
    try:
        result = subprocess.run([editor, str(manifest_path)])
        return result.returncode == 0
    except FileNotFoundError:
        print(f"  error: editor '{editor}' not found")
        return False


def _parse_manifest(
    manifest_path: Path,
    known_sources: dict[str, SourceFile],
    series_dir: Path,
) -> tuple[list[tuple[SourceFile, Path]], list[str]]:
    """Parse an edited KDL manifest file.

    Returns ``(entries, errors)``. If errors is non-empty, the manifest has
    problems the user should fix.
    """
    text = manifest_path.read_text(encoding="utf-8")
    try:
        doc = kdl.parse(text)
    except kdl.ParseError as e:
        return [], [f"  KDL parse error: {e}"]

    entries: list[tuple[SourceFile, Path]] = []
    errors: list[str] = []

    for group_node in doc.nodes:
        # Determine destination subdirectory
        if group_node.name == "specials":
            dest_subdir = series_dir / "Specials"
        elif group_node.name == "season" and group_node.args:
            season_num = int(group_node.args[0])
            dest_subdir = series_dir / f"Season {season_num:02d}"
        else:
            continue

        for ep_node in group_node.nodes:
            if ep_node.name != "episode":
                continue

            # Check for (todo) tag
            if ep_node.tag == "todo":
                errors.append(
                    f"  episode {ep_node.args[0] if ep_node.args else '?'}"
                    f" in {group_node.name}: unresolved (todo) entry"
                )
                continue

            # Extract source and dest from children
            source_name = ""
            dest_name = ""
            for child in ep_node.nodes:
                if child.name == "source" and child.args:
                    source_name = str(child.args[0])
                elif child.name == "dest" and child.args:
                    dest_name = str(child.args[0])

            if not dest_name:
                errors.append(
                    f"  episode {ep_node.args[0] if ep_node.args else '?'}"
                    f" in {group_node.name}: missing dest"
                )
                continue

            sf = known_sources.get(source_name)
            if sf is None:
                errors.append(
                    f"  episode in {group_node.name}: unknown source '{source_name}'"
                )
                continue

            entries.append((sf, dest_subdir / dest_name))

    return entries, errors


_MAX_FILENAME_BYTES = 255  # ext4/Btrfs filename length limit


def _check_filename_length(dest_path: Path) -> Path:
    """Check if the destination filename exceeds the filesystem limit.

    If too long, prompts the user to edit the filename until it fits.
    Returns the (possibly updated) destination path.
    """
    while len(dest_path.name.encode("utf-8")) > _MAX_FILENAME_BYTES:
        name_len = len(dest_path.name.encode("utf-8"))
        print(f"\n  ERROR: filename is {name_len} bytes (max {_MAX_FILENAME_BYTES}):")
        print(f"    {dest_path.name}")
        new_name = prompt_value("  Enter shorter filename", dest_path.name)
        dest_path = dest_path.parent / new_name
    return dest_path


def _execute_manifest(
    entries: list[tuple[SourceFile, Path]],
    dry_run: bool,
    verbose: bool,
) -> tuple[int, int, list[Path]]:
    """Execute the parsed manifest: copy each file to its destination.

    Returns ``(success, failed, triaged_paths)`` — triaged_paths includes
    files that were kept, skipped, or copied (all are marked as processed).
    """
    success = 0
    failed = 0
    triaged_paths: list[Path] = []

    for sf, dest_path in entries:
        # Check filename length before attempting any operations
        dest_path = _check_filename_length(dest_path)

        if verbose:
            print(f"  {sf.path.name} -> {dest_path}")

        # Check for existing file at destination
        if not dry_run:
            action = handle_conflict(sf, dest_path)
            if action in ("keep", "skip"):
                triaged_paths.append(sf.path)
                if action == "skip":
                    failed += 1
                else:
                    success += 1
                continue

        try:
            if copy_reflink(sf.path, dest_path, dry_run=dry_run):
                success += 1
                triaged_paths.append(sf.path)
            else:
                failed += 1
        except OSError as e:
            if e.errno == errno.ENAMETOOLONG:
                dest_path = _check_filename_length(dest_path)
                if copy_reflink(sf.path, dest_path, dry_run=dry_run):
                    success += 1
                    triaged_paths.append(sf.path)
                else:
                    failed += 1
            else:
                print(f"  error: {e}", file=sys.stderr)
                failed += 1

    return success, failed, triaged_paths


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

    # Group by parsed season
    by_season: dict[int, list[SourceFile]] = {}
    no_season: list[SourceFile] = []
    for sf in pool:
        if sf.parsed_season is not None:
            by_season.setdefault(sf.parsed_season, []).append(sf)
        elif sf.parsed_episode is not None:
            by_season.setdefault(1, []).append(sf)
        else:
            no_season.append(sf)

    if not by_season:
        return [], pool

    # Show candidates
    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)
    print(f"\n  {dirname} ({regular_count} regular episodes)")
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

    season_files = sorted(by_season[chosen], key=lambda sf: sf.parsed_episode or 0)

    # If there are more files than the AniDB entry has episodes, take
    # only the first N and leave the rest for the next AniDB ID
    if len(season_files) > regular_count > 0:
        matched = season_files[:regular_count]
        leftover = season_files[regular_count:]
        print(
            f"  AniDB entry has {regular_count} episodes but season has"
            f" {len(season_files)} files — taking first {regular_count},"
            f" {len(leftover)} remaining for next ID."
        )
    else:
        matched = season_files
        leftover = []

    # Renumber episodes to start at 1 (for multi-cour splits where e.g.
    # S03E13 needs to become s1e01 of the second AniDB entry)
    if matched:
        first_ep = matched[0].parsed_episode or 1
        if first_ep != 1:
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
) -> tuple[int, int, list[Path]]:
    """Batch-process a group of files via an editable manifest.

    Same interface as ``_process_group`` but uses a vidir-style workflow:
    build all source→destination mappings, open in $EDITOR, then execute.

    If *pre_parsed* is provided, uses those SourceFiles instead of parsing
    *files*. If *season_override* is set, all episodes are renumbered as
    that season (e.g., ``season_override=1`` forces ``s1eYY``).
    """
    parsed = pre_parsed if pre_parsed is not None else _parse_files(files)

    # Apply season override (for AniDB per-season processing)
    if season_override is not None:
        for sf in parsed:
            sf.parsed_season = season_override

    if not default_concise_name:
        default_concise_name = _extract_concise_name(parsed)
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

    # Build manifest entries (mediainfo + CRC32 verification)
    print()
    entries = _build_manifest_entries(parsed, info, concise_name, series_dir, verbose)

    # Write manifest to temp file
    manifest_path = _write_manifest(entries, info, concise_name, series_dir)

    # Build source lookup by full path for parsing back
    known_sources: dict[str, SourceFile] = {
        str(e.source.path): e.source for e in entries
    }

    file_count = len(parsed)

    # Edit → parse → re-edit loop
    try:
        while True:
            if not _open_editor(manifest_path):
                print("  Editor failed. Skipping group.")
                return 0, file_count, []

            parsed_entries, errors = _parse_manifest(
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
        return _execute_manifest(parsed_entries, dry_run, verbose)

    finally:
        try:
            manifest_path.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Subcommand entry points
#
# Three subcommands (see ADR 2026-03-26-01):
#   run_triage  — bulk import from downloads directory via KDL manifests
#   run_series  — sync from Sonarr-managed anime directory via KDL manifests
#   run_episode — import a single file interactively
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
    new series directories — requires explicit confirmation (default no).
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
                )
                pool = []  # TVDB consumes entire pool

            print(f"\n  Season done: {success} copied, {failed} skipped/failed")
            total_success += success
            total_failed += failed

            if copied_paths and not args.dry_run:
                for p in copied_paths:
                    already_copied.add(resolved_paths.get(p, str(p.resolve())))

        groups_processed += 1

    # Write manifest once at end (skip for dry-run).
    # Save if anything was processed — including files marked 'd' for done.
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
