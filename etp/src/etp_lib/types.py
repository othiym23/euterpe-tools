"""Shared dataclasses and constants for the anime collection manager."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

# ---------------------------------------------------------------------------
# Default paths (NAS layout)
# ---------------------------------------------------------------------------

DEFAULT_DOWNLOADS_DIR = Path("/volume1/docker/pvr/data/downloads")
DEFAULT_ANIME_SOURCE_DIR = Path("/volume1/docker/pvr/data/anime")
DEFAULT_DEST_DIR = Path("/volume1/video/anime")

# ---------------------------------------------------------------------------
# Cache / API constants
# ---------------------------------------------------------------------------

CACHE_MAX_AGE_SECONDS = 86400  # 24 hours
TVDB_MAX_PAGES = 100  # safety limit for paginated fetches

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

    def find_episode_title(self, ep_number: int, season: int = 1) -> str:
        """Find the English title for a regular episode by number and season."""
        for ep in self.episodes:
            if (
                ep.ep_type == "regular"
                and ep.number == ep_number
                and ep.season == season
            ):
                return ep.title_en
        return ""


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
    source_type: str = ""  # "BD", "Web", "DVD", "HDTV", "SDTV", "VCD", "CD-R"
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
class DownloadIndex:
    """Index of download files for matching against source files.

    Files are indexed by both a normalized series name key and by
    (season, episode) within each series, enabling series-aware matching.
    """

    # series_key -> [(season, episode, path, size)]
    by_series: dict[str, list[tuple[int, int, Path, int]]] = field(default_factory=dict)
    file_count: int = 0


@dataclass
class AnimeConfig:
    """Configuration loaded from anime-ingestion.kdl."""

    downloads_dir: Path = field(default_factory=lambda: DEFAULT_DOWNLOADS_DIR)
    anime_source_dir: Path = field(default_factory=lambda: DEFAULT_ANIME_SOURCE_DIR)
    anime_dest_dir: Path = field(default_factory=lambda: DEFAULT_DEST_DIR)
    series_mappings: dict[str, list[tuple[str, int]]] = field(default_factory=dict)
    # series directory name -> concise name from parser (for title matching)
    concise_names: dict[str, str] = field(default_factory=dict)
