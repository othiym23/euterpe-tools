"""Shared dataclasses and constants for the anime collection manager."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path


# ---------------------------------------------------------------------------
# Enums (StrEnum for backwards-compatible string comparisons)
# ---------------------------------------------------------------------------


class EpisodeType(StrEnum):
    REGULAR = "regular"
    SPECIAL = "special"
    CREDIT = "credit"
    TRAILER = "trailer"
    PARODY = "parody"
    OTHER = "other"


class BonusType(StrEnum):
    NCOP = "NCOP"
    NCED = "NCED"
    PV = "PV"
    PREVIEW = "Preview"
    CM = "CM"
    MENU = "Menu"
    BONUS = "Bonus"


class MetadataProvider(StrEnum):
    ANIDB = "anidb"
    TVDB = "tvdb"


class ConflictAction(StrEnum):
    KEEP = "keep"
    REPLACE = "replace"
    BOTH = "both"
    SKIP = "skip"


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
    ep_type: EpisodeType
    title_en: str
    title_ja: str
    special_tag: str  # "S1", "CM01", "NCOP1", "NCED3", "T1", etc.
    title_romaji: str = ""  # x-jat romanization
    season: int = 1  # TVDB season number (AniDB is always 1)


@dataclass
class AnimeInfo:
    anidb_id: int | None
    tvdb_id: int | None
    title_ja: str
    title_en: str
    year: int
    title_romaji: str = ""  # x-jat romanization (e.g. "Youjo Senki")
    aliases: list[str] = field(default_factory=list)
    """All known title variants — synonyms, alternate language names, romaji."""
    episodes: list[Episode] = field(default_factory=list)

    def all_titles(self) -> list[str]:
        """Return every known title variant, deduped, in priority order.

        English/Japanese/romaji main titles come first, followed by any
        additional aliases. Empty strings are filtered out.
        """
        seen: set[str] = set()
        result: list[str] = []
        for t in (self.title_en, self.title_ja, self.title_romaji, *self.aliases):
            if t and t not in seen:
                seen.add(t)
                result.append(t)
        return result

    def find_episode_title(self, ep_number: int, season: int = 1) -> str:
        """Find the episode title by number and season.

        Prefers English, falls back to romaji.
        """
        for ep in self.episodes:
            if (
                ep.ep_type == EpisodeType.REGULAR
                and ep.number == ep_number
                and ep.season == season
            ):
                return ep.title_en or ep.title_romaji
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
class ParsedMetadata:
    """Parser-detected metadata from a media filename.

    Populated by media_parser.parse_component() and stored on SourceFile.
    Separates parser-derived fields from runtime state (mediainfo, download
    matching, path) for cleaner composition.
    """

    series_name: str = ""
    release_group: str = ""
    source_type: str = ""  # "BD", "Web", "DVD", "HDTV", "SDTV", "VCD", "CD-R"
    is_remux: bool = False
    hash_code: str = ""  # e.g. "ABCD1234"
    episode: int | None = None
    season: int | None = None
    version: int | None = None  # e.g. 2 for "v2" releases
    bonus_type: str = ""  # "NCOP", "NCED", "PV", "CM", etc.
    is_special: bool = False
    special_tag: str = ""  # "SP1", "OVA2", "S01OVA", etc.
    episode_title: str = ""
    is_dual_audio: bool = False
    is_uncensored: bool = False
    series_name_alt: str = ""  # alternate-language title
    episodes: list[int] = field(default_factory=list)  # multi-episode
    streaming_service: str = ""  # "AMZN", "CR", "NF", etc.


@dataclass
class SourceFile:
    path: Path
    parsed: ParsedMetadata = field(default_factory=ParsedMetadata)
    media: MediaInfo | None = None
    matched_download: Path | None = None


@dataclass
class MatchedFile:
    """A SourceFile with overridden episode/season/metadata for manifest building.

    Wraps the original SourceFile without mutating it. Overrides are applied
    during season matching and special detection so the original pool data
    is preserved for subsequent processing passes (e.g., multi-cour splits
    where the same pool serves multiple AniDB IDs).
    """

    source: SourceFile
    episode: int | None = None
    season: int | None = None
    is_special: bool = False
    special_tag: str = ""
    bonus_type: str = ""
    episode_title: str = ""
    hash_verified: bool = True  # False if CRC32 mismatch detected

    # Batch-level overrides (release group, dual-audio, etc.)
    release_group: str | None = None  # None = use source.parsed value
    is_dual_audio: bool | None = None
    is_uncensored: bool | None = None

    @property
    def path(self) -> Path:
        return self.source.path

    @property
    def media(self) -> MediaInfo | None:
        return self.source.media

    @media.setter
    def media(self, value: MediaInfo | None) -> None:
        self.source.media = value

    @property
    def matched_download(self) -> Path | None:
        return self.source.matched_download

    @property
    def effective_episode(self) -> int | None:
        return self.episode if self.episode is not None else self.source.parsed.episode

    @property
    def effective_season(self) -> int | None:
        return self.season if self.season is not None else self.source.parsed.season

    @property
    def effective_release_group(self) -> str:
        if self.release_group is not None:
            return self.release_group
        return self.source.parsed.release_group

    @property
    def effective_source_type(self) -> str:
        return self.source.parsed.source_type

    @property
    def effective_is_remux(self) -> bool:
        return self.source.parsed.is_remux

    @property
    def effective_hash_code(self) -> str:
        if not self.hash_verified:
            return ""
        return self.source.parsed.hash_code

    @property
    def effective_version(self) -> int | None:
        return self.source.parsed.version

    @property
    def effective_is_dual_audio(self) -> bool:
        if self.is_dual_audio is not None:
            return self.is_dual_audio
        return self.source.parsed.is_dual_audio

    @property
    def effective_is_uncensored(self) -> bool:
        if self.is_uncensored is not None:
            return self.is_uncensored
        return self.source.parsed.is_uncensored

    @property
    def effective_streaming_service(self) -> str:
        return self.source.parsed.streaming_service

    def to_source_snapshot(self) -> SourceFile:
        """Create a SourceFile snapshot with effective values baked in.

        Used when passing to functions that expect SourceFile (e.g., naming,
        conflict resolution) — applies all overrides to a copy of ParsedMetadata.
        """
        from dataclasses import replace

        pm = replace(
            self.source.parsed,
            episode=self.effective_episode,
            season=self.effective_season,
            is_special=self.is_special or self.source.parsed.is_special,
            special_tag=self.special_tag or self.source.parsed.special_tag,
            bonus_type=self.bonus_type or self.source.parsed.bonus_type,
            episode_title=self.episode_title or self.source.parsed.episode_title,
            hash_code=self.effective_hash_code,
            release_group=self.effective_release_group,
            is_dual_audio=self.effective_is_dual_audio,
            is_uncensored=self.effective_is_uncensored,
            episodes=list(self.source.parsed.episodes),
        )
        return SourceFile(
            path=self.source.path,
            parsed=pm,
            media=self.source.media,
            matched_download=self.source.matched_download,
        )


@dataclass
class GroupDefaults:
    """Sticky defaults that carry across files within a group.

    When processing multiple files together (e.g. triage mode), values
    confirmed for one file become the defaults offered for the next.
    """

    release_group: str = ""
    source_type: str = ""
    is_dual_audio: bool = False
    is_uncensored: bool = False
    streaming_service: str = ""


@dataclass
class ManifestEntry:
    """One line in the triage manifest: source file to destination path."""

    source: SourceFile
    dest_path: Path
    is_todo: bool = False
    hash_failed: bool = False
    episode_name: str = ""
    is_special: bool = False
    special_tag: str = ""


@dataclass
class BatchResult:
    """Result of a batch processing operation."""

    success: int = 0
    failed: int = 0
    skipped: int = 0
    triaged: list[Path] = field(default_factory=list)
    """Paths of files that were processed (copied, kept, or skipped)."""


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
