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
class ParsedMetadata:
    """Parser-detected metadata from a media filename.

    Populated by media_parser.parse_component() and stored on SourceFile.
    Separates parser-derived fields from runtime state (mediainfo, download
    matching, path) for cleaner composition.
    """

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


@dataclass(init=False)
class SourceFile:
    path: Path
    parsed: ParsedMetadata
    media: MediaInfo | None
    matched_download: Path | None

    def __init__(
        self,
        path: Path,
        parsed: ParsedMetadata | None = None,
        media: MediaInfo | None = None,
        matched_download: Path | None = None,
        # Legacy kwargs — forwarded to ParsedMetadata for backwards compat
        **kwargs: object,
    ) -> None:
        self.path = path
        self.media = media
        self.matched_download = matched_download
        if parsed is not None:
            self.parsed = parsed
        elif kwargs:
            # Map legacy SourceFile field names to ParsedMetadata names
            if "parsed_episode" in kwargs:
                kwargs["episode"] = kwargs.pop("parsed_episode")
            if "parsed_season" in kwargs:
                kwargs["season"] = kwargs.pop("parsed_season")
            self.parsed = ParsedMetadata(**kwargs)  # type: ignore[arg-type]
        else:
            self.parsed = ParsedMetadata()

    @property
    def release_group(self) -> str:
        return self.parsed.release_group

    @release_group.setter
    def release_group(self, value: str) -> None:
        self.parsed.release_group = value

    @property
    def source_type(self) -> str:
        return self.parsed.source_type

    @source_type.setter
    def source_type(self, value: str) -> None:
        self.parsed.source_type = value

    @property
    def is_remux(self) -> bool:
        return self.parsed.is_remux

    @is_remux.setter
    def is_remux(self, value: bool) -> None:
        self.parsed.is_remux = value

    @property
    def hash_code(self) -> str:
        return self.parsed.hash_code

    @hash_code.setter
    def hash_code(self, value: str) -> None:
        self.parsed.hash_code = value

    @property
    def parsed_episode(self) -> int | None:
        return self.parsed.episode

    @parsed_episode.setter
    def parsed_episode(self, value: int | None) -> None:
        self.parsed.episode = value

    @property
    def parsed_season(self) -> int | None:
        return self.parsed.season

    @parsed_season.setter
    def parsed_season(self, value: int | None) -> None:
        self.parsed.season = value

    @property
    def version(self) -> int | None:
        return self.parsed.version

    @version.setter
    def version(self, value: int | None) -> None:
        self.parsed.version = value

    @property
    def bonus_type(self) -> str:
        return self.parsed.bonus_type

    @bonus_type.setter
    def bonus_type(self, value: str) -> None:
        self.parsed.bonus_type = value

    @property
    def is_special(self) -> bool:
        return self.parsed.is_special

    @is_special.setter
    def is_special(self, value: bool) -> None:
        self.parsed.is_special = value

    @property
    def special_tag(self) -> str:
        return self.parsed.special_tag

    @special_tag.setter
    def special_tag(self, value: str) -> None:
        self.parsed.special_tag = value

    @property
    def episode_title(self) -> str:
        return self.parsed.episode_title

    @episode_title.setter
    def episode_title(self, value: str) -> None:
        self.parsed.episode_title = value

    @property
    def is_dual_audio(self) -> bool:
        return self.parsed.is_dual_audio

    @is_dual_audio.setter
    def is_dual_audio(self, value: bool) -> None:
        self.parsed.is_dual_audio = value

    @property
    def is_uncensored(self) -> bool:
        return self.parsed.is_uncensored

    @is_uncensored.setter
    def is_uncensored(self, value: bool) -> None:
        self.parsed.is_uncensored = value

    @property
    def series_name_alt(self) -> str:
        return self.parsed.series_name_alt

    @series_name_alt.setter
    def series_name_alt(self, value: str) -> None:
        self.parsed.series_name_alt = value

    @property
    def episodes(self) -> list[int]:
        return self.parsed.episodes

    @episodes.setter
    def episodes(self, value: list[int]) -> None:
        self.parsed.episodes = value

    @property
    def streaming_service(self) -> str:
        return self.parsed.streaming_service

    @streaming_service.setter
    def streaming_service(self, value: str) -> None:
        self.parsed.streaming_service = value


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
