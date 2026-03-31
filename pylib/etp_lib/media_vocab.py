"""Vocabulary sets and data types for media filename parsing.

Separated from media_parser to avoid circular imports with parsy
primitives.  No parsing logic here — just data definitions.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto


class TokenKind(Enum):
    """Token types produced by the tokenizer and classifier."""

    # Structural (phase 1)
    BRACKET = auto()  # [content]
    PAREN = auto()  # (content) -- depth-tracked for nesting
    LENTICULAR = auto()  # 「content」
    TEXT = auto()  # bare text between delimiters
    DOT_TEXT = auto()  # individual segment from dot-separated scene name
    SEPARATOR = auto()  # " - "
    EXTENSION = auto()  # .mkv, .mp4, etc.
    PATH_SEP = auto()  # / boundary between path components

    # Semantic (phase 2)
    RELEASE_GROUP = auto()
    CRC32 = auto()
    EPISODE = auto()
    SEASON = auto()
    VERSION = auto()
    RESOLUTION = auto()
    VIDEO_CODEC = auto()
    AUDIO_CODEC = auto()
    SOURCE = auto()
    REMUX = auto()
    YEAR = auto()
    TITLE = auto()
    EPISODE_TITLE = auto()
    BATCH_RANGE = auto()
    SUBTITLE_INFO = auto()
    LANGUAGE = auto()
    SITE_PREFIX = auto()
    BONUS = auto()  # 映像特典, ノンテロップOP, etc.
    SPECIAL = auto()  # SP1, OVA, S01OVA, S03OP — typed result preserved
    DUAL_AUDIO = auto()
    UNCENSORED = auto()
    EDITION = auto()  # Criterion, Remastered, etc.
    UNKNOWN = auto()


@dataclass
class Token:
    """A single token from the tokenizer/classifier pipeline."""

    kind: TokenKind
    text: str
    # Extracted numeric values (populated by classifier for EPISODE/SEASON/YEAR)
    season: int | None = None
    episode: int | None = None
    version: int | None = None
    year: int | None = None
    batch_start: int | None = None
    batch_end: int | None = None
    is_decimal_special: bool = False  # True for episodes like 01.5


# ---------------------------------------------------------------------------
# Media file extensions
# ---------------------------------------------------------------------------

_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".avi"})
_ALL_EXTENSIONS = frozenset(
    {".mkv", ".mp4", ".avi", ".rar", ".iso", ".zip", ".7z", ".webdl"}
)
_ALL_EXTENSIONS_SORTED = tuple(
    sorted(_ALL_EXTENSIONS, key=lambda e: len(e), reverse=True)
)


# ---------------------------------------------------------------------------
# Vocabulary sets (all lowercase for case-insensitive matching)
# ---------------------------------------------------------------------------

_VIDEO_CODECS = frozenset(
    {
        "hevc",
        "avc",
        "x265",
        "x264",
        "h.264",
        "h.265",
        "h264",
        "h265",
        "av1",
        "xvid",
        "divx",
        "mpeg2",
        "vp9",
    }
)

_AUDIO_CODECS = frozenset(
    {
        "aac",
        "flac",
        "opus",
        "dd",
        "dd+",
        "ddp",
        "dts",
        "dts-hd",
        "dts-hdma",
        "e-ac-3",
        "eac3",
        "ac3",
        "truehd",
        "pcm",
        "lpcm",
    }
)

_SOURCES = frozenset(
    {
        "bd",
        "blu-ray",
        "bluray",
        "bdrip",
        "bdremux",
        "web",
        "web-dl",
        "webdl",
        "webrip",
        "cr",
        "amzn",
        "dsnp",
        "nf",
        "hidive",
        "hidi",
        "hulu",
        "adn",
        "unext",
        "atvp",
        "funi",
        "hdtv",
        "dvd",
        "dvd-r",
        "dvdr",
        "dvdrip",
        "vcd",
        "cd-r",
        "cdr",
        "sdtv",
        "raw",
        # Additional streaming services
        "pcok",
        "pmtp",
        "stan",
        "it",
        "ma",
        "kntv",
        "tver",
        "abema",
        "vrv",
        "bili",
    }
)

# Streaming service tags — these are source keywords that identify the
# specific streaming platform. All map to source_type "Web".
_STREAMING_SERVICES: frozenset[str] = frozenset(
    {
        "amzn",
        "cr",
        "nf",
        "dsnp",
        "hidive",
        "hidi",
        "hulu",
        "adn",
        "unext",
        "atvp",
        "funi",
        "pcok",
        "pmtp",
        "stan",
        "it",
        "ma",
        "kntv",
        "tver",
        "abema",
        "vrv",
        "bili",
    }
)

# Map lowercase source keywords to canonical source_type values.
_SOURCE_TYPE_MAP: dict[str, str] = {
    "bd": "BD",
    "blu-ray": "BD",
    "bluray": "BD",
    "bdrip": "BD",
    "bdremux": "BD",
    "web": "Web",
    "web-dl": "Web",
    "webdl": "Web",
    "webrip": "Web",
    "cr": "Web",
    "amzn": "Web",
    "dsnp": "Web",
    "nf": "Web",
    "hidive": "Web",
    "hidi": "Web",
    "hulu": "Web",
    "adn": "Web",
    "unext": "Web",
    "atvp": "Web",
    "funi": "Web",
    "pcok": "Web",
    "pmtp": "Web",
    "stan": "Web",
    "it": "Web",
    "ma": "Web",
    "kntv": "Web",
    "tver": "Web",
    "abema": "Web",
    "vrv": "Web",
    "bili": "Web",
    "dvd": "DVD",
    "dvdrip": "DVD",
    "dvd-r": "DVD-R",
    "dvdr": "DVD-R",
    "hdtv": "HDTV",
    "sdtv": "SDTV",
    "vcd": "VCD",
    "cd-r": "CD-R",
    "cdr": "CD-R",
}

_LANGUAGES = frozenset(
    {
        "dual",
        "multi",
        "jpn",
        "eng",
        "ger",
        "fre",
        "spa",
        "ita",
        "chi",
        "kor",
        "rus",
        "ara",
        "por",
        "tha",
    }
)

_SUBTITLE_KEYWORDS = frozenset(
    {
        "sub",
        "subs",
        "multisub",
        "msubs",
        "subtitle",
        "subtitles",
        "multiple subtitle",
        "esub",
        "esubs",
        "csub",
        "hsub",
    }
)

_HDR_KEYWORDS = frozenset(
    {
        "hdr",
        "hdr10",
        "hdr10+",
        "dovi",
        "dolby vision",
    }
)

# Token kinds that are metadata (not title)
_METADATA_KINDS = frozenset(
    {
        TokenKind.RELEASE_GROUP,
        TokenKind.CRC32,
        TokenKind.EPISODE,
        TokenKind.SEASON,
        TokenKind.VERSION,
        TokenKind.RESOLUTION,
        TokenKind.VIDEO_CODEC,
        TokenKind.AUDIO_CODEC,
        TokenKind.SOURCE,
        TokenKind.REMUX,
        TokenKind.YEAR,
        TokenKind.BATCH_RANGE,
        TokenKind.SUBTITLE_INFO,
        TokenKind.LANGUAGE,
        TokenKind.SITE_PREFIX,
        TokenKind.BONUS,
        TokenKind.SPECIAL,
        TokenKind.DUAL_AUDIO,
        TokenKind.UNCENSORED,
        TokenKind.EDITION,
        TokenKind.UNKNOWN,
        TokenKind.EXTENSION,
        TokenKind.PATH_SEP,
    }
)
