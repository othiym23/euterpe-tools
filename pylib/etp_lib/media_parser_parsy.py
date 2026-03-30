"""Parser combinator primitives for media filename parsing.

Phase A of the parsy-based parser rewrite: standalone primitives that
recognize individual tokens (resolution, codec, source, episode markers,
etc.) without any structural or contextual logic.  Each primitive is
independently testable and consumes a single whitespace-delimited word.

These primitives will be composed into convention-specific grammars in
a later phase.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from parsy import Parser, Result, regex

# ---------------------------------------------------------------------------
# Vocabulary (shared with the existing parser)
# ---------------------------------------------------------------------------

from etp_lib.media_parser import (
    _AUDIO_CODECS,
    _LANGUAGES,
    _SOURCE_TYPE_MAP,
    _SOURCES,
    _VIDEO_CODECS,
)


# ---------------------------------------------------------------------------
# Result types — lightweight wrappers so each primitive returns a typed value
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Resolution:
    value: str  # e.g. "1080p", "1920x1080"


@dataclass(frozen=True, slots=True)
class VideoCodec:
    value: str  # e.g. "HEVC", "x265"


@dataclass(frozen=True, slots=True)
class AudioCodec:
    value: str  # e.g. "FLAC", "AAC2.0", "DTS-HDMA"


@dataclass(frozen=True, slots=True)
class Source:
    value: str  # raw keyword, e.g. "BluRay"
    source_type: str  # canonical, e.g. "BD"


@dataclass(frozen=True, slots=True)
class Remux:
    pass


@dataclass(frozen=True, slots=True)
class EpisodeSE:
    season: int
    episode: int
    version: int | None = None


@dataclass(frozen=True, slots=True)
class EpisodeBare:
    episode: int
    version: int | None = None


@dataclass(frozen=True, slots=True)
class EpisodeJP:
    episode: int


@dataclass(frozen=True, slots=True)
class SeasonJP:
    season: int


@dataclass(frozen=True, slots=True)
class SeasonWord:
    season: int


@dataclass(frozen=True, slots=True)
class SeasonOnly:
    season: int


@dataclass(frozen=True, slots=True)
class Special:
    tag: str  # "SP", "OVA", etc.
    number: int | None = None


@dataclass(frozen=True, slots=True)
class BatchRange:
    start: int
    end: int


@dataclass(frozen=True, slots=True)
class Version:
    number: int


@dataclass(frozen=True, slots=True)
class Year:
    value: int


@dataclass(frozen=True, slots=True)
class CRC32:
    value: str  # 8 hex chars


@dataclass(frozen=True, slots=True)
class Language:
    value: str


@dataclass(frozen=True, slots=True)
class BonusKeyword:
    bonus_type: str  # "NCOP", "NCED", "PV", etc.
    raw: str


# ---------------------------------------------------------------------------
# Primitives — each parses a complete word/token
# ---------------------------------------------------------------------------

# Resolution: "1080p", "720p", "4K", "1920x1080"
resolution: Parser = regex(
    r"(?:480|540|576|720|1080)[pi]|2160p|4[kK]", re.IGNORECASE
).map(lambda s: Resolution(s)) | regex(r"\d{3,4}x\d{3,4}").map(lambda s: Resolution(s))


# Video codec: "HEVC", "x265", "H.264", etc.
# Case-insensitive match against the vocabulary set.
def _match_set_ci(vocab: frozenset[str], result_type: type):
    """Build a parser that matches any word in a case-insensitive vocabulary set."""

    def _parser(stream: str, index: int):
        # Consume a contiguous non-whitespace token
        end = index
        while end < len(stream) and not stream[end].isspace():
            end += 1
        if end == index:
            return Result.failure(index, frozenset({result_type.__name__}))
        word = stream[index:end]
        if word.lower() in vocab:
            return Result.success(end, result_type(word))
        return Result.failure(index, frozenset({result_type.__name__}))

    return Parser(_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


video_codec: Parser = _match_set_ci(_VIDEO_CODECS, VideoCodec)

# Audio codec: simple keywords + compound forms like "AAC2.0", "DTS-HDMA"
_RE_AC = re.compile(
    r"(?:DTS-HDMA|DTS-HD\s*MA|DTS-HD|DTS|DDP|DD|EAC3|E-AC-3|AC3|AAC|FLAC|TrueHD|PCM|LPCM)"
    r"(?:[.\s]?\d\.\d)?",
    re.IGNORECASE,
)


def _audio_codec_parser(stream: str, index: int):
    m = _RE_AC.match(stream, index)
    if m:
        return Result.success(m.end(), AudioCodec(m.group(0)))
    # Simple keyword fallback
    end = index
    while end < len(stream) and not stream[end].isspace():
        end += 1
    if end > index:
        word = stream[index:end]
        if word.lower() in _AUDIO_CODECS:
            return Result.success(end, AudioCodec(word))
    return Result.failure(index, frozenset({"audio_codec"}))


audio_codec: Parser = Parser(_audio_codec_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Source: "BD", "BluRay", "WEB-DL", "CR", etc.
def _source_parser(stream: str, index: int):
    end = index
    while end < len(stream) and not stream[end].isspace():
        end += 1
    if end == index:
        return Result.failure(index, frozenset({"source"}))
    word = stream[index:end]
    lower = word.lower()
    if lower in _SOURCES:
        source_type = _SOURCE_TYPE_MAP.get(lower, "")
        return Result.success(end, Source(word, source_type))
    return Result.failure(index, frozenset({"source"}))


source: Parser = Parser(_source_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# REMUX
remux: Parser = regex(r"REMUX", re.IGNORECASE).map(lambda _: Remux())

# Episode: SxxExx with optional version
_RE_EP_SE_P = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?", re.IGNORECASE)


def _parse_episode_se(s: str) -> EpisodeSE:
    m = _RE_EP_SE_P.match(s)
    assert m is not None
    return EpisodeSE(
        season=int(m.group(1)),
        episode=int(m.group(2)),
        version=int(m.group(3)) if m.group(3) else None,
    )


episode_se: Parser = regex(
    r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?",
).map(_parse_episode_se)


# Episode: bare number with optional version (not a year)
def _bare_episode_parser(stream: str, index: int):
    m = re.match(r"(\d{1,4})(?:v(\d+))?(?=\s|$|\.)", stream[index:])
    if not m:
        return Result.failure(index, frozenset({"bare_episode"}))
    num = int(m.group(1))
    # Reject years
    if 1900 <= num <= 2099 and m.group(2) is None:
        return Result.failure(index, frozenset({"bare_episode"}))
    version = int(m.group(2)) if m.group(2) else None
    return Result.success(index + m.end(), EpisodeBare(num, version))


episode_bare: Parser = Parser(_bare_episode_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


def _re_group(pattern: str, s: str, group: int = 1, flags: int = 0) -> str:
    """Extract a regex group, asserting the match exists."""
    m = re.match(pattern, s, flags)
    assert m is not None
    return m.group(group)


# Episode: Japanese 第NN話
episode_jp: Parser = regex(r"第(\d{1,4})話").map(
    lambda s: EpisodeJP(int(_re_group(r"第(\d+)話", s)))
)

# Season: Japanese 第N期
season_jp: Parser = regex(r"第(\d{1,2})期").map(
    lambda s: SeasonJP(int(_re_group(r"第(\d+)期", s)))
)

# Season: English ordinal "4th Season"
season_word: Parser = regex(r"(\d+)(?:st|nd|rd|th)\s+Season", re.IGNORECASE).map(
    lambda s: SeasonWord(int(_re_group(r"(\d+)", s)))
)

# Season: bare "S01"
season_only: Parser = regex(r"[Ss](\d{1,2})(?!\d|[Ee])").map(
    lambda s: SeasonOnly(int(_re_group(r"[Ss](\d+)", s)))
)


# Special: SP1, OVA, OAD, ONA
def _parse_special(s: str) -> Special:
    tag = _re_group(r"(SP|OVA|OAD|ONA)", s, flags=re.IGNORECASE).upper()
    m = re.search(r"(\d+)$", s)
    return Special(tag=tag, number=int(m.group(1)) if m else None)


special: Parser = regex(r"(SP|OVA|OAD|ONA)(\d*)", re.IGNORECASE).map(_parse_special)


# Batch range: "01~26", "01 ~ 13"
def _parse_batch_range(s: str) -> BatchRange:
    parts = re.split(r"\s*[~～]\s*", s)
    return BatchRange(start=int(parts[0]), end=int(parts[1]))


batch_range: Parser = regex(r"(\d{1,4})\s*[~～]\s*(\d{1,4})").map(_parse_batch_range)

# Version: "v2", "v3"
version: Parser = regex(r"v(\d+)", re.IGNORECASE).map(
    lambda s: Version(int(_re_group(r"v(\d+)", s, flags=re.IGNORECASE)))
)

# Year: 4-digit year
year: Parser = regex(r"(?:19|20)\d{2}(?!\d)").map(lambda s: Year(int(s)))

# CRC32: exactly 8 hex chars
crc32: Parser = regex(r"[0-9A-Fa-f]{8}(?![0-9A-Fa-f])").map(lambda s: CRC32(s.upper()))

# Language keywords
language: Parser = _match_set_ci(_LANGUAGES, Language)

# Bonus keywords (English)
_RE_BONUS_EN_P = re.compile(
    r"NC\s*(?:OP|ED)\d*|NCOP\d*|NCED\d*|Creditless\s+(?:OP|ED)\d*",
    re.IGNORECASE,
)


def _bonus_parser(stream: str, index: int):
    m = _RE_BONUS_EN_P.match(stream, index)
    if m:
        matched = m.group(0)
        if re.search(r"OP\d*$", matched, re.IGNORECASE):
            return Result.success(m.end(), BonusKeyword("NCOP", matched))
        if re.search(r"ED\d*$", matched, re.IGNORECASE):
            return Result.success(m.end(), BonusKeyword("NCED", matched))
    return Result.failure(index, frozenset({"bonus_keyword"}))


bonus_en: Parser = Parser(_bonus_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Any single metadata keyword (resolution, codec, source, remux, language)
metadata_word: Parser = (
    resolution | audio_codec | video_codec | source | remux | language
)
