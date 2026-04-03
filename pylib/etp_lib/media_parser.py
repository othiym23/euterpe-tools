"""Tokenizer and parser for anime/media file and directory paths.

Two-phase pipeline:
1. Structural tokenizer: splits paths into delimited groups (brackets, parens,
   lenticular quotes) and bare text, handling nesting and dot-separated scene names.
2. Semantic classifier: reclassifies structural tokens by matching content against
   known vocabularies (codecs, sources, resolutions, episode patterns, etc.).

The parser extracts metadata from full relative paths (directory + filename),
handling both English and Japanese naming conventions.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date

from parsy import Parser, Result, regex

# Import and re-export vocabulary — external consumers import from media_parser
from etp_lib.media_vocab import (  # noqa: F401 (re-exports)
    Token,
    TokenKind,
    _ALL_EXTENSIONS,
    _ALL_EXTENSIONS_SORTED,
    _AUDIO_CODECS,
    _HDR_KEYWORDS,
    _LANGUAGES,
    _MEDIA_EXTENSIONS,
    _METADATA_KINDS,
    _SOURCE_TYPE_MAP,
    _SOURCES,
    _STREAMING_SERVICES,
    _SUBTITLE_KEYWORDS,
    _VIDEO_CODECS,
    parse_resolution_text,
)
# ---------------------------------------------------------------------------
# Result types — typed wrappers for each recognizer's output
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Resolution:
    value: str


@dataclass(frozen=True, slots=True)
class VideoCodec:
    value: str


@dataclass(frozen=True, slots=True)
class AudioCodec:
    value: str


@dataclass(frozen=True, slots=True)
class Source:
    value: str
    source_type: str


@dataclass(frozen=True, slots=True)
class Remux:
    pass


@dataclass(frozen=True, slots=True)
class EpisodeSE:
    season: int
    episode: int
    version: int | None = None


@dataclass(frozen=True, slots=True)
class EpisodeMultiSE:
    """Multi-episode: S01E01-E06, S01E01-06, S01E01E02E03."""

    season: int
    episodes: list[int]


@dataclass(frozen=True, slots=True)
class EpisodeBare:
    episode: int
    version: int | None = None
    is_decimal_special: bool = False  # 01.5 → special between episodes


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
class SeasonSpecial:
    """S01OVA, S02SP1, S03OP, S03ED — combined season + special/credit marker."""

    season: int
    tag: str
    number: int | None = None


@dataclass(frozen=True, slots=True)
class Special:
    tag: str
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
    value: str


@dataclass(frozen=True, slots=True)
class Language:
    value: str


@dataclass(frozen=True, slots=True)
class BonusKeyword:
    bonus_type: str
    raw: str


@dataclass(frozen=True, slots=True)
class SubtitleInfo:
    value: str


@dataclass(frozen=True, slots=True)
class HDRInfo:
    value: str


@dataclass(frozen=True, slots=True)
class BitDepth:
    value: int


@dataclass(frozen=True, slots=True)
class DualAudio:
    pass


@dataclass(frozen=True, slots=True)
class Edition:
    value: str  # "Criterion", "Director's Cut", etc.


@dataclass(frozen=True, slots=True)
class Uncensored:
    pass


@dataclass(frozen=True, slots=True)
class Repack:
    pass


@dataclass(frozen=True, slots=True)
class SitePrefix:
    value: str


# ---------------------------------------------------------------------------
# Parsy primitives — typed recognizers for individual tokens
# ---------------------------------------------------------------------------


def _match_set_ci(vocab: frozenset[str], result_type: type) -> Parser:
    """Build a parser that matches any word in a case-insensitive vocabulary set."""

    def _parser(stream: str, index: int):
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


def _re_group(pattern: str, s: str, group: int = 1, flags: int = 0) -> str:
    """Extract a regex group, asserting the match exists."""
    m = re.match(pattern, s, flags)
    assert m is not None
    return m.group(group)


# Resolution — normalize all formats to standard tags via parse_resolution_text
resolution: Parser = regex(
    r"(?:480|540|576|720|1080)[pi]|2160p|4[kK]", re.IGNORECASE
).map(lambda s: Resolution(parse_resolution_text(s))) | regex(
    r"\d{3,4}x\d{3,4}[pi]?"
).map(lambda s: Resolution(parse_resolution_text(s)))

# Video codec
video_codec: Parser = _match_set_ci(_VIDEO_CODECS, VideoCodec)

# Pre-compiled patterns for parsy recognizer functions (avoid per-call compilation)
# NOTE: decimal version (v2.1) is consumed but only the major int is captured.
# If quality ranking needs the full version, change version to str or float.
_RE_BARE_EP = re.compile(r"(\d{1,4})(?:v(\d+)(?:\.\d+)?)?(?=\s|$|\.)")
_RE_EP_PREFIX = re.compile(r"[Ee][Pp]?(\d{1,4})(?:v(\d+))?(?=\s|$|\.)")
_RE_EP_FINAL = re.compile(r"(\d{1,4})(?:v(\d+))?\s*END$", re.IGNORECASE)
_RE_TRAILING_DIGITS = re.compile(r"(\d+)$")
_RE_BONUS_OP = re.compile(r"OP\d*$", re.IGNORECASE)
_RE_BONUS_ED = re.compile(r"ED\d*$", re.IGNORECASE)
_RE_BATCH_SPLIT = re.compile(r"\s*[~～]\s*")
_RE_WORD_SPLIT = re.compile(r"[\s,]+")
_RE_DASH_SUFFIX = re.compile(r"-[A-Za-z].*$")
_RE_DASH_GROUP = re.compile(r"^(.+)-([A-Za-z][A-Za-z0-9]+)$")

# Audio codec (compound forms like AAC2.0, DTS-HD MA)
_RE_AC = re.compile(
    r"(?:DTS-HDMA|DTS-HD\s*MA|DTS-HD|DTS|DDP|DD\+|DD|EAC3|E-AC-3|AC3|AAC|FLAC|TrueHD|PCM|LPCM)"
    r"(?:[.\s]?\d\.\d)?",
    re.IGNORECASE,
)


_RE_AC_DOT_NORM = re.compile(r"(?<=[a-zA-Z])\.(?=\d)")


def _audio_codec_parser(stream: str, index: int):
    m = _RE_AC.match(stream, index)
    if m:
        # Normalize the separator dot between codec name and channel count:
        # AAC.2.0 → AAC 2.0, TrueHD.5.1 → TrueHD 5.1
        # But preserve AAC2.0 (no separator dot — digit directly follows letters)
        text = _RE_AC_DOT_NORM.sub(" ", m.group(0))
        return Result.success(m.end(), AudioCodec(text))
    end = index
    while end < len(stream) and not stream[end].isspace():
        end += 1
    if end > index:
        word = stream[index:end]
        if word.lower() in _AUDIO_CODECS:
            return Result.success(end, AudioCodec(word))
    return Result.failure(index, frozenset({"audio_codec"}))


audio_codec: Parser = Parser(_audio_codec_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Source
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

# Episode: SxxExx (multi-episode and single)

# Multi-episode: S01E01-E06, S01E01-06, S01E01E02E03
_RE_EP_MULTI_SE = re.compile(
    r"[Ss](\d{1,2})[Ee](\d{1,4})"
    r"(?:"
    r"(?:-[Ee]?(\d{1,4}))"  # S01E01-E06 or S01E01-06 (range)
    r"|"
    r"([Ee]\d{1,4}(?:[Ee]\d{1,4})*)"  # S01E01E02E03 (repeated E)
    r")",
    re.IGNORECASE,
)


def _multi_episode_se_parser(stream: str, index: int):
    m = _RE_EP_MULTI_SE.match(stream, index)
    if not m:
        return Result.failure(index, frozenset({"multi_episode_se"}))
    season = int(m.group(1))
    first = int(m.group(2))
    if m.group(3):
        # Range: S01E01-06 or S01E01-E06
        last = int(m.group(3))
        if last < first or last - first > 100:
            return Result.failure(index, frozenset({"multi_episode_se"}))
        episodes = list(range(first, last + 1))
    else:
        # Repeated: S01E01E02E03
        extra = re.findall(r"[Ee](\d{1,4})", m.group(4))
        episodes = [first] + [int(e) for e in extra]
    return Result.success(m.end(), EpisodeMultiSE(season=season, episodes=episodes))


episode_multi_se: Parser = Parser(_multi_episode_se_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Single episode: S01E05, s1e1, S03E13v2
_RE_EP_SE_P = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?", re.IGNORECASE)


def _parse_episode_se(s: str) -> EpisodeSE:
    m = _RE_EP_SE_P.match(s)
    assert m is not None
    return EpisodeSE(
        season=int(m.group(1)),
        episode=int(m.group(2)),
        version=int(m.group(3)) if m.group(3) else None,
    )


episode_se: Parser = regex(r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?").map(
    _parse_episode_se
)


# Episode: bare number
def _bare_episode_parser(stream: str, index: int):
    m = _RE_BARE_EP.match(stream[index:])
    if not m:
        return Result.failure(index, frozenset({"bare_episode"}))
    num = int(m.group(1))
    if 1900 <= num <= 2099 and m.group(2) is None:
        return Result.failure(index, frozenset({"bare_episode"}))
    version = int(m.group(2)) if m.group(2) else None
    return Result.success(index + m.end(), EpisodeBare(num, version))


# Decimal episode special (01.5) — NOT in _RECOGNIZERS to avoid false positives
# from scan_dot_segments compound matching.
_RE_DECIMAL_EP = re.compile(r"^(\d{1,4})\.(\d{1,2})$")


def _try_decimal_episode(text: str) -> Token | None:
    """Try to parse text as a decimal episode special (01.5)."""
    md = _RE_DECIMAL_EP.match(text)
    if md:
        num = int(md.group(1))
        if not (1900 <= num <= 2099):
            return _result_to_token(EpisodeBare(num, is_decimal_special=True), text)
    return None


episode_bare: Parser = Parser(_bare_episode_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Episode: EP05, E5 format
def _ep_prefix_parser(stream: str, index: int):
    m = _RE_EP_PREFIX.match(stream[index:])
    if not m:
        return Result.failure(index, frozenset({"ep_prefix"}))
    version = int(m.group(2)) if m.group(2) else None
    return Result.success(index + m.end(), EpisodeBare(int(m.group(1)), version))


episode_ep: Parser = Parser(_ep_prefix_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Episode: "05 END", "05v2 END" (final episode marker)
def _ep_final_parser(stream: str, index: int):
    m = _RE_EP_FINAL.match(stream[index:])
    if not m:
        return Result.failure(index, frozenset({"ep_final"}))
    version = int(m.group(2)) if m.group(2) else None
    return Result.success(index + m.end(), EpisodeBare(int(m.group(1)), version))


episode_final: Parser = Parser(_ep_final_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Episode: Japanese 第NN話
episode_jp: Parser = regex(r"第(\d{1,4})話").map(
    lambda s: EpisodeJP(int(_re_group(r"第(\d+)話", s)))
)

# Season: Japanese 第N期
season_jp: Parser = regex(r"第(\d{1,2})期").map(
    lambda s: SeasonJP(int(_re_group(r"第(\d+)期", s)))
)

# Season: English ordinal "4th Season" or "Season 01" (GM-Team format)
_RE_SEASON_WORD = re.compile(
    r"(?:(\d+)(?:st|nd|rd|th)\s+Season|Season\s+(\d{1,2}))", re.IGNORECASE
)


def _season_word_parser(stream: str, index: int):
    m = _RE_SEASON_WORD.match(stream, index)
    if not m:
        return Result.failure(index, frozenset({"season_word"}))
    num = int(m.group(1) or m.group(2))
    return Result.success(m.end(), SeasonWord(num))


season_word: Parser = Parser(_season_word_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Season: bare "S01"
season_only: Parser = regex(r"[Ss](\d{1,2})(?!\d|[Ee])").map(
    lambda s: SeasonOnly(int(_re_group(r"[Ss](\d+)", s)))
)


# Special: SP1, OVA, OAD, ONA, Special
def _parse_special(s: str) -> Special:
    tag = _re_group(r"(SP|OVA|OAD|ONA|Special)", s, flags=re.IGNORECASE)
    # Normalize "Special" → "SP" for consistency
    if tag.lower() == "special":
        tag = "SP"
    else:
        tag = tag.upper()
    m = _RE_TRAILING_DIGITS.search(s)
    return Special(tag=tag, number=int(m.group(1)) if m else None)


special: Parser = regex(r"(Special|SP|OVA|OAD|ONA)(\d*)", re.IGNORECASE).map(
    _parse_special
)

# Season + special: S01OVA, S02SP1
_RE_SEASON_SPECIAL = re.compile(
    r"[Ss](\d{1,2})(OVA|OAD|ONA|SP|OP|ED)(\d*)", re.IGNORECASE
)


def _season_special_parser(stream: str, index: int):
    m = _RE_SEASON_SPECIAL.match(stream[index:])
    if not m:
        return Result.failure(index, frozenset({"season_special"}))
    season = int(m.group(1))
    tag = m.group(2).upper()
    num = int(m.group(3)) if m.group(3) else None
    return Result.success(
        index + m.end(), SeasonSpecial(season=season, tag=tag, number=num)
    )


season_special: Parser = Parser(_season_special_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Batch range: "01~26"
def _parse_batch_range(s: str) -> BatchRange:
    parts = _RE_BATCH_SPLIT.split(s)
    return BatchRange(start=int(parts[0]), end=int(parts[1]))


batch_range: Parser = regex(r"(\d{1,4})\s*[~～]\s*(\d{1,4})").map(_parse_batch_range)

# Version: "v2", "v2.1" (decimal minor version consumed but only major kept)
version: Parser = regex(r"v(\d+)(?:\.\d+)?", re.IGNORECASE).map(
    lambda s: Version(int(_re_group(r"v(\d+)", s, flags=re.IGNORECASE)))
)

# Year — reject pre-1940 (oldest content of interest) and future years
_CURRENT_YEAR = date.today().year
_RE_YEAR_P = re.compile(r"((?:19|20)\d{2})(?!\d)")


def _year_parser(stream: str, index: int):
    m = _RE_YEAR_P.match(stream, index)
    if not m:
        return Result.failure(index, frozenset({"year"}))
    y = int(m.group(1))
    if y < 1940 or y > _CURRENT_YEAR + 1:
        return Result.failure(index, frozenset({"year"}))
    return Result.success(m.end(), Year(y))


year: Parser = Parser(_year_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# CRC32
crc32: Parser = regex(r"[0-9A-Fa-f]{8}(?![0-9A-Fa-f])").map(lambda s: CRC32(s.upper()))

# Language
language: Parser = _match_set_ci(_LANGUAGES, Language)

# Subtitle info
subtitle_info: Parser = _match_set_ci(_SUBTITLE_KEYWORDS, SubtitleInfo)

# HDR
hdr_info: Parser = _match_set_ci(_HDR_KEYWORDS, HDRInfo)

# Repack
repack: Parser = regex(r"REPACK\d?", re.IGNORECASE).map(lambda _: Repack())

# Site prefix (www.example.com)
site_prefix: Parser = regex(r"www\.\w+\.\w+", re.IGNORECASE).map(
    lambda s: SitePrefix(s)
)

# Bit depth: 10bit, 10-Bit, Hi10, Hi10P, 8bit
_RE_BIT_DEPTH = re.compile(r"(?:Hi)?(\d+)[- ]?[Bb]it|Hi(\d+)P?", re.IGNORECASE)


def _bit_depth_parser(stream: str, index: int):
    m = _RE_BIT_DEPTH.match(stream[index:])
    if not m:
        return Result.failure(index, frozenset({"bit_depth"}))
    bits = int(m.group(1) or m.group(2))
    return Result.success(index + m.end(), BitDepth(bits))


bit_depth: Parser = Parser(_bit_depth_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Dual Audio / Dual-Audio / Dual.Audio / DUAL (scene shorthand)
_RE_DUAL_AUDIO = re.compile(r"Dual[- .]?Audio|\bDUAL\b", re.IGNORECASE)


def _dual_audio_parser(stream: str, index: int):
    m = _RE_DUAL_AUDIO.match(stream, index)
    if not m:
        return Result.failure(index, frozenset({"dual_audio"}))
    return Result.success(m.end(), DualAudio())


dual_audio: Parser = Parser(_dual_audio_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Edition markers: Criterion, Director's Cut, etc.
_EDITIONS = frozenset({"criterion", "remastered", "uncut", "theatrical", "extended"})
edition: Parser = _match_set_ci(_EDITIONS, Edition)

# Uncensored marker
uncensored: Parser = regex(r"Uncensored", re.IGNORECASE).map(lambda _: Uncensored())

# Bonus keywords (English)
_RE_BONUS_EN_P = re.compile(
    r"NC\s*(?:OP|ED)\d*|NCOP\d*|NCED\d*|Creditless\s+(?:OP|ED)\d*",
    re.IGNORECASE,
)


def _bonus_parser(stream: str, index: int):
    m = _RE_BONUS_EN_P.match(stream, index)
    if m:
        matched = m.group(0)
        if _RE_BONUS_OP.search(matched):
            return Result.success(m.end(), BonusKeyword("NCOP", matched))
        if _RE_BONUS_ED.search(matched):
            return Result.success(m.end(), BonusKeyword("NCED", matched))
    return Result.failure(index, frozenset({"bonus_keyword"}))


bonus_en: Parser = Parser(_bonus_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

# Japanese bonus keywords — matched as substring within text since
# Japanese bonus content often includes titles: 「ノンテロップED「Title」」
_BONUS_JP_MAP: dict[str, str] = {
    "ノンテロップOP": "NCOP",
    "ノンテロップED": "NCED",
    "ノンテロップ": "NCOP",  # fallback when OP/ED not specified
    "PV": "PV",
    "予告": "Preview",
    "告知CM": "CM",
    "メニュー画面集": "Menu",
    "メニュー画面": "Menu",
    "映像特典": "Bonus",
    "特典": "Bonus",
}
# Sort by length descending so longer matches take priority
_BONUS_JP_KEYS: list[str] = sorted(_BONUS_JP_MAP, key=len, reverse=True)  # ty: ignore[invalid-assignment]


def _bonus_jp_parser(stream: str, index: int):
    for keyword in _BONUS_JP_KEYS:
        if stream[index:].startswith(keyword):
            bonus_type = _BONUS_JP_MAP[keyword]
            # Distinguish NCOP vs NCED when ノンテロップ alone matches
            if keyword == "ノンテロップ":
                rest = stream[index + len(keyword) :]
                if rest.startswith("ED") or rest.startswith("ed"):
                    bonus_type = "NCED"
            return Result.success(
                index + len(keyword), BonusKeyword(bonus_type, keyword)
            )
    return Result.failure(index, frozenset({"bonus_jp"}))


bonus_jp: Parser = Parser(_bonus_jp_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# ---------------------------------------------------------------------------
# Result type → TokenKind mapping
# ---------------------------------------------------------------------------

_TYPE_TO_KIND: dict[type, TokenKind] = {
    Resolution: TokenKind.RESOLUTION,
    VideoCodec: TokenKind.VIDEO_CODEC,
    AudioCodec: TokenKind.AUDIO_CODEC,
    Source: TokenKind.SOURCE,
    Remux: TokenKind.REMUX,
    EpisodeSE: TokenKind.EPISODE,
    EpisodeMultiSE: TokenKind.EPISODE,
    EpisodeBare: TokenKind.EPISODE,
    EpisodeJP: TokenKind.EPISODE,
    SeasonJP: TokenKind.SEASON,
    SeasonWord: TokenKind.SEASON,
    SeasonOnly: TokenKind.SEASON,
    Special: TokenKind.SPECIAL,
    SeasonSpecial: TokenKind.SPECIAL,
    BatchRange: TokenKind.BATCH_RANGE,
    Version: TokenKind.VERSION,
    Year: TokenKind.YEAR,
    CRC32: TokenKind.CRC32,
    Language: TokenKind.LANGUAGE,
    BonusKeyword: TokenKind.BONUS,
    SubtitleInfo: TokenKind.SUBTITLE_INFO,
    HDRInfo: TokenKind.HDR,
    Repack: TokenKind.UNKNOWN,
    SitePrefix: TokenKind.SITE_PREFIX,
    BitDepth: TokenKind.BIT_DEPTH,
    DualAudio: TokenKind.DUAL_AUDIO,
    Edition: TokenKind.EDITION,
    Uncensored: TokenKind.UNCENSORED,
}

# Known redistributor site brackets — not release groups
_REDISTRIBUTORS = frozenset({"tgx", "eztv", "eztvx.to", "rartv", "ettv", "ion10"})

# Words that, when preceding a bare number, indicate the number is part
# of the title (not an episode number): "Part 1", "Vol 2", "Chapter 3"
_TITLE_NUMBER_PREFIXES = frozenset({"part", "vol", "volume", "chapter", "movie"})


def _result_to_token(result: object, text: str) -> Token:
    """Convert a parsy primitive result to a Token for the existing pipeline."""
    kind = _TYPE_TO_KIND.get(type(result), TokenKind.UNKNOWN)
    # Use normalized value from result types that carry it
    if isinstance(result, (AudioCodec, Resolution)):
        text = result.value
    token = Token(kind=kind, text=text)

    # Populate numeric fields
    if isinstance(result, EpisodeMultiSE):
        token.season = result.season
        token.episode = result.episodes[0]
        token.batch_start = result.episodes[0]
        token.batch_end = result.episodes[-1]
    elif isinstance(result, EpisodeSE):
        token.season = result.season
        token.episode = result.episode
        token.version = result.version
    elif isinstance(result, EpisodeBare):
        token.episode = result.episode
        token.version = result.version
        token.is_decimal_special = result.is_decimal_special
    elif isinstance(result, EpisodeJP):
        token.episode = result.episode
    elif isinstance(result, SeasonJP):
        token.season = result.season
    elif isinstance(result, SeasonWord):
        token.season = result.season
    elif isinstance(result, SeasonOnly):
        token.season = result.season
    elif isinstance(result, SeasonSpecial):
        token.season = result.season
        token.episode = result.number
    elif isinstance(result, Special):
        token.episode = result.number
    elif isinstance(result, Version):
        token.version = result.number
    elif isinstance(result, Year):
        token.year = result.value
    elif isinstance(result, BatchRange):
        token.batch_start = result.start
        token.batch_end = result.end

    return token


# ---------------------------------------------------------------------------
# Recognizer list — ordered by specificity (most specific first)
# ---------------------------------------------------------------------------

# Each recognizer is tried at the current position. The first (longest) match
# wins. This ordering ensures compound tokens like AAC2.0 are matched before
# AAC, and SxxExx before S-only season markers.

_RECOGNIZERS: list[Parser] = [
    # Episode markers (most distinctive)
    episode_multi_se,  # S01E01-E06, S01E01-06, S01E01E02E03
    episode_se,  # S01E05, s1e1, S03E13v2
    episode_jp,  # 第01話
    batch_range,  # 01~26
    season_special,  # S01OVA, S02SP1
    special,  # SP1, OVA, OAD, ONA
    season_jp,  # 第1期
    season_word,  # 4th Season
    season_only,  # S01 (after episode_se to avoid S01E05 → S01)
    episode_final,  # 05 END, 05v2 END (before bare to match END suffix)
    episode_ep,  # EP05, E5
    episode_bare,  # 08, 12v2 (after season_only to avoid S01 → ep 1)
    # Bonus keywords
    bonus_en,  # NCOP, NC OP1, Creditless ED
    bonus_jp,  # 映像特典, ノンテロップOP, PV, 予告
    # Technical metadata (ordered: compound before simple)
    audio_codec,  # AAC2.0, DTS-HD MA, FLAC — compound first
    resolution,  # 1080p, 1920x1080
    video_codec,  # HEVC, x265, H.264
    source,  # BluRay, WEB-DL, BD
    remux,  # REMUX
    # Identifiers
    crc32,  # ABCD1234
    year,  # 2019 (after episode to avoid episode false positives)
    version,  # v2, v3
    # Context (dual_audio before language — "DUAL" must match as dual-audio, not language)
    dual_audio,  # Dual Audio, Dual-Audio, DUAL
    language,  # jpn, eng, dual
    subtitle_info,  # multisub, msubs
    hdr_info,  # HDR, HDR10, DoVi
    bit_depth,  # 10bit, 10-Bit, Hi10
    uncensored,  # Uncensored
    edition,  # Criterion, Remastered
    repack,  # REPACK, REPACK2
    site_prefix,  # www.example.com
]


# ---------------------------------------------------------------------------
# Position-based scanner
# ---------------------------------------------------------------------------


def scan_words(text: str) -> list[Token]:
    """Scan a space/comma-separated text for known tokens.

    Tries each recognizer at each word boundary, longest match first.
    Unrecognized words become TEXT tokens.  This replaces regex-based
    classification for content within brackets, parens, and bare text
    segments.

    Returns tokens compatible with the existing classify/assembly pipeline.
    """
    tokens: list[Token] = []

    # First pass: try multi-word recognizers on the full text before splitting.
    # This handles patterns like "NC ED1", "Creditless OP", "4th Season",
    # "DTS-HD MA" that span whitespace.
    remaining = text
    pre_tokens: list[tuple[int, int, Token]] = []  # (start, end, token)
    for recognizer in _RECOGNIZERS:
        pos = 0
        while pos < len(remaining):
            result = recognizer(remaining, pos)
            if result.status and result.index > pos:
                # Check it's on a word boundary
                at_start = pos == 0 or remaining[pos - 1] in " ,\t-"
                at_end = (
                    result.index == len(remaining) or remaining[result.index] in " ,\t-"
                )
                if at_start and at_end:
                    pre_tokens.append(
                        (
                            pos,
                            result.index,
                            _result_to_token(
                                result.value, remaining[pos : result.index]
                            ),
                        )
                    )
                    pos = result.index
                    continue
            pos += 1

    # Sort by position, longest match first at each position
    pre_tokens.sort(key=lambda x: (x[0], -(x[1] - x[0])))
    used: list[tuple[int, int, Token]] = []
    last_end = 0
    for start, end, token in pre_tokens:
        if start >= last_end:
            used.append((start, end, token))
            last_end = end

    # Build token list: recognized spans + unrecognized gaps
    def _emit_gap(gap_text: str) -> None:
        """Classify gap words, with dash-compound splitting."""
        # If gap starts with a dash (immediately after a recognized token),
        # the trailing word is likely a release group (e.g., FLAC-TTGA → TTGA)
        starts_with_dash = gap_text.lstrip(" ,").startswith("-")
        gap_text = gap_text.strip(" ,-")
        if not gap_text:
            return
        for w in _RE_WORD_SPLIT.split(gap_text):
            w = w.strip()
            if not w:
                continue
            dec_token = _try_decimal_episode(w)
            if dec_token is not None:
                tokens.append(dec_token)
                continue
            token = _try_recognize(w)
            if token is not None:
                tokens.append(token)
            elif starts_with_dash and tokens:
                # Word after a dash that followed a recognized token — release group
                tokens.append(Token(kind=TokenKind.RELEASE_GROUP, text=w))
                starts_with_dash = False
            elif "-" in w and not w.startswith("-"):
                sub_parts = w.split("-")
                sub_tokens: list[Token] = []
                for sp in sub_parts:
                    sp = sp.strip()
                    if not sp:
                        continue
                    st = _try_recognize(sp)
                    if st is not None:
                        sub_tokens.append(st)
                    else:
                        sub_tokens.append(Token(kind=TokenKind.UNKNOWN, text=sp))
                # Scene convention: last unclassified part after metadata
                # is the release group (e.g., "REMUX-FraMeSToR")
                has_meta = any(t.kind != TokenKind.UNKNOWN for t in sub_tokens)
                if has_meta and sub_tokens and sub_tokens[-1].kind == TokenKind.UNKNOWN:
                    sub_tokens[-1] = Token(
                        kind=TokenKind.RELEASE_GROUP,
                        text=sub_tokens[-1].text,
                    )
                tokens.extend(sub_tokens)
            else:
                tokens.append(Token(kind=TokenKind.UNKNOWN, text=w))

    pos = 0
    for start, end, token in used:
        _emit_gap(text[pos:start])
        tokens.append(token)
        pos = end
    _emit_gap(text[pos:])

    return tokens


def _try_compound_dash_strip(prefix: str, last_part: str) -> list[Token] | None:
    """Try recognizing a compound after stripping a trailing -suffix.

    Given a prefix (e.g., "H" or "TrueHD.5") and a last_part with a dash
    suffix (e.g., "264-VARYG" or "1-Hinna"), strips the suffix from
    last_part, tries recognizing prefix.base as a compound, and if
    successful returns the compound token + suffix token(s).
    """
    base = _RE_DASH_SUFFIX.sub("", last_part)
    if base == last_part:
        return None
    compound = f"{prefix}.{base}"
    token = _try_recognize(compound)
    if token is None:
        return None
    result = [token]
    suffix = last_part[len(base) + 1 :]
    if suffix:
        suffix_token = _try_recognize(suffix)
        if suffix_token is not None:
            result.append(suffix_token)
        else:
            result.append(Token(kind=TokenKind.RELEASE_GROUP, text=suffix))
    return result


def scan_dot_segments(text: str) -> list[Token]:
    """Scan dot-separated scene-style text.

    Instead of the placeholder approach for compound tokens, this scanner
    tries recognizers at each position in the dot-split stream.  When a
    recognizer matches across a dot boundary (e.g. 'H' + '264' → H.264),
    the segments are rejoined.

    Returns DOT_TEXT tokens for unrecognized segments and typed tokens for
    recognized metadata.
    """
    raw_parts = text.split(".")
    tokens: list[Token] = []
    i = 0

    while i < len(raw_parts):
        part = raw_parts[i]

        if not part:
            i += 1
            continue

        # Try multi-segment compounds first (longest match)
        # Check 3-segment: "MA.5.1", "FLAC.2.0", "TrueHD.5.1"
        if i + 2 < len(raw_parts):
            third = raw_parts[i + 2]
            compound3 = f"{part}.{raw_parts[i + 1]}.{third}"
            token = _try_recognize(compound3)
            if token is not None:
                tokens.append(token)
                i += 3
                continue
            # Strip trailing -suffix from third part: "TrueHD.5.1-Hinna"
            if "-" in third:
                stripped_tokens = _try_compound_dash_strip(
                    f"{part}.{raw_parts[i + 1]}", third
                )
                if stripped_tokens is not None:
                    tokens.extend(stripped_tokens)
                    i += 3
                    continue

        # Check 2-segment: "H.264", "AAC2.0"
        if i + 1 < len(raw_parts):
            next_part = raw_parts[i + 1]
            compound2 = f"{part}.{next_part}"
            token = _try_recognize(compound2)
            if token is not None:
                tokens.append(token)
                i += 2
                continue

            # Strip trailing -suffix and try: "H" + "264-VARYG" → "H.264"
            if "-" in next_part:
                stripped_tokens = _try_compound_dash_strip(part, next_part)
                if stripped_tokens is not None:
                    tokens.extend(stripped_tokens)
                    i += 2
                    continue

        # Single segment
        token = _try_recognize(part)
        if token is not None:
            tokens.append(token)
        else:
            # Check if it has a trailing "-GROUP" (scene convention)
            dash_m = _RE_DASH_GROUP.match(part)
            if dash_m:
                prefix = dash_m.group(1)
                suffix = dash_m.group(2)
                prefix_token = _try_recognize(prefix)
                if prefix_token is not None:
                    tokens.append(prefix_token)
                    tokens.append(Token(kind=TokenKind.RELEASE_GROUP, text=suffix))
                else:
                    tokens.append(Token(kind=TokenKind.DOT_TEXT, text=part))
            else:
                tokens.append(Token(kind=TokenKind.DOT_TEXT, text=part))

        i += 1

    return tokens


def _find_recognizer_in_text(
    text: str, recognizers: tuple[Parser, ...]
) -> tuple[int, int, Token] | None:
    """Search for the first recognizer match at any position in text.

    Tries each recognizer at each position, returning the first match.
    Returns ``(start, end, token)`` or None.
    """
    for pos in range(len(text)):
        # Only match at word boundaries (start of string or after space/separator)
        if pos > 0 and text[pos - 1] not in " \t-_":
            continue
        for recognizer in recognizers:
            result = recognizer(text, pos)
            if result.status:
                # End boundary: reject letter→letter continuation (mid-word).
                # "Sp" in "Spring" is rejected (p→r, both letters), but
                # "OVA2" before "E03" is allowed (2→E, digit→letter).
                end = result.index
                if (
                    end < len(text)
                    and end > pos
                    and text[end - 1].isalpha()
                    and text[end].isascii()
                    and text[end].isalpha()
                ):
                    continue
                return (
                    pos,
                    result.index,
                    _result_to_token(result.value, text[pos : result.index]),
                )
    return None


# Recognizers safe for finding markers embedded *within* text.
# Only recognizers with distinctive prefixes (S##E##, 第N話, etc.) — bare
# numbers and patterns that match too broadly (episode_bare, season_word)
# are excluded to avoid false positives on title content.
_EMBEDDED_RECOGNIZERS = (
    episode_multi_se,  # S01E01-E06
    episode_se,  # S01E05
    season_special,  # S01OVA, S03OP
    special,  # OVA, SP1, OAD, ONA
    episode_jp,  # 第01話
    season_jp,  # 第1期
    season_word,  # 4th Season, Season 01
    episode_ep,  # EP05, E5
)


def find_episode_in_text(text: str) -> tuple[int, int, Token] | None:
    """Search for an episode/season+special marker at any position in text.

    Returns (start, end, token) or None.
    """
    return _find_recognizer_in_text(text, _EMBEDDED_RECOGNIZERS)


def _try_recognize(text: str) -> Token | None:
    """Try all recognizers against a complete text. Returns Token or None."""
    for recognizer in _RECOGNIZERS:
        result = recognizer(text, 0)
        if result.status and result.index == len(text):
            return _result_to_token(result.value, text)
    return None


# ---------------------------------------------------------------------------
# Public API — replacements for media_parser classification functions
# ---------------------------------------------------------------------------


def classify_text(text: str) -> TokenKind | None:
    """Classify a bare text string as a known metadata type.

    Drop-in replacement for media_parser._classify_text_content, using
    parsy recognizers instead of manual regex/set checks.
    """
    token = _try_recognize(text.strip())
    return token.kind if token is not None else None


def is_metadata_word(word: str) -> bool:
    """Check if a word (or any of its dash-separated parts) is metadata.

    Drop-in replacement for media_parser._is_metadata_word.
    """
    if classify_text(word) is not None:
        return True
    if "-" in word:
        return any(classify_text(sp) is not None for sp in word.split("-") if sp)
    return False


def count_metadata_words(text: str) -> int:
    """Count how many whitespace/comma-separated words classify as metadata.

    Drop-in replacement for media_parser._count_metadata_words.
    """
    count = 0
    for w in _RE_WORD_SPLIT.split(text):
        w = w.strip()
        if w and is_metadata_word(w):
            count += 1
    return count


# ---------------------------------------------------------------------------
# Structural tokenizer
# ---------------------------------------------------------------------------


def _strip_extension(text: str) -> tuple[str, Token | None]:
    """Strip a known file extension from the end of text.

    Returns (text_without_ext, extension_token_or_None).
    """
    lower = text.lower()
    for ext in _ALL_EXTENSIONS_SORTED:
        if lower.endswith(ext):
            return text[: -len(ext)], Token(kind=TokenKind.EXTENSION, text=ext)
    return text, None


def _is_scene_style(text: str) -> bool:
    """Detect if text uses dot-separated scene naming (2+ dots, no spaces)."""
    return text.count(".") >= 2 and " " not in text


def _split_separators(text: str) -> list[Token]:
    """Split text on ' - ' or '_-_' separators, producing TEXT and SEPARATOR tokens.

    Handles both space-dash-space and underscore-dash-underscore (old fansub
    convention). Does not split on bare dashes in words.
    """
    parts = re.split(r" - |_-_", text)
    tokens: list[Token] = []
    for i, part in enumerate(parts):
        if i > 0:
            tokens.append(Token(kind=TokenKind.SEPARATOR, text=" - "))
        stripped = part.strip().strip("_")
        if stripped:
            tokens.append(Token(kind=TokenKind.TEXT, text=stripped))
    return tokens


def tokenize_component(text: str) -> list[Token]:
    """Tokenize a single path component (directory name or filename).

    Handles brackets [], parens () with nesting, lenticular quotes 「」,
    and detects scene-style dot-separated names.
    """
    # Strip extension first if this looks like a filename
    ext_token: Token | None = None
    text, ext_token = _strip_extension(text)

    tokens: list[Token] = []
    i = 0
    buf: list[str] = []  # accumulates bare text between delimiters

    def _flush_buf() -> None:
        """Flush accumulated bare text as TEXT or DOT_TEXT tokens."""
        if not buf:
            return
        raw = "".join(buf).strip()
        buf.clear()
        if not raw:
            return
        if _is_scene_style(raw):
            tokens.extend(scan_dot_segments(raw))
        else:
            tokens.extend(_split_separators(raw))

    while i < len(text):
        ch = text[i]

        # Square brackets: [content]
        if ch == "[":
            _flush_buf()
            depth = 1
            start = i + 1
            i += 1
            while i < len(text) and depth > 0:
                if text[i] == "[":
                    depth += 1
                elif text[i] == "]":
                    depth -= 1
                i += 1
            content = text[start : i - 1] if depth == 0 else text[start:]
            tokens.append(Token(kind=TokenKind.BRACKET, text=content))
            continue

        # Parentheses: (content) with nesting
        if ch == "(":
            _flush_buf()
            depth = 1
            start = i + 1
            i += 1
            while i < len(text) and depth > 0:
                if text[i] == "(":
                    depth += 1
                elif text[i] == ")":
                    depth -= 1
                i += 1
            content = text[start : i - 1] if depth == 0 else text[start:]
            tokens.append(Token(kind=TokenKind.PAREN, text=content))
            continue

        # Lenticular quotes: 「content」
        if ch == "\u300c":  # 「
            _flush_buf()
            start = i + 1
            i += 1
            # Find matching 」, handling nested 「」
            depth = 1
            while i < len(text) and depth > 0:
                if text[i] == "\u300c":
                    depth += 1
                elif text[i] == "\u300d":
                    depth -= 1
                i += 1
            content = text[start : i - 1] if depth == 0 else text[start:]
            tokens.append(Token(kind=TokenKind.LENTICULAR, text=content))
            continue

        # Accumulate bare text
        buf.append(ch)
        i += 1

    _flush_buf()

    if ext_token:
        tokens.append(ext_token)

    return tokens


def tokenize(path: str) -> list[Token]:
    """Tokenize a relative path into structural tokens.

    Splits on '/' first, tokenizes each component, inserts PATH_SEP between.
    """
    components = path.split("/")
    tokens: list[Token] = []
    for i, comp in enumerate(components):
        if i > 0:
            tokens.append(Token(kind=TokenKind.PATH_SEP, text="/"))
        comp = comp.strip()
        if comp:
            tokens.extend(tokenize_component(comp))
    return tokens


# ---------------------------------------------------------------------------
# Phase 2: Semantic classifier
# ---------------------------------------------------------------------------

_RE_SUBTITLE_SOFT = re.compile(r"softsub", re.IGNORECASE)
# Split letter→digit before resolution patterns in underscored parens: BD1080p → BD 1080p
_RE_PAREN_RES_SPLIT = re.compile(r"([a-zA-Z])(\d+p\b)")

_RE_CRC32 = re.compile(r"^[0-9A-Fa-f]{8}$")

_RE_YEAR = re.compile(r"^(19[4-9]\d|20[0-9]{2})$")  # 1940-2099 structural match

# Release group detection
_RE_SCENE_TRAILING_GROUP = re.compile(r"^(.*)-([A-Za-z][A-Za-z0-9]{1,})$")


def classify_bonus_type(text: str) -> str:
    """Extract a bonus type abbreviation from bonus content text.

    Uses the bonus_jp and bonus_en recognizers to classify Japanese
    keywords (映像特典, ノンテロップOP) and English keywords (NCOP, NCED).
    """
    for pos in range(len(text)):
        for recognizer in (bonus_jp, bonus_en):
            r = recognizer(text, pos)
            if r.status and isinstance(r.value, BonusKeyword):
                return r.value.bonus_type
    return ""


def _classify_episode_text(text: str) -> Token | None:
    """Try to parse text as an episode/season identifier.

    Delegates to the scanner's parsy-based recognizers.
    """
    text = text.strip()

    dec_token = _try_decimal_episode(text)
    if dec_token is not None:
        return dec_token

    token = _try_recognize(text)
    if token is not None and token.kind in (
        TokenKind.EPISODE,
        TokenKind.SPECIAL,
        TokenKind.SEASON,
        TokenKind.BATCH_RANGE,
    ):
        return token
    return None


def _split_text_with_embedded(token: Token) -> list[Token]:
    """Split a TEXT token that contains embedded episode/season markers.

    Uses the same recognizers as ``find_episode_in_text`` to find the first
    embedded marker, then splits into before/match/after tokens.

    E.g. ``"探偵オペラ 第01話"`` → ``[TEXT("探偵オペラ"), EPISODE("第01話")]``
    E.g. ``"Golden Kamuy 4th Season"`` → ``[TEXT("Golden Kamuy"), SEASON("4th Season")]``
    """
    text = token.text
    match = _find_recognizer_in_text(text, _EMBEDDED_RECOGNIZERS)

    # Fallback: bare number at a word boundary ("01 Title" or "Title 09v2.1").
    # episode_bare is too greedy for general embedded search, but a
    # number at a word boundary followed by a space or end is a strong signal.
    if match is None:
        for pos in range(len(text)):
            if pos > 0 and text[pos - 1] != " ":
                continue
            if not text[pos].isdigit():
                continue
            # Skip bare numbers after title-context words (Part 1, Vol 2)
            if pos > 0:
                preceding = text[:pos].rstrip().rsplit(None, 1)
                if preceding and preceding[-1].lower() in _TITLE_NUMBER_PREFIXES:
                    continue
            bare_result = _bare_episode_parser(text, pos)
            if bare_result.status:
                at_end = bare_result.index >= len(text)
                at_space = (
                    bare_result.index < len(text) and text[bare_result.index] == " "
                )
                if at_end or at_space:
                    ep_val = bare_result.value
                    if not (1940 <= (ep_val.episode or 0) <= 2099):
                        match = (
                            pos,
                            bare_result.index,
                            _result_to_token(ep_val, text[pos : bare_result.index]),
                        )
                        break

    if match is None:
        return [token]

    start, end, matched_token = match
    before = text[:start].strip()
    after = text[end:].lstrip(" -")

    # Strip Japanese final-episode marker 「(終)」/「（終）」 after 第XX話
    if after.startswith("(終)") or after.startswith("（終）"):
        after = after[3:].strip()

    result: list[Token] = []
    if before:
        result.append(Token(kind=TokenKind.TEXT, text=before))
    result.append(matched_token)
    if after:
        result.append(Token(kind=TokenKind.TEXT, text=after))
    return result


def _classify_paren(token: Token) -> Token | list[Token]:
    """Classify a PAREN token by its content.

    Returns a single Token for simple cases, or a list of Tokens when
    the paren contains technical metadata that should be expanded.
    """
    text = token.text.strip()

    # Try scanner recognizers for year, season, special, etc.
    recognized = _try_recognize(text)
    if recognized is not None and recognized.kind in (
        TokenKind.YEAR,
        TokenKind.SEASON,
        TokenKind.EPISODE,
        TokenKind.SPECIAL,
        TokenKind.DUAL_AUDIO,
        TokenKind.UNCENSORED,
        TokenKind.EDITION,
    ):
        return recognized

    # Subtitle info: softSub(chi+eng)
    if _RE_SUBTITLE_SOFT.search(text):
        return Token(kind=TokenKind.SUBTITLE_INFO, text=text)

    # Old fansub convention: underscores as separators (10bit_BD1080p_x265)
    if "_" in text:
        normalized = text.replace("_", " ")
        normalized = _RE_PAREN_RES_SPLIT.sub(r"\1 \2", normalized)
    else:
        normalized = text
    if count_metadata_words(normalized) >= 2:
        return scan_words(normalized)

    # Region/variant: (US), (JP)
    if len(text) == 2 and text.isalpha():
        return Token(kind=TokenKind.LANGUAGE, text=text)

    return token


def classify(tokens: list[Token]) -> list[Token]:
    """Classify structural tokens into semantic types.

    Reclassifies BRACKET, PAREN, TEXT, and DOT_TEXT tokens based on content.
    """
    result: list[Token] = []
    first_bracket_seen = False
    seen_episode = False

    for i, token in enumerate(tokens):
        if token.kind in (TokenKind.EPISODE, TokenKind.SPECIAL):
            seen_episode = True

        if token.kind == TokenKind.EXTENSION:
            result.append(token)
            continue

        if token.kind == TokenKind.PATH_SEP:
            result.append(token)
            first_bracket_seen = False  # Reset for new component
            seen_episode = False
            continue

        if token.kind == TokenKind.SEPARATOR:
            result.append(token)
            continue

        if token.kind == TokenKind.BRACKET:
            # CRC32?
            if _RE_CRC32.match(token.text.strip()):
                result.append(Token(kind=TokenKind.CRC32, text=token.text.strip()))
                continue

            # Check if bracket content is metadata keywords
            lower = token.text.lower()
            if lower in _SUBTITLE_KEYWORDS or "subtitle" in lower:
                result.append(Token(kind=TokenKind.SUBTITLE_INFO, text=token.text))
                continue

            # Check for dot-separated metadata in brackets: [x264.AAC]
            if "." in token.text and " " not in token.text.strip():
                dot_tokens = scan_dot_segments(token.text.strip())
                dot_meta = sum(
                    1
                    for t in dot_tokens
                    if t.kind not in (TokenKind.DOT_TEXT, TokenKind.TEXT)
                )
                if dot_meta > 0:
                    result.extend(dot_tokens)
                    first_bracket_seen = True
                    continue

            words = _RE_WORD_SPLIT.split(token.text)
            meta_count = count_metadata_words(token.text)
            is_metadata_bracket = meta_count > 0 and meta_count >= len(words) // 2

            # First bracket: release group unless it's metadata or a
            # known redistributor site (TGx, EZTV, etc.)
            if not first_bracket_seen:
                first_bracket_seen = True
                is_redistributor = token.text.strip().lower() in _REDISTRIBUTORS
                if not is_metadata_bracket and not is_redistributor:
                    result.append(
                        Token(
                            kind=TokenKind.RELEASE_GROUP,
                            text=token.text,
                        )
                    )
                    continue

            # Metadata bracket: expand into individual classified tokens
            # If the first word is not a metadata keyword, treat it as
            # a release group (Sonarr-style: [GROUP QUALITY-res,...])
            if is_metadata_bracket:
                first_word = words[0].strip() if words else ""
                first_is_meta = bool(first_word and classify_text(first_word))
                if not first_is_meta and first_word:
                    first_sub_meta = any(
                        classify_text(sp.strip())
                        for sp in first_word.split("-")
                        if sp.strip()
                    )
                    if not first_sub_meta:
                        result.append(
                            Token(
                                kind=TokenKind.RELEASE_GROUP,
                                text=first_word,
                            )
                        )
                        # Expand remaining words only
                        rest = token.text[len(first_word) :].strip(" ,")
                        result.extend(scan_words(rest))
                        continue

                result.extend(scan_words(token.text))
                continue

            # Short alpha-only bracket (2-6 chars): likely a release group
            # (unless it's a known redistributor)
            stripped = token.text.strip()
            if (
                2 <= len(stripped) <= 6
                and stripped.isalpha()
                and stripped.lower() not in _REDISTRIBUTORS
            ):
                result.append(
                    Token(
                        kind=TokenKind.RELEASE_GROUP,
                        text=stripped,
                    )
                )
                continue

            result.append(token)
            continue

        if token.kind == TokenKind.PAREN:
            paren_result = _classify_paren(token)
            if isinstance(paren_result, Token):
                result.append(paren_result)
            else:
                result.extend(paren_result)
            continue

        if token.kind == TokenKind.LENTICULAR:
            # 「content」 is episode title, or bonus content
            bonus_match = _find_recognizer_in_text(token.text, (bonus_jp, bonus_en))
            if bonus_match is not None:
                _, _, bonus_token = bonus_match
                # Use the BonusKeyword's bonus_type but keep the full text
                result.append(Token(kind=TokenKind.BONUS, text=token.text))
            else:
                result.append(Token(kind=TokenKind.EPISODE_TITLE, text=token.text))
            continue

        # TEXT and DOT_TEXT: try to classify content
        if token.kind in (TokenKind.TEXT, TokenKind.DOT_TEXT):
            text = token.text.strip()

            # Episode/season (full match)?
            ep_token = _classify_episode_text(text)
            if ep_token is not None:
                result.append(ep_token)
                continue

            # Year?
            if _RE_YEAR.match(text):
                y = int(text)
                if 1940 <= y <= _CURRENT_YEAR + 1:
                    result.append(Token(kind=TokenKind.YEAR, text=text, year=y))
                    continue

            # Known metadata keyword? Use the full token from _try_recognize
            # to preserve numeric fields (version, season, episode, etc.)
            recognized_meta = _try_recognize(text)
            if recognized_meta is not None:
                result.append(recognized_meta)
                continue

            # Try splitting TEXT with embedded episode/season markers
            if token.kind == TokenKind.TEXT:
                split = _split_text_with_embedded(token)
                if len(split) > 1:
                    # Re-classify the split tokens
                    result.extend(classify(split))
                    continue

            # Split TEXT with trailing metadata words (e.g., scene-style
            # episode title followed by resolution/codec/source keywords).
            # Scan left-to-right: once metadata starts, everything after is
            # metadata (scene filenames don't interleave title and metadata).
            if token.kind == TokenKind.TEXT and seen_episode:
                words = text.split()
                meta_start = None
                for i, w in enumerate(words):
                    if is_metadata_word(w):
                        # A bare number after Part/Vol/Chapter is a title
                        # fragment, not metadata (e.g., "Part 1", "Vol 2")
                        if i > 0 and words[i - 1].lower() in _TITLE_NUMBER_PREFIXES:
                            continue
                        meta_start = i
                        break
                if meta_start is not None and meta_start > 0:
                    title_part = " ".join(words[:meta_start]).strip()
                    meta_part = " ".join(words[meta_start:])
                    if title_part:
                        result.append(Token(kind=TokenKind.TEXT, text=title_part))
                    result.extend(scan_words(meta_part))
                    continue

            # DOT_TEXT with embedded SxxExx (e.g., "S01E05----Is")
            if token.kind == TokenKind.DOT_TEXT:
                ep_match = find_episode_in_text(text)
                if ep_match:
                    _, _, ep_token = ep_match
                    result.append(ep_token)
                    if ep_token.kind in (TokenKind.EPISODE, TokenKind.SPECIAL):
                        seen_episode = True
                    continue

            # Scene trailing group: "H.264-VARYG" -> split into codec + group
            # Only split when the prefix is a recognized metadata keyword,
            # otherwise keep as-is (e.g. "Cherry-Pick" is a hyphenated title)
            if token.kind == TokenKind.DOT_TEXT:
                m = _RE_SCENE_TRAILING_GROUP.match(text)
                if m:
                    prefix = m.group(1)
                    group = m.group(2)
                    prefix_kind = classify_text(prefix)
                    if prefix_kind:
                        result.append(Token(kind=prefix_kind, text=prefix))
                        result.append(Token(kind=TokenKind.RELEASE_GROUP, text=group))
                        continue

            # Bonus content in text
            if _find_recognizer_in_text(text, (bonus_jp, bonus_en)):
                result.append(Token(kind=TokenKind.BONUS, text=text))
                continue

            # Short trailing DOT_TEXT after metadata = likely release group
            # (scene convention: "Title.S01E01.1080p.BluRay.x265.group.mkv"
            #  or movie: "Title.2022.1080p.BluRay.group.mkv")
            if (
                token.kind == TokenKind.DOT_TEXT
                and 2 <= len(text) <= 8
                and text.isalnum()
                and any(r.kind in _METADATA_KINDS for r in result)
            ):
                remaining = tokens[i + 1 :]
                if all(
                    t.kind in (TokenKind.EXTENSION, TokenKind.PATH_SEP)
                    for t in remaining
                ):
                    result.append(Token(kind=TokenKind.RELEASE_GROUP, text=text))
                    continue

            # Keep as-is
            result.append(token)
            continue

        result.append(token)

    return result


# ---------------------------------------------------------------------------
# Phase 3: Title extraction and ParsedMedia assembly
# ---------------------------------------------------------------------------


@dataclass
class ParsedMedia:
    """Result of parsing a media file path."""

    series_name: str = ""
    series_name_alt: str = ""  # Alternate-language title (CJK/Latin split on / or |)
    episode_title: str = ""
    season: int | None = None
    episode: int | None = None
    episodes: list[int] = field(default_factory=list)
    version: int | None = None
    is_special: bool = False
    special_tag: str = ""
    bonus_type: str = ""  # "NCOP", "NCED", "PV", "CM", "Preview", "Menu", "Bonus"
    batch_range: tuple[int, int] | None = None
    release_group: str = ""
    source_type: str = ""  # "BD", "Web", "DVD", "HDTV", "SDTV", "VCD", "CD-R"
    streaming_service: str = ""  # "AMZN", "CR", "NF", "DSNP", etc.
    is_remux: bool = False
    is_dual_audio: bool = False
    is_criterion: bool = False
    is_uncensored: bool = False
    bit_depth: int | None = None  # 8, 10, etc.
    hdr: str = ""  # "HDR", "HDR10", "HDR10+", "DoVi", "DV", "UHD"
    hash_code: str = ""
    resolution: str = ""
    video_codec: str = ""
    audio_codecs: list[str] = field(default_factory=list)
    year: int | None = None
    extension: str = ""
    # From directory component
    path_series_name: str = ""
    path_is_batch: bool = False


_RE_TITLE_SPLIT = re.compile(r"\s*[/|]\s*")
_RE_CJK_CHAR = re.compile(r"[\u3000-\u9fff\uff00-\uffef]")


def _has_cjk(text: str) -> bool:
    return bool(_RE_CJK_CHAR.search(text))


def _extract_title_from_tokens(tokens: list[Token]) -> tuple[str, str, str]:
    """Extract series name, alt title, and episode title from classified tokens.

    Returns (series_name, series_name_alt, episode_title).

    Strategy: collect TEXT/DOT_TEXT tokens that appear in the "title zone"
    (before the first episode marker or metadata-only region).
    After the episode marker, TEXT tokens become episode title (for scene style).

    If the series name contains a ``/`` or ``|`` separator (common in
    CJK/Latin bilingual releases), splits into primary and alt titles.
    """
    series_parts: list[str] = []
    ep_title_parts: list[str] = []
    seen_episode = False
    seen_metadata = False  # True after first metadata token post-episode

    for token in tokens:
        if token.kind in (TokenKind.PATH_SEP, TokenKind.EXTENSION):
            continue

        if token.kind == TokenKind.EPISODE_TITLE:
            ep_title_parts.append(token.text)
            continue

        if token.kind in (TokenKind.EPISODE, TokenKind.SPECIAL):
            seen_episode = True
            continue

        if token.kind == TokenKind.SEPARATOR:
            # Separators before episode are part of title structure,
            # after episode they separate metadata
            continue

        if token.kind in _METADATA_KINDS:
            if seen_episode:
                seen_metadata = True
            continue

        # TEXT or DOT_TEXT
        if token.kind in (TokenKind.TEXT, TokenKind.DOT_TEXT):
            if not seen_episode:
                series_parts.append(token.text)
            elif not seen_metadata:
                # After episode, before metadata = episode title
                ep_title_parts.append(token.text)
            # After metadata starts, ignore remaining text (metadata residue)
            continue

        # PAREN, BRACKET that weren't reclassified — could be title-adjacent
        if token.kind == TokenKind.PAREN and not seen_episode:
            # Parens in title zone that weren't classified as metadata
            # might be part of the title (e.g., "(Digital)")
            series_parts.append(f"({token.text})")
            continue
        if token.kind == TokenKind.BRACKET and not seen_episode:
            series_parts.append(f"[{token.text}]")
            continue

    # Join title parts
    # For scene-style (DOT_TEXT), join with spaces instead of dots
    series_name = " ".join(series_parts).strip()
    # Clean up artifacts: trailing dashes, extra spaces
    series_name = re.sub(r"\s*-\s*$", "", series_name)
    series_name = re.sub(r"\s+", " ", series_name)

    # Split bilingual titles on / or | (common in CJK/Latin fansub releases).
    # Only split on / when one side contains CJK characters (avoids breaking
    # titles like "Fate/stay night"). Pipe with surrounding spaces is always
    # treated as a bilingual separator.
    series_name_alt = ""
    m = _RE_TITLE_SPLIT.search(series_name)
    if m:
        left = series_name[: m.start()].strip()
        right = series_name[m.end() :].strip()
        sep = m.group(0).strip()
        if sep == "|" or (_has_cjk(left) != _has_cjk(right)):
            series_name = left
            series_name_alt = right

    episode_title = " ".join(ep_title_parts).strip()
    episode_title = re.sub(r"\s+", " ", episode_title)

    return series_name, series_name_alt, episode_title


def _check_special(token: Token, pm: ParsedMedia) -> None:
    """Check if an EPISODE token represents a special episode and update pm.

    Only handles season-0 and decimal specials — typed Special/SeasonSpecial
    results are now routed through TokenKind.SPECIAL instead.
    """
    if token.season == 0:
        pm.is_special = True
    if token.is_decimal_special:
        pm.is_special = True


def _build_parsed_media(tokens: list[Token]) -> ParsedMedia:
    """Build a ParsedMedia from a list of classified tokens for one component."""
    pm = ParsedMedia()

    # Collect all release group tokens — for fansub style the first is
    # correct ([Group] Title...), for scene style the last is correct
    # (Title.S01E05.x265-GROUP).  We pick the first if there's only one,
    # otherwise the last (scene names produce spurious groups from
    # dash-splits like "10-Bit" → "Bit").
    all_groups = [t.text for t in tokens if t.kind == TokenKind.RELEASE_GROUP]
    if len(all_groups) == 1:
        pm.release_group = all_groups[0]
    elif len(all_groups) > 1:
        pm.release_group = all_groups[-1]

    for token in tokens:
        if token.kind == TokenKind.CRC32 and not pm.hash_code:
            pm.hash_code = token.text
        elif token.kind == TokenKind.EPISODE:
            if pm.episode is None:
                pm.episode = token.episode
                if token.season is not None:
                    pm.season = token.season
                if token.version is not None:
                    pm.version = token.version
                # Multi-episode: expand batch_start..batch_end into episodes list
                if (
                    token.batch_start is not None
                    and token.batch_end is not None
                    and token.batch_end >= token.batch_start
                ):
                    pm.episodes = list(range(token.batch_start, token.batch_end + 1))
                # Check for special episodes (including season 0)
                _check_special(token, pm)
            elif pm.season is None and token.season is not None:
                # A later EPISODE token has season info the first one lacked
                # (e.g., bare "001" then "(S01E01)" in LoliHouse-style names).
                # Upgrade with the season; keep original episode number.
                pm.season = token.season
                _check_special(token, pm)
        elif token.kind == TokenKind.SPECIAL:
            pm.is_special = True
            pm.special_tag = token.text.strip()
            # Don't set pm.episode from the special's number — it may be
            # a series/group indicator (OVA2 = "OVA series 2"), not the
            # episode number. A subsequent EPISODE token takes priority.
            if token.season is not None:
                pm.season = token.season
            # S03OP/S03ED → set bonus_type for credit specials
            tag_upper = pm.special_tag.upper()
            if "OP" in tag_upper and "OP" == tag_upper[-2:]:
                pm.bonus_type = "NCOP"
            elif "ED" in tag_upper and "ED" == tag_upper[-2:]:
                pm.bonus_type = "NCED"
        elif token.kind == TokenKind.SEASON:
            if pm.season is None:
                pm.season = token.season
        elif token.kind == TokenKind.VERSION:
            if pm.version is None and token.version is not None:
                pm.version = token.version
        elif token.kind == TokenKind.RESOLUTION and not pm.resolution:
            pm.resolution = token.text
        elif token.kind == TokenKind.VIDEO_CODEC and not pm.video_codec:
            pm.video_codec = token.text
        elif token.kind == TokenKind.AUDIO_CODEC:
            pm.audio_codecs.append(token.text)
        elif token.kind == TokenKind.SOURCE:
            lower = token.text.lower()
            if not pm.source_type:
                mapped = _SOURCE_TYPE_MAP.get(lower)
                if mapped:
                    pm.source_type = mapped
                    if "remux" in lower:
                        pm.is_remux = True
            if not pm.streaming_service and lower in _STREAMING_SERVICES:
                pm.streaming_service = token.text
        elif token.kind == TokenKind.REMUX:
            pm.is_remux = True
            if not pm.source_type:
                pm.source_type = "BD"
        elif token.kind == TokenKind.YEAR:
            if pm.year is None:
                pm.year = token.year
        elif token.kind == TokenKind.BATCH_RANGE:
            if pm.batch_range is None and token.batch_start is not None:
                pm.batch_range = (token.batch_start, token.batch_end or 0)
                pm.path_is_batch = True
        elif token.kind == TokenKind.BONUS:
            pm.is_special = True
            bt = classify_bonus_type(token.text)
            if bt and bt != "Bonus":
                pm.bonus_type = bt
            elif not pm.bonus_type:
                pm.bonus_type = bt or "Bonus"
            # Extract nested song/content title from 「title」 within bonus text
            if not pm.episode_title and "\u300c" in token.text:
                inner_start = token.text.index("\u300c") + 1
                inner_end = token.text.find("\u300d", inner_start)
                if inner_end > inner_start:
                    pm.episode_title = token.text[inner_start:inner_end]
        elif token.kind == TokenKind.EPISODE_TITLE:
            # Episode title content may contain a specific bonus type
            # (e.g., 「PV1」or 「ノンテロップED「Title」」) that is more
            # specific than the generic 映像特典 BONUS marker.
            bt = classify_bonus_type(token.text)
            if bt and (not pm.bonus_type or pm.bonus_type == "Bonus"):
                pm.bonus_type = bt
        elif token.kind == TokenKind.BIT_DEPTH:
            if pm.bit_depth is None:
                m = _RE_BIT_DEPTH.match(token.text)
                if m:
                    pm.bit_depth = int(m.group(1) or m.group(2))
        elif token.kind == TokenKind.HDR:
            if not pm.hdr:
                pm.hdr = token.text
        elif token.kind == TokenKind.DUAL_AUDIO:
            pm.is_dual_audio = True
        elif token.kind == TokenKind.UNCENSORED:
            pm.is_uncensored = True
        elif token.kind == TokenKind.EDITION:
            if token.text.lower() == "criterion":
                pm.is_criterion = True
        elif token.kind == TokenKind.EXTENSION:
            pm.extension = token.text

    # Fallback: if a SPECIAL had a number but no EPISODE token followed,
    # use the special's number as the episode (e.g., SP1 → episode 1)
    if pm.episode is None and pm.is_special:
        for token in tokens:
            if token.kind == TokenKind.SPECIAL and token.episode is not None:
                pm.episode = token.episode
                break

    # Extract title (preserve bonus-extracted episode_title if already set)
    saved_ep_title = pm.episode_title
    pm.series_name, pm.series_name_alt, pm.episode_title = _extract_title_from_tokens(
        tokens
    )
    if saved_ep_title and not pm.episode_title:
        pm.episode_title = saved_ep_title

    return pm


def parse_component(text: str) -> ParsedMedia:
    """Parse a single path component (directory or filename) into ParsedMedia."""
    tokens = classify(tokenize_component(text))
    return _build_parsed_media(tokens)


def _merge_scanned_metadata(tokens: list[Token], pm: ParsedMedia) -> None:
    """Merge metadata tokens from scan_words into a ParsedMedia, filling gaps."""
    for t in tokens:
        if t.kind == TokenKind.SOURCE:
            lower = t.text.lower()
            if not pm.source_type:
                mapped = _SOURCE_TYPE_MAP.get(lower, "")
                if mapped:
                    pm.source_type = mapped
                if "remux" in lower:
                    pm.is_remux = True
            if not pm.streaming_service and lower in _STREAMING_SERVICES:
                pm.streaming_service = t.text
        elif t.kind == TokenKind.REMUX and not pm.is_remux:
            pm.is_remux = True
            if not pm.source_type:
                pm.source_type = "BD"
        elif t.kind == TokenKind.RESOLUTION and not pm.resolution:
            pm.resolution = t.text
        elif t.kind == TokenKind.VIDEO_CODEC and not pm.video_codec:
            pm.video_codec = t.text
        elif t.kind == TokenKind.AUDIO_CODEC:
            pm.audio_codecs.append(t.text)
        elif t.kind == TokenKind.HDR and not pm.hdr:
            pm.hdr = t.text
        elif t.kind == TokenKind.BIT_DEPTH and pm.bit_depth is None:
            m = _RE_BIT_DEPTH.match(t.text)
            if m:
                pm.bit_depth = int(m.group(1) or m.group(2))
        elif t.kind == TokenKind.RELEASE_GROUP and not pm.release_group:
            pm.release_group = t.text
        elif t.kind == TokenKind.DUAL_AUDIO:
            pm.is_dual_audio = True
        elif t.kind == TokenKind.UNCENSORED:
            pm.is_uncensored = True
        elif t.kind == TokenKind.EDITION:
            if t.text.lower() == "criterion":
                pm.is_criterion = True


def parse_media_path(rel_path: str) -> ParsedMedia:
    """Parse a full relative path into ParsedMedia.

    Splits the path into directory and filename components, parses each,
    and merges results with appropriate priority.
    """
    parts = rel_path.split("/")
    if len(parts) <= 1:
        # Single component (just a filename or directory)
        return parse_component(rel_path)

    # Parse directory components and filename separately
    dir_parts = parts[:-1]
    filename = parts[-1]

    # Parse the filename (primary source of episode/metadata)
    file_pm = parse_component(filename)

    # Parse directory components — first for series name, all for metadata.
    # Scan all directory texts for metadata to fill gaps (resolution, codec,
    # release group often appear in subdirectory names).
    dir_pm: ParsedMedia | None = None
    for dp in dir_parts:
        dp = dp.strip()
        if not dp:
            continue
        if dir_pm is None:
            dir_pm = parse_component(dp)
        else:
            # Scan additional directories for metadata that dir_pm missed
            extra_tokens = scan_words(dp)
            _merge_scanned_metadata(extra_tokens, dir_pm)

    if dir_pm is None:
        return file_pm

    # Merge: directory provides path_series_name, filename is primary
    result = file_pm

    # Directory series name — clean metadata from directory name
    if dir_pm.series_name:
        cleaned = clean_series_title(dir_pm.series_name)
        result.path_series_name = cleaned if cleaned else dir_pm.series_name

        # If the directory had metadata words that parse_component didn't
        # extract (common for bare directory names without brackets/dots),
        # scan the full directory text for metadata to fill gaps.
        if not dir_pm.source_type and not dir_pm.resolution:
            _merge_scanned_metadata(scan_words(dir_pm.series_name), dir_pm)

    # Batch info from directory
    if dir_pm.path_is_batch:
        result.path_is_batch = True
    if dir_pm.batch_range and not result.batch_range:
        result.batch_range = dir_pm.batch_range
        result.path_is_batch = True

    # Release group — directory group is more reliable than filename brackets
    # (brackets in filenames may contain artist credits, not release groups)
    if dir_pm.release_group:
        result.release_group = dir_pm.release_group
    # Keep file group only if directory didn't provide one

    # Source type fallback
    if not result.source_type and dir_pm.source_type:
        result.source_type = dir_pm.source_type
    if dir_pm.is_remux and not result.is_remux:
        result.is_remux = True

    # Resolution fallback
    if not result.resolution and dir_pm.resolution:
        result.resolution = dir_pm.resolution

    # Video codec fallback
    if not result.video_codec and dir_pm.video_codec:
        result.video_codec = dir_pm.video_codec

    # Audio codec fallback
    if not result.audio_codecs and dir_pm.audio_codecs:
        result.audio_codecs = dir_pm.audio_codecs

    # HDR fallback
    if not result.hdr and dir_pm.hdr:
        result.hdr = dir_pm.hdr

    # Bit depth fallback
    if result.bit_depth is None and dir_pm.bit_depth is not None:
        result.bit_depth = dir_pm.bit_depth

    # Streaming service fallback
    if not result.streaming_service and dir_pm.streaming_service:
        result.streaming_service = dir_pm.streaming_service

    # Year fallback
    if result.year is None and dir_pm.year is not None:
        result.year = dir_pm.year

    # Season fallback from directory
    if result.season is None and dir_pm.season is not None:
        result.season = dir_pm.season

    # Boolean flags — propagate from directory if set
    if dir_pm.is_dual_audio:
        result.is_dual_audio = True
    if dir_pm.is_criterion:
        result.is_criterion = True
    if dir_pm.is_uncensored:
        result.is_uncensored = True

    # Strip release group prefix from series name (dot-prefix convention:
    # "GroupName.Series.Title.EpNN" → series "GroupName Series Title" but
    # the group is "GroupName")
    if result.release_group and result.series_name:
        prefix = result.release_group + " "
        if result.series_name.startswith(prefix):
            result.series_name = result.series_name[len(prefix) :].strip()

    return result


# ---------------------------------------------------------------------------
# Normalization (CJK-aware)
# ---------------------------------------------------------------------------

# Strips everything except lowercase ASCII alphanumerics and CJK characters
_RE_NON_ALNUM_CJK = re.compile(r"[^a-z0-9\u3000-\u9fff\uff00-\uffef]")


def normalize_for_matching(name: str) -> str:
    """Normalize a series name for matching: NFC unicode normalization,
    lowercase, strip non-alnum, but preserve CJK characters (hiragana,
    katakana, kanji, fullwidth).

    NFC normalization collapses decomposed characters like
    ヘ+゚ (U+30D8 + U+309A) into the precomposed ペ (U+30DA).
    """
    return _RE_NON_ALNUM_CJK.sub("", unicodedata.normalize("NFC", name).lower())


# ---------------------------------------------------------------------------
# Title alias index (built from cached AniDB/TVDB metadata)
# ---------------------------------------------------------------------------


class TitleAliasIndex:
    """Maps normalized title strings to canonical series identifiers.

    Each series gets a unique int key. All known titles for that series
    (main, official, synonyms, aliases in any language) map to the same key.
    Two normalized names belong to the same series if they share a key.
    """

    def __init__(self) -> None:
        # normalized_title → series_key
        self._title_to_key: dict[str, int] = {}
        # series_key → set of all normalized titles
        self._key_to_titles: dict[int, set[str]] = {}
        self._next_key = 0

    def _new_key(self) -> int:
        k = self._next_key
        self._next_key += 1
        return k

    def add_series(self, titles: list[str]) -> None:
        """Register a group of titles that all refer to the same series.

        Each title is normalized before storage. If any title already
        maps to an existing series key, all new titles merge into that key.
        """
        normalized = []
        for t in titles:
            n = normalize_for_matching(t)
            if n:
                normalized.append(n)
        if not normalized:
            return

        # Find existing key if any title already registered
        existing_key: int | None = None
        for n in normalized:
            if n in self._title_to_key:
                existing_key = self._title_to_key[n]
                break

        key = existing_key if existing_key is not None else self._new_key()
        title_set = self._key_to_titles.setdefault(key, set())

        for n in normalized:
            old_key = self._title_to_key.get(n)
            if old_key is not None and old_key != key:
                # Merge old key's titles into this key
                for old_n in self._key_to_titles.pop(old_key, set()):
                    self._title_to_key[old_n] = key
                    title_set.add(old_n)
            self._title_to_key[n] = key
            title_set.add(n)

    def same_series(self, name_a: str, name_b: str) -> bool:
        """Return True if two names are known to refer to the same series."""
        na = normalize_for_matching(name_a)
        nb = normalize_for_matching(name_b)
        if not na or not nb:
            return False
        if na == nb:
            return True
        ka = self._title_to_key.get(na)
        kb = self._title_to_key.get(nb)
        if ka is not None and kb is not None:
            return ka == kb
        return False

    def lookup(self, name: str) -> set[str] | None:
        """Return all known normalized titles for this name's series, or None."""
        n = normalize_for_matching(name)
        key = self._title_to_key.get(n)
        if key is None:
            return None
        return self._key_to_titles.get(key)

    def matching_keys(
        self, series_name: str, index_keys: set[str] | None = None
    ) -> set[str]:
        """Return all normalized keys that could identify *series_name*.

        Combines direct name variants (raw, parsed, cleaned) with alias
        expansions from the title index.  When *index_keys* is provided,
        also includes keys where one is a prefix of the other — this
        handles short vs long title variants (e.g. downloads use "Hell
        Mode" while Sonarr uses the full official title).
        """
        keys: set[str] = set()
        for variant in name_variants(series_name):
            keys.add(variant)
            idx_key = self._title_to_key.get(variant)
            if idx_key is not None:
                aliases = self._key_to_titles.get(idx_key)
                if aliases:
                    keys.update(aliases)

        if index_keys is not None:
            prefix_hits: set[str] = set()
            for cand in keys:
                for ik in index_keys:
                    if ik not in keys and (cand.startswith(ik) or ik.startswith(cand)):
                        prefix_hits.add(ik)
            keys.update(prefix_hits)

        return keys

    @property
    def series_count(self) -> int:
        return len(self._key_to_titles)

    @property
    def title_count(self) -> int:
        return len(self._title_to_key)


# ---------------------------------------------------------------------------
# Series name cleaning (for undelimited metadata in directory names)
# ---------------------------------------------------------------------------

# Truncate at the first recognized metadata keyword.
# Handles directory names like "Show S01-S02 BDRip x265-GROUP" and
# scene-style dotted names like "Show.S02.1080p.BluRay.x265-GROUP"
# where the parser can't separate title from metadata without delimiters.
_RE_META_BOUNDARY = re.compile(
    r"[\s.]+(?:S\d+(?:[+-]S?\d+)?(?:\+OVA)?|"
    r"BD|BDRip|BluRay|Blu-Ray|WEB|WEB-DL|WEBRip|REMUX|DVD|DVDRip|DVD-R|HDTV|SDTV|VCD|CD-R|"
    r"Dual[\s.]Audio|DUAL|Uncensored|x26[45]|HEVC|AVC|H\.26[45]|"
    r"1080p|720p|2160p|4K|"
    r"FLAC|AAC|DTS|AC3|DD|EAC3)\b",
    re.IGNORECASE,
)


_RE_TRAILING_BRACKET = re.compile(r"\s*\[.*?\]\s*$")


def clean_series_title(name: str) -> str:
    """Extract series title by truncating at the first metadata keyword.

    Handles both space-separated and dot-separated names, e.g.
    ``"Show S01-S02 Dual Audio BDRip x265-GROUP"`` → ``"Show"``
    ``"Show.S02.1080p.BluRay.x265-GROUP"`` → ``"Show"``.

    Also strips trailing ``[bracket]`` content (release groups / metadata
    that the parser didn't classify).
    """
    m = _RE_META_BOUNDARY.search(name)
    if m:
        name = name[: m.start()].strip(" .-")
    name = _RE_TRAILING_BRACKET.sub("", name).strip()
    return name


def name_variants(name: str) -> set[str]:
    """Return all normalized key variants for a series name.

    Produces keys from the raw name, the parser-extracted name (without
    year/quality), the metadata-truncated name, and the alternate-language
    title (if present).  Any of these may match download index keys.
    """
    keys: set[str] = set()
    pm = parse_component(name)
    for variant in (name, pm.series_name, pm.series_name_alt, clean_series_title(name)):
        k = normalize_for_matching(variant)
        if k:
            keys.add(k)
    return keys


def _load_anidb_titles(xml_path: str) -> list[str]:
    """Extract all title strings from a cached AniDB XML file."""
    import xml.etree.ElementTree as ET

    try:
        tree = ET.parse(xml_path)
    except ET.ParseError, OSError:
        return []

    root = tree.getroot()
    titles: list[str] = []
    for elem in root.findall("titles/title"):
        text = (elem.text or "").strip()
        ttype = elem.get("type", "")
        if text and ttype in ("main", "official", "synonym"):
            titles.append(text)
    return titles


def _load_tvdb_titles(json_path: str) -> list[str]:
    """Extract all title strings from a cached TVDB JSON file."""
    import json

    try:
        with open(json_path, encoding="utf-8") as f:
            data = json.load(f)
    except ValueError, OSError:
        return []

    titles: list[str] = []

    series = data.get("series", {})
    name = series.get("name", "")
    if name:
        titles.append(name)

    for alias in series.get("aliases", []):
        n = alias.get("name", "")
        if n:
            titles.append(n)

    for _lang, n in data.get("translations", {}).items():
        if n:
            titles.append(n)

    return titles


def build_title_index(cache_dir: str) -> TitleAliasIndex:
    """Build a TitleAliasIndex from all cached AniDB and TVDB metadata.

    Scans ``cache_dir/anidb/*.xml`` and ``cache_dir/tvdb/*.json``.
    """
    import os

    index = TitleAliasIndex()

    anidb_dir = os.path.join(cache_dir, "anidb")
    if os.path.isdir(anidb_dir):
        for name in os.listdir(anidb_dir):
            if name.endswith(".xml"):
                titles = _load_anidb_titles(os.path.join(anidb_dir, name))
                if titles:
                    index.add_series(titles)

    tvdb_dir = os.path.join(cache_dir, "tvdb")
    if os.path.isdir(tvdb_dir):
        for name in os.listdir(tvdb_dir):
            if name.endswith(".json"):
                titles = _load_tvdb_titles(os.path.join(tvdb_dir, name))
                if titles:
                    index.add_series(titles)

    return index
