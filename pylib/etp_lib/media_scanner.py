"""Position-based token scanner using parsy primitives.

Replaces regex-heavy tokenization with a longest-match scanner that tries
typed recognizers at each position.  Structural delimiters (brackets, parens,
lenticular quotes) are handled first as boundaries.  Between boundaries,
content is scanned with parsy primitives for metadata tokens.  Unrecognized
text accumulates as TEXT tokens.

The output is the same Token list that the existing classify() and
_build_parsed_media() expect, so this is a drop-in replacement for
tokenize_component's internal splitting logic.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from parsy import Parser, Result, regex

from etp_lib.media_parser import (
    Token,
    TokenKind,
    _AUDIO_CODECS,
    _HDR_KEYWORDS,
    _LANGUAGES,
    _SOURCE_TYPE_MAP,
    _SOURCES,
    _SUBTITLE_KEYWORDS,
    _VIDEO_CODECS,
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


# Resolution
resolution: Parser = regex(
    r"(?:480|540|576|720|1080)[pi]|2160p|4[kK]", re.IGNORECASE
).map(lambda s: Resolution(s)) | regex(r"\d{3,4}x\d{3,4}").map(lambda s: Resolution(s))

# Video codec
video_codec: Parser = _match_set_ci(_VIDEO_CODECS, VideoCodec)

# Audio codec (compound forms like AAC2.0, DTS-HD MA)
_RE_AC = re.compile(
    r"(?:DTS-HDMA|DTS-HD\s*MA|DTS-HD|DTS|DDP|DD|EAC3|E-AC-3|AC3|AAC|FLAC|TrueHD|PCM|LPCM)"
    r"(?:[.\s]?\d\.\d)?",
    re.IGNORECASE,
)


def _audio_codec_parser(stream: str, index: int):
    m = _RE_AC.match(stream, index)
    if m:
        return Result.success(m.end(), AudioCodec(m.group(0)))
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

# Episode: SxxExx
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
    m = re.match(r"(\d{1,4})(?:v(\d+))?(?=\s|$|\.)", stream[index:])
    if not m:
        return Result.failure(index, frozenset({"bare_episode"}))
    num = int(m.group(1))
    if 1900 <= num <= 2099 and m.group(2) is None:
        return Result.failure(index, frozenset({"bare_episode"}))
    version = int(m.group(2)) if m.group(2) else None
    return Result.success(index + m.end(), EpisodeBare(num, version))


episode_bare: Parser = Parser(_bare_episode_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Episode: EP05, E5 format
def _ep_prefix_parser(stream: str, index: int):
    m = re.match(r"[Ee][Pp]?(\d{1,4})(?:v(\d+))?$", stream[index:])
    if not m:
        return Result.failure(index, frozenset({"ep_prefix"}))
    version = int(m.group(2)) if m.group(2) else None
    return Result.success(index + m.end(), EpisodeBare(int(m.group(1)), version))


episode_ep: Parser = Parser(_ep_prefix_parser)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


# Episode: "05 END", "05v2 END" (final episode marker)
def _ep_final_parser(stream: str, index: int):
    m = re.match(r"(\d{1,4})(?:v(\d+))?\s*END$", stream[index:], re.IGNORECASE)
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


# Batch range: "01~26"
def _parse_batch_range(s: str) -> BatchRange:
    parts = re.split(r"\s*[~～]\s*", s)
    return BatchRange(start=int(parts[0]), end=int(parts[1]))


batch_range: Parser = regex(r"(\d{1,4})\s*[~～]\s*(\d{1,4})").map(_parse_batch_range)

# Version: "v2"
version: Parser = regex(r"v(\d+)", re.IGNORECASE).map(
    lambda s: Version(int(_re_group(r"v(\d+)", s, flags=re.IGNORECASE)))
)

# Year
year: Parser = regex(r"(?:19|20)\d{2}(?!\d)").map(lambda s: Year(int(s)))

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
    EpisodeBare: TokenKind.EPISODE,
    EpisodeJP: TokenKind.EPISODE,
    SeasonJP: TokenKind.SEASON,
    SeasonWord: TokenKind.SEASON,
    SeasonOnly: TokenKind.SEASON,
    Special: TokenKind.EPISODE,
    BatchRange: TokenKind.BATCH_RANGE,
    Version: TokenKind.VERSION,
    Year: TokenKind.YEAR,
    CRC32: TokenKind.CRC32,
    Language: TokenKind.LANGUAGE,
    BonusKeyword: TokenKind.BONUS,
    SubtitleInfo: TokenKind.SUBTITLE_INFO,
    HDRInfo: TokenKind.UNKNOWN,  # HDR stored as UNKNOWN (no dedicated kind yet)
    Repack: TokenKind.UNKNOWN,
    SitePrefix: TokenKind.SITE_PREFIX,
}


def _result_to_token(result: object, text: str) -> Token:
    """Convert a parsy primitive result to a Token for the existing pipeline."""
    kind = _TYPE_TO_KIND.get(type(result), TokenKind.UNKNOWN)
    token = Token(kind=kind, text=text)

    # Populate numeric fields
    if isinstance(result, EpisodeSE):
        token.season = result.season
        token.episode = result.episode
        token.version = result.version
    elif isinstance(result, EpisodeBare):
        token.episode = result.episode
        token.version = result.version
    elif isinstance(result, EpisodeJP):
        token.episode = result.episode
    elif isinstance(result, SeasonJP):
        token.season = result.season
    elif isinstance(result, SeasonWord):
        token.season = result.season
    elif isinstance(result, SeasonOnly):
        token.season = result.season
    elif isinstance(result, Special):
        token.episode = result.number
        # Mark as special via text pattern — classify phase handles this
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
    episode_se,  # S01E05, s1e1, S03E13v2
    episode_jp,  # 第01話
    batch_range,  # 01~26
    special,  # SP1, OVA, OAD, ONA
    season_jp,  # 第1期
    season_word,  # 4th Season
    season_only,  # S01 (after episode_se to avoid S01E05 → S01)
    episode_final,  # 05 END, 05v2 END (before bare to match END suffix)
    episode_ep,  # EP05, E5
    episode_bare,  # 08, 12v2 (after season_only to avoid S01 → ep 1)
    # Bonus keywords
    bonus_en,  # NCOP, NC OP1, Creditless ED
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
    # Context
    language,  # jpn, eng, dual
    subtitle_info,  # multisub, msubs
    hdr_info,  # HDR, HDR10, DoVi
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

    # Sort by position, take non-overlapping matches
    pre_tokens.sort(key=lambda x: x[0])
    used: list[tuple[int, int, Token]] = []
    last_end = 0
    for start, end, token in pre_tokens:
        if start >= last_end:
            used.append((start, end, token))
            last_end = end

    # Build token list: recognized spans + unrecognized gaps as TEXT
    def _emit_gap(gap_text: str) -> None:
        gap_text = gap_text.strip(" ,-")
        if gap_text:
            for w in re.split(r"[\s,\-]+", gap_text):
                w = w.strip()
                if w:
                    tokens.append(Token(kind=TokenKind.TEXT, text=w))

    pos = 0
    for start, end, token in used:
        _emit_gap(text[pos:start])
        tokens.append(token)
        pos = end
    _emit_gap(text[pos:])

    return tokens


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
        # Check 3-segment: "MA.5.1", "FLAC.2.0"
        if i + 2 < len(raw_parts):
            compound3 = f"{part}.{raw_parts[i + 1]}.{raw_parts[i + 2]}"
            token = _try_recognize(compound3)
            if token is not None:
                tokens.append(token)
                i += 3
                continue

        # Check 2-segment: "H.264", "AAC2.0"
        if i + 1 < len(raw_parts):
            # Also try with trailing "-suffix" stripped for "H.264-VARYG"
            next_part = raw_parts[i + 1]
            compound2 = f"{part}.{next_part}"
            token = _try_recognize(compound2)
            if token is not None:
                tokens.append(token)
                i += 2
                continue

            # Strip trailing -suffix and try: "H" + "264-VARYG" → "H.264"
            next_base = re.sub(r"-[A-Za-z].*$", "", next_part)
            if next_base != next_part:
                compound2_stripped = f"{part}.{next_base}"
                token = _try_recognize(compound2_stripped)
                if token is not None:
                    tokens.append(token)
                    # The suffix after the dash is the release group
                    # (scene convention: codec-GROUP)
                    suffix = next_part[len(next_base) + 1 :]
                    if suffix:
                        suffix_token = _try_recognize(suffix)
                        if suffix_token is not None:
                            tokens.append(suffix_token)
                        else:
                            tokens.append(
                                Token(kind=TokenKind.RELEASE_GROUP, text=suffix)
                            )
                    i += 2
                    continue

        # Single segment
        token = _try_recognize(part)
        if token is not None:
            tokens.append(token)
        else:
            # Check if it has a trailing "-GROUP" (scene convention)
            dash_m = re.match(r"^(.+)-([A-Za-z][A-Za-z0-9]+)$", part)
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


def find_episode_in_text(text: str) -> tuple[int, int, Token] | None:
    """Search for an SxxExx episode marker at any position in text.

    Returns (start, end, token) or None. Replaces _RE_EP_SE_SEARCH.search().
    """
    for pos in range(len(text)):
        result = episode_se(text, pos)
        if result.status:
            return (
                pos,
                result.index,
                _result_to_token(result.value, text[pos : result.index]),
            )
    return None


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
    for w in re.split(r"[\s,]+", text):
        w = w.strip()
        if w and is_metadata_word(w):
            count += 1
    return count


def expand_metadata_words(text: str) -> list[Token]:
    """Split text on whitespace/commas and classify each word.

    Drop-in replacement for media_parser._expand_metadata_words.
    Dash-separated compounds are sub-split so each part can be classified.
    """
    tokens: list[Token] = []
    for w in re.split(r"[\s,]+", text):
        w = w.strip()
        if not w:
            continue
        token = _try_recognize(w)
        if token is not None:
            tokens.append(token)
        else:
            sub_parts = w.split("-")
            if len(sub_parts) > 1:
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
    return tokens
