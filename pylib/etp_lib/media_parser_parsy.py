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
    _COMPOUND_TOKENS,
    _LANGUAGES,
    _SOURCE_TYPE_MAP,
    _SOURCES,
    _VIDEO_CODECS,
    ParsedMedia,
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


# ---------------------------------------------------------------------------
# Phase B: Structural primitives and convention parsers
# ---------------------------------------------------------------------------


def _bracket_content(stream: str, index: int) -> Result:
    """Parse content inside [...], handling nested brackets."""
    if index >= len(stream) or stream[index] != "[":
        return Result.failure(index, frozenset({"'['"}))
    depth = 1
    i = index + 1
    while i < len(stream) and depth > 0:
        if stream[i] == "[":
            depth += 1
        elif stream[i] == "]":
            depth -= 1
        i += 1
    if depth != 0:
        return Result.failure(index, frozenset({"']'"}))
    return Result.success(i, stream[index + 1 : i - 1])


bracket: Parser = Parser(_bracket_content)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


def _paren_content(stream: str, index: int) -> Result:
    """Parse content inside (...), handling nested parens."""
    if index >= len(stream) or stream[index] != "(":
        return Result.failure(index, frozenset({"'('"}))
    depth = 1
    i = index + 1
    while i < len(stream) and depth > 0:
        if stream[i] == "(":
            depth += 1
        elif stream[i] == ")":
            depth -= 1
        i += 1
    if depth != 0:
        return Result.failure(index, frozenset({"')'"}))
    return Result.success(i, stream[index + 1 : i - 1])


paren: Parser = Parser(_paren_content)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]


def _lenticular_content(stream: str, index: int) -> Result:
    """Parse content inside 「...」."""
    if index >= len(stream) or stream[index] != "「":
        return Result.failure(index, frozenset({"'「'"}))
    end = stream.find("」", index + 1)
    if end == -1:
        return Result.failure(index, frozenset({"'」'"}))
    return Result.success(end + 1, stream[index + 1 : end])


lenticular: Parser = Parser(_lenticular_content)  # type: ignore[arg-type]  # ty: ignore[invalid-argument-type]

separator: Parser = regex(r"\s+-\s+")
extension: Parser = regex(r"\.[a-zA-Z0-9]{2,4}$").map(lambda s: s.lower())
opt_ws: Parser = regex(r"\s*")


# ---------------------------------------------------------------------------
# Metadata extraction helpers
# ---------------------------------------------------------------------------


def _extract_metadata_from_words(words: list[str]) -> dict:
    """Extract structured metadata from a list of words.

    Returns a dict with keys: source_type, is_remux, resolution, video_codec,
    audio_codecs, release_group.
    """
    meta: dict = {
        "source_type": "",
        "is_remux": False,
        "resolution": "",
        "video_codec": "",
        "audio_codecs": [],
        "release_group": "",
    }
    for w in words:
        _classify_meta_word(w, meta)
    return meta


def _classify_meta_word(w: str, meta: dict) -> None:
    """Classify a single word and update meta dict."""
    lower = w.lower().strip()
    if not lower:
        return
    # Resolution
    if lower in {"480p", "540p", "576p", "720p", "1080p", "1080i", "2160p", "4k"}:
        if not meta["resolution"]:
            meta["resolution"] = w
    elif re.match(r"^\d{3,4}x\d{3,4}$", w):
        if not meta["resolution"]:
            meta["resolution"] = w
    # Video codec
    elif lower in _VIDEO_CODECS:
        if not meta["video_codec"]:
            meta["video_codec"] = w
    # Audio codec
    elif lower in _AUDIO_CODECS or re.match(
        r"(?:DTS-HDMA|DTS-HD\s*MA|DTS-HD|DTS|DDP|DD|EAC3|E-AC-3|AC3|AAC|FLAC|TrueHD|PCM|LPCM)"
        r"(?:[.\s]?\d\.\d)?$",
        w,
        re.IGNORECASE,
    ):
        meta["audio_codecs"].append(w)
    # Source
    elif lower in _SOURCES:
        if not meta["source_type"]:
            meta["source_type"] = _SOURCE_TYPE_MAP.get(lower, "")
    # Remux (exact match)
    elif lower == "remux":
        meta["is_remux"] = True
        if not meta["source_type"]:
            meta["source_type"] = "BD"
    # Dash compounds: split and classify sub-parts (e.g. REMUX-GROUP, Bluray-1080p)
    elif "-" in w and not w.startswith("-"):
        parts = w.split("-")
        has_meta = False
        for p in parts:
            pl = p.lower()
            if pl in _VIDEO_CODECS | _AUDIO_CODECS | _SOURCES or pl == "remux":
                has_meta = True
                break
            if pl in {"480p", "540p", "576p", "720p", "1080p", "1080i", "2160p", "4k"}:
                has_meta = True
                break
        if has_meta:
            for p in parts:
                _classify_meta_word(p, meta)
            # Last unclassified part after metadata may be release group
            last = parts[-1]
            if not _is_metadata_word_simple(last) and not meta["release_group"]:
                meta["release_group"] = last


def _extract_metadata_from_text(text: str) -> dict:
    """Extract metadata from a space/comma-separated string."""
    return _extract_metadata_from_words(re.split(r"[\s,]+", text))


def _apply_metadata(pm: ParsedMedia, meta: dict) -> None:
    """Apply extracted metadata dict to a ParsedMedia."""
    if meta["source_type"] and not pm.source_type:
        pm.source_type = meta["source_type"]
    if meta["is_remux"]:
        pm.is_remux = True
        if not pm.source_type:
            pm.source_type = "BD"
    if meta["resolution"] and not pm.resolution:
        pm.resolution = meta["resolution"]
    if meta["video_codec"] and not pm.video_codec:
        pm.video_codec = meta["video_codec"]
    pm.audio_codecs.extend(meta["audio_codecs"])
    if meta["release_group"] and not pm.release_group:
        pm.release_group = meta["release_group"]


# ---------------------------------------------------------------------------
# Convention: Fansub  [Group] Title - Episode [metadata][hash].ext
# ---------------------------------------------------------------------------


def parse_fansub(text: str) -> ParsedMedia | None:
    """Try to parse a fansub-style filename.

    Pattern: [Group] Title - Episode [metadata]* [hash]? .ext
    """
    pm = ParsedMedia()

    remaining = text

    # Extension
    m = re.search(r"\.([a-zA-Z0-9]{2,4})$", remaining)
    if m:
        pm.extension = m.group(0).lower()
        remaining = remaining[: m.start()]

    # Leading [Group]
    if not remaining.startswith("["):
        return None
    m_bracket = re.match(r"\[([^\]]+)\]", remaining)
    if not m_bracket:
        return None
    group_text = m_bracket.group(1)
    remaining = remaining[m_bracket.end() :].strip()

    # Classify first bracket: release group vs metadata
    group_words = re.split(r"[\s,]+", group_text)
    meta_count = sum(1 for w in group_words if _is_metadata_word_simple(w))
    is_meta_bracket = meta_count > 0 and meta_count >= len(group_words) // 2
    if is_meta_bracket:
        # Sonarr-style: first word may be group, rest is metadata
        if not _is_metadata_word_simple(group_words[0]):
            pm.release_group = group_words[0]
            meta = _extract_metadata_from_words(group_words[1:])
        else:
            meta = _extract_metadata_from_words(group_words)
        _apply_metadata(pm, meta)
    else:
        pm.release_group = group_text

    # Trailing brackets (right to left): hash, metadata
    while True:
        m_trail = re.search(r"\[([^\]]+)\]\s*$", remaining)
        if not m_trail:
            break
        content = m_trail.group(1).strip()
        remaining = remaining[: m_trail.start()].rstrip()
        # CRC32?
        if re.match(r"^[0-9A-Fa-f]{8}$", content):
            pm.hash_code = content.upper()
        else:
            meta = _extract_metadata_from_text(content)
            _apply_metadata(pm, meta)

    # Split on separator " - " to get title and episode parts
    parts = re.split(r"\s+-\s+", remaining)
    if not parts:
        return None

    # Episode is typically the last segment that looks like a number/episode
    ep_found = False
    title_parts = []
    for i, part in enumerate(parts):
        part = part.strip()
        # Try episode patterns
        ep_m = re.match(r"^(\d{1,4})(?:v(\d+))?\s*(END)?$", part, re.IGNORECASE)
        se_m = re.match(r"^[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?$", part)
        sp_m = re.match(r"^(SP|OVA|OAD|ONA)(\d*)$", part, re.IGNORECASE)
        if se_m and not ep_found:
            pm.season = int(se_m.group(1))
            pm.episode = int(se_m.group(2))
            if se_m.group(3):
                pm.version = int(se_m.group(3))
            ep_found = True
        elif sp_m and not ep_found:
            pm.is_special = True
            pm.special_tag = part
            num = sp_m.group(2)
            pm.episode = int(num) if num else None
            ep_found = True
        elif ep_m and not ep_found:
            num = int(ep_m.group(1))
            if not (1900 <= num <= 2099):
                pm.episode = num
                if ep_m.group(2):
                    pm.version = int(ep_m.group(2))
                ep_found = True
            else:
                title_parts.append(part)
        else:
            title_parts.append(part)

    pm.series_name = " ".join(title_parts).strip()
    # Strip trailing " - " artifacts
    pm.series_name = re.sub(r"\s*-\s*$", "", pm.series_name).strip()

    # Check for year in parens within series name
    year_m = re.search(r"\((\d{4})\)", pm.series_name)
    if year_m:
        y = int(year_m.group(1))
        if 1900 <= y <= 2099:
            pm.year = y

    return pm


# ---------------------------------------------------------------------------
# Convention: Scene  Title.S01E05.metadata-GROUP.ext
# ---------------------------------------------------------------------------


def parse_scene(text: str) -> ParsedMedia | None:
    """Try to parse a scene-style dot-separated filename.

    Pattern: Title.S01E05.EpTitle.metadata.codec-GROUP.ext
    """
    pm = ParsedMedia()

    remaining = text

    # Extension
    m = re.search(r"\.([a-zA-Z0-9]{2,4})$", remaining)
    if m:
        pm.extension = m.group(0).lower()
        remaining = remaining[: m.start()]

    # Must contain dots and an SxxExx or S-only pattern
    if "." not in remaining:
        return None

    # Split on dots, preserving compound tokens by re-joining them after split.
    # Compound tokens like H.264, AAC2.0 span multiple dot-separated segments.
    # The next segment may have a trailing "-GROUP" suffix that needs stripping.
    _compound_lowers = {c.lower() for c in _COMPOUND_TOKENS}
    raw_parts = remaining.split(".")
    parts: list[str] = []
    i = 0
    while i < len(raw_parts):
        p = raw_parts[i]
        joined = False
        if i + 1 < len(raw_parts):
            next_raw = raw_parts[i + 1]
            # Strip trailing "-suffix" before checking compound membership
            next_base = re.sub(r"-[A-Za-z].*$", "", next_raw)
            candidate = f"{p}.{next_base}"
            if candidate.lower() in _compound_lowers:
                # Restore the full segment (with any suffix)
                parts.append(f"{p}.{next_raw}")
                i += 2
                joined = True
            else:
                # Three-part compounds (MA.5.1)
                if i + 2 < len(raw_parts):
                    candidate3 = f"{p}.{raw_parts[i + 1]}.{raw_parts[i + 2]}"
                    if candidate3.lower() in _compound_lowers:
                        parts.append(candidate3)
                        i += 3
                        joined = True
        if not joined:
            parts.append(p)
            i += 1

    # Find SxxExx or S-only
    ep_idx = None
    for i, p in enumerate(parts):
        se_m = re.match(r"^[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?$", p)
        if se_m:
            pm.season = int(se_m.group(1))
            pm.episode = int(se_m.group(2))
            if se_m.group(3):
                pm.version = int(se_m.group(3))
            ep_idx = i
            break
        s_only_m = re.match(r"^[Ss](\d{1,2})$", p)
        if s_only_m:
            pm.season = int(s_only_m.group(1))
            ep_idx = i
            break

    if ep_idx is None:
        # No SxxExx, might be "Movie.2005.WEB-DL.2160p"
        # Find first metadata word
        meta_start = None
        for i, p in enumerate(parts):
            if _is_metadata_word_simple(p):
                meta_start = i
                break
            year_m = re.match(r"^(19|20)\d{2}$", p)
            if year_m:
                pm.year = int(p)
                meta_start = i + 1
                break
        if meta_start is not None:
            pm.series_name = " ".join(parts[:meta_start]).strip()
            if pm.year and parts[meta_start - 1] == str(pm.year):
                pm.series_name = " ".join(parts[: meta_start - 1]).strip()
            meta = _extract_metadata_from_words(parts[meta_start:])
            _apply_metadata(pm, meta)
        else:
            pm.series_name = " ".join(parts)
        return pm

    pm.series_name = " ".join(parts[:ep_idx])

    # After episode: find where metadata starts
    after = parts[ep_idx + 1 :]
    ep_title_parts = []
    meta_parts = []
    in_meta = False
    for p in after:
        if not in_meta and _is_metadata_word_simple(p):
            in_meta = True
        if in_meta:
            meta_parts.append(p)
        else:
            ep_title_parts.append(p)

    pm.episode_title = " ".join(ep_title_parts)

    # Last part may be "codec-GROUP"
    if meta_parts:
        last = meta_parts[-1]
        dash_m = re.match(r"^(.*)-([A-Za-z][A-Za-z0-9]+)$", last)
        if dash_m:
            prefix = dash_m.group(1)
            group = dash_m.group(2)
            meta_parts[-1] = prefix
            pm.release_group = group

    meta = _extract_metadata_from_words(meta_parts)
    _apply_metadata(pm, meta)

    return pm


# ---------------------------------------------------------------------------
# Convention: Japanese  [Group] Title 第01話「EpTitle」(metadata).ext
# ---------------------------------------------------------------------------


def parse_japanese(text: str) -> ParsedMedia | None:
    """Try to parse a Japanese-style filename.

    Pattern: [Group] Title (第N期) 第NN話「Episode Title」(metadata).ext
    """
    pm = ParsedMedia()

    remaining = text

    # Extension
    m = re.search(r"\.([a-zA-Z0-9]{2,4})$", remaining)
    if m:
        pm.extension = m.group(0).lower()
        remaining = remaining[: m.start()]

    # Must start with bracket and contain 第...話
    if not remaining.startswith("["):
        return None
    if "第" not in remaining or "話" not in remaining:
        return None

    # Leading [Group]
    m_bracket = re.match(r"\[([^\]]+)\]", remaining)
    if not m_bracket:
        return None
    group_text = m_bracket.group(1)
    remaining = remaining[m_bracket.end() :].strip()

    # Group may contain source type (e.g. "アニメ BD")
    group_words = group_text.split()
    if len(group_words) > 1:
        meta = _extract_metadata_from_words(group_words[1:])
        _apply_metadata(pm, meta)
        pm.release_group = group_words[0]
    else:
        pm.release_group = group_text

    # Trailing (metadata) paren
    m_paren = re.search(r"\(([^)]+)\)\s*$", remaining)
    if m_paren:
        meta = _extract_metadata_from_text(m_paren.group(1))
        _apply_metadata(pm, meta)
        remaining = remaining[: m_paren.start()].rstrip()

    # Episode title in 「」
    m_lent = re.search(r"「(.+?)」", remaining)
    if m_lent:
        pm.episode_title = m_lent.group(1)
        remaining = remaining[: m_lent.start()].rstrip()

    # Japanese episode 第NN話
    m_ep = re.search(r"第(\d{1,4})話", remaining)
    if m_ep:
        pm.episode = int(m_ep.group(1))
        remaining = remaining[: m_ep.start()].rstrip()
        # Check for (終) after 話
        after_ep = text[m_ep.end() :]
        if after_ep.startswith("(終)") or after_ep.startswith("（終）"):
            pass  # Final episode marker, no special handling needed

    # Season in parens 第N期
    m_season = re.search(r"\(第(\d{1,2})期\)", remaining)
    if m_season:
        pm.season = int(m_season.group(1))
        remaining = remaining[: m_season.start()].rstrip()

    pm.series_name = remaining.strip()

    return pm


# ---------------------------------------------------------------------------
# Convention: Sonarr  Title - SxxExx - EpTitle - metadata [audio] - GROUP.ext
#             also:   Title - sNeNN - EpTitle [Group meta,...].ext
# ---------------------------------------------------------------------------


def parse_sonarr(text: str) -> ParsedMedia | None:
    """Try to parse a Sonarr/Radarr-style filename.

    Patterns:
    - Title (Year) - S01E01 - Ep Title - metadata [audio info] - GROUP.ext
    - Title - s1e01 - Ep Title [Group source,res,...].ext
    - Title S01E01 Ep Title metadata.ext  (no separators)
    """
    pm = ParsedMedia()

    remaining = text

    # Extension
    m = re.search(r"\.([a-zA-Z0-9]{2,4})$", remaining)
    if m:
        pm.extension = m.group(0).lower()
        remaining = remaining[: m.start()]

    # Must contain SxxExx somewhere (not starting with [)
    if remaining.startswith("["):
        return None
    ep_m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?", remaining)
    if not ep_m:
        return None

    pm.season = int(ep_m.group(1))
    pm.episode = int(ep_m.group(2))
    if ep_m.group(3):
        pm.version = int(ep_m.group(3))

    before_ep = remaining[: ep_m.start()].rstrip(" -")
    after_ep = remaining[ep_m.end() :].strip()

    # Before episode: series name (may have year in parens)
    year_m = re.search(r"\((\d{4})\)", before_ep)
    if year_m:
        y = int(year_m.group(1))
        if 1900 <= y <= 2099:
            pm.year = y
    # Strip year from series name display (keep it in pm.year)
    series = before_ep.strip()
    if pm.year:
        series = re.sub(r"\s*\(\d{4}\)\s*$", "", series).strip()
    pm.series_name = series

    # Trailing "- GROUP" at the end
    trail_group_m = re.search(r"\s+-\s+(\S+)\s*$", after_ep)
    if trail_group_m:
        candidate = trail_group_m.group(1)
        if not _is_metadata_word_simple(candidate):
            pm.release_group = candidate
            after_ep = after_ep[: trail_group_m.start()].rstrip()

    # Trailing brackets (right to left): audio metadata
    while True:
        m_trail = re.search(r"\[([^\]]+)\]\s*$", after_ep)
        if not m_trail:
            break
        content = m_trail.group(1).strip()
        after_ep = after_ep[: m_trail.start()].rstrip()
        # CRC32?
        if re.match(r"^[0-9A-Fa-f]{8}$", content):
            pm.hash_code = content.upper()
        else:
            # Check if first word is release group (Sonarr bracket style)
            bracket_words = re.split(r"[\s,]+", content)
            if bracket_words and not _is_metadata_word_simple(bracket_words[0]):
                if not pm.release_group:
                    pm.release_group = bracket_words[0]
                meta = _extract_metadata_from_words(bracket_words[1:])
            else:
                meta = _extract_metadata_from_text(content)
            _apply_metadata(pm, meta)

    # After episode text: split on " - " separators
    parts = re.split(r"\s+-\s+", after_ep) if after_ep else []

    ep_title_parts = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        # Check if this part is all/mostly metadata
        words = part.split()
        meta_count = sum(1 for w in words if _is_metadata_word_simple(w))
        if meta_count > 0 and meta_count >= len(words) // 2:
            meta = _extract_metadata_from_words(words)
            _apply_metadata(pm, meta)
        else:
            # Scan left-to-right: once metadata starts, everything after is metadata
            meta_start = None
            for i, w in enumerate(words):
                if _is_metadata_word_simple(w):
                    meta_start = i
                    break
            if meta_start is not None and meta_start > 0:
                ep_title_parts.append(" ".join(words[:meta_start]))
                meta = _extract_metadata_from_words(words[meta_start:])
                _apply_metadata(pm, meta)
            else:
                ep_title_parts.append(part)

    if ep_title_parts:
        pm.episode_title = ep_title_parts[0].strip(" -")

    return pm


# ---------------------------------------------------------------------------
# Convention: Bare  Title SxxExx metadata.ext (no delimiters)
# ---------------------------------------------------------------------------


def parse_bare(text: str) -> ParsedMedia | None:
    """Try to parse a bare title, possibly with embedded SxxExx."""
    pm = ParsedMedia()

    remaining = text

    # Extension
    m = re.search(r"\.([a-zA-Z0-9]{2,4})$", remaining)
    if m:
        pm.extension = m.group(0).lower()
        remaining = remaining[: m.start()]

    # Check for embedded SxxExx
    ep_m = re.search(r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?", remaining)
    if ep_m:
        pm.season = int(ep_m.group(1))
        pm.episode = int(ep_m.group(2))
        if ep_m.group(3):
            pm.version = int(ep_m.group(3))

        before = remaining[: ep_m.start()].strip()
        after = remaining[ep_m.end() :].strip()

        pm.series_name = before.rstrip(" -")

        # After episode: split into ep title + metadata
        words = after.split()
        meta_start = None
        for i, w in enumerate(words):
            if _is_metadata_word_simple(w):
                meta_start = i
                break
        if meta_start is not None:
            if meta_start > 0:
                pm.episode_title = " ".join(words[:meta_start])
            meta = _extract_metadata_from_words(words[meta_start:])
            _apply_metadata(pm, meta)
        elif after:
            pm.episode_title = after
    else:
        # No episode: just title, maybe with year
        year_m = re.search(r"\((\d{4})\)", remaining)
        if year_m:
            y = int(year_m.group(1))
            if 1900 <= y <= 2099:
                pm.year = y
        pm.series_name = remaining.strip()

    return pm


# ---------------------------------------------------------------------------
# Convention detection and dispatch
# ---------------------------------------------------------------------------


def _is_metadata_word_simple(word: str) -> bool:
    """Quick check if a word is a known metadata keyword."""
    lower = word.lower().strip()
    if lower in _VIDEO_CODECS or lower in _AUDIO_CODECS or lower in _SOURCES:
        return True
    if lower in {"480p", "540p", "576p", "720p", "1080p", "1080i", "2160p", "4k"}:
        return True
    if lower == "remux":
        return True
    if lower in _LANGUAGES:
        return True
    # Compound audio codecs: AAC2.0, DDP5.1, etc.
    if re.match(
        r"(?:DTS-HDMA|DTS-HD\s*MA|DTS-HD|DTS|DDP|DD|EAC3|E-AC-3|AC3|AAC|FLAC|TrueHD)"
        r"(?:[.\s]?\d\.\d)?$",
        word,
        re.IGNORECASE,
    ):
        return True
    if "-" in word:
        parts = word.split("-")
        return any(
            p.lower() in _AUDIO_CODECS | _VIDEO_CODECS | _SOURCES for p in parts if p
        )
    return False


def detect_convention(text: str) -> str:
    """Detect which naming convention a filename uses.

    Returns one of: "fansub", "scene", "japanese", "bare".
    """
    # Strip extension for analysis
    base = re.sub(r"\.[a-zA-Z0-9]{2,4}$", "", text)

    # Japanese: has 第...話
    if "第" in base and "話" in base and base.startswith("["):
        return "japanese"

    # Fansub: starts with [bracket]
    if base.startswith("["):
        return "fansub"

    # Scene: dot-separated with no spaces (or minimal spaces)
    if "." in base and base.count(".") >= 3:
        # Count spaces vs dots to distinguish
        space_count = base.count(" ")
        dot_count = base.count(".")
        if dot_count > space_count:
            return "scene"

    # Sonarr-style: has " - SxxExx" or "Title SxxExx" with spaces (no brackets)
    if re.search(r"[Ss]\d+[Ee]\d+", base):
        return "sonarr"

    return "bare"


def parse_component_parsy(text: str) -> ParsedMedia:
    """Parse a single filename component using convention detection and dispatch.

    This is the parsy-based equivalent of media_parser.parse_component.
    """
    convention = detect_convention(text)

    result: ParsedMedia | None = None
    if convention == "japanese":
        result = parse_japanese(text)
    elif convention == "scene":
        result = parse_scene(text)
    elif convention == "fansub":
        result = parse_fansub(text)
    elif convention == "sonarr":
        result = parse_sonarr(text)
    elif convention == "bare":
        result = parse_bare(text)

    if result is None:
        # Fallback: try each convention
        for parser in [
            parse_fansub,
            parse_sonarr,
            parse_scene,
            parse_japanese,
            parse_bare,
        ]:
            result = parser(text)
            if result is not None:
                break

    if result is None:
        result = ParsedMedia()
        # Extract extension
        m = re.search(r"\.([a-zA-Z0-9]{2,4})$", text)
        if m:
            result.extension = m.group(0).lower()
            result.series_name = text[: m.start()].strip()
        else:
            result.series_name = text

    return result
