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


# ---------------------------------------------------------------------------
# Media file extensions
# ---------------------------------------------------------------------------

_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".avi"})
_ALL_EXTENSIONS = frozenset(
    {".mkv", ".mp4", ".avi", ".rar", ".iso", ".zip", ".7z", ".webdl"}
)
# Pre-sorted longest-first for _strip_extension (avoids sorting per call)
_ALL_EXTENSIONS_SORTED = tuple(
    sorted(_ALL_EXTENSIONS, key=lambda e: len(e), reverse=True)
)

# ---------------------------------------------------------------------------
# Compound tokens that should NOT be split on dots
# ---------------------------------------------------------------------------

_COMPOUND_TOKENS = {
    "H.264",
    "H.265",
    "H264",
    "H265",
    "AAC2.0",
    "AAC5.1",
    "DD2.0",
    "DD5.1",
    "DDP2.0",
    "DDP5.1",
    "DDP7.1",
    "DTS-HD",
    "E-AC-3",
    "WEB-DL",
    "Blu-Ray",
    "Blu-ray",
    "BDRip",
    "MA.2.0",
    "MA.5.1",
    "MA.7.1",
    "FLAC.2.0",
}

# Build a regex that matches compound tokens (case-insensitive, longest first)
_COMPOUND_RE = re.compile(
    "|".join(
        re.escape(t)
        for t in sorted(_COMPOUND_TOKENS, key=lambda s: len(s), reverse=True)
    ),
    re.IGNORECASE,
)

# Placeholder string that cannot appear in filenames (NUL bytes are stripped
# from input before tokenization, so a single NUL is safe as a sentinel).
_COMPOUND_PLACEHOLDER = "\x00COMPOUND\x00"

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


def _split_scene_dots(text: str) -> list[Token]:
    """Split dot-separated scene text into DOT_TEXT tokens.

    Preserves compound tokens like H.264, AAC2.0, WEB-DL, DTS-HD.
    """
    # Replace compound tokens with placeholders
    compounds_found: list[str] = []

    def _replace(m: re.Match[str]) -> str:
        compounds_found.append(m.group(0))
        return _COMPOUND_PLACEHOLDER

    processed = _COMPOUND_RE.sub(_replace, text)

    # Split on dots
    parts = processed.split(".")
    tokens: list[Token] = []
    compound_idx = 0
    for part in parts:
        if not part:
            continue
        # Restore any placeholders
        while _COMPOUND_PLACEHOLDER in part:
            part = part.replace(_COMPOUND_PLACEHOLDER, compounds_found[compound_idx], 1)
            compound_idx += 1
        tokens.append(Token(kind=TokenKind.DOT_TEXT, text=part))
    return tokens


def _split_separators(text: str) -> list[Token]:
    """Split text on ' - ' separators, producing TEXT and SEPARATOR tokens.

    Only splits on ' - ' (space-dash-space), not bare dashes in words.
    """
    # Pattern: " - " (exactly one dash surrounded by spaces)
    parts = re.split(r" - ", text)
    tokens: list[Token] = []
    for i, part in enumerate(parts):
        if i > 0:
            tokens.append(Token(kind=TokenKind.SEPARATOR, text=" - "))
        stripped = part.strip()
        if stripped:
            tokens.append(Token(kind=TokenKind.TEXT, text=stripped))
    return tokens


def tokenize_component(text: str) -> list[Token]:
    """Tokenize a single path component (directory name or filename).

    Handles brackets [], parens () with nesting, lenticular quotes 「」,
    and detects scene-style dot-separated names.
    """
    # Strip NUL bytes — they can't appear in filenames and would collide
    # with the compound-token placeholder used in scene-style splitting.
    text = text.replace("\x00", "")

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
            tokens.extend(_split_scene_dots(raw))
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

# Vocabulary sets (all lowercase for case-insensitive matching)

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
    }
)

_AUDIO_CODECS = frozenset(
    {
        "aac",
        "flac",
        "opus",
        "dd",
        "ddp",
        "dts",
        "dts-hd",
        "e-ac-3",
        "eac3",
        "ac3",
        "truehd",
        "pcm",
        "lpcm",
    }
)

# Audio codecs that appear as compound tokens with channel info (e.g. AAC2.0)
_RE_AUDIO_COMPOUND = re.compile(
    r"^(AAC|DD|DDP|DTS-HD\s*MA|FLAC|AC3|EAC3|E-AC-3|TrueHD)"
    r"[.\s]?(\d\.\d)$",
    re.IGNORECASE,
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
    }
)

# Map lowercase source keywords to canonical source_type values.
# Keywords not in this map (e.g. "raw") are recognized as SOURCE tokens
# but don't set a source_type.
_SOURCE_TYPE_MAP: dict[str, str] = {
    # Blu-ray
    "bd": "BD",
    "blu-ray": "BD",
    "bluray": "BD",
    "bdrip": "BD",
    "bdremux": "BD",
    # Web / streaming
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
    # DVD
    "dvd": "DVD",
    "dvdrip": "DVD",
    "dvd-r": "DVD-R",
    "dvdr": "DVD-R",
    # TV capture
    "hdtv": "HDTV",
    "sdtv": "SDTV",
    # Optical
    "vcd": "VCD",
    "cd-r": "CD-R",
    "cdr": "CD-R",
}

_RESOLUTIONS = frozenset(
    {
        "480p",
        "540p",
        "576p",
        "720p",
        "1080p",
        "1080i",
        "2160p",
        "4k",
    }
)

_RE_RESOLUTION_DIMS = re.compile(r"^\d{3,4}x\d{3,4}$")

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
        "multisub",
        "msubs",
        "subtitle",
        "multiple subtitle",
    }
)

_RE_SUBTITLE_SOFT = re.compile(r"softsub", re.IGNORECASE)

_RE_CRC32 = re.compile(r"^[0-9A-Fa-f]{8}$")

_RE_YEAR = re.compile(r"^(1[89]\d{2}|20[0-9]{2})$")

# Episode patterns
_RE_EP_SE = re.compile(r"^[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?$")
_RE_EP_NUM = re.compile(r"^(\d{1,4})(?:v(\d+))?$")
_RE_EP_EP = re.compile(r"^[Ee][Pp]?(\d{1,4})(?:v(\d+))?$")
_RE_EP_JP = re.compile(r"^第(\d{1,4})話")  # 第01話
_RE_SEASON_JP = re.compile(r"^第(\d{1,2})期")  # 第1期
_RE_EP_FINAL = re.compile(r"^(\d{1,4})(?:v(\d+))?\s*END$", re.IGNORECASE)
_RE_SEASON_ONLY = re.compile(r"^[Ss](\d{1,2})$")
_RE_SEASON_WORD = re.compile(r"^(\d+)(?:st|nd|rd|th)\s+Season$", re.IGNORECASE)
_RE_SPECIAL = re.compile(r"^(SP|OVA|OAD|ONA)(\d*)$", re.IGNORECASE)
_RE_BATCH_RANGE = re.compile(r"^(\d{1,4})\s*[~～]\s*(\d{1,4})$")
_RE_VERSION = re.compile(r"^v(\d+)$", re.IGNORECASE)
_RE_REPACK = re.compile(r"^REPACK\d?$", re.IGNORECASE)

# Release group detection
_RE_SCENE_TRAILING_GROUP = re.compile(r"^(.*)-([A-Za-z][A-Za-z0-9]{1,})$")

# Site prefix
_RE_SITE_PREFIX = re.compile(r"^www\.\w+\.\w+$", re.IGNORECASE)

# Remux
_RE_REMUX = re.compile(r"remux$", re.IGNORECASE)

# Japanese bonus content markers
_BONUS_KEYWORDS = frozenset(
    {
        "映像特典",
        "ノンテロップ",
        "メニュー画面集",
        "メニュー画面",
        "予告",
        "特典",
        "告知cm",
    }
)

_RE_BONUS_KEYWORD = re.compile(
    "|".join(re.escape(k) for k in _BONUS_KEYWORDS),
    re.IGNORECASE,
)

# Japanese bonus type → English abbreviation for HamaTV naming
_BONUS_TYPE_MAP: dict[str, str] = {
    "ノンテロップOP": "NCOP",
    "ノンテロップED": "NCED",
    "ノンテロップ": "NCOP",  # fallback when OP/ED not specified
    "PV": "PV",
    "予告": "Preview",
    "告知CM": "CM",
    "メニュー画面集": "Menu",
    "メニュー画面": "Menu",
    "映像特典": "Bonus",  # generic bonus marker
    "特典": "Bonus",
}


def classify_bonus_type(text: str) -> str:
    """Extract a bonus type abbreviation from Japanese bonus content text.

    Checks for known Japanese bonus keywords and returns the English
    abbreviation. Handles compound content like ``ノンテロップED「Title」``.
    """
    for jp, en in _BONUS_TYPE_MAP.items():
        if jp in text:
            # Distinguish NCOP vs NCED when ノンテロップ is present
            if jp == "ノンテロップ":
                if "OP" in text or "op" in text:
                    return "NCOP"
                if "ED" in text or "ed" in text:
                    return "NCED"
            return en
    return ""


# HDR
_HDR_KEYWORDS = frozenset(
    {
        "hdr",
        "hdr10",
        "hdr10+",
        "dovi",
        "dolby vision",
    }
)


def _classify_text_content(text: str) -> TokenKind | None:
    """Try to classify a bare text string as a known metadata type.

    Returns the TokenKind if recognized, None otherwise.
    """
    lower = text.lower().strip()

    # Resolution
    if lower in _RESOLUTIONS or _RE_RESOLUTION_DIMS.match(text):
        return TokenKind.RESOLUTION

    # Video codec
    if lower in _VIDEO_CODECS:
        return TokenKind.VIDEO_CODEC

    # Audio codec (simple)
    if lower in _AUDIO_CODECS:
        return TokenKind.AUDIO_CODEC

    # Audio codec (compound like AAC2.0)
    if _RE_AUDIO_COMPOUND.match(text):
        return TokenKind.AUDIO_CODEC

    # Source
    if lower in _SOURCES:
        return TokenKind.SOURCE

    # Language
    if lower in _LANGUAGES:
        return TokenKind.LANGUAGE

    # Subtitle
    if lower in _SUBTITLE_KEYWORDS:
        return TokenKind.SUBTITLE_INFO

    # Remux
    if _RE_REMUX.match(text):
        return TokenKind.REMUX

    # Repack (treat as metadata, not title)
    if _RE_REPACK.match(text):
        return TokenKind.UNKNOWN

    # HDR
    if lower in _HDR_KEYWORDS:
        return TokenKind.UNKNOWN  # Could add HDR token kind later

    # Version alone (v2, v3)
    if _RE_VERSION.match(text):
        return TokenKind.VERSION

    # Site prefix
    if _RE_SITE_PREFIX.match(text):
        return TokenKind.SITE_PREFIX

    return None


def _classify_episode_text(text: str) -> Token | None:
    """Try to parse text as an episode/season identifier.

    Matches the *entire* text. Returns a new Token with extracted numbers,
    or None.
    """
    stripped = text.strip()

    # S01E05 or S01E05v2
    m = _RE_EP_SE.match(stripped)
    if m:
        t = Token(
            kind=TokenKind.EPISODE,
            text=stripped,
            season=int(m.group(1)),
            episode=int(m.group(2)),
        )
        if m.group(3):
            t.version = int(m.group(3))
        return t

    # 第01話 (Japanese episode) — full match only
    m = _RE_EP_JP.match(stripped)
    if m:
        return Token(
            kind=TokenKind.EPISODE,
            text=stripped,
            episode=int(m.group(1)),
        )

    # 第1期 (Japanese season) — full match only
    m = _RE_SEASON_JP.match(stripped)
    if m:
        return Token(
            kind=TokenKind.SEASON,
            text=stripped,
            season=int(m.group(1)),
        )

    # Special: SP1, OVA, OAD, ONA
    m = _RE_SPECIAL.match(stripped)
    if m:
        ep = int(m.group(2)) if m.group(2) else None
        return Token(
            kind=TokenKind.EPISODE,
            text=stripped,
            episode=ep,
        )

    # Season only: S01
    m = _RE_SEASON_ONLY.match(stripped)
    if m:
        return Token(
            kind=TokenKind.SEASON,
            text=stripped,
            season=int(m.group(1)),
        )

    # "4th Season", "2nd Season"
    m = _RE_SEASON_WORD.match(stripped)
    if m:
        return Token(
            kind=TokenKind.SEASON,
            text=stripped,
            season=int(m.group(1)),
        )

    # EP05, E5
    m = _RE_EP_EP.match(stripped)
    if m:
        t = Token(
            kind=TokenKind.EPISODE,
            text=stripped,
            episode=int(m.group(1)),
        )
        if m.group(2):
            t.version = int(m.group(2))
        return t

    # Batch range: 01 ~ 13, 01~26
    m = _RE_BATCH_RANGE.match(stripped)
    if m:
        return Token(
            kind=TokenKind.BATCH_RANGE,
            text=stripped,
            batch_start=int(m.group(1)),
            batch_end=int(m.group(2)),
        )

    # "05 END" or "05v2 END"
    m = _RE_EP_FINAL.match(stripped)
    if m:
        t = Token(
            kind=TokenKind.EPISODE,
            text=stripped,
            episode=int(m.group(1)),
        )
        if m.group(2):
            t.version = int(m.group(2))
        return t

    # Bare number with optional version: "08", "04v2"
    # But NOT 4-digit numbers that look like years (1900-2099)
    m = _RE_EP_NUM.match(stripped)
    if m and not _RE_YEAR.match(m.group(1)):
        t = Token(
            kind=TokenKind.EPISODE,
            text=stripped,
            episode=int(m.group(1)),
        )
        if m.group(2):
            t.version = int(m.group(2))
        return t

    return None


# Patterns for searching *within* a TEXT token for embedded episodes/seasons
_RE_EP_JP_SEARCH = re.compile(r"第(\d{1,4})話")
_RE_SEASON_JP_SEARCH = re.compile(r"第(\d{1,2})期")
_RE_EP_SE_SEARCH = re.compile(r"[Ss](\d{1,2})[Ee](\d{1,4})(?:v(\d+))?")
_RE_SEASON_WORD_SEARCH = re.compile(r"(\d+)(?:st|nd|rd|th)\s+Season", re.IGNORECASE)


def _split_text_with_embedded(token: Token) -> list[Token]:
    """Split a TEXT token that contains embedded episode/season markers.

    E.g. "探偵オペラ 第01話" → [TEXT("探偵オペラ"), EPISODE("第01話")]
    E.g. "Golden Kamuy 4th Season" → [TEXT("Golden Kamuy"), SEASON("4th Season")]

    Returns a list of 1+ tokens.
    """
    text = token.text

    # Try Japanese episode: 第XX話
    m = _RE_EP_JP_SEARCH.search(text)
    if m:
        before = text[: m.start()].strip()
        matched = text[m.start() : m.end()]
        after = text[m.end() :].strip()
        result: list[Token] = []
        if before:
            result.append(Token(kind=TokenKind.TEXT, text=before))
        # Check for 終 (final episode marker) after 話
        ep_text = matched
        if after.startswith("(終)") or after.startswith("（終）"):
            ep_text = matched
            after = after[3:].strip()
        result.append(
            Token(
                kind=TokenKind.EPISODE,
                text=ep_text,
                episode=int(m.group(1)),
            )
        )
        if after:
            result.append(Token(kind=TokenKind.TEXT, text=after))
        return result

    # Try SxxExx embedded in text with spaces
    m = _RE_EP_SE_SEARCH.search(text)
    if m:
        before = text[: m.start()].strip()
        after = text[m.end() :].strip()
        result = []
        if before:
            result.append(Token(kind=TokenKind.TEXT, text=before))
        t = Token(
            kind=TokenKind.EPISODE,
            text=m.group(0),
            season=int(m.group(1)),
            episode=int(m.group(2)),
        )
        if m.group(3):
            t.version = int(m.group(3))
        result.append(t)
        if after:
            result.append(Token(kind=TokenKind.TEXT, text=after))
        return result

    # Try "Nth Season"
    m = _RE_SEASON_WORD_SEARCH.search(text)
    if m:
        before = text[: m.start()].strip()
        after = text[m.end() :].strip()
        result = []
        if before:
            result.append(Token(kind=TokenKind.TEXT, text=before))
        result.append(
            Token(
                kind=TokenKind.SEASON,
                text=m.group(0),
                season=int(m.group(1)),
            )
        )
        if after:
            result.append(Token(kind=TokenKind.TEXT, text=after))
        return result

    # Try leading bare number: "01 Shiroi Koibito-tachi" → EPISODE + TEXT
    m = re.match(r"^(\d{1,4})(?:v(\d+))?\s", text)
    if m and not _RE_YEAR.match(m.group(1)):
        ep_text = m.group(0).strip()
        after = text[m.end() :].strip()
        if after:
            result = [
                Token(
                    kind=TokenKind.EPISODE,
                    text=ep_text,
                    episode=int(m.group(1)),
                    version=int(m.group(2)) if m.group(2) else None,
                )
            ]
            result.append(Token(kind=TokenKind.TEXT, text=after))
            return result

    return [token]


def _expand_metadata_words(text: str) -> list[Token]:
    """Split text on whitespace/commas and classify each word.

    Dash-separated compounds like ``Bluray-1080p`` are sub-split so each
    part can be classified individually.
    """
    tokens: list[Token] = []
    for w in re.split(r"[\s,]+", text):
        w = w.strip()
        if not w:
            continue
        kind = _classify_text_content(w)
        if kind:
            tokens.append(Token(kind=kind, text=w))
        else:
            sub_parts = w.split("-")
            if len(sub_parts) > 1:
                sub_tokens: list[Token] = []
                for sp in sub_parts:
                    sp = sp.strip()
                    if not sp:
                        continue
                    sk = _classify_text_content(sp)
                    if sk:
                        sub_tokens.append(Token(kind=sk, text=sp))
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


def _is_metadata_word(word: str) -> bool:
    """Check if a word (or any of its dash-separated parts) is metadata."""
    if _classify_text_content(word) is not None:
        return True
    if "-" in word:
        return any(
            _classify_text_content(sp) is not None for sp in word.split("-") if sp
        )
    return False


def _count_metadata_words(text: str) -> int:
    """Count how many whitespace/comma-separated words classify as metadata."""
    count = 0
    for w in re.split(r"[\s,]+", text):
        w = w.strip()
        if w and _classify_text_content(w) is not None:
            count += 1
    return count


def _classify_paren(token: Token) -> Token | list[Token]:
    """Classify a PAREN token by its content.

    Returns a single Token for simple cases, or a list of Tokens when
    the paren contains technical metadata that should be expanded.
    """
    text = token.text.strip()

    # Year: (1964), (2024)
    if _RE_YEAR.match(text):
        return Token(kind=TokenKind.YEAR, text=text, year=int(text))

    # Japanese season: (第1期)
    m = _RE_SEASON_JP.match(text)
    if m:
        return Token(kind=TokenKind.SEASON, text=text, season=int(m.group(1)))

    # OVA in parens
    m = _RE_SPECIAL.match(text)
    if m:
        ep = int(m.group(2)) if m.group(2) else None
        return Token(kind=TokenKind.EPISODE, text=text, episode=ep)

    # Subtitle info: softSub(chi+eng)
    if _RE_SUBTITLE_SOFT.search(text):
        return Token(kind=TokenKind.SUBTITLE_INFO, text=text)

    # Technical metadata paren: contains resolution/codec/source keywords
    if _count_metadata_words(text) >= 2:
        return _expand_metadata_words(text)

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
        if token.kind == TokenKind.EPISODE:
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

            words = re.split(r"[\s,]+", token.text)
            meta_count = _count_metadata_words(token.text)
            is_metadata_bracket = meta_count > 0 and meta_count >= len(words) // 2

            # First bracket: release group unless it's metadata
            if not first_bracket_seen:
                first_bracket_seen = True
                if not is_metadata_bracket:
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
                first_is_meta = bool(first_word and _classify_text_content(first_word))
                if not first_is_meta and first_word:
                    first_sub_meta = any(
                        _classify_text_content(sp.strip())
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
                        result.extend(_expand_metadata_words(rest))
                        continue

                result.extend(_expand_metadata_words(token.text))
                continue

            # Short alpha-only bracket (2-6 chars): likely a release group
            stripped = token.text.strip()
            if 2 <= len(stripped) <= 6 and stripped.isalpha():
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
            if _RE_BONUS_KEYWORD.search(token.text):
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
                result.append(Token(kind=TokenKind.YEAR, text=text, year=int(text)))
                continue

            # Known metadata keyword?
            kind = _classify_text_content(text)
            if kind is not None:
                result.append(Token(kind=kind, text=text))
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
                    if _is_metadata_word(w):
                        meta_start = i
                        break
                if meta_start is not None:
                    title_part = " ".join(words[:meta_start]).strip()
                    meta_part = " ".join(words[meta_start:])
                    if title_part:
                        result.append(Token(kind=TokenKind.TEXT, text=title_part))
                    result.extend(_expand_metadata_words(meta_part))
                    continue

            # DOT_TEXT with embedded SxxExx (e.g., "S01E05----Is")
            if token.kind == TokenKind.DOT_TEXT:
                m = _RE_EP_SE_SEARCH.search(text)
                if m:
                    t = Token(
                        kind=TokenKind.EPISODE,
                        text=m.group(0),
                        season=int(m.group(1)),
                        episode=int(m.group(2)),
                    )
                    if m.group(3):
                        t.version = int(m.group(3))
                    result.append(t)
                    continue

            # Scene trailing group: "H.264-VARYG" -> split into codec + group
            if token.kind == TokenKind.DOT_TEXT:
                m = _RE_SCENE_TRAILING_GROUP.match(text)
                if m:
                    prefix = m.group(1)
                    group = m.group(2)
                    # Classify the prefix part
                    prefix_kind = _classify_text_content(prefix)
                    if prefix_kind:
                        result.append(Token(kind=prefix_kind, text=prefix))
                    else:
                        result.append(Token(kind=token.kind, text=prefix))
                    result.append(Token(kind=TokenKind.RELEASE_GROUP, text=group))
                    continue

            # Bonus content in text
            if _RE_BONUS_KEYWORD.search(text):
                result.append(Token(kind=TokenKind.BONUS, text=text))
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
    episode_title: str = ""
    season: int | None = None
    episode: int | None = None
    version: int | None = None
    is_special: bool = False
    special_tag: str = ""
    bonus_type: str = ""  # "NCOP", "NCED", "PV", "CM", "Preview", "Menu", "Bonus"
    batch_range: tuple[int, int] | None = None
    release_group: str = ""
    source_type: str = ""  # "BD", "Web", "DVD", "HDTV", "SDTV", "VCD", "CD-R"
    is_remux: bool = False
    hash_code: str = ""
    resolution: str = ""
    video_codec: str = ""
    audio_codecs: list[str] = field(default_factory=list)
    year: int | None = None
    extension: str = ""
    # From directory component
    path_series_name: str = ""
    path_is_batch: bool = False


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
        TokenKind.UNKNOWN,
        TokenKind.EXTENSION,
        TokenKind.PATH_SEP,
    }
)


def _extract_title_from_tokens(tokens: list[Token]) -> tuple[str, str]:
    """Extract series name and episode title from classified tokens.

    Returns (series_name, episode_title).

    Strategy: collect TEXT/DOT_TEXT tokens that appear in the "title zone"
    (before the first episode marker or metadata-only region).
    After the episode marker, TEXT tokens become episode title (for scene style).
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

        if token.kind == TokenKind.EPISODE:
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

    episode_title = " ".join(ep_title_parts).strip()
    episode_title = re.sub(r"\s+", " ", episode_title)

    return series_name, episode_title


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
                # Check for special episodes
                stripped = token.text.strip().upper()
                if _RE_SPECIAL.match(stripped):
                    pm.is_special = True
                    pm.special_tag = token.text.strip()
        elif token.kind == TokenKind.SEASON:
            if pm.season is None:
                pm.season = token.season
        elif token.kind == TokenKind.VERSION:
            if pm.version is None:
                m = _RE_VERSION.match(token.text)
                if m:
                    pm.version = int(m.group(1))
        elif token.kind == TokenKind.RESOLUTION and not pm.resolution:
            pm.resolution = token.text
        elif token.kind == TokenKind.VIDEO_CODEC and not pm.video_codec:
            pm.video_codec = token.text
        elif token.kind == TokenKind.AUDIO_CODEC:
            pm.audio_codecs.append(token.text)
        elif token.kind == TokenKind.SOURCE:
            if not pm.source_type:
                lower = token.text.lower()
                mapped = _SOURCE_TYPE_MAP.get(lower)
                if mapped:
                    pm.source_type = mapped
                    if "remux" in lower:
                        pm.is_remux = True
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
        elif token.kind == TokenKind.EXTENSION:
            pm.extension = token.text

    # Extract title (preserve bonus-extracted episode_title if already set)
    saved_ep_title = pm.episode_title
    pm.series_name, pm.episode_title = _extract_title_from_tokens(tokens)
    if saved_ep_title and not pm.episode_title:
        pm.episode_title = saved_ep_title

    return pm


def parse_component(text: str) -> ParsedMedia:
    """Parse a single path component (directory or filename) into ParsedMedia."""
    tokens = classify(tokenize_component(text))
    return _build_parsed_media(tokens)


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

    # Parse the most relevant directory component (usually the immediate parent)
    # Use the first non-trivial directory for series name
    dir_pm: ParsedMedia | None = None
    for dp in dir_parts:
        dp = dp.strip()
        if dp:
            dir_pm = parse_component(dp)
            break

    if dir_pm is None:
        return file_pm

    # Merge: directory provides path_series_name, filename is primary
    result = file_pm

    # Directory series name
    if dir_pm.series_name:
        result.path_series_name = dir_pm.series_name

    # Batch info from directory
    if dir_pm.path_is_batch:
        result.path_is_batch = True
    if dir_pm.batch_range and not result.batch_range:
        result.batch_range = dir_pm.batch_range
        result.path_is_batch = True

    # Release group fallback
    if not result.release_group and dir_pm.release_group:
        result.release_group = dir_pm.release_group

    # Source type fallback
    if not result.source_type and dir_pm.source_type:
        result.source_type = dir_pm.source_type

    # Year fallback
    if result.year is None and dir_pm.year is not None:
        result.year = dir_pm.year

    # Season fallback from directory
    if result.season is None and dir_pm.season is not None:
        result.season = dir_pm.season

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
    r"Dual[\s.]Audio|x26[45]|HEVC|AVC|H\.26[45]|"
    r"1080p|720p|2160p|4K|"
    r"FLAC|AAC|DTS|AC3|DD|EAC3)\b",
    re.IGNORECASE,
)


def clean_series_title(name: str) -> str:
    """Extract series title by truncating at the first metadata keyword.

    Handles both space-separated and dot-separated names, e.g.
    ``"Show S01-S02 Dual Audio BDRip x265-GROUP"`` → ``"Show"``
    ``"Show.S02.1080p.BluRay.x265-GROUP"`` → ``"Show"``.
    """
    m = _RE_META_BOUNDARY.search(name)
    if m:
        return name[: m.start()].strip(" .-")
    return name


def name_variants(name: str) -> set[str]:
    """Return all normalized key variants for a series name.

    Produces keys from the raw name, the parser-extracted name (without
    year/quality), and the metadata-truncated name.  Any of these may
    match download index keys.
    """
    keys: set[str] = set()
    for variant in (name, parse_component(name).series_name, clean_series_title(name)):
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
