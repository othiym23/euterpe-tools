"""ANSI color helpers for media filename display.

Provides token-level colorization of media paths and formatted display of
ParsedMedia objects. Used by both the QA tool and the manifest workflow to
give a visual breakdown of parsed filename components.

Color depth is detected from the terminal environment:
  - 256-color (COLORTERM or "256color" in TERM)
  - 16-color (any other TERM with "color")
  - no color (dumb terminal, non-TTY, or NO_COLOR set)
"""

from __future__ import annotations

import os
import re
import sys
from enum import IntEnum

from etp_lib.media_parser import (
    ParsedMedia,
    TokenKind,
    classify,
    scan_words,
    tokenize_component,
)


# ---------------------------------------------------------------------------
# Color depth detection
# ---------------------------------------------------------------------------


class ColorDepth(IntEnum):
    NONE = 0
    BASIC = 16
    FULL = 256


def detect_color_depth() -> ColorDepth:
    """Detect terminal color support from the environment."""
    # NO_COLOR convention (https://no-color.org/)
    if "NO_COLOR" in os.environ:
        return ColorDepth.NONE

    # Non-TTY stdout
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return ColorDepth.NONE

    term = os.environ.get("TERM", "")
    colorterm = os.environ.get("COLORTERM", "")

    # COLORTERM=truecolor or 24bit implies 256-color support
    if colorterm in ("truecolor", "24bit", "256color"):
        return ColorDepth.FULL

    # TERM contains "256color"
    if "256color" in term:
        return ColorDepth.FULL

    # Any color terminal
    if "color" in term or colorterm:
        return ColorDepth.BASIC

    # dumb or unknown
    if term in ("", "dumb"):
        return ColorDepth.NONE

    # Default: assume basic color if TERM is set to something
    return ColorDepth.BASIC


_color_depth: ColorDepth | None = None


def _get_color_depth() -> ColorDepth:
    global _color_depth
    if _color_depth is None:
        _color_depth = detect_color_depth()
    return _color_depth


def set_color_depth(depth: ColorDepth) -> None:
    """Override the detected color depth (useful for testing)."""
    global _color_depth
    _color_depth = depth


# ---------------------------------------------------------------------------
# Named color palette (256-color numbers)
# ---------------------------------------------------------------------------

_PALETTE: dict[str, int] = {
    "bright_yellow": 228,
    "light_pink": 218,
    "orange": 214,
    "mid_gray": 245,
    "bright_cyan": 51,
    "sky_blue": 39,
    "hot_pink": 207,
    "purple": 141,
    "pale_green": 114,
    "aquamarine": 79,
    "tan": 180,
    "pink": 176,
    "rose": 204,
    "steel_blue": 75,
    "light_aqua": 87,
    "gray_green": 102,
    "olive_gray": 103,
    "orchid": 213,
    "light_gold": 222,
    "red": 196,
    "light_steel": 147,
    "yellow": 226,
    "green": 156,
    "dark_gray": 240,
    "gray": 244,
}

# ---------------------------------------------------------------------------
# Color code generators
# ---------------------------------------------------------------------------


def _c256(n: int) -> str:
    return f"\033[38;5;{n}m"


def _c16(code: str) -> str:
    return f"\033[{code}m"


# 16-color fallbacks: palette name -> ANSI SGR parameter.
# 14 usable codes (excluding 30 black and 90 bright-black which vanish on
# dark backgrounds) for 25 palette entries, so some sharing is inevitable.
# Tokens that commonly co-occur in a filename (group, series, episode,
# resolution, video codec, audio codec, CRC, extension) get distinct codes;
# sharing is limited to tokens that rarely appear together.
_PALETTE_TO_16: dict[str, str] = {
    "bright_yellow": "93",  # TEXT — bright yellow
    "light_pink": "95",  # EPISODE_TITLE — bright magenta
    "orange": "33",  # RELEASE_GROUP — yellow
    "mid_gray": "37",  # CRC32 — white
    "bright_cyan": "96",  # EPISODE — bright cyan
    "sky_blue": "94",  # SEASON — bright blue
    "hot_pink": "31",  # SPECIAL — red
    "purple": "35",  # VERSION — magenta
    "pale_green": "92",  # RESOLUTION — bright green
    "aquamarine": "36",  # VIDEO_CODEC — cyan
    "tan": "97",  # AUDIO_CODEC — bright white
    "pink": "34",  # SOURCE — blue
    "rose": "31",  # REMUX — red (shares with SPECIAL)
    "steel_blue": "94",  # YEAR — bright blue (shares with SEASON)
    "light_aqua": "96",  # BATCH_RANGE — bright cyan (shares with EPISODE)
    "gray_green": "32",  # SUBTITLE_INFO — green
    "olive_gray": "36",  # LANGUAGE — cyan (shares with VIDEO_CODEC)
    "orchid": "95",  # BONUS — bright magenta (shares with EPISODE_TITLE)
    "light_gold": "93",  # DUAL_AUDIO — bright yellow (shares with TEXT)
    "red": "91",  # UNCENSORED — bright red
    "light_steel": "35",  # EDITION — magenta (shares with VERSION)
    "yellow": "92",  # HDR — bright green (shares with RESOLUTION)
    "green": "32",  # BIT_DEPTH — green (shares with SUBTITLE_INFO)
    "dark_gray": "37",  # SEPARATOR/EXT/SITE — white (shares with CRC32)
    "gray": "37",  # UNKNOWN — white (shares with CRC32)
}


# Reverse map: 256-color number -> 16-color SGR code
_256_TO_16: dict[int, str] = {
    num: _PALETTE_TO_16[name]
    for name, num in _PALETTE.items()
    if name in _PALETTE_TO_16
}


def _make_color(n256: int) -> str:
    """Return the appropriate escape sequence for a 256-color code."""
    depth = _get_color_depth()
    if depth == ColorDepth.NONE:
        return ""
    if depth == ColorDepth.BASIC:
        code = _256_TO_16.get(n256)
        return _c16(code) if code else ""
    return _c256(n256)


def _reset() -> str:
    if _get_color_depth() == ColorDepth.NONE:
        return ""
    return "\033[0m"


# ---------------------------------------------------------------------------
# Token color mapping
# ---------------------------------------------------------------------------

_TOKEN_COLORS: dict[TokenKind, int] = {
    TokenKind.TEXT: _PALETTE["bright_yellow"],
    TokenKind.DOT_TEXT: _PALETTE["bright_yellow"],
    TokenKind.EPISODE_TITLE: _PALETTE["light_pink"],
    TokenKind.RELEASE_GROUP: _PALETTE["orange"],
    TokenKind.CRC32: _PALETTE["mid_gray"],
    TokenKind.EPISODE: _PALETTE["bright_cyan"],
    TokenKind.SEASON: _PALETTE["sky_blue"],
    TokenKind.SPECIAL: _PALETTE["hot_pink"],
    TokenKind.VERSION: _PALETTE["purple"],
    TokenKind.RESOLUTION: _PALETTE["pale_green"],
    TokenKind.VIDEO_CODEC: _PALETTE["aquamarine"],
    TokenKind.AUDIO_CODEC: _PALETTE["tan"],
    TokenKind.SOURCE: _PALETTE["pink"],
    TokenKind.REMUX: _PALETTE["rose"],
    TokenKind.YEAR: _PALETTE["steel_blue"],
    TokenKind.BATCH_RANGE: _PALETTE["light_aqua"],
    TokenKind.SUBTITLE_INFO: _PALETTE["gray_green"],
    TokenKind.LANGUAGE: _PALETTE["olive_gray"],
    TokenKind.BONUS: _PALETTE["orchid"],
    TokenKind.DUAL_AUDIO: _PALETTE["light_gold"],
    TokenKind.UNCENSORED: _PALETTE["red"],
    TokenKind.EDITION: _PALETTE["light_steel"],
    TokenKind.HDR: _PALETTE["yellow"],
    TokenKind.BIT_DEPTH: _PALETTE["green"],
    TokenKind.SEPARATOR: _PALETTE["dark_gray"],
    TokenKind.EXTENSION: _PALETTE["dark_gray"],
    TokenKind.SITE_PREFIX: _PALETTE["dark_gray"],
    TokenKind.UNKNOWN: _PALETTE["gray"],
}

_FIELD_TO_KIND: dict[str, TokenKind] = {
    "series": TokenKind.TEXT,
    "alt_title": TokenKind.TEXT,
    "ep_title": TokenKind.EPISODE_TITLE,
    "season": TokenKind.SEASON,
    "episode": TokenKind.EPISODE,
    "episodes": TokenKind.EPISODE,
    "version": TokenKind.VERSION,
    "special": TokenKind.SPECIAL,
    "bonus": TokenKind.BONUS,
    "batch": TokenKind.BATCH_RANGE,
    "group": TokenKind.RELEASE_GROUP,
    "source": TokenKind.SOURCE,
    "streamer": TokenKind.SOURCE,
    "remux": TokenKind.REMUX,
    "dual-audio": TokenKind.DUAL_AUDIO,
    "criterion": TokenKind.EDITION,
    "uncensored": TokenKind.UNCENSORED,
    "res": TokenKind.RESOLUTION,
    "bit_depth": TokenKind.BIT_DEPTH,
    "hdr": TokenKind.HDR,
    "video": TokenKind.VIDEO_CODEC,
    "audio": TokenKind.AUDIO_CODEC,
    "hash": TokenKind.CRC32,
    "year": TokenKind.YEAR,
    "ext": TokenKind.EXTENSION,
    "dir_series": TokenKind.TEXT,
}

_RE_SE_SPLIT = re.compile(r"([Ss]\d{1,2})([Ee]\d{1,4}(?:v\d+)?)", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def colorize(text: str, kind: TokenKind) -> str:
    """Wrap text in ANSI color for a token kind."""
    n = _TOKEN_COLORS.get(kind)
    if n is not None:
        color = _make_color(n)
        if color:
            return f"{color}{text}{_reset()}"
    return text


def color_for_field(field: str) -> str:
    """Return the ANSI color code for a parsed field name."""
    kind = _FIELD_TO_KIND.get(field)
    if kind is not None:
        n = _TOKEN_COLORS.get(kind)
        if n is not None:
            return _make_color(n)
    return ""


def colorize_token_text(text: str, kind: TokenKind) -> str:
    """Colorize a token's text, splitting S01E01 into season+episode colors."""
    if kind == TokenKind.EPISODE:
        m = _RE_SE_SPLIT.match(text)
        if m:
            season_part = colorize(m.group(1), TokenKind.SEASON)
            ep_part = colorize(m.group(2), TokenKind.EPISODE)
            rest = text[m.end() :]
            return season_part + ep_part + (colorize(rest, kind) if rest else "")
    return colorize(text, kind)


def colorize_path(rel_path: str) -> str:
    """Colorize a media path by overlaying token classifications.

    Splits the path into components, classifies each, and reconstructs
    the path with ANSI colors applied to each recognized span. Large
    unclassified TEXT tokens (like directory names with metadata) are
    further scanned with scan_words for finer-grained coloring.
    """
    parts = rel_path.split("/")
    colored_parts: list[str] = []

    for part in parts:
        tokens = classify(tokenize_component(part))
        result: list[str] = []
        remaining = part
        for token in tokens:
            text = token.text

            if token.kind in (TokenKind.TEXT, TokenKind.DOT_TEXT) and " " in text:
                sub_tokens = scan_words(text)
                if any(t.kind != TokenKind.UNKNOWN for t in sub_tokens):
                    idx = remaining.find(text)
                    if idx > 0:
                        result.append(remaining[:idx])
                    sub_remaining = text
                    for st in sub_tokens:
                        si = sub_remaining.find(st.text)
                        if si > 0:
                            result.append(sub_remaining[:si])
                        if si >= 0:
                            kind = (
                                st.kind if st.kind != TokenKind.UNKNOWN else token.kind
                            )
                            result.append(colorize_token_text(st.text, kind))
                            sub_remaining = sub_remaining[si + len(st.text) :]
                        else:
                            result.append(colorize_token_text(st.text, st.kind))
                    if sub_remaining:
                        result.append(sub_remaining)
                    remaining = remaining[idx + len(text) :] if idx >= 0 else remaining
                    continue

            if token.kind == TokenKind.BRACKET:
                search = f"[{text}]"
            elif token.kind == TokenKind.PAREN:
                search = f"({text})"
            elif token.kind == TokenKind.LENTICULAR:
                search = f"\u300c{text}\u300d"
            else:
                search = text

            idx = remaining.find(search)
            if idx == -1:
                idx = remaining.find(text)

            if idx >= 0:
                if idx > 0:
                    result.append(remaining[:idx])
                display = search if search != text else text
                result.append(colorize_token_text(display, token.kind))
                remaining = remaining[idx + len(display) :]
            else:
                result.append(colorize_token_text(text, token.kind))

        if remaining:
            result.append(remaining)

        colored_parts.append("".join(result))

    return "/".join(colored_parts)


def format_parsed_media(pm: ParsedMedia) -> str:
    """Format ParsedMedia for display, showing only non-empty fields."""
    reset = _reset()
    lines = []
    for field, value in [
        ("series", pm.series_name),
        ("alt_title", pm.series_name_alt if pm.series_name_alt else None),
        ("ep_title", pm.episode_title),
        ("season", pm.season),
        ("episode", pm.episode),
        (
            "episodes",
            ", ".join(str(e) for e in pm.episodes) if pm.episodes else None,
        ),
        ("version", pm.version),
        ("special", f"{pm.is_special} ({pm.special_tag})" if pm.is_special else None),
        ("bonus", pm.bonus_type),
        (
            "batch",
            f"{pm.batch_range[0]}~{pm.batch_range[1]}" if pm.batch_range else None,
        ),
        ("group", pm.release_group),
        ("source", pm.source_type),
        ("streamer", pm.streaming_service if pm.streaming_service else None),
        ("remux", pm.is_remux if pm.is_remux else None),
        ("dual-audio", pm.is_dual_audio if pm.is_dual_audio else None),
        ("criterion", pm.is_criterion if pm.is_criterion else None),
        ("uncensored", pm.is_uncensored if pm.is_uncensored else None),
        ("res", pm.resolution),
        ("bit_depth", f"{pm.bit_depth}bit" if pm.bit_depth else None),
        ("hdr", pm.hdr if pm.hdr else None),
        ("video", pm.video_codec),
        ("audio", ", ".join(pm.audio_codecs) if pm.audio_codecs else None),
        ("hash", pm.hash_code),
        ("year", pm.year),
        ("ext", pm.extension),
        ("dir_series", pm.path_series_name if pm.path_series_name else None),
    ]:
        if value is not None and value != "" and value is not False:
            color = color_for_field(field)
            val_str = f"{color}{value}{reset}" if color else str(value)
            lines.append(f"  {field:12s} {val_str}")
    return "\n".join(lines)
