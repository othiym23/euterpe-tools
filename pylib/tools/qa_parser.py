#!/usr/bin/env python3
"""Interactive QA tool for the media filename parser.

Walks a directory tree, parses each media file with parse_media_path,
and presents the result for human review. Files flagged as problematic
are appended to a JSON Lines log for later analysis.

On startup, if the log file has existing entries, those paths are re-parsed
first so you can confirm fixes. After that review, the tool offers to resume
from where you left off last time (position saved on quit).

Usage:
    uv run python pylib/tools/qa_parser.py /path/to/downloads
    uv run python pylib/tools/qa_parser.py /path/to/downloads --log problems.jsonl
    uv run python pylib/tools/qa_parser.py /path/to/downloads --skip 100  # resume after 100
"""

from __future__ import annotations

import argparse
import json
import os
import readline  # noqa: F401 — imported for side effect (enables line editing in input())
import sys
from dataclasses import asdict
from pathlib import Path

# Add pylib to path so we can import etp_lib
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etp_lib.media_parser import (
    ParsedMedia,
    TokenKind,
    classify,
    parse_media_path,
    tokenize_component,
)

_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".avi", ".flac", ".m4a", ".mp3"})


# ANSI color codes for token kinds
# 256-color ANSI codes (\033[38;5;Nm) for maximum distinction.
# Each token kind gets a unique color so the path and field values
# are visually unambiguous.
def _c(n: int) -> str:
    return f"\033[38;5;{n}m"


_TOKEN_COLORS: dict[TokenKind, str] = {
    TokenKind.TEXT: _c(228),  # bright yellow — series name
    TokenKind.DOT_TEXT: _c(228),  # bright yellow — series name
    TokenKind.EPISODE_TITLE: _c(218),  # light pink — episode title
    TokenKind.RELEASE_GROUP: _c(214),  # orange
    TokenKind.CRC32: _c(245),  # mid gray
    TokenKind.EPISODE: _c(51),  # bright cyan — episode number
    TokenKind.SEASON: _c(39),  # deep sky blue — season number
    TokenKind.SPECIAL: _c(207),  # hot pink
    TokenKind.VERSION: _c(141),  # medium purple
    TokenKind.RESOLUTION: _c(114),  # pale green
    TokenKind.VIDEO_CODEC: _c(79),  # medium aquamarine
    TokenKind.AUDIO_CODEC: _c(180),  # tan / light salmon
    TokenKind.SOURCE: _c(176),  # pink / light magenta
    TokenKind.REMUX: _c(204),  # hot pink (distinct from source)
    TokenKind.YEAR: _c(75),  # steel blue
    TokenKind.BATCH_RANGE: _c(87),  # aquamarine
    TokenKind.SUBTITLE_INFO: _c(102),  # gray-green
    TokenKind.LANGUAGE: _c(103),  # olive gray
    TokenKind.BONUS: _c(213),  # orchid
    TokenKind.DUAL_AUDIO: _c(222),  # light gold
    TokenKind.UNCENSORED: _c(196),  # red
    TokenKind.EDITION: _c(147),  # light steel blue
    TokenKind.HDR: _c(226),  # yellow
    TokenKind.BIT_DEPTH: _c(156),  # light green
    TokenKind.SEPARATOR: _c(240),  # dark gray
    TokenKind.EXTENSION: _c(240),  # dark gray
    TokenKind.SITE_PREFIX: _c(240),  # dark gray
    TokenKind.UNKNOWN: _c(244),  # gray — unclassified metadata
}
_RESET = "\033[0m"


def _colorize(text: str, kind: TokenKind) -> str:
    """Wrap text in ANSI color for a token kind."""
    color = _TOKEN_COLORS.get(kind, "")
    if color:
        return f"{color}{text}{_RESET}"
    return text


def _color_for_field(field: str) -> str:
    """Return the ANSI color code for a parsed field name."""
    field_to_kind: dict[str, TokenKind] = {
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
    kind = field_to_kind.get(field)
    if kind:
        return _TOKEN_COLORS.get(kind, "")
    return ""


_RE_SE_SPLIT = __import__("re").compile(
    r"([Ss]\d{1,2})([Ee]\d{1,4}(?:v\d+)?)", __import__("re").IGNORECASE
)


def _colorize_token_text(text: str, kind: TokenKind) -> str:
    """Colorize a token's text, splitting S01E01 into season+episode colors."""
    if kind == TokenKind.EPISODE:
        m = _RE_SE_SPLIT.match(text)
        if m:
            season_part = _colorize(m.group(1), TokenKind.SEASON)
            ep_part = _colorize(m.group(2), TokenKind.EPISODE)
            rest = text[m.end() :]
            return season_part + ep_part + (_colorize(rest, kind) if rest else "")
    return _colorize(text, kind)


def _colorize_path(rel_path: str) -> str:
    """Colorize a media path by overlaying token classifications.

    Splits the path into components, classifies each, and reconstructs
    the path with ANSI colors applied to each recognized span. Large
    unclassified TEXT tokens (like directory names with metadata) are
    further scanned with scan_words for finer-grained coloring.
    """
    from etp_lib.media_parser import scan_words

    parts = rel_path.split("/")
    colored_parts: list[str] = []

    for part in parts:
        tokens = classify(tokenize_component(part))
        # Reconstruct the part by finding each token's text in order
        result: list[str] = []
        remaining = part
        for token in tokens:
            text = token.text

            # Large TEXT/DOT_TEXT tokens may contain unclassified metadata
            # (e.g., directory names). Scan for finer-grained tokens.
            if token.kind in (TokenKind.TEXT, TokenKind.DOT_TEXT) and " " in text:
                sub_tokens = scan_words(text)
                if any(t.kind != TokenKind.UNKNOWN for t in sub_tokens):
                    # Found classifiable content — colorize each sub-token
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
                            result.append(_colorize_token_text(st.text, kind))
                            sub_remaining = sub_remaining[si + len(st.text) :]
                        else:
                            result.append(_colorize_token_text(st.text, st.kind))
                    if sub_remaining:
                        result.append(sub_remaining)
                    remaining = remaining[idx + len(text) :] if idx >= 0 else remaining
                    continue

            # Find the token text in remaining string
            # For brackets/parens, search for the delimited form
            if token.kind == TokenKind.BRACKET:
                search = f"[{text}]"
            elif token.kind == TokenKind.PAREN:
                search = f"({text})"
            elif token.kind == TokenKind.LENTICULAR:
                search = f"「{text}」"
            else:
                search = text

            idx = remaining.find(search)
            if idx == -1:
                idx = remaining.find(text)

            if idx >= 0:
                if idx > 0:
                    result.append(remaining[:idx])
                display = search if search != text else text
                result.append(_colorize_token_text(display, token.kind))
                remaining = remaining[idx + len(display) :]
            else:
                result.append(_colorize_token_text(text, token.kind))

        if remaining:
            result.append(remaining)

        colored_parts.append("".join(result))

    return "/".join(colored_parts)


def _collect_files(root: Path) -> list[Path]:
    """Walk tree and collect media files, sorted."""
    files = []
    for dirpath, _dirs, filenames in os.walk(root):
        for name in filenames:
            if Path(name).suffix.lower() in _MEDIA_EXTENSIONS:
                files.append(Path(dirpath) / name)
    files.sort()
    return files


def _format_pm(pm: ParsedMedia) -> str:
    """Format ParsedMedia for display, showing only non-empty fields."""
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
            color = _color_for_field(field)
            val_str = f"{color}{value}{_RESET}" if color else str(value)
            lines.append(f"  {field:12s} {val_str}")
    return "\n".join(lines)


def _save_problem(log_path: Path, rel_path: str, pm: ParsedMedia, note: str) -> None:
    """Append a problem entry to the JSONL log."""
    entry = {
        "path": rel_path,
        "note": note,
        "parsed": asdict(pm),
    }
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _state_path(log_path: Path) -> Path:
    """Return the path for the position state file, derived from the log path."""
    return log_path.with_suffix(".state")


def _save_position(log_path: Path, position: int) -> None:
    """Save the current scan position so we can resume later."""
    _state_path(log_path).write_text(str(position), encoding="utf-8")


def _load_position(log_path: Path) -> int | None:
    """Load the saved scan position, or None if no state file exists."""
    sp = _state_path(log_path)
    if sp.exists():
        try:
            return int(sp.read_text(encoding="utf-8").strip())
        except ValueError, OSError:
            return None
    return None


def _load_problems(log_path: Path) -> list[tuple[str, str]]:
    """Load unique (path, note) pairs from the problem log, preserving order."""
    if not log_path.exists():
        return []
    seen: set[str] = set()
    problems: list[tuple[str, str]] = []
    with log_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
                p = entry.get("path", "")
                if p and p not in seen:
                    seen.add(p)
                    problems.append((p, entry.get("note", "")))
            except json.JSONDecodeError:
                continue
    return problems


def _review_prior_problems(problems: list[tuple[str, str]]) -> None:
    """Re-parse and display previously flagged paths for confirmation."""
    total = len(problems)
    print(f"\n{'=' * 70}")
    print(f"Reviewing {total} previously flagged path(s)")
    print(f"{'=' * 70}")

    for i, (rel, note) in enumerate(problems):
        pm = parse_media_path(rel)

        print(f"\n{'=' * 70}")
        print(f"[review {i + 1}/{total}] {_colorize_path(rel)}")
        if note:
            print(f"  note: {note}")
        print(f"{'─' * 70}")
        print(_format_pm(pm))
        print()

        try:
            answer = input("Still a problem? [y/N/q] ").strip().lower()
        except EOFError, KeyboardInterrupt:
            print("\nStopping review.")
            return

        if answer == "q":
            return
        elif answer in ("y", "yes"):
            print("  → Noted, keeping in log.")
        else:
            print("  → Looks fixed!")


def main() -> int:
    parser = argparse.ArgumentParser(description="QA the media filename parser")
    parser.add_argument("directory", type=Path, help="Root directory to scan")
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("parser-problems.jsonl"),
        help="Output file for flagged problems (default: parser-problems.jsonl)",
    )
    parser.add_argument(
        "--skip", type=int, default=None, help="Skip the first N files (for resuming)"
    )
    args = parser.parse_args()

    root = args.directory.resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    # Phase 1: Review previously flagged problems
    problems = _load_problems(args.log)
    if problems:
        print(f"Found {len(problems)} previously flagged path(s) in {args.log}")
        try:
            answer = input("Review them first? [Y/n] ").strip().lower()
        except EOFError, KeyboardInterrupt:
            print()
            return 0

        if answer not in ("n", "no"):
            _review_prior_problems(problems)
            print()

    # Phase 2: Full scan
    files = _collect_files(root)
    total = len(files)
    print(f"Found {total} media files in {root}")

    # Determine starting position
    skip = 0
    if args.skip is not None:
        skip = args.skip
    else:
        saved_pos = _load_position(args.log)
        if saved_pos is not None and saved_pos > 0:
            try:
                answer = (
                    input(f"Resume from position {saved_pos + 1}/{total}? [Y/n] ")
                    .strip()
                    .lower()
                )
            except EOFError, KeyboardInterrupt:
                print()
                return 0
            if answer not in ("n", "no"):
                skip = saved_pos

    if skip:
        print(f"Skipping to file {skip + 1}")

    reviewed = 0
    flagged = 0
    last_position = skip

    for i, filepath in enumerate(files):
        if i < skip:
            continue

        rel = str(filepath.relative_to(root))
        pm = parse_media_path(rel)

        print(f"\n{'=' * 70}")
        print(f"[{i + 1}/{total}] {_colorize_path(rel)}")
        print(f"{'─' * 70}")
        print(_format_pm(pm))
        print()

        try:
            answer = input("Problem? [y/N/q/s(kip 10)] ").strip().lower()
        except EOFError, KeyboardInterrupt:
            print("\nStopping.")
            last_position = i
            break

        if answer == "q":
            last_position = i
            break
        elif answer == "s":
            skip = i + 10
            print(f"  Skipping ahead to file {skip + 1}")
            last_position = i + 10
            continue
        elif answer in ("y", "yes"):
            note = ""
            try:
                note = input("  What's wrong? (optional note): ").strip()
            except EOFError, KeyboardInterrupt:
                pass
            _save_problem(args.log, rel, pm, note)
            flagged += 1
            print(f"  → Saved to {args.log}")

        reviewed += 1
        last_position = i + 1
    else:
        # Completed the full scan
        last_position = total

    _save_position(args.log, last_position)
    print(f"\nDone: {reviewed} reviewed, {flagged} flagged → {args.log}")
    print(f"Position saved ({last_position}/{total}) — will offer to resume next run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
