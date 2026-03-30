#!/usr/bin/env python3
"""Interactive QA tool for the media filename parser.

Walks a directory tree, parses each media file with parse_media_path,
and presents the result for human review. Files flagged as problematic
are appended to a JSON Lines log for later analysis.

Usage:
    uv run python pylib/tools/qa_parser.py /path/to/downloads
    uv run python pylib/tools/qa_parser.py /path/to/downloads --log problems.jsonl
    uv run python pylib/tools/qa_parser.py /path/to/downloads --skip 100  # resume after 100
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

# Add pylib to path so we can import etp_lib
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etp_lib.media_parser import ParsedMedia, parse_media_path

_MEDIA_EXTENSIONS = frozenset({".mkv", ".mp4", ".avi", ".flac", ".m4a", ".mp3"})


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
        ("ep_title", pm.episode_title),
        ("season", pm.season),
        ("episode", pm.episode),
        ("version", pm.version),
        ("special", f"{pm.is_special} ({pm.special_tag})" if pm.is_special else None),
        ("bonus", pm.bonus_type),
        (
            "batch",
            f"{pm.batch_range[0]}~{pm.batch_range[1]}" if pm.batch_range else None,
        ),
        ("group", pm.release_group),
        ("source", pm.source_type),
        ("remux", pm.is_remux if pm.is_remux else None),
        ("res", pm.resolution),
        ("video", pm.video_codec),
        ("audio", ", ".join(pm.audio_codecs) if pm.audio_codecs else None),
        ("hash", pm.hash_code),
        ("year", pm.year),
        ("ext", pm.extension),
        ("dir_series", pm.path_series_name if pm.path_series_name else None),
    ]:
        if value is not None and value != "" and value is not False:
            lines.append(f"  {field:12s} {value}")
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
        "--skip", type=int, default=0, help="Skip the first N files (for resuming)"
    )
    args = parser.parse_args()

    root = args.directory.resolve()
    if not root.is_dir():
        print(f"Not a directory: {root}", file=sys.stderr)
        return 1

    files = _collect_files(root)
    total = len(files)
    print(f"Found {total} media files in {root}\n")

    reviewed = 0
    flagged = 0

    for i, filepath in enumerate(files):
        if i < args.skip:
            continue

        rel = str(filepath.relative_to(root))
        pm = parse_media_path(rel)

        print(f"\n{'=' * 70}")
        print(f"[{i + 1}/{total}] {rel}")
        print(f"{'─' * 70}")
        print(_format_pm(pm))
        print()

        try:
            answer = input("Problem? [y/N/q/s(kip 10)] ").strip().lower()
        except EOFError, KeyboardInterrupt:
            print("\nStopping.")
            break

        if answer == "q":
            break
        elif answer == "s":
            args.skip = i + 10
            print(f"  Skipping ahead to file {args.skip + 1}")
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

    print(f"\nDone: {reviewed} reviewed, {flagged} flagged → {args.log}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
