#!/usr/bin/env python3
"""Walk the NAS downloads directory and save relative paths as a JSON fixture.

Usage:
    uv run python pylib/tools/gen_corpus_fixture.py

Requires the NAS to be mounted at /Volumes/docker.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

DOWNLOADS_DIR = "/Volumes/docker/pvr/data/downloads"
FIXTURE_PATH = (
    Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "corpus_paths.json"
)
MEDIA_EXTENSIONS = {".mkv", ".mp4", ".avi"}


def main() -> int:
    if not os.path.isdir(DOWNLOADS_DIR):
        print(f"error: {DOWNLOADS_DIR} is not mounted.", file=sys.stderr)
        return 1

    all_paths: list[str] = []
    media_paths: list[str] = []

    for root, dirs, files in os.walk(DOWNLOADS_DIR):
        for name in files + dirs:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, DOWNLOADS_DIR)
            all_paths.append(rel)
            ext = os.path.splitext(name)[1].lower()
            if ext in MEDIA_EXTENSIONS and os.path.isfile(full):
                media_paths.append(rel)

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FIXTURE_PATH.write_text(
        json.dumps(
            {"all": sorted(all_paths), "media": sorted(media_paths)},
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Wrote {len(all_paths)} paths ({len(media_paths)} media) to {FIXTURE_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
