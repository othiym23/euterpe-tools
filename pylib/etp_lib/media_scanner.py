"""Recursive media-file discovery for ingest source directories."""

from __future__ import annotations

import os
from pathlib import Path

from etp_lib.media_vocab import (
    _MEDIA_EXTENSIONS,
    _SCAN_EXCLUDE_DIRS,
    _VIDEO_EXTENSIONS,
)


def iter_media_files(
    source_dirs: list[Path], include_audio: bool = False
) -> list[Path]:
    """Walk source directories recursively for media files.

    By default only video files are returned.  Set *include_audio* to
    also collect audio files (for the QA tool or extras detection).

    Skips download-client working directories (``temp``, ``incomplete``,
    etc.) — files there are still being downloaded and shouldn't be
    ingested.
    """
    extensions = _MEDIA_EXTENSIONS if include_audio else _VIDEO_EXTENSIONS
    results: list[Path] = []
    for source_dir in source_dirs:
        if not source_dir.is_dir():
            continue
        for root, dirs, files in os.walk(source_dir):
            # Prune excluded directories in place so os.walk skips them
            dirs[:] = [d for d in dirs if d.lower() not in _SCAN_EXCLUDE_DIRS]
            for name in files:
                if Path(name).suffix.lower() in extensions:
                    results.append(Path(root) / name)
    return results
