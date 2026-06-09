"""Recursive media-file discovery and parsing for ingest source directories."""

from __future__ import annotations

import os
from pathlib import Path

from etp_lib import media_parser
from etp_lib.media_vocab import (
    _MEDIA_EXTENSIONS,
    _SCAN_EXCLUDE_DIRS,
    _VIDEO_EXTENSIONS,
)
from etp_lib.types import ParsedMetadata, SourceFile


def parse_source_filename(filename: str) -> SourceFile:
    """Parse a media release filename into a SourceFile.

    Delegates to media_parser for tokenization and classification.
    """
    pm = media_parser.parse_component(filename)
    return SourceFile(
        path=Path(filename),
        parsed=ParsedMetadata(
            series_name=pm.series_name,
            release_group=pm.release_group,
            source_type=pm.source_type,
            is_remux=pm.is_remux,
            hash_code=pm.hash_code,
            episode=pm.episode,
            season=pm.season,
            version=pm.version,
            bonus_type=pm.bonus_type,
            is_special=pm.is_special,
            special_tag=pm.special_tag,
            episode_title=pm.episode_title,
            is_dual_audio=pm.is_dual_audio,
            is_uncensored=pm.is_uncensored,
            series_name_alt=pm.series_name_alt,
            episodes=pm.episodes,
            streaming_service=pm.streaming_service,
        ),
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
