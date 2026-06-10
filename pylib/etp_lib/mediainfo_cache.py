"""Persistent cache of mediainfo analysis results.

Running mediainfo is a subprocess per file against NAS storage — the
dominant cost of planning over a large backlog. Media files are
immutable once a download completes, so results are cached keyed by
absolute path and invalidated by ``(size, mtime)``.

The cache is a process-wide singleton (mirroring the TheTVDB token
cache): call :func:`analyze_file_cached` in place of
:func:`~etp_lib.mediainfo.analyze_file`, and :func:`save_cache` once at
the end of a run to persist new results.
"""

from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path

import msgpack

from etp_lib import mediainfo
from etp_lib.paths import cache_dir
from etp_lib.types import AudioTrack, MediaInfo


def _cache_file() -> Path:
    return cache_dir("mediainfo") / "analysis.msgpack"


_cache: dict[str, dict] | None = None
_dirty = False


def _load() -> dict[str, dict]:
    global _cache
    if _cache is None:
        try:
            raw = _cache_file().read_bytes()
            loaded = msgpack.unpackb(raw, raw=False)
            _cache = loaded if isinstance(loaded, dict) else {}
        except OSError, ValueError, msgpack.UnpackException:
            _cache = {}
    return _cache


def _decode(record: dict) -> MediaInfo:
    tracks = [
        AudioTrack(
            codec=str(t.get("codec", "")),
            language=str(t.get("language", "")),
            title=str(t.get("title", "")),
            is_commentary=bool(t.get("is_commentary", False)),
        )
        for t in record.get("audio_tracks", [])
    ]
    return MediaInfo(
        video_codec=str(record.get("video_codec", "")),
        resolution=str(record.get("resolution", "")),
        width=int(record.get("width", 0)),
        height=int(record.get("height", 0)),
        bit_depth=int(record.get("bit_depth", 8)),
        hdr_type=str(record.get("hdr_type", "")),
        audio_tracks=tracks,
        encoding_lib=str(record.get("encoding_lib", "")),
    )


def analyze_file_cached(path: Path) -> MediaInfo:
    """Analyze *path*, reusing a cached result when size and mtime match.

    Analysis failures propagate (and are never cached), matching
    :func:`~etp_lib.mediainfo.analyze_file`.
    """
    global _dirty
    cache = _load()
    stat = path.stat()
    key = str(path)
    record = cache.get(key)
    if (
        record is not None
        and record.get("size") == stat.st_size
        and record.get("mtime") == int(stat.st_mtime)
        and isinstance(record.get("media"), dict)
    ):
        return _decode(record["media"])

    result = mediainfo.analyze_file(path)
    cache[key] = {
        "size": stat.st_size,
        "mtime": int(stat.st_mtime),
        "media": asdict(result),
    }
    _dirty = True
    return result


def save_cache() -> None:
    """Persist newly analyzed results (atomic write; no-op when clean)."""
    global _dirty
    if not _dirty or _cache is None:
        return
    packed = msgpack.packb(_cache, use_bin_type=True)
    if packed is None:  # pragma: no cover — packb only returns None in stream mode
        return
    target = _cache_file()
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_bytes(packed)
    os.replace(tmp, target)
    _dirty = False
