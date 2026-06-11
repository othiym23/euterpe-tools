"""Shared on-disk JSON response cache for metadata provider clients.

TMDB and TheTVDB responses are cached as JSON files for 24 hours
(:data:`~etp_lib.types.CACHE_MAX_AGE_SECONDS`); this module owns the
freshness policy so every client ages and stores entries the same way.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from etp_lib.types import CACHE_MAX_AGE_SECONDS


def load_cached_json(
    cache_file: Path,
    no_cache: bool = False,
    max_age: int = CACHE_MAX_AGE_SECONDS,
) -> Any | None:
    """Return the cached payload, or None when absent, stale, or bypassed."""
    if no_cache or not cache_file.exists():
        return None
    if time.time() - cache_file.stat().st_mtime >= max_age:
        return None
    return json.loads(cache_file.read_text(encoding="utf-8"))


def store_cached_json(cache_file: Path, payload: Any) -> None:
    """Persist *payload* as the cached response."""
    cache_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
