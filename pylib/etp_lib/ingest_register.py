"""Shared register of already-ingested source files.

All ingest commands (``etp anime``, ``etp movies``, ``etp television``)
draw on the same downloads directory, so they share one register keyed by
resolved absolute source path — a file copied by one command is never
re-processed or double-counted by another.

The register is a JSON array of resolved absolute paths, stored in the
``ingest`` cache directory. Saves are atomic (write-temp-then-rename) so a
crash mid-save never corrupts the register.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from etp_lib.paths import cache_dir


def register_path() -> Path:
    """Path to the shared ingest register."""
    return cache_dir("ingest") / "copied.json"


def legacy_register_path() -> Path:
    """Pre-sharing location, written only by ``etp anime`` triage."""
    return cache_dir("triage") / "copied.json"


def _read_register(path: Path) -> set[str]:
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError, TypeError:
            return set()
    return set()


def load_register() -> set[str]:
    """Load the set of previously copied source file paths.

    Merges in the legacy anime-only triage register so files ingested
    before the register was shared are not re-processed. The legacy file
    is left in place (read-only fallback); the merged set lands in the
    new location on the next :func:`save_register`.
    """
    return _read_register(register_path()) | _read_register(legacy_register_path())


def save_register(copied: set[str]) -> None:
    """Persist the register atomically."""
    path = register_path()
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(sorted(copied), ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)
