"""Simple KEY=VALUE environment file loading."""

from __future__ import annotations

import os
from pathlib import Path


def load_env_file(*paths: Path) -> None:
    """Load environment variables from each env file that exists.

    Supports simple KEY=VALUE lines (no quoting, no interpolation).
    Lines starting with ``#`` are ignored. Existing variables are never
    overwritten, so the process environment wins over every file and
    earlier *paths* win over later ones.
    """
    for env_path in paths:
        if not env_path.is_file():
            continue
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())
