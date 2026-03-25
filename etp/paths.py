"""Platform-aware path resolution for euterpe-tools Python porcelain.

Mirrors etp-lib/src/paths.rs conventions:
  - macOS: ~/Library/Application Support/net.aoaioxxysz.etp/
  - Linux: XDG base directories with app name "euterpe-tools"

No external dependencies — uses only stdlib.
"""

import os
import sys
from pathlib import Path

APP_NAME = "euterpe-tools"
BUNDLE_ID = "net.aoaioxxysz.etp"


def _is_macos() -> bool:
    return sys.platform == "darwin"


def config_dir() -> Path:
    """Config directory: ~/Library/Application Support/<bundle>/ on macOS,
    $XDG_CONFIG_HOME/euterpe-tools/ on Linux."""
    if _is_macos():
        return Path.home() / "Library" / "Application Support" / BUNDLE_ID
    xdg = os.environ.get("XDG_CONFIG_HOME")
    if xdg:
        return Path(xdg) / APP_NAME
    return Path.home() / ".config" / APP_NAME


def data_dir() -> Path:
    """Data directory: ~/Library/Application Support/<bundle>/ on macOS,
    $XDG_DATA_HOME/euterpe-tools/ on Linux."""
    if _is_macos():
        return Path.home() / "Library" / "Application Support" / BUNDLE_ID
    xdg = os.environ.get("XDG_DATA_HOME")
    if xdg:
        return Path(xdg) / APP_NAME
    return Path.home() / ".local" / "share" / APP_NAME


def lib_dir() -> Path:
    """Shared Python library directory: $HOME/.local/lib/etp/ on Linux,
    same as config_dir on macOS (libs live alongside the script in dev)."""
    if _is_macos():
        return Path.home() / "Library" / "Application Support" / BUNDLE_ID
    return Path.home() / ".local" / "lib" / "etp"


def config_file() -> Path:
    """Default config file: config.kdl in the config directory."""
    return config_dir() / "config.kdl"


def catalog_config() -> Path:
    """Default catalog config: catalog.kdl in the config directory."""
    return config_dir() / "catalog.kdl"


def db_path() -> Path:
    """Default database: metadata.sqlite in the data directory."""
    return data_dir() / "metadata.sqlite"
