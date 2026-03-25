# XDG-based path resolution for Python porcelain

Date: 2026-03-24

## Status

Accepted (supersedes default config path in etp-catalog)

## Context

`etp-catalog` currently defaults to `catalog.kdl` in the script's own directory.
This only works when the config is co-located with the script — it breaks when
the binary is installed to `$PATH` on the NAS and the config lives elsewhere.

The Rust plumbing already uses `etcetera` for platform-aware path resolution
(see `docs/adrs/2026-02-22-07-etcetera-xdg-paths.md`), resolving to:

- **macOS**: `~/Library/Application Support/net.aoaioxxysz.etp/`
- **Linux**: `$XDG_CONFIG_HOME/euterpe-tools/` (defaulting to
  `~/.config/euterpe-tools/`)

The Python porcelain should follow the same conventions so that both layers
agree on where configuration lives.

## Decision

Add a `paths` module to the Python porcelain that mirrors `etp-lib/src/paths.rs`
conventions:

- **macOS**: `~/Library/Application Support/net.aoaioxxysz.etp/`
- **Linux**: `$XDG_CONFIG_HOME/euterpe-tools/` (default
  `~/.config/euterpe-tools/`)

`etp-catalog` default config resolution becomes:

1. Explicit `--config` / positional argument (unchanged)
2. `catalog.kdl` in the platform config directory

No new runtime dependencies — the logic is small enough to implement directly.
`platformdirs` is not needed.

The same module provides `data_dir()` for future use (database, CAS), following:

- **macOS**: `~/Library/Application Support/net.aoaioxxysz.etp/`
- **Linux**: `$XDG_DATA_HOME/euterpe-tools/` (default
  `~/.local/share/euterpe-tools/`)

Shared Python library modules (`paths.py`, vendored `kdl/`) are deployed to
`$HOME/.local/lib/etp/` on Linux, following the XDG convention for
user-installed libraries. Scripts resolve the lib directory at startup: if
`~/.local/lib/etp/` exists it is used, otherwise the script's own directory
(development mode).

## Consequences

- `etp catalog` works when binaries are installed to `$PATH` and config is in
  `~/.config/euterpe-tools/catalog.kdl` on the NAS.
- Python and Rust agree on directory locations for each platform.
- The `conf/catalog.kdl` in the repo is a source template — deployment copies it
  to the platform config directory.
- Shared Python modules live in `~/.local/lib/etp/`, not in `~/bin/`.
- No new dependencies in the Python porcelain.
