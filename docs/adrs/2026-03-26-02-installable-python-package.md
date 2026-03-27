# Installable Python package with src layout

Date: 2026-03-26

## Status

Accepted

## Context

The Python plumbing (`etp-anime`, `etp-catalog`, shared libraries `paths.py`,
`media_parser.py`, vendored `kdl/`) was deployed by copying individual files
into `~/.local/lib/etp/` and `~/bin/`. Scripts used `sys.path` manipulation to
find shared modules at runtime. This was fragile — import errors were common
during development, the vendored `kdl/` directory had to be excluded from every
linting and type-checking tool, and adding a new shared module required updating
the `sys.path` hack in every script.

## Decision

Restructure `etp/` as an installable Python package using the `src` layout
convention:

- `src/etp_lib/` — shared library (paths, media_parser)
- `src/etp_commands/` — CLI entry points (dispatcher, anime, catalog)
- `tests/` — pytest test suite

Entry points are declared in `pyproject.toml` under `[project.scripts]`. The
`kdl-py` vendored copy is replaced with a PyPI dependency. Deployment uses
`uv tool install .` which handles the venv, dependencies, and PATH wiring.

Rust plumbing binaries move from `~/bin` to `~/.local/libexec/etp/` (FHS
convention for executables not intended for direct user invocation). The
dispatcher and catalog find them via `etp_lib.paths.find_binary()` which
searches `$ETP_LIBEXEC_DIR`, `~/.local/libexec/etp/`, then `$PATH`.

## Consequences

- `sys.path` hacking is eliminated — all imports use standard package paths.
- Adding a new shared module requires no changes to import machinery.
- `kdl-py` is a managed dependency instead of vendored code — no more linter
  exclusions.
- NAS deployment is `ssh host "cd ~/.local/src/etp && uv tool install ."`
  instead of manual file copying.
- Development uses `uv sync` for an editable install.
- Requires `uv` on the NAS (available via pipx).

Supersedes the `sys.path` + vendored `kdl/` approach from the original SP1.5
implementation.
