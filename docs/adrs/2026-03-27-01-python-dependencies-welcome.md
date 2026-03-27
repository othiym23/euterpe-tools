# Python dependencies are welcome

Date: 2026-03-27

## Status

Accepted (supersedes the dependency-avoidance philosophy in earlier ADRs)

## Context

Early project decisions were shaped by the NAS deployment constraint: Python
scripts had to run on the Synology's stock Python 3.8 with no package manager
available. This led to vendoring libraries (tomli, kdl), avoiding PyPI
dependencies, and implementing functionality inline to avoid `pip install` on
the NAS (see [2026-02-14-01](2026-02-14-01-python-38-vendored-tomli.md),
[2026-03-24-03](2026-03-24-03-xdg-catalog-config-resolution.md)).

The environment has since changed:

- Python 3.14 is installed on the NAS (via a community Synology package).
- `uv` is installed on the NAS (via pipx), providing fast, reliable package
  management and virtual environment creation.
- The Python plumbing is an installable package with `pyproject.toml` dependency
  declarations (see
  [2026-03-26-02](2026-03-26-02-installable-python-package.md)).
- `uv tool install .` handles venv creation, dependency resolution, and PATH
  wiring in a single command.

The original rationale for avoiding dependencies — no package manager on the NAS
— no longer applies.

## Decision

PyPI dependencies are welcome in the Python plumbing. When a well-maintained
library solves a problem better than hand-rolled code, prefer the library.

Guidelines:

- **Prefer standard library** when it's sufficient (pathlib, json, re, etc.).
- **Use PyPI packages** when they provide meaningful value: correctness,
  maintenance burden, or functionality that would be complex to implement (HTTP
  clients, metadata APIs, rich terminal output, etc.).
- **Declare all dependencies** in `pyproject.toml` — `uv` handles the rest.
- **Rust crates** remain statically linked for the musl target. The Rust side
  has always managed dependencies via Cargo and is unaffected by this change.

## Consequences

- New Python functionality can leverage the PyPI ecosystem without friction.
- `uv tool install .` on the NAS installs all declared dependencies
  automatically.
- Earlier ADRs that cite "no new dependencies" or "avoid pip" as decision
  factors should be read in historical context — those constraints no longer
  apply.
- The vendored `tomli` (already superseded) and inline `platformdirs`
  reimplementation in `paths.py` could be replaced by PyPI packages if needed,
  though there is no urgency to do so.
