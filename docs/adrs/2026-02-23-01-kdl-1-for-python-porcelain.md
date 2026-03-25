# Use KDL 1 for Python porcelain config parsing

Date: 2026-02-23

## Status

Accepted

## Context

The Python porcelain (`etp-catalog`) needs to parse KDL configuration files. The
Rust side uses `knuffel` 3.2, which implements KDL 1. The original SP1.5 plan
called for `kdl-py>=2.0`, but that version does not exist on PyPI — the latest
published release is 1.2.0 (January 2024), which implements KDL 1.0.0.

The `kdl-py` GitHub repository's `main` branch has KDL 2.0.0 support, but this
has not been released to PyPI. There is no timeline for a release.

KDL 2 introduces breaking syntax changes (multi-line strings, revised escaping
rules, type annotations), but none of these features are needed by the current
`catalog.kdl` schema, which uses only basic KDL 1 features: string arguments,
child nodes, and slashdash comments.

## Decision

Use `kdl-py` 1.2.0 (KDL 1.0.0) for the Python porcelain, vendored into
`etp/kdl/` so deployment requires no pip install step. Both `knuffel` (Rust) and
`kdl-py` (Python) implement KDL 1, so configuration files are interchangeable
between the two parsers.

Revisit when either `kdl-py` publishes a KDL 2 release to PyPI, or `knuffel` is
replaced with a KDL 2 parser on the Rust side.

## Consequences

- Both Rust and Python parse KDL 1 — config files work identically in both
  parsers.
- The `catalog.kdl` schema must avoid KDL 2-only syntax to remain compatible.
- If a future KDL 2 feature is needed (e.g., multi-line strings for complex
  paths), both parsers must be upgraded in lockstep.
- Supersedes the `kdl-py>=2.0` version specified in the original SP1.5 plan.

See also:
[2026-02-22-03 — Use KDL with knuffel for configuration](2026-02-22-03-kdl-configuration.md).
