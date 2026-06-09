# ADR: Plan/Apply Two-Step CLI for Movies and Television Ingestion

**Date:** 2026-06-09 **Status:** Accepted

## Context

`etp anime ingest` is deeply interactive: it prompts for provider IDs on stdin
and opens a KDL manifest in `$EDITOR` for review before copying. That workflow
works well for a human curator but cannot be driven by an LLM agent, which has
no terminal to type into and no editor session.

The new `etp movies ingest` and `etp television ingest` commands are designed
with agents as first-class users, building on the principles in
[2026-04-06-01-cli-design-for-ai-agents.md](2026-04-06-01-cli-design-for-ai-agents.md)
(explicit modes, fast failure, structured output, idempotency, no hidden state,
composability).

## Decision

Ingestion is split into two non-interactive subcommands sharing one core
(`etp_lib/video_ingest.py`):

- **`ingest plan`** scans the sources, resolves provider IDs, analyzes files,
  and writes a KDL plan manifest plus a summary (stable JSON on stdout with
  `--json`). It never writes to the library and never prompts.
- **`ingest apply MANIFEST`** validates the manifest against the live filesystem
  — all violations reported at once, nothing copied unless the whole manifest is
  clean — then executes reflink copies and subtitle sidecars and records sources
  in the shared ingest register.

The reviewable artifact between the two steps is a file on disk, editable by any
tool. Agent-editable fields are per-entry `status` (`ready` → `skip`), per-entry
`conflict` (`keep`/`replace`/`both`/`skip`, replacing the anime flow's
interactive k/r/b/s prompt with a declarative choice), and provider IDs on
`needs-id` blocks. `plan --refine PREVIOUS.kdl` carries IDs and decisions
forward into a regenerated manifest, so the agent loop is _plan → edit → plan
--refine → apply_. Destinations are always computed by plan; apply never invents
names.

Ambiguity is data, not a prompt: a title whose metadata search has no single
exact title+year match becomes a `needs-id` block carrying the candidate list,
and apply refuses to run while unresolved `needs-id` entries remain (resolve
them or mark them `skip`).

Exit codes follow the established convention: 0 success, 1 failure (validation
errors, drift, copy failures), 2 nothing to do. With `--json`, stdout carries
exactly one JSON document; all human-facing output goes to stderr.

Idempotency: re-applying a fully-applied manifest succeeds (entries whose
destination already exists at the recorded size count as already done; exit 2
when nothing new was copied). A source file that changed size since planning is
drift and fails validation.

## Consequences

- Humans use the same flow with a text editor between the two steps; there is no
  separate interactive mode to maintain.
- A plan manifest is a snapshot: it encodes sizes and destinations at plan time,
  and apply re-validates them rather than trusting them.
- `etp anime ingest` keeps its interactive workflow unchanged; migrating it to
  plan/apply is possible later but out of scope here.
- The manifest schema is versioned (`schema-version 1`); apply rejects manifests
  with a different version.

## Related

- [2026-04-06-01-cli-design-for-ai-agents.md](2026-04-06-01-cli-design-for-ai-agents.md)
  — extended by this decision.
- [2026-04-19-02-anime-triage-manifest-format.md](2026-04-19-02-anime-triage-manifest-format.md)
  — the anime manifest format continues unchanged; the plan manifest is a new
  schema, not a replacement.
