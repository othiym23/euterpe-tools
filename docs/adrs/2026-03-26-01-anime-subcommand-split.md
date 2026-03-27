# 01. Split etp-anime into triage/series/episode subcommands

Date: 2026-03-26

## Status

Accepted

## Context

etp-anime originally had three modes selected via flags: `--triage` for bulk
import, `--anidb`/`--tvdb` for per-series search-and-import, and `--file` for
single-file import. The distinction was unclear from the CLI, and the per-series
mode overlapped significantly with triage.

The three modes serve genuinely different use cases:

1. **Bulk import from downloads**: Files from multiple series are intermingled
   in a downloads directory. The user needs to group, identify, and organize
   them in batch.

2. **Sync from Sonarr-managed directory**: A PVR like Sonarr maintains a
   well-organized anime directory where files arrive over time and may be
   upgraded to better quality. The user needs to keep the curated collection in
   sync with this source of truth.

3. **Quick single-file import**: A newly downloaded episode or movie needs to be
   slotted into an existing series directory without the overhead of batch
   processing.

## Decision

Replace the flat flag-based mode selection with explicit subcommands:

- `etp anime triage [pattern]` — scan downloads directory, group by series,
  process via editable KDL manifests
- `etp anime series [pattern]` — sync from Sonarr-managed anime directory,
  process via editable KDL manifests
- `etp anime episode <file> --anidb ID | --tvdb ID` — import a single file
  interactively

All three subcommands share a KDL configuration file at
`$XDG_CONFIG_HOME/euterpe-tools/anime-ingestion.kdl` for default paths and
per-series AniDB/TVDB ID mappings. Series mappings are populated automatically
as the user provides IDs during triage or series sync, so each ID only needs to
be entered once.

The old per-series search mode (`--anidb`/`--tvdb` without `--file`) is removed.
Its search functionality is subsumed by `series` (for Sonarr-managed content)
and `triage` (for download directory content).

## Consequences

- The CLI is self-documenting: `etp anime --help` shows the three subcommands
  and their purposes.
- Per-series ID mappings accumulate in the config file, reducing repetitive
  prompts across runs.
- The `series` subcommand enables a new workflow: keeping the curated collection
  in sync with Sonarr's upgrades, using the same manifest-editing flow as
  triage.
- The `episode` subcommand is deliberately simple and reluctant to create new
  directories, preventing accidental proliferation of series directories from
  one-off imports.
- Existing `--triage` and `--file` users must update their invocations, but the
  underlying processing logic is unchanged.
