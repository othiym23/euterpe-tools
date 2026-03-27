# Plan: etp-anime triage mode

## Context

When loose episodes for multiple series are intermingled in
`/volume1/docker/pvr/data/downloads/`, the user must currently run
`etp anime --anidb <id>` once per series. A triage mode would scan the downloads
directory, group files by detected series name, and walk the user through each
group.

## CLI

```
etp anime --triage [--source DIR] [--dest DIR] [--dry-run] [-v]
```

- `--triage` is a new flag, mutually exclusive with `--anidb`/`--tvdb`/`--file`
- `--source` overrides default source dirs (same as existing)
- No metadata ID required upfront — prompted per group

## Workflow

1. Scan source directories for all media files (`.mkv`, `.mp4`, `.avi`)
2. Parse each filename with `parse_source_filename` to extract release group and
   episode number
3. Extract a candidate series name from each filename (strip group, episode
   number, hash, metadata brackets — reuse `_extract_concise_name` logic but
   applied per-file, not per-batch)
4. Group files by normalized candidate name (lowercase, strip punctuation for
   matching; display the original form)
5. Present groups sorted by count descending:
   ```
   Found 5 groups:
     1) Champignon no Majo  (9 files)
     2) Eris no Seihai      (11 files)
     3) Girls und Panzer     (3 files)
     4) Milky Holmes         (2 files)
     5) [ungrouped]          (1 file)
   ```
6. For each group (or user-selected subset):
   - Show the file list
   - Prompt: `AniDB ID, TheTVDB ID, or 's' to skip`
   - If an ID is provided, enter the existing per-series flow: fetch metadata,
     confirm titles, create directory, confirm concise name (defaulting to the
     group's detected name), process files
   - If skipped, move to the next group
7. Print summary of all groups processed

## Implementation

### Changes to `etp/etp-anime`

**New function: `_extract_series_name(filename: str) -> str`**

- Similar to `_extract_concise_name` but operates on a single filename string
  rather than a list of `SourceFile` objects
- Returns the candidate series name for grouping

**New function: `_normalize_for_grouping(name: str) -> str`**

- Lowercase, strip non-alphanumeric characters
- Used as the dict key for grouping; the original name is preserved for display

**New function:
`_scan_and_group(source_dirs: list[Path]) -> dict[str, list[Path]]`**

- Scans source dirs using the existing `find_source_files` iteration pattern
  (single-pass iterdir)
- Groups results by normalized series name
- Returns `{display_name: [file_paths]}` ordered by count descending

**New function: `run_triage(args: argparse.Namespace) -> int`**

- Orchestrates the triage workflow
- Calls `_scan_and_group`, presents groups, prompts for IDs
- For each accepted group, calls into the existing per-series logic (refactored
  slightly — `_fetch_anime_info` currently reads from `args.anidb`/`args.tvdb`,
  so either pass the ID directly or create a modified args namespace)

**Modify `build_parser`:**

- Make `--anidb`/`--tvdb` group no longer `required=True`
- Add `--triage` flag
- Validate at runtime: must have exactly one of `--anidb`, `--tvdb`, or
  `--triage`

**Modify `main`:**

- Route to `run_triage` when `--triage` is set

### Changes to `etp/test_anime.py`

- `TestExtractSeriesName` — unit tests for per-file name extraction
- `TestNormalizeForGrouping` — normalization edge cases
- `TestScanAndGroup` — mock filesystem, verify grouping logic
- `TestCLI` — update mutual exclusivity tests for `--triage`

### Refactoring needed

`_fetch_anime_info` currently reads `args.anidb` / `args.tvdb` from the argparse
namespace. For triage mode, the ID comes from interactive input per group.
Options:

- **Option A**: Extract a `fetch_anime_info(anidb_id=None, tvdb_id=None, ...)`
  function that takes IDs directly, and have `_fetch_anime_info(args)` be a thin
  wrapper. Triage mode calls the inner function.
- **Option B**: Build a synthetic argparse namespace per group.

Option A is cleaner.

Similarly, `run_per_series` bundles "fetch + confirm + find files + process".
For triage mode, the files are already found (they're the group). Factor out a
`process_series(info, files, concise_name, dest, dry_run, verbose)` function
that both `run_per_series` and `run_triage` can call after the setup phase.

## Verification

1. `uv run pytest test_anime.py -q` — all tests pass including new triage tests
2. `just check` — ruff, pyright, ty all clean
3. Manual `--triage --dry-run` against real downloads directory on macOS
4. Deploy and test on NAS with real files
