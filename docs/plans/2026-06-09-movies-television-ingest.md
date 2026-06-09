# `etp movies ingest` + `etp television ingest`

## Context

The anime collection has an ingest pipeline (`etp anime ingest`), but movies and
television are still curated by hand. This adds two new porcelain commands on
feature branch `rei/feat/movies-television` that ingest from the
Radarr/Sonarr-managed trees and the shared downloads directory into
`/volume1/video/movies` and `/volume1/video/television`, producing layouts
compatible with **both Plex and Jellyfin** (Plex docs normative, cross-checked
against Jellyfin docs). Shared logic is extracted from `anime.py` into
`pylib/etp_lib` per the rule "extract when used by ≥2 commands". One unit of
work — optimize for loose coupling and readability, not phasing.

## Decisions (settled with user)

1. **Provider IDs**: Plex brace syntax, primary provider only — TV:
   `Show (Year) {tvdb-NNN}`, movies: `Title (Year) {tmdb-NNN}`. No Jellyfin
   brackets, no dual IDs. (Jellyfin parses braces since PR #14927, Feb 2026;
   otherwise falls back to title+year.)
2. **Dual providers**: per-type primary — TV: TVDB primary (episode
   numbering/titles), TMDB cross-check; movies: TMDB primary, TVDB cross-check.
   Secondary ID recorded in manifest only. Cross-check mismatch = warning, never
   fatal. No AniDB.
3. **CLI shape**: non-interactive **plan/apply two-step**, agent-first. No
   prompts, no `$EDITOR`. Stable JSON via `--json`. Extends ADR
   `2026-04-06-01-cli-design-for-ai-agents.md` (exit codes 0/1/2, fail-fast,
   idempotent).
4. **Naming**: keep the etp bracketed quality block
   (`[Group Source,res,codec,…]` via `build_metadata_block`). Movies: file named
   exactly after its folder, **no `- complete movie` marker** (that's a
   HamaTV/TV-library convention). Editions: `{edition-Final Cut}` in both folder
   and file name. TV episodes: `Show (Year) - s01e01 - Title [block].mkv`,
   **zero-padded season** in the tag (anime keeps `s1e01`),
   `Season NN`/`Specials` folders via existing `season_subdir`.
5. **Shared register**: one register across anime/movies/television (the
   downloads dir is shared) — `cache_dir("ingest")/copied.json`, keyed by
   resolved absolute source path, with merge-on-load migration from legacy
   `cache_dir("triage")/copied.json`.

## CLI

```
etp television ingest plan  [--sonarr|--downloads] [--source DIR]... [--force]
                            [--json] [-o FILE] [--refine FILE] [--config FILE] [--no-cache] [-v] [pattern]
etp television ingest apply MANIFEST.kdl [--dry-run] [--json] [-v]
etp movies ingest plan      [--radarr|--downloads] …same shape…
etp movies ingest apply     MANIFEST.kdl …same shape…
```

- `plan`: scan → parse → resolve IDs (config override → exact title+year search
  → ambiguous = `needs-id` with candidates) → mediainfo enrich → write KDL
  manifest + summary (JSON on stdout with `--json`, human text to stderr). Never
  touches dest.
- `apply`: validate manifest against live filesystem (source exists + size
  matches; dest drift; unresolved `needs-id` not skipped → reject all-at-once,
  exit 1, copy nothing), then reflink-copy + subtitle sidecars, update register
  once at end. Idempotent re-apply.
- `--refine FILE`: agent loop is _plan → edit manifest (fill IDs, set
  `conflict`/`status`) → plan --refine → apply_.
- Exit codes: 0 success, 1 failure, 2 nothing to do. No `ORCHESTRATED` collision
  (that set is only `tree/csv/find/query`).

## Manifest (KDL, schema-version 1)

`meta { tool; schema-version 1; created; source-mode; dest-root }`, then
`series`/`movie` blocks: provider IDs (`tvdb`+`tmdb`), `title`/`year`,
`confidence "exact|high|ambiguous|none"`, `dest-dir`, `candidate` nodes when
ambiguous; per-file `episode`/file entries with
`status "ready|needs-id|conflict|skip"`, `source`, `size`,
`season`/`number`/`title` (TV), `dest`, and declarative
`conflict "keep|replace|both|skip"` (reuses `types.ConflictAction`; `both`
reuses anime's CRC-suffix convention from `manifest.py` execute). Strings via
`manifest.escape_kdl`. Agent-editable fields: `conflict`, `status`, provider IDs
(via `--refine`).

JSON envelope (both actions):
`{tool, action, schema_version, manifest, counts{ready,needs_id,conflict,skip,already_ingested}, warnings[], entries[]}`;
apply adds per-entry `result: copied|kept|replaced|both|skipped|failed`.

## Module layout

**Extracted from `cmd/etp/etp_commands/anime.py` (zero anime behavior change
except register location):**

- `anime.py:566-585` → new **`pylib/etp_lib/ingest_register.py`**:
  `register_path()` = `cache_dir("ingest")/copied.json`, `load_register()`
  (merges legacy `cache_dir("triage")/copied.json` if present),
  `save_register()` (atomic tmp+`os.replace`). Same JSON-array format.
- `anime.py:593-616` `_iter_media_files` → new
  **`pylib/etp_lib/media_scanner.py`** `iter_media_files()` (pure move).
- `anime.py:86-105` `_load_env_file` → new **`pylib/etp_lib/envfile.py`**
  `load_env_file(*paths)`; anime calls with `anime_env()`; new commands call
  with `media_env(), anime_env()` (existing TVDB key keeps working).
- Refactor anime.py call sites (grep
  `_load_triage_manifest|_save_triage_manifest|_iter_media_files`); interactive
  flow, prompts, `$EDITOR`, anime manifest format untouched.

**New shared modules (pylib/etp_lib/):**

- **`tmdb.py`** — TMDB v3 client modeled on `tvdb.py` (stdlib urllib, Bearer
  token, 24h cache in `cache_dir("tmdb")`): `search_movie(title, year)`,
  `search_tv(title, year)`, `fetch_tmdb_movie(id)` (+alternative_titles),
  `fetch_tmdb_tv(id)` (+external_ids, cross-check only).
- **`tvdb.py` addition** — `search_tvdb_series(query)` via
  `GET /search?type=series`, cached.
- **`video_ingest.py`** — the shared core, parameterized by `MediaKind` StrEnum
  (`MOVIE`, `TV`): managed-tree walkers (dedicated regexes for clean
  Radarr/Sonarr naming — `Title (Year)` folders, `SxxEyy`, `- complete movie -`
  marker, edition text — falling back to `media_parser.parse_component`);
  `group_downloads()` for downloads mode (parse_component + grouping; simpler
  than anime's `_scan_and_group`, no AniDB machinery); provider resolution with
  confidence ladder + cross-check; manifest dataclasses +
  `write_plan_manifest`/`parse_plan_manifest`;
  `run_plan(kind, …)`/`run_apply(kind, …)`; `summary_json()`. Search/fetch
  functions injected (like `analyze_file_fn` in `manifest.py`) for testability.
  Respects the non-mutating `MatchedFile` pattern.
- **`media_config.py`** — loader for one `media-ingestion.kdl` (mirrors
  `load_anime_config`, anime.py:113-161) + append-style
  `save_movie_mapping`/`save_series_mapping`.

**Modified pylib modules:**

- **`types.py`**: `MetadataProvider.TMDB`; `MovieInfo` dataclass;
  `AnimeInfo.tmdb_id: int | None = None`; `MediaIngestConfig`; defaults
  `DEFAULT_MOVIES_SOURCE_DIR=/volume1/docker/pvr/data/movies`,
  `DEFAULT_MOVIES_DEST_DIR=/volume1/video/movies`,
  `DEFAULT_TELEVISION_SOURCE_DIR=/volume1/docker/pvr/data/television`,
  `DEFAULT_TELEVISION_DEST_DIR=/volume1/video/television` (NAS defaults; laptop
  `/Volumes/...` via config only, matching existing convention).
- **`naming.py`**: `format_movie_dirname(title, year, tmdb_id, edition)`,
  `format_movie_filename(dirname, source)` (dirname + quality block + ext),
  `format_tv_series_dirname(title, year, tvdb_id)`,
  `format_tv_episode_filename(…)` (zero-padded season, multi-ep `s01e02-e03`
  range). Reuse `build_metadata_block`, `season_subdir`, `_sanitize_path`,
  `subtitle_sidecars`. Anime's `format_episode_filename` untouched.
- **`paths.py`**: `media_config()` → `config_dir()/"media-ingestion.kdl"`,
  `media_env()` → `config_dir()/"media.env"` (TVDB_API_KEY + TMDB_API_KEY).

**New commands + registration:**

- **`cmd/etp/etp_commands/movies.py`**, **`television.py`** — thin (~150 lines
  each): argparse with `ingest plan|apply` nested subparsers, load env+config,
  delegate to `video_ingest` with their `MediaKind`.
- Root **`pyproject.toml`** `[project.scripts]` (line 8): add `etp-movies`,
  `etp-television`.
- **`dispatcher.py`** `BUILTIN_COMMANDS` (line 23): add both. Not
  `ORCHESTRATED`.
- **`CLAUDE.md`**: command synopses.

**Reused as-is:** `conflicts.copy_reflink/compute_crc32/verify_hash`,
`manifest.copy_subtitle_sidecars/escape_kdl`, `mediainfo.analyze_file`,
`media_parser.parse_component`, `download_cache`. **Not extracted**
(anime-specific until a 2nd consumer): `_scan_and_group`, download-index
matching, `_match_files_to_season`, interactive `ManifestWorkflow`.

## Config (`~/.config…/media-ingestion.kdl`, one file — downloads dir is shared)

```kdl
paths {
  downloads-dir "/volume1/docker/pvr/data/downloads"
  movies-source-dir "/volume1/docker/pvr/data/movies"
  movies-dest-dir "/volume1/video/movies"
  television-source-dir "/volume1/docker/pvr/data/television"
  television-dest-dir "/volume1/video/television"
}
movie "Blade Runner (1982)" { tmdb 78; edition "Final Cut" }
series "Severance (2022)" { tvdb 371980; tmdb 95396 }
```

## Risks & mitigations

1. **Parser fitness for western names in downloads mode**
   (`Title.2010.1080p.BluRay.x264-GRP`): downloads mode is best-effort —
   unparsed/low-confidence → `needs-id` with candidates, never a guessed dest;
   corpus spot-check against the real downloads dir during sp2.3; managed trees
   (clean naming) are the primary source anyway.
2. **TMDB/TVDB search ambiguity**: never auto-pick below exact title+year;
   ambiguous → `needs-id` + candidates; provider cross-check flags wrong picks.
3. **Register migration**: merge-on-load, atomic save, legacy file left intact;
   tests for legacy-only/new-only/both. Worst case = re-planning already-copied
   files, which apply detects as existing dests.
4. **Filename length**: plan-time check (reuse `_MAX_FILENAME_BYTES` logic from
   `manifest.py`) → error comment + `status "skip"`, no interactive loop.

## Tests (pylib/tests/)

- **Migrated**: register/scanner/env tests out of `test_anime.py` →
  `test_ingest_register.py` (incl. migration cases, tmp `XDG_CACHE_HOME`),
  `test_media_scanner.py`, `test_envfile.py`; fix `test_anime.py` imports.
- **New**: `test_tmdb.py` (mocked urlopen, mirroring `test_tvdb.py`);
  `test_tvdb.py` additions for search; `test_naming.py` additions (movie
  dirname/filename, editions, ID tags, zero-padded TV tag, specials, multi-ep);
  `test_video_ingest.py` (manifest round-trip incl. hypothesis property test;
  plan over synthetic Radarr/Sonarr tmp trees with injected search/fetch stubs;
  apply happy path/drift/conflict actions/idempotent re-apply/exit
  codes/`--refine`; JSON schema snapshot); `test_media_config.py`; CLI smoke
  tests for both commands + dispatcher registration assert.

## Docs

- Plan doc: `docs/plans/2026-06-09-movies-television-ingest.md` (committed
  first).
- ADRs: `2026-06-09-01-plan-apply-ingest-cli.md` (extends 2026-04-06-01,
  cross-ref 2026-04-19-02), `2026-06-09-02-plex-provider-id-directory-tags.md`,
  `2026-06-09-03-shared-ingest-register.md`,
  `2026-06-09-04-tmdb-tvdb-dual-provider.md`.
- `DESIGN_NOTES.md`: new "Movies & Television Ingestion" section (data flow,
  manifest schema, shared-register invariant).

## Execution order (subproject branches `rei/spN.M/…` → PR into feature branch, squash merges)

1. **sp0**: create `rei/feat/movies-television`; commit plan doc.
2. **sp1.1 extraction**: `ingest_register.py` + `media_scanner.py` +
   `envfile.py`; refactor anime.py; migrate tests. `just check && just test`.
3. **sp1.2 providers/types**: `tmdb.py`, `search_tvdb_series`, `types.py`
   additions + tests.
4. **sp1.3 naming**: four `naming.py` formatters + tests.
5. **sp2.1 core**: `paths.py`, `media_config.py`, `video_ingest.py` (manifest,
   plan, apply, JSON) + tests.
6. **sp2.2 commands**: `movies.py`, `television.py`, `pyproject.toml`,
   `dispatcher.py`, smoke tests, CLAUDE.md.
7. **sp2.3 downloads mode**: `group_downloads()`, wiring, real-corpus
   spot-check, parser-gap follow-ups.
8. **sp3.1 docs**: ADRs + DESIGN_NOTES;
   `just format && just check && just test`.

## Verification

- `just check` + `just test` green at every subproject merge; `just format`
  before finishing.
- End-to-end on this laptop with a config pointing at `/Volumes/...`:
  - `etp television ingest plan --sonarr --config /tmp/laptop-media.kdl -o /tmp/tv-plan.kdl --json`
    (read-only by construction) → inspect manifest →
    `etp television ingest apply /tmp/tv-plan.kdl --dry-run --json`.
  - Same pair for `etp movies ingest plan --radarr …`.
  - Regression: `etp anime ingest --downloads --dry-run` behaves identically;
    register merged correctly (old triage entries present in new file).
