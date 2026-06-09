# Design Notes

Implementation details and architecture for euterpe-tools. For conventions and
commands, see [CLAUDE.md](../CLAUDE.md). For architectural decisions, see
[docs/adrs/](adrs/).

## CLI Design Principles

The CLI is designed for both human operators and AI agents. Commands should be
predictable, composable, and safe for automated use.

1. **Explicit over convenient.** Commands require explicit arguments for
   anything that changes behavior. No "smart" auto-detection that silently picks
   a mode — if the tool can't determine what to do from the arguments, it fails
   with a clear error message. Defaults are conservative (do nothing rather than
   guess).

2. **Fast failure.** Validate arguments and preconditions upfront. Fail before
   doing any work, not halfway through. Error messages should include what was
   expected, what was received, and what to do about it.

3. **Structured output for agents.** Commands that produce output should support
   `--format json` or equivalent for machine consumption. Human-readable output
   is the default but not the only option. Exit codes are meaningful (0 =
   success, 1 = failure, 2 = nothing to do).

4. **Idempotent operations.** Running the same command twice with the same
   arguments should produce the same result. Side effects (file copies, config
   writes) happen at the end, not incrementally.

5. **No hidden state.** All state that affects behavior is either in the
   arguments, the config file, or the filesystem. Caches improve performance but
   never change correctness — `--no-cache` always works.

6. **Composable commands.** Each command does one thing. Complex workflows are
   built by running multiple commands in sequence, not by adding flags to make
   one command do everything. Plumbing commands (Rust) handle data; porcelain
   commands (Python) handle interaction.

See ADR `docs/adrs/2026-04-06-01-cli-design-for-ai-agents.md` for the rationale
behind these principles.

## Repository Structure

- `crates/` — Rust libraries (etp-lib, etp-cue)
- `cmd/` — all plumbing commands (Rust binaries and Python entry points)
- `pylib/` — Python shared library (`etp_lib`)
- `conf/` — KDL configuration files

## Rust Crates

Library crate (`crates/etp-lib/src/lib.rs`) re-exports shared modules:

- `ops.rs` — shared operations used by all binary crates
- `scanner.rs` — walkdir-based scanning; skips unchanged directories by mtime
- `csv_writer.rs` — sorted CSV output (`path,size,ctime,mtime`)
- `tree.rs` — tree rendering with ICU4X collation for Unicode-aware sorting
- `finder.rs` — `FindMatch` struct (full path + stat fields) returned by the
  DAO-layer match path. Regex evaluation happens in SQLite via the REGEXP UDF
  (see "Search (etp-find)" below).
- `metadata.rs` — media metadata reading with dual backend: lofty for audio
  formats, mediainfo subprocess for video (MKV, MP4, AVI) and gap audio (WMA,
  MKA). Extension-based dispatch. Extracts audio properties (duration, bitrate,
  channels) and video properties (width, height, bit depth, codec, frame rate,
  HDR). Tag names normalized to `lowercase_snake_case`. See
  `docs/adrs/2026-03-28-01-mediainfo-over-taglib.md`.
- `cas.rs` — content-addressable blob storage using BLAKE3 hashing with atomic
  filesystem writes (safe on Btrfs)
- `db/mod.rs` — SQLite connection factory (WAL mode, foreign keys, cache
  pragmas); dual-path init: new databases use clean `schema.sql`, existing
  databases use incremental `migrations/`. FK enforcement disabled during
  migrations for table recreation compatibility. `.with_regexp()` registers
  sqlx's Rust-regex-backed `REGEXP` UDF on every connection so DAO queries can
  push pattern matching into SQLite (see "Search (etp-find)" below).
- `db/dao.rs` — all database queries (scan CRUD, file UPSERT, metadata, blobs,
  images, cue sheets, move tracking). `FULL_PATH_SQL` constant for path
  reconstruction used across query functions.
- `config.rs` — KDL configuration parsing: catalog config (`Config`) for
  catalog.kdl and runtime config (`RuntimeConfig`) for config.kdl. Runtime
  config provides system file patterns, user excludes, CAS directory override,
  database nicknames, and default database setting.
- `paths.rs` — XDG/native path resolution (etcetera crate)
- `profiling.rs` — self-instrumentation (feature-gated behind `profiling`)

Standalone library crate (`etp-cue/`):

- CUE sheet parser, MusicBrainz disc ID computation (SHA-1 + custom Base64), and
  three display formatters (album summary, CUEtools TOC, EAC TOC)
- Supports multi-file CUE sheets via per-file duration accumulation
- No database dependency — pure data transformation

Each binary crate has a `build.rs` that embeds the short git hash in
`--version`. Binary crates: `etp-csv`, `etp-tree`, `etp-find`, `etp-meta`,
`etp-cas`, `etp-query`.

## Python Package

Python commands live in `cmd/etp/etp_commands/`:

- `dispatcher.py` — git-style dispatcher (`etp <cmd>` → `etp-<cmd>`)
- `anime.py` — interactive anime collection manager
- `catalog.py` — KDL-configured catalog orchestrator
- `video_cli.py` — shared plan/apply CLI for movies and television
- `movies.py`, `television.py` — thin MediaKind bindings onto `video_cli`

Python shared library lives in `pylib/etp_lib/`:

- `paths.py` — XDG-based path resolution and binary search
- `media_vocab.py` — vocabulary sets, Token/TokenKind types, and mapping tables
  shared between the parser and its recognizers
- `media_parser.py` — three-phase media filename parser (see below)
- `media_scanner.py` — recursive media-file discovery and the
  ParsedMedia→SourceFile bridge (`parse_source_filename`)
- `anidb.py`, `tvdb.py`, `tmdb.py` — API clients with local caching
- `types.py` — shared data types (AnimeInfo, MovieInfo, Episode, SourceFile,
  ParsedMetadata, MatchedFile, MediaInfo, SearchCandidate) and StrEnum types
  (EpisodeType, BonusType, MetadataProvider)
- `manifest.py` — anime KDL manifest generation, parsing, execution, and the
  ManifestWorkflow orchestrator
- `video_ingest.py` — non-interactive plan/apply ingestion core for movies and
  television (see below)
- `naming.py` — filename formatting and directory naming for anime, movies, and
  television
- `conflicts.py` — destination conflict resolution with readline support
- `mediainfo.py` — mediainfo subprocess wrapper for audio/video metadata
- `ingest_register.py` — shared already-ingested register (all ingest commands;
  see ADR 2026-06-09-03)
- `envfile.py` — KEY=VALUE env file loading
- `media_config.py` — media-ingestion.kdl loading and mapping persistence

`conf/` contains KDL configuration files.

## Media Filename Parser

The parser (`media_parser.py`) extracts metadata from anime/media filenames that
follow loosely adopted conventions (fansub, scene, Sonarr, Japanese BD). See
`docs/adrs/2026-03-30-02-heuristic-media-filename-parsing.md`.

Three-phase pipeline:

1. **Structural tokenization** (`tokenize_component`): Character-by-character
   scan identifies delimiters (brackets, parens, lenticular quotes). Scene-style
   dot-separated text is handled by `scan_dot_segments`, which uses parsy-based
   recognizers to identify compound tokens (H.264, AAC2.0) across dot
   boundaries. Separator-style text (`-`) is split by `_split_separators`.

2. **Semantic classification** (`classify`): Walks the token list with
   positional state to reclassify content. Uses `_try_recognize` (parsy
   recognizers) for word-level classification and `scan_words` for multi-word
   pattern matching with dash-compound splitting.

3. **Assembly** (`_build_parsed_media`): Extracts series name, episode title,
   and metadata fields from classified tokens into a `ParsedMedia` dataclass.

Token recognition uses parsy `Parser` objects as typed recognizers — each
returns a frozen dataclass (Resolution, VideoCodec, AudioCodec, Source,
EpisodeMultiSE, SeasonSpecial, DualAudio, Uncensored, Edition, etc.) on success
or a failure. The recognizers are ordered by specificity in the `_RECOGNIZERS`
list (compound audio codecs before simple, SxxExx before S-only seasons,
dual_audio before language). See
`docs/adrs/2026-03-30-01-parsy-primitives-for-token-recognition.md`.

Sonarr-inspired enhancements (using `~/projects/sonarr` as reference):

- Multi-episode range expansion: `S01E01-E06` → `episodes: [1..6]`
- Year validation: reject < 1940 and > current year + 1
- Decimal episode specials: `01.5` → `is_special` (fansub-style only)
- GM-Team format: `(Season 01)` alongside ordinal `4th Season`
- LoliHouse dual numbering: bare `001` + `(S01E01)` → season from parens
- Bilingual title splitting: CJK-aware `/` and `|` → `series_name_alt`

`DualAudio`, `Uncensored`, `Edition`, and `Special`/`SeasonSpecial` have
dedicated `TokenKind` values consumed directly in `_build_parsed_media`.
Japanese bonus keywords (`bonus_jp` recognizer) are detected at classification
time alongside English keywords (`bonus_en`).

`parse_media_path` handles full relative paths by parsing directory and filename
components separately, then merging: the filename is primary for
episode/metadata, directories provide series name, release group, and fill
metadata gaps (resolution, codec, source type, audio codecs, dual-audio,
uncensored) via `_merge_scanned_metadata` on directory text.

Vocabulary sets (`_SOURCES`, `_VIDEO_CODECS`, `_AUDIO_CODECS`, etc.) live in
`media_vocab.py` to avoid circular imports between the parser and its
recognizers. The parser re-exports them for backward compatibility.

## Anime Collection Manager

Interactive CLI for managing an anime collection on the NAS: fetches metadata
from AniDB or TheTVDB, analyzes source files with mediainfo, constructs properly
named episode files, and copies them using Btrfs COW reflinks.

Subcommands:

- `etp anime ingest --sonarr` — sync from Sonarr-managed anime directory (uses
  ID files + config mappings, download index for metadata enrichment)
- `etp anime ingest --downloads` — triage files from downloads directory
  (auto-groups by series name, including CJK/Latin alt-title merging)
- `etp anime ingest --sonarr --downloads` — both in sequence (Sonarr first, then
  triage for leftovers)
- `etp anime episode` — single-file import

The `ingest` command (which replaced the former `triage` and `series`
subcommands) delegates to a shared `_process_pool()` function that handles the
interactive ID-prompt → metadata-fetch → season-match → manifest workflow loop.

Extras handling: non-video files from BD batch `Extras/` subdirectories are
copied wholesale to the destination `Extras/` directory, preserving the source
subtree structure. Video files found in `Extras/` prompt the user to choose:
copy as extras, route to Specials for manifest editing, or skip.

Subtitle sidecars: external subtitle files co-located with a source video and
sharing its exact base name (`Episode 01.srt` next to `Episode 01.mkv`) ride
along with that episode, renamed to the video's final destination base using the
Jellyfin/Plex external-subtitle convention `<base>.<lang>[.<flags>].<ext>`
(which ShokoFin replicates verbatim into its VFS, keyed on the shared base name;
see the [Shoko runbook](shoko-runbook.md)). Recognized extensions:
`.srt .ass .ssa .vtt .sub .idx .sup`. Tagged sidecars keep their language/flag
tokens; untagged sidecars default to the language from `--sub-lang` (`en`).
Matching is a literal base-name string test with a `.` boundary so `Show - 01`
never grabs `Show - 011.srt` — the media parser is not involved (see the design
decision below). Both `ingest` and `episode` use the shared
`naming.subtitle_sidecars()` helper.

### Data flow

1. **Parse** source filenames via `parse_source_filename()` into `SourceFile`
   (wrapping `ParsedMetadata` for parser-detected fields)
2. **Enrich** from download index (`_match_to_downloads`) — fills release group,
   hash, source type from matching download files
3. **Match** files to metadata IDs — AniDB per-season (`_match_files_to_season`,
   returns `MatchedFile` wrappers with renumbered episodes) or TVDB all-at-once
4. **Manifest workflow** (`ManifestWorkflow`): build entries (mediainfo + CRC32
   verification + special matching) → write KDL → open `$EDITOR` → parse →
   execute copies. Subtitle sidecars are copied during execution — after each
   video lands — named from that video's _final_ destination path.

### Non-mutating episode matching

`_match_files_to_season` returns `list[MatchedFile]` — wrappers around the
original `SourceFile` with overridden episode/season/special_tag. The original
pool data is never mutated, so multi-cour processing (where the same pool serves
multiple AniDB IDs in sequence) works correctly: each pass sees the original
episode numbers.

`_process_group_batch` snapshots `MatchedFile` overrides into `SourceFile`
copies (`to_source_snapshot()`) before passing them to the manifest workflow.

### Special episode handling

Special detection sources: parser (`is_special`, `special_tag`, `bonus_type`),
season 0, and decimal episodes (01.5). `build_manifest_entries` matches bonus
files (NCOP, NCED, PV, CM) against AniDB special episodes. Unmatched bonus files
get HamaTV-compatible episode numbers (`_HAMATV_RANGES`). When using TVDB,
HamaTV ranges start after the highest existing TVDB special number to avoid
collisions in the single `Specials/` directory.

### Type safety

StrEnum types (`EpisodeType`, `BonusType`, `MetadataProvider`) replace raw
string comparisons throughout. These are backwards-compatible with string
equality checks but provide IDE autocompletion and pyright validation.

### Design decisions

**SPECIAL tokens don't set episode number.** When the parser finds `OVA2E03`,
`OVA2` is a SPECIAL token (number=2) and `E03` is an EPISODE token (episode=3).
The SPECIAL's number is a series/group indicator ("OVA series 2"), not the
episode. `_build_parsed_media` defers episode assignment from SPECIAL tokens —
if a subsequent EPISODE token provides the real episode number, it takes
priority. Only when no EPISODE follows does the SPECIAL's number become the
episode (e.g., `SP1` alone → episode 1).

**MatchedFile wraps, never mutates.** `_match_files_to_season` returns
`MatchedFile` wrappers with overridden episode/season values. The original
`SourceFile.parsed` is never modified during season matching or renumbering.
This ensures multi-cour processing (where the same pool serves multiple AniDB
IDs in sequence) sees the original episode numbers on each pass. Overrides are
baked into `SourceFile` copies via `to_source_snapshot()` only when entering the
manifest workflow.

**ManifestWorkflow encapsulates the edit cycle.** The build → write → edit →
parse → execute sequence is a single unit of work. `ManifestWorkflow.run()`
handles the full cycle including error recovery (re-edit on parse errors) and
cleanup (temp file removal). Callers should use `run()` rather than calling the
individual manifest functions.

**HamaTV ranges start after TVDB specials.** When using TVDB, unmatched bonus
files get HamaTV-compatible episode numbers aligned with the ScudLee anime-lists
conventions (Credits=121+, Trailers=171+, Parodies=221+, Other=321+). NCOP and
NCED share a counter and interleave as OP/ED pairs. Ranges are adjusted to start
after the highest existing TVDB special number (+20 buffer) to avoid collisions.
See [ADR 2026-04-02-04](adrs/2026-04-02-04-hamatv-special-episode-ranges.md).

**Resolution uses height only.** Width is irrelevant for resolution tags —
anamorphic encodes (1440x1080, 848x480) have non-standard widths but standard
heights. The `normalize_resolution` function maps by height alone. Interlaced
scan type comes from the filename (`1080i`) or from mediainfo's `ScanType`
field; the default is progressive.

**Decimal versions are truncated.** Version tags like `v2.1` are consumed by the
parser but only the major integer is stored (`version=2`). The decimal portion
is intentionally discarded — if quality ranking needs it, the `version` field
type should change from `int` to `str` or `float`.

**Subtitle sidecars are parser-free and derived at execute time.** Subtitle
handling deliberately sits _outside_ the media parser: sidecars are matched to a
video by literal source base-name equality (never tokenized), their
language/flag tokens are carried through verbatim, and the only inference is the
trivial "untagged → `--sub-lang` default". The one real constraint is ordering —
a sidecar's destination name is the video's _final_ `dest_path` stem, which
isn't known until after enrichment (mediainfo/CRC may rewrite the filename),
manifest editing (the user may rename), and conflict handling (the `both` action
appends `[crc]`). So `execute_manifest` (and the single-file `_process_file`)
derive and copy sidecars _after_ a successful video copy, not at parse/build
time. The payoff: a sidecar always follows whatever final name its video gets,
with no separate manifest entry to keep in sync — avoiding the Sonarr "loses the
language code on rename" failure mode. The cost: an orphan subtitle — one whose
base matches no source video, including a video/sub `vN` mismatch
(`Show - 01v2.mkv` vs `Show - 01.srt`) — is silently skipped. It is neither a
sidecar nor an extra, since `_EXTRAS_EXTENSIONS` excludes subtitle extensions.

## Movies & Television Ingestion

`etp movies ingest` and `etp television ingest` are thin wrappers
(`cmd/etp/etp_commands/video_cli.py`) around one shared core,
`pylib/etp_lib/video_ingest.py`, parameterized by a `MediaKind` enum. Unlike the
interactive anime flow, both are non-interactive **plan/apply** commands
designed for LLM agents as first-class users (ADR 2026-06-09-01).

### Data flow

```
ingest plan --radarr|--sonarr and/or --downloads
  scan sources         scan_managed_tree (deterministic Radarr/Sonarr naming)
                       scan_downloads (best-effort torrent naming, kind-filtered)
  filter               shared ingest register (unless --force), pattern
  resolve              config mapping / --refine ID  -> confidence "exact"
                       else provider search: exact title+year -> "exact",
                       single fuzzy hit -> "high", else needs-id + candidates
  cross-check          TV: TMDB external_ids must point back at the TVDB id
                       movies: TheTVDB movie search by exact title+year
  enrich               mediainfo per file (quality block in dest names)
  place                reuse existing `Title (Year)` library dir, else
                       `Title (Year) {tmdb-NNN}` / `{tvdb-NNN}` (+ {edition-X});
                       existing dest file -> status "conflict", conflict "keep"
  write                KDL plan manifest (schema-version 1) + JSON summary

ingest apply MANIFEST
  validate everything first (sources exist at recorded sizes, no unresolved
  needs-id, no surprise destinations) — reject all-at-once, copy nothing
  execute reflink copies + subtitle sidecars, honoring declarative per-entry
  conflict actions (keep/replace/both/skip; "both" disambiguates with [CRC32])
  record sources in the shared ingest register (saved once, at the end)
```

Exit codes: 0 success, 1 failure, 2 nothing to do. With `--json`, stdout is
exactly one JSON document; everything human-facing goes to stderr (apply
redirects the copy helpers' stdout for this).

### Provider roles

Television: TheTVDB primary (episode numbering/titles, `{tvdb-NNN}` dir tag);
TMDB cross-check. Movies: TMDB primary (`{tmdb-NNN}` dir tag, IMDb id,
alternative titles); TheTVDB cross-check. Mismatches are warnings, never fatal.
No AniDB. See ADR 2026-06-09-04.

### Invariants

- **Plan never writes to the library; apply never invents names.** Every
  destination in an applied manifest was computed and reviewable at plan time.
- **Shared register.** All three ingest commands (anime included) share
  `cache_dir("ingest")/copied.json`, keyed by resolved absolute source path,
  because they draw on the same downloads directory (ADR 2026-06-09-03).
  Skip-marked entries are not registered and resurface on the next plan.
- **Existing library directories are reused, never renamed.** Matching is on
  normalized `Title (Year)` with brace tags and bracketed alt-titles stripped.
- **Ambiguity is data.** Unresolvable titles become `needs-id` blocks with
  candidates; apply refuses to run while any remain unskipped. The `--refine`
  flag re-plans with IDs/decisions carried forward from an edited manifest.

### Known limitations

Downloads mode shares the directory with anime torrents, so its groups are only
as good as `media_parser` on western/scene names. Observed gaps in the real
corpus: quality tokens occasionally survive in titles
(`KPop Demon Hunters 2025 2160p ...`), years sometimes stick to the title
(`Harmony 2015`), and extras clips (convention panels, OP/ED singles) parse as
standalone titles. All of these fail exact title+year resolution and land as
`needs-id` — wrong destinations are not produced, but downloads-mode plans
benefit from a `pattern` argument to target specific items. Parser improvements
for scene-style naming are tracked as follow-up work.

## Database

SQLite with sqlx, WAL mode, single-threaded tokio (`current_thread`). The
canonical schema is `etp-lib/schema.sql`. Pool is `max_connections(1)` — all
queries are sequential. FK enforcement is disabled during migration execution
(some migrations recreate tables referenced by foreign keys).

Defaults: database is `<dir>/.etp.db`. The scanner indexes everything on disk
(no default excludes). Display-time filtering hides system files and user
excludes — see "Display Filtering" below.

File sync uses UPSERT to preserve file IDs across rescans. When a file's mtime
changes, `metadata_scanned_at` is cleared so the metadata scanner re-reads it.
See `docs/adrs/2026-03-27-03-upsert-file-sync.md`.

File-move tracking: after all directories are flushed, a reconciliation pass
matches removed files against newly appeared files by size, then verifies with
streaming BLAKE3 hash. Matched files get an UPDATE to `dir_id` + `filename`,
preserving their ID and all dependent metadata. Unmatched files are deleted with
dependent cleanup.

### Scanner diagnostics

`scan_to_db` emits `phase: <name> done in <n>s — <stats>` markers on stderr when
`-v` is passed, covering every post-walk step (stale-directory sweep, move
reconciliation, CAS blob cleanup, `finish_scan`). The walk loop and the
reconcile match loop emit a `progress:` / `reconcile:` line every 30 seconds so
long-running scans report liveness. The markers exist so a scan that appears
stuck can be triaged in a single re-run rather than guessed at. The
`csv-verbose` trycmd snapshot pins the format.

## Search (etp-find)

`etp-find` pushes its pattern match into SQLite through the REGEXP UDF
registered on every connection by `.with_regexp()`. The DAO functions
`list_files_matching` and `stream_files_matching` issue
`SELECT … WHERE (<full_path>) REGEXP ?`, where `<full_path>` is the SQL
concatenation of `root_path`, `directories.path`, and `files.filename`. Only
matching rows cross the sqlx boundary into Rust.

`-i` is expressed as a `(?i)` prefix on the pattern string before binding, so
the Rust `regex` crate handles case folding inside the UDF. There is no separate
case-insensitive SQL operator. Case-sensitivity of `etp-find` is therefore a
property of the compiled regex:

|              | ASCII case | non-ASCII 1:1 | `ß` ↔ `SS` (1:2) |
| ------------ | ---------- | ------------- | ---------------- |
| without `-i` | strict     | strict        | no               |
| with `-i`    | folded     | folded        | no               |

The `ß` ↔ `SS` 1:2 mapping is _full_ case folding, which the `regex` crate does
not implement (it does Unicode _simple_ case folding only — see the fixture and
tests in `crates/etp-lib/tests/find_sql_pushdown.rs`).

When `FilterConfig::include_system_files` is false, the query also gets
`AND NOT (<full_path>) REGEXP ?` with a second pattern built from
`filter.system_patterns` via `regex::escape` and component anchors
(`(?:^|/)…(?:$|/)`). SQLite short-circuits `AND` chains, so rows rejected by the
user pattern never incur the system-regex evaluation. The compiled regex is
cached in sqlx-sqlite's per-statement auxdata, so both patterns compile exactly
once per query.

`ops::stream_find_matches` and `collect_find_matches` take
`(pattern: &str, insensitive: bool)` and build the final SQL pattern via
`regexp_pattern()`. After rows arrive, `FilterConfig::should_show_post_sql`
applies the dotfile and user-exclude checks on the filename alone — it skips the
expensive `is_system_path` component walk because the SQL layer has already
applied the system filter (or, when `include_system_files` is true,
intentionally did not).

See `docs/adrs/2026-04-19-01-regexp-pushdown-for-find.md` for the rationale,
benchmarks, and the evolution from a dual LIKE+REGEXP dispatch to the single
REGEXP path.

## Display Filtering

The scanner indexes everything on disk. Filtering happens at display time via
three independent layers:

1. **System files** (`@eaDir`, `@eaStream`, `.etp.db*`, etc.) — NAS/OS
   byproducts. Hidden from listings by default, but included in `--du` size
   calculations. Shown with `--include-system-files`. Patterns are exact name
   matches against file/directory names.

2. **Dotfiles** (names starting with `.`) — hidden by default, shown with
   `-A`/`--all`. Managed by the `show_hidden` field in `FilterConfig`, not by
   user excludes. System files starting with `.` (like `.etp.db`) are exempt
   from dotfile hiding. See
   `docs/adrs/2026-03-28-02-dotfile-hiding-via-all-flag.md`.

3. **User excludes** (empty by default) — glob patterns from `--exclude` and
   `--ignore`, matched against filenames only (not the full path, since absolute
   paths may contain unrelated dot-directories).

System files are exempt from both dotfile hiding and user exclude matching.
`etp-query` does not apply dotfile hiding (it's a lower-level search command).

`FilterConfig` in `ops.rs` bundles all filter state: system patterns, user
excludes, `include_system_files`, and `show_hidden`. It provides `should_show()`
(for full path + filename checks), `should_show_name()` (for individual name
checks in tree rendering), and `should_show_post_sql()` (a lean variant used by
`etp-find` when the SQL layer has already filtered system paths — see "Search
(etp-find)" above).

## Runtime Configuration (config.kdl)

`config.kdl` lives in the platform config directory (`etp-init` generates it).
It provides:

- **System file patterns** — override `DEFAULT_SYSTEM_PATTERNS`
- **User exclude patterns** — override `DEFAULT_USER_EXCLUDES`
- **CAS directory** — override the platform default for blob storage
- **Database nicknames** — map short names to `(root, db)` path pairs
- **Default database** — nickname used when no `--db` and no `.etp.db` exists

All commands load config via `RuntimeConfig::load_or_default()`. If the file
doesn't exist, hardcoded defaults are used. Invalid config (e.g.,
`default-database` naming a nonexistent nickname) errors at load time.

Database nicknames resolve in this order: if the argument exists as a directory
or file, use it as a path; otherwise look it up in config. `resolve_nickname`
prints the resolution to stderr so users can see what's happening.

The `default-database` fallback is used by etp-tree, etp-csv, etp-find, and
etp-query. etp-scan is excluded to prevent accidental writes to the wrong
database. See `docs/adrs/2026-03-28-04-default-database-fallback.md`.

## CLI Boolean Flag Pairs

For flags where both the positive and negative form are meaningful (e.g.,
`--scan` / `--no-scan`, `--include-system-files` / `--no-include-system-files`),
both forms are defined as separate clap args with `default_value_t = false`.
Resolution uses `ops::resolve_bool_pair()`:

- Only `--flag` passed → true
- Only `--no-flag` passed → false
- Neither passed → default
- Both passed → prints a warning to stderr and uses the default

This avoids clap's `overrides_with` (which silently picks the last one) in favor
of explicit conflict detection. The warning helps users who may be combining
flags from shell aliases or scripts without realizing the conflict.

## Profiling

Self-instrumented via `tracing` + `tracing-chrome`, gated behind the `profiling`
Cargo feature. Trace files are named `etp-trace-<binary>-<timestamp>.json` and
written to cwd. On Linux, `/proc/self/io` and `/proc/self/status` metrics are
sampled at phase boundaries. The feature adds no runtime cost when `--profile`
is not passed.

```bash
just build-profile      # native with profiling
just build-nas-profile  # NAS with profiling
etp-csv /path/to/dir --profile
# Open trace in Perfetto: https://ui.perfetto.dev
```

## Cross-Compilation

`.cargo/config.toml` sets the linker for `x86_64-unknown-linux-musl` to
`x86_64-linux-musl-gcc`. Two options:

1. **musl toolchain**: `brew install filosottile/musl-cross/musl-cross`, then
   `rustup target add x86_64-unknown-linux-musl`
2. **cross (Docker-based)**: Must use the git version
   (`cargo install cross --git https://github.com/cross-rs/cross`) — the
   crates.io release (0.2.5) lacks ARM64 Docker image support and fails on Apple
   Silicon.
