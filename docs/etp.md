# etp(1) — euterpe-tools

## NAME

`etp` — incremental filesystem indexing, audio metadata, and media library
ingestion for a Synology NAS.

## SYNOPSIS

```
etp <command> [args...]
```

`etp` is a git-style dispatcher: `etp tree` runs `etp-tree`, found in
`~/.local/libexec/etp/` or on `$PATH`. Every command supports `--help`.

## EXIT CODES

All commands follow one convention:

| Code | Meaning                                                  |
| ---- | -------------------------------------------------------- |
| 0    | success                                                  |
| 1    | failure (bad arguments, validation errors, copy failure) |
| 2    | nothing to do (no scan exists, nothing matched, no work) |

For `tree`, `csv`, `find`, and `query`, the dispatcher catches exit code 2 ("no
scan exists"), runs `etp-scan` automatically, and retries.

## FILESYSTEM INDEX

```sh
etp scan /volume1/music                 # index a tree into <dir>/.etp.db
etp scan /volume1/music --db work.db -e @eaDir -v

etp tree /volume1/music                 # render the indexed tree
etp tree /volume1/music --du --du-subs  # with directory sizes
etp tree /volume1/music --scan          # rescan first

etp csv /volume1/music --output index.csv

etp find '\.flac$' -R /volume1/music    # regex search the index
etp find -i 'beatles' --size            # case-insensitive, with sizes

etp query --db music files              # raw queries against the index
etp query stats --format json
etp query --db music sql 'SELECT COUNT(*) FROM files'
```

## AUDIO METADATA

```sh
etp init                                # write a default config.kdl
etp meta scan -R /volume1/music         # extract tags into the database
etp meta read album/01.flac --images    # show one file's tags
etp meta cue album/disc.cue --format eac

etp cas store cover.jpg                 # content-addressable blob store
etp cas get <blake3-hash> -o cover.jpg
etp cas gc --db music.db                # drop unreferenced blobs
etp cas list

etp catalog                             # run all configured catalog scans
etp catalog --dry-run my-catalog.kdl
```

## ANIME INGESTION (interactive)

`etp anime` is the interactive curator: it prompts for AniDB/TheTVDB IDs and
opens manifests in `$EDITOR`.

```sh
etp anime ingest --sonarr               # sync the Sonarr-managed anime tree
etp anime ingest --downloads frieren    # triage downloads matching "frieren"
etp anime ingest --sonarr --downloads --force
etp anime episode file.mkv --anidb 17617
etp anime episode file.mkv --tvdb 371310
```

Configuration: `anime-ingestion.kdl`; credentials: `anime.env` (see FILES).

## MOVIES & TELEVISION INGESTION (plan/apply)

`etp movies ingest` and `etp television ingest` are **non-interactive** and
share one shape; only the managed-tree flag differs (`--radarr` vs `--sonarr`).
The workflow is always two steps with a reviewable KDL manifest between them:

```
plan  →  review/edit the manifest  →  [plan --refine]  →  apply
```

### Step 1 — plan

Scans sources, resolves provider IDs, and writes a manifest. **Never writes to
the library.**

```sh
# Plan everything new in the Radarr tree; manifest path is printed
etp movies ingest plan --radarr

# Sonarr tree + the shared downloads directory, only titles matching a pattern
etp television ingest plan --sonarr --downloads expanse

# Explicit output path and machine-readable summary (JSON on stdout)
etp movies ingest plan --radarr -o /tmp/movies.kdl --json

# Include files already recorded in the shared ingest register
etp television ingest plan --sonarr --force

# Other flags: --source DIR (override source; repeatable for --downloads),
#              --config FILE, --no-cache, -v (per-title resolution report)
```

ID resolution order: config mapping (`media-ingestion.kdl`) → IDs from a
`--refine` manifest → **Radarr/Sonarr API** (authoritative, needs `radarr`/
`sonarr` url config + API keys) → provider search (TMDB for movies, TheTVDB for
TV) → existing library directory as tiebreaker. Each title gets a `confidence`:

- `exact` — explicit ID, Radarr/Sonarr record, or unique title+year match
- `high` — single plausible hit, or ambiguity broken by the library
- `ambiguous` / `none` — unresolved: entries become `status "needs-id"` and the
  block lists the `candidate` nodes found

### Step 2 — review and edit the manifest

The manifest is a plain KDL file. Fields you may edit:

- per-entry `status` — set `"ready"` entries to `"skip"` to leave files out
- per-entry `conflict` — `"keep"` (default; leaves the existing file and records
  the source as ingested), `"replace"` (atomic in-place upgrade), `"both"` (keep
  both, new file disambiguated with a CRC32 suffix), `"skip"`
- per-title `tmdb`/`tvdb` — add the correct ID to a `needs-id` block

Do **not** hand-edit `dest`/`dest-dir`; destinations are always computed by
plan. Entries noted `same-size file already in library` point at the existing
copy — `keep` marks them ingested without copying.

### Step 2½ — plan --refine (after editing a needs-id manifest)

Re-plans with the IDs and skip/conflict decisions carried forward from the
edited manifest, recomputing destinations:

```sh
etp movies ingest plan --radarr --refine /tmp/movies.kdl -o /tmp/movies2.kdl
```

Loop `plan → edit → plan --refine` until nothing is left `needs-id` (or mark the
leftovers `skip`). Frequently-corrected titles belong in `media-ingestion.kdl`
instead:

```kdl
movie "Blade Runner (1982)" {
  tmdb 78
  edition "Final Cut"
}
series "Severance (2022)" {
  tvdb 371980
}
```

### Step 3 — apply

Validates the manifest against the live filesystem first — sources must exist at
their recorded sizes, no unresolved `needs-id` entries, no destinations that
appeared since planning, no duplicate destinations. All violations are reported
at once and **nothing is copied unless the whole manifest is clean.** Then it
reflink-copies files and subtitle sidecars and records sources in the shared
ingest register.

```sh
etp movies ingest apply /tmp/movies.kdl --dry-run     # validate + show actions
etp movies ingest apply /tmp/movies.kdl               # execute
etp television ingest apply /tmp/tv.kdl --json        # JSON result on stdout
etp television ingest apply /tmp/tv.kdl --sub-lang ja # untagged sidecar language
```

Re-applying a finished manifest is safe: existing same-size destinations count
as already done (exit 2 when there is nothing new). If a source file changed
since planning, apply rejects the manifest — re-run plan.

### Worked example (agent loop)

```sh
$ etp movies ingest plan --radarr --json -o /tmp/plan.kdl
{"tool":"etp-movies","action":"plan", ... "counts":{"ready":12,"needs_id":1,...}}

# 1 needs-id: edit /tmp/plan.kdl, add `tmdb 12345` to that movie block
$ etp movies ingest plan --radarr --refine /tmp/plan.kdl -o /tmp/plan2.kdl --json
{"tool":"etp-movies","action":"plan", ... "counts":{"ready":13,"needs_id":0,...}}

$ etp movies ingest apply /tmp/plan2.kdl --dry-run
$ etp movies ingest apply /tmp/plan2.kdl --json
{"tool":"etp-movies","action":"apply","ok":true,"counts":{"copied":13,...}}
```

With `--json`, stdout carries exactly one JSON document; all human-facing output
goes to stderr.

## FILES

Config directory: `~/.config/euterpe-tools/` (Linux/NAS) or
`~/Library/Application Support/net.aoaioxxysz.etp/` (macOS). Samples for all of
these live in `conf/`.

| File                  | Purpose                                                            |
| --------------------- | ------------------------------------------------------------------ |
| `config.kdl`          | runtime config (system file patterns, excludes, db nicknames)      |
| `catalog.kdl`         | catalog scan definitions                                           |
| `anime-ingestion.kdl` | anime paths + per-series AniDB/TVDB mappings                       |
| `anime.env`           | `ANIDB_CLIENT`, `ANIDB_CLIENTVER`, `TVDB_API_KEY`                  |
| `media-ingestion.kdl` | movies/TV paths, `radarr`/`sonarr` urls, per-title ID mappings     |
| `media.env`           | `TMDB_API_KEY`, `TVDB_API_KEY`, `RADARR_API_KEY`, `SONARR_API_KEY` |

All ingest commands share one register of already-processed source files
(`copied.json` in the `ingest` cache directory), so a file ingested by
`etp anime` is never re-offered by `etp movies` or `etp television`. Bypass it
with `--force`.

## SEE ALSO

- `docs/DESIGN_NOTES.md` — architecture, data flow, invariants
- `docs/adrs/` — architectural decision records
- `docs/etp-anime-parsing-rules.md` — media filename parser behavior
