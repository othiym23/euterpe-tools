# etp-anime parsing and matching rules

Rules for filename parsing, download matching, and output construction in
`etp anime`. Parsing is handled by `etp_lib.media_parser`; the anime module
consumes its output via `ParsedMetadata` and `MatchedFile`.

**Related ADRs:**

- `docs/adrs/2026-03-30-01-parsy-primitives-for-token-recognition.md` — parsy
  recognizer architecture
- `docs/adrs/2026-03-30-02-heuristic-media-filename-parsing.md` — heuristic
  parsing strategy and accepted false positives

## Configuration

### anime-ingestion.kdl

`$XDG_CONFIG_HOME/euterpe-tools/anime-ingestion.kdl` — paths and series ID
mappings.

```kdl
paths {
  downloads-dir "/volume1/docker/pvr/data/downloads"
  anime-source-dir "/volume1/docker/pvr/data/anime"
  anime-dest-dir "/volume1/video/anime"
}

series "Chained Soldier (2024)" {
  anidb 17330
  anidb 18548
  concise "Chained Soldier"
}
```

- Multiple AniDB IDs per series for multi-season entries.
- `concise` stores the parser-extracted series name (without year/metadata) for
  title matching. Saved automatically on first use.

### anime.env

`$XDG_CONFIG_HOME/euterpe-tools/anime.env` — API credentials (`KEY=VALUE`).

## Media path parser (`etp_lib.media_parser`)

A three-phase pipeline: structural tokenization, semantic classification,
assembly. Each phase adds structure without destroying information from the
previous phase.

### Phase 1: Structural tokenization (`tokenize_component`)

Character-by-character scan identifies delimited groups and bare text:

| Structural token | Delimiter | Examples                                      |
| ---------------- | --------- | --------------------------------------------- |
| BRACKET          | `[…]`     | `[Cyan]`, `[1080p x265]`, `[D98B31F3]`        |
| PAREN            | `(…)`     | `(BD 1080p)`, `(S01E01)`, `(Uncensored)`      |
| LENTICULAR       | `「…」`   | `「Episode Title」`                           |
| TEXT             | bare      | `Show Title`, `08`, `MLLSD`                   |
| DOT_TEXT         | `.`-split | `Title`, `S01E05`, `1080p` (from scene names) |
| SEPARATOR        | `-`       | fansub-style title/episode delimiter          |
| EXTENSION        | suffix    | `.mkv`, `.mp4`, `.flac`                       |

Scene-style dot-separated text (2+ dots, no spaces) is handled by
`scan_dot_segments`, which tries parsy-based recognizers at each position to
identify compound tokens across dot boundaries. For example, `H` + `264` →
`H.264`, and `AAC2` + `0` → `AAC2.0`. Trailing `-GROUP` suffixes are detected
and split into a metadata token + release group.

Full paths are split on `/`; each component is tokenized independently.

### Phase 2: Semantic classification (`classify`)

Walks the structural token list and reclassifies content using typed
recognizers, vocabulary sets, and positional state.

#### Refutable matching with parsy recognizers

Each recognizer is a parsy `Parser` object — a function
`(stream, index) → Result` that returns either a typed success (frozen
dataclass) or a failure. This is **refutable matching**: if `audio_codec`
matches `AAC2.0` as a 5-character span, that match is committed and the `.0` is
not available to confuse later patterns. If the match fails, the next recognizer
is tried.

The `_RECOGNIZERS` list orders recognizers from most specific to most general.
Order matters — a recognizer earlier in the list takes priority:

```text
Episode markers (most distinctive):
  episode_multi_se    S01E01-E06, S01E01-06, S01E01E02E03
  episode_se          S01E05, s1e1, S03E13v2
  episode_jp          第01話
  batch_range         01~26
  season_special      S01OVA, S02SP1, S03OP, S03ED
  special             SP1, OVA, OAD, ONA
  season_jp           第1期
  season_word         4th Season, Season 01
  season_only         S01 (after episode_se to avoid S01E05 → S01)
  episode_final       05 END, 05v2 END
  episode_ep          EP05, E5
  episode_bare        08, 12v2 (after season_only to avoid S01 → ep 1)

Bonus keywords:
  bonus_en            NCOP, NC OP1, Creditless ED
  bonus_jp            映像特典, ノンテロップOP, PV, 予告, 告知CM

Technical metadata (compound before simple):
  audio_codec         AAC2.0, DTS-HD MA, FLAC, DD+2.0
  resolution          1080p, 720p, 1920x1080
  video_codec         HEVC, x265, H.264, AVC
  source              BluRay, WEB-DL, BD, AMZN, CR
  remux               REMUX

Identifiers:
  crc32               ABCD1234 (8 hex chars)
  year                1940–current+1 (after episode to avoid false positives)
  version             v2, v3

Context (dual_audio before language — "DUAL" must match as dual-audio):
  dual_audio          Dual Audio, Dual-Audio, Dual.Audio, DUAL
  language            jpn, eng, chi, dual
  subtitle_info       multisub, msubs, sub, subs, ESub
  hdr_info            HDR, HDR10, DoVi
  bit_depth           10bit, 10-Bit, Hi10
  uncensored          Uncensored
  edition             Criterion, Remastered, Uncut
  repack              REPACK, REPACK2
  site_prefix         www.example.com
```

Each recognizer returns a frozen dataclass on success:

| Result type      | Fields                                      | TokenKind   |
| ---------------- | ------------------------------------------- | ----------- |
| `Resolution`     | `value: str`                                | RESOLUTION  |
| `VideoCodec`     | `value: str`                                | VIDEO_CODEC |
| `AudioCodec`     | `value: str`                                | AUDIO_CODEC |
| `Source`         | `value: str`, `source_type: str`            | SOURCE      |
| `Remux`          | (none)                                      | REMUX       |
| `EpisodeSE`      | `season`, `episode`, `version?`             | EPISODE     |
| `EpisodeMultiSE` | `season`, `episodes: list[int]`             | EPISODE     |
| `EpisodeBare`    | `episode`, `version?`, `is_decimal_special` | EPISODE     |
| `EpisodeJP`      | `episode`                                   | EPISODE     |
| `SeasonJP`       | `season`                                    | SEASON      |
| `SeasonWord`     | `season`                                    | SEASON      |
| `SeasonOnly`     | `season`                                    | SEASON      |
| `SeasonSpecial`  | `season`, `tag`, `number?`                  | SPECIAL     |
| `Special`        | `tag`, `number?`                            | SPECIAL     |
| `BatchRange`     | `start`, `end`                              | BATCH_RANGE |
| `Version`        | `number`                                    | VERSION     |
| `Year`           | `value`                                     | YEAR        |
| `CRC32`          | `value`                                     | CRC32       |
| `Language`       | `value`                                     | LANGUAGE    |
| `BonusKeyword`   | `bonus_type`, `raw`                         | BONUS       |
| `DualAudio`      | (none)                                      | DUAL_AUDIO  |
| `Edition`        | `value`                                     | EDITION     |
| `Uncensored`     | (none)                                      | UNCENSORED  |
| `BitDepth`       | `value`                                     | UNKNOWN     |
| `HDRInfo`        | `value`                                     | UNKNOWN     |
| `Repack`         | (none)                                      | UNKNOWN     |

The `_TYPE_TO_KIND` mapping converts result types to `TokenKind` values. Types
mapped to `UNKNOWN` are still recognized as metadata (they appear in
`_METADATA_KINDS`) but have no dedicated kind — they prevent metadata from
leaking into series titles.

#### Classification strategies by token type

**BRACKET tokens** — first bracket is treated as release group (unless it
contains metadata). Subsequent brackets are expanded via `scan_words` if they
contain 2+ metadata words. CRC32 (8 hex chars) is detected directly.
Redistributor brackets (`[TGx]`, `[EZTV]`) are not treated as release groups.

**PAREN tokens** — checked against recognizers for YEAR, SEASON, EPISODE,
DUAL_AUDIO, UNCENSORED, and EDITION. If the paren contains 2+ metadata words, it
is expanded via `scan_words` into multiple tokens. Two-letter alpha content is
treated as a language/region code.

**TEXT tokens** — tried as: episode/season → year → known metadata keyword →
split on embedded episode markers → scan for trailing metadata after episode
marker. When `is_decimal_special` format (e.g. `01.5`) is detected, it is only
matched in non-dot-separated contexts to avoid false positives.

**DOT_TEXT tokens** — tried as: episode/season → year → known metadata keyword →
embedded episode marker → scene trailing group (`codec-GROUP` splitting).

#### Two scanning functions

`scan_words(text)` — for space/comma-separated text (bracket/paren content, bare
text). First pass tries multi-word recognizers across the full text, resolving
overlaps by position then longest match. Gaps between matches are classified
word-by-word. Handles dash-compound splitting (`DTS-HD MA`, `WEB-DL`).

`scan_dot_segments(text)` — for dot-separated scene-style names. Tries 3-segment
then 2-segment then 1-segment compounds at each position. Handles trailing
`-GROUP` suffixes. Unmatched segments become DOT_TEXT.

Both functions use the same `_RECOGNIZERS` list via `_try_recognize(text)`,
which requires a full-text match (the recognizer must consume the entire input).

### Phase 3: Assembly (`_build_parsed_media`)

Iterates classified tokens to populate `ParsedMedia`:

- **Series name**: TEXT/DOT_TEXT tokens before the first EPISODE marker
- **Episode title**: TEXT tokens after EPISODE but before metadata
- **Bilingual titles**: `/` or `|` in series name splits into `series_name` and
  `series_name_alt` (only when one side has CJK and the other doesn't, to avoid
  breaking titles like "Fate/stay night")
- **Special detection**: `TokenKind.SPECIAL` tokens (SP/OVA/OAD/ONA, S##OVA,
  S##OP, S##ED) set `is_special`, `special_tag`, and `bonus_type` directly.
  Season 0 and decimal episodes are handled by `_check_special` on EPISODE
  tokens
- **Multi-episode expansion**: `EpisodeMultiSE` populates `episodes: list[int]`
  with the expanded range (capped at 100 episodes)
- **Season upgrade**: when a bare episode (no season) is followed by an
  `(S01E01)` paren, the season is adopted from the paren (LoliHouse format)
- **Release group prefix stripping**: when a directory provides the release
  group and the filename series name starts with it, the prefix is removed
- **Directory metadata merging** (`parse_media_path`): directories provide
  `path_series_name` and fill metadata gaps via `_merge_scanned_metadata`

### Normalization

`normalize_for_matching`: NFC unicode normalization, lowercase, strip
non-alphanumeric, **preserve CJK characters** (hiragana, katakana, kanji,
fullwidth forms).

### Name variants

`name_variants(name)` returns all normalized keys for a series name:

1. Raw normalized name
2. Parser-extracted name (strips years, quality tags)
3. Alternate-language title (if present)
4. Metadata-truncated name via `clean_series_title` (truncates at first metadata
   keyword like `S01`, `BDRip`, `1080p`; strips trailing `[bracket]` content)

## From parser to anime commands

### ParsedMetadata

`parse_source_filename(filename)` calls `parse_component()` and maps the
`ParsedMedia` result into a `ParsedMetadata` dataclass on a `SourceFile`:

```text
ParsedMedia (parser)          →  ParsedMetadata (anime)
  .release_group                   .release_group
  .source_type                     .source_type
  .is_remux                        .is_remux
  .hash_code                       .hash_code
  .episode                         .episode
  .season                          .season
  .version                         .version
  .bonus_type                      .bonus_type
  .is_special                      .is_special
  .special_tag                     .special_tag
  .episode_title                   .episode_title
  .is_dual_audio                   .is_dual_audio
  .is_uncensored                   .is_uncensored
  .series_name_alt                 .series_name_alt
  .episodes                        .episodes
  .streaming_service               .streaming_service
```

Fields not carried over (used only within the parser): `series_name`,
`resolution`, `video_codec`, `audio_codecs`, `year`, `extension`, `batch_range`,
`is_criterion`, `path_series_name`, `path_is_batch`.

### MatchedFile (non-mutating overrides)

`_match_files_to_season` returns `list[MatchedFile]` — wrappers around the
original `SourceFile` with overridden episode/season values. The original pool
data is never mutated, so multi-cour processing works correctly.

`MatchedFile` provides `effective_*` properties that return the override value
if set, otherwise the original `source.parsed.*` value. Batch-level overrides
(release group, dual-audio, uncensored) are applied via MatchedFile fields.

Before passing to the manifest workflow, `to_source_snapshot()` bakes all
effective values into a `SourceFile` copy.

### Download matching (`etp anime series`)

#### Index construction

`_build_download_index` walks the downloads directory recursively and indexes
each media file under all `name_variants` of its series name, plus
`clean_series_title` applied to raw directory components.

#### Matching algorithm

For each source file, `_match_to_downloads` collects download entries from all
keys returned by `TitleAliasIndex.matching_keys(series_name, index_keys)`:

1. Direct name variants (raw, parsed, cleaned, alt-title)
2. Alias expansion from cached AniDB/TVDB metadata
3. Prefix matching: if a candidate key is a prefix of an index key or vice versa

Then two matching passes:

**Pass 1 — exact (season, episode):** Find download entries with the same
`(season, episode)` tuple. Pick the closest file size. Reject if both files have
release groups and the first word differs.

**Pass 2 — size + release group fallback:** When pass 1 fails, search all series
entries for an exact file-size match with the same release group. Handles
DVD-to-aired order renumbering. **Skipped for season 0** (TVDB specials).

The matched download's release group, CRC32, version, source type, dual-audio,
uncensored, and streaming service enrich the source file.

#### Title alias index

Built at startup from cached AniDB XML and TVDB JSON in `$XDG_CACHE_HOME/etp/`.
Parser-detected alt titles (`series_name_alt`) and config concise names are fed
into the index. Updated incrementally after each metadata fetch.

## AniDB per-season handling

AniDB assigns separate IDs per season. Files are processed one ID at a time
against a shrinking pool:

1. Filter pool by sub-series title similarity (normalized title comparison)
2. Group remaining files by parsed season number
3. User picks which season maps to this AniDB ID
4. Separate regular episodes from specials/bonus files (files with `is_special`
   or `bonus_type` are not counted against the regular episode limit)
5. If more regular episode files than AniDB episodes, take first N; rest goes to
   next ID
6. **Non-mutating renumbering**: episodes are renumbered via `MatchedFile`
   wrappers (e.g. S01E13-S01E24 → ep 1-12 for the second AniDB ID). The original
   pool data is preserved for subsequent passes.

### Specials

Special detection sources (combined):

- **Parser**: `is_special` flag (season 0, SP/OVA tags, decimal episodes)
- **Parser**: `bonus_type` ("NCOP", "NCED", "PV", "CM", "Preview", "Menu",
  "Bonus") from English keywords and Japanese metadata
  (映像特典, ノンテロップOP)
- **Parser**: `special_tag` ("SP1", "S01OVA", "S03OP") from season-special
  patterns

`build_manifest_entries` classifies each file through four branches:

1. **Special without bonus type**: look up by episode number in specials pool
   (TVDB `s0eNN` or parser-detected specials)
2. **Regular episode**: look up episode title from metadata
3. **Special with bonus type**: try `_match_bonus_to_anidb_special()` against
   available AniDB specials (NCOP → credit episodes with "Opening" in title,
   NCED → credit episodes with "Ending", others → normalized title comparison).
   Falls back to parser's special tag if no AniDB match.
4. **Bonus type only** (no episode number): try AniDB matching, then assign
   HamaTV-compatible s0e number if unmatched

#### HamaTV episode number ranges

For bonus files that cannot be matched to AniDB specials:

| Bonus type | Range start | HamaTV category |
| ---------- | ----------- | --------------- |
| NCOP       | 171         | s0e151+         |
| NCED       | 191         | s0e171+         |
| PV         | 321         | s0e301+         |
| Preview    | 321         | s0e301+         |
| CM         | 521         | s0e501+         |
| Bonus      | 521         | s0e501+         |
| Menu       | 921         | s0e901+         |

When using TVDB, ranges start after the highest existing TVDB special number
(+20 buffer) to avoid collisions in the single `Specials/` directory.

Unmatched bonus files are tagged `(todo)` in the manifest for user review.

## ManifestWorkflow

The `ManifestWorkflow` class in `manifest.py` orchestrates the full batch
processing sequence:

1. **Build** (`build_manifest_entries`): analyze each file with mediainfo,
   verify CRC32 hashes, match episodes to metadata, construct destination paths
2. **Write**: generate a KDL manifest file
3. **Edit loop**: open `$EDITOR`, parse the result, retry on errors
4. **Execute**: copy files to destinations using Btrfs COW reflinks
5. **Cleanup**: remove the temp manifest file

### Manifest validation

`parse_manifest` validates user edits with error reporting:

- Missing `source` or `dest` fields: reported with episode label
- Non-integer season numbers: caught (previously crashed)
- Unknown source paths: hints at available source paths
- `(todo)` entries: must be resolved or deleted before execution
- Extras with missing fields: reported (previously silently dropped)
- KDL parse errors: reported with parser error message

## Output formats

### Directory name

```text
{title_ja} [{title_en}] ({year})    # when ja has kanji/kana and en differs
{title} ({year})                     # otherwise
```

### Episode filename

```text
{name} - s{season}e{ep:02d} - {title} [{metadata}] [{hash}].{ext}
```

Specials use the special tag instead of `sXeYY`:

```text
{name} - {special_tag} - {title} [{metadata}] [{hash}].{ext}
```

### Metadata block

`{group} {source},{REMUX,}{resolution},{codec},{hdr,}{10bit,}{enclib,}{audio},{lang}`

Example: `MTBB(v2) BD,REMUX,1080p,HEVC,10bit,x265,flac+aac,dual-audio`

### Batch manifest (KDL)

```kdl
season 1 {
  episode 1 {
    source "/path/to/source.mkv"
    downloaded "/path/to/original/download.mkv"
    dest "Series - s1e01 - Title [metadata] [hash].mkv"
  }
}
```

- `source`: Sonarr-managed file (read-only reference)
- `downloaded`: matched original download (read-only, present when matched)
- `dest`: target filename (editable)
- `/- episode …` skips an entry; `(todo)` entries must be resolved
- `// CRC32 MISMATCH` marks files where the hash was stripped

## Conflict resolution

Before copying, checks for existing files at the destination (exact path and
fuzzy episode match via `sXeYY` parsing):

- **Same metadata + same CRC32**: auto-replace (fixing naming)
- **Same metadata + different CRC32**: prompt user
- **Different metadata**: show comparison, prompt `[k]eep / [r]eplace / [s]kip`

## Type safety

StrEnum types in `types.py` replace raw string comparisons:

- `EpisodeType`: REGULAR, SPECIAL, CREDIT, TRAILER, PARODY, OTHER
- `BonusType`: NCOP, NCED, PV, PREVIEW, CM, MENU, BONUS
- `MetadataProvider`: ANIDB, TVDB

These are backwards-compatible with string equality
(`EpisodeType.REGULAR == "regular"` is True) but provide IDE autocompletion and
pyright validation.
