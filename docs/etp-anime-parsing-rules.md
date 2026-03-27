# etp-anime parsing and matching rules

Rules for filename parsing, download matching, and output construction in
`etp-anime`. Parsing is handled by `etp_lib.media_parser`; the anime module
consumes its output.

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

A two-phase pipeline: structural tokenizer → semantic classifier.

### Tokenizer

Walks input character-by-character, extracting:

- `[content]` — square brackets (release groups, metadata, CRC32 hashes)
- `(content)` — parentheses with depth-tracked nesting
- `「content」` — Japanese lenticular quotes (episode titles)
- `-` — dash separators
- Dot-separated scene names (2+ dots, no spaces): split on `.` preserving
  compound tokens (`H.264`, `AAC2.0`, `WEB-DL`, `DTS-HD`)

Full paths are split on `/`; each component is tokenized independently.

### Classifier

Reclassifies structural tokens against vocabularies:

| Token kind    | Examples                                             |
| ------------- | ---------------------------------------------------- |
| RELEASE_GROUP | `[Cyan]`, trailing `-VARYG`, Sonarr `[GROUP QUAL-…]` |
| CRC32         | `[D98B31F3]` (8 hex chars in brackets)               |
| EPISODE       | `S01E05`, ` - 08`, `第01話`, `EP05`, `SP1`, `OVA`    |
| SEASON        | `S01`, `(第1期)`, `4th Season`                       |
| VERSION       | `v2`, `v3`                                           |
| RESOLUTION    | `1080p`, `720p`, `1920x1080`                         |
| VIDEO_CODEC   | `HEVC`, `AVC`, `x265`, `H.264`                       |
| AUDIO_CODEC   | `AAC`, `FLAC`, `DTS-HD`, `AAC2.0`                    |
| SOURCE        | `BD`, `BluRay`, `WEB-DL`, `CR`, `AMZN`               |
| YEAR          | 4-digit number 1900–2099                             |
| EPISODE_TITLE | content of `「…」`                                   |
| BONUS         | `映像特典`, `ノンテロップOP`                         |

For scene-style names with multiple dash-separated groups (e.g.
`10-Bit.x265-iAHD`), the **last** group is taken as the release group.

### Title extraction

Residual approach: everything not classified as metadata = series title. Three
strategies by naming style:

- **Fansub** (`[Group] Title - Ep [meta]`): title = text between group and last
  separator before the episode number
- **Scene** (`Title.S01E05.meta.codec-Group`): title = dot-text tokens before
  the episode marker, joined with spaces
- **Japanese** (`[Group] Title(第N期) 第XX話「EpTitle」(specs)`): title = text
  between group and season/episode markers

### Normalization

`normalize_for_matching`: lowercase, strip non-alphanumeric, **preserve CJK
characters** (hiragana, katakana, kanji, fullwidth forms).

### Name variants

`name_variants(name)` returns all normalized keys for a series name:

1. Raw normalized name
2. Parser-extracted name (strips years, quality tags)
3. Metadata-truncated name via `clean_series_title` (truncates at first metadata
   keyword like `S01`, `BDRip`, `1080p` — handles both space-separated and
   dot-separated directory names)

## Download matching (`etp anime series`)

### Index construction

`_build_download_index` walks the downloads directory recursively and indexes
each media file under all `name_variants` of its series name, plus
`clean_series_title` applied to raw directory components. Results are cached per
`(series_name, directory)` to avoid redundant parsing.

### Matching algorithm

For each source file, `_match_to_downloads` collects download entries from all
keys returned by `TitleAliasIndex.matching_keys(series_name, index_keys)`:

1. Direct name variants (raw, parsed, cleaned)
2. Alias expansion from cached AniDB/TVDB metadata
3. Prefix matching: if a candidate key is a prefix of an index key or vice versa
   (handles short vs long title variants)

Then two matching passes:

**Pass 1 — exact (season, episode):** Find download entries with the same
`(season, episode)` tuple. Pick the closest file size. Reject if both files have
release groups and the first word differs (different encodes).

**Pass 2 — size + release group fallback:** When pass 1 fails, search all series
entries for an exact file-size match with the same release group. Handles
DVD-to-aired order renumbering where episode numbers differ but file contents
are identical. **Skipped for season 0** (TVDB specials).

The matched download's release group, CRC32, version, and source type replace
the Sonarr-reformatted values on the source file.

### Title alias index

Built at startup from cached AniDB XML and TVDB JSON in `$XDG_CACHE_HOME/etp/`.
AniDB titles of type `main`, `official`, and `synonym` are indexed; TVDB aliases
and canonical translations are indexed. Concise names from the anime config are
also fed in, linking directory names to clean parser-extracted names.

The index is updated incrementally after each metadata fetch so that newly
learned title mappings improve matching within the same session.

## AniDB per-season handling

AniDB assigns separate IDs per season. Files are processed one ID at a time
against a shrinking pool:

1. Group pool files by parsed season number
2. User picks which season maps to this AniDB ID
3. If more files than AniDB episodes, take first N; rest goes to next ID
4. **Renumber only for multi-cour splits**: episodes are renumbered to start at
   1 only when the last episode exceeds the AniDB entry's episode count (e.g.
   S01E13–S01E24 → ep 1–12). Single-season files are not renumbered (e.g. ep 12
   of a 12-episode entry stays as 12).

### Specials

Season 0 in TVDB indicates specials. Specials may use AniDB naming (`S1`,
`NCOP1a`), TVDB naming (`S00EYY`), or have no clear numbering. Ambiguous files
are tagged `(todo)`.

## Output formats

### Directory name

```
{title_ja} [{title_en}] ({year})    # when ja has kanji/kana and en differs
{title} ({year})                     # otherwise
```

### Episode filename

```
{name} - s{season}e{ep:02d} - {title} [{metadata}] [{hash}].{ext}
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
