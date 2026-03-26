# etp-anime parsing and substitution rules

This document captures all parsing logic and text substitution rules used by
`etp-anime` when processing filenames, API responses, and output paths.

## Source filename parsing

`parse_source_filename` extracts structured metadata from anime release
filenames. Patterns are tried in the order listed; the first match wins for each
field.

### Release group

Three patterns are tried in priority order:

1. **Bracket at start**: `^\[([^\]]+)\]` — fansub convention
   - `[Cyan] Show - 05.mkv` → `Cyan`
   - `[FLE] Re ZERO ... [4CC4766E].mkv` → `FLE`
2. **Scene trailing dash**: `-([A-Za-z][A-Za-z0-9]+)` before the file extension
   - `Show.S03E09.1080p.WEB-DL.DUAL-VARYG.mkv` → `VARYG`
3. **Bracket anywhere** (fallback): `\[([A-Za-z]{2,6})\]` — short all-alpha tags
   that aren't CRC32 hashes (which are 8 hex chars)
   - `Re ZERO ... [Dual Audio] [PMR].mkv` → `PMR`
   - Won't match `[Dual Audio]` (space), `[1080p]` (digits), or `[4CC4766E]` (8
     hex chars, matched as hash instead)

When no release group is detected, the user is prompted in interactive mode. In
batch triage mode, the metadata block omits the group.

### Episode number

Patterns are tried in order of specificity (most constrained first). All allow
an optional `v\d+` version suffix (e.g., `05v2`).

1. **Dot S/E** (scene naming): `.S01E05.` with dots on both sides
   - `Show.S01E05.1080p.mkv` → season 1, episode 5
2. **S/E** (general): `S01E05` anywhere
   - `[Group] Show - s1e05 - Title.mkv` → season 1, episode 5
3. **Dash** (fansub naming): ` - 05` followed by whitespace, bracket, dot, or
   end
   - `[Cyan] Show - 08 [1080p].mkv` → episode 8
   - `[MTBB] Show - 05v2 [hash].mkv` → episode 5, version 2
4. **EP prefix**: `EP05` or `E5` followed by whitespace, bracket, dot, or end
   - `Show EP12 [720p].mkv` → episode 12

When no episode number is detected, the user is prompted interactively. In batch
triage mode, the entry is marked with a `(todo)` tag.

### Version

Captured from the `v\d+` suffix on any episode pattern. Stored as an integer
(e.g., `2` for `v2`). When present, the version is appended to the release group
in the metadata block: `MTBB` → `MTBB(v2)`.

### CRC32 hash

Pattern: `\[([0-9A-Fa-f]{8})\]`

Matches an 8-character hex string in brackets anywhere in the filename.

- `[Cyan] Show - 08 [D98B31F3].mkv` → hash `D98B31F3`

### CRC32 verification

When a hash is present, it is verified against the actual file contents before
copying. `verify_hash` computes the CRC32 of the file and returns both the match
result and the computed hash (avoiding a redundant re-read on mismatch).

- **Match**: hash is preserved in the destination filename
- **Mismatch**: in interactive mode, the user is prompted; in batch mode, a
  `// CRC32 MISMATCH` comment is added to the manifest. In both cases, if the
  copy proceeds, the hash is **stripped** from the destination filename by
  clearing `source.hash_code` before `format_episode_filename` is called.

### Source type

Keyword-based detection (word boundary, case-insensitive):

- **BD**: `BD`, `Blu-Ray`, `BluRay`, `BDRip`, `BDREMUX`
- **Web**: `WEB`, `WEBRip`, `WEB-DL`, `CR`, `AMZN`, `DSNP`, `HULU`, `NF`

### REMUX detection

Pattern: `REMUX` (case-insensitive). Sets `is_remux = True`.

### Series name extraction

`_strip_series_name` extracts a candidate series name by stripping metadata from
the filename stem, applied in order:

1. Strip leading release group: `[Group] ` prefix
2. Strip trailing CRC32 hash: ` [ABCD1234]`
3. Strip episode suffix (tried in order, first match wins):
   - ` - 05 [...` (dash-episode followed by metadata)
   - ` - S01E05...` (dash then SxEy)
   - `.S01E05...` (dot then SxEy, scene naming)
   - ` - 05` at end of string (trailing dash-episode, no metadata)
4. Strip trailing whitespace

### Grouping normalization

`_normalize_for_grouping` lowercases the name and strips all non-alphanumeric
characters for use as a dict key when grouping files by series in triage mode.

## AniDB API response parsing

`_parse_anidb_xml` processes the XML response from AniDB's HTTP API.

### Series titles

Title elements have `xml:lang` and `type` attributes. Candidates are collected
in a single pass, then selected by priority:

**Japanese title** (highest to lowest priority):

1. `lang="ja" type="official"` — native Japanese (kanji/kana)
2. `lang="ja" type="main"` — Japanese main title
3. `lang="x-jat" type="main"` — romaji (romanized Japanese)
4. Any `type="main"` title (language-agnostic fallback)

**English title** (highest to lowest priority):

1. `lang="en" type="official"`
2. `lang="en" type="main"`

### Episode titles

For each episode element, titles are extracted from child `<title>` elements:

- `lang="en"` → `title_en` (first match)
- `lang="ja"` → `title_ja` (first match)

### Episode title substitutions

- **English titles only**: backtick (`` ` ``) is replaced with straight
  apostrophe (`'`)

### Episode type mapping

The `type` attribute on `<epno>` maps to episode types:

| AniDB type | Episode type | Tag format |
| ---------- | ------------ | ---------- |
| `1`        | `regular`    | (none)     |
| `2`        | `special`    | `S1`, `S2` |
| `3`        | `credit`     | `C1`, `C2` |
| `4`        | `trailer`    | `T1`, `T2` |
| `5`        | `parody`     | `P1`, `P2` |
| `6`        | `other`      | `O1`, `O2` |

## TheTVDB API response parsing

`_parse_tvdb_json` processes JSON responses from the TheTVDB v4 API.

### Series titles

Title resolution uses canonical translations from the
`/series/{id}/translations/{lang}` endpoint when available, falling back to the
series data and aliases. Only `eng` and `jpn` translations are fetched, and only
when listed in the series' `nameTranslations` array.

**Japanese title** (highest to lowest priority):

1. Canonical `jpn` translation (from translations endpoint)
2. Primary `name` field (the original-language title — Japanese for anime)

**English title** (highest to lowest priority):

1. Canonical `eng` translation (from translations endpoint)
2. First alias with `language: "eng"` from the `aliases` array

Translations are cached alongside the series and episode data in
`$XDG_CACHE_HOME/etp/tvdb/{series_id}.json`.

### Episode titles

Episodes are fetched from `/series/{id}/episodes/default/eng` to get
English-language episode names. The `name` field from each episode object is
used as `title_en`. No Japanese episode titles are available from this endpoint
(`title_ja` is set to empty string).

### Episode type mapping

Episodes with `seasonNumber == 0` are classified as specials (tag `S{number}`).
All other episodes are regular.

## Path sanitization

`_sanitize_path` is applied to all title strings before they are used in
directory names or filenames:

- `/` is replaced with space-dash-space — path separator on all platforms
- `:` is replaced with `-` — HFS legacy separator on macOS

## Redundant year stripping

`_strip_redundant_year` removes a trailing ` (YYYY)` suffix from a title when
the year matches the series release year, to avoid duplication in directory
names that already include the year:

- `鋼の錬金術師 (2009)` with year 2009 → `鋼の錬金術師`
- `鋼の錬金術師 (2003)` with year 2009 → kept as-is (years differ)

This is applied to both Japanese and English titles in `format_series_dirname`.

## Output format reference

### Directory name

Full format (when Japanese title contains kanji/kana and English title differs):

```
{title_ja} [{title_en}] ({year})
```

Single-title format (when Japanese title is romaji or empty, English title is
empty, or both titles are identical after sanitization):

```
{title} ({year})
```

The English title is preferred for the single-title format; the Japanese title
is used as a fallback when no English title exists. Both titles have path
sanitization and redundant year stripping applied before formatting.

Japanese character detection uses a regex matching Hiragana (U+3040–309F),
Katakana (U+30A0–30FF), CJK Unified Ideographs (U+4E00–9FFF), and CJK Extension
A (U+3400–4DBF).

### Episode filename

```
{concise_name} - s{season}e{episode:02d} - {episode_name} [{metadata}] [{hash}].{ext}
```

Variations:

- **No episode name**: `Name - s1e05 [metadata].mkv`
- **Special**: `Name - S1 - Episode Name [metadata] [hash].mkv`
- **Movie**: `DirName - complete movie [metadata] [hash].mkv`
- **Hash stripped** (CRC32 mismatch): hash bracket omitted entirely

### Metadata block

Format: `{prefix},{technical fields}`

**Prefix** (space-separated):

- Release group with optional version: `MTBB(v2)` or `MTBB`
- Source type: `BD` or `Web`

**Technical fields** (comma-separated, in order):

1. `REMUX` (if flagged)
2. Resolution (e.g., `1080p`)
3. Video codec (e.g., `HEVC`, `AVC`)
4. HDR type (e.g., `HDR`, `DoVi`) — if present
5. `10bit` — if bit depth >= 10
6. Encoding library (e.g., `x264`, `x265`) — if detected
7. Audio codecs joined by `+` (e.g., `flac+aac`)
8. Audio language: `dual-audio` (ja+en), `multi-audio` (ja+en+other), or omitted
   (single language)

Example: `MTBB(v2) BD,REMUX,1080p,HEVC,10bit,x265,flac+aac,dual-audio`

## Batch triage manifest (KDL format)

In `--triage` mode, the script generates a KDL manifest file grouped by season,
with source and destination filenames on separate lines for readability. The
manifest is opened in `$VISUAL` / `$EDITOR` / `vi` for editing.

```kdl
// etp-anime triage manifest
// Series: 葬送のフリーレン [Frieren- Beyond Journey's End] (2023)
// AniDB: 17617
// Series dir: /volume1/video/anime/...

season 1 {
  episode 1 {
    source "[FLE] Show - S01E01 ... [4CC4766E].mkv"
    dest "Show - s1e01 - Episode Title [FLE BD,...] [4CC4766E].mkv"
  }
}

season 3 {
  // CRC32 MISMATCH — hash stripped from destination
  episode 1 {
    source "Show - S03E01v2 ... [PMR].mkv"
    dest "Show - s3e01 - Episode Title [PMR(v2) BD,...].mkv"
  }
  (todo)episode 0 {
    source "unmatched_file.mkv"
    dest "Show - s1eXX - EPISODE_NAME [...].mkv"
  }
}
```

- Entries are sorted by episode number within each season group
- `source` is the original filename (read-only reference)
- `dest` is the target filename (editable)
- Season/specials directory is derived from the parent node
- `/- episode ...` (KDL slashdash) skips an entry — the parser excludes it
- `(todo)` tagged entries are rejected at parse time until resolved
- `// CRC32 MISMATCH` comments mark files where the hash was stripped
