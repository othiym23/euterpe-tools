# Plan: Sonarr-inspired anime parsing enhancements

**Date:** 2026-03-30 **Status:** Draft **Branch:** `rei/explore/parsy-parser`

## Background

Sonarr's parser uses ~60 anime-specific regex patterns to handle formats that
our tokenize/classify/assemble pipeline doesn't yet cover well. This plan
proposes integrating the most valuable of those patterns as typed recognizers
within our existing architecture.

Items 1-4 from the Sonarr integration list are already implemented:

1. CRC hashes — already had `crc32` recognizer
2. Multi-episode range expansion — `EpisodeMultiSE` recognizer added
3. Year validation — reject < 1940 and > current year + 1
4. `[SubGroup]` prefix — already handled by bracket tokenizer

This plan covers item 5: absolute episode numbering and Chinese/Japanese mixed
title support.

## What Sonarr does

### Absolute episode numbering

Sonarr uses negative lookahead/lookbehind to distinguish bare absolute episode
numbers (001, 012, 1001) from years and other numeric fields:

```regex
(?<!\d+)\d{2,3}(\.\d{1,2})?(?!\d+)
```

Key technique: a 4-digit year like "2025" has 4 consecutive digits, so `\d{2,3}`
matches only "202" and the `(?!\d+)` fails on the trailing "5".

For long-running anime (1000+ episodes), separate 4-digit patterns:

```regex
(?<!\d+)\d{4}(?!\d+)
```

Decimal episodes (01.5) mark special/OVA episodes between regular episodes.

### Chinese/Japanese mixed titles

Several format families:

1. **LoliHouse-style:** `[Group] Title - 001 (S01E01) [hash]`
   - Absolute number before parenthesized SxxExx
   - Both numbering systems captured

2. **Lilith-Raws batch:** `[Group] Title - 01 ~ 12 [hash]`
   - Tilde separator for absolute episode ranges

3. **GM-Team:** `[Group] Title (Season 01) 01`
   - Season in parentheses, absolute number after

4. **CJK + English:** `[Group] 中文标题 English Title S01E05`
   - Unicode range `[\u4E00-\u9FCC]` for CJK characters
   - Multiple title captures (one CJK, one Latin)

### How Sonarr decides first bracket = subgroup

It doesn't analyze content. All anime patterns start with
`^(?:\[(?<subgroup>.+?)\]...)` — the first `[bracket]` at position 0 is _always_
the subgroup. This works because anime release naming convention places the
group first and metadata brackets later.

## Proposed changes

### A. Absolute episode numbers as a recognizer

**New recognizer: `episode_absolute`**

Matches 2-4 digit numbers using Sonarr's negative lookahead technique, but
implemented as a parsy primitive (refutable matching, not a regex-first-match
approach):

```python
@dataclass(frozen=True, slots=True)
class EpisodeAbsolute:
    episode: int
    decimal: float | None = None  # 01.5 → special

def _absolute_episode_parser(stream, index):
    # Match 2-4 digits not surrounded by other digits
    # Reject if in year range (1940-2027) unless decimal
    ...
```

**Placement in `_RECOGNIZERS`:** After `episode_se` / `episode_multi_se` (SxxExx
takes priority) but before `episode_bare` (which currently handles bare numbers
less specifically).

**Tradeoff — distinguishing absolute episodes from other numbers:**

This is the hardest part. A bare `012` could be:

- An absolute episode number
- Part of a title ("101 Dalmatians")
- A batch count
- A track number

Sonarr solves this by having 60 distinct regex patterns that each match a
_specific structural context_ (e.g., "[Group] Title - NNN [metadata]"). Our
tokenizer already provides that structural context via bracket/text/separator
tokens, but we'd need to add **context-aware classification rules**, not just
recognizer-level matching.

**Approach:** Don't try to recognize absolute episodes at the recognizer level.
Instead, add a heuristic in `_build_parsed_media` or
`_extract_title_from_tokens`: when no SxxExx episode was found, and the filename
starts with a `[bracket]` (fansub style), check if the first bare number after a
separator is plausibly an absolute episode.

Heuristic signals that a bare number is an absolute episode:

- Preceded by `-` separator (strong signal)
- 2-3 digits, not in year range
- Followed by metadata or end of filename
- File has fansub-style structure (`[Group] Title - ...`)

**Unresolved issue:** Our `episode_bare` recognizer already handles this for
most cases. The gap is specifically:

- 4-digit absolute episodes (currently parsed as years if 1940-2027)
- Decimal specials (01.5) — not currently handled
- Distinguishing "101" as episode vs. title component in edge cases

**Recommendation:** Start with decimal special support (01.5) and 4-digit
absolute episodes (disambiguated by requiring preceding `-` separator or
`[Group]` context). Defer the "101 is episode vs title" ambiguity — our current
heuristic (bare number after separator = episode) works for ~95% of cases, and
the user has said that's the target.

### B. Chinese/Japanese mixed title support

**Problem statement:** Filenames like
`[LoliHouse] 中文标题 / English Title - 01 [1080p] [hash].mkv` currently work
partially — the bracket tokenizer handles `[LoliHouse]` as a group and `[hash]`
as CRC, but the title extraction may include the `/` separator between CJK and
Latin titles.

**Proposed changes:**

1. **Title separator recognition:** In `_extract_title_from_tokens`, recognize
   `/` and `|` within title text as variant separators. Store primary (first)
   title in `series_name` and add `series_name_alt: str = ""` field to
   `ParsedMedia` for the alternate-language title.

2. **CJK-aware word boundary handling:** The `scan_words` function splits on
   spaces. CJK text has no spaces between characters. This already works because
   CJK text appears as a single TEXT token, but we should ensure
   `normalize_for_matching` handles CJK-Latin hybrid titles properly (it already
   preserves CJK via the `_RE_NON_ALNUM_CJK` regex).

3. **LoliHouse dual-numbering:** Files like
   `[LoliHouse] Title - 001 (S01E01) [hash]` need the `(S01E01)` in parentheses
   to take priority over the bare `001`. This already works — paren content is
   classified and SxxExx is found — but we should verify with tests.

**Tradeoff — `series_name_alt` field:**

Adding a second series name field increases the API surface. The alternative is
to keep both in `series_name` separated by `/` and let consumers split. The
dedicated field is cleaner for matching (the AniDB title index has separate
entries per language) but adds a field that's empty 95% of the time.

**Recommendation:** Add `series_name_alt` and populate it when a `/` or `|`
separator is found in the title zone. Low complexity, high value for the AniDB
matching use case.

### C. Decimal episode specials

**New support in `EpisodeBare`:**

```python
@dataclass(frozen=True, slots=True)
class EpisodeBare:
    episode: int
    version: int | None = None
    decimal: float | None = None  # 01.5 → special between ep 1 and 2
```

When `decimal` is set, `_build_parsed_media` should set `is_special = True`.
This follows Sonarr's `SpecialAbsoluteEpisodeNumbers` pattern.

**Tradeoff:** Decimal episodes in dot-separated filenames (scene style) are
ambiguous — `Title.01.5.mkv` could be episode 1.5 or episode 1 followed by a
text segment "5". This only makes sense in fansub-style filenames where the
structure is unambiguous. Restrict decimal episode matching to contexts where
the number follows a `-` separator or appears inside brackets/parens.

## Implementation order

1. **Decimal episode specials** — Small, self-contained change to `EpisodeBare`
   and bare episode parser. Add `is_special` detection.

2. **Title variant separator** — Add `series_name_alt` field, split on `/` and
   `|` in title zone. Update `normalize_for_matching` and `name_variants` to
   include the alt title.

3. **4-digit absolute episodes** — Update `episode_bare` to accept 4-digit
   numbers when they're clearly episodes (after separator, not in year range, or
   when accompanied by `[Group]` structure). Requires context-awareness in the
   classifier.

4. **LoliHouse/Lilith-Raws dual numbering** — Add tests to confirm `(S01E01)` in
   parens takes priority over bare absolute. If not, add priority logic.

## Unresolved issues

1. **Absolute vs. title number ambiguity:** "101 Dalmatians" has a number that
   looks like an absolute episode. Sonarr handles this with 60 regex patterns
   that each match a specific structural format. Our tokenizer provides
   structural context but the classifier doesn't currently use it for episode
   disambiguation. This is inherent in heuristic parsing and can't be fully
   solved without a database lookup.

2. **4-digit episode vs. year:** One Piece episode 1001 and year 2001 are
   structurally identical in isolation. Sonarr's negative lookahead works on raw
   strings but our tokenizer has already split the string. We'd need to carry
   positional context (was this token preceded/followed by digits?) or add a
   special-case rule: if a year-range number appears after a separator and a
   `[Group]` is present, prefer episode interpretation.

3. **Batch tilde range for absolute episodes:** `[Group] Title - 01 ~ 12`
   already works via our `batch_range` recognizer (which matches `\d+~\d+`). No
   change needed, but we should add test coverage.

4. **GM-Team `(Season N)` format:** Currently, `(Season 1)` in parentheses would
   be classified as paren text containing "Season 1". The `season_word`
   recognizer handles "1st Season", "2nd Season" etc. but not "Season 1" or
   "Season 01". Minor addition to the recognizer.

## Complexity assessment

| Change              | Lines       | Risk                       | Value                      |
| ------------------- | ----------- | -------------------------- | -------------------------- |
| Decimal specials    | ~30         | Low                        | Medium — niche but correct |
| Title alt separator | ~40         | Low                        | High — CJK matching        |
| 4-digit absolute    | ~20         | Medium — year/ep ambiguity | Medium — rare (One Piece)  |
| LoliHouse dual num  | ~10 (tests) | Low                        | Low — likely already works |
| GM-Team Season N    | ~10         | Low                        | Low — uncommon format      |

Total: ~110 lines of production code, ~100 lines of tests. No architectural
changes. All implemented as recognizers or classifier heuristics within the
existing pipeline.
