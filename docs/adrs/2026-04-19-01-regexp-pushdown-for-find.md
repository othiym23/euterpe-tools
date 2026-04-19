# Push `etp-find` Pattern Matching into SQLite via REGEXP UDF

- **Status**: Accepted
- **Date**: 2026-04-19

## Context

`etp-find` originally pulled every row in the `files` table into Rust
(`stream_all_files` / `list_all_files`), reconstructed each full path with
`format!`, and tested it against a compiled `regex::Regex`. On the production
music database (824k files, 278 MB DB) a warm-cache `etp find -i swans` took
5.52 s wall (8.81 s user CPU). A plain `xsv search` over the same filenames as a
flat CSV finished in under a second — the gap was not the regex engine, it was:

- Per-row `format!` allocations (two per row: one in `FileRecord` construction,
  one in `matches_pattern`)
- `Path::components()` walks in the system-file and dotfile filters
- sqlx row materialization for rows that would be discarded immediately

Substring grepping a flat buffer doesn't pay any of those costs.

sqlx 0.8's `sqlx-sqlite` crate ships a `regexp` feature that registers a
`REGEXP` scalar UDF backed by the Rust `regex` crate, with per-statement auxdata
caching of compiled patterns. With the UDF available, a SQL `WHERE … REGEXP ?`
can discard non-matching rows at SQLite's B-tree scan layer — in C — before they
ever cross the sqlx boundary.

A short-lived design used SQL `LIKE` for patterns that contained no regex
metacharacters, with a Rust `str::contains` re-verify to enforce
case-sensitivity (SQLite's `LIKE` is ASCII case-insensitive by default).
Benchmarking on the real DB showed REGEXP with a literal pattern beat
LIKE+verify by ~30% wall-time, because LIKE's ASCII case-folding makes it return
more candidate rows and the `regex` crate compiles a literal pattern to a SIMD
memmem scan that's at least as fast as LIKE's inner loop.

## Decision

`etp-find` always runs pattern matching inside SQLite via the REGEXP UDF.

1. Enable the `regexp` feature on the sqlx dependency and call `.with_regexp()`
   on `SqliteConnectOptions` in both `open_db` and `open_memory`. The UDF
   registers per-connection.
2. `dao::list_files_matching` / `dao::stream_files_matching` run the query
   `WHERE (<full_path>) REGEXP ?`, where `<full_path>` is the existing SQL
   concatenation already used elsewhere in the DAO.
3. `-i` is expressed as a `(?i)` prefix on the pattern string before binding, so
   the Rust `regex` crate applies Unicode simple case folding inside the UDF.
   There is no separate case-insensitive SQL operator.
4. When `FilterConfig::include_system_files` is false, a second REGEXP filter
   (`AND NOT (<full_path>) REGEXP ?`) is added to the WHERE clause. The
   system-path regex is built at call time from `FilterConfig::system_patterns`
   via `regex::escape` with component-anchored alternation:
   `(?:^|/)(?:<patterns>)(?:$|/)`. SQLite short-circuits `AND` chains, so rows
   that fail the user pattern never trigger the system-regex evaluation.
5. The `should_show_post_sql` branch of `FilterConfig` skips the expensive
   `is_system_path` walk over every path component, since the SQL layer has
   already applied the system filter when one was passed. Dotfile and user
   exclude checks remain Rust-side against the filename alone.
6. Four static `&'static str` query constants cover the four combinations of
   scan-id filter × system-filter. A helper `find_query_str(bool, bool)` picks
   the right one so the streaming API can hand sqlx a static pointer.

Pattern input is validated up front via `ops::compile_pattern` in each binary
caller so bad regex syntax fails with a clear Rust error message rather than
sqlx's less helpful `SQLITE_CONSTRAINT` error at execution time.

### Evolution

An earlier version of this work (committed on the same branch, then removed)
used a dispatch matrix:

| Pattern           | `-i` | SQL op          | Rust post-filter         |
| ----------------- | ---- | --------------- | ------------------------ |
| literal (no meta) | off  | `LIKE … ESCAPE` | `str::contains` re-check |
| literal           | on   | `REGEXP (?i)…`  | none                     |
| regex metachars   | off  | `REGEXP …`      | none                     |
| regex             | on   | `REGEXP (?i)…`  | none                     |

The LIKE path was dropped when benchmarking showed it was slower than REGEXP on
the same data. Dropping the dispatch removed `is_literal_pattern`,
`escape_like_pattern`, and two of the SQL query constants — a net reduction of
~165 lines with no perceptible user-facing change.

## Consequences

- Warm-cache `etp find -i swans` on the 824k-row music DB drops from 5.52 s →
  0.96 s wall, and user CPU from 8.81 s → 0.89 s. The speedup is dominated by
  avoided sqlx row materialization, not regex engine performance.
- `etp-find`'s case-sensitivity is a property of the compiled regex, not of the
  SQL operator. `-i` expands to a `(?i)` prefix in Rust, which means full
  Unicode _simple_ case folding (e.g., `Björk` ↔ `björk`, `Σ` ↔ `σ`). The `ß` ↔
  `SS` 1:2 mapping is _full_ case folding, which the `regex` crate does not
  implement — documented with explicit tests in `find_sql_pushdown.rs`.
- When `include_system_files` is false, system paths never cross the sqlx
  boundary. Real gain is pattern-dependent: narrow patterns (`swans`) see little
  difference because the user REGEXP already rejects most rows; patterns that
  match many system files (`jpg` matching @eaDir thumbnails) benefit more.
- The system-regex is rebuilt per call from `FilterConfig::system_patterns`.
  This honors runtime-config overrides but inherits the same caveat as
  `is_system_path` — if a user adds a generic name like `music` to their system
  patterns, the regex will match every path containing that component. This is a
  pre-existing configuration footgun, not new.
- `sqlx-sqlite`'s auxdata caching compiles each `?`-bound regex once per
  statement, so there is no per-row regex compilation cost even with two REGEXP
  arguments.
- `finder.rs` shrinks to just the `FindMatch` struct. The old
  `matches_pattern(&FileRecord, &Regex) -> Option<FindMatch>` helper and its
  tests were removed once the SQL path was the only production caller.
- A CTE refactor was considered (computing `full_path` once in a WITH clause to
  avoid re-evaluating the concat in the system-filter variant) but benchmarked
  as no faster on the production DB. Not adopted.
