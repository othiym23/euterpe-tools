# Anime Triage Manifest Format

- **Status**: Accepted
- **Date**: 2026-04-19

## Context

`etp anime ingest` (and the deprecated `triage` / `series` aliases) builds an
editable plan before copying files. The plan is a KDL document opened in the
user's `$EDITOR`, who can rename destinations, comment out entries to skip, or
restructure the layout. The manifest file is then re-parsed and executed.

The format needs to:

- Express a directory layout (Season NN, Specials, series root for movies,
  Extras subdir, in-place renames) without committing to those paths in
  English-only conventions.
- Show the user enough source-side context (downloaded path, Sonarr-side path)
  to verify a match before committing — but make `dest` the only field that
  actually drives copy behavior.
- Survive round-trips through an editor without semantic loss: comments,
  warnings, and `(todo)` markers must be preserved on re-parse and rendered
  consistently on re-write.
- Refuse to execute on unresolved or malformed entries rather than silently
  skipping or guessing.
- Live in the codebase as a single grammar interpreted by both writer
  (`pylib/etp_lib/manifest.py:write_manifest`) and parser (`parse_manifest` in
  the same file), with neither being authoritative ahead of the other — the
  format is the contract.

## Decision

The manifest is a KDL document with five top-level node types:

| Node       | Destination              | Purpose                                          |
| ---------- | ------------------------ | ------------------------------------------------ |
| `season N` | `<series>/Season NN/`    | Regular numbered episodes for season `N`         |
| `specials` | `<series>/Specials/`     | Season 0, OP/ED, NCs, PV, CM, Bonus, Menu        |
| `movie`    | `<series>/`              | Movie files copied to the series root            |
| `extras`   | `<series>/Extras/`       | Non-video extras (CDs, scans, OST, etc.)         |
| `renames`  | (in place, source's dir) | Rename pre-existing files for naming consistency |

Each top-level node contains entries describing one source file each. The shape
of an entry differs slightly between the episode-style nodes (`season`,
`specials`, `movie`) and the file-style nodes (`extras`, `renames`).

### Episode entries (`season`, `specials`, `movie`)

```
season N {
  episode M {
    source "/abs/path/to/source.mkv"
    dest   "Filename.mkv"
  }
}
```

- `M` is the destination episode number (an integer; multi-episode files may use
  any integer — the destination filename carries the actual range via
  `s1e02-e03` etc.).
- `source "..."` is the absolute path to the file to be copied. Triage mode
  emits `source`; Sonarr-sync mode emits two children — `downloaded` for the
  original download and `sonarr` for the Sonarr-side path:

  ```
  episode 5 {
    downloaded "/downloads/Show.S01E05.1080p.mkv"
    sonarr     "/data/anime/Show/Season 01/Show - s1e05 - Title.mkv"
    dest       "Show - s1e05 - Title.mkv"
  }
  ```

  The parser accepts either `source` or `sonarr` — both are read as the path to
  copy from. `downloaded` is reference-only context for the user while editing.

- `dest "..."` is the destination _filename only_ (no directory). The
  destination directory is computed from the enclosing node: `season N` →
  `<series>/Season NN/`, `specials` → `<series>/Specials/`, `movie` →
  `<series>/`.

### Extras entries

```
extras {
  file "/source/path/CD/track01.flac" {
    dest "OST/Disc 1/track01.flac"
  }
}
```

- `file "<path>"` is the source path as the node argument (not a child).
- `dest "..."` may include subdirectory components, which are preserved under
  `<series>/Extras/`. The writer auto-detects an existing `Extras/` parent in
  the source path and reproduces its substructure.

### Rename entries

```
renames {
  file {
    source "/dest/series/Season 01/Old Name - s1e01.mkv"
    dest   "New Name - s1e01.mkv"
  }
}
```

- Used to rename pre-existing files in the destination so that all episodes
  share a single concise name. Operates in place — the file is renamed within
  its current parent directory.

### Tags and comments

- `(todo)` prefix on an `episode` node marks the entry as unresolved. Execution
  refuses to run while any `(todo)` entries remain — the user must edit them or
  `/-` comment them out:

  ```
  (todo)episode 0 {
    source "/some/Making Of.mkv"
    dest "s1eXX - EPISODE_NAME.mkv"
  }
  ```

- KDL slashdash (`/-`) on any node skips it cleanly during parse:

  ```
  /- episode 7 {
       source "/some/skip-me.mkv"
       dest "..."
     }
  ```

- Inline comments emitted by the writer signal advisory conditions to the user.
  They are stripped on re-parse and have no effect on execution:
  - `// CRC32 MISMATCH — hash stripped from destination`
  - `// EXISTS — a file already exists at this destination`
  - `// WARNING: filename is N bytes (max 255) — shorten before saving`

### Worked example

A multi-season download grouping containing a regular cour, a movie, an OP
video, and a soundtrack disc renders as:

```
// etp-anime triage manifest
// Series: 葬送のフリーレン [Frieren - Beyond Journey's End] (2023)
// AniDB: 17617
// Series dir: /volume1/video/anime/葬送のフリーレン [Frieren] (2023)
//
// Edit destination filenames. Delete or /- comment out entries to skip.
// Source/sonarr filenames are for reference only — only dest is used.

movie {
  episode 1 {
    source "/downloads/Frieren.Movie.2023.BD.1080p.mkv"
    dest "Frieren - The First Cour Compilation (2023) [BD,1080p,h264].mkv"
  }
}

season 1 {
  episode 1 {
    source "/downloads/[Sub] Frieren - 01 [BD][1080p].mkv"
    dest "Frieren - s1e01 - The Journey's End [BD,1080p,h264].mkv"
  }
  episode 2 {
    source "/downloads/[Sub] Frieren - 02 [BD][1080p].mkv"
    dest "Frieren - s1e02 - It Didn't Have to Be Magic [BD,1080p,h264].mkv"
  }
}

specials {
  episode 121 {
    source "/downloads/[Sub] Frieren - NCOP01 [BD][1080p].mkv"
    dest "Frieren - s0e121 - NCOP01 [BD,1080p,h264].mkv"
  }
}

extras {
  file "/downloads/Frieren BD/Extras/OST/Disc 1/track01.flac" {
    dest "OST/Disc 1/track01.flac"
  }
}
```

## Consequences

- The format is intentionally text-first and editor-friendly: KDL's forgiving
  syntax (slashdash, raw strings, multiline node bodies) makes manual edits
  comfortable.
- Adding a destination layout requires changes in three places: the writer
  (group dispatch + emission), the parser (node-name → subdir computation), and
  a test pinning a sample document. The four extant layouts (`season`,
  `specials`, `movie`, `extras`) plus `renames` cover the union of layouts seen
  in the wild for anime collections.
- `dest` being filename-only means destination directory naming is owned by the
  manifest grammar, not by the user. Users wanting a different on-disk layout
  (e.g., `S01/` instead of `Season 01/`) must change `season_subdir()` in
  `pylib/etp_lib/naming.py`, not edit the manifest.
- The `(todo)` parse-level reject keeps unattended automation safe — a manifest
  cannot execute past an entry that still requires human input.
- The writer adds inline comments (`CRC32 MISMATCH`, `EXISTS`, filename length
  warnings) but the parser ignores them. Round-tripping a manifest through an
  editor strips these comments — that is intentional, since the underlying
  conditions may have been resolved by user edits.
- The presence of both `source` and `sonarr` as accepted source-path child names
  is a wart introduced by the original triage/series split. Now that `ingest` is
  the unified entry point, the writer still emits whichever name reflects how
  the file was discovered, but downstream the two are equivalent. Future cleanup
  could collapse to a single name.
- The format is loaded via the `kdl` Python package (`pyproject.toml`), which
  presents `parse` and `ParseError` from internal modules — pyright flags this
  as a private-import warning that is suppressed in `manifest.py`.
