# euterpe-tools

Incremental filesystem scanner, audio metadata manager, and media collection
toolkit for a Synology NAS. Designed for 200K–500K file scale on spinning disks
(RAID 6 / SHR-2, Btrfs).

## What it does

- **Scan** directories incrementally (only changed directories are re-read)
- **Index** files into a SQLite database with CSV and tree output
- **Read** audio metadata (MP3, FLAC, OGG, Opus, WAV, M4A, DSF, WMA, MKA)
- **Track** file moves/renames across rescans (preserves metadata)
- **Parse** CUE sheets with MusicBrainz disc ID computation
- **Query** the database: search by tag, list files, compute sizes
- **Manage** anime collections (triage, series sync, episode import)

## Installation

### Requirements

- Rust toolchain (edition 2024)
- Python 3.14+ with [uv](https://docs.astral.sh/uv/)
- [cargo-nextest](https://nexte.st/) for running tests
- `mediainfo` (for WMA/MKA metadata reading)

### Build

```bash
just build            # native release (macOS ARM)
just build-nas        # NAS release (x86_64 musl, static)
```

### Deploy to NAS

Mount the NAS home share in Finder, then:

```bash
just deploy           # runs check + test + build-nas + copy
```

This copies Rust binaries to `~/.local/libexec/etp/` and installs the Python
package via `uv tool install`.

## Commands

### Porcelain (user-facing)

```bash
etp tree <directory>                    # scan + tree output
etp find <pattern> [-R <dir>]           # regex file search
etp catalog [--dry-run]                 # batch scan from KDL config
etp anime triage [pattern]              # anime collection triage
etp anime series [pattern]              # sync from Sonarr directory
etp anime episode <file> --anidb ID     # single episode import
```

### Plumbing (Rust)

```bash
etp-scan <dir> [--db path]              # scan directory → update database
etp-csv <dir> [--output file.csv]       # scan → CSV index
etp-tree <dir> [--du]                   # scan → tree display
etp-find <pattern> [-R <dir>]           # regex search with tree/CSV output
etp-meta scan [-R <dir>] [--force]      # read audio metadata into DB
etp-meta read <file>                    # dump file metadata as JSON
etp-meta cue <file> [--audio-file ...]  # CUE sheet + disc ID display
etp-query --db <path> stats             # collection statistics
etp-query --db <path> find --tag genre --value Jazz
etp-query --db <path> size [directory]  # directory size (replaces du)
etp-cas store <file>                    # BLAKE3 hash + store in CAS
etp-cas gc --db <path>                  # remove unreferenced blobs
```

## Repository layout

```
crates/          Rust libraries
  etp-lib/       core library (scanner, DB, metadata, CAS)
  etp-cue/       CUE sheet parser + MusicBrainz disc ID
cmd/             all commands (Rust + Python)
  etp-csv/       CSV output
  etp-tree/      tree output
  etp-find/      regex file search
  etp-meta/      metadata scan/read/cue
  etp-scan/      standalone directory scanner
  etp-cas/       CAS blob operations
  etp-query/     database queries
  etp/           Python porcelain (dispatcher, anime, catalog)
pylib/           Python shared library
conf/            KDL configuration
docs/            plans, ADRs, design notes
```

## Development

```bash
just check            # clippy + ruff + pyright + ty + prettier
just test             # cargo nextest + pytest
just format           # cargo fmt + ruff format + prettier
just fetch-test-dbs   # copy catalog databases from NAS for smoke testing
```

## License

Private project — not published.
