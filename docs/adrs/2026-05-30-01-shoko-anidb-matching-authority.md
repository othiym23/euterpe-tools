# Shoko Server as the anime matching and metadata authority

- **Status**: Accepted
- **Date**: 2026-05-30

## Context

The anime collection is migrating its metadata/agent layer away from the HamaTV
(`HamaTV.bundle`) Plex agent and `etp`'s own AniDB/TVDB fetching toward **Shoko
Server** as the single source of truth for matching and metadata, with two
presentation layers over the same files:

- **Shoko Server** fingerprints every file by ED2K hash and matches it against
  AniDB by content (not filename). It exposes a **VFS** — symlink trees it
  manages, organized by Shoko `SeriesID` folders — consumed by media servers.
- **Plex** reads it via **Shoko Relay** (natyusha's agent/scanner, successor to
  Shoko Metadata; AniDB-based; can inject collections through Plex's HTTP API).
- **Jellyfin** reads it via **ShokoFin** (official plugin, also VFS-driven).
- Each plugin can select the displayed title language, so the existing English
  / 「アニメ」 dual view becomes an English-titled and a Japanese-titled library
  over the same VFS.

Constraints that shape every decision here:

- **AniDB has very restrictive rate limits.** Shoko carefully manages them and
  is the only thing that should be talking to AniDB. `etp` making its own AniDB
  requests would risk a ban. The initial import (started 2026-04-20) is gated by
  AniDB's single-slot UDP file pipeline at ~376 lookups/day; as of 2026-05-31 it
  is ~65% identified (~10,200 unrecognized of ~29,000 files) and projected to
  finish ~2026-06-30. See the runbook for the mechanics.
- **Infrastructure:** Shoko Server + Jellyfin run as a separate docker-compose
  stack (`kagee`) at `/volume1/docker/kagee` on the NAS. It currently
  bind-mounts only `/volume1/video/anime` → `/anime`; **`/television` and
  `/movies` mounts are a prerequisite** the operator will add before the
  non-AniDB fallback work. Plex (and `etp`) see the host paths
  `/volume1/video/...`.
- We keep our **own naming and filesystem organization**; Shoko is the matcher
  underneath, not the namer of the real files.

## Decision

1. **Shoko/AniDB is the matching + metadata authority for anime.** `etp` stops
   calling AniDB directly; when it needs hash-based episode/special
   identification it queries the **local Shoko Server REST API**, never AniDB.
   The current filename-heuristic parser and manifest naming remain for
   placement and for content Shoko does not cover, but are no longer the
   authority for AniDB-matched anime.

2. **Deprecate TVDB for anime; add TMDB for non-anime.** TVDB anime matching in
   `etp` is retired. TVDB and TMDB support is (re)introduced strictly for
   television and movie content that AniDB/Shoko does not cover.

3. **Non-AniDB fallback.** Files Shoko reports as unrecognized (e.g. Robotech, G
   Saviour) are flagged and **COW (Btrfs reflink) copied** into
   `video/television` or `video/movies` — siblings of `video/anime`, all on the
   one Btrfs volume — using standard media-server layouts so each server's own
   TVDB/TMDB agent can match them.

4. **Hash-based identification comes from Shoko, not reimplemented.** `etp` will
   not recompute ED2K hashes or hit AniDB's file API; it reads Shoko's match
   results (including AniDB special classifications: S/C/T/P/O) via the local
   API. The integration model (place-then-let-Shoko-import vs. fingerprint
   first) is deferred to the plan.

5. **Collections via a single AniDB-ID-keyed SSOT.** Manual, concept-ordering
   collections (e.g. PreCure by generation incl. All-Stars/movies; Gundam by
   storyline — UC by epoch, AUs by setting) are maintained in one source of
   truth keyed by AniDB ID (and TVDB/TMDB for the non-anime exiles). The current
   legacy Plex libraries — "Anime" (en) and "アニメ" (ja), both running the same
   HamaTV-derived AniDB agent (Kamehameha is a renamed Hama copy) — are ~97%
   AniDB-keyed, so the SSOT extracts cleanly
   (`scripts/plex_collections_export.py` →
   `collections/anime-collections.yaml`). A near-term stopgap reconciles those
   two legacy libraries to be consistent (ja-canonical names, since アニメ is
   the primary library) while they remain on legacy agents. The same SSOT later
   compiles (via an `etp` subcommand) into Kometa (Plex) and jellyfin-collection
   / direct Jellyfin API (Jellyfin) for the new Shoko-backed libraries, with
   identical membership across the EN/JA views. Compiled output is
   **non-destructive** — it must not clobber Shoko Relay's own dynamic
   collections.

6. **Hard sequencing constraint.** Anything that touches Shoko or AniDB waits
   until the initial import completes (~1 month) and the post-import cleanup
   settles. Non-Shoko work — the collections SSOT (read from the _current_ Plex
   libraries), TMDB support, and the COW-to-television/movies mechanism — may
   proceed now.

7. **Path translation.** `etp`'s future Shoko API client must map host paths
   (`/volume1/video/...`) to/from the container paths Shoko reports (`/anime`,
   `/television`, `/movies`).

## Consequences

- No AniDB ban risk originates from `etp`; AniDB access is funneled through
  Shoko's rate-limited queue.
- `etp`'s heuristic filename parser and HamaTV special-range numbering become
  secondary for AniDB-matched anime, but are retained for file placement and for
  the non-anime cases.
- New integrations/dependencies enter the picture: the Shoko REST API,
  `python-plexapi`, Kometa, `jellyfin-collection` (and/or the Jellyfin API), and
  a TMDB client.
- Collections feasibility is asymmetric: **Kometa has native `anidb_id` /
  `anidb_relation` builders**, so Plex is well-served directly from AniDB IDs;
  **jellyfin-collection has no AniDB builder**, so the Jellyfin side will likely
  resolve AniDB ID → Jellyfin item (via ShokoFin provider IDs) and create
  collections through the Jellyfin API.
- The migration is phased around the ~1-month import. `scripts/shoko.py`
  (subcommands: `eta`/`progress`/`throughput`/`durations`/`queue`) monitors the
  import from the local API + logs without touching AniDB; see the runbook.

## Operational reference

How Shoko's queue/rate-limit/ban model works, the informational endpoints, and
procedures (monitoring, unsticking a job, safe restarts), plus Shoko Relay and
ShokoFin notes, are in [docs/shoko-runbook.md](../shoko-runbook.md).

## Related decisions

This decision affects, and will eventually supersede in part, several earlier
ADRs once the Shoko-dependent work lands:

- [2026-04-02-04-hamatv-special-episode-ranges](2026-04-02-04-hamatv-special-episode-ranges.md)
  — AniDB special classification (S/C/T/P/O) now comes from Shoko.
- [2026-04-02-03-romaji-title-matching](2026-04-02-03-romaji-title-matching.md)
  — title/romaji matching against TVDB for anime is deprecated.
- [2026-03-24-01-anidb-api-local-caching](2026-03-24-01-anidb-api-local-caching.md)
  — `etp` no longer calls AniDB directly; Shoko owns AniDB access.
- [2026-03-26-01-anime-subcommand-split](2026-03-26-01-anime-subcommand-split.md)
  — the `etp anime` surface gains collections/non-AniDB-fallback subcommands.

See the migration plan:
[docs/plans/2026-05-30-shoko-migration.md](../plans/2026-05-30-shoko-migration.md).
