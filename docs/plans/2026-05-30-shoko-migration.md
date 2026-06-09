# Plan: migrate `etp anime` to Shoko/AniDB + dual Plex/Jellyfin collections

Companion to ADR
[2026-05-30-01-shoko-anidb-matching-authority](../adrs/2026-05-30-01-shoko-anidb-matching-authority.md).
This captures the migration from HamaTV + `etp`'s own AniDB/TVDB fetching to
Shoko Server as the matching/metadata authority, with Plex (Shoko Relay) and
Jellyfin (ShokoFin) on top, and a single source of truth for collections.

## Hard constraint: the import gate

The initial Shoko import (~54,800 queued tasks, ~10,603 unrecognized files at
time of writing) is expected to take ~1 month, and AniDB's rate limits mean
**nothing in `etp` may talk to Shoko or AniDB until it finishes** and the
post-import cleanup settles. Work splits cleanly into "can start now" (reads the
current Plex, the filesystem, or TMDB) and "import-gated" (reads Shoko).

## Phase 0 — Monitor the import (now)

- [x] `scripts/shoko.py` — monitors the import from the **local** Shoko API +
      rotated logs, never AniDB (subcommands: eta, progress, throughput,
      durations, queue). ETA = live `UnrecognizedFiles` ÷ log recognition rate;
      `eta --watch` warns on a real AniDB ban. See
      [docs/shoko-runbook.md](../shoko-runbook.md).
- Note `BlockedCount` (jobs blocked on AniDB rate limit) — it gates the long
  tail and is why the ETA is dominated by AniDB throughput, not local CPU.
  (Blocked ≠ banned; check `AniDB/BanStatus`.)

## Phase 1 — Collections SSOT + reconciliation (now; no Shoko)

Goal: one English-named source of truth for the manual, concept-ordering
collections, reconciled across the two current hand-built libraries.

- Read the current Plex **Anime** (EN) and **アニメ** (JA) libraries via
  `python-plexapi`: enumerate manual collections and their members, and capture
  each member's GUIDs (Hama exposes AniDB + TVDB + TMDB).
- **Reconcile drift**: the two libraries' collections are meant to be identical
  but were hand-maintained; diff their memberships and surface the deltas for
  resolution.
- Emit the SSOT (YAML): each collection has an English name, optional
  poster/sort metadata, and a membership list keyed by **AniDB ID** (TVDB/TMDB
  for the non-anime exiles). Keep mappings separate from the show files (no
  reliance on per-show tags unless a target requires it).
- Open question: SSOT schema details (sort titles, poster sourcing, ordering
  within a collection, nesting like "UC → epochs").

## Phase 2 — SSOT → Plex/Kometa compiler (now; no Shoko)

- New `etp anime collections` subcommand that compiles the SSOT into **Kometa**
  collection files. Kometa has native `anidb_id` / `anidb_relation` builders, so
  anime memberships compile directly from AniDB IDs; the non-anime exiles use
  `tmdb_show`/`tvdb_show`/`imdb` builders.
- Configure Kometa to run **non-destructively** so it never removes Shoko
  Relay's dynamically generated collections (namespace ours; don't enable
  unmanaged-collection deletion).
- Produces output for both Plex libraries (EN/JA) with identical membership and
  English collection names.

## Phase 3 — TMDB support + COW-to-television/movies (now; no Shoko for naming)

- Add a TMDB client to `etp` (alongside retained TVDB) for non-anime content.
- `etp` command to COW-copy (Btrfs reflink) audited misfits from `video/anime`
  into `video/television` / `video/movies` using standard media-server layouts
  (`Show (Year)/Season NN/Show - SxxEyy.ext`, `Title (Year)/Title (Year).ext`).
- Open question: how much metadata `etp` builds vs. defers to each server's
  agent (likely: `etp` constructs the path from a TMDB/TVDB ID you supply; the
  server's agent does the rest).

## Phase 4 — SSOT → Jellyfin collections (after Jellyfin/ShokoFin is up)

- jellyfin-collection (JFC) is Kometa-YAML-compatible **but has no AniDB
  builder**. Plan: resolve each AniDB ID → its Jellyfin item (via ShokoFin's
  provider IDs) and create the collection through the **Jellyfin API** directly,
  or via JFC if AniDB membership becomes expressible.
- Same English collection names and identical membership as the Plex side.

## Phase 5 — Shoko-backed ingest (import-gated)

- `etp`'s Shoko API client (with host↔container path translation
  `/volume1/video/... ↔ /anime|/television|/movies`).
- Use Shoko's match results for episode/special identification (AniDB S/C/T/P/O)
  in place of the HamaTV-range heuristic, for `etp`'s own naming.
- **Decide the integration model** (deferred): (a) `etp` places files and lets
  Shoko (watching the folder) import and match, then `etp` reads the match back
  and relocates, telling Shoko about the move; vs. (b) `etp` self-fingerprints
  before Shoko import. (a) leans on Shoko's AniDB handling and is preferred;
  cost is the move-notification dance + path translation.

## Phase 6 — Non-AniDB detection via Shoko (import-gated)

- Confirm "not in AniDB" by querying Shoko (unrecognized files), replacing any
  manual flagging, and feed Phase 3's COW mechanism automatically.

## Phase 7 — One-off Plex→Shoko ID map (import-gated)

- Map current Plex GUIDs (AniDB/TVDB/TMDB) to Shoko VFS entries to (re)create
  the groups per library. The stable bridge is the **AniDB ID** harvested in
  Phase 1; Shoko indexes by AniDB natively, so the mapping is mostly a join.

## Open decisions to settle

1. Phase 5 integration model (place-then-import vs. self-fingerprint).
2. Jellyfin collection mechanism (direct API vs. JFC).
3. television/movies naming depth (path-only vs. embedded metadata).
4. SSOT schema specifics (nesting, sort/poster, per-collection ordering).
