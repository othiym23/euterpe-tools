# ADR: TMDB/TheTVDB Dual-Provider Metadata with Per-Type Primaries

**Date:** 2026-06-09 **Status:** Accepted

## Context

The anime pipeline resolves against AniDB and TheTVDB. Movies and western
television need different sources: Plex's movie agent is TMDB-backed, its TV
agent uses TVDB/TMDB data, and Jellyfin defaults to TMDB with a TVDB plugin for
shows. Relying on a single provider risks silent mismatches (remakes, same-title
films from different years); relying on interactive disambiguation is
unavailable in the non-interactive plan/apply flow.

## Decision

Both commands use **both** TheTVDB and TMDB, with a fixed primary per media
kind:

- **Television:** TheTVDB is primary — it supplies episode numbering and titles
  (reusing the existing `tvdb.py` v4 client) and its ID goes in the directory
  name. TMDB is the cross-check: the planner searches TMDB for the resolved
  title and fetches its external IDs; TMDB pointing back at the same TheTVDB ID
  confirms the match (`cross-check "ok"`), anything else is recorded as
  `mismatch` (a warning, never fatal) or `unavailable`.
- **Movies:** TMDB is primary — full metadata including alternative titles and
  the IMDb ID via the new `tmdb.py` v3 client — and its ID goes in the directory
  name. TheTVDB's movie search is the cross-check, keyed on exact title+year.

AniDB is not used by these commands; anime stays with `etp anime`.

Resolution is conservative: a config mapping or manifest-supplied ID is
authoritative (`confidence "exact"`); otherwise a search hit must match title
and year exactly to auto-resolve. A single non-exact hit is `confidence "high"`
(still planned, flagged for review); multiple or zero plausible hits become
`needs-id` blocks carrying the candidates, resolved via `media-ingestion.kdl`
mappings or the `--refine` loop.

Credentials live in `media.env` (`TMDB_API_KEY`, `TVDB_API_KEY`; `anime.env` is
read as a fallback so the existing TheTVDB key keeps working). The TMDB client
accepts both v3 API keys and v4 read-access tokens. Responses are cached for 24
hours alongside the AniDB/TheTVDB caches.

## Consequences

- The secondary provider's ID is recorded in the manifest, giving every ingested
  title a two-provider paper trail without widening directory names (see
  [2026-06-09-02-plex-provider-id-directory-tags.md](2026-06-09-02-plex-provider-id-directory-tags.md)).
- A cross-check mismatch surfaces in plan output and the JSON `warnings` array
  but does not block ingestion — the curator decides.
- Provider outages degrade gracefully: failed searches make titles `needs-id`;
  failed cross-checks record `unavailable`.

## Related

- [2026-03-24-01-anidb-api-local-caching.md](2026-03-24-01-anidb-api-local-caching.md)
  — the caching pattern these clients follow.
