# ADR: Domain Partitioning of the Shared Downloads Directory

**Date:** 2026-06-10 **Status:** Accepted

## Context

`etp anime`, `etp movies`, and `etp television` all scan the same downloads
directory, but each command owns a different domain (the anime, movies, and
television managed trees). During a long AniDB-bound Shoko import, anime cannot
be ingested, and the backlog piles up in the downloads directory — flooding
`etp television ingest plan` with hundreds of anime titles that are slow to
resolve (TVDB searches and mediainfo runs) and tedious to skip by hand.

Heuristic classification (CJK titles, absolute numbering) would violate the
explicit-over-convenient principle. Two authoritative signals exist instead:
Sonarr records the root folder of every series it manages (`anime` vs
`television`), and the anime managed tree's top-level folders are exactly the
anime-domain titles Sonarr has created.

## Decision

Downloads-mode planning excludes titles another domain owns, using:

1. **Sonarr/Radarr root folders (primary).** The arr index already fetched for
   ID resolution now carries each record's root-folder basename and alternate
   titles (which cover romaji fansub names). A downloads title matching a record
   rooted outside the command's own source tree is foreign-domain and dropped
   before any provider search or mediainfo run. Alternate-title matching uses
   dedicated loose index keys (a `~` prefix) that ID resolution never consults,
   so year-less matches cannot misresolve remakes.
2. **Anime-tree folder names (fallback and supplement).** The top-level
   directory names of the anime managed tree (location read from
   anime-ingestion.kdl, defaulting to the standard layout) are matched against
   downloads titles, with and without trailing years.

Exclusions are counted in plan output
(`N downloads title(s) excluded as foreign-domain`), never silent. Managed-tree
titles are never filtered — the command's own tree is in-domain by definition.
Neither mechanism contacts AniDB, so partitioning is safe to run during
AniDB-rate-limited operations.

The symmetric exclusion for `etp anime` (dropping Sonarr-television series from
anime triage) is deliberately deferred until anime ingestion is runnable again.

## Consequences

- Television and movie plans over a large anime backlog are both clean (no
  foreign blocks to skip) and fast (no wasted provider searches or mediainfo
  runs on foreign titles).
- A series Sonarr does not manage and the anime tree does not name is not
  excluded; it lands as `needs-id` noise at worst, as before.
- Anime titles known only by an alias absent from both Sonarr's alternate titles
  and the tree names can still leak through; the resolution ladder contains them
  as `needs-id`.

## Related

- [2026-06-09-03-shared-ingest-register.md](2026-06-09-03-shared-ingest-register.md)
  — the register prevents double-processing across domains; this ADR prevents
  cross-domain _planning_.
- [2026-06-09-04-tmdb-tvdb-dual-provider.md](2026-06-09-04-tmdb-tvdb-dual-provider.md)
  — the arr index this decision extends.
