# ADR: Plex-Style Provider ID Tags in Library Directory Names

**Date:** 2026-06-09 **Status:** Accepted

## Context

Plex and Jellyfin both match library items more reliably when a metadata
provider ID is embedded in the directory name, but they document different
syntaxes:

- **Plex:** curly braces — `Title (Year) {tmdb-949}`, `{tvdb-371980}`,
  `{imdb-tt0113277}`; bracketed `[...]` text is ignored during matching.
- **Jellyfin:** square brackets — `Title (Year) [tmdbid-949]`,
  `[tvdbid-371980]`, `[imdbid-tt0113277]`. Support for Plex-style braces was
  merged into Jellyfin master in February 2026 (jellyfin/jellyfin#14927).

The existing library uses square brackets for alternate-language titles
(`血は渇いてる [Blood Is Dry] (1960)`), so Jellyfin-style bracket IDs would
visually collide with that convention.

## Decision

Directory names created by `etp movies ingest` and `etp television ingest` embed
exactly one provider ID, in Plex brace syntax, from the kind's primary provider:

- Movies: `Title (Year) {tmdb-NNN}`
- Television: `Title (Year) {tvdb-NNN}`

Titles lead with the original language, following the library's existing
`Original [English]` convention (the movie/TV counterpart of the anime `JA [EN]`
format): when the original-language title genuinely differs from the English
title, the directory name is `Original [English] (Year) {tmdb-NNN}` — e.g.
`올드보이 [Oldboy] (2003) {tmdb-670}`. Pairs differing only in case or
punctuation don't earn brackets. The original title comes from TMDB's
`original_title`/`original_name` (movies, and TV via the cross-check) or
TheTVDB's Japanese translation (TV). Episode _filenames_ keep the concise
English title, matching the anime convention where the directory carries the
dual name and files stay short.

Editions use Plex's documented marker in both the folder and file name:
`Title (Year) {tmdb-NNN} {edition-Final Cut}`. The cross-check provider's ID
(and IMDb ID when known) is recorded in the plan manifest only, never in
directory names — Plex's documentation shows a single brace ID per name, and
multiple-ID behavior is undocumented.

Existing library directories are never renamed: when a `Title (Year)` directory
already exists for a title (with or without tags, with or without a bracketed
alternate title), the planner reuses it instead of creating a parallel tagged
directory.

Destination filenames keep the etp bracketed quality block
(`[Group Source,1080p,HEVC,...]`) — both servers ignore bracketed text — and
movies drop the `- complete movie` marker: that convention belongs to Plex's
absolute-series-scanner/HamaTV for movies inside _TV_ libraries, while a proper
movie library wants the file named exactly after its folder.

## Consequences

- Plex matches new items by ID immediately. Jellyfin matches by ID once the NAS
  instance includes the brace-support change; older versions fall back to
  title+year matching, which the embedded year keeps reliable.
- The library remains visually consistent: brackets mean alternate titles or
  quality blocks, braces mean provider tags.
- If a wrong ID is ever embedded, correcting it means renaming one directory and
  refreshing metadata — the manifest records both providers' IDs to aid
  diagnosis.
