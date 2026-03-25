# 01. AniDB API Local File Caching

Date: 2026-03-24

## Status

Accepted

## Context

etp-anime fetches anime metadata (titles, episode lists) from AniDB's HTTP API.
AniDB enforces strict rate limits — a minimum of 2 seconds between requests —
and will ban clients that make repeated requests for the same data within a
short window. Bans are applied by IP address and can last hours or days with no
programmatic way to lift them.

The typical etp-anime workflow involves running the tool multiple times for the
same series: once to set up the directory, again when new episodes arrive, and
occasionally to re-check episode names. Each run would re-fetch the same anime
entry if not cached, risking a ban.

## Decision

All AniDB HTTP API responses are cached as local files in
`~/.cache/etp/anidb/{aid}.xml`. Cached responses are considered valid for 24
hours (matching AniDB's own recommendation for client-side caching). The cache
is checked before every API request, and only on a miss or expiry does the tool
make a network request. A `--no-cache` flag allows forcing a fresh fetch when
the user knows the upstream data has changed.

The same pattern is applied to TheTVDB responses
(`~/.cache/etp/tvdb/{series_id}.json`), though TheTVDB's rate limits are less
punitive. Using a consistent caching strategy for both APIs simplifies the code
and reduces unnecessary network traffic.

## Consequences

- The tool can be run repeatedly against the same series without risk of AniDB
  bans, which is the most common usage pattern.
- First-run latency includes a network request; subsequent runs within 24 hours
  are instant.
- If AniDB updates episode data (e.g., adds a newly aired episode's title), the
  change won't be visible until the cache expires or `--no-cache` is used.
- The cache directory grows at one file per anime entry, each a few KB — trivial
  disk usage even for large collections.
- The 2-second inter-request rate limit is also enforced in code as a safety
  net, so even cache-miss scenarios in batch operations comply with AniDB's
  rules.
