# HamaTV-Compatible Special Episode Numbering

- **Status**: Accepted
- **Date**: 2026-04-02

## Context

When bonus files (NCOP, NCED, PV, CM, etc.) cannot be matched to AniDB special
episodes, the manifest builder assigns HamaTV-compatible episode numbers so they
sort correctly in Plex and Infuse. HamaTV (Hama.bundle) uses the ScudLee
anime-lists mapping, which assigns AniDB special types to TVDB S00 episode
number ranges.

The actual ScudLee/anime-lists ranges are:

| AniDB type | TVDB S00 range | Description |
| ---------- | -------------- | ----------- |
| S          | s0e1+          | Specials    |
| C          | s0e101+        | Credits     |
| T          | s0e151+        | Trailers    |
| P          | s0e201+        | Parodies    |
| O          | s0e301+        | Other       |

AniDB does not distinguish NCOP from NCED at the type level — both are type "C"
(Credits), differentiated only by their episode title.

## Decision

Map our bonus types to HamaTV ranges with a +20 offset from each range start.
The buffer avoids collisions with AniDB-tracked specials that may later be added
to the same `Specials/` directory.

NCOP and NCED share a single counter so their episode numbers interleave in
OP/ED pairs (NCOP1, NCED1, NCOP2, NCED2). Files are sorted before processing to
ensure this ordering.

| Bonus type   | Range start | HamaTV base | Notes                   |
| ------------ | ----------- | ----------- | ----------------------- |
| NCOP         | 121         | s0e101+     | Credits, shared counter |
| NCED         | 121         | s0e101+     | Credits, shared counter |
| PV / Preview | 171         | s0e151+     | Trailers                |
| CM           | 221         | s0e201+     | Parodies                |
| Bonus / Menu | 321         | s0e301+     | Other                   |

When using TVDB, ranges are further adjusted to start after the highest existing
TVDB special number (+20) to avoid collisions in the single `Specials/`
directory.

### Evolution

The initial implementation used invented ranges that did not match HamaTV
conventions and gave NCOP and NCED separate counters:

| Bonus type   | Original start | Current start | Change reason                    |
| ------------ | -------------- | ------------- | -------------------------------- |
| NCOP         | 171            | 121           | Aligned to actual C range (101+) |
| NCED         | 191            | 121           | Shared counter with NCOP         |
| PV / Preview | 321            | 171           | Aligned to actual T range (151+) |
| CM           | 521            | 221           | Aligned to actual P range (201+) |
| Bonus / Menu | 521 / 921      | 321           | Aligned to actual O range (301+) |

## Consequences

- Episode numbers for unmatched bonus files are now consistent with the
  ScudLee/anime-lists conventions used by Hama.bundle, Plex, and Infuse.
- NCOP and NCED interleave correctly in OP/ED pairs within a single counter.
- The +20 buffer means manually triaged bonus files will not collide with
  AniDB-mapped specials unless a series has more than 20 credits, trailers, etc.
  tracked in AniDB.
- Existing files triaged under the old ranges will not be automatically
  renumbered — only new triages use the updated ranges.
