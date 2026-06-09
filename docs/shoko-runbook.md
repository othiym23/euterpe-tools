# Shoko Server runbook

Everything we know about operating the Shoko Server anime-library import on the
`kagee` stack: how Shoko works, its informational endpoints, day-to-day
procedures, and the Plex/Jellyfin integrations. Companion to ADR
[2026-05-30-01](adrs/2026-05-30-01-shoko-anidb-matching-authority.md) and the
migration plan [2026-05-30](plans/2026-05-30-shoko-migration.md). Monitor with
`scripts/shoko.py`.

> **Snapshot (2026-05-31)** — illustrative, point-in-time: ~29,000 files
> (~18,800 recognized / ~10,200 unrecognized, ~65% identified), 1,212 series;
> import started 2026-04-20; ~376 AniDB file-lookups/day; projected finish
> ~2026-06-30. Re-run `shoko.py` for current numbers.

## Infrastructure

- Stack `kagee` (Shoko + Jellyfin), docker-compose at
  **`/volume1/docker/kagee`** on the NAS (host **euterpe**). The Mac sees the
  same SMB share at `/Volumes/docker/kagee` — `/volume1/...` is NAS-side (use it
  in docker/compose commands), `/Volumes/...` is the Mac mount.
- Shoko API: **`http://euterpe.local:8111`** (auth via `apikey` header).
- Rotated logs: `/Volumes/docker/kagee/shoko/config/Shoko.CLI/logs`
  (`YYYY-MM-DD.log` + dated `.zip`, one inner `.log` per day).
- Bind-mounts only `/volume1/video/anime` → `/anime` today (television/movies
  not yet mounted). Shoko 5.3.3.

## How the import works

Two overlapping phases, then continuous enrichment:

1. **Hashing / discovery** (`DiscoverFileJob`, `HashFileJob`, `ProcessFileJob`)
   — local, fast. ED2K-hashes every file; each hash _enqueues_ a rate-limited
   AniDB lookup, so the queue **grows** during this phase. Finished ~2026-05-12
   here; no new files are added to the unidentified pool afterward.
2. **AniDB identification** (`GetAniDBFileJob` → "Get Cross-References for
   File") — the bottleneck. Matches each hash to an AniDB episode through a
   **single-concurrency, rate-limited UDP pipeline**, ~376 files/day.
3. **Enrichment** — anime metadata (`GetAniDBAnimeJob`, AniDB HTTP API), TMDB
   cross-refs + artwork, AniDB CDN art, stats. Real parallelism (caps 8–12) but
   **input-starved** behind identification.

### Why it's slow: AniDB limits (the binding constraint)

- **UDP API** (file lookup, creators, release groups, MyList): short-term ≤ 1
  packet / 2 s; **long-term ≤ 1 packet / 4 s sustained**; plus an anti-leech
  "karma" ban for prolonged bulk fetching with **no fixed duration**.
- **HTTP API** (anime XML): ≤ 1 request / 2 s; re-fetching the same data bans
  you.
- Bans are **per-IP**, decay ~24 h. Shoko's limiter does 2 s/packet for the
  first hour, then 4 s sustained. One UDP channel carries _all_ AniDB UDP work
  and Shoko keeps a margin under the limit, so the realized pace is ~1 file /
  1–3.5 min. **That ~376/day is the ceiling; more worker threads do not help.**
- A pre-seeded AniDB metadata cache did **not** help here: anime fetches run
  with `ForceRefresh`, which bypasses the cache (~85% were re-fetched over
  HTTP).

### Blocked ≠ banned; idle workers are normal

Almost the whole queue shows as **blocked** (e.g. 54,900 of 54,920) — that is
Shoko holding AniDB/file jobs behind the single concurrency slot, **not** a ban.
Of 10 worker threads ~7–8 sit **idle** because nothing is acquirable, not for
lack of CPU. A real ban is a distinct condition (`AniDB/BanStatus`); we hit one
2026-04-21→23 (throughput → 0, then a 04-24 catch-up spike when it lifted).

**`StartTime` is the enqueue time, not the execution start — and it's UTC.**
Every item in `CurrentlyExecuting`/`Queue/Items` carries the time the job was
_scheduled_ (during the April discovery sweep, for most of the backlog), in UTC
(`…Z`); the logs and `BanStatus.LastUpdatedAt` are local (`-07:00`). So a job
"running since May 4" almost always means **enqueued May 4, still blocked** — it
will execute in a fraction of a second when it reaches the front of the
serialized AniDB lane. Verified: VideoLocalID 21476 reported
`StartTime 2026-05-04T00:38:19Z` yet its log shows it ran
`2026-05-31 12:45:36.429 → .667` (~240 ms). **Do not infer a wedged job from an
old `StartTime`** — old stamps are the norm for the entire blocked backlog.

## Task categories

| Group         | Key jobs                                                                                                        | Rate-limited?            |
| ------------- | --------------------------------------------------------------------------------------------------------------- | ------------------------ |
| File          | `DiscoverFileJob`, `HashFileJob`, `ProcessFileJob`, `MediaInfoJob`                                              | No (local); done ~05-12  |
| AniDB UDP     | `GetAniDBFileJob` (identification), `GetAniDBCreatorJob`, `GetAniDBReleaseGroupJob`, `AddFileToMyListJob`       | **Yes — the bottleneck** |
| AniDB HTTP    | `GetAniDBAnimeJob`                                                                                              | Yes, separate HTTP limit |
| AniDB CDN     | `DownloadAniDBImageJob`                                                                                         | No (CDN), cap 8          |
| TMDB          | `SearchTmdbJob`, `UpdateTmdbShowJob`, `UpdateTmdbMovieJob`, `DownloadTmdbImageJob`, `DownloadTmdbShowImagesJob` | HTTP, caps 8–12          |
| Trakt / Stats | `CheckTraktTokenJob`; `RefreshAnimeStatsJob`                                                                    | No                       |

Execution time ≠ throughput: a `GetAniDBFileJob` runs ~370 ms (median); the
daily ceiling is the throttle _between_ jobs. Per-type mean/median/p90 from the
logs via `shoko.py durations` (only jobs that log a start+completion line are
measurable — file lookups, image downloads; single-line jobs aren't).

## Informational endpoints (read-only, ban-safe)

All are local; none touch AniDB. Auth: header `apikey: <key>` (or `?apikey=`),
or `POST /api/auth {user,pass,device}` → `{apikey}`.

| Endpoint                                                     | Returns / use                                                                                                                   |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------- |
| `GET /api/v3/Queue`                                          | `{TotalCount, WaitingCount, BlockedCount, ThreadCount, CurrentlyExecuting[]}` (PascalCase).                                     |
| `GET /api/v3/Dashboard/Stats`                                | `{FileCount (recognized), UnrecognizedFiles, SeriesCount, …}` — the real progress signal.                                       |
| `GET /api/v3/AniDB/BanStatus`                                | `{UDP:{IsBanned,BanDuration,LastUpdatedAt}, HTTP:{…}}` — authoritative ban state.                                               |
| `GET /api/v3/Queue/DebugStats`                               | `{Queue, TypesToLimit (per-type concurrency caps), TypesToExclude (single-slot AniDB/file group), AvailableConcurrencyGroups}`. |
| `GET /api/v3/Queue/Items?showAll=true&page=N&pageSize=≤1000` | Paged queue items `{Type,Title,IsRunning,IsBlocked,Key,Details}`. `Key` carries the `VideoLocalID`.                             |
| `GET /api/v3/Init/Status`                                    | Server state / uptime.                                                                                                          |

**Gotcha:** `GET /api/v3/Queue/Types` returns _friendly_ job names and **omits
blocked jobs**, so it cannot see the AniDB backlog (it summed ~3k of ~54k in
testing). Use `Dashboard/Stats UnrecognizedFiles` for "remaining," never
`Queue/Types`.

## Procedures

### Monitor

```bash
export SHOKO_URL=http://euterpe.local:8111 SHOKO_APIKEY=<key>
export SHOKO_LOGS_DIR=/Volumes/docker/kagee/shoko/config/Shoko.CLI/logs
./scripts/shoko.py eta --watch      # ETA + live AniDB ban warning
./scripts/shoko.py progress         # per-day identification + ETA (logs)
./scripts/shoko.py throughput       # per-day task counts by type/group
./scripts/shoko.py durations        # per-task execution time
./scripts/shoko.py queue            # live queue breakdown + concurrency + verdict
```

ETA = live `UnrecognizedFiles` ÷ recognition rate (median "Found N episodes"/day
from the logs — the recognition rate, not the lookup rate, is the correct
divisor). `--watch` warns on a real ban and stops at completion/plateau.

### Resubmit one file

```bash
curl -X POST -H "apikey: $SHOKO_APIKEY" \
  "$SHOKO_URL/api/v3/File/<VideoLocalID>/Rescan?priority=true"
```

Re-queues identification for one file
(`ScheduleFindReleaseForVideo(force=true)`), queue-jumping with `priority=true`.
The `VideoLocalID` is in the queue item's `Key`. There is **no per-item
cancel/restart** — Queue only has whole-queue `Pause`/`Resume`/`Clear` (don't
`Clear`; it drops the pending backlog).

### Unstick a wedged "running" job

First, **confirm it's actually wedged** — an old `StartTime` does _not_ prove it
(see "Blocked ≠ banned" above; old stamps are normal). A wedged job and a merely
blocked one look identical in `/Queue`. A real wedge holds its concurrency-group
slot and **never completes**, stalling every other job in that group behind it.
Identify it by _behavior_, not by timestamp: poll `/Queue` a few times and watch
for a job that stays in `CurrentlyExecuting` **across polls** while no new
completions for its group appear in the log (recognition throughput → 0) **and**
`AniDB/BanStatus` is clear. If the job churns (different `Key` each poll) or the
log keeps logging "Found N episodes", nothing is wedged — it's just the
rate-limited backlog draining normally.

Once confirmed: the stuck job won't clear via the API, and a `Rescan` may be
deduped by Quartz (job already exists). The reliable fix is a **Shoko restart**,
which frees the held slot and lets the backlog queued behind it flow:

```bash
docker compose -f /volume1/docker/kagee/docker-compose.yml restart shoko-server
```

Safe mid-import: the Quartz queue is **DB-persisted**, so all pending jobs
survive; AniDB re-auths and the rate limiter resets benignly (verify `BanStatus`
is clear first — a restart won't cause a ban). A wedged job holds one worker but
does **not** slow a rate-limited import (threads aren't the constraint), so it's
low-urgency.

## Shoko Relay (Plex) & ShokoFin (Jellyfin)

The new presentation layer over Shoko (post-import); not yet stood up here.

- **Shoko Relay** (`natyusha/ShokoRelay`, successor to Shoko Metadata) — a Plex
  agent/scanner + automation scripts. Plex matches via Shoko (AniDB). It has a
  native **"Generate Collections"** that injects collections through Plex's HTTP
  API (Plex's provider framework can't auto-assign them) — our compiled
  collections must coexist non-destructively with these.
- **ShokoFin** — official Jellyfin plugin; requires Shoko Server.
- **VFS (Virtual File System)** — both consume Shoko's symlink trees, organized
  by Shoko `SeriesID` folders (`S##E##… [{ShokoFileID}].ext`), so on-disk naming
  is irrelevant to the media servers. Both can pick the displayed title
  language, which gives the EN / JA dual view over one VFS.
- **Collections asymmetry:** Kometa has native `anidb_id`/`anidb_relation`
  builders (Plex maps straight from AniDB IDs); `jellyfin-collection` has **no**
  AniDB builder, so the Jellyfin side will resolve AniDB ID → Jellyfin item (via
  ShokoFin provider IDs) and create collections through the Jellyfin API.
- **Legacy libraries today:** "Anime" (en) + "アニメ" (ja) on the HamaTV-derived
  agent (Kamehameha is a renamed Hama copy), ~97% AniDB-keyed via GUIDs like
  `…://anidb-715`. These stay as-is until Plex drops legacy-agent support; the
  near-term task is just making their collections consistent (see the migration
  plan / `scripts/plex_collections_export.py`).
