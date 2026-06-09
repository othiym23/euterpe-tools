#!/usr/bin/env python3
"""shoko — monitor a Shoko Server anime-library import.

One tool, several views, all reading the LOCAL Shoko API and/or its rotated
logs. It never contacts AniDB directly (Shoko owns AniDB's strict rate limits;
extra traffic risks a ban), so every subcommand is safe to run freely.

Subcommands:
  eta         live remaining (Dashboard UnrecognizedFiles) ÷ identification rate
              -> when the import finishes. --watch warns on a real AniDB ban.
  progress    per-day identification throughput from the logs + an ETA.
  throughput  per-day task counts by job type / provider group (from logs).
  durations   per-task-type execution time (mean/median/p90) from the logs.
  queue       live queue breakdown + concurrency rules + a workers verdict.

Config via flags or env: SHOKO_URL, SHOKO_APIKEY, SHOKO_LOGS_DIR. Add --json to
any subcommand for machine-readable output.

Example (this collection):
  export SHOKO_URL=http://euterpe.local:8111 SHOKO_APIKEY=...
  export SHOKO_LOGS_DIR=/Volumes/docker/kagee/shoko/config/Shoko.CLI/logs
  ./scripts/shoko.py eta --watch
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field as dc_field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NamedTuple

# ---------------------------------------------------------------------------
# Log primitives (shared by every log-reading subcommand)
# ---------------------------------------------------------------------------

RE_LOG_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})\.(log|zip)$")
# [YYYY-MM-DD HH:MM:SS:mmm] Level|<context> > <message>   (millis after a ':')
RE_LINE = re.compile(
    r"^\[\d{4}-\d{2}-\d{2} (\d{2}):(\d{2}):(\d{2}):(\d{3})\] (\w+)\|(.+?) > (.*)"
)
RE_JOBCLASS = re.compile(r"^([A-Za-z0-9]+Job)(?:_|$|\s)")
RE_LOOKUP = re.compile(r"Get AniDB file info")  # one AniDB UDP file lookup
RE_RECOGNIZED = re.compile(r"Found [1-9]\d* episodes for file")  # >=1 episode
RE_NOTFOUND = re.compile(r"Found 0 episodes for file")
RE_PROCESS = re.compile(r"ProcessFileJob")
RE_BAN = re.compile(r"AniDBBanned|UDPBan|HTTPBan|FloodControl", re.I)
RE_ERR_JOB = re.compile(r"\bJob\s+[\w.]*?([A-Za-z0-9]+Job)")


class LogLine(NamedTuple):
    sec: float  # seconds within the day (for spans / hourly buckets)
    hour: int
    level: str
    context: str
    message: str


def parse_line(line: str) -> LogLine | None:
    m = RE_LINE.match(line)
    if not m:
        return None
    h, mi, s, ms = int(m[1]), int(m[2]), int(m[3]), int(m[4])
    return LogLine(h * 3600 + mi * 60 + s + ms / 1000, h, m[5], m[6], m[7])


def job_class(context: str) -> str | None:
    m = RE_JOBCLASS.match(context)
    return m[1] if m else None


def iter_log_lines(path: Path):
    """Yield decoded lines from a .log or a .zip-wrapped daily .log."""
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as zf:
            inner = next((n for n in zf.namelist() if n.endswith(".log")), None)
            if inner is None:
                return
            with zf.open(inner) as f:
                for raw in f:
                    yield raw.decode("utf-8", "replace")
    else:
        with path.open(encoding="utf-8", errors="replace") as f:
            yield from f


def discover_days(logs_dir: Path) -> dict[date, Path]:
    """Map each day to its log file (a live ``.log`` wins over a ``.zip``).

    Raises SystemExit with a clear message if *logs_dir* is missing or not a
    directory (e.g. an unmounted NAS share), rather than an opaque traceback.
    """
    if not logs_dir.is_dir():
        raise SystemExit(f"error: --logs-dir is not a directory: {logs_dir}")
    days: dict[date, Path] = {}
    for p in sorted(logs_dir.iterdir()):
        m = RE_LOG_DATE.match(p.name)
        if not m:
            continue
        d = date(int(m[1]), int(m[2]), int(m[3]))
        if d not in days or p.suffix == ".log":
            days[d] = p
    return days


# ---------------------------------------------------------------------------
# Small numeric helpers
# ---------------------------------------------------------------------------


def trailing(values: list, window: int) -> list:
    """Drop the last (partial) entry, then take the trailing *window*.

    window <= 0 (or >= len) means "all full entries". Avoids the ``[-0:]``
    footgun (which would select the whole list).
    """
    full = values[:-1] or values  # keep the sole entry if there's only one
    if window <= 0 or window >= len(full):
        return full
    return full[-window:]


def percentile(xs: list[float], p: float = 90) -> float | None:
    """Inclusive p-th percentile — never extrapolates beyond the observed max."""
    if not xs:
        return None
    if len(xs) == 1:
        return xs[0]
    return statistics.quantiles(sorted(xs), n=100, method="inclusive")[int(p) - 1]


def estimate_slope(
    points: list[tuple[float, float]], window_hours: float
) -> tuple[float, float] | None:
    """Least-squares slope (value/second) over the trailing window, plus the
    span in hours. Negative slope = value dropping. None if too little data."""
    if len(points) < 2:
        return None
    cutoff = points[-1][0] - window_hours * 3600
    pts = [p for p in points if p[0] >= cutoff] or points
    if len(pts) < 2:
        return None
    n = len(pts)
    t0 = pts[0][0]
    xs = [t - t0 for t, _ in pts]
    ys = [float(v) for _, v in pts]
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return None
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
    return slope, (pts[-1][0] - pts[0][0]) / 3600


def fmt_duration(seconds: float) -> str:
    seconds = int(max(seconds, 0))
    d, r = divmod(seconds, 86400)
    h, r = divmod(r, 3600)
    m, _ = divmod(r, 60)
    parts = []
    if d:
        parts.append(f"{d}d")
    if h:
        parts.append(f"{h}h")
    if m or not parts:
        parts.append(f"{m}m")
    return " ".join(parts)


_MAX_ETA_SECONDS = 3650 * 86400  # clamp so date arithmetic can't overflow


def eta_done(seconds: float | None) -> datetime | None:
    if seconds is None or seconds <= 0 or seconds > _MAX_ETA_SECONDS:
        return None
    return datetime.now().astimezone() + timedelta(seconds=seconds)


# ---------------------------------------------------------------------------
# Shoko REST client
# ---------------------------------------------------------------------------


def field(d: Any, name: str, default: Any = None) -> Any:
    """Fetch *name* from a dict tolerant of PascalCase/camelCase serialization."""
    if not isinstance(d, dict):
        return default
    if name in d:
        return d[name]
    low = name[0].lower() + name[1:]
    if low in d:
        return d[low]
    nl = name.lower()
    for k, v in d.items():
        if k.lower() == nl:
            return v
    return default


class ShokoClient:
    def __init__(self, base: str, apikey: str) -> None:
        self.base = base.rstrip("/")
        self.apikey = apikey

    @classmethod
    def connect(cls, args: argparse.Namespace) -> ShokoClient:
        base = (args.url or "http://127.0.0.1:8111").rstrip("/")
        apikey = args.apikey
        if not apikey and args.user:
            apikey = cls._login(base, args.user, args.password or "", args.device)
            print(f"obtained api key for device {args.device!r}", file=sys.stderr)
        if not apikey:
            raise SystemExit(
                "error: need --apikey/$SHOKO_APIKEY or --user/--pass to authenticate"
            )
        return cls(base, apikey)

    @staticmethod
    def _request(
        base: str, path: str, apikey: str | None, body: dict | None = None
    ) -> Any:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if apikey:  # omit the header entirely when falsy -> clean 401, not TypeError
            headers["apikey"] = apikey
        req = urllib.request.Request(
            f"{base}{path}",
            data=data,
            headers=headers,
            method="POST" if body else "GET",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
        return json.loads(raw) if raw else None

    @classmethod
    def _login(cls, base: str, user: str, password: str, device: str) -> str:
        out = cls._request(
            base, "/api/auth", None, {"user": user, "pass": password, "device": device}
        )
        key = field(out, "apikey", "")
        if not key:
            raise SystemExit("login failed: no apikey in response")
        return str(key)

    def get(self, path: str) -> Any:
        return self._request(self.base, path, self.apikey)

    def queue(self) -> dict:
        return self.get("/api/v3/Queue") or {}

    def stats(self) -> dict:
        return self.get("/api/v3/Dashboard/Stats") or {}

    def ban_status(self) -> dict:
        return self.get("/api/v3/AniDB/BanStatus") or {}

    def debug_stats(self) -> dict:
        return self.get("/api/v3/Queue/DebugStats") or {}

    def queue_items(self, page: int, page_size: int = 1000) -> dict:
        return (
            self.get(
                f"/api/v3/Queue/Items?showAll=true&pageSize={page_size}&page={page}"
            )
            or {}
        )


def ban_lines(ban: dict) -> tuple[bool, list[str]]:
    """Return (any_banned, human warning lines) from a BanStatus response."""
    msgs: list[str] = []
    banned = False
    for kind in ("UDP", "HTTP"):
        b = field(ban, kind, {})
        if field(b, "IsBanned", False):
            banned = True
            dur = field(b, "BanDuration")
            since = field(b, "LastUpdatedAt")
            msgs.append(f"AniDB {kind} BAN active (duration {dur}, since {since})")
    return banned, msgs


# ---------------------------------------------------------------------------
# Log analysis passes
# ---------------------------------------------------------------------------


@dataclass
class DayCounts:
    lookups: int = 0
    recognized: int = 0
    notfound: int = 0
    process: int = 0
    bans: int = 0
    hour_lookups: dict[int, int] = dc_field(default_factory=dict)


def scan_days(logs_dir: Path, hourly_days: int = 0) -> dict[date, DayCounts]:
    """Per-day identification counts. ``hourly_days`` records an hourly lookup
    histogram for the most recent N days."""
    days = discover_days(logs_dir)
    hourly_set = set(sorted(days)[-hourly_days:]) if hourly_days else set()
    out: dict[date, DayCounts] = {}
    for d, path in sorted(days.items()):
        c = DayCounts()
        want_hours = d in hourly_set
        for line in iter_log_lines(path):
            if RE_BAN.search(line):
                c.bans += 1
            ll = parse_line(line)
            if ll is None:
                continue
            msg = ll.message
            if RE_LOOKUP.search(msg):
                c.lookups += 1
                if want_hours:
                    c.hour_lookups[ll.hour] = c.hour_lookups.get(ll.hour, 0) + 1
            elif RE_RECOGNIZED.search(msg):
                c.recognized += 1
            elif RE_NOTFOUND.search(msg):
                c.notfound += 1
            elif RE_PROCESS.search(ll.context):
                c.process += 1
        out[d] = c
    return out


def recognition_rate(days: dict[date, DayCounts], window: int) -> float | None:
    """Median files-recognized/day over recent active full days.

    Recognition (not lookup) rate is the correct divisor for UnrecognizedFiles:
    a lookup only drains the unrecognized pool when it actually links episodes.
    """
    counts = [c.recognized for _d, c in sorted(days.items())]
    recent = [n for n in trailing(counts, window) if n > 0]
    return statistics.median(recent) if recent else None


# ---------------------------------------------------------------------------
# Subcommand: eta
# ---------------------------------------------------------------------------


@dataclass
class EtaSample:
    ts: float
    total: int
    blocked: int
    running: int
    recognized: int | None
    unrecognized: int | None


def _eta_poll(client: ShokoClient) -> tuple[EtaSample, bool, list[str]]:
    q = client.queue()
    executing = field(q, "CurrentlyExecuting", []) or []
    stats = {}
    try:
        stats = client.stats()
    except urllib.error.URLError:
        pass
    banned, ban_msgs = False, []
    try:
        banned, ban_msgs = ban_lines(client.ban_status())
    except urllib.error.URLError:
        ban_msgs = ["(ban status unavailable)"]
    rec = field(stats, "FileCount")
    unrec = field(stats, "UnrecognizedFiles")
    s = EtaSample(
        ts=time.time(),
        total=int(field(q, "TotalCount", 0)),
        blocked=int(field(q, "BlockedCount", 0)),
        running=len(executing) if isinstance(executing, list) else 0,
        recognized=int(rec) if rec is not None else None,
        unrecognized=int(unrec) if unrec is not None else None,
    )
    return s, banned, ban_msgs


def cmd_eta(args: argparse.Namespace) -> int:
    client = ShokoClient.connect(args)
    log_rate = None
    if args.logs_dir:
        log_rate = recognition_rate(scan_days(args.logs_dir), args.window_days)

    samples: list[EtaSample] = []

    def once() -> EtaSample:
        s, banned, ban_msgs = _eta_poll(client)
        samples.append(s)
        if args.json:
            print(
                json.dumps(_eta_json(s, banned, samples, log_rate, args), default=str)
            )
        else:
            _eta_render(s, banned, ban_msgs, samples, log_rate, args)
        return s

    try:
        if not args.watch:
            once()
            return 0
        while True:
            s = once()
            if s.unrecognized == 0 or s.total == 0:
                print("identification complete — nothing left unrecognized.")
                return 0
            # Plateau: unrecognized stopped dropping over a full window -> residue.
            pts = [
                (x.ts, float(x.unrecognized))
                for x in samples
                if x.unrecognized is not None
            ]
            fit = estimate_slope(pts, args.window_hours)
            if fit and fit[1] >= args.window_hours and abs(-fit[0] * 86400) < 1:
                print(
                    f"identification plateaued at {s.unrecognized:,} residual files "
                    f"(likely unmatchable) — stopping."
                )
                return 0
            time.sleep(args.interval)
    except KeyboardInterrupt:
        return 130
    except urllib.error.HTTPError as e:
        print(f"HTTP {e.code} from Shoko: {e.reason}", file=sys.stderr)
        return 1
    except urllib.error.URLError as e:
        print(f"cannot reach Shoko at {client.base}: {e.reason}", file=sys.stderr)
        return 1


def _eta_rate(samples: list[EtaSample], log_rate: float | None, window_hours: float):
    """Pick the ETA rate: prefer the live-measured UnrecognizedFiles drain once
    there's enough history; otherwise the log recognition rate. Returns
    (per_day, source) or (None, source)."""
    pts = [(x.ts, float(x.unrecognized)) for x in samples if x.unrecognized is not None]
    fit = estimate_slope(pts, window_hours)
    if fit and fit[0] < 0:
        return -fit[0] * 86400, "live drain"
    if log_rate:
        return log_rate, "log recognition rate"
    return None, "live drain" if fit else "n/a"


def _eta_compute(s: EtaSample, samples: list[EtaSample], log_rate, window_hours):
    if s.unrecognized is None:
        return None, None, None
    per_day, src = _eta_rate(samples, log_rate, window_hours)
    if not per_day:
        return None, src, None
    eta_s = s.unrecognized / per_day * 86400
    return per_day, src, eta_s


def _eta_render(s, banned, ban_msgs, samples, log_rate, args):
    when = datetime.fromtimestamp(s.ts, tz=timezone.utc).astimezone()
    pct = ""
    if s.recognized is not None and s.unrecognized is not None:
        denom = s.recognized + s.unrecognized
        if denom:
            pct = f", {100 * s.recognized / denom:.1f}% identified"
    rec = f"{s.recognized:,}" if s.recognized is not None else "?"
    unrec = f"{s.unrecognized:,}" if s.unrecognized is not None else "?"
    print(f"[{when:%Y-%m-%d %H:%M:%S}] recognized={rec} unrecognized={unrec}{pct}")
    if banned:
        for m in ban_msgs:
            print(f"  ** WARNING: {m} **")
        ban_note = "AniDB BAN ACTIVE"
    else:
        ban_note = "AniDB ban: clear — blocked = rate-limit priority"
    print(
        f"  queue total={s.total:,}  blocked={s.blocked:,} ({ban_note})  running={s.running}"
    )
    per_day, src, eta_s = _eta_compute(s, samples, log_rate, args.window_hours)
    if eta_s is None:
        if s.unrecognized is not None and log_rate is None:
            print(
                "  ETA: pass --logs-dir for a rate, or wait for a 2nd sample (--watch)."
            )
        return
    done = eta_done(eta_s)
    tail = f"-> {done:%Y-%m-%d}" if done else "(beyond 10y horizon)"
    print(
        f"  ETA ({src} {per_day:,.0f}/day × {s.unrecognized:,} left): ~{fmt_duration(eta_s)} {tail}"
    )


def _eta_json(s, banned, samples, log_rate, args):
    per_day, src, eta_s = _eta_compute(s, samples, log_rate, args.window_hours)
    done = eta_done(eta_s)
    return {
        "ts": s.ts,
        "recognized": s.recognized,
        "unrecognized": s.unrecognized,
        "queue_total": s.total,
        "blocked": s.blocked,
        "running": s.running,
        "anidb_banned": banned,
        "rate_per_day": per_day,
        "rate_source": src,
        "eta_seconds": eta_s,
        "eta_date": done.date().isoformat() if done else None,
    }


# ---------------------------------------------------------------------------
# Subcommand: progress
# ---------------------------------------------------------------------------


def cmd_progress(args: argparse.Namespace) -> int:
    days = scan_days(args.logs_dir, hourly_days=args.hourly)
    if not days:
        raise SystemExit(f"no YYYY-MM-DD.log/.zip files in {args.logs_dir}")
    series = sorted(days.items())

    rate = recognition_rate(days, args.window_days)
    rec_counts = [c.recognized for _d, c in series]
    active = [n for n in trailing(rec_counts, args.window_days) if n > 0]

    remaining = args.remaining
    src = "given"
    if remaining is None and args.url and args.apikey:
        try:
            unrec = field(
                ShokoClient(args.url, args.apikey).stats(), "UnrecognizedFiles"
            )
            if unrec is not None:
                remaining, src = int(unrec), "live UnrecognizedFiles"
        except urllib.error.URLError as e:
            print(f"(live stats unavailable: {e})", file=sys.stderr)

    eta_days = remaining / rate if (remaining is not None and rate) else None
    done = eta_done(eta_days * 86400) if eta_days is not None else None

    if args.json:
        print(
            json.dumps(
                {
                    "days": {
                        str(d): vars(c) | {"hour_lookups": c.hour_lookups}
                        for d, c in series
                    },
                    "recognition_rate_per_day": rate,
                    "remaining": remaining,
                    "remaining_source": src,
                    "eta_days": eta_days,
                    "eta_date": done.date().isoformat() if done else None,
                },
                default=str,
            )
        )
        return 0

    print(
        f"{'date':<12}{'lookups':>9}{'recog':>8}{'notfnd':>8}{'hashed':>8}{'bans':>6}"
    )
    print("-" * 51)
    for d, c in series:
        print(
            f"{str(d):<12}{c.lookups:>9,}{c.recognized:>8,}{c.notfound:>8,}{c.process:>8,}{c.bans:>6}"
        )
    print()
    if rate is None:
        print("not enough active days to estimate a recognition rate.")
        return 0
    print(
        f"recognition rate (median of {len(active)} active days): {rate:,.0f}/day "
        f"(range {min(active):,}–{max(active):,})"
    )
    if remaining is None:
        print("\npass --remaining N (or --url/--apikey) for an ETA.")
        return 0
    tail = f"-> {done:%Y-%m-%d}" if done else "(beyond 10y horizon)"
    print(f"\nremaining ({src}): {remaining:,}")
    print(f"ETA: ~{eta_days:.1f} days {tail}")
    _print_hourly(series, args.hourly)
    return 0


def _print_hourly(series, hourly: int) -> None:
    if not hourly:
        return
    print("\nhourly lookups (most recent day):")
    for d, c in series[-hourly:]:
        if not c.hour_lookups:
            continue
        hours = sorted(c.hour_lookups)
        cells = " ".join(f"{h:02d}:{c.hour_lookups[h]}" for h in hours)
        avg = statistics.mean(c.hour_lookups[h] for h in hours)
        print(f"  {d}: {cells}\n    avg {avg:.0f}/h (~{avg * 24:,.0f}/day)")


# ---------------------------------------------------------------------------
# Subcommand: throughput
# ---------------------------------------------------------------------------

GROUP_ORDER = ["File", "AniDB", "TMDB", "Trakt", "Stats", "Other"]


def group_of(cls: str) -> str:
    c = cls.lower()
    if "anidb" in c or "mylist" in c:
        return "AniDB"
    if "tmdb" in c:
        return "TMDB"
    if "trakt" in c:
        return "Trakt"
    if "discoverfile" in c or "processfile" in c or "hashfile" in c:
        return "File"
    if "stats" in c or "refresh" in c:
        return "Stats"
    return "Other"


def cmd_throughput(args: argparse.Namespace) -> int:
    days = discover_days(args.logs_dir)
    if not days:
        raise SystemExit(f"no YYYY-MM-DD.log/.zip files in {args.logs_dir}")
    per_day: dict[date, dict[str, int]] = {}
    per_err: dict[date, dict[str, int]] = {}
    for d, path in sorted(days.items()):
        seen: dict[str, set[str]] = defaultdict(set)
        errs: dict[str, int] = defaultdict(int)
        for line in iter_log_lines(path):
            ll = parse_line(line)
            if ll is None:
                continue
            cls = job_class(ll.context)
            if cls:
                seen[cls].add(ll.context)
            if ll.level == "Error":
                em = RE_ERR_JOB.search(line)
                if em:
                    errs[em[1]] += 1
        per_day[d] = {k: len(v) for k, v in seen.items()}
        per_err[d] = dict(errs)

    dates = sorted(per_day)
    classes = sorted(
        {c for m in per_day.values() for c in m}
        | {c for m in per_err.values() for c in m}
    )
    grp = {c: group_of(c) for c in classes}
    window = trailing(dates, args.window_days)

    if args.json:
        print(
            json.dumps(
                {
                    "per_day": {
                        str(d): {"tasks": per_day[d], "errors": per_err[d]}
                        for d in dates
                    },
                    "groups": grp,
                },
                default=str,
            )
        )
        return 0

    print("Per task type (distinct job instances):")
    print(f"  {'task type':<26}{'group':<7}{'total':>9}{'recent/day':>12}{'errors':>8}")
    print("  " + "-" * 61)
    rows = []
    for c in classes:
        total = sum(per_day[d].get(c, 0) for d in dates)
        recent = sum(per_day[d].get(c, 0) for d in window) / max(len(window), 1)
        errcount = sum(per_err[d].get(c, 0) for d in dates)
        rows.append((total, c, recent, errcount))
    for total, c, recent, errcount in sorted(rows, reverse=True):
        print(f"  {c:<26}{grp[c]:<7}{total:>9,}{recent:>12,.0f}{errcount:>8,}")

    print("\nDaily throughput by group (tasks/day):")
    hdr = (
        "  "
        + f"{'date':<12}"
        + "".join(f"{g:>8}" for g in GROUP_ORDER)
        + f"{'errors':>8}"
    )
    print(hdr + "\n  " + "-" * (len(hdr) - 2))
    for d in dates:
        by_group: dict[str, int] = defaultdict(int)
        for c, n in per_day[d].items():
            by_group[grp[c]] += n
        cells = "".join(f"{by_group.get(g, 0):>8,}" for g in GROUP_ORDER)
        print(f"  {str(d):<12}{cells}{sum(per_err[d].values()):>8,}")
    return 0


# ---------------------------------------------------------------------------
# Subcommand: durations
# ---------------------------------------------------------------------------


def cmd_durations(args: argparse.Namespace) -> int:
    days = discover_days(args.logs_dir)
    if not days:
        raise SystemExit(f"no YYYY-MM-DD.log/.zip files in {args.logs_dir}")
    durations: dict[str, list[float]] = defaultdict(list)
    singleline: dict[str, int] = defaultdict(int)

    def flush(lo: float, hi: float, size: int, cls: str) -> None:
        # A cluster of >=2 lines is one measurable run; a lone line is a run
        # whose completion is logged by another logger (unmeasurable here).
        if size >= 2:
            durations[cls].append(hi - lo)
        else:
            singleline[cls] += 1

    for _d, path in sorted(days.items()):
        times: dict[str, list[float]] = defaultdict(list)
        for line in iter_log_lines(path):
            ll = parse_line(line)
            if ll is None:
                continue
            if job_class(ll.context):
                times[ll.context].append(ll.sec)
        for context, ts in times.items():
            cls = job_class(context)
            if cls is None:  # only job contexts were stored, but narrow for safety
                continue
            ts.sort()
            lo = prev = ts[0]
            size = 1
            for t in ts[1:]:
                if t - prev <= args.max_gap:  # same execution
                    prev, size = t, size + 1
                else:  # gap too large -> new execution
                    flush(lo, prev, size, cls)
                    lo = prev = t
                    size = 1
            flush(lo, prev, size, cls)

    classes = sorted(set(durations) | set(singleline))
    rows = []
    for cls in classes:
        ds = durations.get(cls, [])
        one = singleline.get(cls, 0)
        cover = len(ds) / (len(ds) + one) if (len(ds) + one) else 0.0
        rows.append(
            (
                cls,
                len(ds),
                one,
                cover,
                statistics.mean(ds) if ds else None,
                statistics.median(ds) if ds else None,
                percentile(ds, 90) if ds else None,
            )
        )
    rows.sort(key=lambda r: (r[3] < 0.5, -r[3], -r[1]))

    if args.json:
        print(
            json.dumps(
                [
                    {
                        "job_type": c,
                        "measured": m,
                        "single_line": o,
                        "coverage": cov,
                        "mean_s": me,
                        "median_s": md,
                        "p90_s": p,
                    }
                    for c, m, o, cov, me, md, p in rows
                ],
                default=str,
            )
        )
        return 0

    def f(s):
        if s is None:
            return "     —"
        return f"{s * 1000:>4.0f}ms" if s < 1 else f"{s:>5.2f}s"

    print("Per-task-type execution time (span between start and completion lines):\n")
    print(
        f"  {'task type':<26}{'measured':>9}{'1-line':>8}{'cover':>7}{'mean':>8}{'median':>8}{'p90':>8}"
    )
    print("  " + "-" * 74)
    for c, m, o, cov, me, md, p in rows:
        print(f"  {c:<26}{m:>9,}{o:>8,}{cov * 100:>6.0f}%{f(me):>8}{f(md):>8}{f(p):>8}")
    print(
        "\n  Low cover = one log line per run (completion logged elsewhere); its\n"
        "  mean/median is from few/mispaired samples — treat as unreliable."
    )
    return 0


# ---------------------------------------------------------------------------
# Subcommand: queue
# ---------------------------------------------------------------------------


def _short(asm_name: str) -> str:
    return asm_name.split(",")[0].split(".")[-1]


def cmd_queue(args: argparse.Namespace) -> int:
    client = ShokoClient.connect(args)
    by_type: dict[str, int] = defaultdict(int)
    blocked: dict[str, int] = defaultdict(int)
    running = 0
    total = 0
    page = 1
    while True:
        d = client.queue_items(page)
        total = int(field(d, "Total", 0))
        items = field(d, "List", []) or []
        if not items:
            break
        for it in items:
            t = str(field(it, "Type", "?"))
            by_type[t] += 1
            if field(it, "IsRunning"):
                running += 1
            elif field(it, "IsBlocked"):
                blocked[t] += 1
        if sum(by_type.values()) >= total:
            break
        page += 1

    ds = client.debug_stats()
    q = field(ds, "Queue", {})
    thread = int(field(q, "ThreadCount", 0))
    run = len(field(q, "CurrentlyExecuting", []) or [])
    limits = {_short(k): v for k, v in (field(ds, "TypesToLimit", {}) or {}).items()}
    excluded = [_short(x) for x in (field(ds, "TypesToExclude", []) or [])]
    banned, ban_msgs = ban_lines(client.ban_status())

    if args.json:
        print(
            json.dumps(
                {
                    "total": total,
                    "sampled": sum(by_type.values()),
                    "running": running,
                    "by_type": dict(by_type),
                    "blocked_by_type": dict(blocked),
                    "thread_count": thread,
                    "active_workers": run,
                    "idle_workers": thread - run,
                    "concurrency_caps": limits,
                    "excluded_types": excluded,
                    "anidb_banned": banned,
                },
                default=str,
            )
        )
        return 0

    print(
        f"queue total={total:,}  sampled={sum(by_type.values()):,}  running={running}\n"
    )
    print(f"  {'queued task type':<38}{'count':>8}{'blocked':>9}")
    print("  " + "-" * 55)
    for t, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
        print(f"  {t:<38}{n:>8,}{blocked.get(t, 0):>9,}")
    print(
        f"\nworkers: {thread} threads, {run} active, {thread - run} idle  |  "
        f"blocked={int(field(q, 'BlockedCount', 0)):,}"
    )
    if banned:
        for m in ban_msgs:
            print(f"  ** WARNING: {m} **")
    print("\nconcurrency caps (max parallel):")
    for k, v in sorted(limits.items(), key=lambda kv: -kv[1])[:8]:
        print(f"  {k:<30} {v}")
    print(
        f"\nexcluded while running ({len(excluded)} types, single-slot AniDB/file pipeline):"
    )
    print("  " + ", ".join(excluded))
    idle = thread - run
    print(
        f"\nverdict: {'idle workers exist — ' if idle > 0 else ''}"
        "the AniDB pipeline is single-slot + rate-limited, so more workers would not help."
    )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="shoko",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = ap.add_subparsers(dest="command", required=True)

    def add_api(p):
        p.add_argument(
            "--url", default=os.environ.get("SHOKO_URL", "http://127.0.0.1:8111")
        )
        p.add_argument("--apikey", default=os.environ.get("SHOKO_APIKEY"))
        p.add_argument("--user")
        p.add_argument("--pass", dest="password")
        p.add_argument("--device", default="shoko-monitor")
        p.add_argument("--json", action="store_true")

    def add_logs(p):
        p.add_argument(
            "--logs-dir", type=Path, default=os.environ.get("SHOKO_LOGS_DIR")
        )
        p.add_argument("--window-days", type=int, default=7)

    e = sub.add_parser("eta", help="live remaining ÷ identification rate -> finish ETA")
    add_api(e)
    e.add_argument("--logs-dir", type=Path, default=os.environ.get("SHOKO_LOGS_DIR"))
    e.add_argument("--window-days", type=int, default=7)
    e.add_argument("--window-hours", type=float, default=24.0)
    e.add_argument("--watch", action="store_true")
    e.add_argument("--interval", type=float, default=300.0)
    e.set_defaults(func=cmd_eta)

    p = sub.add_parser("progress", help="per-day identification throughput + ETA")
    add_logs(p)
    p.add_argument("--url", default=os.environ.get("SHOKO_URL"))
    p.add_argument("--apikey", default=os.environ.get("SHOKO_APIKEY"))
    p.add_argument("--remaining", type=int)
    p.add_argument("--hourly", type=int, default=1)
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_progress)

    t = sub.add_parser("throughput", help="per-day task counts by type/group")
    add_logs(t)
    t.add_argument("--json", action="store_true")
    t.set_defaults(func=cmd_throughput)

    d = sub.add_parser("durations", help="per-task-type execution time")
    d.add_argument("--logs-dir", type=Path, default=os.environ.get("SHOKO_LOGS_DIR"))
    d.add_argument(
        "--max-gap",
        type=float,
        default=60.0,
        help="seconds; lines of one context within this gap are one run",
    )
    d.add_argument("--json", action="store_true")
    d.set_defaults(func=cmd_durations)

    qp = sub.add_parser("queue", help="live queue breakdown + concurrency rules")
    add_api(qp)
    qp.set_defaults(func=cmd_queue)

    return ap


def main() -> int:
    args = build_parser().parse_args()
    if getattr(args, "logs_dir", None) is not None:
        args.logs_dir = Path(args.logs_dir)
    if (
        getattr(args, "func", None) in (cmd_progress, cmd_throughput, cmd_durations)
        and not args.logs_dir
    ):
        raise SystemExit("error: --logs-dir (or $SHOKO_LOGS_DIR) is required")
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
