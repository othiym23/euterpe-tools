"""Shared plan/apply ingestion core for ``etp movies`` and ``etp television``.

Both commands are thin wrappers around this module, parameterized by
:class:`MediaKind`. The pipeline is non-interactive and designed for LLM
agents as first-class users:

``plan``
    Scan the managed (Radarr/Sonarr) tree and/or the downloads directory,
    parse filenames, resolve provider IDs (config override → exact
    title+year search → ambiguous becomes ``needs-id`` with candidates),
    analyze files with mediainfo, and write a KDL plan manifest plus a
    machine-readable summary. Never touches the destination.

``apply``
    Validate a plan manifest against the live filesystem (fail fast on
    drift, all violations reported at once), then execute reflink copies
    and subtitle sidecars, and record sources in the shared ingest
    register.

Provider roles are fixed per kind: television resolves against TheTVDB
(episode numbering and titles) and cross-checks via TMDB; movies resolve
against TMDB and cross-check via TheTVDB. Mismatches are warnings, never
fatal. Directory names embed only the primary provider's ID, in Plex's
curly-brace syntax (``{tvdb-NNN}`` / ``{tmdb-NNN}``).

Exit codes follow the AI-agent CLI conventions: 0 success, 1 failure,
2 nothing to do.
"""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from dataclasses import replace as dc_replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import kdl

from etp_lib import arr, media_parser, tmdb, tvdb
from etp_lib.conflicts import compute_crc32, copy_reflink
from etp_lib.ingest_register import load_register, save_register
from etp_lib.manifest import _MAX_FILENAME_BYTES, copy_subtitle_sidecars, escape_kdl
from etp_lib.media_config import anime_source_dir, lookup_mapping
from etp_lib.media_scanner import iter_media_files, parse_source_filename
from etp_lib.media_vocab import _PVR_TOOL_NAMES
from etp_lib.mediainfo import analyze_file
from etp_lib.mediainfo_cache import analyze_file_cached, save_cache
from etp_lib.naming import (
    classify_extra,
    crc_suffixed,
    extra_display_name,
    extras_dir_category,
    format_movie_dirname,
    is_sample,
    format_movie_filename,
    format_tv_episode_filename,
    format_tv_series_dirname,
    normalize_title,
    season_subdir,
    word_prefix,
)
from etp_lib.types import (
    AnimeInfo,
    ConflictAction,
    Episode,
    MediaIngestConfig,
    MediaInfo,
    MetadataProvider,
    MovieInfo,
    SearchCandidate,
    SourceFile,
    TitleMapping,
    TmdbTvInfo,
)

SCHEMA_VERSION = 1

# Provider/network/parse failures that degrade a single title to
# needs-id/unavailable instead of aborting the whole plan.
_PROVIDER_ERRORS = (OSError, ValueError, KeyError, json.JSONDecodeError)


class MediaKind(StrEnum):
    MOVIE = "movie"
    TV = "tv"

    @property
    def tool(self) -> str:
        return "etp-movies" if self is MediaKind.MOVIE else "etp-television"

    @property
    def cli(self) -> str:
        return "etp movies" if self is MediaKind.MOVIE else "etp television"

    @property
    def block_name(self) -> str:
        """Top-level manifest node name for one title."""
        return "movie" if self is MediaKind.MOVIE else "series"

    @property
    def entry_name(self) -> str:
        """Manifest node name for one source file."""
        return "file" if self is MediaKind.MOVIE else "episode"

    @property
    def managed_mode(self) -> str:
        """Name of the managed-tree source mode (matches the CLI flag)."""
        return "radarr" if self is MediaKind.MOVIE else "sonarr"

    @property
    def domain(self) -> str:
        """This kind's domain label for foreign-domain partitioning.

        Matches the Radarr/Sonarr root-folder basename convention and
        the ``domain`` property in config mappings. A fixed label, not
        the scan root's basename — a ``--source`` override must not
        change what counts as our own domain.
        """
        return "movies" if self is MediaKind.MOVIE else "television"

    @property
    def primary_provider(self) -> MetadataProvider:
        return (
            MetadataProvider.TMDB if self is MediaKind.MOVIE else MetadataProvider.TVDB
        )


class EntryStatus(StrEnum):
    READY = "ready"
    NEEDS_ID = "needs-id"
    CONFLICT = "conflict"
    SKIP = "skip"


class Confidence(StrEnum):
    EXACT = "exact"  # single search hit with matching title and year
    HIGH = "high"  # single search hit, title/year not an exact match
    AMBIGUOUS = "ambiguous"  # multiple plausible hits
    NONE = "none"  # no hits or provider unavailable


class CrossCheck(StrEnum):
    OK = "ok"
    MISMATCH = "mismatch"
    UNAVAILABLE = "unavailable"


class ManifestError(Exception):
    """A plan manifest is malformed or fails validation."""


# ---------------------------------------------------------------------------
# Scanning (managed Radarr/Sonarr trees)
# ---------------------------------------------------------------------------

# "Title (1999)" / "Title [Alt Title] (1999)" managed-tree folder names
_RE_TITLE_YEAR_DIR = re.compile(
    r"^(?P<title>.+?)(?:\s+\[(?P<alt>[^\]]+)\])?\s+\((?P<year>\d{4})\)$"
)
_RE_SEASON_DIR = re.compile(r"^season[ _]?(\d+)$", re.IGNORECASE)
# "- cd1" / ".pt2" / " part 3" multi-part movie suffixes (Plex split names)
_RE_PART_SUFFIX = re.compile(
    r"[-. _](?:cd|dvd|disc|disk|part|pt)[ _]?(\d+)\b", re.IGNORECASE
)
# Radarr's "- complete movie -" marker with optional trailing edition text
_RE_COMPLETE_MOVIE = re.compile(
    r"\s*-\s*complete movie\s*(?:-\s*)?(?P<edition>[^\[\]]*?)\s*(?=\[|$)",
    re.IGNORECASE,
)


@dataclass
class ScannedFile:
    """One media file found in a source tree."""

    source: SourceFile
    season: int | None = None
    episode: int | None = None
    episodes: list[int] = field(default_factory=list)
    episode_title: str = ""
    part: int | None = None  # multi-part movies: cd1/pt2/...
    extra_category: str = ""  # extras subdir ("Featurettes", ...) when an extra


@dataclass
class ScannedTitle:
    """One movie or series found in a source tree, with its files."""

    raw_title: str  # source folder name; the stable matching key
    title: str
    year: int
    alt_title: str = ""
    edition: str = ""
    files: list[ScannedFile] = field(default_factory=list)


def scan_managed_tree(root: Path, kind: MediaKind) -> list[ScannedTitle]:
    """Scan a Radarr/Sonarr-managed tree into per-title file groups.

    Managed trees have deterministic naming (``Title (Year)`` folders,
    ``Season NN`` subdirs, ``SxxEyy`` episode tags), so folder names are
    authoritative for title/year and the media parser only fills in
    file-level details.
    """
    titles: list[ScannedTitle] = []
    if not root.is_dir():
        return titles
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        m = _RE_TITLE_YEAR_DIR.match(child.name)
        if m:
            title = m.group("title")
            alt = m.group("alt") or ""
            year = int(m.group("year"))
        else:
            pm = media_parser.parse_component(child.name)
            title = pm.series_name or child.name
            alt = pm.series_name_alt
            year = 0
        scanned = ScannedTitle(
            raw_title=child.name, title=title, year=year, alt_title=alt
        )
        for path in sorted(iter_media_files([child])):
            scanned.files.append(_scan_managed_file(path, child, kind, scanned))
        if scanned.files:
            titles.append(scanned)
    return titles


def _scanned_source(path: Path) -> SourceFile:
    """Parse a file's name and attach its path."""
    sf = parse_source_filename(path.name)
    sf.path = path
    return sf


def _scan_managed_file(
    path: Path, title_dir: Path, kind: MediaKind, scanned: ScannedTitle
) -> ScannedFile:
    sf = _scanned_source(path)
    # Radarr/Sonarr naming templates leave their own name where a release
    # group would appear ("[Radarr Remux-1080p,...]"); it is not a group.
    if sf.parsed.release_group in _PVR_TOOL_NAMES:
        sf.parsed.release_group = ""

    if kind is MediaKind.MOVIE:
        part_match = _RE_PART_SUFFIX.search(path.stem)
        edition_match = _RE_COMPLETE_MOVIE.search(path.stem)
        edition = edition_match.group("edition").strip(" -") if edition_match else ""
        if not edition and sf.parsed.is_criterion:
            edition = "Criterion Collection"
        if edition and not scanned.edition:
            scanned.edition = edition
        return ScannedFile(
            source=sf, part=int(part_match.group(1)) if part_match else None
        )

    # Television: the SxxEyy tag is authoritative when present; the season
    # folder fills the gap otherwise.
    season = sf.parsed.season
    if season is None and path.parent != title_dir:
        dirname = path.parent.name
        season_match = _RE_SEASON_DIR.match(dirname)
        if season_match:
            season = int(season_match.group(1))
        elif dirname.lower() == "specials":
            season = 0
    if season is None:
        season = 0 if sf.parsed.is_special else 1
    return ScannedFile(
        source=sf,
        season=season,
        episode=sf.parsed.episode,
        episodes=list(sf.parsed.episodes),
        episode_title=sf.parsed.episode_title,
    )


def _torrent_dir(path: Path, roots: list[Path]) -> Path | None:
    """The torrent's top-level directory under a downloads root, or None
    for files sitting directly in a root."""
    for root in roots:
        if root in path.parents:
            rel = path.relative_to(root)
            if len(rel.parts) > 1:
                return root / rel.parts[0]
            return None
    return None


def _extras_dir_context(path: Path, roots: list[Path]) -> tuple[Path, str] | None:
    """The owning directory and extras category for a file stored under a
    recognized extras directory (``Featurettes/``, ``Extras/``, ...).

    Returns ``(owner directory, category)`` where the owner is the
    directory containing the extras directory, or None when no ancestor
    inside the downloads tree is an extras directory (or the extras
    directory sits directly in a root, leaving no owner to attach to).
    """
    for parent in path.parents:
        if parent in roots:
            return None
        category = extras_dir_category(parent.name)
        if category is not None:
            owner = parent.parent
            if owner in roots:
                return None
            return owner, category or classify_extra(path.stem)
    return None


def _analyze_movie_torrents(
    files: list[Path], roots: list[Path]
) -> tuple[dict[Path, tuple[Path, str]], set[Path]]:
    """Sort each torrent directory's videos into a main film and extras.

    A movie torrent often carries extras as sibling videos (interviews,
    making-ofs, trailers) that would otherwise each become a junk
    ``needs-id`` group. Within each torrent directory the largest video
    is the main film; videos under half its size are its extras, mapped
    to ``(main file, extras subdirectory)``. The half-size guard keeps
    multi-part mains (cd1/cd2) and alternate cuts out of the extras bin.

    A directory with three or more main-sized videos that aren't movie
    parts is a season pack, not a movie — every file in it is returned
    in the exclusion set so episode batches can never flood a movie plan
    one "movie" per episode, however their filenames parse.
    """
    by_dir: dict[Path, list[Path]] = {}
    for path in files:
        torrent = _torrent_dir(path, roots)
        if torrent is not None:
            by_dir.setdefault(torrent, []).append(path)

    extras: dict[Path, tuple[Path, str]] = {}
    season_pack_files: set[Path] = set()
    for members in by_dir.values():
        if len(members) < 2:
            continue
        sizes = {p: _size_of(p) for p in members}
        main = max(members, key=lambda p: sizes[p])
        if not sizes[main]:
            continue
        mains = [
            p
            for p in members
            if sizes[p] * 2 > sizes[main] and not _RE_PART_SUFFIX.search(p.stem)
        ]
        if len(mains) >= 3:
            season_pack_files.update(members)
            continue
        for path in members:
            if path is main or sizes[path] * 2 > sizes[main]:
                continue
            if is_sample(path.stem):
                category = ""
            else:
                # An enclosing Featurettes/Extras/... directory names the
                # category more reliably than the filename does.
                ctx = _extras_dir_context(path, roots)
                category = ctx[1] if ctx else classify_extra(path.stem)
            extras[path] = (main, category)
    return extras, season_pack_files


def scan_downloads(roots: list[Path], kind: MediaKind) -> list[ScannedTitle]:
    """Group downloads-directory files into per-title groups (best effort).

    The downloads directory mixes anime, television, and movies under
    torrent-style naming, so this is deliberately conservative: files are
    grouped by parsed title, files that don't look like the target kind
    (episode markers for TV, their absence for movies) are left out, and
    the resolution confidence ladder does the real gatekeeping — anything
    questionable lands in the manifest as ``needs-id``, never a guessed
    destination.
    """
    files = sorted(iter_media_files(roots))
    extras: dict[Path, tuple[Path, str]] = {}
    season_pack_files: set[Path] = set()
    if kind is MediaKind.MOVIE:
        extras, season_pack_files = _analyze_movie_torrents(files, roots)
    group_of_main: dict[Path, ScannedTitle] = {}

    # Directory names get parsed once, not once per contained file (a
    # season pack re-reads its dirname for every episode).
    dir_parses: dict[str, media_parser.ParsedMedia] = {}

    def parse_dir(name: str) -> media_parser.ParsedMedia:
        if name not in dir_parses:
            dir_parses[name] = media_parser.parse_component(name)
        return dir_parses[name]

    groups: dict[str, ScannedTitle] = {}
    for path in files:
        if path in season_pack_files:
            continue  # episode batch, not movie material
        if path in extras:
            continue  # attached to its main film's group below

        # A file under a recognized extras directory (Featurettes/,
        # Extras/, ...) belongs to the title named by the directory above
        # the extras directory, not to whatever its own filename parses
        # as — featurette names look nothing like episodes and would each
        # become a junk block of their own.
        ctx = _extras_dir_context(path, roots)
        if ctx is not None:
            if is_sample(path.stem):
                continue
            owner, category = ctx
            owner_pm = parse_dir(owner.name)
            owner_episodic = (
                owner_pm.season is not None
                or owner_pm.episode is not None
                or owner_pm.is_special
            )
            if not owner_pm.series_name or (kind is MediaKind.TV) != owner_episodic:
                continue
            group = _group_for(
                groups,
                kind,
                owner_pm.series_name,
                owner_pm.series_name_alt,
                owner_pm.year or 0,
            )
            group.files.append(
                ScannedFile(source=_scanned_source(path), extra_category=category)
            )
            continue

        sf = _scanned_source(path)
        pm = sf.parsed

        title = pm.series_name
        alt = pm.series_name_alt
        year = pm.year or 0

        # Pre-organized Show/Season N/episode trees inside downloads: the
        # season directory carries the season and the directory above it
        # carries the show's name (the filename usually holds only the
        # episode number and title).
        season_from_dir: int | None = None
        context_dir: Path | None = None
        if path.parent not in roots:
            season_match = _RE_SEASON_DIR.match(path.parent.name)
            if season_match:
                season_from_dir = int(season_match.group(1))
                if path.parent.parent not in roots:
                    context_dir = path.parent.parent
            else:
                context_dir = path.parent

        # The containing directory often carries the descriptive name while
        # the file inside is abbreviated. Prefer it when the filename gave
        # nothing, or (for movies) when only the directory parse finds a
        # release year. Only parse the directory when it could matter.
        dir_might_help = not title or (kind is MediaKind.MOVIE and not year)
        if dir_might_help and context_dir is not None:
            dir_pm = parse_dir(context_dir.name)
            if not title or (dir_pm.series_name and dir_pm.year):
                title = dir_pm.series_name
                alt = dir_pm.series_name_alt
                year = dir_pm.year or 0

        if not title:
            continue
        looks_episodic = (
            pm.episode is not None
            or pm.season is not None
            or pm.is_special
            or season_from_dir is not None
        )
        if (kind is MediaKind.TV) != looks_episodic:
            continue

        group = _group_for(groups, kind, title, alt, year)

        if kind is MediaKind.TV:
            season = pm.season
            if season is None:
                season = season_from_dir
            if season is None:
                season = 0 if pm.is_special else 1
            group.files.append(
                ScannedFile(
                    source=sf,
                    season=season,
                    episode=pm.episode,
                    episodes=list(pm.episodes),
                    episode_title=pm.episode_title,
                )
            )
        else:
            if pm.is_criterion and not group.edition:
                group.edition = "Criterion Collection"
            part_match = _RE_PART_SUFFIX.search(path.stem)
            group.files.append(
                ScannedFile(
                    source=sf,
                    part=int(part_match.group(1)) if part_match else None,
                )
            )
            group_of_main[path] = group

    for path, (main, category) in sorted(extras.items()):
        if not category:
            continue  # torrent sample clip — junk, not an extra
        group = group_of_main.get(main)
        if group is None:
            continue  # main film didn't group; extras stay out too
        group.files.append(
            ScannedFile(source=_scanned_source(path), extra_category=category)
        )

    return [groups[key] for key in sorted(groups)]


def _group_for(
    groups: dict[str, ScannedTitle],
    kind: MediaKind,
    title: str,
    alt: str,
    year: int,
) -> ScannedTitle:
    """Get or create the downloads group a title belongs to.

    Movies group by title AND year: a remake must not collapse into its
    original ("Suspiria.1977" vs "Suspiria.2018" are two films);
    same-film copies that differ only in having a year split here and
    re-merge by provider ID after resolution. Shows group by title
    alone — a series spans years.
    """
    key = normalize_title(title)
    if kind is MediaKind.MOVIE and year:
        key = f"{key} ({year})"
    group = groups.get(key)
    if group is None:
        group = groups[key] = ScannedTitle(
            raw_title=title, title=title, year=year, alt_title=alt
        )
    if year and not group.year:
        group.year = year
    return group


# ---------------------------------------------------------------------------
# Provider resolution
# ---------------------------------------------------------------------------


# Environment variable carrying each provider's API key (see media.env).
API_KEY_ENV = {
    MetadataProvider.TMDB: "TMDB_API_KEY",
    MetadataProvider.TVDB: "TVDB_API_KEY",
}

# Environment variable carrying each kind's PVR-tool API key.
ARR_KEY_ENV = {
    MediaKind.MOVIE: "RADARR_API_KEY",
    MediaKind.TV: "SONARR_API_KEY",
}


def _fetch_tvdb_series_tv(
    series_id: int, api_key: str, no_cache: bool = False
) -> AnimeInfo:
    """fetch_tvdb_series against a television-only cache directory.

    The anime pipeline's title-alias index slurps every record in the
    default ``tvdb`` cache, so general-television records fetched here
    must never land there (a live-action Death Note would merge into the
    anime's alias group and contaminate anime download matching).
    """
    return tvdb.fetch_tvdb_series(series_id, api_key, no_cache, cache_name="tvdb-tv")


@dataclass
class Providers:
    """Provider call points and credentials, injectable for tests."""

    tmdb_search_movie: Callable[..., list[SearchCandidate]] = tmdb.search_movie
    tmdb_search_tv: Callable[..., list[SearchCandidate]] = tmdb.search_tv
    tmdb_fetch_movie: Callable[..., MovieInfo] = tmdb.fetch_tmdb_movie
    tmdb_fetch_tv: Callable[..., TmdbTvInfo] = tmdb.fetch_tmdb_tv
    tvdb_search_series: Callable[..., list[SearchCandidate]] = tvdb.search_tvdb_series
    tvdb_search_movies: Callable[..., list[SearchCandidate]] = tvdb.search_tvdb_movies
    tvdb_fetch_series: Callable[..., AnimeInfo] = _fetch_tvdb_series_tv
    radarr_fetch: Callable[..., dict[str, arr.ArrEntry]] = arr.fetch_radarr_index
    sonarr_fetch: Callable[..., dict[str, arr.ArrEntry]] = arr.fetch_sonarr_index
    analyze: Callable[..., MediaInfo | None] = analyze_file
    tmdb_key: str = ""
    tvdb_key: str = ""
    arr_key: str = ""  # Radarr/Sonarr API key, per kind
    cross_arr_key: str = ""  # the other kind's PVR-tool API key
    no_cache: bool = False


def pick_candidate(
    candidates: list[SearchCandidate], title: str, alt_title: str, year: int
) -> tuple[Confidence, SearchCandidate | None]:
    """Pick a search candidate by exact title+year match.

    A single exact match is ``exact``; a single hit that isn't an exact
    match is ``high`` (year-filtered searches with one result are nearly
    always right, and plan output is reviewed before apply); anything
    else is ``ambiguous``/``none`` and must be resolved by ID.
    """
    if not candidates:
        return Confidence.NONE, None
    wanted = {normalize_title(t) for t in (title, alt_title) if t}

    def is_exact(c: SearchCandidate) -> bool:
        names = {normalize_title(c.title), normalize_title(c.original_title)} - {""}
        year_ok = not year or not c.year or c.year == year
        return bool(wanted & names) and year_ok

    exact = [c for c in candidates if is_exact(c)]
    if len(exact) == 1:
        return Confidence.EXACT, exact[0]
    if not exact and len(candidates) == 1:
        return Confidence.HIGH, candidates[0]
    return Confidence.AMBIGUOUS, None


# ---------------------------------------------------------------------------
# Plan manifest model
# ---------------------------------------------------------------------------


@dataclass
class FileEntry:
    """One source file in a plan manifest."""

    source: str  # absolute path
    size: int
    status: EntryStatus
    dest: str = ""  # destination path relative to the title's dest-dir
    season: int | None = None
    number: int | None = None
    episodes: list[int] = field(default_factory=list)
    title: str = ""  # episode title
    conflict: ConflictAction | None = None
    note: str = ""


@dataclass
class TitleBlock:
    """One movie or series block in a plan manifest."""

    raw_title: str
    title: str = ""
    original_title: str = ""
    """Original-language title; leads the directory name when it differs
    from the English title (library convention: ``Original [English]``)."""
    year: int = 0
    tmdb_id: int | None = None
    tvdb_id: int | None = None
    imdb_id: str = ""
    edition: str = ""
    confidence: Confidence = Confidence.NONE
    cross_check: CrossCheck | None = None
    cross_check_note: str = ""
    dest_dir: str = ""  # directory name under the dest root
    note: str = ""
    candidates: list[SearchCandidate] = field(default_factory=list)
    entries: list[FileEntry] = field(default_factory=list)


@dataclass
class PlanManifest:
    kind: MediaKind
    created: str
    source_mode: str
    dest_root: str
    schema_version: int = SCHEMA_VERSION
    blocks: list[TitleBlock] = field(default_factory=list)


def write_plan_manifest(manifest: PlanManifest, path: Path) -> None:
    """Serialize a plan manifest to KDL."""
    kind = manifest.kind
    lines = [
        f"// Generated by `{kind.cli} ingest plan`. Review and edit, then run",
        f"// `{kind.cli} ingest apply {path.name}`.",
        "//",
        '// Editable: per-entry `status` ("ready" -> "skip"), `conflict`',
        '// ("keep"|"replace"|"both"|"skip"), and `tmdb`/`tvdb` IDs on needs-id',
        "// blocks (then re-run plan with --refine to recompute destinations).",
        "// `dest`/`dest-dir` are computed by plan; do not edit them by hand.",
        "meta {",
        f'  tool "{kind.tool}"',
        f'  kind "{kind}"',
        f"  schema-version {manifest.schema_version}",
        f'  created "{manifest.created}"',
        f'  source-mode "{manifest.source_mode}"',
        f'  dest-root "{escape_kdl(manifest.dest_root)}"',
        "}",
    ]
    for block in manifest.blocks:
        lines.append("")
        lines.append(f'{kind.block_name} "{escape_kdl(block.raw_title)}" {{')
        if block.title:
            lines.append(f'  title "{escape_kdl(block.title)}"')
        if block.original_title:
            lines.append(f'  original-title "{escape_kdl(block.original_title)}"')
        if block.year:
            lines.append(f"  year {block.year}")
        if block.tmdb_id:
            lines.append(f"  tmdb {block.tmdb_id}")
        if block.tvdb_id:
            lines.append(f"  tvdb {block.tvdb_id}")
        if block.imdb_id:
            lines.append(f'  imdb "{escape_kdl(block.imdb_id)}"')
        if block.edition:
            lines.append(f'  edition "{escape_kdl(block.edition)}"')
        lines.append(f'  confidence "{block.confidence}"')
        if block.cross_check is not None:
            note = (
                f' note="{escape_kdl(block.cross_check_note)}"'
                if block.cross_check_note
                else ""
            )
            lines.append(f'  cross-check "{block.cross_check}"{note}')
        if block.note:
            lines.append(f'  note "{escape_kdl(block.note)}"')
        if block.dest_dir:
            lines.append(f'  dest-dir "{escape_kdl(block.dest_dir)}"')
        for c in block.candidates:
            lines.append(
                f'  candidate provider="{c.provider}" id={c.id}'
                f' name="{escape_kdl(c.title)}" year={c.year}'
            )
        for e in block.entries:
            lines.append(f"  {kind.entry_name} {{")
            lines.append(f'    status "{e.status}"')
            lines.append(f'    source "{escape_kdl(e.source)}"')
            lines.append(f"    size {e.size}")
            if e.season is not None:
                lines.append(f"    season {e.season}")
            if e.number is not None:
                lines.append(f"    number {e.number}")
            if len(e.episodes) > 1:
                lines.append(f"    episodes {' '.join(str(n) for n in e.episodes)}")
            if e.title:
                lines.append(f'    title "{escape_kdl(e.title)}"')
            if e.dest:
                lines.append(f'    dest "{escape_kdl(e.dest)}"')
            if e.conflict is not None:
                lines.append(f'    conflict "{e.conflict}"')
            if e.note:
                lines.append(f'    note "{escape_kdl(e.note)}"')
            lines.append("  }")
        lines.append("}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _first_arg(node: kdl.Node, default: object = "") -> object:
    return node.args[0] if node.args else default


def _to_int(value: object) -> int:
    """Coerce a KDL argument (which kdl-py parses as float) to int."""
    if isinstance(value, int | float):
        return int(value)
    try:
        return int(str(value))
    except ValueError:
        return 0


def parse_plan_manifest(path: Path) -> PlanManifest:
    """Parse and structurally validate a KDL plan manifest."""
    try:
        doc = kdl.parse(path.read_text(encoding="utf-8"))
    except OSError as e:
        raise ManifestError(f"cannot read manifest: {e}") from e
    except Exception as e:  # kdl.errors don't share a public base class
        raise ManifestError(f"invalid KDL in {path}: {e}") from e

    meta = doc.get("meta")
    if meta is None:
        raise ManifestError("manifest has no meta block")
    meta_vals: dict[str, object] = {n.name: _first_arg(n) for n in meta.nodes}

    schema = _to_int(meta_vals.get("schema-version"))
    if schema != SCHEMA_VERSION:
        raise ManifestError(
            f"unsupported schema-version {schema} (expected {SCHEMA_VERSION})"
        )
    try:
        kind = MediaKind(str(meta_vals.get("kind")))
    except ValueError as e:
        raise ManifestError(f"unknown manifest kind {meta_vals.get('kind')!r}") from e

    manifest = PlanManifest(
        kind=kind,
        created=str(meta_vals.get("created") or ""),
        source_mode=str(meta_vals.get("source-mode") or ""),
        dest_root=str(meta_vals.get("dest-root") or ""),
        schema_version=schema,
    )
    if not manifest.dest_root:
        raise ManifestError("manifest meta has no dest-root")

    for node in doc.getAll(kind.block_name):
        block = TitleBlock(raw_title=str(_first_arg(node)))
        for child in node.nodes:
            name = child.name
            if name == "title":
                block.title = str(_first_arg(child))
            elif name == "original-title":
                block.original_title = str(_first_arg(child))
            elif name == "year":
                block.year = _to_int(_first_arg(child, 0))
            elif name == "tmdb":
                block.tmdb_id = _to_int(_first_arg(child, 0)) or None
            elif name == "tvdb":
                block.tvdb_id = _to_int(_first_arg(child, 0)) or None
            elif name == "imdb":
                block.imdb_id = str(_first_arg(child))
            elif name == "edition":
                block.edition = str(_first_arg(child))
            elif name == "confidence":
                block.confidence = _enum_field(
                    Confidence, str(_first_arg(child)), "confidence", path
                )
            elif name == "cross-check":
                block.cross_check = _enum_field(
                    CrossCheck, str(_first_arg(child)), "cross-check", path
                )
                block.cross_check_note = str(child.props.get("note") or "")
            elif name == "note":
                block.note = str(_first_arg(child))
            elif name == "dest-dir":
                block.dest_dir = str(_first_arg(child))
            elif name == "candidate":
                block.candidates.append(
                    SearchCandidate(
                        provider=_enum_field(
                            MetadataProvider,
                            str(child.props.get("provider")),
                            "candidate provider",
                            path,
                        ),
                        id=_to_int(child.props.get("id")),
                        title=str(child.props.get("name") or ""),
                        year=_to_int(child.props.get("year")),
                    )
                )
            elif name == kind.entry_name:
                block.entries.append(_parse_entry(child, path))
        manifest.blocks.append(block)
    return manifest


def _enum_field[E: StrEnum](enum_cls: type[E], value: str, what: str, path: Path) -> E:
    """Coerce a manifest field to its enum, with a clean error on typos.

    Several of these fields are documented as hand-editable; a typo must
    surface as a ManifestError naming the field, never a raw traceback.
    """
    try:
        return enum_cls(value)
    except ValueError as e:
        allowed = ", ".join(m.value for m in enum_cls)
        raise ManifestError(
            f"{path}: bad {what} {value!r} (expected one of: {allowed})"
        ) from e


def _parse_entry(node: kdl.Node, path: Path) -> FileEntry:
    vals: dict[str, kdl.Node] = {n.name: n for n in node.nodes}

    def text(name: str) -> str:
        n = vals.get(name)
        return str(_first_arg(n)) if n else ""

    def num(name: str) -> int | None:
        n = vals.get(name)
        return _to_int(_first_arg(n, 0)) if n else None

    source = text("source")
    if not source:
        raise ManifestError(f"{path}: entry with no source")
    status = _enum_field(EntryStatus, text("status"), f"status for {source}", path)
    conflict_text = text("conflict")
    episodes_node = vals.get("episodes")
    return FileEntry(
        source=source,
        size=num("size") or 0,
        status=status,
        dest=text("dest"),
        season=num("season"),
        number=num("number"),
        episodes=[_to_int(a) for a in episodes_node.args] if episodes_node else [],
        title=text("title"),
        conflict=_enum_field(
            ConflictAction, conflict_text, f"conflict for {source}", path
        )
        if conflict_text
        else None,
        note=text("note"),
    )


# ---------------------------------------------------------------------------
# Plan pipeline
# ---------------------------------------------------------------------------


@dataclass
class PlanOptions:
    managed: bool = False  # --radarr / --sonarr
    downloads: bool = False  # --downloads (wired in by the downloads scanner)
    sources: list[Path] = field(default_factory=list)
    pattern: str = ""
    force: bool = False
    output: Path | None = None
    json_output: bool = False
    refine: Path | None = None
    no_cache: bool = False
    verbose: bool = False


def _say(opts_json: bool, message: str) -> None:
    """Human-facing progress output: stderr when stdout carries JSON."""
    print(message, file=sys.stderr if opts_json else sys.stdout)


# Tags ({tvdb-1} / {edition-X}) and bracketed alt titles are decoration on
# top of "Title (Year)" — strip them when matching against the library.
_RE_BRACE_TAG = re.compile(r"\s*\{[^}]*\}")
_RE_BRACKETED = re.compile(r"\s*\[[^\]]*\]")


def _normalize_dirname(name: str) -> str:
    return normalize_title(_RE_BRACKETED.sub("", _RE_BRACE_TAG.sub("", name)))


def _dest_dir_index(dest_root: Path) -> dict[str, str]:
    """Map normalized existing library directory names to actual names."""
    index: dict[str, str] = {}
    try:
        children = sorted(dest_root.iterdir())
    except OSError:
        return index
    for child in children:
        if child.is_dir():
            index.setdefault(_normalize_dirname(child.name), child.name)
    return index


def _existing_dest_dir(
    index: dict[str, str], titles: list[str], year: int
) -> str | None:
    """Find an existing library directory for this title, if any.

    The library predates provider ID tags, so an existing ``Title (Year)``
    directory (with or without tags or a bracketed alt title) is reused
    rather than creating a parallel tagged directory for the same title.
    """
    for title in titles:
        if not title:
            continue
        key = normalize_title(f"{title} ({year})") if year else normalize_title(title)
        if key in index:
            return index[key]
    return None


def _resolve_movie(
    scanned: ScannedTitle,
    block: TitleBlock,
    override_tmdb: int | None,
    providers: Providers,
) -> MovieInfo | None:
    """Resolve a movie against TMDB (primary), cross-check via TheTVDB."""
    tmdb_id = override_tmdb
    if tmdb_id is None:
        try:
            candidates = providers.tmdb_search_movie(
                scanned.title, scanned.year, providers.tmdb_key, providers.no_cache
            )
        except _PROVIDER_ERRORS as e:
            block.confidence = Confidence.NONE
            block.note = f"TMDB search failed: {e}"
            return None
        confidence, chosen = pick_candidate(
            candidates, scanned.title, scanned.alt_title, scanned.year
        )
        block.confidence = confidence
        if chosen is None:
            block.candidates = candidates[:5]
            return None
        tmdb_id = chosen.id
    else:
        block.confidence = Confidence.EXACT

    try:
        info = providers.tmdb_fetch_movie(
            tmdb_id, providers.tmdb_key, providers.no_cache
        )
    except _PROVIDER_ERRORS as e:
        block.confidence = Confidence.NONE
        block.note = f"TMDB fetch of {tmdb_id} failed: {e}"
        return None

    block.tmdb_id = info.tmdb_id
    block.imdb_id = info.imdb_id
    block.title = info.title
    if info.original_title != info.title:
        block.original_title = info.original_title
    block.year = info.year or scanned.year

    # Cross-check: does TheTVDB know a movie with this title and year?
    if not providers.tvdb_key:
        block.cross_check = CrossCheck.UNAVAILABLE
        return info
    try:
        tvdb_candidates = providers.tvdb_search_movies(
            info.title, providers.tvdb_key, providers.no_cache
        )
    except _PROVIDER_ERRORS:
        block.cross_check = CrossCheck.UNAVAILABLE
        return info
    _, tvdb_match = pick_candidate(
        tvdb_candidates, info.title, info.original_title, block.year
    )
    if tvdb_match is not None:
        block.cross_check = CrossCheck.OK
        block.tvdb_id = tvdb_match.id
    elif tvdb_candidates:
        block.cross_check = CrossCheck.MISMATCH
        best = tvdb_candidates[0]
        block.cross_check_note = (
            f"TheTVDB best match '{best.title} ({best.year})' does not match"
        )
    else:
        block.cross_check = CrossCheck.UNAVAILABLE
    return info


def _resolve_series(
    scanned: ScannedTitle,
    block: TitleBlock,
    override_tvdb: int | None,
    providers: Providers,
) -> AnimeInfo | None:
    """Resolve a series against TheTVDB (primary), cross-check via TMDB."""
    tvdb_id = override_tvdb
    if tvdb_id is None:
        try:
            candidates = providers.tvdb_search_series(
                scanned.title, providers.tvdb_key, providers.no_cache
            )
        except _PROVIDER_ERRORS as e:
            block.confidence = Confidence.NONE
            block.note = f"TheTVDB search failed: {e}"
            return None
        confidence, chosen = pick_candidate(
            candidates, scanned.title, scanned.alt_title, scanned.year
        )
        block.confidence = confidence
        if chosen is None:
            block.candidates = candidates[:5]
            return None
        tvdb_id = chosen.id
    else:
        block.confidence = Confidence.EXACT

    try:
        info = providers.tvdb_fetch_series(
            tvdb_id, providers.tvdb_key, providers.no_cache
        )
    except _PROVIDER_ERRORS as e:
        block.confidence = Confidence.NONE
        block.note = f"TheTVDB fetch of {tvdb_id} failed: {e}"
        return None

    block.tvdb_id = info.tvdb_id
    block.title = info.title_en or info.title_ja
    # title_ja carries TheTVDB's original-language translation (Japanese
    # for anime, Korean for K-dramas, the primary name for English
    # originals); TMDB's original_name fills the gap below when the
    # cross-check resolves.
    if info.title_ja and info.title_ja != block.title:
        block.original_title = info.title_ja
    block.year = info.year or scanned.year

    # Cross-check: TMDB's record for this series should point back at the
    # same TheTVDB ID via its external IDs.
    if not providers.tmdb_key:
        block.cross_check = CrossCheck.UNAVAILABLE
        return info
    try:
        tmdb_candidates = providers.tmdb_search_tv(
            block.title, block.year, providers.tmdb_key, providers.no_cache
        )
        _, tmdb_match = pick_candidate(tmdb_candidates, block.title, "", block.year)
        if tmdb_match is None:
            block.cross_check = CrossCheck.UNAVAILABLE
            return info
        tv_info = providers.tmdb_fetch_tv(
            tmdb_match.id, providers.tmdb_key, providers.no_cache
        )
    except _PROVIDER_ERRORS:
        block.cross_check = CrossCheck.UNAVAILABLE
        return info

    external_tvdb = tv_info.tvdb_id
    block.tmdb_id = tv_info.tmdb_id
    if not block.original_title and tv_info.original_title != block.title:
        block.original_title = tv_info.original_title
    if external_tvdb is None:
        block.cross_check = CrossCheck.UNAVAILABLE
    elif external_tvdb == tvdb_id:
        block.cross_check = CrossCheck.OK
    else:
        block.cross_check = CrossCheck.MISMATCH
        block.cross_check_note = (
            f"TMDB {tmdb_match.id} points at TheTVDB {external_tvdb}, not {tvdb_id}"
        )
    return info


def _episode_title(info: AnimeInfo, season: int, number: int | None) -> str:
    """Episode title lookup covering both regular episodes and specials."""
    if number is None:
        return ""
    for ep in info.episodes:
        if ep.season == season and ep.number == number:
            return ep.title_en or ep.title_romaji
    return ""


# Extras files from per-season directories carry a "Season N - " prefix
# that TheTVDB special titles don't.
_RE_SEASON_NAME_PREFIX = re.compile(r"^season\s+\d+\s*-\s*", re.IGNORECASE)


def _match_extra_to_special(
    info: AnimeInfo, stem: str, release_group: str = ""
) -> Episode | None:
    """The TheTVDB season-0 special an extras file is, if it is one.

    Many BD batches ship featurettes that TheTVDB tracks as specials
    (after-shows, panels, blooper reels); those belong in ``Specials/``
    under their special number, not in a ``Featurettes/`` directory.
    Matches by normalized title — exact, or the file's name opening the
    special's title word-for-word (TheTVDB often appends guest lists).
    Anything ambiguous or unmatched stays an extra.
    """
    name = _RE_SEASON_NAME_PREFIX.sub("", extra_display_name(stem, release_group))
    target = normalize_title(name)
    if len(target.replace(" ", "")) < 8:
        return None  # short generic names match far too easily
    matches = [
        ep
        for ep in info.episodes
        if ep.season == 0
        and ep.title_en
        and word_prefix(normalize_title(ep.title_en), target)
    ]
    return matches[0] if len(matches) == 1 else None


def _dest_size_index(dest_root: Path) -> dict[int, list[Path]]:
    """Map file size → existing library videos, in one walk of the tree.

    The library predates this tool's naming, so an already-ingested copy
    usually exists under a *different* name — sometimes in the title's
    own directory, sometimes elsewhere (a trilogy box dir, an alternate
    naming). One size-keyed index answers both questions for every
    planned entry without re-listing destination directories.
    """
    index: dict[int, list[Path]] = {}
    for path in iter_media_files([dest_root]):
        try:
            index.setdefault(path.stat().st_size, []).append(path)
        except OSError:
            continue
    return index


def _check_entry_placement(
    entry: FileEntry, dest_root: Path, dest_dir: str, size_index: dict[int, list[Path]]
) -> None:
    """Plan-time checks shared by all entries: length limits, conflicts."""
    dest_path = dest_root / dest_dir / entry.dest
    name_bytes = len(dest_path.name.encode("utf-8"))
    if name_bytes > _MAX_FILENAME_BYTES:
        entry.status = EntryStatus.SKIP
        entry.note = (
            f"destination filename is {name_bytes} bytes (max {_MAX_FILENAME_BYTES})"
        )
        return
    if dest_path.exists():
        entry.status = EntryStatus.CONFLICT
        entry.conflict = ConflictAction.KEEP
        try:
            existing = dest_path.stat().st_size
            entry.note = f"destination exists ({existing} bytes)"
        except OSError:
            entry.note = "destination exists"
        return
    twins = size_index.get(entry.size, []) if entry.size else []
    in_dir = next(
        (p for p in twins if p.parent == dest_path.parent and p.name != dest_path.name),
        None,
    )
    if in_dir is not None:
        # Point the entry at the existing file: "keep" records it as
        # ingested, "replace" re-encodes it in place, "both" keeps both.
        entry.status = EntryStatus.CONFLICT
        entry.conflict = ConflictAction.KEEP
        entry.dest = str(in_dir.relative_to(dest_root / dest_dir))
        entry.note = f"same-size file already in library: {in_dir.name}"
        return
    if twins:
        # The copy lives outside this title's directory, so the entry
        # can't point at it — surface it and let the curator decide.
        entry.status = EntryStatus.SKIP
        entry.note = (
            f"same-size file already in library: {twins[0].relative_to(dest_root)}"
        )


def _build_movie_block(
    scanned: ScannedTitle,
    block: TitleBlock,
    info: MovieInfo | None,
    dest_root: Path,
    dest_index: dict[str, str],
    size_index: dict[int, list[Path]],
    providers: Providers,
) -> None:
    if info is None:
        for f in scanned.files:
            block.entries.append(_needs_id_entry(f))
        return

    titles = [block.title, info.original_title, *info.aliases]
    existing = _existing_dest_dir(dest_index, titles, block.year)
    block.dest_dir = existing or format_movie_dirname(
        block.title,
        block.year,
        block.tmdb_id,
        block.edition,
        original_title=block.original_title,
    )
    if existing and not block.note:
        block.note = "reusing existing library directory"

    group = _block_release_group(scanned)
    versions: list[FileEntry] = []
    for f in scanned.files:
        if f.extra_category:
            entry = _extra_entry(f, group, dest_root, block.dest_dir, size_index)
            block.entries.append(entry)
            continue
        _analyze(f.source, providers)
        base = block.dest_dir if f.part is None else f"{block.dest_dir} - pt{f.part}"
        entry = FileEntry(
            source=str(f.source.path),
            size=_size_of(f.source.path),
            status=EntryStatus.READY,
            dest=format_movie_filename(base, f.source),
        )
        _check_entry_placement(entry, dest_root, block.dest_dir, size_index)
        block.entries.append(entry)
        if f.part is None:
            versions.append(entry)
    _skip_additional_versions(versions)


def _block_release_group(scanned: ScannedTitle) -> str:
    """The release group of the title's main files, for extras cleanup.

    A torrent's extras carry its group as a ``-Suffix``; knowing the
    group lets :func:`extra_display_name` strip exactly it and nothing
    else (titles ending in hyphenated names keep their last word).
    """
    for f in scanned.files:
        if not f.extra_category and f.source.parsed.release_group:
            return f.source.parsed.release_group
    return ""


def _extra_entry(
    f: ScannedFile,
    release_group: str,
    dest_root: Path,
    dest_dir: str,
    size_index: dict[int, list[Path]],
) -> FileEntry:
    """Plan one extras file into its Plex/Jellyfin extras subdirectory.

    Extras keep clean names — both servers display the filename as the
    extra's title — and skip the quality block.
    """
    ext = f.source.path.suffix or ".mkv"
    display = extra_display_name(f.source.path.stem, release_group)
    entry = FileEntry(
        source=str(f.source.path),
        size=_size_of(f.source.path),
        status=EntryStatus.READY,
        dest=str(Path(f.extra_category) / f"{display}{ext}"),
    )
    _check_entry_placement(entry, dest_root, dest_dir, size_index)
    return entry


def _skip_additional_versions(versions: list[FileEntry]) -> None:
    """Default additional encodes of one title to ``skip``.

    A block can collect several distinct encodes of the same film or
    episode (a managed copy plus stray re-encodes, HD/SD pairs). Stacking
    versions is a curation decision, so only one copy stays actionable:
    an existing library copy if there is one, else the first new encode
    (the managed tree scans first). The rest are skipped with a note;
    flip them back to ``ready`` to keep multiple versions deliberately.
    """
    present = [
        e
        for e in versions
        if e.status is EntryStatus.CONFLICT or "already in library" in e.note
    ]
    ready = [e for e in versions if e.status is EntryStatus.READY]
    keeper = None if present else (ready[0] if ready else None)
    for e in ready:
        if e is keeper:
            continue
        e.status = EntryStatus.SKIP
        if present:
            e.note = "additional version; an existing copy is already in the library"
        else:
            assert keeper is not None
            e.note = f"additional version of {Path(keeper.source).name}"


def _build_series_block(
    scanned: ScannedTitle,
    block: TitleBlock,
    info: AnimeInfo | None,
    dest_root: Path,
    dest_index: dict[str, str],
    size_index: dict[int, list[Path]],
    providers: Providers,
) -> None:
    if info is None:
        for f in scanned.files:
            block.entries.append(_needs_id_entry(f))
        return

    titles = [block.title, block.original_title, info.title_ja, *info.aliases]
    existing = _existing_dest_dir(dest_index, titles, block.year)
    block.dest_dir = existing or format_tv_series_dirname(
        block.title, block.year, block.tvdb_id, original_title=block.original_title
    )
    if existing and not block.note:
        block.note = "reusing existing library directory"

    group = _block_release_group(scanned)
    for f in scanned.files:
        if f.extra_category:
            # A featurette TheTVDB tracks as a season-0 special gets its
            # special number and the standard episode naming; the rest
            # land in their extras subdirectory.
            special = _match_extra_to_special(info, f.source.path.stem, group)
            if special is not None:
                f = dc_replace(
                    f,
                    season=0,
                    episode=special.number,
                    episodes=[],
                    episode_title="",
                    extra_category="",
                )
            else:
                entry = _extra_entry(f, group, dest_root, block.dest_dir, size_index)
                block.entries.append(entry)
                continue
        season = f.season if f.season is not None else 1
        entry = FileEntry(
            source=str(f.source.path),
            size=_size_of(f.source.path),
            status=EntryStatus.READY,
            season=season,
            number=f.episode,
            episodes=list(f.episodes),
            title=_episode_title(info, season, f.episode) or f.episode_title,
        )
        if f.episode is None:
            entry.status = EntryStatus.NEEDS_ID
            entry.note = "could not determine episode number"
            block.entries.append(entry)
            continue
        _analyze(f.source, providers)
        subdir = season_subdir(Path(), season).name
        filename = format_tv_episode_filename(
            block.title,
            block.year,
            season,
            f.episode,
            entry.title,
            f.source,
            episodes=f.episodes or None,
        )
        entry.dest = str(Path(subdir) / filename)
        _check_entry_placement(entry, dest_root, block.dest_dir, size_index)
        block.entries.append(entry)

    # One copy per episode, like movie versions: HD/SD pairs and repeated
    # grabs of the same episode default to skip.
    by_episode: dict[tuple[int | None, object], list[FileEntry]] = {}
    for entry in block.entries:
        if entry.number is None:
            continue
        key = (entry.season, tuple(entry.episodes) or entry.number)
        by_episode.setdefault(key, []).append(entry)
    for episode_entries in by_episode.values():
        _skip_additional_versions(episode_entries)


def _needs_id_entry(f: ScannedFile) -> FileEntry:
    return FileEntry(
        source=str(f.source.path),
        size=_size_of(f.source.path),
        status=EntryStatus.NEEDS_ID,
        season=f.season,
        number=f.episode,
        episodes=list(f.episodes),
        title=f.episode_title,
    )


def _size_of(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _analyze(sf: SourceFile, providers: Providers) -> None:
    """Run mediainfo on a source, degrading to no-metadata on failure.

    One unreadable file (permission denied, mid-seed, corrupt) must
    never abort a whole plan; mediainfo failures surface as a missing
    quality block, not a crash. SubprocessError covers the mediainfo
    binary exiting non-zero, which _PROVIDER_ERRORS does not.
    """
    if sf.media is None:
        try:
            sf.media = providers.analyze(sf.path)
        except (*_PROVIDER_ERRORS, subprocess.SubprocessError):
            sf.media = None


def _load_refine(
    path: Path, kind: MediaKind
) -> tuple[dict[str, TitleBlock], dict[str, FileEntry]]:
    """Carry forward IDs and per-entry decisions from a previous manifest."""
    previous = parse_plan_manifest(path)
    if previous.kind != kind:
        raise ManifestError(f"--refine manifest is for {previous.kind}, not {kind}")
    blocks = {b.raw_title: b for b in previous.blocks}
    entries = {e.source: e for b in previous.blocks for e in b.entries}
    return blocks, entries


def _resolve_title(
    t: ScannedTitle,
    block: TitleBlock,
    *,
    mapped_id: int | None,
    refined_id: int | None,
    arr_id: int | None,
    resolver: Callable[..., MovieInfo | AnimeInfo | None],
    providers: Providers,
    dest_index: dict[str, str],
) -> tuple[MovieInfo | AnimeInfo | None, bool]:
    """Run the ID-resolution ladder for one scanned title.

    Precedence: config mapping → ``--refine`` manifest ID → the
    Radarr/Sonarr record → provider search inside *resolver*; an
    ambiguous search gets one retry with the candidate that matches an
    existing library directory. Returns the resolved info and whether
    the Radarr/Sonarr record supplied the ID.
    """
    override = mapped_id or refined_id
    arr_used = override is None and arr_id is not None
    if arr_used:
        override = arr_id
    info = resolver(t, block, override, providers)
    if info is None and block.confidence is Confidence.AMBIGUOUS:
        pick = _library_pick(block.candidates, dest_index)
        if pick is not None:
            info = resolver(t, block, pick.id, providers)
            if info is not None:
                _note_library_pick(block)
    return info, arr_used


def _fetch_arr_index(
    name: str,
    url: str,
    fetch: Callable[..., dict[str, arr.ArrEntry]],
    api_key: str,
    degraded: str,
    json_output: bool,
) -> dict[str, arr.ArrEntry]:
    """Fetch one PVR tool's index, degrading to empty with a warning."""
    if not url or not api_key:
        return {}
    try:
        return fetch(url, api_key)
    except _PROVIDER_ERRORS as e:
        _say(json_output, f"warning: {name} query failed ({e}); {degraded}")
        return {}


def run_plan(
    kind: MediaKind,
    config: MediaIngestConfig,
    opts: PlanOptions,
    providers: Providers | None = None,
) -> int:
    """Scan, resolve, and write a plan manifest. Read-only on the library."""
    providers = providers or Providers()

    if kind is MediaKind.MOVIE:
        source_root = config.movies_source_dir
        dest_root = config.movies_dest_dir
        mappings = config.movie_mappings
    else:
        source_root = config.television_source_dir
        dest_root = config.television_dest_dir
        mappings = config.series_mappings
    # anime-ingest convention: --source values are the downloads dirs in
    # downloads mode, and the first value also overrides the managed root.
    downloads_dirs = opts.sources or [config.downloads_dir]
    if opts.sources:
        source_root = opts.sources[0]

    # Fail fast before any provider calls or scanning.
    problems = []
    if opts.managed and not source_root.is_dir():
        problems.append(f"source directory not found: {source_root}")
    if opts.downloads and not any(d.is_dir() for d in downloads_dirs):
        problems.append(f"downloads directory not found: {downloads_dirs[0]}")
    if not dest_root.is_dir():
        problems.append(f"destination directory not found: {dest_root}")
    # Only require the binary when the real analyzer will run (tests inject).
    if providers.analyze is analyze_file and shutil.which("mediainfo") is None:
        problems.append("mediainfo not found on PATH")
    if problems:
        for p in problems:
            print(f"error: {p}", file=sys.stderr)
        return 1

    refine_blocks: dict[str, TitleBlock] = {}
    refine_entries: dict[str, FileEntry] = {}
    if opts.refine is not None:
        try:
            refine_blocks, refine_entries = _load_refine(opts.refine, kind)
        except ManifestError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1

    # mediainfo dominates plan time and its inputs are immutable, so the
    # real analyzer runs behind a persistent cache (test stubs bypass it).
    if providers.analyze is analyze_file:
        providers = dc_replace(providers, analyze=analyze_file_cached)

    # Radarr/Sonarr know the provider IDs (and domains) of everything
    # they manage — this kind's index feeds both the foreign-domain
    # filter and ID resolution. The other tool's index matters for the
    # domain filter only: without it, Radarr-managed anime movies leak
    # into television plans (and vice versa).
    radarr = ("Radarr", config.radarr_url, providers.radarr_fetch)
    sonarr = ("Sonarr", config.sonarr_url, providers.sonarr_fetch)
    own, cross = (radarr, sonarr) if kind is MediaKind.MOVIE else (sonarr, radarr)
    arr_name = own[0]
    arr_index = _fetch_arr_index(
        *own, providers.arr_key, "falling back to search", opts.json_output
    )
    cross_index = _fetch_arr_index(
        *cross,
        providers.cross_arr_key,
        "cross-domain exclusion degraded",
        opts.json_output,
    )

    managed_titles: list[ScannedTitle] = []
    downloads_titles: list[ScannedTitle] = []
    modes: list[str] = []
    if opts.managed:
        managed_titles = scan_managed_tree(source_root, kind)
        modes.append(kind.managed_mode)
    if opts.downloads:
        downloads_titles = scan_downloads(downloads_dirs, kind)
        modes.append("downloads")

    # The downloads directory is shared with the anime pipeline; titles
    # another domain owns are not this command's to plan.
    if downloads_titles:
        domain_index = {**cross_index, **arr_index}
        downloads_titles, foreign = _drop_foreign_domain(
            downloads_titles, domain_index, kind.domain, downloads_dirs, mappings
        )
        if foreign:
            _say(
                opts.json_output,
                f"{foreign} downloads title(s) excluded as foreign-domain",
            )

    scanned: list[ScannedTitle] = managed_titles + downloads_titles
    if opts.managed and opts.downloads:
        twins = _drop_hardlink_twins(scanned)
        if twins:
            _say(
                opts.json_output,
                f"{twins} hardlinked duplicate source(s) skipped"
                " (managed tree and downloads share the file)",
            )

    # --refine narrows the plan to the manifest's contents: deleting a
    # block or entry from the manifest deletes it from the plan. Files
    # that arrived since the original plan wait for the next fresh plan.
    if opts.refine is not None:
        dropped = 0
        for t in scanned:
            kept_files = [f for f in t.files if str(f.source.path) in refine_entries]
            dropped += len(t.files) - len(kept_files)
            t.files = kept_files
        scanned = [t for t in scanned if t.files]
        if dropped:
            _say(
                opts.json_output,
                f"{dropped} source(s) outside the refine manifest dropped",
            )

    if opts.pattern:
        needle = opts.pattern.lower()
        scanned = [t for t in scanned if needle in t.raw_title.lower()]

    register = load_register()
    already_ingested = 0
    if not opts.force:
        for t in scanned:
            kept_files = []
            for f in t.files:
                if str(f.source.path.resolve()) in register:
                    already_ingested += 1
                else:
                    kept_files.append(f)
            t.files = kept_files
        scanned = [t for t in scanned if t.files]

    manifest = PlanManifest(
        kind=kind,
        created=datetime.now(UTC).isoformat(timespec="seconds"),
        source_mode="+".join(modes),
        dest_root=str(dest_root),
    )

    if not scanned:
        _say(opts.json_output, f"nothing to do ({already_ingested} already ingested)")
        if opts.json_output:
            print(json.dumps(_plan_summary(manifest, None, already_ingested)))
        return 2

    dest_index = _dest_dir_index(dest_root)
    size_index = _dest_size_index(dest_root)

    # Phase 1: resolve every scanned title to provider IDs.
    resolved_titles: list[
        tuple[ScannedTitle, TitleBlock, MovieInfo | AnimeInfo | None]
    ] = []
    for t in scanned:
        block = TitleBlock(raw_title=t.raw_title)
        mapping = lookup_mapping(
            mappings, t.raw_title, f"{t.title} ({t.year})", t.title
        )
        refined = refine_blocks.get(t.raw_title)
        arr_entry = arr.lookup(arr_index, t.raw_title, t.title, t.year)

        if mapping and mapping.edition and not t.edition:
            t.edition = mapping.edition
        block.edition = t.edition

        if kind is MediaKind.MOVIE:
            info, arr_used = _resolve_title(
                t,
                block,
                mapped_id=mapping.tmdb_id if mapping else None,
                refined_id=refined.tmdb_id if refined else None,
                arr_id=arr_entry.tmdb_id if arr_entry else None,
                resolver=_resolve_movie,
                providers=providers,
                dest_index=dest_index,
            )
        else:
            info, arr_used = _resolve_title(
                t,
                block,
                mapped_id=mapping.tvdb_id if mapping else None,
                refined_id=refined.tvdb_id if refined else None,
                arr_id=arr_entry.tvdb_id if arr_entry else None,
                resolver=_resolve_series,
                providers=providers,
                dest_index=dest_index,
            )

        if arr_used and info is not None and not block.note:
            block.note = f"resolved via {arr_name}"
        resolved_titles.append((t, block, info))

    # Phase 2: one title, one block — the same film often arrives via both
    # the managed tree and the downloads directory.
    _merge_same_title_blocks(resolved_titles, kind)

    # Phase 3: compute destinations and entries.
    for t, block, info in resolved_titles:
        if kind is MediaKind.MOVIE:
            movie_info = info if isinstance(info, MovieInfo) else None
            _build_movie_block(
                t, block, movie_info, dest_root, dest_index, size_index, providers
            )
        else:
            series_info = info if isinstance(info, AnimeInfo) else None
            _build_series_block(
                t, block, series_info, dest_root, dest_index, size_index, providers
            )

        # Carry forward decisions an agent made on a previous manifest.
        for entry in block.entries:
            prior = refine_entries.get(entry.source)
            if prior is None:
                continue
            if prior.status is EntryStatus.SKIP:
                entry.status = EntryStatus.SKIP
            if prior.conflict is not None and entry.status is EntryStatus.CONFLICT:
                entry.conflict = prior.conflict
        manifest.blocks.append(block)

        if opts.verbose:
            ids = " ".join(
                f"{{{tag}-{value}}}"
                for tag, value in (("tvdb", block.tvdb_id), ("tmdb", block.tmdb_id))
                if value
            )
            _say(
                opts.json_output,
                f"{t.raw_title}: {block.confidence}"
                + (f" {ids}" if ids else "")
                + f", {len(block.entries)} file(s) -> {block.dest_dir or '(unresolved)'}",
            )

    _mark_duplicate_dests(manifest)
    save_cache()  # persist newly analyzed mediainfo results

    out_path = opts.output or Path(
        f"{kind.tool}-plan-{datetime.now(UTC).strftime('%Y%m%d-%H%M%S')}.kdl"
    )
    write_plan_manifest(manifest, out_path)

    summary = _plan_summary(manifest, out_path, already_ingested)
    if opts.json_output:
        print(json.dumps(summary, ensure_ascii=False))
    counts = summary["counts"]
    _say(
        opts.json_output,
        f"plan written to {out_path}: {counts['ready']} ready,"
        f" {counts['needs_id']} needs-id, {counts['conflict']} conflict,"
        f" {counts['skip']} skip, {already_ingested} already ingested",
    )
    for w in summary["warnings"]:
        _say(opts.json_output, f"warning: {w['title']}: {w['detail']}")
    return 0


_RE_TRAILING_YEAR = re.compile(r"\s*\(\d{4}\)\s*$")


def _anime_tree_names() -> set[str]:
    """Normalized series-folder names from the anime managed tree.

    Sonarr creates a folder there for every anime it manages, so the
    tree's top level is a ready-made list of anime-domain titles even
    when the Sonarr API isn't configured. Each name is indexed with and
    without its year, since downloads groups often parse without one.
    """
    names: set[str] = set()
    try:
        for child in anime_source_dir().iterdir():
            if child.is_dir():
                names.add(_normalize_dirname(child.name))
                names.add(_normalize_dirname(_RE_TRAILING_YEAR.sub("", child.name)))
    except OSError:
        pass
    return names


def _drop_foreign_domain(
    titles: list[ScannedTitle],
    arr_index: dict[str, arr.ArrEntry],
    own_root: str,
    roots: list[Path],
    mappings: dict[str, TitleMapping] | None = None,
) -> tuple[list[ScannedTitle], int]:
    """Drop downloads titles that belong to another domain's pipeline.

    A ``domain`` property on a config mapping is authoritative in both
    directions — it covers titles no index can place (Sonarr knows the
    Hana yori Dango anime only as "Boys Over Flowers") and protects
    same-name titles from false drops. Next signal: the Radarr/Sonarr
    record's root folder (a series rooted in ``anime`` is not
    television's to plan; alternate titles cover romaji fansub names).
    Fallback/supplement: a title matching a series folder in the anime
    managed tree. Never consults AniDB, so it is safe to run during
    AniDB-heavy operations.

    A torrent is a single-publisher unit, so groups whose files all live
    in torrent directories that foreign titles came from are foreign too
    — creditless/extra files parse to junk titles ("Show NCOP1") that no
    index can match, but they ship inside the show's own torrent.
    """
    foreign_tree = _anime_tree_names()
    phrases = arr.loose_phrases(arr_index)  # once, not per title
    kept: list[ScannedTitle] = []
    dropped: list[ScannedTitle] = []
    for t in titles:
        mapping = lookup_mapping(mappings or {}, t.raw_title, t.title, t.alt_title)
        if mapping is not None and mapping.domain:
            (kept if mapping.domain == own_root else dropped).append(t)
            continue
        domain = arr.domain_of(
            arr_index, t.raw_title, t.title, t.alt_title, t.year, phrases
        )
        in_foreign_tree = any(
            _normalize_dirname(name) in foreign_tree
            for name in (
                f"{t.title} ({t.year})" if t.year else "",
                t.title,
                t.alt_title,
            )
            if name
        )
        if (domain is not None and domain != own_root) or in_foreign_tree:
            dropped.append(t)
        else:
            kept.append(t)

    foreign_dirs = {
        d for t in dropped for f in t.files if (d := _torrent_dir(f.source.path, roots))
    }
    if foreign_dirs:
        survivors: list[ScannedTitle] = []
        for t in kept:
            dirs = [_torrent_dir(f.source.path, roots) for f in t.files]
            if dirs and all(d is not None and d in foreign_dirs for d in dirs):
                dropped.append(t)
            else:
                survivors.append(t)
        kept = survivors
    return kept, len(dropped)


def _drop_hardlink_twins(scanned: list[ScannedTitle]) -> int:
    """Drop sources that are hardlinks of an earlier-scanned file.

    Radarr/Sonarr import by hardlinking, so a still-seeding download is
    the same inode as its managed-tree copy and would otherwise show up
    twice (the downloads side usually with a garbled torrent name). The
    managed tree scans first, so it claims the inode. Returns the number
    of twins dropped; *scanned* is pruned in place.
    """
    seen: dict[tuple[int, int], Path] = {}
    twins = 0
    for t in scanned:
        kept: list[ScannedFile] = []
        for f in t.files:
            try:
                st = f.source.path.stat()
            except OSError:
                kept.append(f)
                continue
            key = (st.st_dev, st.st_ino)
            first = seen.setdefault(key, f.source.path)
            if first == f.source.path:
                kept.append(f)
            else:
                twins += 1
        t.files = kept
    scanned[:] = [t for t in scanned if t.files]
    return twins


def _library_pick(
    candidates: list[SearchCandidate], dest_index: dict[str, str]
) -> SearchCandidate | None:
    """Resolve an ambiguous search against the library itself.

    When exactly one candidate's ``Title (Year)`` matches an existing
    destination directory, that is the title this collection means —
    e.g. several films named "After Hours", but the library already has
    an ``After Hours (1985)`` directory.
    """
    hits: dict[int, SearchCandidate] = {}
    for c in candidates:
        for name in (c.title, c.original_title):
            if name and _existing_dest_dir(dest_index, [name], c.year):
                hits.setdefault(c.id, c)
                break
    if len(hits) == 1:
        return next(iter(hits.values()))
    return None


def _note_library_pick(block: TitleBlock) -> None:
    """Mark a block as resolved by matching an existing library directory."""
    block.candidates = []
    block.confidence = Confidence.HIGH
    block.note = "disambiguated by existing library directory"


def _merge_same_title_blocks(
    items: list[tuple[ScannedTitle, TitleBlock, MovieInfo | AnimeInfo | None]],
    kind: MediaKind,
) -> None:
    """Merge blocks that resolved to the same provider ID, in place.

    The same film often arrives via both the managed tree and the
    downloads directory — Radarr names its copy blandly while the torrent
    carries edition markers and packs featurettes. One title must mean
    one block, or the film and its extras land in different directories.
    The first block (the managed tree scans first) absorbs the later
    ones' files and adopts their edition when it has none of its own;
    blocks with genuinely different editions stay separate — different
    editions are different directories by design.
    """
    kept_by_id: dict[int, tuple[ScannedTitle, TitleBlock]] = {}
    merged: list[tuple[ScannedTitle, TitleBlock, MovieInfo | AnimeInfo | None]] = []
    for t, block, info in items:
        key = block.tmdb_id if kind is MediaKind.MOVIE else block.tvdb_id
        if info is None or not key:
            merged.append((t, block, info))
            continue
        kept = kept_by_id.get(key)
        if kept is None:
            kept_by_id[key] = (t, block)
            merged.append((t, block, info))
            continue
        kept_t, kept_block = kept
        if kept_block.edition and block.edition and kept_block.edition != block.edition:
            merged.append((t, block, info))
            continue
        kept_t.files.extend(t.files)
        if block.edition and not kept_block.edition:
            kept_block.edition = block.edition
    items[:] = merged


def _dest_key(dest_dir: str, dest: str) -> tuple[str, str]:
    """Collision key for a destination, shared by the plan and apply checks.

    Casefolded so colliders are caught even on case-insensitive mounts.
    """
    return (dest_dir.casefold(), dest.casefold())


def _mark_duplicate_dests(manifest: PlanManifest) -> None:
    """Skip entries whose destination collides with an earlier entry.

    Two sources can compute the same destination (two versions of one
    movie or episode whose quality blocks come out identical). The
    filesystem conflict check can't see this — neither destination exists
    yet — and at apply time the second copy would silently land on top of
    the first, so collisions are surfaced for manual resolution instead.
    """
    seen: dict[tuple[str, str], str] = {}
    for block in manifest.blocks:
        for entry in block.entries:
            if entry.status in (EntryStatus.SKIP, EntryStatus.NEEDS_ID):
                continue
            if not entry.dest:
                continue
            first = seen.setdefault(_dest_key(block.dest_dir, entry.dest), entry.source)
            if first != entry.source:
                entry.status = EntryStatus.SKIP
                entry.conflict = None
                entry.note = f"destination duplicates {first} — rename or remove one"


def _plan_summary(
    manifest: PlanManifest, out_path: Path | None, already_ingested: int
) -> dict:
    counts = {"ready": 0, "needs_id": 0, "conflict": 0, "skip": 0}
    entries = []
    warnings = []
    for block in manifest.blocks:
        if block.cross_check is CrossCheck.MISMATCH:
            warnings.append(
                {
                    "kind": "cross-check-mismatch",
                    "title": block.raw_title,
                    "detail": block.cross_check_note,
                }
            )
        if block.confidence in (Confidence.AMBIGUOUS, Confidence.NONE) and block.note:
            warnings.append(
                {"kind": "unresolved", "title": block.raw_title, "detail": block.note}
            )
        for e in block.entries:
            counts[e.status.replace("-", "_")] += 1
            entries.append(
                {
                    "source": e.source,
                    "status": str(e.status),
                    "dest": str(Path(block.dest_dir) / e.dest) if e.dest else "",
                    "title": block.raw_title,
                    "confidence": str(block.confidence),
                }
            )
    return {
        "tool": manifest.kind.tool,
        "action": "plan",
        "schema_version": manifest.schema_version,
        "manifest": str(out_path) if out_path else "",
        "counts": {**counts, "already_ingested": already_ingested},
        "warnings": warnings,
        "entries": entries,
    }


# ---------------------------------------------------------------------------
# Apply pipeline
# ---------------------------------------------------------------------------


@dataclass
class ApplyOptions:
    dry_run: bool = False
    json_output: bool = False
    verbose: bool = False
    sub_lang: str = "en"


def _validate_apply(manifest: PlanManifest) -> tuple[list[str], int]:
    """Validate a manifest against the live filesystem.

    Returns (problems, already_done). All violations are reported at
    once; apply copies nothing unless the whole manifest is clean.
    """
    problems: list[str] = []
    already_done = 0
    dest_root = Path(manifest.dest_root)
    if not dest_root.is_dir():
        problems.append(f"destination root not found: {dest_root}")
        return problems, 0

    seen_dests: dict[tuple[str, str], str] = {}
    for block in manifest.blocks:
        for e in block.entries:
            if e.status is EntryStatus.SKIP:
                continue
            if e.status is EntryStatus.NEEDS_ID:
                problems.append(
                    f"{e.source}: needs-id — set an ID and re-plan with"
                    " --refine, or set status to skip"
                )
                continue
            src = Path(e.source)
            if not src.is_file():
                problems.append(f"{e.source}: source file missing")
                continue
            src_size = src.stat().st_size
            if e.size and src_size != e.size:
                problems.append(
                    f"{e.source}: size changed since plan ({src_size} != {e.size})"
                )
                continue
            if not block.dest_dir or not e.dest:
                problems.append(f"{e.source}: entry has no destination")
                continue
            # Two entries placing the same destination would silently
            # stack at execution time (the second sees an existing dest).
            first = seen_dests.setdefault(_dest_key(block.dest_dir, e.dest), e.source)
            if first != e.source:
                problems.append(
                    f"{e.source}: destination duplicates {first}"
                    f" ({block.dest_dir}/{e.dest}) — keep one, mark the"
                    " other skip"
                )
                continue
            dest = dest_root / block.dest_dir / e.dest
            if dest.exists() and e.conflict is None:
                if dest.stat().st_size == e.size:
                    already_done += 1  # idempotent re-apply
                else:
                    problems.append(
                        f"{e.source}: destination appeared since plan"
                        f" ({dest}) — re-run plan"
                    )
    return problems, already_done


def run_apply(kind: MediaKind, manifest_path: Path, opts: ApplyOptions) -> int:
    """Validate and execute a plan manifest."""
    try:
        manifest = parse_plan_manifest(manifest_path)
    except ManifestError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if manifest.kind != kind:
        print(
            f"error: {manifest_path} is a {manifest.kind.tool} manifest;"
            f" use `{manifest.kind.cli} ingest apply`",
            file=sys.stderr,
        )
        return 1

    problems, _ = _validate_apply(manifest)
    if problems:
        for p in problems:
            print(f"error: {p}", file=sys.stderr)
        if opts.json_output:
            print(
                json.dumps(
                    {
                        "tool": kind.tool,
                        "action": "apply",
                        "schema_version": manifest.schema_version,
                        "manifest": str(manifest_path),
                        "ok": False,
                        "problems": problems,
                    },
                    ensure_ascii=False,
                )
            )
        return 1

    dest_root = Path(manifest.dest_root)
    register = load_register()
    results: list[dict] = []
    counts = {
        "copied": 0,
        "kept": 0,
        "replaced": 0,
        "both": 0,
        "skipped": 0,
        "failed": 0,
    }

    def record(entry: FileEntry, result: str, dest: Path | None) -> None:
        counts[result] += 1
        results.append(
            {
                "source": entry.source,
                "result": result,
                "dest": str(dest) if dest else "",
            }
        )
        if opts.verbose or result == "failed":
            _say(opts.json_output, f"{result}: {entry.source}")

    def place(entry: FileEntry, src: Path, dest: Path, result: str) -> None:
        if copy_reflink(src, dest, dry_run=opts.dry_run):
            copy_subtitle_sidecars(src, dest, opts.sub_lang, opts.dry_run, opts.verbose)
            register.add(str(src.resolve()))
            record(entry, result, dest)
        else:
            record(entry, "failed", dest)

    def replace(entry: FileEntry, src: Path, dest: Path) -> None:
        # Copy beside the existing file, then atomically swap, so a failed
        # copy never destroys the existing library file.
        tmp = dest.with_name(dest.name + ".etp-tmp")
        if copy_reflink(src, tmp, dry_run=opts.dry_run):
            if not opts.dry_run:
                os.replace(tmp, dest)
            copy_subtitle_sidecars(src, dest, opts.sub_lang, opts.dry_run, opts.verbose)
            register.add(str(src.resolve()))
            record(entry, "replaced", dest)
        else:
            tmp.unlink(missing_ok=True)
            record(entry, "failed", dest)

    # copy_reflink and the sidecar copier report progress on stdout; with
    # --json, stdout must stay pure JSON, so route their output to stderr.
    redirect = (
        contextlib.redirect_stdout(sys.stderr)
        if opts.json_output
        else contextlib.nullcontext()
    )
    with redirect:
        for block in manifest.blocks:
            for e in block.entries:
                if e.status is EntryStatus.SKIP:
                    record(e, "skipped", None)
                    continue
                src = Path(e.source)
                dest = dest_root / block.dest_dir / e.dest
                action = e.conflict

                if not dest.exists():
                    place(e, src, dest, "copied")
                elif action is None or action is ConflictAction.KEEP:
                    # Validation guarantees same-size when action is None.
                    register.add(str(src.resolve()))
                    record(e, "kept", dest)
                elif action is ConflictAction.SKIP:
                    record(e, "skipped", None)
                elif action is ConflictAction.REPLACE:
                    replace(e, src, dest)
                else:
                    # ConflictAction.BOTH: keep the existing file, place the
                    # new one alongside it disambiguated by CRC32 (the anime
                    # ingest convention).
                    crc = compute_crc32(src)
                    dest = crc_suffixed(dest, crc)
                    place(e, src, dest, "both")

    if not opts.dry_run:
        save_register(register)

    if opts.json_output:
        print(
            json.dumps(
                {
                    "tool": kind.tool,
                    "action": "apply",
                    "schema_version": manifest.schema_version,
                    "manifest": str(manifest_path),
                    "ok": counts["failed"] == 0,
                    "dry_run": opts.dry_run,
                    "counts": counts,
                    "entries": results,
                },
                ensure_ascii=False,
            )
        )
    _say(
        opts.json_output,
        f"apply{' (dry-run)' if opts.dry_run else ''}: "
        + ", ".join(f"{v} {k}" for k, v in counts.items() if v),
    )

    if counts["failed"]:
        return 1
    if not (counts["copied"] or counts["replaced"] or counts["both"]):
        return 2
    return 0
