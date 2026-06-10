"""Episode filename formatting and series directory naming."""

from __future__ import annotations

import re
import unicodedata
from pathlib import Path

from etp_lib.types import AudioTrack, SourceFile


def extras_relpath(path: Path) -> Path | None:
    """If *path* lives under a directory named ``Extras``, return its path
    relative to that directory (preserving any substructure); otherwise None.

    Shared by the triage scanner (to pull Extras videos out of the episode
    pool) and the manifest writer (to preserve ``Extras/`` subtree structure
    under the destination ``Extras/``) so both agree on what counts as an
    Extras subtree.
    """
    parts = path.parts
    lowered = [p.lower() for p in parts]
    if "extras" not in lowered:
        return None
    idx = lowered.index("extras")
    tail = parts[idx + 1 :]
    return Path(*tail) if tail else Path(path.name)


# Sidecar subtitle extensions recognized by Jellyfin/Plex/Emby (and imported
# by Sonarr/Radarr's "import extra files"). A sidecar sharing a video's base
# name is carried alongside the renamed destination video.
SUBTITLE_EXTENSIONS = frozenset(
    {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".sup"}
)


def subtitle_sidecars(
    source_video: Path,
    dest_video: Path,
    default_lang: str = "en",
) -> list[tuple[Path, Path]]:
    """Map subtitle sidecars next to *source_video* to destinations by *dest_video*.

    A sidecar is a file in the same directory whose name is the source video's
    base name followed by ``.<ext>`` (untagged) or ``.<tokens>.<ext>`` (tagged
    with a language and/or flags like ``en`` / ``en.forced`` / ``en.sdh``),
    where ``<ext>`` is a known subtitle extension. The ``.`` boundary after the
    base prevents ``Show - 01`` from matching ``Show - 011.srt``.

    Each match is mapped to a destination next to *dest_video*, named
    ``<dest-base>.<lang>[.<flags>].<ext>`` per Jellyfin's external-subtitle
    convention (which ShokoFin replicates verbatim into its VFS, keyed on the
    shared base name). Tagged sidecars keep their tokens; untagged sidecars get
    *default_lang* inserted. Returns ``(src, dst)`` pairs sorted by source name.
    """
    src_base = source_video.stem
    dest_base = dest_video.stem
    pairs: list[tuple[Path, Path]] = []
    try:
        siblings = source_video.parent.iterdir()
    except OSError:
        return []
    for cand in siblings:
        ext = cand.suffix.lower()
        if ext not in SUBTITLE_EXTENSIONS or not cand.is_file():
            continue
        if not cand.name.startswith(src_base):
            continue
        remainder = cand.name[len(src_base) :]  # ".srt" or ".en.forced.srt"
        if not remainder.startswith("."):  # boundary: skip "Show - 011.srt"
            continue
        # Tokens between base and extension: "" (untagged) or "en"/"en.forced".
        tokens = remainder.removesuffix(cand.suffix).removeprefix(".")
        dst = dest_video.parent / f"{dest_base}.{tokens or default_lang}{ext}"
        pairs.append((cand, dst))
    return sorted(pairs)


def unique_audio_codecs(tracks: list[AudioTrack]) -> list[str]:
    """Return deduplicated non-commentary audio codec names in order."""
    seen: set[str] = set()
    codecs: list[str] = []
    for t in tracks:
        if not t.is_commentary and t.codec not in seen:
            seen.add(t.codec)
            codecs.append(t.codec)
    return codecs


def build_metadata_block(source: SourceFile) -> str:
    """Build the [...] metadata block for an episode filename.

    Format: ``release-group source,(REMUX,)res,codec,...``
    The release group and source type are space-separated; all subsequent
    fields are comma-separated.
    """
    if source.media is None:
        return ""

    media = source.media

    # Prefix part: "group source" (space-separated)
    # Append version to release group: "MTBB" + v2 -> "MTBB(v2)"
    prefix_parts: list[str] = []
    if source.parsed.release_group:
        group = source.parsed.release_group
        if source.parsed.version is not None:
            group = f"{group}(v{source.parsed.version})"
        prefix_parts.append(group)
    # Default to "Web" when no source type detected -- ensures the space
    # separator between group and tech fields is always present
    source_type = source.parsed.source_type or "Web"
    prefix_parts.append(source_type)
    prefix = " ".join(prefix_parts)

    # Comma-separated technical metadata
    tech: list[str] = []

    # REMUX
    if source.parsed.is_remux:
        tech.append("REMUX")

    # Resolution
    if media.resolution:
        tech.append(media.resolution)

    # Video codec
    if media.video_codec:
        tech.append(media.video_codec)

    # HDR/UHD/DoVi
    if media.hdr_type:
        tech.append(media.hdr_type)

    # 10bit (always for HEVC, also for other codecs with 10-bit)
    if media.bit_depth >= 10:
        tech.append("10bit")

    # Encoding library (x264/x265) -- only when detected
    if media.encoding_lib:
        tech.append(media.encoding_lib)

    # Audio codecs and language detection
    non_commentary = [t for t in media.audio_tracks if not t.is_commentary]
    if non_commentary:
        codecs = unique_audio_codecs(media.audio_tracks)
        languages: set[str] = set()
        for t in non_commentary:
            if t.language:
                languages.add(t.language)
        tech.append("+".join(codecs))

        has_ja = "ja" in languages or "jpn" in languages
        has_en = "en" in languages or "eng" in languages
        other_langs = languages - {"ja", "jpn", "en", "eng"}

        if has_ja and has_en and other_langs:
            tech.append("multi-audio")
        elif has_ja and has_en:
            tech.append("dual-audio")

    tech_str = ",".join(tech)

    if prefix and tech_str:
        return f"{prefix},{tech_str}"
    return prefix or tech_str


def _sanitize_path(name: str) -> str:
    """Sanitize a string for use in file/directory names.

    Replaces ``/`` with `` - ``. A colon followed by a space is a
    title/subtitle separator and becomes `` - `` ("Hellboy II: The Golden
    Army" → "Hellboy II - The Golden Army", the library's convention);
    a bare colon is squeezed to ``-`` ("Re:ZERO" → "Re-ZERO"). The
    filesystem-safe colon stand-ins some release tools use (U+A789 ꞉,
    U+2236 ∶) are treated as colons.
    """
    name = name.replace("꞉", ":").replace("∶", ":")
    return name.replace("/", " - ").replace(": ", " - ").replace(":", "-")


def season_subdir(series_dir: Path, season: int, is_special: bool = False) -> Path:
    """Return the subdirectory for a season or Specials."""
    if is_special or season == 0:
        return series_dir / "Specials"
    return series_dir / f"Season {season:02d}"


# Extras subdirectory names recognized by BOTH Plex and Jellyfin inside a
# movie folder (Plex matches these exact title-case names; Jellyfin's
# folder matching is case-insensitive and its supported list includes all
# of them). Ordered keyword → subdirectory; first match wins.
_EXTRA_CATEGORIES: list[tuple[str, str]] = [
    ("trailer", "Trailers"),
    ("interview", "Interviews"),
    ("deleted", "Deleted Scenes"),
    ("behind the scenes", "Behind The Scenes"),
    ("behindthescenes", "Behind The Scenes"),
    ("making of", "Behind The Scenes"),
    ("short", "Shorts"),
]
_DEFAULT_EXTRA_CATEGORY = "Featurettes"

# Directory names that mark their contents as extras: the set both
# servers recognize, the generic "extras", and the anime creditless
# convention ("NC" dirs of NCOP/NCED files). Maps to the canonical
# category; "" means generic — classify each file by its own name.
_EXTRAS_DIR_NAMES: dict[str, str] = {
    "featurettes": "Featurettes",
    "behind the scenes": "Behind The Scenes",
    "deleted scenes": "Deleted Scenes",
    "interviews": "Interviews",
    "scenes": "Scenes",
    "shorts": "Shorts",
    "trailers": "Trailers",
    "other": "Other",
    "extras": "",
    "nc": "",
}


def extras_dir_category(dirname: str) -> str | None:
    """Canonical extras category for a directory name.

    Returns None when the directory is not an extras directory, and ""
    for the generic ``Extras`` (classify each file by its own name).
    """
    return _EXTRAS_DIR_NAMES.get(dirname.casefold())


_RE_RELEASE_GROUP_SUFFIX = re.compile(r"-[A-Za-z0-9]+$")

_RE_SAMPLE = re.compile(r"(?:^|[\W_])sample(?:[\W_]|$)", re.IGNORECASE)


def is_sample(stem: str) -> bool:
    """Torrent sample clips are junk to drop, not extras to keep."""
    return bool(_RE_SAMPLE.search(stem))


def extra_display_name(stem: str) -> str:
    """Clean a torrent extra's filename into its display title.

    Plex and Jellyfin show an extra's filename as its title, so release
    cruft must go: ``Crafting.Anomalisa-Grym`` → ``Crafting Anomalisa``.
    """
    name = _RE_RELEASE_GROUP_SUFFIX.sub("", stem)
    name = name.replace(".", " ").replace("_", " ")
    return _sanitize_path(" ".join(name.split()))


def classify_extra(stem: str) -> str:
    """Map an extra's name to its Plex/Jellyfin extras subdirectory."""
    name = extra_display_name(stem).casefold()
    for keyword, category in _EXTRA_CATEGORIES:
        if keyword in name:
            return category
    return _DEFAULT_EXTRA_CATEGORY


def crc_suffixed(dest: Path, crc: str) -> Path:
    """Disambiguate *dest* with a CRC32 suffix: ``name [ABCD1234].ext``.

    The keep-both convention shared by the anime manifest executor and
    the movies/television apply path.
    """
    return dest.parent / f"{dest.stem} [{crc}]{dest.suffix}"


def _format_episode_tag(season: int, episode: int, episodes: list[int] | None) -> str:
    """Format the sXeYY portion of an episode filename.

    For multi-episode files uses the Sonarr-style ``s1e02-e03`` range form
    (first and last episode), matching HamaTV/Plex conventions.
    """
    if episodes and len(episodes) > 1:
        first = episodes[0]
        last = episodes[-1]
        return f"s{season}e{first:02d}-e{last:02d}"
    return f"s{season}e{episode:02d}"


def format_episode_filename(
    concise_name: str,
    season: int,
    episode: int,
    episode_name: str,
    source: SourceFile,
    is_movie: bool = False,
    movie_dir_name: str = "",
    is_special: bool = False,
    special_tag: str = "",
    episodes: list[int] | None = None,
) -> str:
    """Build the full episode filename."""
    ext = source.path.suffix or ".mkv"
    metadata = build_metadata_block(source)
    meta_str = f" [{metadata}]" if metadata else ""
    hash_str = f" [{source.parsed.hash_code}]" if source.parsed.hash_code else ""

    concise_name = _sanitize_path(concise_name)
    episode_name = _sanitize_path(episode_name)
    movie_dir_name = _sanitize_path(movie_dir_name)

    if is_movie and not is_special:
        # Single-file movie: `DirName - complete movie [metadata] [hash].ext`
        return f"{movie_dir_name} - complete movie{meta_str}{hash_str}{ext}"

    if is_special:
        # Special: `Name - TAG - Episode Name [metadata] [hash].ext`
        if episode_name:
            return (
                f"{concise_name} - {special_tag} - "
                f"{episode_name}{meta_str}{hash_str}{ext}"
            )
        return f"{concise_name} - {special_tag}{meta_str}{hash_str}{ext}"

    # Regular episode: `Name - sXeYY - Episode Name [metadata] [hash].ext`
    ep_tag = _format_episode_tag(season, episode, episodes)
    if episode_name:
        return f"{concise_name} - {ep_tag} - {episode_name}{meta_str}{hash_str}{ext}"
    return f"{concise_name} - {ep_tag}{meta_str}{hash_str}{ext}"


# ---------------------------------------------------------------------------
# Movie / television naming (Plex conventions, Jellyfin-compatible)
# ---------------------------------------------------------------------------
#
# Provider IDs use Plex's curly-brace syntax ({tmdb-NNN}, {tvdb-NNN}) with
# only the primary provider's ID embedded: TMDB for movies, TheTVDB for
# television. Editions use Plex's {edition-Name} marker. The bracketed
# quality block is ignored by both Plex and Jellyfin during matching.


def _title_year(title: str, year: int) -> str:
    """``Title (Year)``, omitting the year suffix when it is unknown."""
    title = _sanitize_path(_strip_redundant_year(title, year))
    return f"{title} ({year})" if year else title


def normalize_title(text: str) -> str:
    """Normalize a title for comparison: casefolded alphanumeric words.

    Apostrophes are deleted (``Wolf's`` and ``Wolfs`` are the same
    title) and accents folded (``Fiancée``/``Fiancee``); all other
    punctuation becomes a word break. Shared by dual-title bracket
    suppression here and by candidate matching / library-directory
    reuse in video_ingest, so all title comparisons agree on what "the
    same title" means.
    """
    decomposed = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in decomposed if not unicodedata.combining(c))
    text = text.replace("'", "").replace("’", "")
    cleaned = "".join(c if c.isalnum() or c.isspace() else " " for c in text.casefold())
    return " ".join(cleaned.split())


def format_display_title(original_title: str, english_title: str) -> str:
    """``Original [English]`` when both exist and genuinely differ.

    Library convention: directory names lead with the original-language
    title, with the English title bracketed after it (the movie/TV
    counterpart of the anime ``JA [EN]`` convention). Falls back to
    whichever title exists; pairs that differ only in case or punctuation
    don't earn brackets.
    """
    original_title = _sanitize_path(original_title)
    english_title = _sanitize_path(english_title)
    if (
        original_title
        and english_title
        and normalize_title(original_title) != normalize_title(english_title)
    ):
        return f"{original_title} [{english_title}]"
    return english_title or original_title


def format_movie_dirname(
    title: str,
    year: int,
    tmdb_id: int | None,
    edition: str = "",
    original_title: str = "",
) -> str:
    """Build a movie directory name.

    ``Original [English] (Year) {tmdb-NNN} {edition-X}`` — the bracketed
    English title appears only when *original_title* genuinely differs.
    Plex recommends edition markers of at most 32 characters; longer ones
    are embedded as-is and left to plan-time validation to flag.
    """
    name = _title_year(format_display_title(original_title, title), year)
    if tmdb_id:
        name += f" {{tmdb-{tmdb_id}}}"
    if edition:
        name += f" {{edition-{_sanitize_path(edition)}}}"
    return name


def format_movie_filename(movie_dirname: str, source: SourceFile) -> str:
    """Build a movie filename: the directory name plus the quality block.

    Plex's movie naming wants the file named exactly after its folder;
    the bracketed metadata block and any release hash are appended after
    since bracketed text is ignored during matching.
    """
    ext = source.path.suffix or ".mkv"
    metadata = build_metadata_block(source)
    meta_str = f" [{metadata}]" if metadata else ""
    hash_str = f" [{source.parsed.hash_code}]" if source.parsed.hash_code else ""
    return f"{movie_dirname}{meta_str}{hash_str}{ext}"


def format_tv_series_dirname(
    title: str, year: int, tvdb_id: int | None, original_title: str = ""
) -> str:
    """Build a series directory name.

    ``Original [English] (Year) {tvdb-NNN}`` — the bracketed English title
    appears only when *original_title* genuinely differs.
    """
    name = _title_year(format_display_title(original_title, title), year)
    if tvdb_id:
        name += f" {{tvdb-{tvdb_id}}}"
    return name


def _format_padded_episode_tag(
    season: int, episode: int, episodes: list[int] | None
) -> str:
    """Zero-padded ``s01e02`` tag (multi-episode files: ``s01e02-e03``).

    Unlike the anime tag (``s1e02``), the season is zero-padded per the
    Plex/Jellyfin TV naming examples; specials use season 0 (``s00e05``).
    """
    if episodes and len(episodes) > 1:
        return f"s{season:02d}e{episodes[0]:02d}-e{episodes[-1]:02d}"
    return f"s{season:02d}e{episode:02d}"


def format_tv_episode_filename(
    series_title: str,
    year: int,
    season: int,
    episode: int,
    episode_name: str,
    source: SourceFile,
    episodes: list[int] | None = None,
) -> str:
    """Build a TV episode filename.

    ``Show (Year) - s01e01 - Episode Title [quality block] [hash].ext``
    """
    ext = source.path.suffix or ".mkv"
    metadata = build_metadata_block(source)
    meta_str = f" [{metadata}]" if metadata else ""
    hash_str = f" [{source.parsed.hash_code}]" if source.parsed.hash_code else ""

    base = f"{_title_year(series_title, year)} - "
    base += _format_padded_episode_tag(season, episode, episodes)
    episode_name = _sanitize_path(episode_name)
    if episode_name:
        base += f" - {episode_name}"
    return f"{base}{meta_str}{hash_str}{ext}"


# ---------------------------------------------------------------------------
# Directory naming
# ---------------------------------------------------------------------------


def _strip_redundant_year(title: str, year: int) -> str:
    """Strip a trailing `` (YYYY)`` suffix if it matches the series year."""
    suffix = f" ({year})"
    if title.endswith(suffix):
        return title[: -len(suffix)]
    return title


# Hiragana, Katakana, CJK Unified Ideographs, CJK Extension A
_RE_JAPANESE = re.compile(r"[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF\u3400-\u4DBF]")


def _has_japanese(text: str) -> bool:
    """Return True if *text* contains any CJK, hiragana, or katakana characters."""
    return bool(_RE_JAPANESE.search(text))


def format_series_dirname(title_ja: str, title_en: str, year: int) -> str:
    """Build the series directory name.

    Format is ``JA [EN] (YYYY)`` when both a native Japanese title and a
    distinct English title exist.  Falls back to ``TITLE (YYYY)`` when:
    - The Japanese title is empty or romaji (no kanji/kana)
    - The English title is empty
    - Both titles are identical
    """
    ja = _sanitize_path(_strip_redundant_year(title_ja, year))
    en = _sanitize_path(_strip_redundant_year(title_en, year))

    ja_is_native = bool(ja) and _has_japanese(ja)

    if ja_is_native and en and ja != en:
        return f"{ja} [{en}] ({year})"

    # Use whichever title is available; prefer English for readability
    title = en or ja
    return f"{title} ({year})"
