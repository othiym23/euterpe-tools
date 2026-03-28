"""Episode filename formatting and series directory naming."""

from __future__ import annotations

import re

from etp_lib.types import AudioTrack, SourceFile


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
    if source.release_group:
        group = source.release_group
        if source.version is not None:
            group = f"{group}(v{source.version})"
        prefix_parts.append(group)
    # Default to "Web" when no source type detected -- ensures the space
    # separator between group and tech fields is always present
    source_type = source.source_type or "Web"
    prefix_parts.append(source_type)
    prefix = " ".join(prefix_parts)

    # Comma-separated technical metadata
    tech: list[str] = []

    # REMUX
    if source.is_remux:
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

    Replaces ``/`` with `` - `` and ``:`` with ``-``.
    """
    return name.replace("/", " - ").replace(":", "-")


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
) -> str:
    """Build the full episode filename."""
    ext = source.path.suffix or ".mkv"
    metadata = build_metadata_block(source)
    meta_str = f" [{metadata}]" if metadata else ""
    hash_str = f" [{source.hash_code}]" if source.hash_code else ""

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
    ep_tag = f"s{season}e{episode:02d}"
    if episode_name:
        return f"{concise_name} - {ep_tag} - {episode_name}{meta_str}{hash_str}{ext}"
    return f"{concise_name} - {ep_tag}{meta_str}{hash_str}{ext}"


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
