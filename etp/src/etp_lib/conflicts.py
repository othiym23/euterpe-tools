"""Conflict detection and resolution for destination files."""

from __future__ import annotations

import platform
import re
import subprocess
import sys
import zlib
from dataclasses import dataclass
from pathlib import Path

from etp_lib.naming import unique_audio_codecs
from etp_lib.types import MediaInfo, SourceFile

_IS_LINUX = platform.system() == "Linux"


# ---------------------------------------------------------------------------
# Interactive prompts
# ---------------------------------------------------------------------------


def prompt_value(label: str, default: str = "") -> str:
    """Prompt for a value with an optional default."""
    if default:
        raw = input(f"{label} [{default}]: ").strip()
        return raw if raw else default
    return input(f"{label}: ").strip()


def prompt_confirm(message: str, default: bool = True) -> bool:
    """Prompt for yes/no confirmation."""
    suffix = "[Y/n]" if default else "[y/N]"
    raw = input(f"{message} {suffix}: ").strip().lower()
    if not raw:
        return default
    return raw in ("y", "yes")


# ---------------------------------------------------------------------------
# CRC32 verification
# ---------------------------------------------------------------------------


def compute_crc32(path: Path) -> str:
    """Compute the CRC32 hash of a file, returned as uppercase hex."""
    crc = 0
    with path.open("rb") as f:
        while chunk := f.read(1 << 20):  # 1 MiB chunks
            crc = zlib.crc32(chunk, crc)
    return f"{crc & 0xFFFFFFFF:08X}"


def verify_hash(source: SourceFile) -> tuple[bool, str] | None:
    """Verify the CRC32 hash embedded in the filename against the file.

    Returns ``(True, actual_hash)`` if the hash matches,
    ``(False, actual_hash)`` if it mismatches, or ``None`` if no hash is
    present in the filename.
    """
    if not source.hash_code:
        return None
    actual = compute_crc32(source.path)
    return (actual.upper() == source.hash_code.upper(), actual)


# ---------------------------------------------------------------------------
# Conflict detection and resolution
# ---------------------------------------------------------------------------


@dataclass
class ConflictInfo:
    """Describes a conflict between an incoming file and an existing destination."""

    existing_path: Path
    existing_size: int
    existing_media: MediaInfo | None
    incoming_source: SourceFile
    incoming_dest: Path  # the intended destination filename
    metadata_matches: bool


def _extract_key_metadata(sf: SourceFile) -> tuple[str, str, str, str]:
    """Extract key metadata elements for comparison: (group, source, codec, audio)."""
    audio = ""
    if sf.media and sf.media.audio_tracks:
        audio = "+".join(unique_audio_codecs(sf.media.audio_tracks))
    codec = sf.media.video_codec if sf.media else ""
    return (sf.release_group, sf.source_type, codec, audio)


def check_destination_conflict(
    source: SourceFile,
    dest_path: Path,
    intended_dest: Path | None = None,
    parse_source_filename_fn=None,
    analyze_file_fn=None,
) -> ConflictInfo | None:
    """Check if destination already exists and return conflict info.

    *intended_dest* is the filename the user intends to write (may differ
    from *dest_path* when fuzzy-matching found an existing file with a
    different naming convention).

    *parse_source_filename_fn* and *analyze_file_fn* are callables injected
    from the main module to avoid circular imports.
    """
    try:
        existing_size = dest_path.stat().st_size
    except FileNotFoundError:
        return None

    # Parse existing filename and analyze with mediainfo for comparison
    if parse_source_filename_fn is not None:
        existing_sf = parse_source_filename_fn(dest_path.name)
    else:
        existing_sf = SourceFile(path=dest_path)
    existing_sf.path = dest_path
    existing_media: MediaInfo | None = None
    try:
        if analyze_file_fn is not None:
            existing_media = analyze_file_fn(dest_path)
            existing_sf.media = existing_media
    except subprocess.CalledProcessError, FileNotFoundError:
        pass

    src_meta = _extract_key_metadata(source)
    dst_meta = _extract_key_metadata(existing_sf)
    metadata_matches = src_meta == dst_meta

    return ConflictInfo(
        existing_path=dest_path,
        existing_size=existing_size,
        existing_media=existing_media,
        incoming_source=source,
        incoming_dest=intended_dest or dest_path,
        metadata_matches=metadata_matches,
    )


def _format_size(size: int) -> str:
    """Format a file size in human-readable form."""
    if size >= 1 << 30:
        return f"{size / (1 << 30):.1f} GB"
    if size >= 1 << 20:
        return f"{size / (1 << 20):.1f} MB"
    return f"{size / (1 << 10):.1f} KB"


def _format_media_summary(media: MediaInfo | None) -> str:
    """Format a one-line mediainfo summary."""
    if media is None:
        return "(mediainfo unavailable)"
    parts: list[str] = []
    if media.video_codec:
        parts.append(media.video_codec)
    if media.resolution:
        parts.append(media.resolution)
    if media.bit_depth >= 10:
        parts.append(f"{media.bit_depth}bit")
    if media.hdr_type:
        parts.append(media.hdr_type)
    if media.audio_tracks:
        codecs = "+".join(unique_audio_codecs(media.audio_tracks))
        if codecs:
            parts.append(codecs)
    return ", ".join(parts)


def resolve_conflict(conflict: ConflictInfo) -> str:
    """Handle a destination conflict. Returns 'replace', 'keep', or 'skip'.

    For matching metadata with matching CRC32, auto-replaces silently.
    """
    if conflict.metadata_matches:
        # Short-circuit: if file sizes differ, CRC32 can't match
        incoming_size = conflict.incoming_source.path.stat().st_size
        if incoming_size == conflict.existing_size:
            src_crc = compute_crc32(conflict.incoming_source.path)
            dst_crc = compute_crc32(conflict.existing_path)
            if src_crc == dst_crc:
                print(
                    "  Destination exists with matching encode"
                    " (CRC32 match) — replacing to fix naming."
                )
                return "replace"
            print("  WARNING: Same encode metadata but CRC32 differs!")
        else:
            print("  WARNING: Same encode metadata but file sizes differ!")
        print(f"    existing: {conflict.existing_path.name}")
        print(f"         new: {conflict.incoming_dest.name}")

    else:
        print("  Conflict: destination exists")
        print(f"    existing: {conflict.existing_path.name}")
        print(f"         new: {conflict.incoming_dest.name}")
        print()

        existing_size = _format_size(conflict.existing_size)
        incoming_size = _format_size(conflict.incoming_source.path.stat().st_size)
        existing_summary = _format_media_summary(conflict.existing_media)
        incoming_summary = _format_media_summary(conflict.incoming_source.media)
        print(f"    Existing: {existing_summary}, {existing_size}")
        print(f"         New: {incoming_summary}, {incoming_size}")

    print()
    while True:
        choice = input("  [k]eep existing  [r]eplace  [s]kip: ").strip().lower()
        if choice in ("k", "keep"):
            return "keep"
        if choice in ("r", "replace"):
            return "replace"
        if choice in ("s", "skip"):
            return "skip"
        print("  Please enter k, r, or s.")


# Matches episode tags like "s1e01", "s01e01", "s1e1" in filenames
_RE_EP_TAG = re.compile(r"[Ss](\d+)[Ee](\d+)")


def _find_existing_episode(dest_path: Path) -> Path | None:
    """Find an existing file for the same episode in the destination directory.

    Matches by episode tag (sXeYY) with fuzzy season/episode zero-padding,
    so ``s1e01`` matches ``s01e01`` and vice versa.
    """
    dest_dir = dest_path.parent
    if not dest_dir.is_dir():
        return None

    # Extract the episode tag from the target filename
    m = _RE_EP_TAG.search(dest_path.name)
    if not m:
        return None
    target_season = int(m.group(1))
    target_episode = int(m.group(2))

    # Scan the directory for a file with the same episode
    for existing in dest_dir.iterdir():
        if existing == dest_path or not existing.is_file():
            continue
        em = _RE_EP_TAG.search(existing.name)
        if (
            em
            and int(em.group(1)) == target_season
            and int(em.group(2)) == target_episode
        ):
            return existing

    return None


def handle_conflict(
    source: SourceFile,
    dest_path: Path,
    parse_source_filename_fn=None,
    analyze_file_fn=None,
) -> str | None:
    """Check for and resolve a destination conflict.

    First checks for an exact path match, then does a fuzzy search for
    an existing file with the same episode tag (handles different
    zero-padding conventions like s1e01 vs s01e01).

    Returns ``None`` if no conflict, or 'replace'/'keep'/'skip'.
    When 'replace' is returned, the existing file has already been removed.
    """
    # Exact path match
    conflict = check_destination_conflict(
        source,
        dest_path,
        intended_dest=dest_path,
        parse_source_filename_fn=parse_source_filename_fn,
        analyze_file_fn=analyze_file_fn,
    )

    # Fuzzy match: same episode, different filename formatting
    if conflict is None:
        existing = _find_existing_episode(dest_path)
        if existing is not None:
            conflict = check_destination_conflict(
                source,
                existing,
                intended_dest=dest_path,
                parse_source_filename_fn=parse_source_filename_fn,
                analyze_file_fn=analyze_file_fn,
            )

    if conflict is None:
        return None
    action = resolve_conflict(conflict)
    if action == "replace":
        conflict.existing_path.unlink()
    return action


def copy_reflink(src: Path, dst: Path, dry_run: bool = False) -> bool:
    """Copy a file using Btrfs COW reflink."""
    if dry_run:
        print(f"  [dry-run] cp --reflink=always {src} -> {dst}")
        return True

    # Ensure destination directory exists
    dst.parent.mkdir(parents=True, exist_ok=True)

    if _IS_LINUX:
        cmd = ["cp", "--reflink=always", str(src), str(dst)]
    else:
        print(
            f"  warning: reflinks not supported on {platform.system()}, "
            f"using regular copy"
        )
        cmd = ["cp", str(src), str(dst)]

    try:
        subprocess.run(cmd, check=True)
        return True
    except subprocess.CalledProcessError as e:
        print(f"  error: copy failed: {e}", file=sys.stderr)
        return False
