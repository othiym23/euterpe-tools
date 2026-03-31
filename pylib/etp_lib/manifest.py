"""KDL manifest writing, parsing, and execution for batch triage."""

from __future__ import annotations

import errno
import os
import re
import shlex
import subprocess
import sys
import tempfile
from pathlib import Path

import kdl

from etp_lib.conflicts import (
    copy_reflink,
    handle_conflict,
    prompt_confirm,
    prompt_value,
    verify_hash,
)
from etp_lib.media_parser import normalize_for_matching
from etp_lib.naming import format_episode_filename, format_series_dirname
from etp_lib.types import (
    AnimeInfo,
    BonusType,
    Episode,
    EpisodeType,
    ManifestEntry,
    SourceFile,
)

# HamaTV-compatible special episode ranges.
# Offset by +20 from each range start to avoid collisions with
# AniDB-tracked specials that may be added later.
_HAMATV_RANGES: dict[str, int] = {
    BonusType.NCOP: 171,  # s0e151+ range, +20 buffer
    BonusType.NCED: 191,  # separate from NCOP to avoid collisions
    BonusType.PV: 321,  # s0e301+ range, +20 buffer
    BonusType.PREVIEW: 321,  # alias for PV — same HamaTV category
    BonusType.CM: 521,  # s0e501+ range, +20 buffer
    BonusType.BONUS: 521,  # alias for CM — same HamaTV category
    BonusType.MENU: 921,  # s0e901+ range, +20 buffer
}


def _match_bonus_to_anidb_special(
    bonus_type: str, episode_title: str, specials: list[Episode]
) -> Episode | None:
    """Try to match a bonus file against AniDB special episodes.

    Uses bonus type to guide matching:
    - NCOP → credit episodes with "Opening" in title
    - NCED → credit episodes with "Ending" in title
    - Others → compare normalized episode titles
    """
    if not specials:
        return None

    if bonus_type == BonusType.NCOP:
        for ep in specials:
            if ep.ep_type == EpisodeType.CREDIT and "opening" in ep.title_en.lower():
                return ep
    elif bonus_type == BonusType.NCED:
        for ep in specials:
            if ep.ep_type == EpisodeType.CREDIT and "ending" in ep.title_en.lower():
                return ep
    elif episode_title:
        ep_norm = normalize_for_matching(episode_title)
        if ep_norm:
            for ep in specials:
                en_norm = normalize_for_matching(ep.title_en)
                ja_norm = normalize_for_matching(ep.title_ja)
                if (en_norm and ep_norm in en_norm) or (ja_norm and ep_norm in ja_norm):
                    return ep
    return None


def escape_kdl(s: str) -> str:
    """Escape a string for use inside a KDL quoted value."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


_MAX_FILENAME_BYTES = 255  # ext4/Btrfs filename length limit


def build_manifest_entries(
    parsed: list[SourceFile],
    info: AnimeInfo,
    concise_name: str,
    series_dir: Path,
    verbose: bool,
    analyze_file_fn=None,
) -> list[ManifestEntry]:
    """Build manifest entries for all files without per-file prompts.

    Runs mediainfo, verifies CRC32 hashes, matches episodes, and constructs
    destination paths using defaults.
    """
    entries: list[ManifestEntry] = []
    # Track AniDB specials already matched so each is used at most once
    matched_special_tags: set[str] = set()
    specials = [ep for ep in info.episodes if ep.ep_type != EpisodeType.REGULAR]
    specials_by_num: dict[int, Episode] = {
        ep.number: ep for ep in specials if ep.season == 0
    }

    # When using TVDB, start HamaTV ranges after the highest existing
    # TVDB special number to avoid collisions in the single Specials/ dir.
    max_special_num = max((ep.number for ep in specials_by_num.values()), default=0)
    hamatv_counters: dict[str, int] = {}
    if info.tvdb_id is not None and max_special_num > 0:
        for key, default_start in _HAMATV_RANGES.items():
            hamatv_counters[key] = max(default_start, max_special_num + 20)
    total = len(parsed)
    for i, sf in enumerate(parsed, 1):
        print(f"  Analyzing {i}/{total}: {sf.path.name}")

        # Analyze with mediainfo
        try:
            if analyze_file_fn is not None:
                sf.media = analyze_file_fn(sf.path)
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            if verbose:
                print(f"    warning: mediainfo failed: {e}")

        # Verify CRC32 hash -- on mismatch, clear hash so it's stripped from dest
        hash_failed = False
        hash_result = verify_hash(sf)
        if hash_result is not None:
            ok, actual = hash_result
            if not ok:
                print(
                    f"    CRC32 MISMATCH: expected {sf.parsed.hash_code}, got {actual}"
                )
                sf.parsed.hash_code = ""
                hash_failed = True
            elif verbose:
                print(f"    CRC32 verified: {sf.parsed.hash_code}")

        ep_number = sf.parsed.episode
        season = sf.parsed.season if sf.parsed.season is not None else 1
        is_special = season == 0 or sf.parsed.is_special
        special_tag = ""
        episode_name = ""
        is_unmatched_special = False

        bonus_type = sf.parsed.bonus_type
        episode_title = sf.parsed.episode_title

        if ep_number is not None and is_special and not bonus_type:
            ep = specials_by_num.get(ep_number)
            if ep is not None:
                episode_name = ep.title_en
                special_tag = ep.special_tag
                season = 0
        elif ep_number is not None and not is_special:
            episode_name = info.find_episode_title(ep_number, season)
        elif ep_number is not None and is_special and bonus_type:
            # Parser detected both an episode number and a bonus type
            # (e.g. S03ED from SeasonSpecial) — try bonus matching first
            available = [
                ep for ep in specials if ep.special_tag not in matched_special_tags
            ]
            matched_ep = _match_bonus_to_anidb_special(
                bonus_type, episode_title, available
            )
            if matched_ep is not None:
                special_tag = sf.parsed.special_tag or matched_ep.special_tag
                episode_name = episode_title or matched_ep.title_en
                matched_special_tags.add(matched_ep.special_tag)
                sf.parsed.episode = matched_ep.number
                sf.parsed.season = 0
                ep_number = matched_ep.number
                season = 0
            else:
                # Use parser special tag directly (e.g. S03OP, S01OVA)
                special_tag = sf.parsed.special_tag
                episode_name = episode_title
                season = 0
        elif bonus_type:
            available = [
                ep for ep in specials if ep.special_tag not in matched_special_tags
            ]
            matched_ep = _match_bonus_to_anidb_special(
                bonus_type, episode_title, available
            )
            if matched_ep is not None:
                is_special = True
                if bonus_type in (BonusType.NCOP, BonusType.NCED):
                    # Build tag like NCOP1, NCED1a from AniDB title.
                    # AniDB titles: "Opening", "Opening 1", "Ending 1a"
                    m = re.search(r"(\d+)([a-z]*)\s*$", matched_ep.title_en)
                    suffix = (
                        f"{m.group(1)}{m.group(2)}" if m else str(matched_ep.number)
                    )
                    special_tag = f"{bonus_type}{suffix}"
                else:
                    special_tag = matched_ep.special_tag
                ep_number = matched_ep.number
                episode_name = episode_title or matched_ep.title_en
                matched_special_tags.add(matched_ep.special_tag)
                sf.parsed.episode = ep_number
                sf.parsed.season = 0
            else:
                # Assign HamaTV-compatible s0e number, tagged (todo)
                is_special = True
                range_start = _HAMATV_RANGES.get(bonus_type, 521)
                ep_number = hamatv_counters.get(bonus_type, range_start)
                hamatv_counters[bonus_type] = ep_number + 1
                episode_name = bonus_type
                if episode_title:
                    episode_name = f"{bonus_type} - {episode_title}"
                season = 0
                is_unmatched_special = True
                sf.parsed.episode = ep_number
                sf.parsed.season = 0

        # Build destination path
        if ep_number is None:
            # Can't auto-match -- mark as TODO
            placeholder = format_episode_filename(
                concise_name=concise_name,
                season=season,
                episode=0,
                episode_name="EPISODE_NAME",
                source=sf,
                is_special=is_special,
                special_tag=special_tag,
            )
            placeholder = placeholder.replace("s1e00", "s1eXX")
            dest_dir = series_dir / f"Season {season:02d}"
            entries.append(
                ManifestEntry(
                    source=sf,
                    dest_path=dest_dir / placeholder,
                    is_todo=True,
                    hash_failed=hash_failed,
                )
            )
        else:
            filename = format_episode_filename(
                concise_name=concise_name,
                season=season,
                episode=ep_number,
                episode_name=episode_name,
                source=sf,
                is_special=is_special,
                special_tag=special_tag,
            )
            if is_special:
                dest_dir = series_dir / "Specials"
            else:
                dest_dir = series_dir / f"Season {season:02d}"
            entries.append(
                ManifestEntry(
                    source=sf,
                    dest_path=dest_dir / filename,
                    is_todo=is_unmatched_special,
                    hash_failed=hash_failed,
                )
            )

    return entries


def write_manifest(
    entries: list[ManifestEntry],
    info: AnimeInfo,
    concise_name: str,
    series_dir: Path,
    extras: list[Path] | None = None,
) -> Path:
    """Write manifest entries to a KDL file for editing."""
    provider = ""
    if info.anidb_id is not None:
        provider = f"AniDB: {info.anidb_id}"
    elif info.tvdb_id is not None:
        provider = f"TheTVDB: {info.tvdb_id}"

    dirname = format_series_dirname(info.title_ja, info.title_en, info.year)

    # Group entries by season/specials
    groups: dict[str, list[ManifestEntry]] = {}
    for entry in entries:
        # Determine group key from the destination path
        dest_parent = entry.dest_path.parent.name
        if dest_parent == "Specials":
            key = "specials"
        else:
            # "Season 01" -> season number
            key = dest_parent
        groups.setdefault(key, []).append(entry)

    # Build KDL document as text (easier than constructing Node objects
    # for the header comments)
    lines: list[str] = []
    lines.append("// etp-anime triage manifest")
    lines.append(f"// Series: {dirname}")
    if provider:
        lines.append(f"// {provider}")
    lines.append(f"// Series dir: {series_dir}")
    lines.append("//")
    lines.append(
        "// Edit destination filenames. Delete or /- comment out entries to skip."
    )
    lines.append("// Source filenames are for reference only — only dest is used.")
    lines.append("")

    for group_key in sorted(groups.keys()):
        group_entries = sorted(
            groups[group_key], key=lambda e: e.source.parsed.episode or 0
        )
        if group_key == "specials":
            lines.append("specials {")
        else:
            # "Season 01" -> season 1
            season_num = group_key.replace("Season ", "").lstrip("0") or "0"
            lines.append(f"season {season_num} {{")

        for entry in group_entries:
            ep_num = entry.source.parsed.episode or 0
            tag = "(todo)" if entry.is_todo else ""
            if entry.hash_failed:
                lines.append("  // CRC32 MISMATCH — hash stripped from destination")
            lines.append(f"  {tag}episode {ep_num} {{")
            lines.append(f'    source "{escape_kdl(str(entry.source.path))}"')
            if entry.source.matched_download is not None:
                lines.append(
                    f'    downloaded "{escape_kdl(str(entry.source.matched_download))}"'
                )
            dest_name = entry.dest_path.name
            if len(dest_name.encode("utf-8")) > _MAX_FILENAME_BYTES:
                lines.append(
                    f"    // WARNING: filename is"
                    f" {len(dest_name.encode('utf-8'))} bytes"
                    f" (max {_MAX_FILENAME_BYTES}) — shorten before saving"
                )
            lines.append(f'    dest "{escape_kdl(dest_name)}"')
            lines.append("  }")

        lines.append("}")
        lines.append("")

    # Non-video extras (CDs, scans, etc.) — user can delete entries to skip
    if extras:
        lines.append("extras {")
        for f in sorted(extras, key=lambda p: p.name):
            lines.append(f'  file "{escape_kdl(str(f))}" {{')
            lines.append(f'    dest "{escape_kdl(f.name)}"')
            lines.append("  }")
        lines.append("}")
        lines.append("")

    fd, path = tempfile.mkstemp(suffix=".kdl", prefix="etp-triage-")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")

    return Path(path)


def open_editor(manifest_path: Path) -> bool:
    """Open the manifest in the user's editor. Returns True on success."""
    editor = os.environ.get("VISUAL") or os.environ.get("EDITOR") or "vi"
    try:
        result = subprocess.run([*shlex.split(editor), str(manifest_path)])
        return result.returncode == 0
    except FileNotFoundError:
        print(f"  error: editor '{editor}' not found")
        return False


def parse_manifest(
    manifest_path: Path,
    known_sources: dict[str, SourceFile],
    series_dir: Path,
) -> tuple[list[tuple[SourceFile, Path]], list[str], list[tuple[Path, Path]]]:
    """Parse an edited KDL manifest file.

    Returns ``(entries, errors, extras)`` where extras is a list of
    ``(source_path, dest_path)`` pairs for non-video files.
    """
    text = manifest_path.read_text(encoding="utf-8")
    try:
        doc = kdl.parse(text)
    except kdl.ParseError as e:
        return [], [f"  KDL parse error: {e}"], []

    entries: list[tuple[SourceFile, Path]] = []
    extras: list[tuple[Path, Path]] = []
    errors: list[str] = []

    def _ep_label(ep_node: object, group_name: str) -> str:
        """Format a label like 'episode 5 in season 1' for error messages."""
        ep_arg = getattr(ep_node, "args", [])
        ep_str = str(ep_arg[0]) if ep_arg else "?"
        return f"episode {ep_str} in {group_name}"

    for group_node in doc.nodes:
        # Extras section: non-video files
        if group_node.name == "extras":
            extras_dir = series_dir / "Extras"
            for file_node in group_node.nodes:
                if file_node.name != "file":
                    continue
                source_path = str(file_node.args[0]) if file_node.args else ""
                dest_name = ""
                for child in file_node.nodes:
                    if child.name == "dest" and child.args:
                        dest_name = str(child.args[0])
                if not source_path:
                    errors.append("  extras file: missing source path")
                elif not dest_name:
                    errors.append(
                        f"  extras file '{Path(source_path).name}': missing dest"
                    )
                else:
                    extras.append((Path(source_path), extras_dir / dest_name))
            continue

        # Determine destination subdirectory
        if group_node.name == "specials":
            dest_subdir = series_dir / "Specials"
        elif group_node.name == "season" and group_node.args:
            try:
                season_num = int(group_node.args[0])
            except ValueError, TypeError:
                errors.append(f"  invalid season number: '{group_node.args[0]}'")
                continue
            dest_subdir = series_dir / f"Season {season_num:02d}"
        else:
            continue

        for ep_node in group_node.nodes:
            if ep_node.name != "episode":
                continue
            label = _ep_label(ep_node, group_node.name)

            # Check for (todo) tag
            if ep_node.tag == "todo":
                errors.append(f"  {label}: unresolved (todo) entry")
                continue

            # Extract source and dest from children
            source_name = ""
            dest_name = ""
            for child in ep_node.nodes:
                if child.name == "source" and child.args:
                    source_name = str(child.args[0])
                elif child.name == "dest" and child.args:
                    dest_name = str(child.args[0])

            if not dest_name:
                errors.append(f"  {label}: missing dest")
                continue

            if not source_name:
                errors.append(f"  {label}: missing source")
                continue

            sf = known_sources.get(source_name)
            if sf is None:
                # Show a few available sources to help the user find the right path
                available = list(known_sources.keys())[:5]
                hint = ""
                if available:
                    hint = (
                        f"\n    available sources ({len(known_sources)} total):"
                        + "".join(f"\n      {p}" for p in available)
                    )
                errors.append(f"  {label}: unknown source '{source_name}'{hint}")
                continue

            entries.append((sf, dest_subdir / dest_name))

    return entries, errors, extras


def _check_filename_length(dest_path: Path) -> Path:
    """Check if the destination filename exceeds the filesystem limit.

    If too long, prompts the user to edit the filename until it fits.
    Returns the (possibly updated) destination path.
    """
    while len(dest_path.name.encode("utf-8")) > _MAX_FILENAME_BYTES:
        name_len = len(dest_path.name.encode("utf-8"))
        print(f"\n  ERROR: filename is {name_len} bytes (max {_MAX_FILENAME_BYTES}):")
        print(f"    {dest_path.name}")
        new_name = prompt_value("  Enter shorter filename", dest_path.name)
        dest_path = dest_path.parent / new_name
    return dest_path


def execute_manifest(
    entries: list[tuple[SourceFile, Path]],
    dry_run: bool,
    verbose: bool,
    parse_source_filename_fn=None,
    analyze_file_fn=None,
) -> tuple[int, int, list[Path]]:
    """Execute the parsed manifest: copy each file to its destination.

    Returns ``(success, failed, triaged_paths)`` -- triaged_paths includes
    files that were kept, skipped, or copied (all are marked as processed).
    """
    success = 0
    failed = 0
    triaged_paths: list[Path] = []

    for sf, dest_path in entries:
        # Check filename length before attempting any operations
        dest_path = _check_filename_length(dest_path)

        if verbose:
            print(f"  {sf.path.name} -> {dest_path}")

        # Check for existing file at destination
        if not dry_run:
            action = handle_conflict(
                sf,
                dest_path,
                parse_source_filename_fn=parse_source_filename_fn,
                analyze_file_fn=analyze_file_fn,
            )
            if action in ("keep", "skip"):
                triaged_paths.append(sf.path)
                if action == "skip":
                    failed += 1
                else:
                    success += 1
                continue

        try:
            if copy_reflink(sf.path, dest_path, dry_run=dry_run):
                success += 1
                triaged_paths.append(sf.path)
            else:
                failed += 1
        except OSError as e:
            if e.errno == errno.ENAMETOOLONG:
                dest_path = _check_filename_length(dest_path)
                if copy_reflink(sf.path, dest_path, dry_run=dry_run):
                    success += 1
                    triaged_paths.append(sf.path)
                else:
                    failed += 1
            else:
                print(f"  error: {e}", file=sys.stderr)
                failed += 1

    return success, failed, triaged_paths


# ---------------------------------------------------------------------------
# ManifestWorkflow — encapsulates the build → write → edit → parse → execute
# sequence used by both triage and series commands.
# ---------------------------------------------------------------------------


class ManifestWorkflow:
    """Orchestrates the manifest editing workflow for a batch of files.

    Encapsulates the full sequence: build manifest entries (with mediainfo
    analysis and CRC verification), write to a temp file, open in $EDITOR
    for user editing, parse the result, and execute the copy operations.

    Usage::

        wf = ManifestWorkflow(parsed, info, concise_name, series_dir,
                              verbose=True, analyze_file_fn=analyze_file)
        success, failed, triaged = wf.run(
            dry_run=False,
            extras=extras,
            parse_source_filename_fn=parse_source_filename,
        )
    """

    def __init__(
        self,
        parsed: list[SourceFile],
        info: AnimeInfo,
        concise_name: str,
        series_dir: Path,
        verbose: bool = False,
        analyze_file_fn=None,
    ) -> None:
        self.parsed = parsed
        self.info = info
        self.concise_name = concise_name
        self.series_dir = series_dir
        self.verbose = verbose
        self.analyze_file_fn = analyze_file_fn

        self.entries: list[ManifestEntry] = []
        self.manifest_path: Path | None = None

    def build(self) -> list[ManifestEntry]:
        """Build manifest entries (mediainfo + CRC32 verification)."""
        print()
        self.entries = build_manifest_entries(
            self.parsed,
            self.info,
            self.concise_name,
            self.series_dir,
            self.verbose,
            analyze_file_fn=self.analyze_file_fn,
        )
        return self.entries

    def write(self, extras: list[Path] | None = None) -> Path:
        """Write manifest to a temp file for editing."""
        self.manifest_path = write_manifest(
            self.entries,
            self.info,
            self.concise_name,
            self.series_dir,
            extras=extras or [],
        )
        return self.manifest_path

    def edit_loop(
        self,
    ) -> tuple[list[tuple[SourceFile, Path]], list[tuple[Path, Path]]]:
        """Open editor, parse, and retry on errors. Returns (entries, extras).

        Raises ValueError if the user cancels or the manifest is empty.
        """
        assert self.manifest_path is not None
        known_sources: dict[str, SourceFile] = {
            str(e.source.path): e.source for e in self.entries
        }

        while True:
            if not open_editor(self.manifest_path):
                raise ValueError("Editor failed")

            parsed_entries, errors, extra_entries = parse_manifest(
                self.manifest_path, known_sources, self.series_dir
            )

            if errors:
                print(f"\n  Manifest has {len(errors)} error(s):")
                for err in errors:
                    print(err)
                if prompt_confirm("\n  Re-open editor to fix?"):
                    continue
                raise ValueError("User cancelled after errors")

            if not parsed_entries:
                raise ValueError("Manifest is empty")

            return parsed_entries, extra_entries

    def execute(
        self,
        parsed_entries: list[tuple[SourceFile, Path]],
        extra_entries: list[tuple[Path, Path]],
        dry_run: bool,
        parse_source_filename_fn=None,
    ) -> tuple[int, int, list[Path]]:
        """Execute the manifest: copy files and extras."""
        print(f"\n  Copying {len(parsed_entries)} file(s)...")
        result = execute_manifest(
            parsed_entries,
            dry_run,
            self.verbose,
            parse_source_filename_fn=parse_source_filename_fn,
            analyze_file_fn=self.analyze_file_fn,
        )

        if extra_entries:
            print(f"  Copying {len(extra_entries)} extra(s)...")
            for src, dst in extra_entries:
                dst.parent.mkdir(parents=True, exist_ok=True)
                copy_reflink(src, dst, dry_run=dry_run)

        return result

    def cleanup(self) -> None:
        """Remove the temp manifest file."""
        if self.manifest_path is not None:
            try:
                self.manifest_path.unlink()
            except OSError:
                pass

    def run(
        self,
        dry_run: bool = False,
        extras: list[Path] | None = None,
        parse_source_filename_fn=None,
    ) -> tuple[int, int, list[Path]]:
        """Run the full workflow: build → write → edit → execute → cleanup.

        Returns ``(success, failed, triaged_paths)``.
        """
        self.build()
        self.write(extras=extras)
        file_count = len(self.parsed)

        try:
            parsed_entries, extra_entries = self.edit_loop()
        except ValueError as e:
            print(f"  {e}. Skipping group.")
            return 0, file_count, []
        finally:
            self.cleanup()

        return self.execute(
            parsed_entries,
            extra_entries,
            dry_run,
            parse_source_filename_fn=parse_source_filename_fn,
        )
