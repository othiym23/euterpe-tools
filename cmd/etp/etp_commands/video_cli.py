"""Shared CLI for etp-movies and etp-television.

Both commands expose the same non-interactive plan/apply interface,
parameterized by :class:`~etp_lib.video_ingest.MediaKind`:

    etp <cmd> ingest plan  --<radarr|sonarr> [options] [pattern]
    etp <cmd> ingest apply MANIFEST [--dry-run] [--json]

The pipeline itself lives in :mod:`etp_lib.video_ingest`; this module
only parses arguments, loads config and credentials, and dispatches.

Configuration: ~/.config/euterpe-tools/media-ingestion.kdl (paths + IDs)
Environment:   ~/.config/euterpe-tools/media.env (TMDB_API_KEY,
               TVDB_API_KEY; anime.env is read as a fallback)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from etp_lib import paths as etp_paths
from etp_lib.envfile import load_env_file
from etp_lib.media_config import MediaConfigError, load_media_config
from etp_lib.mediainfo_cache import save_cache
from etp_lib.video_ingest import (
    API_KEY_ENV,
    ARR_KEY_ENV,
    ApplyOptions,
    MediaKind,
    PlanOptions,
    Providers,
    run_apply,
    run_plan,
)

VERSION = "0.1.0"


def build_parser(kind: MediaKind) -> argparse.ArgumentParser:
    """Build the plan/apply argument parser for *kind*."""
    noun = "movie" if kind is MediaKind.MOVIE else "television"
    p = argparse.ArgumentParser(
        prog=kind.tool,
        description=f"Non-interactive {noun} collection ingestion (plan/apply)",
    )
    p.add_argument("--version", "-V", action="version", version=VERSION)
    sub = p.add_subparsers(dest="command")

    ingest = sub.add_parser(
        "ingest",
        help=f"Import {noun} files via an editable plan manifest",
        description="Two-step ingestion: `plan` writes a KDL manifest "
        "(read-only), `apply` validates and executes it.",
    )
    ingest.set_defaults(ingest_parser=ingest)
    actions = ingest.add_subparsers(dest="action")

    plan = actions.add_parser(
        "plan",
        help="Scan sources and write a plan manifest (never writes to the library)",
    )
    plan.add_argument("pattern", nargs="?", help="Filter titles by substring")
    plan.add_argument(
        f"--{kind.managed_mode}",
        dest="managed",
        action="store_true",
        help=f"Plan from the {kind.managed_mode.capitalize()}-managed source tree",
    )
    plan.add_argument(
        "--downloads",
        action="store_true",
        help="Plan from the shared downloads directory (best-effort parsing)",
    )
    plan.add_argument(
        "--source",
        type=Path,
        action="append",
        metavar="DIR",
        help="Override source directories (repeatable for --downloads; the"
        f" first value also overrides the --{kind.managed_mode} scan root)",
    )
    plan.add_argument(
        "--force",
        action="store_true",
        help="Include files already recorded in the shared ingest register",
    )
    plan.add_argument(
        "-o",
        "--output",
        type=Path,
        metavar="FILE",
        help="Manifest output path (default: ./<tool>-plan-<timestamp>.kdl)",
    )
    plan.add_argument(
        "--refine",
        type=Path,
        metavar="FILE",
        help="Carry provider IDs and skip/conflict decisions forward from a"
        " previous manifest",
    )
    plan.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit a machine-readable summary on stdout (human output -> stderr)",
    )
    plan.add_argument(
        "--config",
        type=Path,
        metavar="FILE",
        help="Config file (default: media-ingestion.kdl in the config dir)",
    )
    plan.add_argument(
        "--no-cache", action="store_true", help="Bypass metadata provider caches"
    )
    plan.add_argument("-v", "--verbose", action="store_true")

    apply_p = actions.add_parser(
        "apply", help="Validate a plan manifest against disk, then execute it"
    )
    apply_p.add_argument(
        "manifest", type=Path, help="Plan manifest written by `ingest plan`"
    )
    apply_p.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and report what would happen without copying",
    )
    apply_p.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Emit a machine-readable result on stdout (human output -> stderr)",
    )
    apply_p.add_argument(
        "--sub-lang",
        default="en",
        metavar="LANG",
        help="Language tag for untagged subtitle sidecars (default: en)",
    )
    apply_p.add_argument("-v", "--verbose", action="store_true")

    return p


def _run_plan(kind: MediaKind, args: argparse.Namespace) -> int:
    if not (args.managed or args.downloads):
        print(
            f"error: specify at least one source mode:"
            f" --{kind.managed_mode} and/or --downloads",
            file=sys.stderr,
        )
        return 1

    # Only the primary provider's key is required; without the secondary
    # key, cross-checks degrade to "unavailable" (a warning, never fatal).
    primary = API_KEY_ENV[kind.primary_provider]
    secondary = next(v for v in API_KEY_ENV.values() if v != primary)
    if not os.environ.get(primary):
        print(
            f"error: {primary} not set (configure in {etp_paths.media_env()})",
            file=sys.stderr,
        )
        return 1
    if not os.environ.get(secondary):
        print(
            f"warning: {secondary} not set; provider cross-checks will be"
            " recorded as unavailable",
            file=sys.stderr,
        )

    try:
        config = load_media_config(args.config)
    except MediaConfigError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    opts = PlanOptions(
        managed=args.managed,
        downloads=args.downloads,
        sources=args.source or [],
        pattern=args.pattern or "",
        force=args.force,
        output=args.output,
        json_output=args.json_output,
        refine=args.refine,
        no_cache=args.no_cache,
        verbose=args.verbose,
    )
    other_kind = MediaKind.TV if kind is MediaKind.MOVIE else MediaKind.MOVIE
    providers = Providers(
        tmdb_key=os.environ.get("TMDB_API_KEY", ""),
        tvdb_key=os.environ.get("TVDB_API_KEY", ""),
        arr_key=os.environ.get(ARR_KEY_ENV[kind], ""),
        cross_arr_key=os.environ.get(ARR_KEY_ENV[other_kind], ""),
        no_cache=args.no_cache,
    )
    return run_plan(kind, config, opts, providers)


def main(kind: MediaKind) -> int:
    """Entry point shared by etp-movies and etp-television."""
    load_env_file(etp_paths.media_env(), etp_paths.anime_env())

    parser = build_parser(kind)
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 0
    if not args.action:
        args.ingest_parser.print_help()
        return 0

    # mediainfo analysis is the dominant cost of planning a backlog;
    # persist whatever was analyzed even when the plan crashes or is
    # interrupted (mirrors anime.py's main).
    try:
        if args.action == "plan":
            return _run_plan(kind, args)
        return run_apply(
            kind,
            args.manifest,
            ApplyOptions(
                dry_run=args.dry_run,
                json_output=args.json_output,
                verbose=args.verbose,
                sub_lang=args.sub_lang,
            ),
        )
    finally:
        save_cache()
