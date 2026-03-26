"""Incremental filesystem catalog builder driven by KDL config.

Orchestrates etp-tree and etp-csv across multiple directory trees,
generating tree files and CSV metadata indexes. Evolved from
scripts/catalog-nas.py with KDL config support and direct etp-*
binary invocation.
"""

import argparse
import os
import re
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Self


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


def resolve_global(global_cfg: dict[str, str]) -> dict[str, str]:
    """Expand env vars and resolve {key} interpolation in global paths.

    Keys are processed in definition order so that later values can
    reference earlier ones (e.g. trees_path references home_base).
    """
    resolved: dict[str, str] = {}
    for key, value in global_cfg.items():
        # First expand $ENV_VAR / ${ENV_VAR}
        value = os.path.expandvars(value)
        # Then resolve {other_key} references to already-resolved values
        value = re.sub(
            r"\{(\w+)\}",
            lambda m: resolved.get(m.group(1), m.group(0)),
            value,
        )
        resolved[key] = value
    return resolved


def load_config(path: Path) -> dict[str, Any]:
    """Load and resolve a catalog KDL config file."""
    import kdl

    text = path.read_text(encoding="utf-8")
    doc = kdl.parse(text)

    raw_global: dict[str, str] = {}
    scans: dict[str, dict[str, Any]] = {}

    for node in doc.nodes:
        if node.name == "global":
            for child in node.nodes or []:
                # Convert kebab-case to snake_case for Python
                key = child.name.replace("-", "_")
                if child.args:
                    raw_global[key] = str(child.args[0])
        elif node.name == "scan":
            name = str(node.args[0]) if node.args else "unnamed"
            scan_cfg: dict[str, Any] = {}
            for child in node.nodes or []:
                key = child.name.replace("-", "_")
                if child.args:
                    scan_cfg[key] = str(child.args[0])
            scans[name] = scan_cfg

    cfg: dict[str, Any] = {}
    cfg["global"] = resolve_global(raw_global)
    cfg["scans"] = scans
    return cfg


# ---------------------------------------------------------------------------
# Timing
# ---------------------------------------------------------------------------


class Timer:
    """Context manager that captures wall-clock and child-process CPU time."""

    def __enter__(self) -> Self:
        self.wall = time.monotonic()
        self.times = os.times()
        self.elapsed = 0.0
        self.user = 0.0
        self.sys = 0.0
        return self

    def __exit__(self, *_exc: Any) -> bool:
        wall_end = time.monotonic()
        times_end = os.times()
        self.elapsed = wall_end - self.wall
        self.user = times_end.children_user - self.times.children_user
        self.sys = times_end.children_system - self.times.children_system
        return False

    def __str__(self) -> str:
        return f"real {self.elapsed:.1f}s  user {self.user:.1f}s  sys {self.sys:.1f}s"


# ---------------------------------------------------------------------------
# Subprocess helper
# ---------------------------------------------------------------------------


def run_cmd(
    args: Sequence[str],
    *,
    capture: bool = False,
    env_extra: dict[str, str] | None = None,
    verbose: bool = False,
) -> str | None:
    """Run a command, optionally capturing stdout.

    Returns captured stdout when capture=True, otherwise None.
    """
    env = os.environ.copy()
    if env_extra:
        env.update(env_extra)

    if verbose:
        print(f"  $ {' '.join(args)}", flush=True)

    result = subprocess.run(
        args,
        check=True,
        capture_output=capture,
        text=capture,
        env=env,
    )
    if capture:
        return result.stdout
    return None


# ---------------------------------------------------------------------------
# Binary resolution
# ---------------------------------------------------------------------------


def require_binary(name: str) -> str:
    """Find an etp-* binary in libexec or on $PATH, or exit."""
    from etp_lib.paths import find_binary

    found = find_binary(name)
    if found:
        return found

    print(f"error: '{name}' not found", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Tree generators
# ---------------------------------------------------------------------------


VALID_MODES = {"used", "df", "subs"}
REQUIRED_SCAN_FIELDS = ("disk", "header", "mode", "desc")


def _validate_scan_cfg(name: str, scan_cfg: dict[str, Any]) -> None:
    """Raise SystemExit if required fields are missing from a scan config."""
    missing = [f for f in REQUIRED_SCAN_FIELDS if f not in scan_cfg]
    if missing:
        print(
            f"error: scan '{name}' missing required field(s): {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)


def generate_tree(
    name: str,
    scan_cfg: dict[str, Any],
    global_cfg: dict[str, str],
    *,
    verbose: bool = False,
    profile: bool = False,
) -> None:
    """Generate a tree file for a scan entry.

    Mode controls the summary appended after the tree output:
      - 'used': --du on disk
      - 'df':   df -PH on disk
      - 'subs': df -PH on disk + --du --du-subs
    """
    _validate_scan_cfg(name, scan_cfg)
    disk = scan_cfg["disk"]
    header = scan_cfg["header"]
    mode = scan_cfg["mode"]
    desc = scan_cfg["desc"]

    etp_tree = require_binary("etp-tree")
    tree_file = Path(global_cfg["trees_path"]) / f"{desc}.tree"
    db_file = Path(global_cfg["db_path"]) / f"{desc}.db"

    # Run etp-tree
    cmd: list[str] = [etp_tree, disk, "--db", str(db_file), "-N"]
    if mode in ("used", "subs"):
        cmd.append("--du")
    if mode == "subs":
        cmd.append("--du-subs")
    if verbose:
        cmd.append("-v")
    if profile:
        cmd.append("--profile")

    tree_out = run_cmd(cmd, capture=True, verbose=verbose) or ""

    suffix_parts: list[str] = []
    if mode in ("df", "subs"):
        suffix_parts.append(
            run_cmd(["df", "-PH", disk], capture=True, verbose=verbose) or ""
        )

    with open(tree_file, "w", encoding="utf-8") as f:
        f.write(header + "\n\n")
        f.write(tree_out)
        for part in suffix_parts:
            f.write("\n")
            f.write(part)


# ---------------------------------------------------------------------------
# Scan runner
# ---------------------------------------------------------------------------


def run_scan(
    name: str,
    scan_cfg: dict[str, Any],
    global_cfg: dict[str, str],
    *,
    verbose: bool = False,
    profile: bool = False,
) -> bool:
    _validate_scan_cfg(name, scan_cfg)
    disk = scan_cfg["disk"]
    desc = scan_cfg["desc"]
    mode = scan_cfg["mode"]

    if not Path(disk).exists():
        print(f"warning: {disk} does not exist, skipping", file=sys.stderr)
        return False

    if mode not in VALID_MODES:
        print(f"error: unknown mode '{mode}' for scan '{name}'", file=sys.stderr)
        return False

    etp_csv = require_binary("etp-csv")
    csv_file = Path(global_cfg["csvs_path"]) / f"{desc}.csv"
    db_file = Path(global_cfg["db_path"]) / f"{desc}.db"

    print(f"\n# cataloging {name}: {disk}", flush=True)

    ok = True
    with Timer() as total:
        with Timer() as tree_t:
            try:
                generate_tree(
                    name, scan_cfg, global_cfg, verbose=verbose, profile=profile
                )
            except subprocess.CalledProcessError as exc:
                print(
                    f"warning: {exc.cmd} failed (code {exc.returncode}): {exc.stderr or ''}",
                    file=sys.stderr,
                )
                return False
        print(f"# tree: {tree_t}", flush=True)

        # CSV run reuses the DB from the tree scan
        cmd = [
            etp_csv,
            disk,
            "--db",
            str(db_file),
            "--no-scan",
            "-o",
            str(csv_file),
        ]
        if verbose:
            cmd.append("-v")
        if profile:
            cmd.append("--profile")

        with Timer() as scan_t:
            try:
                run_cmd(cmd, verbose=verbose)
            except subprocess.CalledProcessError as exc:
                print(
                    f"warning: CSV generation failed (code {exc.returncode})",
                    file=sys.stderr,
                )
                ok = False
        print(f"# csv: {scan_t}", flush=True)

    print(f"# {name} TOTAL: {total}", flush=True)
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Catalog filesystem trees using etp-tree, etp-csv, and df.",
    )
    parser.add_argument(
        "config",
        nargs="?",
        default=None,
        help="Path to catalog KDL config (default: catalog.kdl in config dir)",
    )
    parser.add_argument(
        "--scan",
        action="append",
        dest="scans",
        metavar="NAME",
        help="Run only named scan(s); repeatable",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print plan without executing",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output; passes -v to etp-tree/etp-csv",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Pass --profile to etp-tree/etp-csv for Chrome Trace output",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    # Resolve config path: explicit arg > XDG/platform config dir
    if args.config is not None:
        config_path = Path(args.config)
    else:
        from etp_lib import paths

        config_path = paths.catalog_config()

    if not config_path.exists():
        print(f"error: config file not found: {config_path}", file=sys.stderr)
        return 1

    cfg = load_config(config_path)
    global_cfg = cfg["global"]
    scans = cfg["scans"]

    # Validate required global fields
    required_global = ("trees_path", "csvs_path", "db_path")
    missing_global = [f for f in required_global if f not in global_cfg]
    if missing_global:
        print(
            f"error: global config missing required field(s): {', '.join(missing_global)}",
            file=sys.stderr,
        )
        return 1

    # Filter to requested scans
    if args.scans:
        unknown = set(args.scans) - set(scans.keys())
        if unknown:
            print(
                f"error: unknown scan(s): {', '.join(sorted(unknown))}",
                file=sys.stderr,
            )
            return 1
        scans = {k: v for k, v in scans.items() if k in args.scans}

    if not scans:
        print("No scans to run.")
        return 0

    if args.dry_run:
        print("Dry run — would execute the following scans:\n")
        print(f"  trees_path: {global_cfg.get('trees_path', '(not set)')}")
        print(f"  csvs_path:  {global_cfg.get('csvs_path', '(not set)')}")
        print(f"  db_path:    {global_cfg.get('db_path', '(not set)')}")
        print()
        for name, scan_cfg in scans.items():
            mode = scan_cfg.get("mode", "used")
            disk = scan_cfg.get("disk", "(not set)")
            desc = scan_cfg.get("desc", name)
            print(f"  [{name}] mode={mode} disk={disk}")
            print(f"    desc: {desc}")
        return 0

    with Timer() as running_time:
        # ensure necessary directories exist
        Path(global_cfg["trees_path"]).mkdir(parents=True, exist_ok=True)
        Path(global_cfg["csvs_path"]).mkdir(parents=True, exist_ok=True)
        Path(global_cfg["db_path"]).mkdir(parents=True, exist_ok=True)

        # Run scans
        failed: list[str] = []
        for name, scan_cfg in scans.items():
            try:
                ok = run_scan(
                    name,
                    scan_cfg,
                    global_cfg,
                    verbose=args.verbose,
                    profile=args.profile,
                )
                if not ok:
                    failed.append(name)
            except subprocess.CalledProcessError as exc:
                print(
                    f"\nerror: in {name} scan, '{exc.cmd}' failed: {exc}",
                    file=sys.stderr,
                )
                failed.append(name)
            except Exception as exc:
                print(f"\nerror: scan '{name}': {exc}", file=sys.stderr)
                failed.append(name)

        if failed:
            print(f"\n{len(failed)} scan(s) failed: {', '.join(failed)}")
            return 1

    print("\nAll scans completed successfully.", flush=True)
    print(f"Run time: {running_time}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
