"""Git-style dispatcher for euterpe-tools.

Usage: etp <command> [args...]

Finds etp-<command> in libexec or on $PATH, then replaces the
current process with it.

Orchestrated commands (tree, csv, find, query) run via subprocess
so the dispatcher can catch exit code 2 (no scan exists) and
auto-run etp-scan before retrying.
"""

from __future__ import annotations

import os
import subprocess
import sys

from etp_lib.paths import find_binary

VERSION = "0.1.0"

BUILTIN_COMMANDS = {
    "anime": "Interactive anime collection manager",
    "cas": "Content-addressable blob storage operations",
    "catalog": "Run catalog scans across configured directory trees",
    "csv": "Generate CSV index from indexed database",
    "find": "Search indexed files by regex pattern",
    "init": "Create a default configuration file",
    "meta": "Audio metadata scan, read, and CUE sheet display",
    "query": "Query indexed files and metadata",
    "scan": "Scan a directory and update the database",
    "tree": "Display directory tree from indexed database",
}

# Commands that benefit from orchestration (auto-scan on exit code 2).
# Other commands use os.execv for zero overhead.
ORCHESTRATED = {"tree", "csv", "find", "query"}

EXIT_NO_SCAN = 2


def _extract_target(args: list[str]) -> tuple[str | None, str | None]:
    """Extract the directory (first positional arg) and --db value from argv.

    This is a lightweight parser — just enough to construct an etp-scan
    invocation. It doesn't need to understand all flags.
    """
    directory = None
    db = None
    i = 0
    while i < len(args):
        arg = args[i]
        if arg == "--db" and i + 1 < len(args):
            db = args[i + 1]
            i += 2
            continue
        if arg.startswith("--db="):
            db = arg[5:]
            i += 1
            continue
        if arg.startswith("-") and arg not in ("-R",):
            # Skip flags and their values (heuristic: flags with = are self-contained,
            # others that take values are followed by the value)
            if "=" in arg:
                i += 1
            elif arg in (
                "--root",
                "-o",
                "--output",
                "-I",
                "--ignore",
                "-e",
                "--exclude",
                "--tree",
                "--csv",
                "--find",
                "--format",
            ):
                i += 2  # skip flag + value
            else:
                i += 1  # boolean flag
            continue
        if arg == "-R" and i + 1 < len(args):
            directory = args[i + 1]
            i += 2
            continue
        # First non-flag positional argument is the directory
        if directory is None and not arg.startswith("-"):
            directory = arg
        i += 1
    return directory, db


def print_help() -> None:
    print("etp — euterpe-tools dispatcher\n")
    print("Usage: etp <command> [args...]\n")
    print("Available commands:")
    for cmd, desc in sorted(BUILTIN_COMMANDS.items()):
        print(f"  {cmd:12s} {desc}")
    print()
    print("Run 'etp <command> --help' for command-specific help.")


def main() -> int:
    if len(sys.argv) < 2:
        print_help()
        return 0

    cmd = sys.argv[1]

    if cmd in ("--help", "-h"):
        print_help()
        return 0

    if cmd in ("--version", "-V"):
        print(f"etp {VERSION}")
        return 0

    exe = find_binary(f"etp-{cmd}")
    if exe is None:
        print(f"error: unknown command '{cmd}'", file=sys.stderr)
        print("Run 'etp --help' for available commands.", file=sys.stderr)
        return 1

    if cmd not in ORCHESTRATED:
        os.execv(exe, [exe] + sys.argv[2:])
        return 0  # unreachable

    # Orchestrated command: run via subprocess so we can catch exit code 2.
    result = subprocess.run([exe, *sys.argv[2:]])
    if result.returncode != EXIT_NO_SCAN:
        return result.returncode

    # No scan exists — auto-run etp-scan, then retry.
    scan_exe = find_binary("etp-scan")
    if scan_exe is None:
        print("error: etp-scan not found; cannot auto-scan", file=sys.stderr)
        return EXIT_NO_SCAN

    directory, db = _extract_target(sys.argv[2:])
    if directory is None:
        print(
            "error: no scan exists and no directory given; cannot auto-scan",
            file=sys.stderr,
        )
        return EXIT_NO_SCAN

    if not os.path.isdir(directory):
        # The extracted "directory" isn't a real path (e.g., a regex pattern
        # from etp-find). Don't attempt to scan it.
        return EXIT_NO_SCAN

    scan_cmd: list[str] = [scan_exe, directory]
    if db:
        scan_cmd += ["--db", db]

    print(f"scanning {directory}...", file=sys.stderr)
    scan_result = subprocess.run(scan_cmd)
    if scan_result.returncode != 0:
        print("error: auto-scan failed", file=sys.stderr)
        return scan_result.returncode

    # Retry the original command
    result = subprocess.run([exe, *sys.argv[2:]])
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
