"""Git-style dispatcher for euterpe-tools.

Usage: etp <command> [args...]

Finds etp-<command> in libexec or on $PATH, then replaces the
current process with it.
"""

from __future__ import annotations

import os
import sys

from etp_lib.paths import find_binary

VERSION = "0.1.0"

BUILTIN_COMMANDS = {
    "anime": "Interactive anime collection manager",
    "catalog": "Run catalog scans across configured directory trees",
    "csv": "Incremental filesystem scanner with CSV output",
    "find": "Search indexed files by regex pattern",
    "tree": "Incremental filesystem scanner with tree output",
}


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

    # Replace current process
    os.execv(exe, [exe] + sys.argv[2:])
    return 0  # unreachable


if __name__ == "__main__":
    sys.exit(main())
