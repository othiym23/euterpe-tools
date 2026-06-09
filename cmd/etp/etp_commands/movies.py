"""Movie collection ingestion (plan/apply).

Thin wrapper binding the shared video CLI to MediaKind.MOVIE — see
etp_commands.video_cli and etp_lib.video_ingest for the implementation.
"""

from __future__ import annotations

import sys

from etp_commands.video_cli import main as video_main
from etp_lib.video_ingest import MediaKind


def main() -> int:
    return video_main(MediaKind.MOVIE)


if __name__ == "__main__":
    sys.exit(main())
