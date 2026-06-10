"""Configuration loading for movies/television ingestion (media-ingestion.kdl).

One config file serves both ``etp movies`` and ``etp television`` — they
share the downloads directory, and per-title overrides rarely collide:

.. code-block:: kdl

    paths {
      downloads-dir "/volume1/docker/pvr/data/downloads"
      movies-source-dir "/volume1/docker/pvr/data/movies"
      movies-dest-dir "/volume1/video/movies"
      television-source-dir "/volume1/docker/pvr/data/television"
      television-dest-dir "/volume1/video/television"
    }
    movie "Blade Runner (1982)" { tmdb 78; edition "Final Cut" }
    series "Severance (2022)" { tvdb 371980; tmdb 95396 }

Mapping names match the source folder name or parsed title,
case-insensitively.
"""

from __future__ import annotations

from pathlib import Path

import kdl

from etp_lib.manifest import escape_kdl
from etp_lib.types import MediaIngestConfig, TitleMapping


class MediaConfigError(Exception):
    """media-ingestion.kdl is malformed; the message says what and where."""


def load_media_config(path: Path | None = None) -> MediaIngestConfig:
    """Load the media ingestion config, falling back to defaults.

    Raises :class:`MediaConfigError` with an actionable message on
    malformed KDL or non-integer provider IDs.
    """
    if path is None:
        from etp_lib import paths as etp_paths

        path = etp_paths.media_config()

    config = MediaIngestConfig()
    if not path.exists():
        return config

    try:
        doc = kdl.parse(path.read_text(encoding="utf-8"))
    except OSError as e:
        raise MediaConfigError(f"cannot read {path}: {e}") from e
    except Exception as e:  # kdl.errors don't share a public base class
        raise MediaConfigError(f"invalid KDL in {path}: {e}") from e

    paths_node = doc.get("paths")
    if paths_node is not None:
        for child in paths_node.nodes:
            if not child.args:
                continue
            val = Path(str(child.args[0]))
            if child.name == "downloads-dir":
                config.downloads_dir = val
            elif child.name == "movies-source-dir":
                config.movies_source_dir = val
            elif child.name == "movies-dest-dir":
                config.movies_dest_dir = val
            elif child.name == "television-source-dir":
                config.television_source_dir = val
            elif child.name == "television-dest-dir":
                config.television_dest_dir = val

    for tool in ("radarr", "sonarr"):
        node = doc.get(tool)
        if node is not None:
            for child in node.nodes:
                if child.name == "url" and child.args:
                    setattr(config, f"{tool}_url", str(child.args[0]))

    _read_mappings(doc, "movie", config.movie_mappings)
    _read_mappings(doc, "series", config.series_mappings)
    return config


def _read_mappings(
    doc: kdl.Document, node_name: str, target: dict[str, TitleMapping]
) -> None:
    def provider_id(name: str, child: kdl.Node) -> int:
        try:
            return int(child.args[0])  # kdl-py parses bare numbers as floats
        except ValueError, TypeError:
            raise MediaConfigError(
                f'{node_name} "{name}": {child.name} ID must be an integer,'
                f" got {child.args[0]!r}"
            ) from None

    for node in doc.getAll(node_name):
        name = str(node.args[0]) if node.args else ""
        if not name:
            continue
        mapping = target.setdefault(name.casefold(), TitleMapping())
        for child in node.nodes:
            if not child.args:
                continue
            if child.name == "tmdb":
                mapping.tmdb_id = provider_id(name, child)
            elif child.name == "tvdb":
                mapping.tvdb_id = provider_id(name, child)
            elif child.name == "edition":
                mapping.edition = str(child.args[0])


def lookup_mapping(
    mappings: dict[str, TitleMapping], *names: str
) -> TitleMapping | None:
    """Find the first mapping matching any of *names*, case-insensitively."""
    for name in names:
        if name and (found := mappings.get(name.casefold())):
            return found
    return None


def save_title_mapping(
    node_name: str,
    name: str,
    *,
    tmdb_id: int | None = None,
    tvdb_id: int | None = None,
    edition: str = "",
    path: Path | None = None,
) -> None:
    """Append a ``movie``/``series`` ID mapping block to the config file.

    Later blocks for the same name override earlier ones on re-read, so
    appending is also how an existing mapping gets corrected.
    """
    if path is None:
        from etp_lib import paths as etp_paths

        path = etp_paths.media_config()

    path.parent.mkdir(parents=True, exist_ok=True)

    lines = [f'\n{node_name} "{escape_kdl(name)}" {{']
    if tmdb_id:
        lines.append(f"  tmdb {tmdb_id}")
    if tvdb_id:
        lines.append(f"  tvdb {tvdb_id}")
    if edition:
        lines.append(f'  edition "{escape_kdl(edition)}"')
    lines.append("}\n")
    with path.open("a", encoding="utf-8") as f:
        f.write("\n".join(lines))
