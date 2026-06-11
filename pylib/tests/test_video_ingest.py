"""Tests for the movies/television plan/apply ingestion core."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from etp_lib import video_ingest
from etp_lib.media_scanner import parse_source_filename
from etp_lib.types import (
    AnimeInfo,
    AudioTrack,
    ConflictAction,
    Episode,
    EpisodeType,
    MediaIngestConfig,
    MediaInfo,
    MetadataProvider,
    MovieInfo,
    SearchCandidate,
    TmdbTvInfo,
)
from etp_lib.video_ingest import (
    ApplyOptions,
    Confidence,
    CrossCheck,
    EntryStatus,
    FileEntry,
    ManifestError,
    MediaKind,
    PlanManifest,
    PlanOptions,
    Providers,
    TitleBlock,
    parse_plan_manifest,
    pick_candidate,
    run_apply,
    run_plan,
    scan_downloads,
    scan_managed_tree,
    write_plan_manifest,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

MEDIA_INFO = MediaInfo(
    video_codec="AVC",
    resolution="1080p",
    width=1920,
    height=1080,
    bit_depth=8,
    hdr_type="",
    audio_tracks=[AudioTrack("EAC3", "en", "English", False)],
    encoding_lib="x264",
)

SEVERANCE = AnimeInfo(
    anidb_id=None,
    tvdb_id=371980,
    title_ja="Severance",
    title_en="Severance",
    year=2022,
    episodes=[
        Episode(1, EpisodeType.REGULAR, "Good News About Hell", "", "", season=1),
        Episode(2, EpisodeType.REGULAR, "Half Loop", "", "", season=1),
        Episode(5, EpisodeType.SPECIAL, "Inside Severance", "", "s0e05", season=0),
    ],
)

HEAT = MovieInfo(
    tmdb_id=949,
    title="Heat",
    year=1995,
    original_title="Heat",
    imdb_id="tt0113277",
    aliases=["Heat"],
)


def _with_overrides(providers: Providers, overrides: dict) -> Providers:
    for name, value in overrides.items():
        setattr(providers, name, value)
    return providers


def tv_providers(**overrides) -> Providers:
    providers = Providers(
        tvdb_search_series=lambda q, key, no_cache=False: [
            SearchCandidate(MetadataProvider.TVDB, 371980, "Severance", 2022)
        ],
        tvdb_fetch_series=lambda i, key, no_cache=False: SEVERANCE,
        tmdb_search_tv=lambda q, y, key, no_cache=False: [
            SearchCandidate(MetadataProvider.TMDB, 95396, "Severance", 2022)
        ],
        tmdb_fetch_tv=lambda i, key, no_cache=False: TmdbTvInfo(
            tmdb_id=95396, title="Severance", year=2022, tvdb_id=371980
        ),
        analyze=lambda p: MEDIA_INFO,
        tmdb_key="key",
        tvdb_key="key",
    )
    return _with_overrides(providers, overrides)


def movie_providers(**overrides) -> Providers:
    providers = Providers(
        tmdb_search_movie=lambda q, y, key, no_cache=False: [
            SearchCandidate(MetadataProvider.TMDB, 949, "Heat", 1995)
        ],
        tmdb_fetch_movie=lambda i, key, no_cache=False: HEAT,
        tvdb_search_movies=lambda q, key, no_cache=False: [
            SearchCandidate(MetadataProvider.TVDB, 137, "Heat", 1995)
        ],
        analyze=lambda p: MEDIA_INFO,
        tmdb_key="key",
        tvdb_key="key",
    )
    return _with_overrides(providers, overrides)


@pytest.fixture
def register(monkeypatch, tmp_path):
    """Redirect the shared ingest register to a per-test file."""
    reg_file = tmp_path / "register.json"

    def load() -> set[str]:
        if reg_file.exists():
            return set(json.loads(reg_file.read_text(encoding="utf-8")))
        return set()

    def save(copied: set[str]) -> None:
        reg_file.write_text(json.dumps(sorted(copied)), encoding="utf-8")

    monkeypatch.setattr(video_ingest, "load_register", load)
    monkeypatch.setattr(video_ingest, "save_register", save)
    return reg_file


def _mkfile(path: Path, size: int = 64) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


@pytest.fixture
def tv_tree(tmp_path):
    """Synthetic Sonarr tree + empty destination root."""
    src = tmp_path / "pvr" / "television"
    show = src / "Severance (2022)"
    _mkfile(
        show / "Season 01" / "Severance - S01E01 - Good News About Hell WEBDL-1080p.mkv"
    )
    _mkfile(show / "Season 01" / "Severance - S01E02 - Half Loop WEBDL-1080p.mkv")
    _mkfile(show / "Specials" / "Severance - s00e05 - Inside Severance WEBDL-1080p.mkv")
    dest = tmp_path / "video" / "television"
    dest.mkdir(parents=True)
    return src, dest


@pytest.fixture
def movie_tree(tmp_path):
    """Synthetic Radarr tree + empty destination root."""
    src = tmp_path / "pvr" / "movies"
    _mkfile(
        src
        / "Heat [Heat] (1995)"
        / "Heat [Heat] (1995) - complete movie - [BONE Bluray-1080p,,x265,8bit,AAC].mkv"
    )
    dest = tmp_path / "video" / "movies"
    dest.mkdir(parents=True)
    return src, dest


def tv_config(src: Path, dest: Path) -> MediaIngestConfig:
    return MediaIngestConfig(television_source_dir=src, television_dest_dir=dest)


def movie_config(src: Path, dest: Path) -> MediaIngestConfig:
    return MediaIngestConfig(movies_source_dir=src, movies_dest_dir=dest)


def plan_opts(tmp_path: Path, **kw) -> PlanOptions:
    kw.setdefault("output", tmp_path / "plan.kdl")
    kw.setdefault("managed", True)
    return PlanOptions(**kw)


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------


class TestScanManagedTree:
    def test_tv_seasons_and_specials(self, tv_tree):
        src, _ = tv_tree
        titles = scan_managed_tree(src, MediaKind.TV)
        assert len(titles) == 1
        t = titles[0]
        assert t.raw_title == "Severance (2022)"
        assert t.title == "Severance"
        assert t.year == 2022
        by_ep = {(f.season, f.episode) for f in t.files}
        assert by_ep == {(1, 1), (1, 2), (0, 5)}

    def test_movie_alt_title_and_marker(self, movie_tree):
        src, _ = movie_tree
        titles = scan_managed_tree(src, MediaKind.MOVIE)
        assert len(titles) == 1
        t = titles[0]
        assert t.title == "Heat"
        assert t.alt_title == "Heat"
        assert t.year == 1995
        assert t.edition == ""

    def test_movie_edition_extracted(self, tmp_path):
        src = tmp_path / "movies"
        _mkfile(
            src
            / "Blade Runner (1982)"
            / "Blade Runner (1982) - complete movie - Extended Remastered"
            " [X Bluray-1080p,,x265,8bit,AAC].mkv"
        )
        titles = scan_managed_tree(src, MediaKind.MOVIE)
        assert titles[0].edition == "Extended Remastered"

    def test_movie_parts(self, tmp_path):
        src = tmp_path / "movies"
        _mkfile(src / "Goemon (2009)" / "Goemon (2009) - cd1.mkv")
        _mkfile(src / "Goemon (2009)" / "Goemon (2009) - cd2.mkv")
        titles = scan_managed_tree(src, MediaKind.MOVIE)
        assert sorted(f.part for f in titles[0].files if f.part is not None) == [1, 2]
        assert all(f.part is not None for f in titles[0].files)

    def test_empty_dirs_skipped(self, tmp_path):
        src = tmp_path / "movies"
        (src / "Empty (2000)").mkdir(parents=True)
        assert scan_managed_tree(src, MediaKind.MOVIE) == []

    def test_missing_root(self, tmp_path):
        assert scan_managed_tree(tmp_path / "nope", MediaKind.TV) == []


# ---------------------------------------------------------------------------
# Candidate picking
# ---------------------------------------------------------------------------


def _cand(id: int, title: str, year: int) -> SearchCandidate:
    return SearchCandidate(MetadataProvider.TMDB, id, title, year)


class TestPickCandidate:
    def test_exact(self):
        conf, chosen = pick_candidate(
            [_cand(1, "Heat", 1995), _cand(2, "Heat", 1986)], "Heat", "", 1995
        )
        assert conf is Confidence.EXACT
        assert chosen is not None and chosen.id == 1

    def test_exact_via_alt_title(self):
        conf, chosen = pick_candidate(
            [_cand(1, "Olde Boye", 2003)], "Oldboy", "Olde Boye", 2003
        )
        assert conf is Confidence.EXACT

    def test_single_fuzzy_is_high(self):
        conf, chosen = pick_candidate([_cand(1, "Heat: Redux", 1995)], "Heat", "", 1995)
        assert conf is Confidence.HIGH
        assert chosen is not None

    def test_multiple_exact_is_ambiguous(self):
        conf, chosen = pick_candidate(
            [_cand(1, "Heat", 1995), _cand(2, "Heat", 1995)], "Heat", "", 1995
        )
        assert conf is Confidence.AMBIGUOUS
        assert chosen is None

    def test_multiple_fuzzy_is_ambiguous(self):
        conf, _ = pick_candidate(
            [_cand(1, "Heat 2", 1995), _cand(2, "White Heat", 1949)], "Heat", "", 1995
        )
        assert conf is Confidence.AMBIGUOUS

    def test_no_candidates(self):
        conf, chosen = pick_candidate([], "Heat", "", 1995)
        assert conf is Confidence.NONE

    def test_year_zero_matches_any(self):
        conf, _ = pick_candidate([_cand(1, "Heat", 1995)], "Heat", "", 0)
        assert conf is Confidence.EXACT

    def test_punctuation_normalized(self):
        conf, _ = pick_candidate([_cand(1, "WALL·E", 2008)], "WALL-E", "", 2008)
        assert conf is Confidence.EXACT


# ---------------------------------------------------------------------------
# Manifest round-trip
# ---------------------------------------------------------------------------


def _sample_manifest(kind: MediaKind) -> PlanManifest:
    block = TitleBlock(
        raw_title="Severance (2022)" if kind is MediaKind.TV else "Heat [Heat] (1995)",
        title="Severance" if kind is MediaKind.TV else "Heat",
        original_title="" if kind is MediaKind.TV else "올드보이",
        year=2022 if kind is MediaKind.TV else 1995,
        tmdb_id=95396,
        tvdb_id=371980,
        imdb_id="tt11280740",
        edition="" if kind is MediaKind.TV else "Final Cut",
        confidence=Confidence.EXACT,
        cross_check=CrossCheck.MISMATCH,
        cross_check_note='points at "another" id',
        dest_dir="Severance (2022) {tvdb-371980}",
        note="reusing existing library directory",
        candidates=[SearchCandidate(MetadataProvider.TVDB, 1, 'A "B" C', 1999)],
        entries=[
            FileEntry(
                source="/src/a.mkv",
                size=64,
                status=EntryStatus.READY,
                dest="Season 01/a.mkv",
                season=1,
                number=1,
                title="Pilot",
            ),
            FileEntry(
                source="/src/b.mkv",
                size=65,
                status=EntryStatus.CONFLICT,
                dest="Season 01/b.mkv",
                season=1,
                number=2,
                episodes=[2, 3],
                conflict=ConflictAction.REPLACE,
                note="destination exists (99 bytes)",
            ),
            FileEntry(source="/src/c.mkv", size=66, status=EntryStatus.NEEDS_ID),
        ],
    )
    return PlanManifest(
        kind=kind,
        created="2026-06-09T18:00:00+00:00",
        source_mode=kind.managed_mode,
        dest_root="/video/dest",
        blocks=[block],
    )


class TestManifestRoundtrip:
    @pytest.mark.parametrize("kind", [MediaKind.TV, MediaKind.MOVIE])
    def test_full_roundtrip(self, kind, tmp_path):
        manifest = _sample_manifest(kind)
        path = tmp_path / "plan.kdl"
        write_plan_manifest(manifest, path)
        parsed = parse_plan_manifest(path)
        assert parsed == manifest

    def test_missing_meta(self, tmp_path):
        path = tmp_path / "plan.kdl"
        path.write_text('series "X" {\n}\n', encoding="utf-8")
        with pytest.raises(ManifestError, match="meta"):
            parse_plan_manifest(path)

    def test_bad_schema_version(self, tmp_path):
        manifest = _sample_manifest(MediaKind.TV)
        manifest.schema_version = 99
        path = tmp_path / "plan.kdl"
        write_plan_manifest(manifest, path)
        with pytest.raises(ManifestError, match="schema-version"):
            parse_plan_manifest(path)

    def test_invalid_kdl(self, tmp_path):
        path = tmp_path / "plan.kdl"
        path.write_text("meta { unterminated", encoding="utf-8")
        with pytest.raises(ManifestError, match="invalid KDL"):
            parse_plan_manifest(path)

    @pytest.mark.parametrize(
        ("field", "value", "match"),
        [
            ("conflict", "kep", "bad conflict"),
            ("status", "rady", "bad status"),
        ],
    )
    def test_bad_editable_enum_is_clean_error(self, tmp_path, field, value, match):
        """`conflict` and `status` are documented as hand-editable; a
        typo must raise ManifestError, never a raw ValueError."""
        manifest = _sample_manifest(MediaKind.TV)
        path = tmp_path / "plan.kdl"
        write_plan_manifest(manifest, path)
        text = path.read_text(encoding="utf-8")
        if field == "conflict":
            text = text.replace(
                'status "ready"', 'status "ready"\n    conflict "kep"', 1
            )
        else:
            text = text.replace('status "ready"', f'status "{value}"', 1)
        path.write_text(text, encoding="utf-8")
        with pytest.raises(ManifestError, match=match):
            parse_plan_manifest(path)

    @given(
        title=st.text(
            alphabet=st.characters(blacklist_categories=("Cs", "Cc")),
            min_size=1,
            max_size=40,
        ),
        year=st.integers(min_value=0, max_value=2100),
    )
    def test_title_roundtrip_property(self, title, year, tmp_path_factory):
        tmp = tmp_path_factory.mktemp("prop")
        manifest = PlanManifest(
            kind=MediaKind.MOVIE,
            created="2026-06-09T18:00:00+00:00",
            source_mode="radarr",
            dest_root="/video/dest",
            blocks=[
                TitleBlock(
                    raw_title=title,
                    title=title,
                    year=year,
                    confidence=Confidence.NONE,
                )
            ],
        )
        path = tmp / "plan.kdl"
        write_plan_manifest(manifest, path)
        parsed = parse_plan_manifest(path)
        assert parsed.blocks[0].raw_title == title
        assert parsed.blocks[0].year == year


# ---------------------------------------------------------------------------
# Plan pipeline
# ---------------------------------------------------------------------------


class TestRunPlanTv:
    def test_happy_path(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        opts = plan_opts(tmp_path, json_output=True)
        rc = run_plan(MediaKind.TV, tv_config(src, dest), opts, tv_providers())
        assert rc == 0

        summary = json.loads(capsys.readouterr().out)
        assert summary["action"] == "plan"
        assert summary["counts"] == {
            "ready": 3,
            "needs_id": 0,
            "conflict": 0,
            "skip": 0,
            "already_ingested": 0,
        }

        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.tvdb_id == 371980
        assert block.tmdb_id == 95396
        assert block.cross_check is CrossCheck.OK
        assert block.dest_dir == "Severance (2022) {tvdb-371980}"
        dests = sorted(e.dest for e in block.entries)
        assert dests[0].startswith(
            "Season 01/Severance (2022) - s01e01 - Good News About Hell ["
        )
        assert dests[2].startswith(
            "Specials/Severance (2022) - s00e05 - Inside Severance ["
        )

    def test_reuses_existing_library_dir(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        (dest / "Severance (2022)").mkdir()
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), tv_providers()
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].dest_dir == "Severance (2022)"

    def test_conflict_detected(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), tv_providers()
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entry = manifest.blocks[0].entries[0]
        # Place a file at the planned destination, then re-plan.
        _mkfile(dest / manifest.blocks[0].dest_dir / entry.dest, size=99)
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), tv_providers()
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entry = next(e for e in manifest.blocks[0].entries if e.source == entry.source)
        assert entry.status is EntryStatus.CONFLICT
        assert entry.conflict is ConflictAction.KEEP

    def test_ambiguous_becomes_needs_id(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        providers = tv_providers(
            tvdb_search_series=lambda q, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TVDB, 1, "Severance", 2022),
                SearchCandidate(MetadataProvider.TVDB, 2, "Severance", 2022),
            ]
        )
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.confidence is Confidence.AMBIGUOUS
        assert len(block.candidates) == 2
        assert all(e.status is EntryStatus.NEEDS_ID for e in block.entries)

    def test_cross_check_mismatch_is_warning(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        providers = tv_providers(
            tmdb_fetch_tv=lambda i, key, no_cache=False: TmdbTvInfo(
                tmdb_id=95396, title="Severance", year=2022, tvdb_id=99999
            )
        )
        opts = plan_opts(tmp_path, json_output=True)
        rc = run_plan(MediaKind.TV, tv_config(src, dest), opts, providers)
        assert rc == 0
        summary = json.loads(capsys.readouterr().out)
        assert summary["warnings"][0]["kind"] == "cross-check-mismatch"
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].cross_check is CrossCheck.MISMATCH
        # Mismatch is a warning, not fatal: entries still ready.
        assert all(e.status is EntryStatus.READY for e in manifest.blocks[0].entries)

    def test_register_filters_sources(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        files = sorted((src / "Severance (2022)").rglob("*.mkv"))
        register.write_text(
            json.dumps([str(f.resolve()) for f in files]), encoding="utf-8"
        )
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), tv_providers()
        )
        assert rc == 2

    def test_force_overrides_register(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        files = sorted((src / "Severance (2022)").rglob("*.mkv"))
        register.write_text(
            json.dumps([str(f.resolve()) for f in files]), encoding="utf-8"
        )
        rc = run_plan(
            MediaKind.TV,
            tv_config(src, dest),
            plan_opts(tmp_path, force=True),
            tv_providers(),
        )
        assert rc == 0

    def test_pattern_filter(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        rc = run_plan(
            MediaKind.TV,
            tv_config(src, dest),
            plan_opts(tmp_path, pattern="no-such-show"),
            tv_providers(),
        )
        assert rc == 2

    def test_missing_source_dir_fails_fast(self, tmp_path, register):
        config = tv_config(tmp_path / "nope", tmp_path / "also-nope")
        rc = run_plan(MediaKind.TV, config, plan_opts(tmp_path), tv_providers())
        assert rc == 1

    def test_provider_failure_degrades(self, tv_tree, tmp_path, register):
        src, dest = tv_tree

        def boom(q, key, no_cache=False):
            raise OSError("network down")

        providers = tv_providers(tvdb_search_series=boom)
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.confidence is Confidence.NONE
        assert "network down" in block.note
        assert all(e.status is EntryStatus.NEEDS_ID for e in block.entries)


class TestRunPlanMovie:
    def test_happy_path(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.tmdb_id == 949
        assert block.imdb_id == "tt0113277"
        assert block.tvdb_id == 137  # recorded from the cross-check
        assert block.cross_check is CrossCheck.OK
        assert block.dest_dir == "Heat (1995) {tmdb-949}"
        entry = block.entries[0]
        assert entry.status is EntryStatus.READY
        assert entry.dest.startswith("Heat (1995) {tmdb-949} [")
        assert "complete movie" not in entry.dest

    def test_edition_in_dest_dir(self, tmp_path, register):
        src = tmp_path / "movies"
        _mkfile(
            src
            / "Blade Runner (1982)"
            / "Blade Runner (1982) - complete movie - Final Cut [X,1080p].mkv"
        )
        dest = tmp_path / "dest"
        dest.mkdir()
        providers = movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TMDB, 78, "Blade Runner", 1982)
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=78, title="Blade Runner", year=1982
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )
        rc = run_plan(
            MediaKind.MOVIE, movie_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.dest_dir == "Blade Runner (1982) {tmdb-78} {edition-Final Cut}"
        assert block.cross_check is CrossCheck.UNAVAILABLE

    def test_config_mapping_overrides_search(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        config = movie_config(src, dest)
        from etp_lib.types import TitleMapping

        config.movie_mappings["heat [heat] (1995)"] = TitleMapping(tmdb_id=949)
        fetched: list[int] = []

        def fetch(i, key, no_cache=False):
            fetched.append(i)
            return HEAT

        providers = movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: pytest.fail(
                "search should not run when an ID is mapped"
            ),
            tmdb_fetch_movie=fetch,
        )
        rc = run_plan(MediaKind.MOVIE, config, plan_opts(tmp_path), providers)
        assert rc == 0
        assert fetched == [949]
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].confidence is Confidence.EXACT


class TestRefine:
    def test_ids_carried_into_replan(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        # First plan: ambiguous, needs-id.
        ambiguous = tv_providers(
            tvdb_search_series=lambda q, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TVDB, 1, "Severance", 2022),
                SearchCandidate(MetadataProvider.TVDB, 2, "Severance", 2022),
            ]
        )
        first = plan_opts(tmp_path, output=tmp_path / "first.kdl")
        assert run_plan(MediaKind.TV, tv_config(src, dest), first, ambiguous) == 0

        # Agent edits the manifest: fills in the tvdb ID.
        text = (tmp_path / "first.kdl").read_text(encoding="utf-8")
        text = text.replace('confidence "ambiguous"', "tvdb 371980")
        (tmp_path / "first.kdl").write_text(text, encoding="utf-8")

        second = plan_opts(
            tmp_path, output=tmp_path / "second.kdl", refine=tmp_path / "first.kdl"
        )
        assert run_plan(MediaKind.TV, tv_config(src, dest), second, ambiguous) == 0
        manifest = parse_plan_manifest(tmp_path / "second.kdl")
        block = manifest.blocks[0]
        assert block.tvdb_id == 371980
        assert block.confidence is Confidence.EXACT
        assert all(e.status is EntryStatus.READY for e in block.entries)

    def test_skip_carried_into_replan(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        first = plan_opts(tmp_path, output=tmp_path / "first.kdl")
        assert run_plan(MediaKind.TV, tv_config(src, dest), first, tv_providers()) == 0
        manifest = parse_plan_manifest(tmp_path / "first.kdl")
        skipped_source = manifest.blocks[0].entries[0].source
        text = (tmp_path / "first.kdl").read_text(encoding="utf-8")
        text = text.replace('status "ready"', 'status "skip"', 1)
        (tmp_path / "first.kdl").write_text(text, encoding="utf-8")

        second = plan_opts(
            tmp_path, output=tmp_path / "second.kdl", refine=tmp_path / "first.kdl"
        )
        assert run_plan(MediaKind.TV, tv_config(src, dest), second, tv_providers()) == 0
        manifest = parse_plan_manifest(tmp_path / "second.kdl")
        entry = next(
            e for e in manifest.blocks[0].entries if e.source == skipped_source
        )
        assert entry.status is EntryStatus.SKIP

    def test_kind_mismatch_rejected(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        manifest = _sample_manifest(MediaKind.MOVIE)
        write_plan_manifest(manifest, tmp_path / "movie.kdl")
        opts = plan_opts(tmp_path, refine=tmp_path / "movie.kdl")
        rc = run_plan(MediaKind.TV, tv_config(src, dest), opts, tv_providers())
        assert rc == 1


# ---------------------------------------------------------------------------
# Apply pipeline
# ---------------------------------------------------------------------------


def _last_json(capsys) -> dict:
    """Parse the last stdout line as JSON (earlier lines are human output)."""
    return json.loads(capsys.readouterr().out.strip().splitlines()[-1])


def _plan_tv(tv_tree, tmp_path, register, **plan_kw) -> Path:
    src, dest = tv_tree
    path = plan_kw.pop("output", tmp_path / "plan.kdl")
    opts = plan_opts(tmp_path, output=path, **plan_kw)
    rc = run_plan(MediaKind.TV, tv_config(src, dest), opts, tv_providers())
    assert rc == 0
    return path


class TestRunApply:
    @pytest.fixture(autouse=True)
    def plain_copy(self, monkeypatch):
        """CI filesystems lack reflink support; copy plainly in tests.

        Production keeps `cp --reflink=always` on Linux by design (Btrfs
        NAS, ADR 2026-03-24-02) — only the test substitutes the copier.
        """

        def fake_copy(src: Path, dst: Path, dry_run: bool = False) -> bool:
            if dry_run:
                return True
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)
            return True

        monkeypatch.setattr(video_ingest, "copy_reflink", fake_copy)

    def test_happy_path(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions(json_output=True))
        assert rc == 0
        summary = _last_json(capsys)
        assert summary["ok"] is True
        assert summary["counts"]["copied"] == 3

        copied = sorted(p.name for p in dest.rglob("*.mkv"))
        assert len(copied) == 3
        assert json.loads(register.read_text(encoding="utf-8"))  # sources recorded

    def test_idempotent_reapply(self, tv_tree, tmp_path, register, capsys):
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        assert run_apply(MediaKind.TV, plan_path, ApplyOptions()) == 0
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions(json_output=True))
        assert rc == 2  # everything already done
        summary = _last_json(capsys)
        assert summary["counts"]["kept"] == 3
        assert summary["counts"]["failed"] == 0

    def test_dry_run_copies_nothing(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions(dry_run=True))
        assert rc == 0
        assert list(dest.rglob("*.mkv")) == []
        assert not register.exists()  # register not saved on dry-run

    def test_source_drift_rejected(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        # A source file grows after planning.
        victim = next((src / "Severance (2022)" / "Season 01").glob("*.mkv"))
        victim.write_bytes(b"y" * 999)
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions(json_output=True))
        assert rc == 1
        summary = _last_json(capsys)
        assert summary["ok"] is False
        assert any("size changed" in p for p in summary["problems"])
        assert list(dest.rglob("*.mkv")) == []  # nothing copied at all

    def test_unresolved_needs_id_rejected(self, tv_tree, tmp_path, register):
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        text = plan_path.read_text(encoding="utf-8")
        plan_path.write_text(
            text.replace('status "ready"', 'status "needs-id"', 1), encoding="utf-8"
        )
        assert run_apply(MediaKind.TV, plan_path, ApplyOptions()) == 1

    def test_dest_appeared_rejected(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        manifest = parse_plan_manifest(plan_path)
        block = manifest.blocks[0]
        # A different-size file appears at a planned destination after plan.
        _mkfile(dest / block.dest_dir / block.entries[0].dest, size=999)
        assert run_apply(MediaKind.TV, plan_path, ApplyOptions()) == 1

    def test_conflict_replace(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        manifest = parse_plan_manifest(plan_path)
        block = manifest.blocks[0]
        existing = _mkfile(dest / block.dest_dir / block.entries[0].dest, size=999)
        # Re-plan sees the conflict; choose replace.
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        text = plan_path.read_text(encoding="utf-8")
        plan_path.write_text(
            text.replace('conflict "keep"', 'conflict "replace"'), encoding="utf-8"
        )
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions())
        assert rc == 0
        assert existing.stat().st_size == 64  # replaced by the 64-byte source

    def test_conflict_both(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        manifest = parse_plan_manifest(plan_path)
        block = manifest.blocks[0]
        _mkfile(dest / block.dest_dir / block.entries[0].dest, size=999)
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        text = plan_path.read_text(encoding="utf-8")
        plan_path.write_text(
            text.replace('conflict "keep"', 'conflict "both"'), encoding="utf-8"
        )
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions())
        assert rc == 0
        season = dest / block.dest_dir / "Season 01"
        # Both the original 999-byte file and a CRC-suffixed copy exist.
        first_ep = [p for p in season.iterdir() if "s01e01" in p.name]
        assert len(first_ep) == 2

    def test_conflict_keep(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        manifest = parse_plan_manifest(plan_path)
        block = manifest.blocks[0]
        existing = _mkfile(dest / block.dest_dir / block.entries[0].dest, size=999)
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions(json_output=True))
        assert rc == 0
        summary = _last_json(capsys)
        assert summary["counts"]["kept"] == 1
        assert summary["counts"]["copied"] == 2
        assert existing.stat().st_size == 999  # untouched

    def test_kind_mismatch(self, tmp_path, register):
        manifest = _sample_manifest(MediaKind.MOVIE)
        path = tmp_path / "movie.kdl"
        write_plan_manifest(manifest, path)
        assert run_apply(MediaKind.TV, path, ApplyOptions()) == 1

    def test_skip_entries_not_copied(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        text = plan_path.read_text(encoding="utf-8")
        plan_path.write_text(
            text.replace('status "ready"', 'status "skip"', 1), encoding="utf-8"
        )
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions(json_output=True))
        assert rc == 0
        summary = _last_json(capsys)
        assert summary["counts"]["skipped"] == 1
        assert summary["counts"]["copied"] == 2


# ---------------------------------------------------------------------------
# Downloads mode
# ---------------------------------------------------------------------------


class TestScanDownloads:
    def test_tv_groups_by_title(self, tmp_path):
        dl = tmp_path / "downloads"
        _mkfile(dl / "The.Expanse.S02E01.1080p.WEB-DL.mkv")
        _mkfile(dl / "The.Expanse.S02E02.1080p.WEB-DL.mkv")
        _mkfile(dl / "Heat.1995.1080p.BluRay.x264-GRP.mkv")  # not episodic
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.TV)
        assert len(titles) == 1
        t = titles[0]
        assert t.title == "The Expanse"
        assert {(f.season, f.episode) for f in t.files} == {(2, 1), (2, 2)}

    def test_movies_exclude_episodic(self, tmp_path):
        dl = tmp_path / "downloads"
        _mkfile(dl / "Heat.1995.1080p.BluRay.x264-GRP.mkv")
        _mkfile(dl / "The.Expanse.S02E01.1080p.WEB-DL.mkv")  # episodic
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 1
        t = titles[0]
        assert t.title == "Heat"
        assert t.year == 1995

    def test_batch_dir_name_fallback(self, tmp_path):
        dl = tmp_path / "downloads"
        _mkfile(dl / "Heat.1995.1080p.BluRay.x264-GRP" / "heat-grp.mkv")
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 1
        assert titles[0].title == "Heat"
        assert titles[0].year == 1995


class TestRunPlanDownloads:
    def test_tv_downloads_mode(self, tmp_path, register):
        dl = tmp_path / "downloads"
        _mkfile(dl / "Severance.S01E01.1080p.WEB-DL.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(downloads_dir=dl, television_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.TV, config, opts, tv_providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.source_mode == "downloads"
        block = manifest.blocks[0]
        assert block.tvdb_id == 371980
        entry = block.entries[0]
        assert entry.status is EntryStatus.READY
        assert entry.dest.startswith(
            "Season 01/Severance (2022) - s01e01 - Good News About Hell ["
        )

    def test_downloads_dir_missing_fails_fast(self, tmp_path, register):
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(
            downloads_dir=tmp_path / "nope", television_dest_dir=dest
        )
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        assert run_plan(MediaKind.TV, config, opts, tv_providers()) == 1


class TestManagedScanGroupCleanup:
    def test_radarr_placeholder_group_stripped(self, movie_tree):
        src, _ = movie_tree
        _mkfile(
            src
            / "Duel (1971)"
            / "Duel (1971) - complete movie - [Radarr Remux-1080p,,AVC,8bit,DTS].mkv"
        )
        titles = scan_managed_tree(src, MediaKind.MOVIE)
        duel = next(t for t in titles if t.title == "Duel")
        assert duel.files[0].source.parsed.release_group == ""


class TestOriginalTitleNaming:
    """Directory names lead with the original-language title."""

    def test_movie_dual_title_dest_dir(self, tmp_path, register):
        src = tmp_path / "movies"
        _mkfile(
            src
            / "Oldboy (2003)"
            / "Oldboy (2003) - complete movie - [X Bluray-1080p,,x265,8bit,AAC].mkv"
        )
        dest = tmp_path / "dest"
        dest.mkdir()
        providers = movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TMDB, 670, "Oldboy", 2003)
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=670,
                title="Oldboy",
                year=2003,
                original_title="올드보이",
                aliases=["Oldboy", "올드보이"],
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )
        rc = run_plan(
            MediaKind.MOVIE, movie_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.original_title == "올드보이"
        assert block.dest_dir == "올드보이 [Oldboy] (2003) {tmdb-670}"
        assert block.entries[0].dest.startswith("올드보이 [Oldboy] (2003) {tmdb-670} [")

    def test_tv_dual_title_from_tvdb_translation(self, tmp_path, register):
        src = tmp_path / "television"
        show = src / "Ayaka (2024)"
        _mkfile(show / "Season 01" / "Ayaka - S01E01 - First WEBDL-1080p.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        info = AnimeInfo(
            anidb_id=None,
            tvdb_id=443158,
            title_ja="彩香ちゃんは弘子先輩に恋してる",
            title_en="Ayaka is in Love with Hiroko!",
            year=2024,
            episodes=[Episode(1, EpisodeType.REGULAR, "First", "", "", season=1)],
        )
        providers = tv_providers(
            tvdb_search_series=lambda q, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TVDB, 443158, "Ayaka", 2024)
            ],
            tvdb_fetch_series=lambda i, key, no_cache=False: info,
            tmdb_search_tv=lambda q, y, key, no_cache=False: [],
        )
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.original_title == "彩香ちゃんは弘子先輩に恋してる"
        assert block.dest_dir == (
            "彩香ちゃんは弘子先輩に恋してる [Ayaka is in Love with Hiroko!]"
            " (2024) {tvdb-443158}"
        )
        # Episode files keep the concise English title (anime convention).
        assert block.entries[0].dest.startswith(
            "Season 01/Ayaka is in Love with Hiroko! (2024) - s01e01"
        )

    def test_existing_native_dir_reused(self, tmp_path, register):
        src = tmp_path / "movies"
        _mkfile(
            src
            / "Oldboy (2003)"
            / "Oldboy (2003) - complete movie - [X Bluray-1080p].mkv"
        )
        dest = tmp_path / "dest"
        (dest / "올드보이 [Oldboy] (2003)").mkdir(parents=True)
        providers = movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TMDB, 670, "Oldboy", 2003)
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=670,
                title="Oldboy",
                year=2003,
                original_title="올드보이",
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )
        rc = run_plan(
            MediaKind.MOVIE, movie_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].dest_dir == "올드보이 [Oldboy] (2003)"
        assert manifest.blocks[0].note == "reusing existing library directory"


class TestDuplicateDests:
    """Two sources computing the same destination must never silently stack."""

    def _two_identical_movies(self, tmp_path):
        src = tmp_path / "movies"
        # Two video files in one movie folder, no part markers: with
        # identical analyzed metadata both compute the same destination.
        _mkfile(src / "Heat (1995)" / "Heat.1995.GRP-a.mkv")
        _mkfile(src / "Heat (1995)" / "Heat.1995.GRP-b.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        return src, dest

    def test_plan_marks_second_as_skip(self, tmp_path, register):
        src, dest = self._two_identical_movies(tmp_path)
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entries = manifest.blocks[0].entries
        statuses = sorted(str(e.status) for e in entries)
        assert statuses == ["ready", "skip"]
        skipped = next(e for e in entries if e.status is EntryStatus.SKIP)
        assert "additional version" in skipped.note

    def test_apply_rejects_handmade_duplicates(self, tmp_path, register):
        src, dest = self._two_identical_movies(tmp_path)
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        # Re-mark the skipped duplicate as ready, as a careless edit would.
        text = (tmp_path / "plan.kdl").read_text(encoding="utf-8")
        text = text.replace('status "skip"', 'status "ready"')
        (tmp_path / "plan.kdl").write_text(text, encoding="utf-8")
        assert run_apply(MediaKind.MOVIE, tmp_path / "plan.kdl", ApplyOptions()) == 1
        assert list(dest.rglob("*.mkv")) == []  # nothing copied


class TestReplaceSafety:
    def test_failed_replace_preserves_existing_dest(
        self, tv_tree, tmp_path, register, monkeypatch
    ):
        src, dest = tv_tree
        plan_path = _plan_tv(tv_tree, tmp_path, register)
        manifest = parse_plan_manifest(plan_path)
        block = manifest.blocks[0]
        existing = _mkfile(dest / block.dest_dir / block.entries[0].dest, size=999)

        plan_path = _plan_tv(tv_tree, tmp_path, register)
        text = plan_path.read_text(encoding="utf-8")
        plan_path.write_text(
            text.replace('conflict "keep"', 'conflict "replace"'), encoding="utf-8"
        )

        monkeypatch.setattr(
            video_ingest, "copy_reflink", lambda src, dst, dry_run=False: False
        )
        rc = run_apply(MediaKind.TV, plan_path, ApplyOptions())
        assert rc == 1  # the replace failed...
        assert existing.stat().st_size == 999  # ...and the original survives
        leftovers = [p for p in existing.parent.iterdir() if "etp-tmp" in p.name]
        assert leftovers == []


class TestCrossCheckWithoutSecondaryKey:
    def test_movie_cross_check_unavailable(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        providers = movie_providers(
            tvdb_key="",
            tvdb_search_movies=lambda q, key, no_cache=False: pytest.fail(
                "cross-check must not run without a TheTVDB key"
            ),
        )
        rc = run_plan(
            MediaKind.MOVIE, movie_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.cross_check is CrossCheck.UNAVAILABLE
        assert block.tmdb_id == 949  # primary resolution unaffected

    def test_tv_cross_check_unavailable(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        providers = tv_providers(
            tmdb_key="",
            tmdb_search_tv=lambda q, y, key, no_cache=False: pytest.fail(
                "cross-check must not run without a TMDB key"
            ),
        )
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].cross_check is CrossCheck.UNAVAILABLE
        assert manifest.blocks[0].tvdb_id == 371980


class TestPlanVerbose:
    def test_verbose_reports_each_title(self, tv_tree, tmp_path, register, capsys):
        src, dest = tv_tree
        opts = plan_opts(tmp_path, verbose=True)
        rc = run_plan(MediaKind.TV, tv_config(src, dest), opts, tv_providers())
        assert rc == 0
        out = capsys.readouterr().out
        assert "Severance (2022): exact {tvdb-371980}" in out


# ---------------------------------------------------------------------------
# Plan-quality fixes: same-size detection, library disambiguation, arr APIs
# ---------------------------------------------------------------------------


class TestSameSizeDetection:
    def test_same_size_different_name_is_conflict_keep(
        self, movie_tree, tmp_path, register
    ):
        src, dest = movie_tree
        # The library already holds the same encode under the old naming.
        existing = _mkfile(dest / "Heat (1995)" / "Heat (1995) - complete movie.mkv")
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entry = manifest.blocks[0].entries[0]
        assert entry.status is EntryStatus.CONFLICT
        assert entry.conflict is ConflictAction.KEEP
        assert entry.dest == existing.name
        assert "same-size" in entry.note

    def test_apply_keep_registers_without_copying(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        existing = _mkfile(dest / "Heat (1995)" / "Heat (1995) - complete movie.mkv")
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        rc = run_apply(MediaKind.MOVIE, tmp_path / "plan.kdl", ApplyOptions())
        assert rc == 2  # nothing new copied; the library already had it
        assert sorted(p.name for p in (dest / "Heat (1995)").iterdir()) == [
            existing.name
        ]
        assert json.loads(register.read_text(encoding="utf-8"))  # source recorded

    def test_different_size_not_matched(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        _mkfile(dest / "Heat (1995)" / "Heat (1995) - old dvd rip.mkv", size=32)
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].entries[0].status is EntryStatus.READY


class TestLibraryDisambiguation:
    def _ambiguous_providers(self):
        return movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TMDB, 666, "After Hours", 2004),
                SearchCandidate(MetadataProvider.TMDB, 777, "After Hours", 1985),
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=777, title="After Hours", year=1985
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )

    def test_existing_dir_resolves_ambiguity(self, tmp_path, register):
        src = tmp_path / "movies"
        _mkfile(src / "After Hours" / "After.Hours.1080p.mkv")
        dest = tmp_path / "dest"
        (dest / "After Hours (1985)").mkdir(parents=True)
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            self._ambiguous_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.tmdb_id == 777
        assert block.confidence is Confidence.HIGH
        assert block.note == "disambiguated by existing library directory"
        assert block.dest_dir == "After Hours (1985)"  # reused, not retagged
        assert block.candidates == []
        assert block.entries[0].status is EntryStatus.READY

    def test_no_matching_dir_stays_ambiguous(self, tmp_path, register):
        src = tmp_path / "movies"
        _mkfile(src / "After Hours" / "After.Hours.1080p.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            self._ambiguous_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.confidence is Confidence.AMBIGUOUS
        assert len(block.candidates) == 2
        assert block.entries[0].status is EntryStatus.NEEDS_ID

    def test_two_matching_dirs_stays_ambiguous(self, tmp_path, register):
        src = tmp_path / "movies"
        _mkfile(src / "After Hours" / "After.Hours.1080p.mkv")
        dest = tmp_path / "dest"
        (dest / "After Hours (1985)").mkdir(parents=True)
        (dest / "After Hours (2004)").mkdir(parents=True)
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            self._ambiguous_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].confidence is Confidence.AMBIGUOUS


class TestArrResolution:
    def test_radarr_resolves_without_search(self, movie_tree, tmp_path, register):
        from etp_lib.arr import ArrEntry

        src, dest = movie_tree
        config = movie_config(src, dest)
        config.radarr_url = "http://radarr:7878"
        providers = movie_providers(
            arr_key="rk",
            radarr_fetch=lambda url, key: {
                "heat [heat] (1995)": ArrEntry(
                    title="Heat", year=1995, folder="Heat [Heat] (1995)", tmdb_id=949
                )
            },
            tmdb_search_movie=lambda q, y, key, no_cache=False: pytest.fail(
                "search must not run when Radarr knows the ID"
            ),
        )
        rc = run_plan(MediaKind.MOVIE, config, plan_opts(tmp_path), providers)
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        assert block.tmdb_id == 949
        assert block.confidence is Confidence.EXACT
        assert block.note == "resolved via Radarr"

    def test_arr_failure_degrades_to_search(
        self, movie_tree, tmp_path, register, capsys
    ):
        src, dest = movie_tree
        config = movie_config(src, dest)
        config.radarr_url = "http://radarr:7878"

        def boom(url, key):
            raise OSError("connection refused")

        providers = movie_providers(arr_key="rk", radarr_fetch=boom)
        rc = run_plan(MediaKind.MOVIE, config, plan_opts(tmp_path), providers)
        assert rc == 0
        assert "Radarr query failed" in capsys.readouterr().out
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].tmdb_id == 949  # search fallback resolved it

    def test_no_url_no_fetch(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        providers = movie_providers(
            arr_key="rk",
            radarr_fetch=lambda url, key: pytest.fail(
                "must not query Radarr without a configured URL"
            ),
        )
        rc = run_plan(
            MediaKind.MOVIE, movie_config(src, dest), plan_opts(tmp_path), providers
        )
        assert rc == 0


class TestHardlinkTwins:
    def test_downloads_twin_of_managed_file_dropped(self, tmp_path, register):
        import os

        managed = tmp_path / "movies"
        downloads = tmp_path / "downloads"
        src_file = _mkfile(
            managed / "Heat (1995)" / "Heat (1995) - complete movie - [X,1080p].mkv"
        )
        # Radarr-style import: downloads copy is a hardlink of the same bytes.
        twin = downloads / "Heat.1995.1080p.REMUX.GARBLED-GRP.mkv"
        twin.parent.mkdir(parents=True)
        os.link(src_file, twin)

        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(
            downloads_dir=downloads,
            movies_source_dir=managed,
            movies_dest_dir=dest,
        )
        opts = plan_opts(tmp_path, managed=True, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, movie_providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        sources = [e.source for b in manifest.blocks for e in b.entries]
        assert len(sources) == 1
        assert str(src_file) in sources[0]  # the managed copy won

    def test_unrelated_downloads_file_kept(self, tmp_path, register):
        managed = tmp_path / "movies"
        downloads = tmp_path / "downloads"
        _mkfile(managed / "Heat (1995)" / "Heat (1995) - complete movie.mkv")
        _mkfile(downloads / "Heat.1995.Other.Encode-GRP.mkv", size=128)

        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(
            downloads_dir=downloads,
            movies_source_dir=managed,
            movies_dest_dir=dest,
        )
        opts = plan_opts(tmp_path, managed=True, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, movie_providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        sources = [e.source for b in manifest.blocks for e in b.entries]
        assert len(sources) == 2  # distinct files both planned


class TestLibraryWideSameSize:
    def test_copy_elsewhere_in_library_skips(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        # The same encode already lives in a box-set directory the title
        # doesn't resolve to (e.g. a trilogy dir).
        _mkfile(dest / "Crime Collection (1995)" / "Heat 1080p.mkv")
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entry = manifest.blocks[0].entries[0]
        assert entry.status is EntryStatus.SKIP
        assert "Crime Collection (1995)" in entry.note

    def test_same_dir_match_takes_precedence(self, movie_tree, tmp_path, register):
        src, dest = movie_tree
        _mkfile(dest / "Crime Collection (1995)" / "Heat 1080p.mkv")
        existing = _mkfile(dest / "Heat (1995)" / "Heat (1995) - old name.mkv")
        rc = run_plan(
            MediaKind.MOVIE,
            movie_config(src, dest),
            plan_opts(tmp_path),
            movie_providers(),
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entry = manifest.blocks[0].entries[0]
        # In-directory copy wins: conflict/keep pointing at it.
        assert entry.status is EntryStatus.CONFLICT
        assert entry.dest == existing.name


class TestMovieExtras:
    """Featurettes packed beside a main film in its torrent directory."""

    def _grym_tree(self, tmp_path):
        dl = tmp_path / "downloads"
        torrent = dl / "Anomalisa.2015.1080p.BluRay.x264-Grym"
        _mkfile(torrent / "Anomalisa.2015.1080p.BluRay.x264-Grym.mkv", size=10_000)
        _mkfile(torrent / "Crafting.Anomalisa-Grym.mkv", size=900)
        _mkfile(torrent / "Theatrical.Trailer-Grym.mkv", size=100)
        dest = tmp_path / "dest"
        dest.mkdir()
        return dl, dest

    def _anomalisa_providers(self):
        return movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TMDB, 291270, "Anomalisa", 2015)
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=291270, title="Anomalisa", year=2015
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )

    def test_scan_attaches_extras_to_main(self, tmp_path):
        dl, _ = self._grym_tree(tmp_path)
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 1  # no junk groups for the extras
        t = titles[0]
        assert t.title == "Anomalisa"
        categories = sorted(f.extra_category for f in t.files)
        assert categories == ["", "Featurettes", "Trailers"]

    def test_plan_places_extras_in_subdirs(self, tmp_path, register):
        dl, dest = self._grym_tree(tmp_path)
        config = MediaIngestConfig(downloads_dir=dl, movies_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._anomalisa_providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        dests = sorted(e.dest for e in block.entries)
        assert dests[1] == "Featurettes/Crafting Anomalisa.mkv"
        assert dests[2] == "Trailers/Theatrical Trailer.mkv"
        assert all(e.status is EntryStatus.READY for e in block.entries)
        # The main film keeps its quality block; extras stay clean.
        assert "[" in dests[0] and "Anomalisa (2015)" in dests[0]

    def test_multipart_not_demoted_to_extras(self, tmp_path):
        dl = tmp_path / "downloads"
        torrent = dl / "Goemon.2009.DVDRip-GRP"
        _mkfile(torrent / "Goemon.2009.cd1-GRP.mkv", size=5000)
        _mkfile(torrent / "Goemon.2009.cd2-GRP.mkv", size=4500)
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert all(not f.extra_category for t in titles for f in t.files)

    def test_nested_extras_subdir(self, tmp_path):
        dl = tmp_path / "downloads"
        torrent = dl / "Anomalisa.2015.1080p.BluRay.x264-Grym"
        _mkfile(torrent / "Anomalisa.2015.1080p.BluRay.x264-Grym.mkv", size=10_000)
        _mkfile(torrent / "Extras" / "Crafting.Anomalisa-Grym.mkv", size=900)
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 1
        extras = [f for f in titles[0].files if f.extra_category]
        assert len(extras) == 1
        assert extras[0].extra_category == "Featurettes"


class TestTvExtrasDirs:
    """Featurettes/Extras directories inside TV batch torrents attach to
    the show named by the directory above them (the Expanse layout)."""

    def _expanse_tree(self, tmp_path):
        dl = tmp_path / "downloads"
        batch = (
            dl / "The Expanse (2015) S01-S06 (1080p BluRay x265 10bit EAC3 5.1 Ghost)"
        )
        s1 = batch / "The Expanse (2015) S01"
        s2 = batch / "The Expanse (2015) S02"
        _mkfile(
            s1 / "The Expanse (2015) S01E01 Dulcinea (1080p BluRay x265 Ghost).mkv",
            size=10_000,
        )
        _mkfile(
            s1
            / "The Expanse (2015) S01E02 The Big Empty (1080p BluRay x265 Ghost).mkv",
            size=10_000,
        )
        _mkfile(
            s2 / "The Expanse (2015) S02E01 Safe (1080p BluRay x265 Ghost).mkv",
            size=10_000,
        )
        _mkfile(
            s1 / "Featurettes" / "Season 1 - 2015 New York Comic Con Panel.mkv",
            size=900,
        )
        _mkfile(s2 / "Featurettes" / "Season 2 - Blooper Reel.mkv", size=800)
        return dl

    def test_scan_attaches_featurettes_to_show(self, tmp_path):
        dl = self._expanse_tree(tmp_path)
        titles = scan_downloads([dl], MediaKind.TV)
        assert len(titles) == 1  # no junk groups for the featurettes
        t = titles[0]
        assert t.title == "The Expanse"
        assert t.year == 2015
        extras = sorted(
            f.source.path.name for f in t.files if f.extra_category == "Featurettes"
        )
        assert extras == [
            "Season 1 - 2015 New York Comic Con Panel.mkv",
            "Season 2 - Blooper Reel.mkv",
        ]
        assert sum(1 for f in t.files if not f.extra_category) == 3

    def test_show_featurettes_stay_out_of_movie_plans(self, tmp_path):
        dl = self._expanse_tree(tmp_path)
        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert titles == []  # season pack: nothing here is movie material

    def test_extras_dir_directly_in_root_ignored(self, tmp_path):
        dl = tmp_path / "downloads"
        _mkfile(dl / "Featurettes" / "Some Orphan Clip.mkv")
        assert scan_downloads([dl], MediaKind.TV) == []

    def test_sample_in_extras_dir_dropped(self, tmp_path):
        dl = self._expanse_tree(tmp_path)
        batch = next(dl.iterdir())
        _mkfile(
            batch / "The Expanse (2015) S01" / "Featurettes" / "Sample.mkv", size=10
        )
        (titles,) = scan_downloads([dl], MediaKind.TV)
        assert all(f.source.path.name != "Sample.mkv" for f in titles.files)

    def test_featurette_matching_tvdb_special_becomes_special(self, tmp_path, register):
        """A featurette TheTVDB tracks as a season-0 special is planned
        into Specials/ under its special number (the Expanse aftershow
        pattern), not Featurettes/."""
        dl = tmp_path / "downloads"
        torrent = dl / "Severance (2022) S01 (1080p WEB-DL)"
        _mkfile(
            torrent / "Severance (2022) S01E01 Good News About Hell (1080p).mkv",
            size=10_000,
        )
        _mkfile(torrent / "Featurettes" / "Season 1 - Inside Severance.mkv", size=500)
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(downloads_dir=dl, television_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        assert run_plan(MediaKind.TV, config, opts, tv_providers()) == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        (block,) = manifest.blocks
        special = next(e for e in block.entries if "Inside Severance" in e.source)
        assert special.season == 0
        assert special.number == 5
        assert special.title == "Inside Severance"
        assert special.dest.startswith("Specials/")
        assert "s00e05" in special.dest
        assert special.status is EntryStatus.READY

    def test_plan_places_show_extras_at_show_level(self, tmp_path, register):
        dl = tmp_path / "downloads"
        torrent = dl / "Severance (2022) S01 (1080p WEB-DL)"
        _mkfile(
            torrent / "Severance (2022) S01E01 Good News About Hell (1080p).mkv",
            size=10_000,
        )
        _mkfile(torrent / "Featurettes" / "Season 1 - Making Severance.mkv", size=500)
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(downloads_dir=dl, television_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.TV, config, opts, tv_providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert len(manifest.blocks) == 1
        block = manifest.blocks[0]
        dests = sorted(e.dest for e in block.entries)
        # The extra lands in a show-level Featurettes/ dir with a clean
        # name; the episode keeps its Season NN placement.
        assert dests[0] == "Featurettes/Season 1 - Making Severance.mkv"
        assert dests[1].startswith("Season 01/")
        assert all(e.status is EntryStatus.READY for e in block.entries)


class TestAnalyzeRobustness:
    """One unreadable file must degrade to a missing quality block, not
    abort the whole plan."""

    def test_mediainfo_subprocess_failure_degrades(self):
        import subprocess

        from etp_lib.media_scanner import parse_source_filename
        from etp_lib.video_ingest import _analyze

        def boom(path):
            raise subprocess.CalledProcessError(1, ["mediainfo"])

        sf = parse_source_filename("Show - S01E01.mkv")
        sf.path = Path("/nope/Show - S01E01.mkv")
        _analyze(sf, Providers(analyze=boom))
        assert sf.media is None


class TestRemakeGrouping:
    """Same-titled films of different years are different films."""

    def test_remake_splits_from_original(self, tmp_path):
        dl = tmp_path / "downloads"
        _mkfile(dl / "Suspiria.1977.1080p.BluRay.x264-GRP.mkv", size=5000)
        _mkfile(dl / "Suspiria.2018.1080p.WEB.H264-GRP.mkv", size=6000)
        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert sorted((t.title, t.year) for t in titles) == [
            ("Suspiria", 1977),
            ("Suspiria", 2018),
        ]

    def test_yearless_copy_still_groups_alone(self, tmp_path):
        """Provider-ID merging reunites same-film blocks after
        resolution; grouping itself must never guess a year."""
        dl = tmp_path / "downloads"
        _mkfile(dl / "Heat.1995.1080p.BluRay.x264-GRP.mkv", size=5000)
        _mkfile(dl / "Heat.720p.BluRay.x264-OLD.mkv", size=3000)
        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 2


class TestTvdbCacheSeparation:
    """General-television TheTVDB records must not land in the anime
    pipeline's tvdb cache (build_title_index slurps it wholesale, so a
    live-action Death Note would merge into the anime's alias group)."""

    def test_default_fetcher_uses_tv_cache(self, monkeypatch):
        from etp_lib import tvdb as tvdb_mod

        seen = {}

        def capture(series_id, api_key, no_cache=False, *, cache_name="tvdb"):
            seen["cache_name"] = cache_name
            return SEVERANCE

        monkeypatch.setattr(tvdb_mod, "fetch_tvdb_series", capture)
        assert Providers().tvdb_fetch_series(371980, "key") is SEVERANCE
        assert seen["cache_name"] == "tvdb-tv"

    def test_fetch_tvdb_series_honors_cache_name(self, tmp_path, monkeypatch):
        from etp_lib import tvdb as tvdb_mod

        def fake_cache_dir(provider: str):
            d = tmp_path / provider
            d.mkdir(parents=True, exist_ok=True)
            return d

        monkeypatch.setattr(tvdb_mod, "cache_dir", fake_cache_dir)
        record = {
            "series": {"name": "Murderbot", "year": "2025", "aliases": []},
            "episodes": [{"seasonNumber": 1, "number": 1, "name": "FreeCommerce"}],
            "translations": {"eng": "Murderbot"},
        }
        (tmp_path / "tvdb-tv").mkdir()
        (tmp_path / "tvdb-tv" / "443396.json").write_text(
            json.dumps(record), encoding="utf-8"
        )
        # Must read the tvdb-tv copy, never touching the anime tvdb dir.
        info = tvdb_mod.fetch_tvdb_series(443396, "key", cache_name="tvdb-tv")
        assert info.title_en == "Murderbot"
        assert not (tmp_path / "tvdb" / "443396.json").exists()


class TestMatchExtraToSpecial:
    """Title matching between extras files and TheTVDB season-0 specials."""

    def _info(self, *titles):
        return AnimeInfo(
            anidb_id=None,
            tvdb_id=1,
            title_ja="Show",
            title_en="Show",
            year=2020,
            episodes=[
                Episode(i + 1, EpisodeType.SPECIAL, t, "", f"s0e{i + 1:02d}", season=0)
                for i, t in enumerate(titles)
            ],
        )

    def _match(self, info, stem):
        from etp_lib.video_ingest import _match_extra_to_special

        return _match_extra_to_special(info, stem)

    def test_exact_title_with_season_prefix(self):
        info = self._info("Inside Severance")
        ep = self._match(info, "Season 1 - Inside Severance")
        assert ep is not None and ep.number == 1

    def test_file_name_opens_tvdb_title(self):
        """TheTVDB appends guest lists; the file's name is the opening."""
        info = self._info(
            "The Expanse Aftershow - Season 5, Episode 1:"
            " Wes Chatham, Ty Franck, & Naren Shankar"
        )
        ep = self._match(
            info, "Season 5 - “The Expanse” Aftershow - Season 5, Episode 1"
        )
        assert ep is not None and ep.number == 1

    def test_ambiguous_title_stays_extra(self):
        info = self._info("Blooper Reel Extended", "Blooper Reel Extended")
        assert self._match(info, "Blooper Reel Extended") is None

    def test_short_generic_name_stays_extra(self):
        info = self._info("Trailer")
        assert self._match(info, "Trailer") is None

    def test_unmatched_stays_extra(self):
        info = self._info("Inside Severance")
        assert self._match(info, "Making the Music") is None


class TestSameTitleMerging:
    """One resolved title -> one block, even across source modes."""

    _ENZ = MovieInfo(tmdb_id=1156125, title="Evil Does Not Exist", year=2023)

    def _providers(self):
        return movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(
                    MetadataProvider.TMDB, 1156125, "Evil Does Not Exist", 2023
                )
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: self._ENZ,
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )

    def test_film_and_featurettes_share_one_block(self, tmp_path, register):
        import os

        managed = tmp_path / "movies"
        downloads = tmp_path / "downloads"
        radarr_copy = _mkfile(
            managed
            / "Evil Does Not Exist (2023)"
            / "Evil Does Not Exist (2023) - complete movie - [GRP Bluray-1080p].mkv",
            size=9000,
        )
        torrent = downloads / "Evil.Does.Not.Exist.2023.Criterion.1080p.BluRay-GRP"
        main = torrent / "Evil.Does.Not.Exist.2023.Criterion.1080p.BluRay-GRP.mkv"
        main.parent.mkdir(parents=True)
        os.link(radarr_copy, main)  # Radarr imported by hardlinking
        _mkfile(torrent / "On.the.Set.Interview-GRP.mkv", size=500)

        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(
            downloads_dir=downloads, movies_source_dir=managed, movies_dest_dir=dest
        )
        opts = plan_opts(tmp_path, managed=True, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert len(manifest.blocks) == 1  # film + extras, one block
        block = manifest.blocks[0]
        # Edition adopted from the torrent's Criterion marker.
        assert block.edition == "Criterion Collection"
        assert "{edition-Criterion Collection}" in block.dest_dir
        dests = sorted(e.dest for e in block.entries)
        assert len(dests) == 2  # hardlinked main deduped; film + interview
        assert dests[0].startswith("Evil Does Not Exist (2023)")
        assert dests[1] == "Interviews/On the Set Interview.mkv"

    def test_distinct_encodes_merge_into_one_block(self, tmp_path, register):
        managed = tmp_path / "movies"
        downloads = tmp_path / "downloads"
        _mkfile(
            managed
            / "Evil Does Not Exist (2023)"
            / "Evil Does Not Exist (2023) - complete movie - [GRP Bluray-1080p].mkv",
            size=9000,
        )
        _mkfile(
            downloads / "Evil.Does.Not.Exist.2023.2160p.WEB-DL-OTHER.mkv", size=20000
        )
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(
            downloads_dir=downloads, movies_source_dir=managed, movies_dest_dir=dest
        )
        opts = plan_opts(tmp_path, managed=True, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert len(manifest.blocks) == 1
        assert len(manifest.blocks[0].entries) == 2  # both versions, one dir

    def test_conflicting_editions_stay_separate(self, tmp_path, register):
        managed = tmp_path / "movies"
        downloads = tmp_path / "downloads"
        _mkfile(
            managed
            / "Evil Does Not Exist (2023)"
            / "Evil Does Not Exist (2023) - complete movie - Theatrical"
            " [GRP Bluray-1080p].mkv",
            size=9000,
        )
        _mkfile(
            downloads / "Evil.Does.Not.Exist.2023.Criterion.1080p.BluRay-GRP.mkv",
            size=20000,
        )
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(
            downloads_dir=downloads, movies_source_dir=managed, movies_dest_dir=dest
        )
        opts = plan_opts(tmp_path, managed=True, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        editions = sorted(b.edition for b in manifest.blocks)
        assert editions == ["Criterion Collection", "Theatrical"]


class TestTvDuplicateDests:
    def test_same_episode_twice_marks_second_skip(self, tmp_path, register):
        src = tmp_path / "television"
        show = src / "Severance (2022)"
        _mkfile(show / "Season 01" / "Severance - S01E01 - A WEBDL-1080p.mkv")
        _mkfile(show / "Season 01" / "Severance - S01E01 - B WEBDL-1080p.mkv", size=128)
        dest = tmp_path / "dest"
        dest.mkdir()
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), tv_providers()
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        entries = manifest.blocks[0].entries
        statuses = sorted(str(e.status) for e in entries)
        assert statuses == ["ready", "skip"]
        skipped = next(e for e in entries if e.status is EntryStatus.SKIP)
        assert "additional version" in skipped.note


class TestSamplesAndVersions:
    def _providers(self):
        return movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(
                    MetadataProvider.TMDB, 152584, "The Last of the Unjust", 2013
                )
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=152584, title="The Last of the Unjust", year=2013
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )

    def test_sample_clip_dropped_entirely(self, tmp_path, register):
        dl = tmp_path / "downloads"
        torrent = dl / "The.Last.of.the.Unjust.2013.720p.BluRay.x265-SARTRE"
        _mkfile(torrent / "The.Last.of.the.Unjust.2013.720p.x265-SARTRE.mkv", 10_000)
        _mkfile(torrent / "sample.mkv", size=75)
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(downloads_dir=dl, movies_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        sources = [e.source for b in manifest.blocks for e in b.entries]
        assert len(sources) == 1
        assert Path(sources[0]).name != "sample.mkv"

    def test_extra_encode_skipped_when_library_has_film(self, tmp_path, register):
        managed = tmp_path / "movies"
        downloads = tmp_path / "downloads"
        _mkfile(
            managed
            / "The Last of the Unjust (2013)"
            / "The Last of the Unjust (2013) - complete movie - [TRiPS,720p].mkv",
            size=9000,
        )
        # The library already holds the managed encode under older naming.
        dest = tmp_path / "dest"
        _mkfile(
            dest / "The Last of the Unjust (2013)" / "old library name.mkv", size=9000
        )
        # A different re-encode is still sitting in downloads.
        _mkfile(
            downloads / "The.Last.of.the.Unjust.2013.720p.x265-SARTRE.mkv", size=3600
        )
        config = MediaIngestConfig(
            downloads_dir=downloads, movies_source_dir=managed, movies_dest_dir=dest
        )
        opts = plan_opts(tmp_path, managed=True, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert len(manifest.blocks) == 1
        by_status = {str(e.status): e for e in manifest.blocks[0].entries}
        assert by_status["conflict"].conflict is ConflictAction.KEEP  # managed copy
        skipped = by_status["skip"]
        assert "x265-SARTRE" in skipped.source
        assert skipped.note == (
            "additional version; an existing copy is already in the library"
        )

    def test_first_new_encode_stays_ready(self, tmp_path, register):
        downloads = tmp_path / "downloads"
        _mkfile(downloads / "The.Last.of.the.Unjust.2013.720p-TRiPS.mkv", size=9000)
        _mkfile(downloads / "The.Last.of.the.Unjust.2013.720p.x265-SARTRE.mkv", 3600)
        dest = tmp_path / "dest"
        dest.mkdir()
        config = MediaIngestConfig(downloads_dir=downloads, movies_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, self._providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        statuses = sorted(str(e.status) for e in manifest.blocks[0].entries)
        assert statuses == ["ready", "skip"]
        skipped = next(
            e for e in manifest.blocks[0].entries if e.status is EntryStatus.SKIP
        )
        assert skipped.note.startswith("additional version of ")

    def test_multipart_versions_not_skipped(self, tmp_path, register):
        downloads = tmp_path / "downloads"
        torrent = downloads / "Goemon.2009.DVDRip"
        _mkfile(torrent / "Goemon.2009.cd1.mkv", size=5000)
        _mkfile(torrent / "Goemon.2009.cd2.mkv", size=4800)
        dest = tmp_path / "dest"
        dest.mkdir()
        providers = movie_providers(
            tmdb_search_movie=lambda q, y, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TMDB, 25050, "Goemon", 2009)
            ],
            tmdb_fetch_movie=lambda i, key, no_cache=False: MovieInfo(
                tmdb_id=25050, title="Goemon", year=2009
            ),
            tvdb_search_movies=lambda q, key, no_cache=False: [],
        )
        config = MediaIngestConfig(downloads_dir=downloads, movies_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.MOVIE, config, opts, providers)
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert all(e.status is EntryStatus.READY for e in manifest.blocks[0].entries)


class TestSeasonPackGuard:
    def test_episode_batch_excluded_from_movie_plan(self, tmp_path):
        dl = tmp_path / "downloads"
        pack = dl / "[Pod] Police in a Pod - (BD 1080p AVC FLAC)"
        for n in range(1, 14):
            # Unparseable-as-episode or not, similar-size batches are TV.
            _mkfile(pack / f"[Pod] Police in a Pod - {n:02d} - (BD 1080p).mkv", 5000)
        from etp_lib.video_ingest import scan_downloads

        assert scan_downloads([dl], MediaKind.MOVIE) == []

    def test_movie_with_extras_unaffected(self, tmp_path):
        dl = tmp_path / "downloads"
        torrent = dl / "Anomalisa.2015.1080p.BluRay.x264-Grym"
        _mkfile(torrent / "Anomalisa.2015.1080p.BluRay.x264-Grym.mkv", size=10_000)
        _mkfile(torrent / "Crafting.Anomalisa-Grym.mkv", size=900)
        _mkfile(torrent / "Intimacy.in.Miniature-Grym.mkv", size=800)
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 1
        assert sum(1 for f in titles[0].files if f.extra_category) == 2

    def test_three_part_movie_not_a_pack(self, tmp_path):
        dl = tmp_path / "downloads"
        torrent = dl / "Shoah.1985.DVDRip"
        for n in (1, 2, 3):
            _mkfile(torrent / f"Shoah.1985.pt{n}.mkv", size=5000)
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([dl], MediaKind.MOVIE)
        assert len(titles) == 1
        assert len(titles[0].files) == 3


class TestSeasonDirLayout:
    """iTunes-style Show/Season N/NN Title.m4v trees inside downloads."""

    def _columbo_tree(self, tmp_path):
        dl = tmp_path / "downloads"
        show = dl / "Columbo"
        _mkfile(show / "Season 1" / "01 Prescription_ Murder (HD).m4v", size=5000)
        _mkfile(show / "Season 1" / "01 Prescription_ Murder.m4v", size=2000)
        _mkfile(show / "Season 1" / "02 Ransom for a Dead Man (HD).m4v", size=5100)
        _mkfile(show / "Season 2" / "01 Etude in Black (HD).m4v", size=5200)
        return dl

    def test_scan_groups_by_show_and_season(self, tmp_path):
        from etp_lib.video_ingest import scan_downloads

        titles = scan_downloads([self._columbo_tree(tmp_path)], MediaKind.TV)
        assert len(titles) == 1
        t = titles[0]
        assert t.title == "Columbo"
        episodes = sorted((f.season, f.episode) for f in t.files)
        assert episodes == [(1, 1), (1, 1), (1, 2), (2, 1)]

    def test_excluded_from_movie_mode(self, tmp_path):
        from etp_lib.video_ingest import scan_downloads

        assert scan_downloads([self._columbo_tree(tmp_path)], MediaKind.MOVIE) == []

    def test_plan_one_copy_per_episode(self, tmp_path, register):
        dl = self._columbo_tree(tmp_path)
        dest = tmp_path / "dest"
        dest.mkdir()
        info = AnimeInfo(
            anidb_id=None,
            tvdb_id=71316,
            title_ja="Columbo",
            title_en="Columbo",
            year=1971,
            episodes=[
                Episode(
                    1, EpisodeType.REGULAR, "Prescription: Murder", "", "", season=1
                ),
                Episode(
                    2, EpisodeType.REGULAR, "Ransom for a Dead Man", "", "", season=1
                ),
                Episode(1, EpisodeType.REGULAR, "Étude in Black", "", "", season=2),
            ],
        )
        providers = tv_providers(
            tvdb_search_series=lambda q, key, no_cache=False: [
                SearchCandidate(MetadataProvider.TVDB, 71316, "Columbo", 1971)
            ],
            tvdb_fetch_series=lambda i, key, no_cache=False: info,
            tmdb_search_tv=lambda q, y, key, no_cache=False: [],
        )
        config = MediaIngestConfig(downloads_dir=dl, television_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.TV, config, opts, providers)
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        block = manifest.blocks[0]
        ready = [e for e in block.entries if e.status is EntryStatus.READY]
        skipped = [e for e in block.entries if e.status is EntryStatus.SKIP]
        assert len(ready) == 3  # one per episode
        assert len(skipped) == 1  # the SD copy of s01e01
        assert "additional version" in skipped[0].note
        # Provider episode titles flow into destinations, colon convention.
        s1e1 = next(e for e in ready if e.season == 1 and e.number == 1)
        assert s1e1.dest.startswith(
            "Season 01/Columbo (1971) - s01e01 - Prescription - Murder ["
        )


class TestRefineScope:
    """--refine narrows the plan to the manifest's contents."""

    def test_deleted_block_stays_deleted(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        # Add a second show so the manifest has two blocks.
        _mkfile(
            src
            / "Other Show (2020)"
            / "Season 01"
            / "Other Show - S01E01 - X WEBDL-1080p.mkv"
        )
        providers = tv_providers(
            tvdb_search_series=lambda q, key, no_cache=False: (
                [SearchCandidate(MetadataProvider.TVDB, 371980, "Severance", 2022)]
                if "severance" in q.lower()
                else []
            ),
        )
        first = plan_opts(tmp_path, output=tmp_path / "first.kdl")
        assert run_plan(MediaKind.TV, tv_config(src, dest), first, providers) == 0
        text = (tmp_path / "first.kdl").read_text(encoding="utf-8")
        assert "Other Show" in text
        # The curator deletes the unwanted block wholesale.
        kept_lines = []
        skipping = False
        for line in text.splitlines():
            if line.startswith('series "Other Show'):
                skipping = True
            if not skipping:
                kept_lines.append(line)
            if skipping and line == "}":
                skipping = False
        (tmp_path / "first.kdl").write_text("\n".join(kept_lines), encoding="utf-8")

        second = plan_opts(
            tmp_path, output=tmp_path / "second.kdl", refine=tmp_path / "first.kdl"
        )
        assert run_plan(MediaKind.TV, tv_config(src, dest), second, providers) == 0
        refined = (tmp_path / "second.kdl").read_text(encoding="utf-8")
        assert "Other Show" not in refined
        assert "Severance" in refined

    def test_deleted_entry_stays_deleted(self, tv_tree, tmp_path, register):
        src, dest = tv_tree
        first = plan_opts(tmp_path, output=tmp_path / "first.kdl")
        assert run_plan(MediaKind.TV, tv_config(src, dest), first, tv_providers()) == 0
        manifest = parse_plan_manifest(tmp_path / "first.kdl")
        victim = manifest.blocks[0].entries[0].source
        # Remove just that episode node from the manifest.
        manifest.blocks[0].entries = [
            e for e in manifest.blocks[0].entries if e.source != victim
        ]
        write_plan_manifest(manifest, tmp_path / "first.kdl")

        second = plan_opts(
            tmp_path, output=tmp_path / "second.kdl", refine=tmp_path / "first.kdl"
        )
        assert run_plan(MediaKind.TV, tv_config(src, dest), second, tv_providers()) == 0
        refined = parse_plan_manifest(tmp_path / "second.kdl")
        sources = [e.source for b in refined.blocks for e in b.entries]
        assert victim not in sources
        assert len(sources) == 2  # the other two episodes survive


class TestDomainPartitioning:
    """Foreign-domain titles in downloads stay out of TV/movie plans."""

    def _downloads_with_anime(self, tmp_path):
        dl = tmp_path / "downloads"
        _mkfile(dl / "Frieren - S01E01 - (BD 1080p).mkv")
        _mkfile(dl / "Severance - S01E01 - Good News WEBDL-1080p.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        return dl, dest

    def test_sonarr_root_excludes_anime(self, tmp_path, register, monkeypatch):
        from etp_lib.arr import ArrEntry

        dl, dest = self._downloads_with_anime(tmp_path)
        monkeypatch.setattr(video_ingest, "_anime_tree_names", lambda: set())
        index = {
            "~frieren": ArrEntry(
                title="Frieren: Beyond Journey's End",
                year=2023,
                folder="Frieren (2023)",
                tvdb_id=424536,
                root="anime",
            ),
        }
        config = MediaIngestConfig(
            downloads_dir=dl, television_dest_dir=dest, sonarr_url="http://s:8989"
        )
        providers = tv_providers(arr_key="sk", sonarr_fetch=lambda url, key: index)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.TV, config, opts, providers)
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        titles = [b.raw_title for b in manifest.blocks]
        assert titles == ["Severance"]

    def test_anime_tree_fallback_excludes(self, tmp_path, register, monkeypatch):
        dl, dest = self._downloads_with_anime(tmp_path)
        anime_tree = tmp_path / "anime"
        (anime_tree / "Frieren (2023)").mkdir(parents=True)
        monkeypatch.setattr(video_ingest, "anime_source_dir", lambda: anime_tree)
        config = MediaIngestConfig(downloads_dir=dl, television_dest_dir=dest)
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        rc = run_plan(MediaKind.TV, config, opts, tv_providers())
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        titles = [b.raw_title for b in manifest.blocks]
        assert titles == ["Severance"]

    def test_managed_titles_never_filtered(
        self, tv_tree, tmp_path, register, monkeypatch
    ):
        src, dest = tv_tree
        # Even if the anime tree claims the same name, the managed
        # television tree is in-domain by definition.
        monkeypatch.setattr(
            video_ingest,
            "_anime_tree_names",
            lambda: {video_ingest._normalize_dirname("Severance (2022)")},
        )
        rc = run_plan(
            MediaKind.TV, tv_config(src, dest), plan_opts(tmp_path), tv_providers()
        )
        assert rc == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert manifest.blocks[0].raw_title == "Severance (2022)"

    def test_cross_arr_index_excludes_other_kinds_titles(
        self, tmp_path, register, monkeypatch
    ):
        """A Radarr-managed anime movie never shows up in a TV plan."""
        from etp_lib.arr import ArrEntry

        dl = tmp_path / "downloads"
        _mkfile(dl / "Shisha no Teikoku - 01 (BD 1080p).mkv")
        _mkfile(dl / "Severance - S01E01 - Good News WEBDL-1080p.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        monkeypatch.setattr(video_ingest, "_anime_tree_names", lambda: set())
        radarr = {}
        from etp_lib import arr as arr_mod

        arr_mod._index(
            radarr,
            ArrEntry(
                title="The Empire of Corpses",
                year=2015,
                folder="The Empire of Corpses (2015)",
                root="anime",
            ),
            ["Shisha no Teikoku"],
        )
        config = MediaIngestConfig(
            downloads_dir=dl,
            television_dest_dir=dest,
            sonarr_url="http://s:8989",
            radarr_url="http://r:7878",
        )
        providers = tv_providers(
            arr_key="sk",
            cross_arr_key="rk",
            sonarr_fetch=lambda url, key: {},
            radarr_fetch=lambda url, key: radarr,
        )
        opts = plan_opts(tmp_path, managed=False, downloads=True)
        assert run_plan(MediaKind.TV, config, opts, providers) == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert [b.raw_title for b in manifest.blocks] == ["Severance"]


class TestDomainMatching:
    """arr.domain_of folds punctuation/spacing and uses phrase containment."""

    def _index(self, title, year=2000, root="anime", alts=()):
        from etp_lib import arr as arr_mod
        from etp_lib.arr import ArrEntry

        index = {}
        arr_mod._index(
            index,
            ArrEntry(title=title, year=year, folder=f"{title} ({year})", root=root),
            list(alts),
        )
        return index

    def _domain(self, index, scanned, alt="", year=0):
        from etp_lib import arr as arr_mod

        return arr_mod.domain_of(index, scanned, scanned, alt, year)

    def test_apostrophe_folded(self):
        index = self._index("Wolf's Rain", 2003)
        assert self._domain(index, "Wolfs Rain") == "anime"

    def test_accent_folded(self):
        index = self._index(
            "An Observation Log of My Fiancée Who Calls Herself a Villainess", 2026
        )
        scanned = "An Observation Log of My Fiancee Who Calls Herself a Villainess"
        assert self._domain(index, scanned) == "anime"

    def test_spacing_folded(self):
        index = self._index("Re: ZERO, Starting Life in Another World", 2016)
        assert self._domain(index, "ReZERO -Starting Life in Another World") == "anime"

    def test_scanned_title_contained_in_managed(self):
        index = self._index(
            "WataMote: No Matter How I Look at It,"
            " It's You Guys' Fault I'm Not Popular!",
            2013,
        )
        assert self._domain(index, "WataMote") == "anime"

    def test_managed_title_contained_in_scanned(self):
        index = self._index("Outlaw Star", 1998)
        assert self._domain(index, "Outlaw Star Art Gallery") == "anime"
        index = self._index("Great Teacher Onizuka", 1999)
        assert self._domain(index, "GTO Great Teacher Onizuka") == "anime"

    def test_word_prefix_matches_with_lower_guard(self):
        index = self._index("Gintama", 2006)
        # An anchored word-aligned prefix counts from 6 alphanumerics...
        assert self._domain(index, "Gintama The Semi-Final") == "anime"
        index = self._index("Re: ZERO, Starting Life in Another World", 2016)
        assert self._domain(index, "Re.Zero") == "anime"

    def test_short_phrases_do_not_count(self):
        # ...but mid-phrase containment needs 8, and very short titles
        # never match anything.
        index = self._index("Gintama", 2006)
        assert self._domain(index, "The Gintama Special Collection") is None
        index = self._index("Dark", 2017)  # 4 alphanumerics
        assert self._domain(index, "Dark Matter") is None

    def test_unrelated_title_no_match(self):
        index = self._index("Frieren: Beyond Journey's End", 2023)
        assert self._domain(index, "Severance", year=2022) is None


class TestForeignDomainDrop:
    """_drop_foreign_domain: torrent inheritance and config domain overrides."""

    def _group(self, title, *paths):
        from etp_lib.video_ingest import ScannedFile, ScannedTitle

        t = ScannedTitle(raw_title=title, title=title, year=0)
        for p in paths:
            sf = parse_source_filename(p.name)
            sf.path = p
            t.files.append(ScannedFile(source=sf))
        return t

    def test_torrent_inheritance_drops_junk_siblings(self, tmp_path, monkeypatch):
        from etp_lib.video_ingest import _drop_foreign_domain

        monkeypatch.setattr(video_ingest, "_anime_tree_names", lambda: set())
        dl = tmp_path / "downloads"
        torrent = dl / "[Joliver] Aura Battler Dunbine [BD]"
        ep = _mkfile(torrent / "[Joliver] Aura Battler Dunbine - 01 [BD].mkv")
        ncop = _mkfile(torrent / "[Joliver] Aura Battler Dunbine - B1 - NCOP1 [BD].mkv")
        other = _mkfile(dl / "Columbo" / "01 Murder by the Book.mkv")
        from etp_lib import arr as arr_mod
        from etp_lib.arr import ArrEntry

        index = {}
        arr_mod._index(
            index,
            ArrEntry(
                title="Aura Battler Dunbine",
                year=1983,
                folder="Aura Battler Dunbine (1983)",
                root="anime",
            ),
            [],
        )
        titles = [
            self._group("Aura Battler Dunbine", ep),
            self._group("Aura Battler Dunbine B1", ncop),
            self._group("Columbo", other),
        ]
        kept, dropped = _drop_foreign_domain(titles, index, "television", [dl])
        assert dropped == 2
        assert [t.title for t in kept] == ["Columbo"]

    def test_mapping_domain_is_authoritative(self, tmp_path, monkeypatch):
        from etp_lib.types import TitleMapping
        from etp_lib.video_ingest import _drop_foreign_domain

        monkeypatch.setattr(video_ingest, "_anime_tree_names", lambda: set())
        dl = tmp_path / "downloads"
        hana = _mkfile(dl / "hyd" / "Hana yori Dango - 01.mkv")
        titles = [self._group("Hana yori Dango", hana)]
        mappings = {"hana yori dango": TitleMapping(domain="anime")}
        kept, dropped = _drop_foreign_domain(titles, {}, "television", [dl], mappings)
        assert dropped == 1 and kept == []

    def test_source_override_keeps_own_domain_titles(
        self, tmp_path, register, monkeypatch
    ):
        """--source must not change what counts as our own domain: a
        Sonarr-managed television title stays in the plan even when the
        downloads dir basename isn't 'television'."""
        from etp_lib import arr as arr_mod
        from etp_lib.arr import ArrEntry

        dl = tmp_path / "dl"  # basename deliberately not 'television'
        _mkfile(dl / "Severance - S01E01 - Good News WEBDL-1080p.mkv")
        dest = tmp_path / "dest"
        dest.mkdir()
        monkeypatch.setattr(video_ingest, "_anime_tree_names", lambda: set())
        index = {}
        arr_mod._index(
            index,
            ArrEntry(
                title="Severance",
                year=2022,
                folder="Severance (2022)",
                root="television",
            ),
            [],
        )
        config = MediaIngestConfig(television_dest_dir=dest, sonarr_url="http://s")
        providers = tv_providers(arr_key="sk", sonarr_fetch=lambda url, key: index)
        opts = plan_opts(tmp_path, managed=False, downloads=True, sources=[dl])
        assert run_plan(MediaKind.TV, config, opts, providers) == 0
        manifest = parse_plan_manifest(tmp_path / "plan.kdl")
        assert [b.raw_title for b in manifest.blocks] == ["Severance"]

    def test_mapping_domain_protects_own_titles(self, tmp_path, monkeypatch):
        """domain matching own root wins over index/tree foreign signals."""
        from etp_lib import arr as arr_mod
        from etp_lib.arr import ArrEntry
        from etp_lib.types import TitleMapping
        from etp_lib.video_ingest import _drop_foreign_domain

        monkeypatch.setattr(video_ingest, "_anime_tree_names", lambda: set())
        dl = tmp_path / "downloads"
        f = _mkfile(dl / "mon" / "Monster - 01.mkv")
        index = {}
        arr_mod._index(
            index,
            ArrEntry(title="Monster", year=2004, folder="Monster (2004)", root="anime"),
            [],
        )
        titles = [self._group("Monster", f)]
        mappings = {"monster": TitleMapping(domain="television")}
        kept, dropped = _drop_foreign_domain(
            titles, index, "television", [dl], mappings
        )
        assert dropped == 0
        assert [t.title for t in kept] == ["Monster"]
