"""Tests for the movies/television plan/apply ingestion core."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from hypothesis import given, strategies as st

from etp_lib import video_ingest
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
