"""Tests for the movies/television plan/apply ingestion core."""

from __future__ import annotations

import json
import shutil
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
        assert "duplicates" in skipped.note

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
