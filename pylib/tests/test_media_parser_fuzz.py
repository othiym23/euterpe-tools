"""Fuzz tests for the media path parser using Hypothesis."""

from __future__ import annotations

from hypothesis import given, settings, HealthCheck
from hypothesis import strategies as st

from etp_lib import media_parser as mp


# ===================================================================
# Strategies
# ===================================================================

# Building blocks for structured filenames
_GROUPS = st.sampled_from(
    ["SubGroup", "MTBB", "Hinna", "Cyan", "pspspsps", "Erai-raws", "A&C"]
)
_SERIES_EN = st.sampled_from(
    [
        "Golden Kamuy",
        "BEASTARS",
        "Re ZERO",
        "Nights with a Cat",
        "My Hero Academia",
        "Chainsaw Man",
    ]
)
_SERIES_JA = st.sampled_from(
    [
        "ゴールデンカムイ",
        "違国日記",
        "夜は猫といっしょ",
        "呪術廻戦",
        "推しの子",
        "葬送のフリーレン",
    ]
)
_SEASONS = st.integers(min_value=0, max_value=20)
_EPISODES = st.integers(min_value=0, max_value=999)
_RESOLUTIONS = st.sampled_from(["480p", "720p", "1080p", "2160p", "4K"])
_VIDEO_CODECS = st.sampled_from(["x264", "x265", "HEVC", "AVC", "AV1", "VP9"])
_AUDIO_CODECS = st.sampled_from(["AAC", "FLAC", "opus", "AC3", "DTS", "DTS-HD", "EAC3"])
_SOURCES = st.sampled_from(
    [
        "BD",
        "BluRay",
        "WEB",
        "WEB-DL",
        "WEBRip",
        "HDTV",
        "SDTV",
        "DVD",
        "DVDRip",
        "DVD-R",
        "VCD",
        "CD-R",
        "CR",
        "AMZN",
        "NF",
    ]
)
_HASHES = st.from_regex(r"[0-9A-F]{8}", fullmatch=True)
_YEARS = st.integers(min_value=1950, max_value=2030)
_EXTENSIONS = st.sampled_from([".mkv", ".mp4", ".avi", ".flac", ".m4a"])
_VERSIONS = st.integers(min_value=2, max_value=5)


def _fansub_filename():
    """Generate fansub-style filenames: [Group] Title - 01 [meta] [hash].ext"""
    return st.builds(
        lambda group, title, ep, res, codec, source, hash_, ext, year, version: (
            f"[{group}] {title}"
            + (f" ({year})" if year else "")
            + f" - {ep:02d}"
            + (f"v{version}" if version else "")
            + (f" [{source} {res} {codec}]" if res else "")
            + (f" [{hash_}]" if hash_ else "")
            + ext
        ),
        group=_GROUPS,
        title=st.one_of(_SERIES_EN, _SERIES_JA),
        ep=_EPISODES,
        res=st.one_of(st.just(""), _RESOLUTIONS),
        codec=_VIDEO_CODECS,
        source=_SOURCES,
        hash_=st.one_of(st.just(""), _HASHES),
        ext=_EXTENSIONS,
        year=st.one_of(st.just(None), _YEARS),
        version=st.one_of(st.just(None), _VERSIONS),
    )


def _scene_filename():
    """Generate scene-style filenames: Title.S01E05.1080p.BluRay.x265-GROUP.mkv"""
    return st.builds(
        lambda title, season, ep, res, source, codec, group, ext: (
            title.replace(" ", ".")
            + f".S{season:02d}E{ep:02d}"
            + (f".{res}" if res else "")
            + f".{source}.{codec}-{group}"
            + ext
        ),
        title=_SERIES_EN,
        season=_SEASONS,
        ep=_EPISODES,
        res=st.one_of(st.just(""), _RESOLUTIONS),
        source=_SOURCES,
        codec=_VIDEO_CODECS,
        group=_GROUPS,
        ext=_EXTENSIONS,
    )


def _sonarr_filename():
    """Generate Sonarr-style filenames: Title - s1e01 - Episode Name [Group source-res,...].ext"""
    return st.builds(
        lambda title, season, ep, ep_name, group, source, res, codec, ext: (
            f"{title} - s{season}e{ep:02d}"
            + (f" - {ep_name}" if ep_name else "")
            + f" [{group} {source},{res},{codec}]"
            + ext
        ),
        title=_SERIES_EN,
        season=st.integers(min_value=1, max_value=10),
        ep=_EPISODES,
        ep_name=st.one_of(
            st.just(""),
            st.sampled_from(["Wenkamuy", "The Beginning", "Final Battle"]),
        ),
        group=_GROUPS,
        source=_SOURCES,
        res=_RESOLUTIONS,
        codec=_VIDEO_CODECS,
        ext=_EXTENSIONS,
    )


def _bare_episode_filename():
    """Generate bare episode filenames: Title - 01.mkv"""
    return st.builds(
        lambda title, ep, version, ext: (
            f"{title} - {ep:02d}" + (f"v{version}" if version else "") + ext
        ),
        title=st.one_of(_SERIES_EN, _SERIES_JA),
        ep=_EPISODES,
        version=st.one_of(st.just(None), _VERSIONS),
        ext=_EXTENSIONS,
    )


def _media_path():
    """Generate full media paths with directory components."""
    return st.builds(
        lambda title, year, season, filename: (
            f"{title} ({year})/Season {season:02d}/{filename}"
        ),
        title=_SERIES_EN,
        year=_YEARS,
        season=st.integers(min_value=0, max_value=10),
        filename=st.one_of(_fansub_filename(), _scene_filename(), _sonarr_filename()),
    )


# All filename styles combined
_any_filename = st.one_of(
    _fansub_filename(),
    _scene_filename(),
    _sonarr_filename(),
    _bare_episode_filename(),
)


# ===================================================================
# Tests: never-crash invariants
# ===================================================================


class TestNeverCrash:
    """The parser should never raise on any input string."""

    @given(text=st.text(min_size=0, max_size=500))
    @settings(max_examples=500, suppress_health_check=[HealthCheck.too_slow])
    def test_parse_component_arbitrary_text(self, text):
        result = mp.parse_component(text)
        assert isinstance(result, mp.ParsedMedia)

    @given(text=st.text(min_size=0, max_size=500))
    @settings(max_examples=500, suppress_health_check=[HealthCheck.too_slow])
    def test_parse_media_path_arbitrary_text(self, text):
        result = mp.parse_media_path(text)
        assert isinstance(result, mp.ParsedMedia)

    @given(
        text=st.text(
            alphabet=st.characters(categories=("L", "N", "P", "S", "Z")),
            min_size=0,
            max_size=300,
        )
    )
    @settings(max_examples=500, suppress_health_check=[HealthCheck.too_slow])
    def test_tokenize_arbitrary_text(self, text):
        tokens = mp.tokenize_component(text)
        assert isinstance(tokens, list)

    @given(
        text=st.binary(min_size=0, max_size=200).map(
            lambda b: b.decode("utf-8", errors="replace")
        )
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_parse_component_decoded_bytes(self, text):
        result = mp.parse_component(text)
        assert isinstance(result, mp.ParsedMedia)

    @given(filename=_any_filename)
    @settings(max_examples=500, suppress_health_check=[HealthCheck.too_slow])
    def test_structured_filenames_never_crash(self, filename):
        result = mp.parse_component(filename)
        assert isinstance(result, mp.ParsedMedia)

    @given(path=_media_path())
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_full_paths_never_crash(self, path):
        result = mp.parse_media_path(path)
        assert isinstance(result, mp.ParsedMedia)


# ===================================================================
# Tests: idempotency
# ===================================================================


class TestIdempotency:
    """Parsing the same input twice should produce identical results."""

    @given(filename=_any_filename)
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_parse_component_idempotent(self, filename):
        a = mp.parse_component(filename)
        b = mp.parse_component(filename)
        assert a.series_name == b.series_name
        assert a.episode == b.episode
        assert a.season == b.season
        assert a.source_type == b.source_type
        assert a.resolution == b.resolution
        assert a.video_codec == b.video_codec
        assert a.release_group == b.release_group
        assert a.hash_code == b.hash_code
        assert a.year == b.year
        assert a.is_remux == b.is_remux

    @given(text=st.text(min_size=0, max_size=300))
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_parse_component_idempotent_arbitrary(self, text):
        a = mp.parse_component(text)
        b = mp.parse_component(text)
        assert a.series_name == b.series_name
        assert a.episode == b.episode
        assert a.source_type == b.source_type


# ===================================================================
# Tests: structured property checks
# ===================================================================

# Reuse the parser's canonical source type mapping (lowercased keys)
# to validate that structured filenames produce the expected source_type.
_SOURCE_TYPE_MAP = mp._SOURCE_TYPE_MAP


class TestStructuredProperties:
    """When known tokens appear in the input, they should be recognized."""

    @given(
        group=_GROUPS,
        title=_SERIES_EN,
        ep=st.integers(min_value=1, max_value=999),
        res=_RESOLUTIONS,
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_resolution_detected(self, group, title, ep, res):
        filename = f"[{group}] {title} - {ep:02d} [{res}].mkv"
        pm = mp.parse_component(filename)
        assert pm.resolution == res or pm.resolution == res.upper()

    @given(
        title=_SERIES_EN,
        season=st.integers(min_value=1, max_value=20),
        ep=st.integers(min_value=1, max_value=99),
        source=_SOURCES,
        codec=_VIDEO_CODECS,
        group=_GROUPS,
    )
    @settings(max_examples=300, suppress_health_check=[HealthCheck.too_slow])
    def test_scene_source_type_detected(self, title, season, ep, source, codec, group):
        filename = (
            f"{title.replace(' ', '.')}.S{season:02d}E{ep:02d}"
            f".1080p.{source}.{codec}-{group}.mkv"
        )
        pm = mp.parse_component(filename)
        expected = _SOURCE_TYPE_MAP.get(source.lower())
        if expected:
            assert pm.source_type == expected, (
                f"source={source!r} → expected {expected!r}, "
                f"got {pm.source_type!r} for {filename!r}"
            )

    @given(
        title=_SERIES_EN,
        season=st.integers(min_value=1, max_value=10),
        ep=st.integers(min_value=1, max_value=99),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_season_episode_detected(self, title, season, ep):
        filename = f"{title} S{season:02d}E{ep:02d}.mkv"
        pm = mp.parse_component(filename)
        assert pm.season == season, f"expected season {season}, got {pm.season}"
        assert pm.episode == ep, f"expected episode {ep}, got {pm.episode}"

    @given(
        group=_GROUPS,
        title=_SERIES_EN,
        ep=st.integers(min_value=1, max_value=999),
        hash_=_HASHES,
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_hash_detected(self, group, title, ep, hash_):
        filename = f"[{group}] {title} - {ep:02d} [{hash_}].mkv"
        pm = mp.parse_component(filename)
        assert pm.hash_code == hash_

    @given(
        title=_SERIES_EN,
        year=_YEARS,
        ep=st.integers(min_value=1, max_value=99),
    )
    @settings(max_examples=200, suppress_health_check=[HealthCheck.too_slow])
    def test_year_detected(self, title, year, ep):
        filename = f"[Group] {title} ({year}) - {ep:02d}.mkv"
        pm = mp.parse_component(filename)
        assert pm.year == year


# ===================================================================
# Tests: boundary conditions
# ===================================================================


class TestBoundaryConditions:
    """Edge cases and boundary conditions."""

    def test_empty_string(self):
        pm = mp.parse_component("")
        assert pm.series_name == ""
        assert pm.episode is None

    def test_just_extension(self):
        pm = mp.parse_component(".mkv")
        assert pm.extension == ".mkv"

    def test_only_brackets(self):
        pm = mp.parse_component("[][][]")
        assert isinstance(pm, mp.ParsedMedia)

    def test_only_dots(self):
        pm = mp.parse_component(".....")
        assert isinstance(pm, mp.ParsedMedia)

    def test_only_dashes(self):
        pm = mp.parse_component("-----")
        assert isinstance(pm, mp.ParsedMedia)

    def test_very_long_filename(self):
        title = "A" * 500
        pm = mp.parse_component(f"[Group] {title} - 01.mkv")
        assert isinstance(pm, mp.ParsedMedia)

    def test_nested_brackets(self):
        pm = mp.parse_component("[Group [inner]] Title - 01.mkv")
        assert isinstance(pm, mp.ParsedMedia)

    def test_unicode_brackets(self):
        pm = mp.parse_component("「エピソードタイトル」.mkv")
        assert isinstance(pm, mp.ParsedMedia)

    def test_mixed_separators(self):
        pm = mp.parse_component("[G] T.i.t.l.e - 01 - Name.mkv")
        assert isinstance(pm, mp.ParsedMedia)

    def test_episode_zero(self):
        pm = mp.parse_component("[Group] Title - 00.mkv")
        assert pm.episode == 0

    def test_episode_999(self):
        pm = mp.parse_component("[Group] Title - 999.mkv")
        assert pm.episode == 999

    def test_season_zero(self):
        pm = mp.parse_component("Title S00E01.mkv")
        assert pm.season == 0
        assert pm.episode == 1

    def test_null_bytes_in_input(self):
        pm = mp.parse_component("Title\x00Episode.mkv")
        assert isinstance(pm, mp.ParsedMedia)

    def test_all_metadata_no_title(self):
        pm = mp.parse_component("[1080p BD HEVC FLAC].mkv")
        assert isinstance(pm, mp.ParsedMedia)

    def test_multiple_years(self):
        pm = mp.parse_component("[Group] Show (2020) - 01 (2021).mkv")
        assert pm.year == 2020  # First year wins

    def test_multiple_episodes(self):
        pm = mp.parse_component("Show S01E05 S02E10.mkv")
        # First episode wins
        assert pm.season == 1
        assert pm.episode == 5

    def test_path_with_many_components(self):
        path = "a/b/c/d/e/f/g/h/file.mkv"
        pm = mp.parse_media_path(path)
        assert isinstance(pm, mp.ParsedMedia)

    def test_remux_implies_bd(self):
        pm = mp.parse_component("[Group] Show - 01 [REMUX 1080p].mkv")
        assert pm.is_remux is True
        assert pm.source_type == "BD"

    def test_source_type_not_overwritten(self):
        """First source type wins — HDTV is not overwritten by later WEB."""
        pm = mp.parse_component("[HDTV] [WEB] Show.mkv")
        assert pm.source_type == "HDTV"
