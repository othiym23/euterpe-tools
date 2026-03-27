"""Tests for etp-anime."""

from __future__ import annotations

from pathlib import Path

import pytest

from etp_commands import anime


# ===================================================================
# Test classes
# ===================================================================


class TestParseSourceFilename:
    """Tests for source filename parsing."""

    def test_bracketed_group_with_dash_episode(self):
        sf = anime.parse_source_filename(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        assert sf.release_group == "Cyan"
        assert sf.parsed_episode == 8
        assert sf.hash_code == "D98B31F3"

    def test_scene_format_with_s_e(self):
        sf = anime.parse_source_filename("BEASTARS.S01E05.1080p.BluRay.x264-GROUP.mkv")
        assert sf.parsed_season == 1
        assert sf.parsed_episode == 5
        assert sf.source_type == "BD"

    def test_erai_raws_format(self):
        sf = anime.parse_source_filename(
            "[Erai-raws] Champignon no Majo - 11 "
            "[1080p CR WEB-DL AVC AAC][MultiSub][0A021911].mkv"
        )
        assert sf.release_group == "Erai-raws"
        assert sf.parsed_episode == 11
        assert sf.source_type == "Web"
        assert sf.hash_code == "0A021911"

    def test_no_group_no_hash(self):
        sf = anime.parse_source_filename("My Anime - 03 (1080p).mkv")
        assert sf.release_group == ""
        assert sf.parsed_episode == 3
        assert sf.hash_code == ""

    def test_bd_remux_detection(self):
        sf = anime.parse_source_filename("[Group] Anime - 01 [BDREMUX 1080p HEVC].mkv")
        assert sf.source_type == "BD"
        assert sf.is_remux is True

    def test_web_sources(self):
        sf = anime.parse_source_filename(
            "[Erai-raws] Show - 01 [1080p CR WEB-DL AVC].mkv"
        )
        assert sf.source_type == "Web"

    def test_sonarr_format(self):
        sf = anime.parse_source_filename(
            "BEASTARS - s1e01 - The Moon and the Beast "
            "[NH Bluray-1080p,10bit,x264,AAC].mkv"
        )
        assert sf.parsed_season == 1
        assert sf.parsed_episode == 1
        assert sf.source_type == "BD"

    def test_no_episode_number(self):
        sf = anime.parse_source_filename("[Group] Movie Title [BD 1080p].mkv")
        assert sf.parsed_episode is None

    def test_scene_trailing_group(self):
        sf = anime.parse_source_filename(
            "Re.ZERO.Starting.Life.in.Another.World.S03E09.1080p.CR.WEB-DL.AAC2.0.H.264.DUAL-VARYG.mkv"
        )
        assert sf.release_group == "VARYG"
        assert sf.parsed_season == 3
        assert sf.parsed_episode == 9

    def test_scene_group_not_overridden_by_bracket(self):
        """Bracket group takes priority over scene trailing group."""
        sf = anime.parse_source_filename("[FLE] Show - 01 [1080p]-GROUP.mkv")
        assert sf.release_group == "FLE"

    def test_bracket_group_fallback(self):
        """Short bracketed tag like [PMR] at end is picked up as release group."""
        sf = anime.parse_source_filename(
            "Re ZERO Starting Life in Another World - S03E01v2 "
            "(BD Remux 1080p AVC FLAC E-AC-3) [Dual Audio] [PMR].mkv"
        )
        assert sf.release_group == "PMR"
        assert sf.parsed_season == 3
        assert sf.parsed_episode == 1
        assert sf.version == 2

    def test_sonarr_metadata_block_group(self):
        """Sonarr-style metadata block '[GROUP QUALITY-res,...]' extracts group."""
        sf = anime.parse_source_filename(
            "You and I Are Polar Opposites - s01e01 - You, My Polar Opposite "
            "[VARYG WEBDL-1080p,8bit,x264,AAC].mkv"
        )
        assert sf.release_group == "VARYG"

    def test_sonarr_metadata_block_erai_raws(self):
        """Sonarr metadata block with hyphenated group name."""
        sf = anime.parse_source_filename(
            "Show - s01e11 - Title [Erai-raws WEBDL-1080p,8bit,x264,AAC].mkv"
        )
        assert sf.release_group == "Erai-raws"

    def test_bracket_group_not_crc32(self):
        """8-char hex in brackets is a CRC32 hash, not a release group."""
        sf = anime.parse_source_filename("[FLE] Show - 01 [4CC4766E].mkv")
        assert sf.release_group == "FLE"
        assert sf.hash_code == "4CC4766E"

    def test_version_dash_format(self):
        sf = anime.parse_source_filename("[MTBB] Title - 05v2 [hash1234].mkv")
        assert sf.release_group == "MTBB"
        assert sf.parsed_episode == 5
        assert sf.version == 2

    def test_version_s_e_format(self):
        sf = anime.parse_source_filename("Show.S01E05v3.1080p.BluRay.mkv")
        assert sf.parsed_season == 1
        assert sf.parsed_episode == 5
        assert sf.version == 3

    def test_no_version(self):
        sf = anime.parse_source_filename("[Group] Title - 05 [hash1234].mkv")
        assert sf.parsed_episode == 5
        assert sf.version is None

    def test_version_in_metadata_block(self):
        sf = anime.SourceFile(
            path=Path("test.mkv"),
            release_group="MTBB",
            version=2,
            media=anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )
        block = anime.build_metadata_block(sf)
        assert block.startswith("MTBB(v2)")

    def test_no_version_in_metadata_block(self):
        sf = anime.SourceFile(
            path=Path("test.mkv"),
            release_group="MTBB",
            media=anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )
        block = anime.build_metadata_block(sf)
        assert block.startswith("MTBB Web,")


class TestDirectoryNaming:
    """Tests for directory creation and ID files."""

    def test_create_series_directory(self, tmp_path):
        info = anime.AnimeInfo(
            anidb_id=28,
            tvdb_id=None,
            title_ja="アキラ",
            title_en="Akira",
            year=1988,
            episodes=[],
        )
        series_dir = anime.create_series_directory(tmp_path, info, seasons=[1])
        assert series_dir.is_dir()
        assert (series_dir / "Season 01").is_dir()
        # Specials dir is created on demand, not eagerly
        assert not (series_dir / "Specials").exists()
        assert (series_dir / "anidb.id").read_text().strip() == "28"

    def test_create_series_directory_tvdb(self, tmp_path):
        info = anime.AnimeInfo(
            anidb_id=None,
            tvdb_id=79604,
            title_ja="ブラックラグーン",
            title_en="BLACK LAGOON",
            year=2006,
            episodes=[],
        )
        series_dir = anime.create_series_directory(tmp_path, info, seasons=[1, 2])
        assert (series_dir / "Season 01").is_dir()
        assert (series_dir / "Season 02").is_dir()
        assert (series_dir / "tvdb.id").read_text().strip() == "79604"

    def test_create_series_directory_dry_run(self, tmp_path, capsys):
        info = anime.AnimeInfo(
            anidb_id=28,
            tvdb_id=None,
            title_ja="アキラ",
            title_en="Akira",
            year=1988,
            episodes=[],
        )
        anime.create_series_directory(tmp_path, info, seasons=[1], dry_run=True)
        captured = capsys.readouterr()
        assert "[dry-run]" in captured.out


class TestScanDestIds:
    """Tests for scanning destination directory for ID files."""

    def test_finds_anidb_id(self, tmp_path):
        series = tmp_path / "アキラ [Akira] (1988)"
        series.mkdir()
        (series / "anidb.id").write_text("28\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result[("anidb", 28)] == series

    def test_finds_tvdb_id(self, tmp_path):
        series = tmp_path / "BLACK LAGOON (2006)"
        series.mkdir()
        (series / "tvdb.id").write_text("79604\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result[("tvdb", 79604)] == series

    def test_finds_both_ids(self, tmp_path):
        series = tmp_path / "Some Anime (2020)"
        series.mkdir()
        (series / "anidb.id").write_text("1234\n")
        (series / "tvdb.id").write_text("5678\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result[("anidb", 1234)] == series
        assert result[("tvdb", 5678)] == series

    def test_multiple_series(self, tmp_path):
        s1 = tmp_path / "Series One"
        s2 = tmp_path / "Series Two"
        s1.mkdir()
        s2.mkdir()
        (s1 / "anidb.id").write_text("100\n")
        (s2 / "anidb.id").write_text("200\n")

        result = anime.scan_dest_ids(tmp_path)
        assert len(result) == 2
        assert result[("anidb", 100)] == s1
        assert result[("anidb", 200)] == s2

    def test_empty_directory(self, tmp_path):
        assert anime.scan_dest_ids(tmp_path) == {}

    def test_nonexistent_directory(self):
        assert anime.scan_dest_ids(Path("/nonexistent")) == {}

    def test_ignores_invalid_id(self, tmp_path):
        series = tmp_path / "Bad ID"
        series.mkdir()
        (series / "anidb.id").write_text("not_a_number\n")

        result = anime.scan_dest_ids(tmp_path)
        assert result == {}

    def test_ignores_files_in_dest(self, tmp_path):
        (tmp_path / "somefile.txt").write_text("hello")
        assert anime.scan_dest_ids(tmp_path) == {}


class TestResolveSeriesDirectory:
    """Tests for the 3-step directory resolution."""

    def _make_info(self, anidb_id: int | None = 28, tvdb_id: int | None = None):  # type: ignore[no-untyped-def]
        return anime.AnimeInfo(
            anidb_id=anidb_id,
            tvdb_id=tvdb_id,
            title_ja="アキラ",
            title_en="Akira",
            year=1988,
            episodes=[],
        )

    def test_finds_by_anidb_id(self, tmp_path):
        existing = tmp_path / "Akira (old name)"
        existing.mkdir()
        id_map = {("anidb", 28): existing}

        result = anime.resolve_series_directory(
            tmp_path, self._make_info(), id_map=id_map
        )
        assert result == existing

    def test_finds_by_tvdb_id(self, tmp_path):
        existing = tmp_path / "Akira (different name)"
        existing.mkdir()
        id_map = {("tvdb", 5678): existing}

        result = anime.resolve_series_directory(
            tmp_path, self._make_info(anidb_id=None, tvdb_id=5678), id_map=id_map
        )
        assert result == existing

    def test_finds_by_conventional_name(self, tmp_path):
        conventional = tmp_path / "アキラ [Akira] (1988)"
        conventional.mkdir()

        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result == conventional

    def test_creates_new_when_no_match(self, tmp_path, monkeypatch):
        monkeypatch.setattr("builtins.input", lambda _: "")
        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result.name == "アキラ [Akira] (1988)"
        assert result.is_dir()
        assert (result / "anidb.id").read_text().strip() == "28"

    def test_id_match_takes_priority_over_name(self, tmp_path):
        """If both ID and conventional name exist, ID wins."""
        id_dir = tmp_path / "Akira (custom name)"
        id_dir.mkdir()
        conventional = tmp_path / "アキラ [Akira] (1988)"
        conventional.mkdir()
        id_map = {("anidb", 28): id_dir}

        result = anime.resolve_series_directory(
            tmp_path, self._make_info(), id_map=id_map
        )
        assert result == id_dir

    def test_creates_season_subdirs_on_id_match(self, tmp_path):
        existing = tmp_path / "Akira (old)"
        existing.mkdir()
        id_map = {("anidb", 28): existing}

        anime.resolve_series_directory(
            tmp_path,
            self._make_info(),
            id_map=id_map,
            seasons=[1, 2],
        )
        assert (existing / "Season 01").is_dir()
        assert (existing / "Season 02").is_dir()

    def test_writes_id_file_on_name_match(self, tmp_path):
        conventional = tmp_path / "アキラ [Akira] (1988)"
        conventional.mkdir()

        anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert (conventional / "anidb.id").read_text().strip() == "28"

    def test_manual_path_absolute(self, tmp_path, monkeypatch):
        manual = tmp_path / "My Custom Akira Dir"
        manual.mkdir()
        monkeypatch.setattr("builtins.input", lambda _: str(manual))

        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result == manual

    def test_manual_path_relative(self, tmp_path, monkeypatch):
        manual = tmp_path / "My Custom Akira Dir"
        manual.mkdir()
        monkeypatch.setattr("builtins.input", lambda _: "My Custom Akira Dir")

        result = anime.resolve_series_directory(tmp_path, self._make_info(), id_map={})
        assert result == manual


class TestCLI:
    """Tests for subcommand-based CLI argument parsing."""

    def test_triage_subcommand(self):
        parser = anime.build_parser()
        args = parser.parse_args(["triage"])
        assert args.command == "triage"

    def test_triage_with_pattern(self):
        parser = anime.build_parser()
        args = parser.parse_args(["triage", "beastars"])
        assert args.command == "triage"
        assert args.pattern == "beastars"

    def test_triage_with_dry_run(self):
        parser = anime.build_parser()
        args = parser.parse_args(["triage", "--dry-run"])
        assert args.dry_run is True

    def test_triage_with_force(self):
        parser = anime.build_parser()
        args = parser.parse_args(["triage", "--force"])
        assert args.force is True

    def test_series_subcommand(self):
        parser = anime.build_parser()
        args = parser.parse_args(["series"])
        assert args.command == "series"

    def test_series_with_pattern(self):
        parser = anime.build_parser()
        args = parser.parse_args(["series", "beastars"])
        assert args.command == "series"
        assert args.pattern == "beastars"

    def test_episode_requires_file_and_id(self):
        parser = anime.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["episode"])  # no file

    def test_episode_with_anidb(self):
        parser = anime.build_parser()
        args = parser.parse_args(["episode", "/tmp/test.mkv", "--anidb", "28"])
        assert args.command == "episode"
        assert args.file == Path("/tmp/test.mkv")
        assert args.anidb == 28
        assert args.tvdb is None

    def test_episode_with_tvdb(self):
        parser = anime.build_parser()
        args = parser.parse_args(["episode", "/tmp/test.mkv", "--tvdb", "12345"])
        assert args.command == "episode"
        assert args.tvdb == 12345
        assert args.anidb is None

    def test_episode_anidb_and_tvdb_mutually_exclusive(self):
        parser = anime.build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(
                ["episode", "/tmp/test.mkv", "--anidb", "28", "--tvdb", "12345"]
            )


class TestAnimeConfig:
    """Tests for KDL config loading and series mapping."""

    def test_defaults_when_no_file(self, tmp_path):
        config = anime.load_anime_config(tmp_path / "nonexistent.kdl")
        assert config.downloads_dir == anime.DEFAULT_DOWNLOADS_DIR
        assert config.anime_source_dir == anime.DEFAULT_ANIME_SOURCE_DIR
        assert config.anime_dest_dir == anime.DEFAULT_DEST_DIR
        assert config.series_mappings == {}

    def test_load_paths(self, tmp_path):
        cfg = tmp_path / "config.kdl"
        cfg.write_text(
            'paths {\n  downloads-dir "/tmp/dl"\n  anime-dest-dir "/tmp/dest"\n}\n',
            encoding="utf-8",
        )
        config = anime.load_anime_config(cfg)
        assert config.downloads_dir == Path("/tmp/dl")
        assert config.anime_dest_dir == Path("/tmp/dest")
        # Unset field keeps default
        assert config.anime_source_dir == anime.DEFAULT_ANIME_SOURCE_DIR

    def test_load_series_mappings(self, tmp_path):
        cfg = tmp_path / "config.kdl"
        cfg.write_text(
            'series "BEASTARS" {\n  anidb 14659\n}\n'
            'series "Re ZERO" {\n  tvdb 305089\n}\n',
            encoding="utf-8",
        )
        config = anime.load_anime_config(cfg)
        assert config.series_mappings["BEASTARS"] == [("anidb", 14659)]
        assert config.series_mappings["Re ZERO"] == [("tvdb", 305089)]

    def test_save_series_mapping(self, tmp_path):
        cfg = tmp_path / "config.kdl"
        cfg.write_text("// empty config\n", encoding="utf-8")
        anime.save_series_mapping("Test Show", "anidb", 12345, path=cfg)
        content = cfg.read_text(encoding="utf-8")
        assert 'series "Test Show"' in content
        assert "anidb 12345" in content
        # Verify it's loadable
        config = anime.load_anime_config(cfg)
        assert config.series_mappings["Test Show"] == [("anidb", 12345)]

    def test_lookup_case_insensitive(self):
        config = anime.AnimeConfig(series_mappings={"BEASTARS": [("anidb", 14659)]})
        assert anime.lookup_series_ids("beastars", config) == [("anidb", 14659)]
        assert anime.lookup_series_ids("BEASTARS", config) == [("anidb", 14659)]
        assert anime.lookup_series_ids("unknown", config) == []

    def test_multiple_ids_per_series(self, tmp_path):
        cfg = tmp_path / "config.kdl"
        cfg.write_text(
            'series "Chained Soldier" {\n  anidb 17330\n  anidb 18548\n}\n',
            encoding="utf-8",
        )
        config = anime.load_anime_config(cfg)
        ids = config.series_mappings["Chained Soldier"]
        assert len(ids) == 2
        assert ("anidb", 17330) in ids
        assert ("anidb", 18548) in ids


class TestExtractSeriesName:
    """Tests for per-file series name extraction."""

    def test_bracketed_group_stripped(self):
        name = anime._extract_series_name(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        assert name == "Champignon no Majo"

    def test_sonarr_format(self):
        name = anime._extract_series_name(
            "BEASTARS - s1e01 - The Moon and the Beast "
            "[NH Bluray-1080p,10bit,x264,AAC].mkv"
        )
        assert name == "BEASTARS"

    def test_scene_format(self):
        name = anime._extract_series_name(
            "Girls.und.Panzer.S01E05.1080p.BluRay.x264-GROUP.mkv"
        )
        assert name == "Girls und Panzer"

    def test_no_group_no_hash(self):
        name = anime._extract_series_name("My Anime - 03 (1080p).mkv")
        assert name == "My Anime"

    def test_movie_no_episode(self):
        name = anime._extract_series_name("[Group] Movie Title [BD 1080p].mkv")
        assert name == "Movie Title"

    def test_empty_filename(self):
        name = anime._extract_series_name("")
        assert name == ""


class TestScanAndGroup:
    """Tests for scanning and grouping source files."""

    def test_groups_by_series_name(self, tmp_path):
        # Create some fake media files
        (tmp_path / "[Cyan] Show A - 01 [1080p][AAAA1111].mkv").touch()
        (tmp_path / "[Cyan] Show A - 02 [1080p][BBBB2222].mkv").touch()
        (tmp_path / "[Cyan] Show B - 01 [1080p][CCCC3333].mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        assert len(groups) == 2
        # Show A should have 2 files
        assert any(len(files) == 2 for files in groups.values())
        # Show B should have 1 file
        assert any(len(files) == 1 for files in groups.values())

    def test_ordered_by_count_descending(self, tmp_path):
        (tmp_path / "[G] Big - 01.mkv").touch()
        (tmp_path / "[G] Big - 02.mkv").touch()
        (tmp_path / "[G] Big - 03.mkv").touch()
        (tmp_path / "[G] Small - 01.mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        counts = [len(files) for files in groups.values()]
        assert counts == sorted(counts, reverse=True)

    def test_subdirectory_scanning(self, tmp_path):
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "[G] SubShow - 01.mkv").touch()
        (sub / "[G] SubShow - 02.mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        assert len(groups) == 1
        assert len(list(groups.values())[0]) == 2

    def test_non_media_files_ignored(self, tmp_path):
        (tmp_path / "readme.txt").touch()
        (tmp_path / "subtitle.srt").touch()
        (tmp_path / "[G] Show - 01.mkv").touch()

        groups = anime._scan_and_group([tmp_path])
        assert len(groups) == 1

    def test_empty_directory(self, tmp_path):
        groups = anime._scan_and_group([tmp_path])
        assert groups == {}

    def test_nonexistent_directory(self):
        groups = anime._scan_and_group([Path("/nonexistent/path")])
        assert groups == {}

    def test_multiple_source_dirs(self, tmp_path):
        dir_a = tmp_path / "a"
        dir_b = tmp_path / "b"
        dir_a.mkdir()
        dir_b.mkdir()
        (dir_a / "[G] Show - 01.mkv").touch()
        (dir_b / "[G] Show - 02.mkv").touch()

        groups = anime._scan_and_group([dir_a, dir_b])
        assert len(groups) == 1
        assert len(list(groups.values())[0]) == 2


class TestExtractConciseName:
    """Tests for concise name extraction from filenames."""

    def test_bracketed_group_stripped(self):
        sf = anime.SourceFile(
            path=Path("[Cyan] Champignon no Majo - 08 [1080p][ABCD1234].mkv")
        )
        name = anime._extract_concise_name([sf])
        assert name == "Champignon no Majo"

    def test_sonarr_format(self):
        sf = anime.SourceFile(
            path=Path(
                "BEASTARS - s1e01 - The Moon and the Beast "
                "[NH Bluray-1080p,10bit,x264,AAC].mkv"
            )
        )
        name = anime._extract_concise_name([sf])
        assert name == "BEASTARS"

    def test_empty_list(self):
        assert anime._extract_concise_name([]) == ""


class TestStripYear:
    """Tests for _strip_year removing trailing (YYYY) from series names."""

    def test_strip_trailing_year(self):
        assert anime._strip_year("Golden Kamuy (2018)") == "Golden Kamuy"

    def test_strip_year_with_brackets(self):
        assert (
            anime._strip_year("ゴールデンカムイ [Golden Kamuy] (2018)")
            == "ゴールデンカムイ [Golden Kamuy]"
        )

    def test_no_year_unchanged(self):
        assert anime._strip_year("Golden Kamuy") == "Golden Kamuy"

    def test_year_in_middle_unchanged(self):
        assert anime._strip_year("Show (2020) Special") == "Show (2020) Special"


class TestConciseNameFromConfig:
    """Tests for saved concise name being used as the prompt default."""

    def test_config_concise_name_used(self, monkeypatch):
        """Saved concise name from config takes priority over directory name."""
        captured = {}

        class _Done(Exception):
            pass

        def fake_prompt(prompt, default=""):
            if "concise" in prompt.lower():
                captured["default"] = default
                raise _Done
            return default

        monkeypatch.setattr(anime, "prompt_value", fake_prompt)

        config = anime.AnimeConfig(
            concise_names={"ゴールデンカムイ [Golden Kamuy] (2018)": "Golden Kamuy"},
        )
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="ゴールデンカムイ",
            title_en="Golden Kamuy",
            year=2018,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        parsed = [
            anime.SourceFile(path=Path("ep1.mkv"), parsed_season=1, parsed_episode=1),
        ]
        with pytest.raises(_Done):
            anime._process_group_batch(
                [],
                info,
                {},
                Path("/tmp/test"),
                dry_run=True,
                verbose=False,
                default_concise_name="ゴールデンカムイ [Golden Kamuy] (2018)",
                pre_parsed=parsed,
                config=config,
            )
        assert captured["default"] == "Golden Kamuy"

    def test_year_stripped_without_config(self, monkeypatch):
        """Directory name has year stripped even without config."""
        captured = {}

        class _Done(Exception):
            pass

        def fake_prompt(prompt, default=""):
            if "concise" in prompt.lower():
                captured["default"] = default
                raise _Done
            return default

        monkeypatch.setattr(anime, "prompt_value", fake_prompt)

        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="ゴールデンカムイ",
            title_en="Golden Kamuy",
            year=2018,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        parsed = [
            anime.SourceFile(path=Path("ep1.mkv"), parsed_season=1, parsed_episode=1),
        ]
        with pytest.raises(_Done):
            anime._process_group_batch(
                [],
                info,
                {},
                Path("/tmp/test"),
                dry_run=True,
                verbose=False,
                default_concise_name="Golden Kamuy (2018)",
                pre_parsed=parsed,
            )
        assert captured["default"] == "Golden Kamuy"


class TestGroupDefaults:
    """Tests for sticky group defaults across files."""

    def test_defaults_initial_state(self):
        defaults = anime.GroupDefaults()
        assert defaults.release_group == ""
        assert defaults.source_type == ""

    def test_process_file_prompts_for_missing_group(self, monkeypatch):
        """When release_group is empty, _process_file prompts for it."""
        inputs = iter(["MTBB", "s1e01", "n"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))
        monkeypatch.setattr(
            anime,
            "analyze_file",
            lambda _: anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )

        sf = anime.SourceFile(path=Path("/tmp/test.mkv"))
        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="Test",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        defaults = anime.GroupDefaults()
        anime._process_file(
            sf,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=defaults,
        )
        assert sf.release_group == "MTBB"
        assert defaults.release_group == "MTBB"

    def test_defaults_carry_to_next_file(self, monkeypatch):
        """Defaults set for one file are offered for the next."""
        # First file: user types "MTBB" at release group prompt
        # Second file: user accepts default (empty input)
        prompts_seen: list[str] = []
        call_count = 0

        def fake_input(prompt: str) -> str:
            nonlocal call_count
            prompts_seen.append(prompt)
            call_count += 1
            # File 1: release group prompt → "MTBB", episode confirm → accept,
            #          copy confirm → no
            # File 2: release group prompt → accept default, episode → accept,
            #          copy confirm → no
            if "Release group" in prompt:
                return "MTBB" if call_count <= 3 else ""
            if "Episode" in prompt:
                return ""
            if "Copy" in prompt:
                return "n"
            return ""

        monkeypatch.setattr("builtins.input", fake_input)
        monkeypatch.setattr(
            anime,
            "analyze_file",
            lambda _: anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="Test",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        defaults = anime.GroupDefaults()

        sf1 = anime.SourceFile(path=Path("/tmp/ep01.mkv"), parsed_episode=1)
        anime._process_file(
            sf1,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=defaults,
        )
        assert defaults.release_group == "MTBB"

        sf2 = anime.SourceFile(path=Path("/tmp/ep02.mkv"), parsed_episode=2)
        anime._process_file(
            sf2,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=defaults,
        )
        # Second file should have picked up the default
        assert sf2.release_group == "MTBB"

    def test_no_prompt_when_group_present(self, monkeypatch):
        """No release group prompt when filename already has one."""
        prompts_seen: list[str] = []

        def fake_input(prompt: str) -> str:
            prompts_seen.append(prompt)
            if "Copy" in prompt:
                return "n"
            return ""

        monkeypatch.setattr("builtins.input", fake_input)
        monkeypatch.setattr(
            anime,
            "analyze_file",
            lambda _: anime.MediaInfo(
                video_codec="HEVC",
                resolution="1080p",
                width=1920,
                height=1080,
                bit_depth=8,
                hdr_type="",
            ),
        )

        info = anime.AnimeInfo(
            anidb_id=1,
            tvdb_id=None,
            title_ja="Test",
            title_en="Test",
            year=2020,
            episodes=[],
        )
        sf = anime.SourceFile(
            path=Path("/tmp/test.mkv"),
            release_group="Cyan",
            parsed_episode=1,
        )
        anime._process_file(
            sf,
            info,
            "Test",
            Path("/tmp/out"),
            dry_run=True,
            verbose=False,
            defaults=anime.GroupDefaults(),
        )
        assert not any("Release group" in p for p in prompts_seen)


class TestTriageManifest:
    """Tests for triage copy-tracking manifest."""

    def test_roundtrip(self, monkeypatch, tmp_path):
        cache_dir = tmp_path / "triage"
        cache_dir.mkdir()
        monkeypatch.setattr(
            anime, "_triage_manifest_path", lambda: cache_dir / "copied.json"
        )

        assert anime._load_triage_manifest() == set()

        paths = {"/vol/a.mkv", "/vol/b.mkv"}
        anime._save_triage_manifest(paths)
        assert anime._load_triage_manifest() == paths

    def test_corrupt_manifest(self, monkeypatch, tmp_path):
        manifest = tmp_path / "copied.json"
        manifest.write_text("not json!!!", encoding="utf-8")
        monkeypatch.setattr(anime, "_triage_manifest_path", lambda: manifest)
        assert anime._load_triage_manifest() == set()

    def test_manifest_accumulates(self, monkeypatch, tmp_path):
        cache_dir = tmp_path / "triage"
        cache_dir.mkdir()
        monkeypatch.setattr(
            anime, "_triage_manifest_path", lambda: cache_dir / "copied.json"
        )

        anime._save_triage_manifest({"/vol/a.mkv"})
        loaded = anime._load_triage_manifest()
        loaded.add("/vol/b.mkv")
        anime._save_triage_manifest(loaded)
        assert anime._load_triage_manifest() == {"/vol/a.mkv", "/vol/b.mkv"}


class TestMatchToDownloads:
    """Tests for enriching source files with download metadata."""

    def test_enriches_release_group(self, tmp_path):
        # Source file (Sonarr name with matching release group)
        src = tmp_path / "source" / "Show - s01e01 - Title [FLE WEBDL-1080p].mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 100)

        # Download file (original name, rich metadata, same group)
        dl = tmp_path / "downloads" / "[FLE] Show - 01 [BD 1080p] [ABCD1234].mkv"
        dl.parent.mkdir(parents=True)
        dl.write_bytes(b"x" * 100)  # same size

        index = anime._build_download_index(tmp_path / "downloads")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="Show")

        assert len(enriched) == 1
        assert enriched[0].release_group == "FLE"
        assert enriched[0].hash_code == "ABCD1234"
        assert enriched[0].path == src

    def test_mismatched_group_rejects_match(self, tmp_path):
        """Different release groups = different encodes, no match."""
        src = tmp_path / "source" / "Show - s01e01 - Title [VARYG WEBDL-1080p].mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 100)

        dl = tmp_path / "downloads" / "[FLE] Show - 01 [BD 1080p] [ABCD1234].mkv"
        dl.parent.mkdir(parents=True)
        dl.write_bytes(b"x" * 100)

        index = anime._build_download_index(tmp_path / "downloads")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="Show")

        assert len(enriched) == 1
        assert enriched[0].matched_download is None
        assert enriched[0].release_group == "VARYG WEBDL-1080p"  # unchanged

    def test_picks_closest_size(self, tmp_path):
        src = tmp_path / "source" / "Show - s01e01 - Title.mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 1000)

        # Two downloads for same episode, different sizes
        dl1 = tmp_path / "dl" / "[A] Show - 01 [720p].mkv"
        dl1.parent.mkdir(parents=True)
        dl1.write_bytes(b"x" * 500)  # wrong size

        dl2 = tmp_path / "dl" / "[B] Show - 01 [1080p].mkv"
        dl2.write_bytes(b"x" * 1000)  # same size

        index = anime._build_download_index(tmp_path / "dl")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="Show")

        assert enriched[0].release_group == "B"

    def test_no_match_preserves_original(self, tmp_path):
        src = tmp_path / "Show - s01e05 - Title.mkv"
        src.write_bytes(b"data")

        parsed = anime._parse_files([src])
        original_group = parsed[0].release_group
        enriched = anime._match_to_downloads(parsed, anime.DownloadIndex())

        assert enriched[0].release_group == original_group
        assert enriched[0].path == src

    def test_build_download_index(self, tmp_path):
        (tmp_path / "[G] Show - S01E01.mkv").write_bytes(b"a")
        (tmp_path / "[G] Show - S01E02.mkv").write_bytes(b"b")
        (tmp_path / "[G] Show - S02E01.mkv").write_bytes(b"c")
        (tmp_path / "not-media.txt").write_bytes(b"d")

        index = anime._build_download_index(tmp_path)
        assert index.file_count == 3
        assert len(index.by_series) > 0

    def test_download_index_file_count(self, tmp_path):
        """Regression: file_count must be accessible for progress display."""
        (tmp_path / "[G] Show - S01E01.mkv").write_bytes(b"a")
        (tmp_path / "[G] Show - S01E02.mkv").write_bytes(b"b")
        index = anime._build_download_index(tmp_path)
        assert index.file_count == 2

    def test_series_aware_matching(self, tmp_path):
        """Series-specific match is preferred over global episode match."""
        src = tmp_path / "source" / "ShowA - s01e01 - Title.mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 100)

        # Download for ShowA
        dl_a = tmp_path / "dl" / "[GroupA] ShowA - 01 [1080p].mkv"
        dl_a.parent.mkdir(parents=True)
        dl_a.write_bytes(b"x" * 100)

        # Download for ShowB (same episode number, same size)
        dl_b = tmp_path / "dl" / "[GroupB] ShowB - 01 [1080p].mkv"
        dl_b.write_bytes(b"x" * 100)

        index = anime._build_download_index(tmp_path / "dl")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="ShowA")
        assert enriched[0].release_group == "GroupA"

    def test_no_cross_series_match(self, tmp_path):
        """Regression: downloads from a different series must not match."""
        src = tmp_path / "source" / "ShowA - s01e01 - Title.mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 100)

        # Download is for a different show with same episode number
        dl = tmp_path / "dl" / "[Group] ShowB - S01E01 [1080p].mkv"
        dl.parent.mkdir(parents=True)
        dl.write_bytes(b"x" * 100)  # same size

        index = anime._build_download_index(tmp_path / "dl")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="ShowA")
        # Should NOT pick up ShowB's release group
        assert enriched[0].release_group != "Group"
        assert enriched[0].matched_download is None

    def test_season_zero_does_not_match_regular(self, tmp_path):
        """Season 0 (TVDB specials) must not match regular season episodes."""
        src = tmp_path / "source" / "[FLE] Show - s00e01 - Special [BD 1080p].mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 100)

        dl = tmp_path / "dl" / "[FLE] Show - S01E01 [BD 1080p].mkv"
        dl.parent.mkdir(parents=True)
        dl.write_bytes(b"x" * 100)

        index = anime._build_download_index(tmp_path / "dl")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="Show")
        assert enriched[0].matched_download is None

    def test_size_group_fallback_dvd_order(self, tmp_path):
        """Size+group fallback matches when episode numbers differ (DVD vs aired)."""
        # Source: s01e15 (aired order)
        src = tmp_path / "source" / "[iAHD] Show - s01e15 - Title [iAHD BD-1080p].mkv"
        src.parent.mkdir(parents=True)
        src.write_bytes(b"x" * 500)

        # Download: S02E01 (DVD order) — same group, same size
        dl = tmp_path / "dl" / "Show.S02E01.1080p.Blu-Ray.x265-iAHD.mkv"
        dl.parent.mkdir(parents=True)
        dl.write_bytes(b"x" * 500)

        index = anime._build_download_index(tmp_path / "dl")
        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, index, series_name="Show")
        assert enriched[0].matched_download is not None
        assert enriched[0].matched_download.name == dl.name

    def test_recursive_download_scan(self, tmp_path):
        """Download index finds files in deeply nested directories."""
        nested = tmp_path / "dl" / "Batch Release" / "Season 01"
        nested.mkdir(parents=True)
        dl = nested / "[G] Show - S01E01 [1080p].mkv"
        dl.write_bytes(b"x" * 100)

        index = anime._build_download_index(tmp_path / "dl")
        assert index.file_count == 1

    def test_unseasoned_source_not_enriched(self, tmp_path):
        """Files without season/episode are passed through unchanged."""
        src = tmp_path / "special.mkv"
        src.write_bytes(b"data")

        parsed = anime._parse_files([src])
        enriched = anime._match_to_downloads(parsed, anime.DownloadIndex())
        assert len(enriched) == 1


class TestSubSeriesTitleFiltering:
    """Regression: sub-series title filtering must use exact match."""

    def test_exact_match_includes_base_title(self, monkeypatch):
        """S1 files match when AniDB title matches the file's series name."""
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] 探偵オペラミルキィホームズ(第1期) 第01話「Test」.mkv"
                ),
                parsed_season=1,
                parsed_episode=1,
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=7627,
            tvdb_id=None,
            title_ja="探偵オペラ ミルキィホームズ",
            title_en="Detective Opera Milky Holmes",
            year=2010,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 1

    def test_subseries_with_suffix_excluded(self, monkeypatch):
        """Files from 探偵オペラミルキィホームズ 第2幕 must not match base title."""
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] 探偵オペラミルキィホームズ(第1期) 第01話「S1」.mkv"
                ),
                parsed_season=1,
                parsed_episode=1,
            ),
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] 探偵オペラミルキィホームズ 第2幕(第2期) 第01話「S2」.mkv"
                ),
                parsed_season=2,
                parsed_episode=1,
            ),
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] 探偵オペラミルキィホームズ Alternative(OVA) 第01話「OVA」.mkv"
                ),
                parsed_season=None,
                parsed_episode=1,
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=7627,
            tvdb_id=None,
            title_ja="探偵オペラ ミルキィホームズ",
            title_en="Detective Opera Milky Holmes",
            year=2010,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        # Only the S1 file should match; S2 and Alternative stay in pool
        assert len(matched) == 1
        assert matched[0].parsed_season == 1
        assert len(remaining) == 2

    def test_different_series_title_excluded(self, monkeypatch):
        """Files from ふたりは and 探偵歌劇TD must not match 探偵オペラ."""
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] 探偵オペラミルキィホームズ(第1期) 第01話「S1」.mkv"
                ),
                parsed_season=1,
                parsed_episode=1,
            ),
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] ふたりはミルキィホームズ(第3期) 第01話「S3」.mkv"
                ),
                parsed_season=3,
                parsed_episode=1,
            ),
            anime.SourceFile(
                path=Path(
                    "[アニメ BD] 探偵歌劇ミルキィホームズTD(第4期) 第01話「S4」.mkv"
                ),
                parsed_season=4,
                parsed_episode=1,
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=7627,
            tvdb_id=None,
            title_ja="探偵オペラ ミルキィホームズ",
            title_en="Detective Opera Milky Holmes",
            year=2010,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 1
        assert len(remaining) == 2

    def test_title_filter_bypass_when_no_match(self, monkeypatch):
        """When title filter excludes everything, user can bypass it."""
        # First input: "y" to bypass filter, second: "1" to pick season
        inputs = iter(["y", "1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path("[G] 完全に違うタイトル(第1期) 第01話「Ep」.mkv"),
                parsed_season=1,
                parsed_episode=1,
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="全然マッチしない",
            title_en="No Match",
            year=2020,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 1

    def test_unseasoned_files_promoted_when_all_unseasoned(self, monkeypatch):
        """OVA files with no season/episode should still be processable."""
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path("[G] テスト(OVA) 映像特典「PV」.mkv"),
                parsed_season=None,
                parsed_episode=None,
            ),
            anime.SourceFile(
                path=Path("[G] テスト(OVA)「さようなら」.mkv"),
                parsed_season=None,
                parsed_episode=None,
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        # Both unseasoned files should be included (promoted to season 1)
        assert len(matched) == 2
        assert len(remaining) == 0


class TestBonusFilesNotCountedAgainstEpisodeLimit:
    """Regression: bonus files must not consume regular episode slots."""

    def test_bonus_files_included_with_all_episodes(self, monkeypatch):
        """12 episodes + 7 bonus files = 19 files for a 12-ep AniDB entry.

        All 19 should match — bonuses don't count against the limit.
        """
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = []
        # 12 regular episodes
        for i in range(1, 13):
            pool.append(
                anime.SourceFile(
                    path=Path(f"[アニメ BD] テスト(第1期) 第{i:02d}話「Title」.mkv"),
                    parsed_season=1,
                    parsed_episode=i,
                )
            )
        # 7 bonus files (no episode number)
        for label in ["PV1", "PV2", "PV3", "NCOP", "NCED", "CM1", "CM2"]:
            pool.append(
                anime.SourceFile(
                    path=Path(f"[アニメ BD] テスト(第1期) 映像特典「{label}」.mkv"),
                    parsed_season=1,
                    parsed_episode=None,
                )
            )

        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        eps = [sf for sf in matched if sf.parsed_episode is not None]
        bonus = [sf for sf in matched if sf.parsed_episode is None]
        assert len(eps) == 12
        assert len(bonus) == 7
        assert len(matched) == 19
        assert len(remaining) == 0

    def test_multi_cour_with_bonus_splits_correctly(self, monkeypatch):
        """24 episodes + 3 bonus for a 12-ep AniDB entry.

        Should take first 12 episodes + all 3 bonus; leave 12 episodes.
        """
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = []
        for i in range(1, 25):
            pool.append(
                anime.SourceFile(
                    path=Path(f"ep{i}.mkv"),
                    parsed_season=1,
                    parsed_episode=i,
                )
            )
        for j in range(3):
            pool.append(
                anime.SourceFile(
                    path=Path(f"[G] テスト(第1期) 映像特典「PV{j + 1}」.mkv"),
                    parsed_season=1,
                    parsed_episode=None,
                )
            )

        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        eps = [sf for sf in matched if sf.parsed_episode is not None]
        bonus = [sf for sf in matched if sf.parsed_episode is None]
        assert len(eps) == 12
        assert len(bonus) == 3
        # Remaining should be ep 13-24 (no bonus)
        remaining_eps = sorted(
            sf.parsed_episode for sf in remaining if sf.parsed_episode is not None
        )
        assert remaining_eps == list(range(13, 25))


class TestMatchFilesToSeason:
    """Tests for AniDB per-season file matching."""

    def test_single_season_auto_matches(self, monkeypatch):
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(path=Path(f"ep{i}.mkv"), parsed_season=1, parsed_episode=i)
            for i in range(1, 13)
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 12
        assert len(remaining) == 0

    def test_multi_season_picks_one(self, monkeypatch):
        inputs = iter(["2"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = []
        for s in [1, 2]:
            for i in range(1, 13):
                pool.append(
                    anime.SourceFile(
                        path=Path(f"s{s}e{i:02d}.mkv"),
                        parsed_season=s,
                        parsed_episode=i,
                    )
                )
        info = anime.AnimeInfo(
            anidb_id=200,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2021,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 12
        assert all(sf.parsed_season == 2 for sf in matched)
        assert len(remaining) == 12

    def test_unseasoned_files_included(self, monkeypatch):
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(path=Path("ep1.mkv"), parsed_season=1, parsed_episode=1),
            anime.SourceFile(path=Path("01.mkv")),  # no season, episode, or series name
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2020,
            episodes=[anime.Episode(1, "regular", "Ep 1", "", "")],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 2  # ep1 + special
        assert len(remaining) == 0

    def test_multi_cour_takes_first_n(self, monkeypatch):
        """24-ep season split into two 12-ep AniDB entries takes first 12."""
        inputs = iter(["3"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(f"s3e{i:02d}.mkv"), parsed_season=3, parsed_episode=i
            )
            for i in range(1, 25)  # S03E01-S03E24
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2024,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 12
        assert len(remaining) == 12
        # First 12 episodes matched, episodes 1-12
        assert matched[0].parsed_episode == 1
        assert matched[-1].parsed_episode == 12

    def test_multi_cour_renumbers_second_half(self, monkeypatch):
        """Second cour (ep 13-24) gets renumbered to 1-12."""
        inputs = iter(["3"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        # Pool only has the leftover second half (ep 13-24)
        pool = [
            anime.SourceFile(
                path=Path(f"s3e{i:02d}.mkv"), parsed_season=3, parsed_episode=i
            )
            for i in range(13, 25)
        ]
        info = anime.AnimeInfo(
            anidb_id=200,
            tvdb_id=None,
            title_ja="テスト Part 2",
            title_en="Test Part 2",
            year=2025,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 12
        assert len(remaining) == 0
        # Episodes renumbered: 13→1, 14→2, ..., 24→12
        assert matched[0].parsed_episode == 1
        assert matched[-1].parsed_episode == 12

    def test_no_renumber_when_eps_fit(self, monkeypatch):
        """Ep 12 of a 12-episode entry stays as 12, not renumbered to 1."""
        inputs = iter(["2"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path("s2e12.mkv"), parsed_season=2, parsed_episode=12
            ),
        ]
        info = anime.AnimeInfo(
            anidb_id=300,
            tvdb_id=None,
            title_ja="テスト Season 2",
            title_en="Test Season 2",
            year=2026,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 1
        assert matched[0].parsed_episode == 12  # NOT renumbered to 1

    def test_multi_cour_leftover_back_in_pool(self, monkeypatch):
        """Leftover files from a multi-cour split go back in the pool."""
        inputs = iter(["3"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(f"s3e{i:02d}.mkv"), parsed_season=3, parsed_episode=i
            )
            for i in range(1, 25)
        ]
        # AniDB entry with only 12 episodes
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="テスト",
            title_en="Test",
            year=2024,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 13)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        # Remaining should have ep 13-24 still with original numbering
        remaining_eps = sorted(sf.parsed_episode or 0 for sf in remaining)
        assert remaining_eps == list(range(13, 25))

    def test_title_filter_matches_english_name(self, monkeypatch):
        """Files with English series name match when AniDB title is Japanese."""
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(f"[SubGroup] Journal with Witch - {i:02d}.mkv"),
                parsed_episode=i,
            )
            for i in range(1, 4)
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="違国日記",
            title_en="Journal with Witch",
            year=2026,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 14)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 3
        assert len(remaining) == 0

    def test_title_filter_matches_japanese_name(self, monkeypatch):
        """Files with Japanese series name still match against title_ja."""
        inputs = iter(["1"])
        monkeypatch.setattr("builtins.input", lambda _: next(inputs))

        pool = [
            anime.SourceFile(
                path=Path(f"[SubGroup] 違国日記 - {i:02d}.mkv"),
                parsed_episode=i,
            )
            for i in range(1, 4)
        ]
        info = anime.AnimeInfo(
            anidb_id=100,
            tvdb_id=None,
            title_ja="違国日記",
            title_en="Journal with Witch",
            year=2026,
            episodes=[
                anime.Episode(i, "regular", f"Ep {i}", "", "") for i in range(1, 14)
            ],
        )
        matched, remaining = anime._match_files_to_season(pool, info)
        assert len(matched) == 3
        assert len(remaining) == 0
