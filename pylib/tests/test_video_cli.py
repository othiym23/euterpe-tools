"""CLI smoke tests for etp-movies / etp-television (etp_commands.video_cli)."""

from __future__ import annotations

from pathlib import Path

import pytest

from etp_commands import movies, television, video_cli
from etp_commands.dispatcher import BUILTIN_COMMANDS
from etp_lib.video_ingest import MediaKind


class TestDispatcherRegistration:
    def test_commands_registered(self):
        assert "movies" in BUILTIN_COMMANDS
        assert "television" in BUILTIN_COMMANDS


class TestBuildParser:
    @pytest.mark.parametrize(
        ("kind", "mode_flag"),
        [(MediaKind.MOVIE, "--radarr"), (MediaKind.TV, "--sonarr")],
    )
    def test_plan_args(self, kind, mode_flag):
        parser = video_cli.build_parser(kind)
        args = parser.parse_args(
            [
                "ingest",
                "plan",
                mode_flag,
                "--force",
                "--json",
                "-o",
                "out.kdl",
                "--refine",
                "prev.kdl",
                "--no-cache",
                "-v",
                "severance",
            ]
        )
        assert args.command == "ingest"
        assert args.action == "plan"
        assert args.managed is True
        assert args.force is True
        assert args.json_output is True
        assert args.output == Path("out.kdl")
        assert args.refine == Path("prev.kdl")
        assert args.no_cache is True
        assert args.pattern == "severance"

    @pytest.mark.parametrize("kind", [MediaKind.MOVIE, MediaKind.TV])
    def test_apply_args(self, kind):
        parser = video_cli.build_parser(kind)
        args = parser.parse_args(
            ["ingest", "apply", "plan.kdl", "--dry-run", "--json", "--sub-lang", "ja"]
        )
        assert args.action == "apply"
        assert args.manifest == Path("plan.kdl")
        assert args.dry_run is True
        assert args.json_output is True
        assert args.sub_lang == "ja"

    def test_movie_parser_rejects_sonarr(self):
        parser = video_cli.build_parser(MediaKind.MOVIE)
        with pytest.raises(SystemExit):
            parser.parse_args(["ingest", "plan", "--sonarr"])

    def test_tv_parser_rejects_radarr(self):
        parser = video_cli.build_parser(MediaKind.TV)
        with pytest.raises(SystemExit):
            parser.parse_args(["ingest", "plan", "--radarr"])


class TestMainDispatch:
    def test_no_command_prints_help(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["etp-movies"])
        assert movies.main() == 0
        assert "ingest" in capsys.readouterr().out

    def test_ingest_without_action_prints_help(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["etp-television", "ingest"])
        assert television.main() == 0
        assert "plan" in capsys.readouterr().out

    def test_plan_requires_mode_flag(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["etp-movies", "ingest", "plan"])
        assert movies.main() == 1
        assert "--radarr" in capsys.readouterr().err

    def test_plan_requires_api_keys(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["etp-movies", "ingest", "plan", "--radarr"])
        monkeypatch.delenv("TMDB_API_KEY", raising=False)
        monkeypatch.delenv("TVDB_API_KEY", raising=False)
        # Keep the credential files out of the picture.
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        assert movies.main() == 1
        assert "TMDB_API_KEY" in capsys.readouterr().err

    def test_plan_dispatches_to_run_plan(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            "sys.argv",
            ["etp-television", "ingest", "plan", "--sonarr", "--json", "pat"],
        )
        monkeypatch.setenv("TMDB_API_KEY", "tk")
        monkeypatch.setenv("TVDB_API_KEY", "vk")
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        monkeypatch.setattr(video_cli, "load_media_config", lambda path=None: "CONFIG")
        seen = {}

        def fake_run_plan(kind, config, opts, providers):
            seen.update(kind=kind, config=config, opts=opts, providers=providers)
            return 0

        monkeypatch.setattr(video_cli, "run_plan", fake_run_plan)
        assert television.main() == 0
        assert seen["kind"] is MediaKind.TV
        assert seen["config"] == "CONFIG"
        assert seen["opts"].pattern == "pat"
        assert seen["opts"].json_output is True
        assert seen["providers"].tmdb_key == "tk"
        assert seen["providers"].tvdb_key == "vk"

    @pytest.mark.parametrize(
        ("entry", "kind", "own_key", "cross_key"),
        [("tv", MediaKind.TV, "sk", "rk"), ("movies", MediaKind.MOVIE, "rk", "sk")],
    )
    def test_arr_keys_wired_per_kind(
        self, monkeypatch, entry, kind, own_key, cross_key
    ):
        """A swap of the Radarr/Sonarr keys silently degrades domain
        filtering at runtime (both fetches 401 into warnings), so the
        env-var-to-field wiring must be pinned per kind."""
        flag = "--sonarr" if kind is MediaKind.TV else "--radarr"
        monkeypatch.setattr("sys.argv", [f"etp-{entry}", "ingest", "plan", flag])
        monkeypatch.setenv("TMDB_API_KEY", "tk")
        monkeypatch.setenv("TVDB_API_KEY", "vk")
        monkeypatch.setenv("RADARR_API_KEY", "rk")
        monkeypatch.setenv("SONARR_API_KEY", "sk")
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        monkeypatch.setattr(video_cli, "load_media_config", lambda path=None: "CONFIG")
        seen = {}

        def fake_run_plan(kind, config, opts, providers):
            seen["providers"] = providers
            return 0

        monkeypatch.setattr(video_cli, "run_plan", fake_run_plan)
        main = television.main if kind is MediaKind.TV else movies.main
        assert main() == 0
        assert seen["providers"].arr_key == own_key
        assert seen["providers"].cross_arr_key == cross_key

    def test_apply_dispatches_to_run_apply(self, monkeypatch, tmp_path):
        manifest = tmp_path / "plan.kdl"
        monkeypatch.setattr(
            "sys.argv", ["etp-movies", "ingest", "apply", str(manifest), "--dry-run"]
        )
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        seen = {}

        def fake_run_apply(kind, path, opts):
            seen.update(kind=kind, path=path, opts=opts)
            return 2

        monkeypatch.setattr(video_cli, "run_apply", fake_run_apply)
        assert movies.main() == 2
        assert seen["kind"] is MediaKind.MOVIE
        assert seen["path"] == manifest
        assert seen["opts"].dry_run is True


class TestPrimaryKeyOnly:
    """Only the primary provider's key is mandatory for plan."""

    def test_tv_plan_runs_with_only_tvdb_key(self, monkeypatch, capsys):
        monkeypatch.setattr(
            "sys.argv", ["etp-television", "ingest", "plan", "--sonarr"]
        )
        monkeypatch.setenv("TVDB_API_KEY", "vk")
        monkeypatch.delenv("TMDB_API_KEY", raising=False)
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        monkeypatch.setattr(video_cli, "load_media_config", lambda path=None: "CONFIG")
        monkeypatch.setattr(video_cli, "run_plan", lambda *a: 0)
        assert television.main() == 0
        err = capsys.readouterr().err
        assert "TMDB_API_KEY" in err and "cross-checks" in err

    def test_movie_plan_requires_tmdb_key(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["etp-movies", "ingest", "plan", "--radarr"])
        monkeypatch.setenv("TVDB_API_KEY", "vk")
        monkeypatch.delenv("TMDB_API_KEY", raising=False)
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        assert movies.main() == 1
        assert "TMDB_API_KEY" in capsys.readouterr().err

    def test_config_error_reported_cleanly(self, monkeypatch, capsys, tmp_path):
        bad = tmp_path / "bad.kdl"
        bad.write_text('movie "X" {\n  tmdb "abc"\n}\n', encoding="utf-8")
        monkeypatch.setattr(
            "sys.argv",
            ["etp-movies", "ingest", "plan", "--radarr", "--config", str(bad)],
        )
        monkeypatch.setenv("TMDB_API_KEY", "tk")
        monkeypatch.setenv("TVDB_API_KEY", "vk")
        monkeypatch.setattr(video_cli, "load_env_file", lambda *paths: None)
        assert movies.main() == 1
        err = capsys.readouterr().err
        assert err.startswith("error:") and "must be an integer" in err
