"""Tests for etp-catalog config loading, resolution, and CLI."""

import textwrap
from unittest.mock import patch

from etp_commands import catalog


class TestResolveGlobal:
    def test_plain_values_pass_through(self):
        result = catalog.resolve_global({"a": "hello", "b": "world"})
        assert result == {"a": "hello", "b": "world"}

    def test_env_var_expansion(self, monkeypatch):
        monkeypatch.setenv("TEST_VAR", "/some/path")
        result = catalog.resolve_global({"dir": "$TEST_VAR/sub"})
        assert result == {"dir": "/some/path/sub"}

    def test_key_interpolation(self):
        result = catalog.resolve_global(
            {
                "base": "/root",
                "sub": "{base}/child",
            }
        )
        assert result == {"base": "/root", "sub": "/root/child"}

    def test_chained_interpolation(self):
        result = catalog.resolve_global(
            {
                "a": "/root",
                "b": "{a}/mid",
                "c": "{b}/leaf",
            }
        )
        assert result == {
            "a": "/root",
            "b": "/root/mid",
            "c": "/root/mid/leaf",
        }

    def test_unresolvable_key_left_as_is(self):
        result = catalog.resolve_global({"x": "{unknown}/path"})
        assert result == {"x": "{unknown}/path"}

    def test_env_var_and_interpolation_combined(self, monkeypatch):
        monkeypatch.setenv("HOME", "/home/user")
        result = catalog.resolve_global(
            {
                "base": "$HOME/data",
                "sub": "{base}/trees",
            }
        )
        assert result == {
            "base": "/home/user/data",
            "sub": "/home/user/data/trees",
        }


class TestLoadConfig:
    def test_loads_kdl_with_scans(self, tmp_path):
        config = tmp_path / "test.kdl"
        config.write_text(
            textwrap.dedent("""\
            global {
                home-base "/data"
                trees-path "{home_base}/trees"
                csvs-path "{trees_path}/csv"
                db-path "{trees_path}/db"
            }

            scan "mydir" {
                mode "used"
                disk "/tmp/test"
                desc "test directory"
                header "test header"
            }
        """)
        )

        cfg = catalog.load_config(config)
        assert cfg["global"]["home_base"] == "/data"
        assert cfg["global"]["trees_path"] == "/data/trees"
        assert cfg["global"]["db_path"] == "/data/trees/db"
        assert "mydir" in cfg["scans"]
        assert cfg["scans"]["mydir"]["mode"] == "used"

    def test_empty_config(self, tmp_path):
        config = tmp_path / "empty.kdl"
        config.write_text("")

        cfg = catalog.load_config(config)
        assert cfg["global"] == {}
        assert cfg["scans"] == {}

    def test_global_interpolation_applied(self, tmp_path):
        config = tmp_path / "interp.kdl"
        config.write_text(
            textwrap.dedent("""\
            global {
                home-base "/vol"
                trees-path "{home_base}/trees"
            }
        """)
        )

        cfg = catalog.load_config(config)
        assert cfg["global"]["trees_path"] == "/vol/trees"

    def test_slashdash_disables_scan(self, tmp_path):
        config = tmp_path / "slash.kdl"
        config.write_text(
            textwrap.dedent("""\
            scan "active" {
                mode "used"
                disk "/tmp/test"
                desc "active scan"
                header "active"
            }

            /- scan "disabled" {
                mode "used"
                disk "/tmp/old"
                desc "disabled scan"
                header "disabled"
            }
        """)
        )

        cfg = catalog.load_config(config)
        assert len(cfg["scans"]) == 1
        assert "active" in cfg["scans"]


class TestTimer:
    def test_str_format(self):
        with catalog.Timer() as t:
            pass
        s = str(t)
        assert s.startswith("real ")
        assert "user " in s
        assert "sys " in s

    def test_elapsed_is_positive(self):
        import time

        with catalog.Timer() as t:
            time.sleep(0.01)
        assert t.elapsed >= 0.01


class TestCLIDryRun:
    def test_dry_run_prints_plan(self, tmp_path, capsys):
        config = tmp_path / "test.kdl"
        config.write_text(
            textwrap.dedent("""\
            global {
                trees-path "/tmp/trees"
                csvs-path "/tmp/csv"
                db-path "/tmp/db"
            }

            scan "mytest" {
                mode "df"
                disk "/tmp/testdisk"
                desc "test scan"
                header "test header"
            }
        """)
        )

        rc = catalog.main(["--dry-run", str(config)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "Dry run" in out
        assert "[mytest]" in out
        assert "mode=df" in out

    def test_unknown_scan_name_errors(self, tmp_path, capsys):
        config = tmp_path / "test.kdl"
        config.write_text(
            textwrap.dedent("""\
            global {
                trees-path "/tmp/trees"
                csvs-path "/tmp/csv"
                db-path "/tmp/db"
            }

            scan "real" {
                mode "used"
                disk "/tmp"
                desc "test"
                header "test"
            }
        """)
        )

        rc = catalog.main(["--scan", "bogus", str(config)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "bogus" in err

    def test_no_scans(self, tmp_path, capsys):
        config = tmp_path / "test.kdl"
        config.write_text(
            textwrap.dedent("""\
            global {
                trees-path "/tmp/trees"
                csvs-path "/tmp/csvs"
                db-path "/tmp/db"
            }
        """)
        )

        rc = catalog.main([str(config)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "No scans to run" in out

    def test_missing_global_fields_errors(self, tmp_path, capsys):
        config = tmp_path / "test.kdl"
        config.write_text(
            textwrap.dedent("""\
            global {
                trees-path "/tmp/trees"
            }
        """)
        )

        rc = catalog.main([str(config)])
        assert rc == 1
        err = capsys.readouterr().err
        assert "missing required field(s)" in err
        assert "csvs_path" in err
        assert "db_path" in err

    def test_missing_config_errors(self, capsys):
        rc = catalog.main(["/nonexistent/config.kdl"])
        assert rc == 1
        err = capsys.readouterr().err
        assert "not found" in err


def _make_scan_cfg(mode, disk="/vol/data"):
    return {
        "mode": mode,
        "disk": disk,
        "desc": "test-scan",
        "header": "Test Header",
    }


def _make_global_cfg(tmp_path):
    trees = tmp_path / "trees"
    db = tmp_path / "db"
    trees.mkdir()
    db.mkdir()
    return {
        "trees_path": str(trees),
        "db_path": str(db),
    }


def _fake_run_cmd(responses):
    """Return a mock for run_cmd that returns responses in order."""
    calls = []
    it = iter(responses)

    def mock(args, *, capture=False, verbose=False, env_extra=None):
        calls.append(list(args))
        return next(it) if capture else None

    return mock, calls


class TestGenerateTree:
    def test_mode_used(self, tmp_path):
        global_cfg = _make_global_cfg(tmp_path)
        scan_cfg = _make_scan_cfg("used")
        # etp-tree with --du returns tree output
        mock, calls = _fake_run_cmd(["tree output\nSize: 42.00 MiB (root)\n"])

        with (
            patch.object(catalog, "run_cmd", mock),
            patch.object(catalog, "require_binary", return_value="/usr/bin/etp-tree"),
        ):
            catalog.generate_tree("mytest", scan_cfg, global_cfg)

        tree_file = tmp_path / "trees" / "test-scan.tree"
        content = tree_file.read_text()
        assert content.startswith("Test Header\n\n")
        assert "tree output" in content

        # etp-tree with --du
        assert len(calls) == 1
        assert calls[0][0] == "/usr/bin/etp-tree"
        assert "--du" in calls[0]
        assert "--du-subs" not in calls[0]

    def test_mode_df(self, tmp_path):
        global_cfg = _make_global_cfg(tmp_path)
        scan_cfg = _make_scan_cfg("df")
        # etp-tree output, then df -PH output
        mock, calls = _fake_run_cmd(["tree output\n", "Filesystem Size Used\n"])

        with (
            patch.object(catalog, "run_cmd", mock),
            patch.object(catalog, "require_binary", return_value="/usr/bin/etp-tree"),
        ):
            catalog.generate_tree("mytest", scan_cfg, global_cfg)

        tree_file = tmp_path / "trees" / "test-scan.tree"
        content = tree_file.read_text()
        assert "tree output" in content
        assert "Filesystem Size Used" in content

        # etp-tree (no --du for df mode) + df -PH
        assert len(calls) == 2
        assert "--du" not in calls[0]
        assert calls[1] == ["df", "-PH", "/vol/data"]

    def test_mode_subs(self, tmp_path):
        global_cfg = _make_global_cfg(tmp_path)
        scan_cfg = _make_scan_cfg("subs")
        # etp-tree with --du --du-subs, then df -PH
        mock, calls = _fake_run_cmd(
            [
                "tree output\nSize: 100.00 MiB (root)\n  50.00 MiB  alpha\n  50.00 MiB  beta\n",
                "Filesystem Size Used\n",
            ]
        )

        with (
            patch.object(catalog, "run_cmd", mock),
            patch.object(catalog, "require_binary", return_value="/usr/bin/etp-tree"),
        ):
            catalog.generate_tree("mytest", scan_cfg, global_cfg)

        tree_file = tmp_path / "trees" / "test-scan.tree"
        content = tree_file.read_text()
        assert "tree output" in content
        assert "Filesystem Size Used" in content

        # etp-tree with --du --du-subs + df -PH
        assert len(calls) == 2
        assert "--du" in calls[0]
        assert "--du-subs" in calls[0]
        assert calls[1] == ["df", "-PH", "/vol/data"]
