"""Tests for KEY=VALUE env file loading (etp_lib.envfile)."""

import os

from etp_lib.envfile import load_env_file


class TestLoadEnvFile:
    def test_sets_missing_variable(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ETP_TEST_KEY", raising=False)
        env = tmp_path / "a.env"
        env.write_text("ETP_TEST_KEY=hello\n", encoding="utf-8")
        load_env_file(env)
        assert os.environ["ETP_TEST_KEY"] == "hello"
        monkeypatch.delenv("ETP_TEST_KEY")

    def test_does_not_overwrite_process_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("ETP_TEST_KEY", "from-process")
        env = tmp_path / "a.env"
        env.write_text("ETP_TEST_KEY=from-file\n", encoding="utf-8")
        load_env_file(env)
        assert os.environ["ETP_TEST_KEY"] == "from-process"

    def test_comments_and_blank_lines_ignored(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ETP_TEST_KEY", raising=False)
        env = tmp_path / "a.env"
        env.write_text(
            "# comment\n\nETP_TEST_KEY = spaced \nnot a pair\n", encoding="utf-8"
        )
        load_env_file(env)
        assert os.environ["ETP_TEST_KEY"] == "spaced"
        monkeypatch.delenv("ETP_TEST_KEY")

    def test_earlier_file_wins(self, tmp_path, monkeypatch):
        monkeypatch.delenv("ETP_TEST_KEY", raising=False)
        first = tmp_path / "first.env"
        second = tmp_path / "second.env"
        first.write_text("ETP_TEST_KEY=first\n", encoding="utf-8")
        second.write_text("ETP_TEST_KEY=second\nETP_TEST_OTHER=set\n", encoding="utf-8")
        monkeypatch.delenv("ETP_TEST_OTHER", raising=False)
        load_env_file(first, second)
        assert os.environ["ETP_TEST_KEY"] == "first"
        assert os.environ["ETP_TEST_OTHER"] == "set"
        monkeypatch.delenv("ETP_TEST_KEY")
        monkeypatch.delenv("ETP_TEST_OTHER")

    def test_missing_file_ignored(self, tmp_path):
        load_env_file(tmp_path / "nope.env")
