"""Tests for paths module — platform-aware directory resolution."""

import sys
from pathlib import Path

from etp_lib import paths


class TestConfigDir:
    def test_macos_uses_application_support(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        result = paths.config_dir()
        assert "Library" in result.parts
        assert "Application Support" in result.parts
        assert paths.BUNDLE_ID in result.parts

    def test_linux_uses_xdg_default(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
        result = paths.config_dir()
        assert ".config" in result.parts
        assert paths.APP_NAME in result.parts

    def test_linux_respects_xdg_config_home(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("XDG_CONFIG_HOME", "/custom/config")
        result = paths.config_dir()
        assert result == Path("/custom/config") / paths.APP_NAME


class TestDataDir:
    def test_macos_uses_application_support(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        result = paths.data_dir()
        assert "Library" in result.parts
        assert "Application Support" in result.parts
        assert paths.BUNDLE_ID in result.parts

    def test_linux_uses_xdg_default(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.delenv("XDG_DATA_HOME", raising=False)
        result = paths.data_dir()
        assert ".local" in result.parts
        assert "share" in result.parts
        assert paths.APP_NAME in result.parts

    def test_linux_respects_xdg_data_home(self, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.setenv("XDG_DATA_HOME", "/custom/data")
        result = paths.data_dir()
        assert result == Path("/custom/data") / paths.APP_NAME


class TestCacheDir:
    def test_macos_uses_library_caches(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sys, "platform", "darwin")
        monkeypatch.setenv("HOME", str(tmp_path))
        result = paths.cache_dir("anidb")
        assert result == tmp_path / "Library" / "Caches" / paths.BUNDLE_ID / "anidb"

    def test_linux_uses_xdg_default(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sys, "platform", "linux")
        monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
        # Point HOME to tmp_path so mkdir doesn't touch the real homedir
        monkeypatch.setenv("HOME", str(tmp_path))
        result = paths.cache_dir("tvdb")
        assert ".cache" in result.parts
        assert "etp" in result.parts
        assert result.parts[-1] == "tvdb"

    def test_linux_respects_xdg_cache_home(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sys, "platform", "linux")
        custom = tmp_path / "custom-cache"
        monkeypatch.setenv("XDG_CACHE_HOME", str(custom))
        result = paths.cache_dir("anidb")
        assert result == custom / "etp" / "anidb"

    def test_creates_directory(self, monkeypatch, tmp_path):
        monkeypatch.setattr(sys, "platform", "linux")
        custom = tmp_path / "fresh-cache"
        monkeypatch.setenv("XDG_CACHE_HOME", str(custom))
        result = paths.cache_dir("anidb")
        assert result.is_dir()


class TestCatalogConfig:
    def test_filename_is_catalog_kdl(self):
        result = paths.catalog_config()
        assert result.name == "catalog.kdl"
        assert result.parent == paths.config_dir()


class TestDbPath:
    def test_filename_is_metadata_sqlite(self):
        result = paths.db_path()
        assert result.name == "metadata.sqlite"
        assert result.parent == paths.data_dir()
