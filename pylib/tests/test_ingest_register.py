"""Tests for the shared ingest register (etp_lib.ingest_register)."""

import json

import pytest

from etp_lib import ingest_register


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """Redirect cache_dir() to a per-test temporary directory."""

    def fake_cache_dir(provider: str):
        d = tmp_path / provider
        d.mkdir(parents=True, exist_ok=True)
        return d

    monkeypatch.setattr(ingest_register, "cache_dir", fake_cache_dir)
    return tmp_path


class TestRegisterRoundtrip:
    def test_empty(self, cache):
        assert ingest_register.load_register() == set()

    def test_roundtrip(self, cache):
        paths = {"/vol/a.mkv", "/vol/b.mkv"}
        ingest_register.save_register(paths)
        assert ingest_register.load_register() == paths

    def test_accumulates(self, cache):
        ingest_register.save_register({"/vol/a.mkv"})
        loaded = ingest_register.load_register()
        loaded.add("/vol/b.mkv")
        ingest_register.save_register(loaded)
        assert ingest_register.load_register() == {"/vol/a.mkv", "/vol/b.mkv"}

    def test_corrupt_register(self, cache):
        ingest_register.register_path().write_text("not json!!!", encoding="utf-8")
        assert ingest_register.load_register() == set()

    def test_saved_as_sorted_json_array(self, cache):
        ingest_register.save_register({"/vol/b.mkv", "/vol/a.mkv"})
        data = json.loads(ingest_register.register_path().read_text(encoding="utf-8"))
        assert data == ["/vol/a.mkv", "/vol/b.mkv"]

    def test_save_leaves_no_temp_file(self, cache):
        ingest_register.save_register({"/vol/a.mkv"})
        leftovers = [
            p
            for p in ingest_register.register_path().parent.iterdir()
            if p.name != "copied.json"
        ]
        assert leftovers == []


class TestLegacyMigration:
    """The pre-sharing anime triage register is merged in on load."""

    def test_legacy_only(self, cache):
        ingest_register.legacy_register_path().write_text(
            json.dumps(["/vol/old.mkv"]), encoding="utf-8"
        )
        assert ingest_register.load_register() == {"/vol/old.mkv"}

    def test_merges_legacy_and_new(self, cache):
        ingest_register.legacy_register_path().write_text(
            json.dumps(["/vol/old.mkv"]), encoding="utf-8"
        )
        ingest_register.save_register({"/vol/new.mkv"})
        assert ingest_register.load_register() == {"/vol/old.mkv", "/vol/new.mkv"}

    def test_save_migrates_without_touching_legacy(self, cache):
        legacy = ingest_register.legacy_register_path()
        legacy.write_text(json.dumps(["/vol/old.mkv"]), encoding="utf-8")

        ingest_register.save_register(ingest_register.load_register())

        assert json.loads(legacy.read_text(encoding="utf-8")) == ["/vol/old.mkv"]
        migrated = json.loads(
            ingest_register.register_path().read_text(encoding="utf-8")
        )
        assert migrated == ["/vol/old.mkv"]

    def test_corrupt_legacy_ignored(self, cache):
        ingest_register.legacy_register_path().write_text("][", encoding="utf-8")
        ingest_register.save_register({"/vol/a.mkv"})
        assert ingest_register.load_register() == {"/vol/a.mkv"}
