"""Tests for download_cache — msgpack caching of download directory scans."""

from __future__ import annotations

from pathlib import Path

import pytest

from etp_lib.download_cache import (
    DownloadCache,
    _deserialize,
    _serialize,
    find_stale_dirs,
    load_cache,
    save_cache,
    scan_dir_mtimes,
)
from etp_lib.types import DownloadIndex


class TestSerializeRoundTrip:
    def test_empty_cache(self) -> None:
        cache = DownloadCache()
        raw = _serialize(cache)
        result = _deserialize(raw)
        assert result is not None
        assert result.groups == {}
        assert result.download_index.by_series == {}
        assert result.dir_mtimes == {}

    def test_groups_round_trip(self) -> None:
        cache = DownloadCache(
            groups={"Series A": [Path("/a/ep1.mkv"), Path("/a/ep2.mkv")]},
            dir_mtimes={"/downloads": 1000},
        )
        result = _deserialize(_serialize(cache))
        assert result is not None
        assert list(result.groups.keys()) == ["Series A"]
        assert len(result.groups["Series A"]) == 2
        assert result.groups["Series A"][0] == Path("/a/ep1.mkv")

    def test_download_index_round_trip(self) -> None:
        dl = DownloadIndex(
            by_series={"key": [(1, 5, Path("/dl/ep5.mkv"), 1024)]},
            file_count=1,
        )
        cache = DownloadCache(download_index=dl, dir_mtimes={"/dl": 2000})
        result = _deserialize(_serialize(cache))
        assert result is not None
        assert result.download_index.file_count == 1
        entries = result.download_index.by_series["key"]
        assert len(entries) == 1
        s, e, p, sz = entries[0]
        assert (s, e, sz) == (1, 5, 1024)
        assert p == Path("/dl/ep5.mkv")

    def test_corrupt_data_returns_none(self) -> None:
        assert _deserialize(b"not msgpack") is None

    def test_wrong_version_returns_none(self) -> None:
        import msgpack

        data: bytes = msgpack.packb({"v": 999}, use_bin_type=True)  # type: ignore[assignment]
        assert _deserialize(data) is None


class TestSaveAndLoad:
    def test_round_trip_via_disk(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "etp_lib.download_cache.cache_path", lambda: tmp_path / "test.msgpack"
        )
        cache = DownloadCache(
            groups={"Test": [Path("/test/ep1.mkv")]},
            dir_mtimes={"/test": 12345},
        )
        save_cache(cache)
        loaded = load_cache()
        assert loaded is not None
        assert loaded.groups == cache.groups

    def test_load_missing_returns_none(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "etp_lib.download_cache.cache_path", lambda: tmp_path / "nope.msgpack"
        )
        assert load_cache() is None

    def test_merge_preserves_other_fields(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Saving groups doesn't clobber existing download_index."""
        cache_file = tmp_path / "test.msgpack"
        monkeypatch.setattr("etp_lib.download_cache.cache_path", lambda: cache_file)

        # Save download_index first
        dl = DownloadIndex(
            by_series={"key": [(1, 1, Path("/dl/ep1.mkv"), 100)]}, file_count=1
        )
        save_cache(DownloadCache(download_index=dl, dir_mtimes={"/dl": 1000}))

        # Save groups — should preserve download_index
        save_cache(
            DownloadCache(groups={"S": [Path("/s/ep.mkv")]}, dir_mtimes={"/s": 2000})
        )

        loaded = load_cache()
        assert loaded is not None
        assert loaded.groups == {"S": [Path("/s/ep.mkv")]}
        assert loaded.download_index.file_count == 1

    def test_dest_ids_round_trip(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        cache_file = tmp_path / "test.msgpack"
        monkeypatch.setattr("etp_lib.download_cache.cache_path", lambda: cache_file)

        cache = DownloadCache(
            dest_ids={
                ("anidb", 123): "/anime/series1",
                ("tvdb", 456): "/anime/series2",
            },
            dest_mtimes={"/anime": 9999},
        )
        save_cache(cache)
        loaded = load_cache()
        assert loaded is not None
        assert loaded.dest_ids == {
            ("anidb", 123): "/anime/series1",
            ("tvdb", 456): "/anime/series2",
        }
        assert loaded.dest_mtimes == {"/anime": 9999}


class TestScanDirMtimes:
    def test_scans_root_and_subdirs(self, tmp_path: Path) -> None:
        (tmp_path / "subdir_a").mkdir()
        (tmp_path / "subdir_b").mkdir()
        (tmp_path / "file.txt").touch()

        mtimes = scan_dir_mtimes([tmp_path])
        assert str(tmp_path) in mtimes
        assert str(tmp_path / "subdir_a") in mtimes
        assert str(tmp_path / "subdir_b") in mtimes
        # Files are not included
        assert str(tmp_path / "file.txt") not in mtimes

    def test_nonexistent_dir_skipped(self, tmp_path: Path) -> None:
        mtimes = scan_dir_mtimes([tmp_path / "nope"])
        assert mtimes == {}


class TestFindStaleDirs:
    def test_no_changes(self) -> None:
        mtimes = {"/a": 100, "/b": 200}
        changed, removed = find_stale_dirs(mtimes, mtimes)
        assert changed == set()
        assert removed == set()

    def test_new_dir(self) -> None:
        cached = {"/a": 100}
        current = {"/a": 100, "/b": 200}
        changed, removed = find_stale_dirs(cached, current)
        assert changed == {"/b"}
        assert removed == set()

    def test_changed_mtime(self) -> None:
        cached = {"/a": 100}
        current = {"/a": 200}
        changed, removed = find_stale_dirs(cached, current)
        assert changed == {"/a"}
        assert removed == set()

    def test_removed_dir(self) -> None:
        cached = {"/a": 100, "/b": 200}
        current = {"/a": 100}
        changed, removed = find_stale_dirs(cached, current)
        assert changed == set()
        assert removed == {"/b"}

    def test_mixed_changes(self) -> None:
        cached = {"/a": 100, "/b": 200, "/c": 300}
        current = {"/a": 100, "/b": 999, "/d": 400}
        changed, removed = find_stale_dirs(cached, current)
        assert changed == {"/b", "/d"}
        assert removed == {"/c"}
