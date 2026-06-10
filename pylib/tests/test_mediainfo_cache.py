"""Tests for the persistent mediainfo analysis cache."""

from __future__ import annotations

import os

import pytest

from etp_lib import mediainfo_cache
from etp_lib.types import AudioTrack, MediaInfo

MEDIA = MediaInfo(
    video_codec="AVC",
    resolution="1080p",
    width=1920,
    height=800,
    bit_depth=8,
    hdr_type="",
    audio_tracks=[AudioTrack("EAC3", "en", "English", False)],
    encoding_lib="x264",
)


@pytest.fixture
def cache_env(tmp_path, monkeypatch):
    """Isolated cache file, counted fake analyzer, reset module state."""
    cache_file = tmp_path / "analysis.msgpack"
    monkeypatch.setattr(mediainfo_cache, "_cache_file", lambda: cache_file)
    monkeypatch.setattr(mediainfo_cache, "_cache", None)
    monkeypatch.setattr(mediainfo_cache, "_dirty", False)

    calls: list[str] = []

    def fake_analyze(path):
        calls.append(str(path))
        return MEDIA

    monkeypatch.setattr(mediainfo_cache.mediainfo, "analyze_file", fake_analyze)
    return cache_file, calls


def _reset(monkeypatch):
    monkeypatch.setattr(mediainfo_cache, "_cache", None)
    monkeypatch.setattr(mediainfo_cache, "_dirty", False)


class TestMediaInfoCache:
    def test_second_call_is_a_hit(self, tmp_path, cache_env):
        _, calls = cache_env
        video = tmp_path / "a.mkv"
        video.write_bytes(b"x" * 64)
        assert mediainfo_cache.analyze_file_cached(video) == MEDIA
        assert mediainfo_cache.analyze_file_cached(video) == MEDIA
        assert len(calls) == 1

    def test_changed_file_reanalyzed(self, tmp_path, cache_env):
        _, calls = cache_env
        video = tmp_path / "a.mkv"
        video.write_bytes(b"x" * 64)
        mediainfo_cache.analyze_file_cached(video)
        video.write_bytes(b"y" * 128)  # size change
        mediainfo_cache.analyze_file_cached(video)
        assert len(calls) == 2

    def test_mtime_change_reanalyzed(self, tmp_path, cache_env):
        _, calls = cache_env
        video = tmp_path / "a.mkv"
        video.write_bytes(b"x" * 64)
        mediainfo_cache.analyze_file_cached(video)
        os.utime(video, (1_000_000_000, 1_000_000_000))
        mediainfo_cache.analyze_file_cached(video)
        assert len(calls) == 2

    def test_persists_across_processes(self, tmp_path, cache_env, monkeypatch):
        cache_file, calls = cache_env
        video = tmp_path / "a.mkv"
        video.write_bytes(b"x" * 64)
        mediainfo_cache.analyze_file_cached(video)
        mediainfo_cache.save_cache()
        assert cache_file.exists()

        _reset(monkeypatch)  # simulate a fresh process
        result = mediainfo_cache.analyze_file_cached(video)
        assert result == MEDIA
        assert len(calls) == 1  # served from disk, no re-analysis

    def test_corrupt_cache_tolerated(self, tmp_path, cache_env):
        cache_file, calls = cache_env
        cache_file.write_bytes(b"not msgpack at all")
        video = tmp_path / "a.mkv"
        video.write_bytes(b"x" * 64)
        assert mediainfo_cache.analyze_file_cached(video) == MEDIA
        assert len(calls) == 1

    def test_save_noop_when_clean(self, cache_env):
        cache_file, _ = cache_env
        mediainfo_cache.save_cache()
        assert not cache_file.exists()
