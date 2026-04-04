"""Tests for etp_lib.colorize — ANSI color helpers for media filenames."""

from __future__ import annotations

import re

import pytest

from etp_lib.colorize import (
    ColorDepth,
    colorize,
    colorize_path,
    colorize_token_text,
    color_for_field,
    detect_color_depth,
    format_parsed_media,
    set_color_depth,
)
from etp_lib.media_parser import ParsedMedia, TokenKind


@pytest.fixture(autouse=True)
def _reset_color_depth():
    """Ensure each test starts with a known color depth and resets after."""
    set_color_depth(ColorDepth.FULL)
    yield
    # Reset to re-detect on next use
    import etp_lib.colorize as _mod

    _mod._color_depth = None


# ---------------------------------------------------------------------------
# Color depth detection
# ---------------------------------------------------------------------------


def _mock_tty(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make sys.stdout.isatty() return True for detection tests."""
    monkeypatch.setattr("sys.stdout.isatty", lambda: True)


class TestDetectColorDepth:
    def test_no_color_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("NO_COLOR", "1")
        assert detect_color_depth() == ColorDepth.NONE

    def test_non_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.stdout.isatty", lambda: False)
        monkeypatch.setenv("TERM", "xterm-256color")
        monkeypatch.delenv("NO_COLOR", raising=False)
        assert detect_color_depth() == ColorDepth.NONE

    def test_dumb_terminal(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("TERM", "dumb")
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.delenv("COLORTERM", raising=False)
        assert detect_color_depth() == ColorDepth.NONE

    def test_empty_term(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("TERM", "")
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.delenv("COLORTERM", raising=False)
        assert detect_color_depth() == ColorDepth.NONE

    def test_256color_term(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("TERM", "xterm-256color")
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.delenv("COLORTERM", raising=False)
        assert detect_color_depth() == ColorDepth.FULL

    def test_colorterm_truecolor(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("COLORTERM", "truecolor")
        monkeypatch.setenv("TERM", "xterm")
        monkeypatch.delenv("NO_COLOR", raising=False)
        assert detect_color_depth() == ColorDepth.FULL

    def test_colorterm_24bit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("COLORTERM", "24bit")
        monkeypatch.setenv("TERM", "xterm")
        monkeypatch.delenv("NO_COLOR", raising=False)
        assert detect_color_depth() == ColorDepth.FULL

    def test_basic_color_term(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("TERM", "xterm-color")
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.delenv("COLORTERM", raising=False)
        assert detect_color_depth() == ColorDepth.BASIC

    def test_no_color_takes_precedence(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("NO_COLOR", "")
        monkeypatch.setenv("TERM", "xterm-256color")
        monkeypatch.setenv("COLORTERM", "truecolor")
        assert detect_color_depth() == ColorDepth.NONE

    def test_unknown_term_assumed_basic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _mock_tty(monkeypatch)
        monkeypatch.setenv("TERM", "vt100")
        monkeypatch.delenv("NO_COLOR", raising=False)
        monkeypatch.delenv("COLORTERM", raising=False)
        assert detect_color_depth() == ColorDepth.BASIC


# ---------------------------------------------------------------------------
# colorize() — single token coloring
# ---------------------------------------------------------------------------


class TestColorize:
    def test_256_color(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize("hello", TokenKind.EPISODE)
        assert result == "\033[38;5;51mhello\033[0m"

    def test_16_color(self) -> None:
        set_color_depth(ColorDepth.BASIC)
        result = colorize("hello", TokenKind.EPISODE)
        # EPISODE (51) maps to bright cyan (96)
        assert result == "\033[96mhello\033[0m"

    def test_no_color(self) -> None:
        set_color_depth(ColorDepth.NONE)
        result = colorize("hello", TokenKind.EPISODE)
        assert result == "hello"

    def test_unknown_kind_no_crash(self) -> None:
        # TokenKind values not in the map should return plain text
        set_color_depth(ColorDepth.FULL)
        # BRACKET/PAREN/LENTICULAR are not in _TOKEN_COLORS
        result = colorize("test", TokenKind.BRACKET)
        assert result == "test"

    def test_all_token_kinds_have_colors(self) -> None:
        """Every TokenKind in the color map produces output with color codes."""
        set_color_depth(ColorDepth.FULL)
        from etp_lib.colorize import _TOKEN_COLORS

        for kind in _TOKEN_COLORS:
            result = colorize("x", kind)
            assert "\033[" in result, f"{kind} did not produce color"

    def test_16_color_mapping_complete(self) -> None:
        """Every palette color used by tokens has a 16-color mapping."""
        from etp_lib.colorize import _PALETTE, _PALETTE_TO_16, _TOKEN_COLORS

        palette_nums = set(_PALETTE.values())
        for kind, num in _TOKEN_COLORS.items():
            assert num in palette_nums, f"color {num} ({kind}) not in _PALETTE"
        for name in _PALETTE:
            assert name in _PALETTE_TO_16, (
                f"palette color {name!r} has no 16-color mapping"
            )


# ---------------------------------------------------------------------------
# color_for_field()
# ---------------------------------------------------------------------------


class TestColorForField:
    def test_known_field(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = color_for_field("series")
        assert "\033[38;5;228m" == result

    def test_unknown_field(self) -> None:
        result = color_for_field("nonexistent")
        assert result == ""

    def test_no_color_mode(self) -> None:
        set_color_depth(ColorDepth.NONE)
        result = color_for_field("series")
        assert result == ""


# ---------------------------------------------------------------------------
# colorize_token_text() — S01E01 splitting
# ---------------------------------------------------------------------------


class TestColorizeTokenText:
    def test_episode_with_season(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize_token_text("S01E05", TokenKind.EPISODE)
        # Should contain season color (39) for S01 and episode color (51) for E05
        assert "\033[38;5;39m" in result  # season
        assert "\033[38;5;51m" in result  # episode
        assert "S01" in result
        assert "E05" in result

    def test_episode_without_season(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize_token_text("05", TokenKind.EPISODE)
        # No S01E01 pattern, should just colorize as episode
        assert "\033[38;5;51m" in result
        assert "05" in result

    def test_non_episode_kind(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize_token_text("1080p", TokenKind.RESOLUTION)
        assert "\033[38;5;114m" in result
        assert "1080p" in result

    def test_no_color_passthrough(self) -> None:
        set_color_depth(ColorDepth.NONE)
        result = colorize_token_text("S01E05", TokenKind.EPISODE)
        assert result == "S01E05"

    def test_episode_with_version(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize_token_text("S02E10v2", TokenKind.EPISODE)
        assert "S02" in result
        assert "E10v2" in result


# ---------------------------------------------------------------------------
# colorize_path() — full path colorization
# ---------------------------------------------------------------------------


class TestColorizePath:
    def test_simple_filename(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize_path("[SubGroup] Series Name - 01 [1080p].mkv")
        # Should contain color codes
        assert "\033[" in result
        # Original text should be preserved (minus color codes)
        stripped = re.sub(r"\033\[[^m]*m", "", result)
        assert stripped == "[SubGroup] Series Name - 01 [1080p].mkv"

    def test_path_with_slashes(self) -> None:
        set_color_depth(ColorDepth.FULL)
        result = colorize_path("Series Name/Season 1/episode.mkv")
        assert "/" in result  # slashes preserved
        stripped = re.sub(r"\033\[[^m]*m", "", result)
        assert stripped == "Series Name/Season 1/episode.mkv"

    def test_no_color_returns_original(self) -> None:
        set_color_depth(ColorDepth.NONE)
        path = "[SubGroup] Series - 01 [1080p][ABCD1234].mkv"
        result = colorize_path(path)
        assert result == path

    def test_16_color_has_codes(self) -> None:
        set_color_depth(ColorDepth.BASIC)
        result = colorize_path("[SubGroup] Series - 01 [1080p].mkv")
        assert "\033[" in result
        # Should not contain 256-color sequences
        assert "\033[38;5;" not in result

    def test_empty_string(self) -> None:
        result = colorize_path("")
        assert result == ""


# ---------------------------------------------------------------------------
# format_parsed_media()
# ---------------------------------------------------------------------------


class TestFormatParsedMedia:
    @staticmethod
    def _make_pm(**kwargs: object) -> ParsedMedia:  # type: ignore[no-untyped-def]
        from etp_lib.media_parser import parse_media_path

        # Start from a real parse and override specific fields
        pm = parse_media_path("")
        for k, v in kwargs.items():
            object.__setattr__(pm, k, v)
        return pm

    def test_empty_media(self) -> None:
        pm = self._make_pm()
        result = format_parsed_media(pm)
        assert result == ""

    def test_series_and_episode(self) -> None:
        set_color_depth(ColorDepth.FULL)
        pm = self._make_pm(series_name="Test Series", episode=5, extension=".mkv")
        result = format_parsed_media(pm)
        assert "series" in result
        assert "Test Series" in result
        assert "episode" in result
        assert "5" in result
        assert ".mkv" in result

    def test_no_color_output(self) -> None:
        set_color_depth(ColorDepth.NONE)
        pm = self._make_pm(series_name="Test", episode=1)
        result = format_parsed_media(pm)
        assert "\033[" not in result
        assert "Test" in result

    def test_special_fields(self) -> None:
        set_color_depth(ColorDepth.FULL)
        pm = self._make_pm(is_special=True, special_tag="OVA")
        result = format_parsed_media(pm)
        assert "special" in result
        assert "OVA" in result

    def test_batch_range(self) -> None:
        pm = self._make_pm(batch_range=(1, 12))
        result = format_parsed_media(pm)
        assert "1~12" in result

    def test_false_fields_hidden(self) -> None:
        pm = self._make_pm(is_remux=False, is_dual_audio=False)
        result = format_parsed_media(pm)
        assert "remux" not in result
        assert "dual-audio" not in result
