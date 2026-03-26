"""Tests for AniDB XML parsing."""

from __future__ import annotations

import pytest

from etp_lib.anidb import _parse_anidb_xml

# ---------------------------------------------------------------------------
# Fixtures: AniDB XML
# ---------------------------------------------------------------------------

ANIDB_XML_SAMPLE = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="28" restricted="false">
  <type>Movie</type>
  <episodecount>1</episodecount>
  <startdate>1988-07-16</startdate>
  <enddate>1988-07-16</enddate>
  <titles>
    <title xml:lang="ja" type="official">アキラ</title>
    <title xml:lang="en" type="official">Akira</title>
    <title xml:lang="x-jat" type="main">Akira</title>
  </titles>
  <episodes>
    <episode id="1" update="2023-01-15">
      <epno type="1">1</epno>
      <length>124</length>
      <rating votes="5">8.50</rating>
      <title xml:lang="en">Akira</title>
      <title xml:lang="ja">アキラ</title>
    </episode>
    <episode id="2" update="2023-01-15">
      <epno type="3">C1</epno>
      <length>2</length>
      <title xml:lang="en">Opening</title>
    </episode>
    <episode id="3" update="2023-01-15">
      <epno type="3">C2</epno>
      <length>2</length>
      <title xml:lang="en">Ending</title>
    </episode>
    <episode id="4" update="2023-01-15">
      <epno type="2">S1</epno>
      <length>30</length>
      <title xml:lang="en">Making of Akira</title>
      <title xml:lang="ja">アキラのメイキング</title>
    </episode>
    <episode id="5" update="2023-01-15">
      <epno type="4">T1</epno>
      <length>1</length>
      <title xml:lang="en">Trailer</title>
    </episode>
  </episodes>
</anime>
"""

ANIDB_XML_SERIES = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="1234" restricted="false">
  <type>TV Series</type>
  <episodecount>12</episodecount>
  <startdate>2020-01-01</startdate>
  <titles>
    <title xml:lang="ja" type="official">テストアニメ</title>
    <title xml:lang="en" type="official">Test Anime</title>
    <title xml:lang="x-jat" type="main">Test Anime</title>
  </titles>
  <episodes>
    <episode id="100"><epno type="1">1</epno>
      <title xml:lang="en">The Beginning</title>
      <title xml:lang="ja">始まり</title>
    </episode>
    <episode id="101"><epno type="1">2</epno>
      <title xml:lang="en">The Journey</title>
    </episode>
    <episode id="102"><epno type="1">3</epno>
      <title xml:lang="en">The End</title>
    </episode>
  </episodes>
</anime>
"""

ANIDB_XML_ERROR = "<error>Anime not found</error>"


class TestAnidbParsing:
    """Tests for AniDB XML parsing."""

    def test_parse_movie(self):
        info = _parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        assert info.anidb_id == 28
        assert info.title_ja == "アキラ"
        assert info.title_en == "Akira"
        assert info.year == 1988

    def test_episode_types(self):
        info = _parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        types = {e.ep_type for e in info.episodes}
        assert "regular" in types
        assert "credit" in types
        assert "special" in types
        assert "trailer" in types

    def test_episode_count(self):
        info = _parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        assert len(info.episodes) == 5

    def test_special_tags(self):
        info = _parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        specials = [e for e in info.episodes if e.ep_type != "regular"]
        tags = {e.special_tag for e in specials}
        assert "C1" in tags
        assert "C2" in tags
        assert "S1" in tags
        assert "T1" in tags

    def test_episode_titles(self):
        info = _parse_anidb_xml(ANIDB_XML_SAMPLE, 28)
        regular = [e for e in info.episodes if e.ep_type == "regular"]
        assert regular[0].title_en == "Akira"
        assert regular[0].title_ja == "アキラ"

    def test_backtick_replaced_in_en_episode_title(self):
        """Backticks in English episode titles are replaced with apostrophes."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="99" restricted="false">
  <type>TV Series</type>
  <episodecount>1</episodecount>
  <startdate>2020-01-01</startdate>
  <titles>
    <title xml:lang="ja" type="official">テスト</title>
    <title xml:lang="en" type="official">Test</title>
  </titles>
  <episodes>
    <episode id="1"><epno type="1">1</epno>
      <title xml:lang="en">The King`s Gambit</title>
      <title xml:lang="ja">王の`策略</title>
    </episode>
  </episodes>
</anime>
"""
        info = _parse_anidb_xml(xml, 99)
        assert info.episodes[0].title_en == "The King's Gambit"
        assert info.episodes[0].title_ja == "王の`策略"

    def test_series_with_multiple_episodes(self):
        info = _parse_anidb_xml(ANIDB_XML_SERIES, 1234)
        regular = [e for e in info.episodes if e.ep_type == "regular"]
        assert len(regular) == 3
        assert regular[0].title_en == "The Beginning"
        assert regular[1].title_en == "The Journey"
        assert regular[2].title_en == "The End"

    def test_ja_official_preferred_over_romaji_main(self):
        """ja official title is preferred even when x-jat main appears first in XML."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="6107" restricted="false">
  <type>TV Series</type>
  <episodecount>64</episodecount>
  <startdate>2009-04-05</startdate>
  <titles>
    <title xml:lang="x-jat" type="main">Hagane no Renkinjutsushi (2009)</title>
    <title xml:lang="en" type="official">Fullmetal Alchemist: Brotherhood</title>
    <title xml:lang="ja" type="official">鋼の錬金術師 (2009)</title>
  </titles>
  <episodes/>
</anime>
"""
        info = _parse_anidb_xml(xml, 6107)
        assert info.title_ja == "鋼の錬金術師 (2009)"
        assert info.title_en == "Fullmetal Alchemist: Brotherhood"

    def test_romaji_fallback_when_no_ja(self):
        """Falls back to x-jat main when no ja title exists."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="99" restricted="false">
  <type>TV Series</type>
  <episodecount>12</episodecount>
  <startdate>2020-01-01</startdate>
  <titles>
    <title xml:lang="x-jat" type="main">Romaji Title</title>
    <title xml:lang="en" type="official">English Title</title>
  </titles>
  <episodes/>
</anime>
"""
        info = _parse_anidb_xml(xml, 99)
        assert info.title_ja == "Romaji Title"
        assert info.title_en == "English Title"

    def test_en_synonym_fallback(self):
        """Falls back to en synonym when no en official or main exists."""
        xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<anime id="7627" restricted="false">
  <type>TV Series</type>
  <episodecount>12</episodecount>
  <startdate>2010-10-07</startdate>
  <titles>
    <title xml:lang="x-jat" type="main">Tantei Opera Milky Holmes</title>
    <title xml:lang="en" type="synonym">Detective Opera Milky Holmes</title>
    <title xml:lang="ja" type="official">\u63a2\u5075\u30aa\u30da\u30e9 \u30df\u30eb\u30ad\u30a3\u30db\u30fc\u30e0\u30ba</title>
  </titles>
  <episodes/>
</anime>
"""
        info = _parse_anidb_xml(xml, 7627)
        assert info.title_en == "Detective Opera Milky Holmes"
        assert info.title_ja == "探偵オペラ ミルキィホームズ"

    def test_error_response(self):
        with pytest.raises(ValueError, match="Anime not found"):
            _parse_anidb_xml(ANIDB_XML_ERROR, 99999)
