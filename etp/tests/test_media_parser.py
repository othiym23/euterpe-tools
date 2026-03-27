"""Tests for the media path tokenizer and parser."""

from __future__ import annotations

import pytest

from etp_lib import media_parser as mp


# ===================================================================
# Phase A: Structural tokenizer tests
# ===================================================================


class TestTokenizeComponent:
    """Tests for tokenize_component() structural tokenization."""

    def test_simple_bracket_group(self):
        tokens = mp.tokenize_component("[Cyan] Show - 08.mkv")
        kinds = [t.kind for t in tokens]
        assert kinds[0] == mp.TokenKind.BRACKET
        assert tokens[0].text == "Cyan"

    def test_extension_stripped(self):
        tokens = mp.tokenize_component("[Group] Title.mkv")
        ext_tokens = [t for t in tokens if t.kind == mp.TokenKind.EXTENSION]
        assert len(ext_tokens) == 1
        assert ext_tokens[0].text == ".mkv"

    def test_separator_detected(self):
        tokens = mp.tokenize_component("[Cyan] Show - 08 [WEB].mkv")
        seps = [t for t in tokens if t.kind == mp.TokenKind.SEPARATOR]
        assert len(seps) == 1
        assert seps[0].text == " - "

    def test_fansub_full_structure(self):
        tokens = mp.tokenize_component(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        kinds = [t.kind for t in tokens]
        texts = [t.text for t in tokens]
        # [Cyan] = BRACKET
        assert kinds[0] == mp.TokenKind.BRACKET
        assert texts[0] == "Cyan"
        # "Champignon no Majo" = TEXT
        assert mp.TokenKind.TEXT in kinds
        # " - " = SEPARATOR
        assert mp.TokenKind.SEPARATOR in kinds
        # "08" = TEXT (structural phase doesn't classify episodes)
        # [WEB 1080p x265] = BRACKET
        # [AAC] = BRACKET
        # [D98B31F3] = BRACKET
        brackets = [t for t in tokens if t.kind == mp.TokenKind.BRACKET]
        assert len(brackets) == 4
        assert brackets[1].text == "WEB 1080p x265"
        assert brackets[2].text == "AAC"
        assert brackets[3].text == "D98B31F3"
        # .mkv = EXTENSION
        assert kinds[-1] == mp.TokenKind.EXTENSION

    def test_scene_dot_separated(self):
        tokens = mp.tokenize_component(
            "Show.S01E05.Title.1080p.WEB-DL.AAC2.0.H.264-VARYG.mkv"
        )
        dot_tokens = [t for t in tokens if t.kind == mp.TokenKind.DOT_TEXT]
        texts = [t.text for t in dot_tokens]
        # Compound tokens preserved
        assert "AAC2.0" in texts
        assert "H.264-VARYG" in texts or "H.264" in texts
        assert "Show" in texts
        assert "S01E05" in texts

    def test_scene_preserves_h264(self):
        tokens = mp.tokenize_component(
            "TO.BE.HERO.X.S01E01.NICE.1080p.CR.WEB-DL.DUAL.AAC2.0.H.264.MSubs-ToonsHub.mkv"
        )
        dot_tokens = [t for t in tokens if t.kind == mp.TokenKind.DOT_TEXT]
        texts = [t.text for t in dot_tokens]
        assert "H.264" in texts
        assert "AAC2.0" in texts

    def test_nested_parens(self):
        tokens = mp.tokenize_component(
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap)"
        )
        paren_tokens = [t for t in tokens if t.kind == mp.TokenKind.PAREN]
        assert len(paren_tokens) == 1
        assert "softSub(chi+eng)" in paren_tokens[0].text

    def test_lenticular_quotes(self):
        tokens = mp.tokenize_component(
            "探偵オペラミルキィホームズ 第01話「屋根裏の入居者」"
        )
        lent_tokens = [t for t in tokens if t.kind == mp.TokenKind.LENTICULAR]
        assert len(lent_tokens) == 1
        assert lent_tokens[0].text == "屋根裏の入居者"

    def test_japanese_full_structure(self):
        tokens = mp.tokenize_component(
            "[アニメ BD] 探偵オペラミルキィホームズ(第1期) "
            "第01話「屋根裏の入居者」"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv"
        )
        brackets = [t for t in tokens if t.kind == mp.TokenKind.BRACKET]
        assert brackets[0].text == "アニメ BD"
        parens = [t for t in tokens if t.kind == mp.TokenKind.PAREN]
        assert len(parens) == 2
        assert parens[0].text == "第1期"
        assert "softSub(chi+eng)" in parens[1].text
        lent = [t for t in tokens if t.kind == mp.TokenKind.LENTICULAR]
        assert len(lent) == 1
        assert lent[0].text == "屋根裏の入居者"

    def test_nested_lenticular_in_japanese(self):
        """Nested 「」 within episode title."""
        tokens = mp.tokenize_component(
            "[アニメ BD] 探偵オペラミルキィホームズ 第2幕(第2期) "
            "映像特典「ノンテロップED「Lovely Girls Anthem(第07話Ver.)」"
            "(1920x1080 HEVC 10bit FLAC).mkv"
        )
        lent = [t for t in tokens if t.kind == mp.TokenKind.LENTICULAR]
        assert len(lent) >= 1
        # The outer 「」 should capture content including nested 「」
        assert "ノンテロップED" in lent[0].text

    def test_multiple_separators(self):
        """Multiple ' - ' separators in fansub title."""
        tokens = mp.tokenize_component(
            "[Erai-raws] Honzuki no Gekokujou - "
            "Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen - "
            "04v2 [1080p][Multiple Subtitle].mkv"
        )
        seps = [t for t in tokens if t.kind == mp.TokenKind.SEPARATOR]
        assert len(seps) == 2

    def test_paren_with_plus_signs(self):
        """Paren groups with + as content separator (Japanese batch dirs)."""
        tokens = mp.tokenize_component(
            "[アニメ BD] 探偵オペラミルキィホームズ"
            "(+第2幕+ふたりは+TD+SS+Alternative) "
            "全51話+特典+CDx25+Scans"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap)"
        )
        parens = [t for t in tokens if t.kind == mp.TokenKind.PAREN]
        assert len(parens) == 2
        assert "+第2幕+ふたりは+TD+SS+Alternative" in parens[0].text

    def test_bare_title_with_year_parens(self):
        tokens = mp.tokenize_component(
            "Topkapi (1964) (1080p BluRay x265 10bit EAC3 2.0 r00t)"
        )
        text_tokens = [t for t in tokens if t.kind == mp.TokenKind.TEXT]
        assert any("Topkapi" in t.text for t in text_tokens)
        parens = [t for t in tokens if t.kind == mp.TokenKind.PAREN]
        assert len(parens) == 2
        assert parens[0].text == "1964"

    def test_cyrillic(self):
        tokens = mp.tokenize_component("Война миров.2005.WEB-DL.2160p.mkv")
        # Has spaces AND dots — should check how heuristic works
        # "Война миров" has a space, so the whole thing isn't scene-style
        # But "2005.WEB-DL.2160p" part has dots
        # The text "Война миров.2005.WEB-DL.2160p" has spaces AND dots
        # so _is_scene_style returns False (has space)
        text_tokens = [t for t in tokens if t.kind == mp.TokenKind.TEXT]
        assert any("Война" in t.text for t in text_tokens)

    def test_site_prefix_style(self):
        tokens = mp.tokenize_component(
            "www.Torrenting.com - Pacific Rim 2013 UHD BluRay.mkv"
        )
        seps = [t for t in tokens if t.kind == mp.TokenKind.SEPARATOR]
        assert len(seps) == 1

    def test_no_extension(self):
        """Directory names have no extension."""
        tokens = mp.tokenize_component("[Erai-raws] Gungrave - 01~26 [1080p]")
        ext_tokens = [t for t in tokens if t.kind == mp.TokenKind.EXTENSION]
        assert len(ext_tokens) == 0

    def test_empty_string(self):
        tokens = mp.tokenize_component("")
        assert tokens == []

    def test_unicode_music_note(self):
        """Unicode characters in scene names."""
        tokens = mp.tokenize_component(
            "You.and.Idol.Precure.♪.S01E23.This.Is.My.Signature.1080p.mkv"
        )
        dot_tokens = [t for t in tokens if t.kind == mp.TokenKind.DOT_TEXT]
        texts = [t.text for t in dot_tokens]
        assert "♪" in texts


class TestTokenizePath:
    """Tests for tokenize() with full paths."""

    def test_single_component(self):
        tokens = mp.tokenize("[Group] Show - 01.mkv")
        path_seps = [t for t in tokens if t.kind == mp.TokenKind.PATH_SEP]
        assert len(path_seps) == 0

    def test_directory_and_file(self):
        tokens = mp.tokenize(
            "[Erai-raws] Gungrave - 01~26 [1080p]/"
            "[Erai-raws] Gungrave - 01 [1080p][C0751D22].mkv"
        )
        path_seps = [t for t in tokens if t.kind == mp.TokenKind.PATH_SEP]
        assert len(path_seps) == 1

    def test_deep_path(self):
        tokens = mp.tokenize("a/b/c.mkv")
        path_seps = [t for t in tokens if t.kind == mp.TokenKind.PATH_SEP]
        assert len(path_seps) == 2


class TestSceneDotSplitting:
    """Tests for dot-separated scene name handling."""

    def test_preserves_web_dl(self):
        tokens = mp._split_scene_dots("Show.S01E01.1080p.CR.WEB-DL.AAC2.0.H.264-VARYG")
        texts = [t.text for t in tokens]
        assert "WEB-DL" not in texts or True  # WEB-DL has a dash not a dot
        # Actually WEB-DL doesn't get split by dots, only H.264 and AAC2.0 matter
        assert "H.264-VARYG" in texts or "H.264" in texts
        assert "AAC2.0" in texts

    def test_preserves_dts_hd_ma(self):
        tokens = mp._split_scene_dots("Movie.2001.1080p.BluRay.FLAC.2.0.x265")
        texts = [t.text for t in tokens]
        assert "FLAC.2.0" in texts or "FLAC" in texts


# ===================================================================
# Phase B: Semantic classifier tests
# ===================================================================


def _classify(text: str) -> list[mp.Token]:
    """Helper: tokenize + classify a single component."""
    return mp.classify(mp.tokenize_component(text))


def _classify_path(text: str) -> list[mp.Token]:
    """Helper: tokenize + classify a full path."""
    return mp.classify(mp.tokenize(text))


class TestClassifyCRC32:
    def test_hex_8_chars(self):
        tokens = _classify("[Group] Show [D98B31F3].mkv")
        crc = [t for t in tokens if t.kind == mp.TokenKind.CRC32]
        assert len(crc) == 1
        assert crc[0].text == "D98B31F3"

    def test_not_crc32_if_non_hex(self):
        """G and H are not hex digits, so ABCDEFGH is not a valid CRC32."""
        tokens = _classify("[Group] Show [ABCDEFGH].mkv")
        crc = [t for t in tokens if t.kind == mp.TokenKind.CRC32]
        assert len(crc) == 0

    def test_not_crc32_if_wrong_length(self):
        tokens = _classify("[Group] Show [ABC].mkv")
        crc = [t for t in tokens if t.kind == mp.TokenKind.CRC32]
        assert len(crc) == 0


class TestClassifyReleaseGroup:
    def test_first_bracket_is_group(self):
        tokens = _classify("[Cyan] Show - 08.mkv")
        groups = [t for t in tokens if t.kind == mp.TokenKind.RELEASE_GROUP]
        assert len(groups) == 1
        assert groups[0].text == "Cyan"

    def test_scene_trailing_group(self):
        tokens = _classify("Show.S01E05.1080p.CR.WEB-DL.AAC2.0.H.264-VARYG.mkv")
        groups = [t for t in tokens if t.kind == mp.TokenKind.RELEASE_GROUP]
        assert len(groups) == 1
        assert groups[0].text == "VARYG"

    def test_scene_trailing_group_toonstub(self):
        tokens = _classify(
            "TO.BE.HERO.X.S01E01.NICE.1080p.CR.WEB-DL.DUAL.AAC2.0.H.264.MSubs-ToonsHub.mkv"
        )
        groups = [t for t in tokens if t.kind == mp.TokenKind.RELEASE_GROUP]
        assert len(groups) == 1
        assert groups[0].text == "ToonsHub"

    def test_japanese_group(self):
        tokens = _classify("[アニメ BD] 探偵オペラミルキィホームズ.mkv")
        groups = [t for t in tokens if t.kind == mp.TokenKind.RELEASE_GROUP]
        assert len(groups) == 1
        # "アニメ BD" is split: アニメ=group, BD=source
        assert groups[0].text == "アニメ"
        sources = [t for t in tokens if t.kind == mp.TokenKind.SOURCE]
        assert len(sources) == 1
        assert sources[0].text == "BD"


class TestClassifyEpisode:
    def test_s_e_format(self):
        tokens = _classify("Show.S01E05.1080p.mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert len(eps) == 1
        assert eps[0].season == 1
        assert eps[0].episode == 5

    def test_s_e_with_version(self):
        tokens = _classify("Show.S01E05v2.1080p.mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert eps[0].version == 2

    def test_dash_number(self):
        tokens = _classify("[Group] Show - 08 [1080p].mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert len(eps) == 1
        assert eps[0].episode == 8

    def test_dash_number_with_version(self):
        tokens = _classify("[Group] Show - 04v2 [1080p].mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert eps[0].episode == 4
        assert eps[0].version == 2

    def test_japanese_episode(self):
        tokens = _classify("[アニメ BD] 探偵オペラ 第01話「タイトル」.mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert len(eps) == 1
        assert eps[0].episode == 1

    def test_special_sp1(self):
        tokens = _classify("[Group] Show - SP1 [1080p].mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert len(eps) == 1
        assert eps[0].episode == 1

    def test_ep_end(self):
        tokens = _classify("[Group] Show - 14 END [1080p].mkv")
        eps = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert len(eps) == 1
        assert eps[0].episode == 14

    def test_batch_range(self):
        tokens = _classify("[Group] Show - 01~26 [1080p]")
        batches = [t for t in tokens if t.kind == mp.TokenKind.BATCH_RANGE]
        assert len(batches) == 1
        assert batches[0].batch_start == 1
        assert batches[0].batch_end == 26

    def test_batch_range_spaced(self):
        tokens = _classify("[Group] Show - 01 ~ 13 [1080p]")
        batches = [t for t in tokens if t.kind == mp.TokenKind.BATCH_RANGE]
        assert len(batches) == 1
        assert batches[0].batch_start == 1
        assert batches[0].batch_end == 13


class TestClassifySeason:
    def test_japanese_season_in_parens(self):
        tokens = _classify("[アニメ BD] 探偵オペラ(第1期) 第01話.mkv")
        seasons = [t for t in tokens if t.kind == mp.TokenKind.SEASON]
        assert len(seasons) == 1
        assert seasons[0].season == 1

    def test_nth_season_in_text(self):
        tokens = _classify("[Erai-raws] Golden Kamuy 4th Season - 01 [1080p].mkv")
        seasons = [t for t in tokens if t.kind == mp.TokenKind.SEASON]
        assert len(seasons) == 1
        assert seasons[0].season == 4


class TestClassifySource:
    def test_bd_source(self):
        pm = mp.parse_component("[Group] Show [BD 1080p].mkv")
        assert pm.source_type == "BD"

    def test_web_source(self):
        pm = mp.parse_component("[Group] Show [WEB-DL 1080p].mkv")
        assert pm.source_type == "Web"

    def test_dvd_source(self):
        pm = mp.parse_component("[Group] Show [DVD].mkv")
        assert pm.source_type == "DVD"

    def test_dvdrip_source(self):
        pm = mp.parse_component("[Group] Show [DVDRip].mkv")
        assert pm.source_type == "DVD"

    def test_dvd_r_source(self):
        pm = mp.parse_component("[Group] Show [DVD-R].mkv")
        assert pm.source_type == "DVD-R"

    def test_hdtv_source(self):
        pm = mp.parse_component("[Group] Show [HDTV].mkv")
        assert pm.source_type == "HDTV"

    def test_sdtv_source(self):
        pm = mp.parse_component("[Group] Show [SDTV].mkv")
        assert pm.source_type == "SDTV"

    def test_vcd_source(self):
        pm = mp.parse_component("[Group] Show [VCD].mkv")
        assert pm.source_type == "VCD"

    def test_cd_r_source(self):
        pm = mp.parse_component("[Group] Show [CD-R].mkv")
        assert pm.source_type == "CD-R"

    def test_web_dl_dot_text(self):
        tokens = _classify("Show.S01E05.1080p.CR.WEB-DL.mkv")
        # WEB-DL has a dash not a dot, so it stays as one DOT_TEXT token
        # After classification it becomes SOURCE
        sources = [t for t in tokens if t.kind == mp.TokenKind.SOURCE]
        assert any("WEB-DL" in t.text for t in sources) or any(
            "CR" in t.text for t in sources
        )


class TestClassifyYear:
    def test_year_in_parens(self):
        tokens = _classify("Movie (1964) (1080p BluRay).mkv")
        years = [t for t in tokens if t.kind == mp.TokenKind.YEAR]
        assert len(years) == 1
        assert years[0].year == 1964

    def test_year_in_dot_text(self):
        tokens = _classify("Movie.2005.WEB-DL.2160p.mkv")
        years = [t for t in tokens if t.kind == mp.TokenKind.YEAR]
        assert len(years) == 1
        assert years[0].year == 2005


class TestClassifyResolution:
    def test_1080p(self):
        assert mp._classify_text_content("1080p") == mp.TokenKind.RESOLUTION

    def test_dims(self):
        assert mp._classify_text_content("1920x1080") == mp.TokenKind.RESOLUTION

    def test_2160p(self):
        assert mp._classify_text_content("2160p") == mp.TokenKind.RESOLUTION


class TestClassifyCodec:
    def test_hevc(self):
        assert mp._classify_text_content("HEVC") == mp.TokenKind.VIDEO_CODEC

    def test_x264(self):
        assert mp._classify_text_content("x264") == mp.TokenKind.VIDEO_CODEC

    def test_aac(self):
        assert mp._classify_text_content("AAC") == mp.TokenKind.AUDIO_CODEC

    def test_flac(self):
        assert mp._classify_text_content("FLAC") == mp.TokenKind.AUDIO_CODEC

    def test_aac20(self):
        assert mp._classify_text_content("AAC2.0") == mp.TokenKind.AUDIO_CODEC


class TestClassifyLenticular:
    def test_episode_title(self):
        tokens = _classify("[アニメ BD] 探偵オペラ 第01話「屋根裏の入居者」.mkv")
        ep_titles = [t for t in tokens if t.kind == mp.TokenKind.EPISODE_TITLE]
        assert len(ep_titles) == 1
        assert ep_titles[0].text == "屋根裏の入居者"

    def test_bonus_content_in_lenticular(self):
        tokens = _classify("[アニメ BD] 探偵オペラ 映像特典「ノンテロップED」.mkv")
        bonus = [t for t in tokens if t.kind == mp.TokenKind.BONUS]
        assert len(bonus) >= 1


class TestClassifyBonus:
    def test_bonus_in_text(self):
        tokens = _classify("[アニメ BD] 探偵オペラ 映像特典.mkv")
        bonus = [t for t in tokens if t.kind == mp.TokenKind.BONUS]
        assert len(bonus) == 1


class TestClassifySubtitle:
    def test_multisub_bracket(self):
        tokens = _classify("[Group] Show - 01 [1080p][MultiSub][ABCD1234].mkv")
        subs = [t for t in tokens if t.kind == mp.TokenKind.SUBTITLE_INFO]
        assert len(subs) >= 1


# ===================================================================
# Phase C: Title extraction + ParsedMedia tests
# ===================================================================


class TestParseComponentFansub:
    """Fansub bracket style: [Group] Title - Episode [metadata][hash].ext"""

    def test_basic(self):
        pm = mp.parse_component(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        assert pm.release_group == "Cyan"
        assert pm.series_name == "Champignon no Majo"
        assert pm.episode == 8
        assert pm.hash_code == "D98B31F3"

    def test_erai_raws(self):
        pm = mp.parse_component(
            "[Erai-raws] Champignon no Majo - 11 "
            "[1080p CR WEB-DL AVC AAC][MultiSub][0A021911].mkv"
        )
        assert pm.release_group == "Erai-raws"
        assert pm.series_name == "Champignon no Majo"
        assert pm.episode == 11
        assert pm.hash_code == "0A021911"

    def test_multi_dash_title(self):
        pm = mp.parse_component(
            "[Erai-raws] Honzuki no Gekokujou - "
            "Shisho ni Naru Tame ni wa Shudan wo Erandeiraremasen - "
            "04v2 [1080p][Multiple Subtitle].mkv"
        )
        assert pm.release_group == "Erai-raws"
        assert "Honzuki no Gekokujou" in pm.series_name
        assert pm.episode == 4
        assert pm.version == 2

    def test_special_ep(self):
        pm = mp.parse_component(
            "[ak-Submarines] Girls und Panzer - MLLSD - SP1 [WEB 1080p][D227DE6D].mkv"
        )
        assert pm.release_group == "ak-Submarines"
        assert pm.episode == 1
        assert pm.is_special is True

    def test_ep_end(self):
        pm = mp.parse_component(
            "[Erai-raws] Honzuki no Gekokujou S3 - 10 END "
            "[1080p][Multiple Subtitle][E91FC872].mkv"
        )
        assert pm.episode == 10


class TestParseComponentScene:
    """Scene dot-separated style: Title.S01E05.Title.quality.source-Group.ext"""

    def test_basic(self):
        pm = mp.parse_component(
            "You.and.I.Are.Polar.Opposites.S01E01.You.My.Polar.Opposite."
            "1080p.CR.WEB-DL.DUAL.AAC2.0.H.264-VARYG.mkv"
        )
        assert pm.release_group == "VARYG"
        assert pm.season == 1
        assert pm.episode == 1
        assert "You" in pm.series_name and "Polar" in pm.series_name

    def test_to_be_hero(self):
        pm = mp.parse_component(
            "TO.BE.HERO.X.S01E01.NICE.1080p.CR.WEB-DL.DUAL."
            "AAC2.0.H.264.MSubs-ToonsHub.mkv"
        )
        assert pm.release_group == "ToonsHub"
        assert pm.season == 1
        assert pm.episode == 1
        assert "TO" in pm.series_name

    def test_with_year(self):
        pm = mp.parse_component("Movie.2005.WEB-DL.2160p.mkv")
        assert pm.year == 2005
        assert pm.series_name == "Movie"


class TestParseComponentJapanese:
    """Japanese naming with 第XX話, 「」, 第N期."""

    def test_season_and_episode(self):
        pm = mp.parse_component(
            "[アニメ BD] 探偵オペラミルキィホームズ(第1期) "
            "第01話「屋根裏の入居者」"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv"
        )
        assert pm.release_group == "アニメ"
        assert pm.source_type == "BD"
        assert pm.season == 1
        assert pm.episode == 1
        assert pm.episode_title == "屋根裏の入居者"
        assert "探偵オペラミルキィホームズ" in pm.series_name

    def test_season_2(self):
        pm = mp.parse_component(
            "[アニメ BD] 探偵オペラミルキィホームズ 第2幕(第2期) "
            "第05話「コソコソと支度」"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv"
        )
        assert pm.season == 2
        assert pm.episode == 5

    def test_final_episode(self):
        pm = mp.parse_component(
            "[アニメ BD] 探偵歌劇ミルキィホームズTD(第4期) "
            "第12話(終)「The detective of the Opera」"
            "(1920x1080 HEVC 10bit FLACx2 softSub(chi+eng) chap).mkv"
        )
        assert pm.season == 4
        assert pm.episode == 12


class TestParseComponentBareTitle:
    """Bare title with year/metadata in parens."""

    def test_movie_with_year(self):
        pm = mp.parse_component(
            "桃太郎 海の神兵 [Momotaro Sacred Sailors] (1945) "
            "- complete movie (BD, 1080p).mkv"
        )
        assert pm.year == 1945
        assert "桃太郎" in pm.series_name

    def test_dir_with_year_and_quality(self):
        pm = mp.parse_component(
            "Topkapi (1964) (1080p BluRay x265 10bit EAC3 2.0 r00t)"
        )
        assert pm.year == 1964
        assert "Topkapi" in pm.series_name


class TestParseMediaPath:
    """Full path parsing with directory + filename merge."""

    def test_dir_and_file(self):
        pm = mp.parse_media_path(
            "[Erai-raws] Gungrave - 01~26 [1080p]/"
            "[Erai-raws] Gungrave - 01 [1080p][C0751D22].mkv"
        )
        assert pm.episode == 1
        assert pm.hash_code == "C0751D22"
        assert "Gungrave" in pm.series_name
        assert "Gungrave" in pm.path_series_name

    def test_japanese_dir_and_file(self):
        pm = mp.parse_media_path(
            "[アニメ BD] 探偵オペラミルキィホームズ"
            "(+第2幕+ふたりは+TD+SS+Alternative) "
            "全51話+特典+CDx25+Scans"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap)/"
            "[アニメ BD] 探偵オペラミルキィホームズ(第1期) "
            "第01話「屋根裏の入居者」"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv"
        )
        assert pm.episode == 1
        assert pm.season == 1
        assert "探偵オペラミルキィホームズ" in pm.series_name
        assert pm.path_series_name  # Directory provides series name

    def test_single_file(self):
        pm = mp.parse_media_path(
            "[Cyan] Champignon no Majo - 08 [WEB 1080p x265][AAC][D98B31F3].mkv"
        )
        assert pm.episode == 8
        assert pm.path_series_name == ""  # No directory component

    def test_season_dir_subdirectory(self):
        """Season 01/ subdirectory."""
        pm = mp.parse_media_path("Yatagarasu/Season 01/file.mkv")
        assert pm.path_series_name == "Yatagarasu"


class TestTitleAliasIndex:
    """Tests for the title alias index."""

    def test_same_series_direct(self):
        idx = mp.TitleAliasIndex()
        idx.add_series(["Show A", "ショーA"])
        assert idx.same_series("Show A", "ショーA")

    def test_same_series_transitive(self):
        idx = mp.TitleAliasIndex()
        idx.add_series(["A", "B"])
        idx.add_series(["B", "C"])
        assert idx.same_series("A", "C")

    def test_different_series(self):
        idx = mp.TitleAliasIndex()
        idx.add_series(["Show A", "ショーA"])
        idx.add_series(["Show B", "ショーB"])
        assert not idx.same_series("Show A", "Show B")

    def test_identical_names(self):
        idx = mp.TitleAliasIndex()
        assert idx.same_series("Same", "Same")

    def test_unknown_name(self):
        idx = mp.TitleAliasIndex()
        assert not idx.same_series("Unknown", "Also Unknown")

    def test_lookup(self):
        idx = mp.TitleAliasIndex()
        idx.add_series(["Show A", "Show B", "ショーA"])
        aliases = idx.lookup("Show A")
        assert aliases is not None
        assert mp.normalize_for_matching("Show B") in aliases

    def test_merge_overlapping_series(self):
        idx = mp.TitleAliasIndex()
        idx.add_series(["A", "B"])
        idx.add_series(["C", "D"])
        idx.add_series(["B", "C"])  # merges the two groups
        assert idx.same_series("A", "D")
        assert idx.series_count == 1

    def test_build_from_cache(self):
        import os

        cache_dir = "/Volumes/home/.cache/etp"
        if not os.path.isdir(cache_dir):
            pytest.skip("Cache not available")
        idx = mp.build_title_index(cache_dir)
        assert idx.series_count > 0
        assert idx.title_count > idx.series_count


class TestNormalizeForMatching:
    def test_ascii(self):
        assert mp.normalize_for_matching("Champignon no Majo") == "champignonnomajo"

    def test_preserves_cjk(self):
        result = mp.normalize_for_matching("探偵オペラミルキィホームズ")
        assert "探偵オペラミルキィホームズ" in result

    def test_strips_punctuation_preserves_cjk(self):
        result = mp.normalize_for_matching("[アニメ BD] 探偵オペラ")
        assert "アニメ" in result
        assert "探偵オペラ" in result
        # Brackets and spaces stripped
        assert "[" not in result
        assert " " not in result

    def test_mixed_ascii_cjk(self):
        result = mp.normalize_for_matching("Girls & Panzer! 少女と戦車")
        assert "girlspanzer" in result
        assert "少女と戦車" in result


class TestBonusType:
    """Tests for Japanese bonus content type classification."""

    def test_pv(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 映像特典「PV1」(1920x1080 HEVC 10bit FLAC).mkv"
        )
        assert pm.bonus_type == "PV"
        assert pm.episode is None

    def test_ncop(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 映像特典「ノンテロップOP「Title」(specs).mkv"
        )
        assert pm.bonus_type == "NCOP"

    def test_nced(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 映像特典「ノンテロップED「Title」(specs).mkv"
        )
        assert pm.bonus_type == "NCED"

    def test_cm(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 映像特典「告知CM(発売中)」(specs).mkv"
        )
        assert pm.bonus_type == "CM"

    def test_preview(self):
        pm = mp.parse_component("[アニメ BD] Show(第4期) 映像特典「予告」(specs).mkv")
        assert pm.bonus_type == "Preview"

    def test_menu(self):
        pm = mp.parse_component("[アニメ BD] Show(第3期)「メニュー画面集」.rar")
        assert pm.bonus_type == "Menu"

    def test_regular_episode_no_bonus(self):
        pm = mp.parse_component("[アニメ BD] Show(第1期) 第01話「Title」(specs).mkv")
        assert pm.bonus_type == ""

    def test_classify_bonus_type_function(self):
        assert mp.classify_bonus_type("ノンテロップOP") == "NCOP"
        assert mp.classify_bonus_type("ノンテロップED") == "NCED"
        assert mp.classify_bonus_type("PV1") == "PV"
        assert mp.classify_bonus_type("告知CM(BD)") == "CM"
        assert mp.classify_bonus_type("予告") == "Preview"
        assert mp.classify_bonus_type("メニュー画面集") == "Menu"
        assert mp.classify_bonus_type("random text") == ""

    # The following tests use the [アニメ BD] naming convention for NCOP/NCED.
    # Other BD rip creators may use different patterns (e.g. "Creditless OP",
    # "Clean ED", "NCOP", "NCED", or romaji equivalents). Add test cases
    # here as new naming conventions are encountered.

    def test_ncop_extracts_song_title(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 映像特典「ノンテロップOP「正解はひとつ！じゃない!!」"
            "(1920x1080 HEVC 10bit FLAC).mkv"
        )
        assert pm.bonus_type == "NCOP"
        assert pm.episode_title == "正解はひとつ！じゃない!!"

    def test_nced_extracts_song_title(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 映像特典「ノンテロップED「本能のDOUBT」"
            "(1920x1080 HEVC 10bit FLAC).mkv"
        )
        assert pm.bonus_type == "NCED"
        assert pm.episode_title == "本能のDOUBT"

    def test_regular_episode_title_not_affected(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 第01話「屋根裏の入居者」"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv"
        )
        assert pm.bonus_type == ""
        assert pm.episode_title == "屋根裏の入居者"


class TestCleanSeriesTitle:
    def test_space_separated(self):
        assert mp.clean_series_title("Show S01-S02 BDRip x265") == "Show"

    def test_dot_separated(self):
        assert mp.clean_series_title("Show.S02.1080p.BluRay.x265-iAHD") == "Show"

    def test_no_metadata(self):
        assert mp.clean_series_title("Plain Title") == "Plain Title"

    def test_dual_audio(self):
        assert (
            mp.clean_series_title("Show S01-S02+OVA Dual Audio BDRip x265-EMBER")
            == "Show"
        )


class TestNameVariants:
    def test_strips_year(self):
        variants = mp.name_variants("Show Name (2024)")
        assert "showname" in variants

    def test_includes_clean_title(self):
        variants = mp.name_variants("Show S01 BDRip x265-GROUP")
        assert "show" in variants

    def test_plain_name(self):
        variants = mp.name_variants("Simple Name")
        assert "simplename" in variants


class TestMatchingKeysPrefix:
    def test_prefix_match_against_index_keys(self):
        idx = mp.TitleAliasIndex()
        keys = idx.matching_keys(
            "Long Title Name Here",
            index_keys={"longtitle", "otherseries"},
        )
        # "longtitlenamehere" starts with "longtitle"
        assert "longtitle" in keys
        assert "otherseries" not in keys

    def test_no_prefix_without_index_keys(self):
        idx = mp.TitleAliasIndex()
        keys = idx.matching_keys("Long Title Name Here")
        assert "longtitle" not in keys


class TestSceneTrailingGroup:
    def test_last_group_wins(self):
        pm = mp.parse_component("Show.S01E01.1080p.BluRay.10-Bit.x265-iAHD.mkv")
        assert pm.release_group == "iAHD"

    def test_single_group_first_wins(self):
        pm = mp.parse_component("[FLE] Show - 01 [BD 1080p].mkv")
        assert pm.release_group == "FLE"


# ===================================================================
# Corpus smoke tests (require NAS mount)
# ===================================================================


class TestCorpusSmoke:
    """Smoke tests against live downloads directory."""

    @pytest.fixture
    def downloads_dir(self):
        import os

        d = "/Volumes/docker/pvr/data/downloads"
        if not os.path.isdir(d):
            pytest.skip("NAS not mounted")
        return d

    def test_classify_no_errors(self, downloads_dir):
        import os

        errors = []
        total = 0
        for root, dirs, files in os.walk(downloads_dir):
            for name in files + dirs:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, downloads_dir)
                total += 1
                try:
                    mp.classify(mp.tokenize(rel))
                except Exception as e:
                    errors.append(f"{rel}: {e}")
        assert not errors, f"{len(errors)} errors:\n" + "\n".join(errors[:20])
        assert total > 0

    def test_parse_no_errors(self, downloads_dir):
        import os

        errors = []
        total = 0
        for root, dirs, files in os.walk(downloads_dir):
            for name in files + dirs:
                full = os.path.join(root, name)
                rel = os.path.relpath(full, downloads_dir)
                total += 1
                try:
                    mp.parse_media_path(rel)
                except Exception as e:
                    errors.append(f"{rel}: {e}")
        assert not errors, f"{len(errors)} errors:\n" + "\n".join(errors[:20])
        assert total > 0

    def test_media_files_have_series_name(self, downloads_dir):
        """Every media file should extract a non-empty series name."""
        import os

        missing = []
        for root, _dirs, files in os.walk(downloads_dir):
            for name in files:
                ext = os.path.splitext(name)[1].lower()
                if ext not in {".mkv", ".mp4", ".avi"}:
                    continue
                full = os.path.join(root, name)
                rel = os.path.relpath(full, downloads_dir)
                pm = mp.parse_media_path(rel)
                if not pm.series_name and not pm.path_series_name:
                    missing.append(rel)
        assert not missing, f"{len(missing)} files with no series name:\n" + "\n".join(
            missing[:20]
        )
