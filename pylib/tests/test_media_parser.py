"""Tests for the media path tokenizer and parser."""

from __future__ import annotations

import pytest

from etp_lib import media_parser as mp
from etp_lib.media_parser import scan_words, scan_dot_segments, _try_recognize


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
        all_texts = [t.text for t in tokens]
        # Compound tokens preserved (now as typed tokens, not DOT_TEXT)
        assert "AAC2.0" in all_texts
        assert "H.264" in all_texts
        assert "Show" in all_texts
        # S01E05 is now EPISODE, not DOT_TEXT
        ep_tokens = [t for t in tokens if t.kind == mp.TokenKind.EPISODE]
        assert len(ep_tokens) == 1

    def test_scene_preserves_h264(self):
        tokens = mp.tokenize_component(
            "TO.BE.HERO.X.S01E01.NICE.1080p.CR.WEB-DL.DUAL.AAC2.0.H.264.MSubs-ToonsHub.mkv"
        )
        all_texts = [t.text for t in tokens]
        assert "H.264" in all_texts
        assert "AAC2.0" in all_texts

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
    """Tests for dot-separated scene name handling via scanner."""

    def test_preserves_compound_tokens(self):
        """H.264 and AAC2.0 are recognized as typed tokens, not split on dots."""
        tokens = mp.tokenize_component(
            "Show.S01E01.1080p.CR.WEB-DL.AAC2.0.H.264-VARYG.mkv"
        )
        all_texts = [t.text for t in tokens]
        assert "H.264" in all_texts
        assert "AAC2.0" in all_texts

    def test_preserves_flac(self):
        tokens = mp.tokenize_component("Movie.2001.1080p.BluRay.FLAC.2.0.x265.mkv")
        audio = [t for t in tokens if t.kind == mp.TokenKind.AUDIO_CODEC]
        assert any("FLAC" in t.text for t in audio)


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
    @pytest.mark.parametrize(
        "filename,expected_text",
        [
            ("[Group] Show [D98B31F3].mkv", "D98B31F3"),
            ("[Group] Show [ABCDEFGH].mkv", None),  # non-hex digits
            ("[Group] Show [ABC].mkv", None),  # wrong length
        ],
    )
    def test_crc32_detection(self, filename, expected_text):
        tokens = _classify(filename)
        crc = [t for t in tokens if t.kind == mp.TokenKind.CRC32]
        if expected_text:
            assert len(crc) == 1
            assert crc[0].text == expected_text
        else:
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
        specials = [t for t in tokens if t.kind == mp.TokenKind.SPECIAL]
        assert len(specials) == 1
        assert specials[0].episode == 1

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
    @pytest.mark.parametrize(
        "tag,expected",
        [
            ("BD 1080p", "BD"),
            ("WEB-DL 1080p", "Web"),
            ("DVD", "DVD"),
            ("DVDRip", "DVD"),
            ("DVD-R", "DVD-R"),
            ("HDTV", "HDTV"),
            ("SDTV", "SDTV"),
            ("VCD", "VCD"),
            ("CD-R", "CD-R"),
        ],
    )
    def test_source_type(self, tag, expected):
        pm = mp.parse_component(f"[Group] Show [{tag}].mkv")
        assert pm.source_type == expected

    def test_web_dl_dot_text(self):
        tokens = _classify("Show.S01E05.1080p.CR.WEB-DL.mkv")
        sources = [t for t in tokens if t.kind == mp.TokenKind.SOURCE]
        assert any("WEB-DL" in t.text for t in sources) or any(
            "CR" in t.text for t in sources
        )


class TestClassifyYear:
    @pytest.mark.parametrize(
        "filename,expected_year",
        [
            ("Movie (1964) (1080p BluRay).mkv", 1964),
            ("Movie.2005.WEB-DL.2160p.mkv", 2005),
        ],
    )
    def test_year_extraction(self, filename, expected_year):
        tokens = _classify(filename)
        years = [t for t in tokens if t.kind == mp.TokenKind.YEAR]
        assert len(years) == 1
        assert years[0].year == expected_year


class TestClassifyTextKind:
    """Test classify_text() for resolution and codec keywords."""

    @pytest.mark.parametrize(
        "word,expected_kind",
        [
            ("1080p", mp.TokenKind.RESOLUTION),
            ("1920x1080", mp.TokenKind.RESOLUTION),
            ("2160p", mp.TokenKind.RESOLUTION),
            ("HEVC", mp.TokenKind.VIDEO_CODEC),
            ("x264", mp.TokenKind.VIDEO_CODEC),
            ("AAC", mp.TokenKind.AUDIO_CODEC),
            ("FLAC", mp.TokenKind.AUDIO_CODEC),
            ("AAC2.0", mp.TokenKind.AUDIO_CODEC),
        ],
    )
    def test_classify_text(self, word, expected_kind):
        assert mp.classify_text(word) == expected_kind


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
    @pytest.mark.parametrize(
        "text,expected_in,not_expected_in",
        [
            ("Champignon no Majo", ["champignonnomajo"], []),
            ("探偵オペラミルキィホームズ", ["探偵オペラミルキィホームズ"], []),
            ("[アニメ BD] 探偵オペラ", ["アニメ", "探偵オペラ"], ["[", " "]),
            ("Girls & Panzer! 少女と戦車", ["girlspanzer", "少女と戦車"], []),
        ],
    )
    def test_normalize(self, text, expected_in, not_expected_in):
        result = mp.normalize_for_matching(text)
        for s in expected_in:
            assert s in result, f"{s!r} not in {result!r}"
        for s in not_expected_in:
            assert s not in result, f"{s!r} unexpectedly in {result!r}"


class TestBonusType:
    """Tests for Japanese bonus content type classification."""

    @pytest.mark.parametrize(
        "filename,expected_bonus",
        [
            (
                "[アニメ BD] Show(第1期) 映像特典「PV1」(1920x1080 HEVC 10bit FLAC).mkv",
                "PV",
            ),
            (
                "[アニメ BD] Show(第1期) 映像特典「ノンテロップOP「Title」(specs).mkv",
                "NCOP",
            ),
            (
                "[アニメ BD] Show(第1期) 映像特典「ノンテロップED「Title」(specs).mkv",
                "NCED",
            ),
            ("[アニメ BD] Show(第1期) 映像特典「告知CM(発売中)」(specs).mkv", "CM"),
            ("[アニメ BD] Show(第4期) 映像特典「予告」(specs).mkv", "Preview"),
            ("[アニメ BD] Show(第3期)「メニュー画面集」.rar", "Menu"),
            ("[アニメ BD] Show(第1期) 第01話「Title」(specs).mkv", ""),
        ],
    )
    def test_bonus_type_from_filename(self, filename, expected_bonus):
        pm = mp.parse_component(filename)
        assert pm.bonus_type == expected_bonus

    @pytest.mark.parametrize(
        "text,expected",
        [
            ("ノンテロップOP", "NCOP"),
            ("ノンテロップED", "NCED"),
            ("PV1", "PV"),
            ("告知CM(BD)", "CM"),
            ("予告", "Preview"),
            ("メニュー画面集", "Menu"),
            ("random text", ""),
        ],
    )
    def test_classify_bonus_type_function(self, text, expected):
        assert mp.classify_bonus_type(text) == expected

    # The following tests use the [アニメ BD] naming convention for NCOP/NCED.
    # Other BD rip creators may use different patterns (e.g. "Creditless OP",
    # "Clean ED", "NCOP", "NCED", or romaji equivalents). Add test cases
    # here as new naming conventions are encountered.

    @pytest.mark.parametrize(
        "filename,expected_bonus,expected_title",
        [
            (
                "[アニメ BD] Show(第1期) 映像特典「ノンテロップOP「正解はひとつ！じゃない!!」"
                "(1920x1080 HEVC 10bit FLAC).mkv",
                "NCOP",
                "正解はひとつ！じゃない!!",
            ),
            (
                "[アニメ BD] Show(第1期) 映像特典「ノンテロップED「本能のDOUBT」"
                "(1920x1080 HEVC 10bit FLAC).mkv",
                "NCED",
                "本能のDOUBT",
            ),
        ],
    )
    def test_song_title_extraction(self, filename, expected_bonus, expected_title):
        pm = mp.parse_component(filename)
        assert pm.bonus_type == expected_bonus
        assert pm.episode_title == expected_title

    def test_regular_episode_title_not_affected(self):
        pm = mp.parse_component(
            "[アニメ BD] Show(第1期) 第01話「屋根裏の入居者」"
            "(1920x1080 HEVC 10bit FLAC softSub(chi+eng) chap).mkv"
        )
        assert pm.bonus_type == ""
        assert pm.episode_title == "屋根裏の入居者"


class TestCleanSeriesTitle:
    @pytest.mark.parametrize(
        "title,expected",
        [
            ("Show S01-S02 BDRip x265", "Show"),
            ("Show.S02.1080p.BluRay.x265-iAHD", "Show"),
            ("Plain Title", "Plain Title"),
            ("Show S01-S02+OVA Dual Audio BDRip x265-EMBER", "Show"),
        ],
    )
    def test_clean_series_title(self, title, expected):
        assert mp.clean_series_title(title) == expected


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
# Corpus smoke tests (require cached fixture or NAS mount)
# ===================================================================


class TestCorpusSmoke:
    """Smoke tests against real download filenames.

    Uses a cached fixture of relative paths generated by
    ``pylib/tools/gen_corpus_fixture.py``.  The fixture is auto-generated
    on first run if the NAS is mounted, and reused on subsequent runs.
    """

    def test_corpus_smoke(self, corpus_paths):
        """Single-pass check: classify, parse, and verify series names."""
        all_paths = corpus_paths["all"]
        media_paths = set(corpus_paths["media"])

        classify_errors: list[str] = []
        parse_errors: list[str] = []
        missing_name: list[str] = []

        for rel in all_paths:
            # Classify
            try:
                mp.classify(mp.tokenize(rel))
            except Exception as e:
                classify_errors.append(f"{rel}: {e}")

            # Parse
            try:
                pm = mp.parse_media_path(rel)
            except Exception as e:
                parse_errors.append(f"{rel}: {e}")
                continue

            # Series name (media files only)
            if rel in media_paths:
                if not pm.series_name and not pm.path_series_name:
                    missing_name.append(rel)

        errors: list[str] = []
        if classify_errors:
            errors.append(
                f"{len(classify_errors)} classify errors:\n"
                + "\n".join(classify_errors[:10])
            )
        if parse_errors:
            errors.append(
                f"{len(parse_errors)} parse errors:\n" + "\n".join(parse_errors[:10])
            )
        if missing_name:
            errors.append(
                f"{len(missing_name)} media files with no series name:\n"
                + "\n".join(missing_name[:10])
            )
        assert not errors, "\n\n".join(errors)


# ===================================================================
# Scanner: word-level scanning
# ===================================================================


class TestScanWords:
    """Test word-level scanning with parsy recognizers."""

    def test_audio_codec_compound(self):
        tokens = scan_words("AAC2.0")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.AUDIO_CODEC
        assert tokens[0].text == "AAC2.0"

    def test_dts_hdma(self):
        tokens = scan_words("DTS-HDMA")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.AUDIO_CODEC
        assert tokens[0].text == "DTS-HDMA"

    def test_episode_se(self):
        tokens = scan_words("S01E05")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.EPISODE
        assert tokens[0].season == 1
        assert tokens[0].episode == 5

    def test_episode_se_version(self):
        tokens = scan_words("S01E01v2")
        assert len(tokens) == 1
        assert tokens[0].version == 2

    def test_episode_bare(self):
        tokens = scan_words("08")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.EPISODE
        assert tokens[0].episode == 8

    def test_year_not_episode(self):
        tokens = scan_words("2019")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.YEAR
        assert tokens[0].year == 2019

    def test_unrecognized_text(self):
        tokens = scan_words("Champignon")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.UNKNOWN
        assert tokens[0].text == "Champignon"

    def test_mixed_content(self):
        """Bracket content like 'WEB 1080p x265' should produce typed tokens."""
        tokens = scan_words("WEB 1080p x265")
        kinds = [t.kind for t in tokens]
        assert mp.TokenKind.SOURCE in kinds
        assert mp.TokenKind.RESOLUTION in kinds
        assert mp.TokenKind.VIDEO_CODEC in kinds

    def test_bracket_audio_metadata(self):
        """[LPCM 2.0 + DTS-HDMA 2.1] content should recognize audio codecs."""
        tokens = scan_words("LPCM 2.0 + DTS-HDMA 2.1")
        audio = [t for t in tokens if t.kind == mp.TokenKind.AUDIO_CODEC]
        assert len(audio) >= 1
        texts = [t.text for t in audio]
        assert any("LPCM" in t for t in texts)

    def test_dash_compound_remux_group(self):
        """REMUX-FraMeSToR should split into REMUX + unknown FraMeSToR."""
        tokens = scan_words("REMUX-FraMeSToR")
        kinds = [t.kind for t in tokens]
        assert mp.TokenKind.REMUX in kinds
        texts = [t.text for t in tokens]
        assert "FraMeSToR" in texts

    def test_sonarr_bracket_content(self):
        """Sonarr-style [Group source,res,...] bracket content."""
        tokens = scan_words("Hinna Bluray-1080p Remux,8bit,AVC,FLAC")
        assert tokens[0].kind == mp.TokenKind.UNKNOWN
        assert tokens[0].text == "Hinna"

    def test_bonus_ncop(self):
        tokens = scan_words("NCOP")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.BONUS

    def test_bonus_nc_ed1(self):
        tokens = scan_words("NC ED1")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.BONUS

    def test_japanese_episode(self):
        tokens = scan_words("第01話")
        assert len(tokens) == 1
        assert tokens[0].kind == mp.TokenKind.EPISODE
        assert tokens[0].episode == 1


# ===================================================================
# Scanner: dot-separated scene scanning
# ===================================================================


class TestScanDotSegments:
    """Test dot-separated scene scanning with compound token handling."""

    def test_simple_scene(self):
        tokens = scan_dot_segments("Show.S01E05.1080p.BluRay.x265-GROUP")
        texts = [t.text for t in tokens]
        kinds = [t.kind for t in tokens]
        assert "Show" in texts
        assert mp.TokenKind.EPISODE in kinds
        assert mp.TokenKind.RESOLUTION in kinds
        assert mp.TokenKind.SOURCE in kinds

    def test_compound_h264(self):
        """H.264 should be recognized as a single video codec token."""
        tokens = scan_dot_segments("Show.S01E05.1080p.H.264-VARYG")
        video = [t for t in tokens if t.kind == mp.TokenKind.VIDEO_CODEC]
        assert len(video) == 1
        assert video[0].text == "H.264"

    def test_compound_h264_with_trailing_group(self):
        """H.264-VARYG should produce H.264 (codec) + VARYG (group)."""
        tokens = scan_dot_segments("Show.S01E05.H.264-VARYG")
        groups = [t for t in tokens if t.kind == mp.TokenKind.RELEASE_GROUP]
        assert len(groups) == 1
        assert groups[0].text == "VARYG"

    def test_compound_aac20(self):
        """AAC2.0 should be recognized as a single audio codec token."""
        tokens = scan_dot_segments("Show.S01E05.AAC2.0.x265")
        audio = [t for t in tokens if t.kind == mp.TokenKind.AUDIO_CODEC]
        assert len(audio) == 1
        assert audio[0].text == "AAC2.0"

    def test_scene_trailing_group(self):
        """x265-GROUP should split into codec + release group."""
        tokens = scan_dot_segments("Show.S01E05.1080p.x265-GROUP")
        groups = [t for t in tokens if t.kind == mp.TokenKind.RELEASE_GROUP]
        assert len(groups) == 1
        assert groups[0].text == "GROUP"

    def test_full_scene_filename(self):
        """Full scene filename with all metadata types."""
        tokens = scan_dot_segments(
            "You.and.I.Are.Polar.Opposites.S01E01.You.My.Polar.Opposite"
            ".1080p.CR.WEB-DL.DUAL.AAC2.0.H.264-VARYG"
        )
        kinds = {t.kind for t in tokens}
        assert mp.TokenKind.EPISODE in kinds
        assert mp.TokenKind.RESOLUTION in kinds
        assert mp.TokenKind.AUDIO_CODEC in kinds
        assert mp.TokenKind.VIDEO_CODEC in kinds
        assert mp.TokenKind.RELEASE_GROUP in kinds

        video = [t for t in tokens if t.kind == mp.TokenKind.VIDEO_CODEC]
        assert video[0].text == "H.264"
        audio = [t for t in tokens if t.kind == mp.TokenKind.AUDIO_CODEC]
        assert audio[0].text == "AAC2.0"

    def test_season_only(self):
        """S01 without E should be recognized as season."""
        tokens = scan_dot_segments("Golden.Kamuy.S01.1080p.BluRay")
        seasons = [t for t in tokens if t.kind == mp.TokenKind.SEASON]
        assert len(seasons) == 1
        assert seasons[0].season == 1

    def test_unrecognized_words_are_dot_text(self):
        """Words that don't match any recognizer stay as DOT_TEXT."""
        tokens = scan_dot_segments("You.and.I.Are.Polar.Opposites")
        assert all(t.kind == mp.TokenKind.DOT_TEXT for t in tokens)
        assert [t.text for t in tokens] == [
            "You",
            "and",
            "I",
            "Are",
            "Polar",
            "Opposites",
        ]


# ===================================================================
# Scanner: individual word recognition
# ===================================================================


class TestTryRecognize:
    """Test individual word recognition via parsy primitives."""

    @pytest.mark.parametrize(
        "word,expected_kind",
        [
            ("1080p", mp.TokenKind.RESOLUTION),
            ("HEVC", mp.TokenKind.VIDEO_CODEC),
            ("x265", mp.TokenKind.VIDEO_CODEC),
            ("FLAC", mp.TokenKind.AUDIO_CODEC),
            ("AAC2.0", mp.TokenKind.AUDIO_CODEC),
            ("DTS-HDMA", mp.TokenKind.AUDIO_CODEC),
            ("BD", mp.TokenKind.SOURCE),
            ("BluRay", mp.TokenKind.SOURCE),
            ("WEB-DL", mp.TokenKind.SOURCE),
            ("REMUX", mp.TokenKind.REMUX),
            ("S01E05", mp.TokenKind.EPISODE),
            ("OVA", mp.TokenKind.SPECIAL),
            ("v2", mp.TokenKind.VERSION),
            ("2019", mp.TokenKind.YEAR),
            ("D98B31F3", mp.TokenKind.CRC32),
            ("jpn", mp.TokenKind.LANGUAGE),
            ("NCOP", mp.TokenKind.BONUS),
        ],
    )
    def test_recognized(self, word, expected_kind):
        token = _try_recognize(word)
        assert token is not None, f"{word!r} was not recognized"
        assert token.kind == expected_kind, (
            f"{word!r}: expected {expected_kind.name}, got {token.kind.name}"
        )

    @pytest.mark.parametrize(
        "word",
        ["Champignon", "the", "of", "Hello", "FraMeSToR", "VARYG"],
    )
    def test_not_recognized(self, word):
        assert _try_recognize(word) is None


# ===================================================================
# Regression: QA-discovered edge cases
# ===================================================================


class TestQARegression:
    """Regression tests from parser QA review of real filenames."""

    def test_sxxexx_dash_title_no_space(self):
        """S01E01-Title (no space before dash) should strip leading dash."""
        pm = mp.parse_component("S01E01-A World Without Books.mkv")
        assert pm.season == 1
        assert pm.episode == 1
        assert pm.episode_title == "A World Without Books"

    def test_directory_metadata_propagation(self):
        """File with no metadata should inherit from directory."""
        pm = mp.parse_media_path(
            "Ascendance of a Bookworm S01-S02+OVA Dual Audio BDRip x265-EMBER/"
            "01.Ascendance of a Bookworm S01 1080p Dual Audio BDRip 10 bits x265-EMBER/"
            "S01E01-A World Without Books.mkv"
        )
        assert pm.series_name or pm.path_series_name
        assert pm.episode == 1
        # Episode title should not have leading dash
        assert not pm.episode_title.startswith("-")

    def test_multi_season_batch_directory_cleaning(self):
        """S01-S02+OVA should be stripped from path_series_name."""
        pm = mp.parse_media_path(
            "Ascendance of a Bookworm S01-S02+OVA Dual Audio BDRip x265-EMBER/"
            "S01E01-A World Without Books.mkv"
        )
        # path_series_name should be cleaned of metadata
        assert "S01-S02" not in pm.path_series_name
        assert "BDRip" not in pm.path_series_name
        assert "Ascendance" in pm.path_series_name

    def test_directory_metadata_fills_gaps(self):
        """When file has no source/resolution/codec, directory should fill in."""
        pm = mp.parse_media_path(
            "Show S01 1080p BDRip x265-GROUP/S01E01-Episode Title.mkv"
        )
        assert pm.source_type == "BD"
        assert pm.resolution == "1080p"
        assert pm.video_codec == "x265"
        assert pm.release_group == "GROUP"

    def test_s01ova_recognized_as_special(self):
        """S01OVA should be parsed as season 1 OVA special."""
        pm = mp.parse_component("S01OVA-Eustachius's Incognito Operation.mkv")
        assert pm.season == 1
        assert pm.is_special is True
        assert pm.episode_title == "Eustachius's Incognito Operation"

    def test_directory_release_group_dash_audio(self):
        """FLAC-TTGA in directory should extract TTGA as release group."""
        pm = mp.parse_media_path(
            "Show S03+SP 1080p Dual Audio BD Remux FLAC-TTGA/"
            "S03E01-The Beginning of Winter.mkv"
        )
        assert pm.release_group == "TTGA"

    def test_multi_directory_metadata_propagation(self):
        """Metadata should be found across multiple directory components."""
        pm = mp.parse_media_path(
            "Ascendance of a Bookworm S01-S02+OVA Dual Audio BDRip x265-EMBER/"
            "01.Ascendance of a Bookworm S01 1080p Dual Audio BDRip 10 bits x265-EMBER/"
            "S01E01-A World Without Books.mkv"
        )
        assert pm.resolution == "1080p"
        assert pm.release_group == "EMBER"
        assert pm.source_type == "BD"

    def test_directory_audio_codec_propagation(self):
        """FLAC in directory should propagate to file audio_codecs."""
        pm = mp.parse_media_path(
            "Show S03 1080p BD Remux FLAC-TTGA/S03E01-Episode Title.mkv"
        )
        assert any("FLAC" in c for c in pm.audio_codecs)

    @pytest.mark.parametrize(
        "filename,expected_bonus",
        [
            ("S03ED-Kotoba ni Dekinai [Maaya Sakamoto].mkv", "NCED"),
            ("S03OP-Ano hi No Kotoba [Nao Toyama].mkv", "NCOP"),
        ],
    )
    def test_sxxop_sxxed_credit_special(self, filename, expected_bonus):
        pm = mp.parse_component(filename)
        assert pm.season == 3
        assert pm.is_special is True
        assert pm.bonus_type == expected_bonus

    def test_bracket_artist_not_release_group(self):
        """[Artist Name] should not override directory release group."""
        pm = mp.parse_media_path(
            "Show S03 1080p BD FLAC-TTGA/S03ED-Song Title [Artist Name].mkv"
        )
        # Directory group TTGA should win over bracket artist
        assert pm.release_group == "TTGA"

    def test_season_zero_is_special(self):
        """S00E01 should be marked as a special."""
        pm = mp.parse_component(
            "Buddy.Daddies.S00E01.Cherry-Pick.1080p.BluRay.Remux."
            "FLAC2.0.H.264-CRUCiBLE.mkv"
        )
        assert pm.season == 0
        assert pm.episode == 1
        assert pm.is_special is True
        assert "Cherry-Pick" in pm.episode_title or "Cherry" in pm.episode_title

    def test_scene_hyphenated_episode_title(self):
        """Cherry-Pick should not be split into Cherry + release group Pick."""
        pm = mp.parse_component("Show.S01E05.Cherry-Pick.1080p.BluRay.x265-GROUP.mkv")
        assert pm.episode_title == "Cherry-Pick"
        assert pm.release_group == "GROUP"

    def test_dd_plus_audio_codec(self):
        """DD+ / DD+2.0 should be recognized as Dolby Digital Plus."""
        pm = mp.parse_component("Movie.2020.1080p.WEB-DL.DD+2.0.H.264-GROUP.mkv")
        assert any("DD+" in c for c in pm.audio_codecs)

    def test_chi_in_scene_title(self):
        """'Chi' in a scene title is a known false positive for Chinese language.

        We accept this because chi is a valid language code needed for donghua.
        The parser will exclude 'Chi' from the series name for this title.
        """
        pm = mp.parse_component(
            "Chi.wa.kawaiteru.1960.1080p.WEB-DL.DD+2.0.H.264-SbR.mkv"
        )
        # Chi is recognized as language — accepted false positive
        assert pm.year == 1960
        assert pm.source_type == "Web"

    def test_esub_recognized(self):
        """ESub (English subtitle) should be recognized as metadata."""
        token = mp._try_recognize("ESub")
        assert token is not None

    @pytest.mark.parametrize(
        "filename,expected_service",
        [
            ("Show.S01E01.1080p.AMZN.WEB-DL.DDP2.0.H.264-GROUP.mkv", "AMZN"),
            ("Show.S01E01.1080p.CR.WEB-DL.AAC2.0.H.264-GROUP.mkv", "CR"),
        ],
    )
    def test_streaming_service_parsed(self, filename, expected_service):
        pm = mp.parse_component(filename)
        assert pm.streaming_service == expected_service

    def test_10bit_recognized_as_metadata(self):
        """10bit / 10-Bit should not appear in series name."""
        pm = mp.parse_component(
            "Eraserhead.1977.1080p.BluRay.x265.10bit.AAC.2.0-HeVK.mkv"
        )
        assert "10bit" not in pm.series_name

    @pytest.mark.parametrize(
        "filename",
        [
            "[Group] Show - 01 (BD 1080p HEVC Opus) [Dual Audio].mkv",
            "Show.S01E01.1080p.BluRay.Dual-Audio.x265-GROUP.mkv",
        ],
    )
    def test_dual_audio_detected(self, filename):
        pm = mp.parse_component(filename)
        assert pm.is_dual_audio is True

    def test_criterion_edition(self):
        """Criterion should be recognized as metadata, not title."""
        pm = mp.parse_component("Movie.1977.Criterion.1080p.BluRay.x265-GROUP.mkv")
        assert "Criterion" not in pm.series_name
        assert pm.is_criterion is True

    def test_redistributor_not_release_group(self):
        """[TGx] redistributor bracket should not override scene group."""
        pm = mp.parse_media_path(
            "Movie.2022.2160p.WEB-DL.DD5.1.H.265-EVO[TGx]/"
            "Movie.2022.2160p.WEB-DL.DD5.1.H.265-EVO.mkv"
        )
        assert pm.release_group == "EVO"

    def test_bracket_dot_separated_metadata(self):
        """[x264.AAC] should be expanded as metadata, not release group."""
        pm = mp.parse_component("[Group] Show - 01 [x264.AAC][BD056DD6].mkv")
        assert pm.video_codec == "x264"
        assert "AAC" in pm.audio_codecs
        assert pm.hash_code == "BD056DD6"

    def test_hdr_detected_scene(self):
        """HDR in scene-style filename should populate hdr field."""
        pm = mp.parse_media_path(
            "Confess.Fletch.2022.2160p.WEB-DL.DD5.1.HDR.H.265-EVO[TGx]/"
            "Confess.Fletch.2022.2160p.WEB-DL.DD5.1.HDR.H.265-EVO.mkv"
        )
        assert pm.hdr == "HDR"
        assert pm.resolution == "4K"
        assert pm.release_group == "EVO"

    @pytest.mark.parametrize(
        "filename,expected_hdr",
        [
            ("Movie.2022.2160p.BluRay.HDR10.x265-GROUP.mkv", "HDR10"),
            ("Movie.2022.2160p.UHD.BluRay.x265-GROUP.mkv", "UHD"),
            ("Movie.2022.2160p.BluRay.DoVi.x265-GROUP.mkv", "DoVi"),
        ],
    )
    def test_hdr_format_detected(self, filename, expected_hdr):
        pm = mp.parse_component(filename)
        assert pm.hdr == expected_hdr

    def test_hdr_not_in_series_name(self):
        pm = mp.parse_component("Movie.2022.2160p.HDR.BluRay.x265-GROUP.mkv")
        assert "HDR" not in pm.series_name

    @pytest.mark.parametrize(
        "filename,expected_depth",
        [
            ("Movie.2022.1080p.BluRay.10bit.x265-GROUP.mkv", 10),
            ("Movie.2022.1080p.BluRay.8bit.x264-GROUP.mkv", 8),
        ],
    )
    def test_bit_depth(self, filename, expected_depth):
        pm = mp.parse_component(filename)
        assert pm.bit_depth == expected_depth

    def test_bit_depth_hi10p(self):
        """Hi10P should be recognized as 10-bit with extra field checks."""
        pm = mp.parse_component(
            "Fullmetal.Alchemist.Brotherhood.53.v2.1080p.BluRay."
            "Dual-Audio.FLAC2.0.Hi10P.x264-JySzE.mkv"
        )
        assert pm.bit_depth == 10
        assert pm.episode == 53
        assert pm.version == 2
        assert "Hi10" not in pm.series_name

    def test_bit_depth_not_in_series_name(self):
        pm = mp.parse_component("Movie.2022.1080p.10bit.BluRay.x265-GROUP.mkv")
        assert "10bit" not in pm.series_name

    def test_underscore_separated_old_fansub(self):
        """Old fansub convention: _-_ separator, underscored paren metadata."""
        pm = mp.parse_media_path(
            "Gintama The Semi-Final/"
            "[DB]Gintama The Semi-Final_-_01_(10bit_BD1080p_x265).mkv"
        )
        assert pm.series_name == "Gintama The Semi-Final"
        assert pm.episode == 1
        assert pm.bit_depth == 10
        assert pm.source_type == "BD"
        assert pm.resolution == "1080p"
        assert pm.video_codec == "x265"
        assert pm.release_group == "DB"
        assert pm.bit_depth == 10

    def test_version_after_space(self):
        """S01E01 v2 — version as separate token after episode."""
        pm = mp.parse_component("Jigokuraku - S01E01 v2 (BD 1080p HEVC) [Vodes].mkv")
        assert pm.version == 2
        assert pm.episode == 1

    def test_truehd_5_1_compound(self):
        """TrueHD.5.1 should be recognized as compound audio codec."""
        pm = mp.parse_media_path(
            "Golden.Kamuy.S01.1080p.BluRay.Remux.AVC.TrueHD.5.1-Hinna/"
            "Golden.Kamuy.S01E04.1080p.BluRay.Remux.AVC.TrueHD.5.1-Hinna.mkv"
        )
        assert "TrueHD 5.1" in pm.audio_codecs
        assert pm.release_group == "Hinna"

    @pytest.mark.parametrize(
        "filename,expected_res",
        [
            ("Show - 01 (BDRip 1440x1080p x265 HEVC FLAC 2.0)[sxales].mkv", "1080p"),
            ("Movie.2022.1920x1080.BluRay.x265-GROUP.mkv", "1080p"),
            ("Show - 01 (BDRip 720x480 x265 AC3 2.0)[Group].mkv", "480p"),
            ("Show.S01E01.1080i.HDTV.x264-GROUP.mkv", "1080i"),
            ("Movie.2022.4K.UHD.BluRay.x265-GROUP.mkv", "4K"),
        ],
    )
    def test_resolution_normalization(self, filename, expected_res):
        pm = mp.parse_component(filename)
        assert pm.resolution == expected_res

    def test_ova_space_episode(self):
        """OVA 01 with space should detect special + episode."""
        pm = mp.parse_component(
            "Show - OVA 01 Notice (BDRip 720x480p x265 HEVC AC3 2.0)[sxales].mkv"
        )
        assert pm.is_special is True
        assert pm.episode == 1
        assert pm.episode_title == "Notice"

    def test_eraserhead_aac_2_0(self):
        """AAC.2.0 should be parsed as compound audio, not episode."""
        pm = mp.parse_component(
            "Eraserhead.1977.Criterion.1080p.BluRay.x265.hevc.10bit.AAC.2.0-HeVK.mkv"
        )
        assert "AAC 2.0" in pm.audio_codecs
        assert pm.episode is None
        assert pm.year == 1977

    def test_ova2e03_episode_from_e_prefix(self):
        """OVA2E03 — episode should be 3 (from E03), not 2 (from OVA2)."""
        pm = mp.parse_component("Golden Kamuy - OVA2E03 Shiton Animal Chronicles.mkv")
        assert pm.series_name == "Golden Kamuy"
        assert pm.episode == 3
        assert pm.is_special is True
        assert pm.special_tag == "OVA2"
        assert pm.episode_title == "Shiton Animal Chronicles"

    def test_sp_in_spring_not_special(self):
        """Sp inside 'Spring' should not match as special tag."""
        pm = mp.parse_component("S03E04-Spring Prayer.mkv")
        assert pm.is_special is False
        assert pm.episode == 4
        assert pm.episode_title == "Spring Prayer"

    def test_part_number_not_episode(self):
        """'Part 1' should be preserved as title, not consumed as episode."""
        pm = mp.parse_component("S03SP01-Ascendance of a Bookworm   Part 1.mkv")
        assert pm.episode_title == "Ascendance of a Bookworm Part 1"

    def test_trailing_dot_segment_release_group(self):
        """Short trailing dot-segment after metadata = release group."""
        pm = mp.parse_component("Confess.Fletch.2022.BDRemux.1080p.pk.mkv")
        assert pm.series_name == "Confess Fletch"
        assert pm.release_group == "pk"

    def test_oad_in_road_not_special(self):
        """OAD inside 'Road' should not be matched as a special tag."""
        pm = mp.parse_component(
            "Kimagure Orange Road - OVA 02 Notice "
            "(BDRip 720x480p x265 HEVC AC3 2.0)[sxales].mkv"
        )
        assert pm.series_name == "Kimagure Orange Road"
        assert pm.is_special is True
        assert pm.episode == 2

    def test_dual_standalone_scene(self):
        """Scene-style DUAL (without Audio) should set is_dual_audio."""
        pm = mp.parse_component(
            "Anne.Shirley.S01E01.1080p.CR.WEB-DL.DUAL.AAC2.0.H.264-VARYG.mkv"
        )
        assert pm.is_dual_audio is True
        assert pm.streaming_service == "CR"
        assert pm.release_group == "VARYG"

    def test_dual_dot_audio_scene(self):
        """Dual.Audio (dot-separated) should set is_dual_audio."""
        pm = mp.parse_component(
            "Chained.Soldier.S01E07.BD.1080p.x264.FLAC.EAC3.Dual.Audio-Freehold.mkv"
        )
        assert pm.is_dual_audio is True

    def test_dual_audio_propagated_from_directory(self):
        """Dual Audio in directory name should propagate to file ParsedMedia."""
        pm = mp.parse_media_path(
            "Ascendance of a Bookworm S03+SP 1080p Dual Audio BD Remux FLAC-TTGA/"
            "S03E01-The Beginning of Winter.mkv"
        )
        assert pm.is_dual_audio is True
        assert pm.release_group == "TTGA"

    def test_dual_audio_propagated_nested_directory(self):
        """Dual Audio in grandparent directory should propagate."""
        pm = mp.parse_media_path(
            "Ascendance of a Bookworm S01-S02+OVA Dual Audio BDRip x265-EMBER/"
            "01.Ascendance of a Bookworm S01 1080p Dual Audio BDRip 10 bits x265-EMBER/"
            "S01E01-A World Without Books.mkv"
        )
        assert pm.is_dual_audio is True

    @pytest.mark.parametrize(
        "filename",
        [
            "Chained.Soldier.S02E01.ADN.WEB-DL.1080p.x264.AAC.EAC3.Dual.Audio.Uncensored-Freehold.mkv",
            "[SubsPlus+] Chained Soldier - S02E01 (ADN WEB-DL 1080p AVC AAC) (Uncensored) [76A7C1CD].mkv",
        ],
    )
    def test_uncensored_detected(self, filename):
        pm = mp.parse_component(filename)
        assert pm.is_uncensored is True

    # -- Multi-episode range expansion (Sonarr-inspired) --

    def test_multi_episode_range_with_e(self):
        """S01E01-E06 should expand to episodes [1..6]."""
        pm = mp.parse_component("Show.S01E01-E06.720p.BluRay.x265-GROUP.mkv")
        assert pm.season == 1
        assert pm.episode == 1
        assert pm.episodes == [1, 2, 3, 4, 5, 6]

    def test_multi_episode_range_bare(self):
        """S01E01-06 should expand to episodes [1..6]."""
        pm = mp.parse_component("Show.S01E01-06.720p.BluRay.x265-GROUP.mkv")
        assert pm.season == 1
        assert pm.episode == 1
        assert pm.episodes == [1, 2, 3, 4, 5, 6]

    def test_multi_episode_repeated(self):
        """S01E01E02E03 should produce episodes [1, 2, 3]."""
        pm = mp.parse_component("Show.S01E01E02E03.720p.BluRay.x265-GROUP.mkv")
        assert pm.season == 1
        assert pm.episode == 1
        assert pm.episodes == [1, 2, 3]

    def test_single_episode_no_episodes_list(self):
        """Single S01E05 should not populate episodes list."""
        pm = mp.parse_component("Show.S01E05.720p.BluRay.x265-GROUP.mkv")
        assert pm.episode == 5
        assert pm.episodes == []

    # -- Year validation (Sonarr-inspired, extended to 1940) --

    def test_year_validation(self):
        """Years outside [1940, current+1] should be rejected."""
        from etp_lib.media_parser import _CURRENT_YEAR

        # Rejected
        assert mp.parse_component("Movie.1935.1080p.BluRay.mkv").year is None
        assert (
            mp.parse_component(f"Movie.{_CURRENT_YEAR + 2}.1080p.BluRay.mkv").year
            is None
        )
        # Accepted
        assert mp.parse_component("Movie.1940.1080p.BluRay.mkv").year == 1940
        assert (
            mp.parse_component(f"Movie.{_CURRENT_YEAR + 1}.1080p.BluRay.mkv").year
            == _CURRENT_YEAR + 1
        )

    # -- Bilingual title splitting --

    def test_cjk_slash_title_split(self):
        """CJK / English title should split into series_name and series_name_alt."""
        pm = mp.parse_component(
            "[LoliHouse] 中文标题 / English Title - 01 [1080p] [ABCD1234].mkv"
        )
        assert pm.series_name == "中文标题"
        assert pm.series_name_alt == "English Title"
        assert pm.episode == 1

    def test_pipe_title_split(self):
        """Title with | separator should split into primary and alt."""
        pm = mp.parse_component(
            "[Group] 日本語タイトル | Japanese Title - 05 [720p].mkv"
        )
        assert pm.series_name == "日本語タイトル"
        assert pm.series_name_alt == "Japanese Title"

    def test_no_split_without_separator(self):
        """Normal title without / or | should not populate alt."""
        pm = mp.parse_component("[Group] My Normal Title - 08 [1080p].mkv")
        assert pm.series_name == "My Normal Title"
        assert pm.series_name_alt == ""

    # -- LoliHouse dual numbering --

    def test_lolihouse_dual_numbering_season(self):
        """[Group] Title - 001 (S01E01) should get season from parens."""
        pm = mp.parse_component(
            "[LoliHouse] My Anime Title - 001 (S01E01) [ABCD1234].mkv"
        )
        assert pm.series_name == "My Anime Title"
        assert pm.season == 1
        assert pm.episode == 1
        assert pm.hash_code == "ABCD1234"

    # -- dir_series cleaning --

    def test_dir_series_strips_trailing_brackets(self):
        """Trailing [group] should be stripped from path_series_name."""
        pm = mp.parse_media_path(
            "Blue Reflection Ray v2 [WEB Dual Audio 1080p AVC E-AC3 AAC] [hchcsen]/"
            "[hchcsen] Blue Reflection Ray S01E02 v2 [WEB Dual Audio 1080p AVC AAC].mkv"
        )
        assert pm.path_series_name == "Blue Reflection Ray v2"

    # -- sub as subtitle keyword --

    def test_sub_not_in_series_name(self):
        """'.sub' suffix should be recognized as subtitle keyword, not title."""
        pm = mp.parse_component(
            "Chi.wa.kawaiteru.1960.1080p.WEB-DL.DD+2.0.H.264-SbR.Rus.sub.mkv"
        )
        assert "sub" not in pm.series_name

    # -- Season N format --

    @pytest.mark.parametrize(
        "filename,expected_season,expected_ep",
        [
            ("[GM-Team] Title (Season 01) 01.mkv", 1, 1),
            ("[GM-Team] Title (Season 2) 05.mkv", 2, 5),
            ("[Group] Title 2nd Season - 05.mkv", 2, 5),
        ],
    )
    def test_season_n_formats(self, filename, expected_season, expected_ep):
        pm = mp.parse_component(filename)
        assert pm.season == expected_season
        assert pm.episode == expected_ep

    # -- Decimal episode specials (Sonarr-inspired) --

    @pytest.mark.parametrize(
        "filename,expected_ep,expected_special",
        [
            ("[Group] Title - 01.5 [1080p].mkv", 1, True),
            ("[Group] Title - 12.5 [720p][ABCD1234].mkv", 12, True),
            ("Title.01.5.1080p.BluRay.mkv", None, False),  # dot-separated: no decimal
        ],
    )
    def test_decimal_episodes(self, filename, expected_ep, expected_special):
        pm = mp.parse_component(filename)
        assert pm.is_special is expected_special
        if expected_ep is not None:
            assert pm.episode == expected_ep
