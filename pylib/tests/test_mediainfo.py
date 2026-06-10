"""Tests for mediainfo JSON parsing."""

from __future__ import annotations

from etp_lib.media_vocab import normalize_resolution
from etp_lib.mediainfo import _resolution_from_mediainfo, parse_mediainfo_json

# ---------------------------------------------------------------------------
# Fixtures: sample mediainfo JSON
# ---------------------------------------------------------------------------

MEDIAINFO_HEVC_DUAL_AUDIO = {
    "media": {
        "track": [
            {"@type": "General", "VideoCount": "1", "AudioCount": "2"},
            {
                "@type": "Video",
                "Format": "HEVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "10",
                "Encoded_Library_Name": "x265",
                "Encoded_Library": "x265 - 2.6+22",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "en",
                "Title": "English 5.1",
            },
            {
                "@type": "Audio",
                "Format": "FLAC",
                "Language": "ja",
                "Title": "Japanese 2.0",
            },
        ]
    }
}

MEDIAINFO_AVC_X264 = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "AVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "10",
                "Encoded_Library_Name": "x264",
                "Encoded_Library": "x264 - core 161 r3018",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "en",
                "Title": "English 2.0 AAC",
            },
            {
                "@type": "Audio",
                "Format": "FLAC",
                "Language": "ja",
                "Title": "Japanese 2.0",
            },
        ]
    }
}

MEDIAINFO_HDR_DOLBY_VISION = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "HEVC",
                "Width": "3840",
                "Height": "2160",
                "BitDepth": "10",
                "HDR_Format": "Dolby Vision, Version 1.0, dvhe.08.06, BL+RPU",
                "HDR_Format_Compatibility": "HDR10",
            },
            {
                "@type": "Audio",
                "Format": "DTS",
                "Language": "ja",
                "Title": "Japanese 5.1",
            },
        ]
    }
}

MEDIAINFO_COMMENTARY = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "AVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "8",
            },
            {
                "@type": "Audio",
                "Format": "AC-3",
                "Language": "ja",
                "Title": "Japanese 2.0",
            },
            {
                "@type": "Audio",
                "Format": "AC-3",
                "Language": "ja",
                "Title": "Director's Commentary",
            },
        ]
    }
}

MEDIAINFO_MULTI_AUDIO = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "HEVC",
                "Width": "1920",
                "Height": "1080",
                "BitDepth": "10",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "ja",
                "Title": "Japanese",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "en",
                "Title": "English",
            },
            {
                "@type": "Audio",
                "Format": "AAC",
                "Language": "de",
                "Title": "German",
            },
        ]
    }
}


MEDIAINFO_CHAPTER_IMAGES = {
    "media": {
        "track": [
            {"@type": "General"},
            {
                "@type": "Video",
                "Format": "AVC",
                "Width": "960",
                "Height": "720",
                "BitDepth": "8",
                "Title": "Main Program",
            },
            {
                "@type": "Video",
                "Format": "JPEG",
                "Width": "640",
                "Height": "480",
                "Title": "Chapter Images",
            },
            {"@type": "Audio", "Format": "AAC", "Language": "en"},
        ]
    }
}


class TestMediaInfoParsing:
    """Tests for mediainfo JSON parsing."""

    def test_still_image_track_ignored(self):
        """m4v chapter-thumbnail tracks must not shadow the main video."""
        mi = parse_mediainfo_json(MEDIAINFO_CHAPTER_IMAGES)
        assert mi.video_codec == "AVC"
        assert mi.resolution == "720p"
        assert (mi.width, mi.height) == (960, 720)

    def test_null_media_tolerated(self):
        """mediainfo emits "media": null for files it cannot open."""
        mi = parse_mediainfo_json({"media": None})
        assert mi.video_codec == ""
        assert mi.audio_tracks == []

    def test_hevc_dual_audio(self):
        mi = parse_mediainfo_json(MEDIAINFO_HEVC_DUAL_AUDIO)
        assert mi.video_codec == "HEVC"
        assert mi.resolution == "1080p"
        assert mi.bit_depth == 10
        assert mi.encoding_lib == "x265"
        assert len(mi.audio_tracks) == 2
        assert mi.audio_tracks[0].codec == "aac"
        assert mi.audio_tracks[0].language == "en"
        assert mi.audio_tracks[1].codec == "flac"
        assert mi.audio_tracks[1].language == "ja"

    def test_avc_x264(self):
        mi = parse_mediainfo_json(MEDIAINFO_AVC_X264)
        assert mi.video_codec == "AVC"
        assert mi.encoding_lib == "x264"

    def test_hdr_dolby_vision_with_compatibility(self):
        mi = parse_mediainfo_json(MEDIAINFO_HDR_DOLBY_VISION)
        assert mi.video_codec == "HEVC"
        assert mi.resolution == "4K"
        assert mi.hdr_type == "DoVi,HDR"

    def test_dolby_vision_without_compatibility(self):
        data = {
            "media": {
                "track": [
                    {"@type": "General"},
                    {
                        "@type": "Video",
                        "Format": "HEVC",
                        "Width": "3840",
                        "Height": "2160",
                        "BitDepth": "10",
                        "HDR_Format": "Dolby Vision, Version 1.0, dvhe.05.06, BL+RPU",
                    },
                ]
            }
        }
        mi = parse_mediainfo_json(data)
        assert mi.hdr_type == "DoVi"

    def test_commentary_detection(self):
        mi = parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert len(mi.audio_tracks) == 2
        assert mi.audio_tracks[0].is_commentary is False
        assert mi.audio_tracks[1].is_commentary is True

    def test_codec_case_conventions(self):
        """Open-source codecs lowercase, proprietary uppercase."""
        mi = parse_mediainfo_json(MEDIAINFO_HEVC_DUAL_AUDIO)
        assert mi.audio_tracks[0].codec == "aac"  # lowercase
        assert mi.audio_tracks[1].codec == "flac"  # lowercase

        mi2 = parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert mi2.audio_tracks[0].codec == "AC3"  # uppercase

    def test_resolution_normalization(self):
        assert normalize_resolution(1080) == "1080p"
        assert normalize_resolution(720) == "720p"
        assert normalize_resolution(2160) == "4K"
        assert normalize_resolution(540) == "540p"
        assert normalize_resolution(480) == "480p"
        assert normalize_resolution(576) == "576p"

    def test_resolution_interlaced(self):
        assert normalize_resolution(1080, scan_type="i") == "1080i"
        assert normalize_resolution(480, scan_type="i") == "480i"
        assert normalize_resolution(2160, scan_type="i") == "4K"  # 4K always 4K

    def test_mediainfo_resolution_with_scan_type(self):
        assert _resolution_from_mediainfo(1080, "Progressive") == "1080p"
        assert _resolution_from_mediainfo(1080, "Interlaced") == "1080i"
        assert _resolution_from_mediainfo(720, "Progressive") == "720p"
        assert _resolution_from_mediainfo(480, "Interlaced") == "480i"
        assert _resolution_from_mediainfo(2160, "Progressive") == "4K"

    def test_multi_audio(self):
        mi = parse_mediainfo_json(MEDIAINFO_MULTI_AUDIO)
        assert len(mi.audio_tracks) == 3
        languages = {t.language for t in mi.audio_tracks}
        assert languages == {"ja", "en", "de"}

    def test_no_encoding_lib(self):
        mi = parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert mi.encoding_lib == ""


class TestWidthAwareResolution:
    """Cropped widescreen encodes classify by width, not just height."""

    def test_scope_1080p_not_720p(self):
        # 2.39:1 "scope" Blu-ray: 1920x800
        assert normalize_resolution(800, width=1920) == "1080p"

    def test_scope_4k(self):
        assert normalize_resolution(1608, width=3840) == "4K"

    def test_height_only_unchanged(self):
        assert normalize_resolution(800) == "720p"

    def test_ultrawide_1080_not_4k(self):
        assert normalize_resolution(1080, width=2560) == "1080p"

    def test_full_frame_unaffected(self):
        assert normalize_resolution(1080, width=1920) == "1080p"
        assert normalize_resolution(2160, width=3840) == "4K"

    def test_anamorphic_dvd_height_wins(self):
        # Anamorphic PAL DVD: narrow width, standard height
        assert normalize_resolution(576, width=720) == "576p"

    def test_interlaced_scope(self):
        assert normalize_resolution(800, scan_type="i", width=1920) == "1080i"


class TestAudioCodecFamilies:
    """Lossless/lossy Dolby and DTS families stay distinct."""

    def test_eac3_not_collapsed(self):
        from etp_lib.mediainfo import _normalize_audio_codec

        assert _normalize_audio_codec("E-AC-3") == "EAC3"
        assert _normalize_audio_codec("E-AC-3 JOC") == "EAC3"  # Atmos

    def test_truehd(self):
        from etp_lib.mediainfo import _normalize_audio_codec

        assert _normalize_audio_codec("TrueHD") == "TrueHD"
        assert _normalize_audio_codec("MLP FBA") == "TrueHD"
        assert _normalize_audio_codec("MLP FBA 16-ch") == "TrueHD"

    def test_ac3_and_dts_unchanged(self):
        from etp_lib.mediainfo import _normalize_audio_codec

        assert _normalize_audio_codec("AC-3") == "AC3"
        assert _normalize_audio_codec("DTS-HD MA") == "DTS"
