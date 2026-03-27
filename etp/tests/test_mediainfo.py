"""Tests for mediainfo JSON parsing."""

from __future__ import annotations

from etp_lib.mediainfo import _resolution_shorthand, parse_mediainfo_json

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


class TestMediaInfoParsing:
    """Tests for mediainfo JSON parsing."""

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

    def test_resolution_shorthands(self):
        assert _resolution_shorthand(1920, 1080) == "1080p"
        assert _resolution_shorthand(1280, 720) == "720p"
        assert _resolution_shorthand(3840, 2160) == "4K"
        assert _resolution_shorthand(960, 540) == "540p"
        assert _resolution_shorthand(720, 480) == "480p"

    def test_multi_audio(self):
        mi = parse_mediainfo_json(MEDIAINFO_MULTI_AUDIO)
        assert len(mi.audio_tracks) == 3
        languages = {t.language for t in mi.audio_tracks}
        assert languages == {"ja", "en", "de"}

    def test_no_encoding_lib(self):
        mi = parse_mediainfo_json(MEDIAINFO_COMMENTARY)
        assert mi.encoding_lib == ""
