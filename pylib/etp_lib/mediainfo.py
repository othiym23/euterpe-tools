"""Mediainfo parsing — analyze video files and extract technical metadata."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

from etp_lib.types import AudioTrack, MediaInfo

# Match: "commentary" as a whole word in audio track titles
_RE_COMMENTARY = re.compile(r"\bcommentary\b", re.IGNORECASE)

# Map mediainfo Format values to our normalized codec names
_VIDEO_CODEC_MAP: dict[str, str] = {
    "HEVC": "HEVC",
    "AVC": "AVC",
    "AV1": "AV1",
    "MPEG-4 Visual": "XviD",  # Usually XviD for anime
    "VP9": "VP9",
    "MPEG Video": "MPEG2",
}

# Audio codec normalization: open-source lowercase, proprietary uppercase
_AUDIO_CODEC_MAP: dict[str, str] = {
    "AAC": "aac",
    "FLAC": "flac",
    "Opus": "opus",
    "Vorbis": "vorbis",
    "PCM": "pcm",
    "AC-3": "AC3",
    "E-AC-3": "AC3",
    "DTS": "DTS",
    "DTS-HD": "DTS",
    "DTS-HD MA": "DTS",
    "DTS-HD Master Audio": "DTS",
    "MLP FBA": "DTS",  # TrueHD/Atmos shows as MLP FBA in some cases
    "TrueHD": "AC3",  # Dolby TrueHD -> treat as AC3 family
    "MP3": "mp3",
    "MPEG Audio": "mp3",
    "mp2": "mp2",
}


def _resolution_shorthand(width: int, height: int) -> str:
    """Convert width x height to shorthand like '1080p', '720p', '4K'."""
    if height >= 2160 or width >= 3840:
        return "4K"
    if height >= 1080 or width >= 1920:
        return "1080p"
    if height >= 720 or width >= 1280:
        return "720p"
    if height >= 540 or width >= 960:
        return "540p"
    if height >= 480 or width >= 720:
        return "480p"
    return f"{height}p"


def _detect_hdr(video_track: dict) -> str:
    """Detect HDR type from mediainfo video track."""
    hdr_format = video_track.get("HDR_Format", "")
    hdr_compat = video_track.get("HDR_Format_Compatibility", "")
    transfer = video_track.get("transfer_characteristics", "")

    if "Dolby Vision" in hdr_format:
        if "HDR10" in hdr_compat:
            return "DoVi,HDR"
        return "DoVi"

    if "HDR10+" in hdr_format or "HDR10" in hdr_format:
        return "HDR"

    if "SMPTE ST 2084" in transfer or "PQ" in transfer:
        return "HDR"

    if "HLG" in hdr_format or "HLG" in transfer:
        return "HDR"

    # For 4K content without explicit HDR metadata
    return ""


def _normalize_audio_codec(format_str: str) -> str:
    """Normalize mediainfo audio Format to our naming convention."""
    # Try exact match first
    if format_str in _AUDIO_CODEC_MAP:
        return _AUDIO_CODEC_MAP[format_str]

    # Try prefix matching for variants like "DTS XLL" etc.
    for prefix in ("DTS", "AC-3", "E-AC-3", "AAC", "MLP"):
        if format_str.startswith(prefix):
            return _AUDIO_CODEC_MAP.get(prefix, format_str)

    return format_str.lower()


def _detect_encoding_lib(video_track: dict) -> str:
    """Detect x264/x265 from encoding library metadata."""
    lib_name = video_track.get("Encoded_Library_Name", "")
    lib_full = video_track.get("Encoded_Library", "")
    writing_lib = video_track.get("Writing_library", "")

    for field_val in (lib_name, lib_full, writing_lib):
        val = field_val.lower()
        if "x264" in val or "libx264" in val:
            return "x264"
        if "x265" in val or "libx265" in val:
            return "x265"

    return ""


def parse_mediainfo_json(data: dict) -> MediaInfo:
    """Parse mediainfo JSON output into a MediaInfo dataclass."""
    tracks = data.get("media", {}).get("track", [])

    video_codec = ""
    resolution = ""
    width = 0
    height = 0
    bit_depth = 8
    hdr_type = ""
    encoding_lib = ""
    audio_tracks: list[AudioTrack] = []

    for track in tracks:
        track_type = track.get("@type", "")

        if track_type == "Video":
            raw_format: str = track.get("Format", "")
            video_codec = _VIDEO_CODEC_MAP.get(raw_format, raw_format)
            width = int(track.get("Width", 0))
            height = int(track.get("Height", 0))
            bit_depth = int(track.get("BitDepth", 8))
            resolution = _resolution_shorthand(width, height)
            hdr_type = _detect_hdr(track)
            encoding_lib = _detect_encoding_lib(track)

        elif track_type == "Audio":
            raw_format = track.get("Format", "")
            codec = _normalize_audio_codec(raw_format)
            language = track.get("Language", "")
            title = track.get("Title", "")
            is_commentary = bool(_RE_COMMENTARY.search(title))
            audio_tracks.append(
                AudioTrack(
                    codec=codec,
                    language=language,
                    title=title,
                    is_commentary=is_commentary,
                )
            )

    return MediaInfo(
        video_codec=video_codec,
        resolution=resolution,
        width=width,
        height=height,
        bit_depth=bit_depth,
        hdr_type=hdr_type,
        audio_tracks=audio_tracks,
        encoding_lib=encoding_lib,
    )


def analyze_file(path: Path) -> MediaInfo:
    """Run mediainfo on a file and return parsed MediaInfo."""
    result = subprocess.run(
        ["mediainfo", "--Output=JSON", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(result.stdout)
    return parse_mediainfo_json(data)
