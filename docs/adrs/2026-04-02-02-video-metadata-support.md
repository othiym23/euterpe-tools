# Extend Metadata System to Support Video Files

- **Status**: Accepted
- **Date**: 2026-04-02
- **Extends**:
  [2026-03-28-01 Mediainfo Over TagLib](2026-03-28-01-mediainfo-over-taglib.md)

## Context

The metadata system originally handled audio files only: lofty for most formats,
mediainfo subprocess for WMA/MKA (see ADR 2026-03-28-01). The anime ingestion
pipeline needs video metadata -- resolution, codec, bit depth, HDR format --
from .mkv, .mp4, and .avi files.

## Decision

Extend the existing dual-backend metadata system to handle video files:

- Rename `AUDIO_EXTENSIONS` to `MEDIA_EXTENSIONS` and add video extensions (mkv,
  mp4, avi).
- Route all video extensions to the mediainfo subprocess backend (added to
  `MEDIAINFO_EXTENSIONS`).
- Extract video-specific properties from mediainfo's Video track: width, height,
  bit depth, codec (Format), frame rate, and HDR format. These are stored as
  `video_*` prefixed keys alongside existing `audio_*` keys.
- Rename `AudioFileRecord` to `MediaFileRecord` throughout to reflect the
  broader scope.

The `FileMetadata` struct and its key-value properties/tags model remain
unchanged -- video properties are simply additional entries in the properties
list.

## Consequences

- **Positive**: Single metadata path for both audio and video. No new
  dependencies or backends needed.
- **Positive**: The anime pipeline can extract resolution and codec info using
  the same infrastructure as audio metadata scanning.
- **Negative**: mediainfo is now required for routine scanning, not just rare
  WMA/MKA files. It must be installed on any machine that scans video.
- **Negative**: Subprocess overhead (~10ms per file) applies to all video files,
  which may be more numerous than the WMA/MKA edge cases the mediainfo backend
  was originally designed for.
