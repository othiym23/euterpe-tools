# Mediainfo Subprocess Over TagLib FFI

- **Status**: Accepted
- **Date**: 2026-03-28
- **Supersedes**: Original SP2.2 plan (TagLib C++ FFI)

## Context

The original plan called for TagLib C++ FFI bindings to support DSF, WMA, and
MKA formats that lofty doesn't handle. TagLib is a C++ library, which creates
significant complexity for the musl static linking pipeline used for NAS
deployment:

- Requires a C++ standard library (libstdc++) statically linked for musl
- Needs TagLib compiled with musl-gcc + C++ support
- Adds CMake build dependency and `taglib-sys` build script modifications
- Increases binary size and build complexity

## Decision

Use `mediainfo --Output=JSON` as a subprocess for formats lofty doesn't support
(currently WMA and MKA). DSF is handled natively by lofty.

The metadata module dispatches by file extension: lofty for its 12+ supported
formats, mediainfo subprocess for WMA/MKA. Both backends produce the same
`FileMetadata` struct with normalized tag names.

mediainfo is already installed on the NAS and already used by the Python anime
module (`etp_lib/mediainfo.py`).

## Consequences

- **Positive**: No C++ dependencies. musl static linking works unchanged. No new
  Cargo dependencies for format support.
- **Positive**: mediainfo can serve as a general fallback for any future format
  lofty doesn't support.
- **Negative**: Subprocess spawn overhead per file (~10ms). Acceptable since
  WMA/MKA files are rare in the collection.
- **Negative**: No embedded image extraction for mediainfo-backed formats
  (mediainfo reports cover art presence but can't extract bytes).
- **Negative**: mediainfo must be installed on the system. Clear error if
  missing (`MetadataError::MediainfoNotFound`).
