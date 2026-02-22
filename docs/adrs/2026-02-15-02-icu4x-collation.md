# Use ICU4X for Unicode-aware collation

Date: 2026-02-15

## Status

Accepted

## Context

Tree output needs to sort filenames in a way that matches user expectations for
mixed-case and Unicode names. The target platform is a Synology NAS running musl
libc, where `strcoll` is equivalent to `strcmp` — purely byte-order, no
locale-aware sorting. The binary must be statically linked for deployment.

## Decision

Use `icu_collator` (ICU4X) with root locale, `Strength::Quaternary`, and
`AlternateHandling::Shifted`. ICU4X embeds its data at compile time via
`icu_collator::CollatorBorrowed`, requiring no system ICU installation or
runtime data files.

## Consequences

- Tree output sorts case-insensitively (Alpha.txt before zebra.txt) and handles
  punctuation naturally (shifted handling treats punctuation as ignorable at
  primary strength).
- Works identically on macOS (development) and musl Linux (NAS) — no
  platform-dependent sort order.
- Adds ~2 MB to the static binary from embedded collation data.
- ICU4X collation is used for ALL output sorting — both CSV and tree. This
  ensures consistent sort order across output formats and platforms.
