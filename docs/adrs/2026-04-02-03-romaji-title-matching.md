# 03. Romaji Title Fallback for Anime Matching

Date: 2026-04-02

## Status

Accepted

## Context

AniDB provides anime titles in multiple languages: English (`en`), Japanese
(`ja`), and romanized Japanese (`x-jat`, e.g. "Youjo Senki"). Anime filenames in
the wild frequently use romaji titles, especially when the AniDB English title
is a localized translation (e.g. "Saga of Tanya the Evil") and the Japanese
title is in kanji. Without checking romaji, files using these titles fail to
match against their AniDB entries during triage and episode matching.

## Decision

Add a `title_romaji` field to both `AnimeInfo` and `Episode`, populated from
AniDB `x-jat` titles during API response parsing. The title filter in triage
builds its `known_titles` list in priority order: English, Japanese, then
romaji. Bonus matching (`manifest.py`) and `find_episode_title` also fall back
to romaji when English and Japanese titles produce no match.

The matching order is intentional: romaji is checked last to avoid false
positives when a more specific English or Japanese title is available. Prefix
matching applies equally to all three title variants.

## Consequences

- Files using romaji titles now match correctly, covering a common real-world
  naming pattern that previously required manual intervention.
- Each `AnimeInfo` and `Episode` stores one additional string field from the
  AniDB response. The overhead is negligible.
- Romaji as a last-resort fallback means it only influences matching when
  English and Japanese titles both fail. This minimizes the risk of incorrect
  matches from romanization ambiguity.
