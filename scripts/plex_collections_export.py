#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = ["pyyaml"]
# ///
"""Export & reconcile Plex anime collections into a single source of truth.

Reads a (copied) Plex ``com.plexapp.plugins.library.db`` and pulls every
collection from the English "Anime" and Japanese "アニメ" libraries. Both run
the same legacy AniDB agent (Kamehameha is a renamed copy of HamaTV), so shows
are ~97% keyed by the AniDB ID embedded in their GUID (``…://anidb-715?lang=en``);
the collections are hand-maintained and meant to be identical across the two
libraries, but have drifted.

Collections are ``tags`` (tag_type=2); membership is in ``taggings`` linking a
show (``metadata_items`` type 2) to the collection tag.

It writes a YAML SSOT (one entry per collection, keyed by member AniDB IDs, with
which libraries it appears in and any per-library membership drift) and prints a
**reconciliation report** for making the two legacy libraries consistent:

  * present in both, identical — already consistent
  * present in both, membership drift — sync members
  * same concept, different name — rename for consistency (and shared posters);
    found by member-set overlap, so ``ALL THE GUNDAM`` ↔ ``ALL THE GUNDAMS``
    matches even though the names differ
  * present in one library only — create it in the other

    ./scripts/plex_collections_export.py --db /tmp/plexdb/plex.db --out collections/anime-collections.yaml
"""

from __future__ import annotations

import argparse
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

import yaml

LIBRARIES = {10: "Anime", 7: "アニメ"}
GUID_PATTERNS = [
    ("anidb", re.compile(r"anidb-(\d+)")),
    ("tvdb", re.compile(r"tvdb3?-(\d+)")),
    ("tvdb", re.compile(r"thetvdb://(\d+)")),
    ("tmdb", re.compile(r"tmdb-(\d+)")),
    ("tmdb", re.compile(r"themoviedb://(\d+)")),
]
RENAME_JACCARD = 0.5  # member-set overlap above which differently-named
#                       collections are treated as the same concept


def parse_guid(guid: str) -> tuple[str, int] | None:
    for provider, pat in GUID_PATTERNS:
        m = pat.search(guid or "")
        if m:
            return provider, int(m[1])
    return None


@dataclass
class Member:
    provider: str
    pid: int
    title: str
    year: int | None


@dataclass
class Collection:
    name: str
    by_library: dict[str, dict[tuple[str, int], Member]] = field(default_factory=dict)

    @property
    def libraries(self) -> list[str]:
        return [lib for lib in LIBRARIES.values() if self.by_library.get(lib)]

    def keys(self, lib: str) -> frozenset[tuple[str, int]]:
        return frozenset(self.by_library.get(lib, {}))

    def union_members(self) -> dict[tuple[str, int], Member]:
        out: dict[tuple[str, int], Member] = {}
        for lib in LIBRARIES.values():  # Anime (en) first -> en titles win
            for k, m in self.by_library.get(lib, {}).items():
                out.setdefault(k, m)
        return out


def connect(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    for name in ("uca", "naturalsort", "natural"):
        conn.create_collation(name, lambda a, b: (a > b) - (a < b))
    return conn


def extract(conn: sqlite3.Connection) -> dict[str, Collection]:
    collections: dict[str, Collection] = {}
    for section_id, label in LIBRARIES.items():
        for tag, title, year, guid in conn.execute(
            """
            SELECT t.tag, mi.title, mi.year, mi.guid
            FROM taggings tg
            JOIN tags t ON tg.tag_id = t.id AND t.tag_type = 2
            JOIN metadata_items mi ON tg.metadata_item_id = mi.id
            WHERE mi.library_section_id = ? AND mi.metadata_type = 2
            """,
            (section_id,),
        ):
            col = collections.setdefault(tag, Collection(name=tag))
            members = col.by_library.setdefault(label, {})
            parsed = parse_guid(guid)
            key = parsed if parsed else ("unknown", len(members))
            members[key] = Member(key[0], key[1] if parsed else 0, title, year)
    return collections


def member_yaml(m: Member) -> dict:
    d: dict = {m.provider: m.pid} if m.provider != "unknown" else {}
    d["title"] = m.title
    if m.year:
        d["year"] = m.year
    return d


def jaccard(a: frozenset, b: frozenset) -> float:
    return len(a & b) / len(a | b) if (a or b) else 0.0


def reconcile(collections: dict[str, Collection]) -> dict[str, list]:
    """Categorize every collection for the two-library reconciliation."""
    a_names = {n for n, c in collections.items() if "Anime" in c.libraries}
    j_names = {n for n, c in collections.items() if "アニメ" in c.libraries}

    report: dict[str, list] = {
        "identical": [],  # in both, same members
        "drifted": [],  # in both, different members
        "rename": [],  # same concept, different name (member overlap)
        "anime_only": [],  # only in en (and no member-match in ja)
        "anime_jp_only": [],  # only in ja (and no member-match in en)
    }

    for n in sorted(a_names & j_names):
        c = collections[n]
        if c.keys("Anime") == c.keys("アニメ"):
            report["identical"].append(n)
        else:
            report["drifted"].append(n)

    a_only = sorted(a_names - j_names)
    j_only = sorted(j_names - a_names)
    j_keysets = {n: collections[n].keys("アニメ") for n in j_names}
    a_keysets = {n: collections[n].keys("Anime") for n in a_names}
    matched_j: set[str] = set()

    for n in a_only:
        ak = collections[n].keys("Anime")
        best, score = None, 0.0
        for jn, jk in j_keysets.items():
            if jn in matched_j or jn == n:
                continue
            s = jaccard(ak, jk)
            if s > score:
                best, score = jn, s
        if best and score >= RENAME_JACCARD:
            matched_j.add(best)
            report["rename"].append(
                {"anime": n, "アニメ": best, "overlap": round(score, 2)}
            )
        else:
            report["anime_only"].append(n)

    for n in j_only:
        if n in matched_j:
            continue
        jk = collections[n].keys("アニメ")
        best, score = None, 0.0
        for an, ak in a_keysets.items():
            if an == n:
                continue
            s = jaccard(jk, ak)
            if s > score:
                best, score = an, s
        if best and score >= RENAME_JACCARD:
            report["rename"].append(
                {"anime": best, "アニメ": n, "overlap": round(score, 2)}
            )
        else:
            report["anime_jp_only"].append(n)
    return report


def build_ssot(collections: dict[str, Collection]) -> list[dict]:
    entries = []
    for name in sorted(collections):
        c = collections[name]
        union = c.union_members()
        entry: dict = {
            "name": name,
            "libraries": c.libraries,
            "members": [
                member_yaml(union[k]) for k in sorted(union, key=lambda k: (k[0], k[1]))
            ],
        }
        if len(c.libraries) == 2 and c.keys("Anime") != c.keys("アニメ"):
            only_a = c.keys("Anime") - c.keys("アニメ")
            only_j = c.keys("アニメ") - c.keys("Anime")
            entry["drift"] = {}
            if only_a:
                entry["drift"]["Anime_only"] = [
                    member_yaml(c.by_library["Anime"][k]) for k in sorted(only_a)
                ]
            if only_j:
                entry["drift"]["アニメ_only"] = [
                    member_yaml(c.by_library["アニメ"][k]) for k in sorted(only_j)
                ]
        entries.append(entry)
    return entries


# Member-set matches that are NOT the same collection (one is a subset / related
# work). Routed to the create lists instead of renamed. Edit as you review.
FALSE_MATCHES = {
    ("PriPara", "Pretty Series collection"),
    ("Riding Bean collection", "Gunsmith Cats collection"),
}


def _members_for(col: Collection, keys) -> list[dict]:
    out = []
    for k in sorted(keys, key=lambda k: (k[0], k[1])):
        for lib in LIBRARIES.values():
            m = col.by_library.get(lib, {}).get(k)
            if m:
                out.append(member_yaml(m))
                break
    return out


def build_plan(collections: dict[str, Collection], report: dict[str, list]) -> dict:
    """Action plan to make the libraries consistent, with the アニメ (ja) name
    canonical: rename the Anime(en) collection to the ja name, union memberships,
    and create the missing ones."""
    name_matched = set(report["identical"]) | set(report["drifted"])
    anime_only = list(report["anime_only"])
    jp_only = list(report["anime_jp_only"])
    renames, excluded = [], []

    for r in report["rename"]:
        an, jn = r["anime"], r["アニメ"]
        if (an, jn) in FALSE_MATCHES:
            excluded.append(
                {
                    "anime": an,
                    "anime_jp": jn,
                    "overlap": r["overlap"],
                    "reason": "share members but are different collections",
                }
            )
            anime_only.append(an)
            jp_only.append(jn)
            continue
        ca, cj = collections[an], collections[jn]
        ak, jk = ca.keys("Anime"), cj.keys("アニメ")
        e: dict = {
            "canonical": jn,
            "rename_anime_from": an,
            "overlap": r["overlap"],
            "members": len(ak | jk),
        }
        if jk - ak:
            e["add_to_anime"] = _members_for(cj, jk - ak)
        if ak - jk:
            e["add_to_anime_jp"] = _members_for(ca, ak - jk)
        flags = []
        if r["overlap"] < 0.8:
            flags.append("low member overlap — verify the match")
        if an in name_matched:
            flags.append(
                f"a separate アニメ collection is also named {an!r} (likely a stray) — verify"
            )
        if flags:
            e["review"] = flags
        renames.append(e)

    sync = []
    for n in report["drifted"]:
        c = collections[n]
        ak, jk = c.keys("Anime"), c.keys("アニメ")
        e = {"name": n}
        if jk - ak:
            e["add_to_anime"] = _members_for(c, jk - ak)
        if ak - jk:
            e["add_to_anime_jp"] = _members_for(c, ak - jk)
        sync.append(e)

    create_jp = [
        {
            "name": n,
            "members": _members_for(collections[n], collections[n].keys("Anime")),
        }
        for n in sorted(set(anime_only))
    ]
    create_an = [
        {
            "name": n,
            "members": _members_for(collections[n], collections[n].keys("アニメ")),
        }
        for n in sorted(set(jp_only))
    ]
    return {
        "canonical": "アニメ (ja) names; rename the Anime (en) collections to match",
        "rename_in_anime": renames,
        "sync_members": sync,
        "create_in_anime_jp": create_jp,
        "create_in_anime": create_an,
        "excluded_false_matches": excluded,
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--db", type=Path, default=Path("/tmp/plexdb/plex.db"))
    ap.add_argument(
        "--out", type=Path, default=Path("collections/anime-collections.yaml")
    )
    ap.add_argument(
        "--plan", type=Path, default=Path("collections/reconciliation-plan.yaml")
    )
    args = ap.parse_args()

    conn = connect(args.db)
    collections = extract(conn)
    entries = build_ssot(collections)
    report = reconcile(collections)

    plan = build_plan(collections, report)
    args.plan.parent.mkdir(parents=True, exist_ok=True)
    args.plan.write_text(
        "# Generated by scripts/plex_collections_export.py — re-running OVERWRITES\n"
        "# this file; fold any hand edits back before regenerating.\n"
        "# Reconciliation plan: make the legacy Anime (en) and アニメ (ja) Plex\n"
        "# libraries consistent. CANONICAL = the アニメ (ja) name (primary library).\n"
        "# rename_in_anime: rename the Anime(en) collection to `canonical`; then\n"
        "#   union memberships (add_to_anime / add_to_anime_jp).\n"
        "# sync_members: same name in both, members differ -> add the listed shows.\n"
        "# create_*: collection exists in only one library -> create in the other.\n"
        "# Review entries with a `review:` note before applying.\n"
        + yaml.safe_dump(plan, allow_unicode=True, sort_keys=False, width=100),
        encoding="utf-8",
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(
        "# Generated by scripts/plex_collections_export.py — re-running OVERWRITES\n"
        "# this file; fold any hand edits back before regenerating.\n"
        "# Single source of truth for anime collections, extracted from the\n"
        "# legacy Plex 'Anime' (en) + 'アニメ' (ja) libraries. Keyed by member\n"
        "# AniDB IDs. `drift` lists members present in only one library.\n"
        "# Goal: make both legacy libraries consistent (same name + members).\n"
        + yaml.safe_dump(
            {"collections": entries}, allow_unicode=True, sort_keys=False, width=100
        ),
        encoding="utf-8",
    )

    n_anidb = sum(
        any(k[0] == "anidb" for k in collections[n].union_members())
        for n in collections
    )
    print(
        f"collections: {len(collections)}   anidb-keyed: {n_anidb}/{len(collections)}"
    )
    print(f"wrote SSOT -> {args.out}")
    print(f"wrote plan -> {args.plan}\n")
    print("Reconciliation plan (canonical = アニメ/ja names):")
    print(
        f"  rename Anime(en) -> ja name : {len(plan['rename_in_anime'])}"
        f"  ({sum('review' in e for e in plan['rename_in_anime'])} flagged for review)"
    )
    print(f"  sync membership drift       : {len(plan['sync_members'])}")
    print(f"  create in アニメ (ja)        : {len(plan['create_in_anime_jp'])}")
    print(f"  create in Anime (en)        : {len(plan['create_in_anime'])}")
    print(f"  excluded (false matches)    : {len(plan['excluded_false_matches'])}")
    print(f"  already identical           : {len(report['identical'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
