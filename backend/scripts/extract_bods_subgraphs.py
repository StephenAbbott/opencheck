"""Extract per-LEI BODS v0.4 subgraphs from the Open Ownership processed
GLEIF and UK PSC datasets.

Why this exists
---------------

The live mappers in OpenCheck produce a thin slice of BODS v0.4 — they
work off the GLEIF Level 2 endpoints and the UK Companies House public
JSON, neither of which expresses the full ownership chain in a way
that the dagre visualiser can connect into a single graph.

Open Ownership publish the *processed* CH and GLEIF datasets as proper
BODS v0.4 — interconnected subject/interestedParty relationships, all
the way up to ultimate beneficial owners — at:

* https://bods-data.openownership.org/source/uk_version_0_4/
* https://bods-data.openownership.org/source/gleif_version_0_4/

The Datasette JSON API in front of those works for `WHERE id = '<LEI>'`
lookups but times out on the relationship walks (no indexes on
``recorddetails_subject`` / ``recorddetails_interestedparty`` against
6 M / 14 M row tables, 5 s default query timeout).

This script runs against your *local* copies of the SQLite files
(downloaded from the bods-data pages above), creates the missing
indexes once, walks the relationship graph from each LEI out to N
hops, and writes the resulting subgraph as BODS v0.4 JSON-Lines.

Usage
-----

::

    cd backend
    python scripts/extract_bods_subgraphs.py \\
        --gleif /path/to/gleif_version_0_4.db \\
        --uk /path/to/uk_version_0_4.db \\
        --leis 213800LH1BZH3DI6G760 253400JT3MQWNDKMJE44 \\
        --max-hops 3

Output (relative to repo root)::

    data/cache/bods_data/gleif/<LEI>.jsonl
    data/cache/bods_data/uk/<GB-COH>.jsonl   # only when LEI is UK-domiciled

The runtime loader (``opencheck/bods_data.py``) reads these files
during ``/lookup`` and uses them as the canonical BODS bundle for the
GLEIF + Companies House sources, overriding the live transformation.

Performance notes
-----------------

* The first run creates indexes on ``recorddetails_subject``,
  ``recorddetails_interestedparty``, and the ``entity_statement.statementid``.
  This is one-off (~1–5 minutes per index on the 14 M-row UK table)
  and persists in the SQLite file.
* Subsequent walks per LEI complete in milliseconds.
* ``--max-hops 3`` is enough to reach the typical 2–4 layer chains
  that AMLA cares about; bump to 5 if you want to stress-test the
  ``COMPLEX_OWNERSHIP_LAYERS`` rule.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

LEI_SCHEME = "XI-LEI"
GB_COH_SCHEME = "GB-COH"


# ---------------------------------------------------------------------
# Index helpers — one-off, idempotent
# ---------------------------------------------------------------------


_REQUIRED_INDEXES = [
    # Walk both directions of the relationship graph.
    (
        "idx_rel_subject",
        "CREATE INDEX IF NOT EXISTS idx_rel_subject "
        "ON relationship_statement(recorddetails_subject)",
    ),
    (
        "idx_rel_interestedparty",
        "CREATE INDEX IF NOT EXISTS idx_rel_interestedparty "
        "ON relationship_statement(recorddetails_interestedparty)",
    ),
    # Resolve the other side of a relationship to an entity_statement.
    (
        "idx_entity_sid",
        "CREATE INDEX IF NOT EXISTS idx_entity_sid "
        "ON entity_statement(statementid)",
    ),
    # Join interest rows back to their parent relationship.
    (
        "idx_rel_interests_link",
        "CREATE INDEX IF NOT EXISTS idx_rel_interests_link "
        "ON relationship_recordDetails_interests(_link_relationship_statement)",
    ),
    # Identifier lookup: find an entity_statement by its LEI / GB-COH.
    (
        "idx_ids_value_scheme",
        "CREATE INDEX IF NOT EXISTS idx_ids_value_scheme "
        "ON entity_recordDetails_identifiers(id, scheme)",
    ),
    # Address / asserted-by joins.
    (
        "idx_addr_link",
        "CREATE INDEX IF NOT EXISTS idx_addr_link "
        "ON entity_recordDetails_addresses(_link_entity_statement)",
    ),
    (
        "idx_ids_link",
        "CREATE INDEX IF NOT EXISTS idx_ids_link "
        "ON entity_recordDetails_identifiers(_link_entity_statement)",
    ),
]

# UK PSC has person tables too; same idea.
_UK_EXTRA_INDEXES = [
    (
        "idx_person_sid",
        "CREATE INDEX IF NOT EXISTS idx_person_sid "
        "ON person_statement(statementid)",
    ),
    (
        "idx_person_names_link",
        "CREATE INDEX IF NOT EXISTS idx_person_names_link "
        "ON person_recordDetails_names(_link_person_statement)",
    ),
    (
        "idx_person_nat_link",
        "CREATE INDEX IF NOT EXISTS idx_person_nat_link "
        "ON person_recordDetails_nationalities(_link_person_statement)",
    ),
    (
        "idx_person_addr_link",
        "CREATE INDEX IF NOT EXISTS idx_person_addr_link "
        "ON person_recordDetails_addresses(_link_person_statement)",
    ),
]


def ensure_indexes(conn: sqlite3.Connection, *, has_persons: bool) -> None:
    """Create the indexes we need for fast subgraph walks."""
    indexes = list(_REQUIRED_INDEXES)
    if has_persons:
        indexes += _UK_EXTRA_INDEXES
    for name, ddl in indexes:
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError as exc:
            # Skip tables that don't exist in this dataset (GLEIF has
            # no person_statement, for example).
            if "no such table" in str(exc).lower():
                continue
            raise
    conn.commit()


def has_table(conn: sqlite3.Connection, name: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    )
    return cur.fetchone() is not None


# ---------------------------------------------------------------------
# Lookups
# ---------------------------------------------------------------------


def find_entity_link_by_identifier(
    conn: sqlite3.Connection, value: str, scheme: str
) -> str | None:
    cur = conn.execute(
        "SELECT _link_entity_statement FROM entity_recordDetails_identifiers "
        "WHERE id = ? AND scheme = ? LIMIT 1",
        (value, scheme),
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_entity_statementid(conn: sqlite3.Connection, link: str) -> str | None:
    cur = conn.execute(
        "SELECT statementid FROM entity_statement WHERE _link = ?", (link,)
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_gb_coh_for_lei(conn: sqlite3.Connection, lei: str) -> str | None:
    """If the GLEIF subject is UK-domiciled, return its GB-COH number."""
    link = find_entity_link_by_identifier(conn, lei, LEI_SCHEME)
    if link is None:
        return None
    cur = conn.execute(
        "SELECT id FROM entity_recordDetails_identifiers "
        "WHERE _link_entity_statement = ? AND scheme = ? LIMIT 1",
        (link, GB_COH_SCHEME),
    )
    row = cur.fetchone()
    return row[0] if row else None


# ---------------------------------------------------------------------
# Subgraph walk
# ---------------------------------------------------------------------


def walk_subgraph(
    conn: sqlite3.Connection, root_link: str, *, max_hops: int = 3
) -> tuple[set[str], set[str], set[str]]:
    """BFS over the relationship graph starting from a root entity.

    Returns ``(entity_links, person_links, relationship_links)`` where
    each set holds the SQLite ``_link`` primary keys of the matching
    rows. Persons are leaves — we don't walk past them (a person can't
    own a person in BODS).
    """
    entity_links: set[str] = {root_link}
    person_links: set[str] = set()
    relationship_links: set[str] = set()

    root_sid = get_entity_statementid(conn, root_link)
    if root_sid is None:
        return entity_links, person_links, relationship_links

    has_persons = has_table(conn, "person_statement")
    frontier: set[str] = {root_sid}

    for _hop in range(max_hops):
        if not frontier:
            break
        next_frontier: set[str] = set()
        # Fetch every relationship touching the frontier, in either
        # direction. Indexed by the script bootstrap so this is fast.
        params = list(frontier) + list(frontier)
        placeholders = ",".join("?" for _ in frontier)
        cur = conn.execute(
            f"""
            SELECT _link, recorddetails_subject, recorddetails_interestedparty
            FROM relationship_statement
            WHERE recorddetails_subject IN ({placeholders})
               OR recorddetails_interestedparty IN ({placeholders})
            """,
            params,
        )
        for rel_link, subject, ip in cur.fetchall():
            if rel_link in relationship_links:
                continue
            relationship_links.add(rel_link)
            for other in (subject, ip):
                if not other or other in frontier:
                    continue
                # Try entity first.
                e = conn.execute(
                    "SELECT _link FROM entity_statement WHERE statementid = ?",
                    (other,),
                ).fetchone()
                if e is not None:
                    if e[0] not in entity_links:
                        entity_links.add(e[0])
                        next_frontier.add(other)
                    continue
                # Fall through to person (only UK PSC).
                if has_persons:
                    p = conn.execute(
                        "SELECT _link FROM person_statement WHERE statementid = ?",
                        (other,),
                    ).fetchone()
                    if p is not None:
                        person_links.add(p[0])
        frontier = next_frontier

    return entity_links, person_links, relationship_links


# ---------------------------------------------------------------------
# BODS reconstruction
# ---------------------------------------------------------------------


def _row_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {k: row[k] for k in row.keys()}


def _publication_block(row: dict[str, Any]) -> dict[str, Any]:
    pub: dict[str, Any] = {}
    for src, dst in (
        ("publicationdetails_publicationdate", "publicationDate"),
        ("publicationdetails_bodsversion", "bodsVersion"),
        ("publicationdetails_license", "license"),
    ):
        v = row.get(src)
        if v is None or v == "":
            continue
        # SQLite is loose about column types — ``bodsversion`` in
        # particular can come back as a REAL (e.g. 0.4) when the
        # importer parsed the JSON number directly. BODS specifies it
        # as a string, and downstream consumers (notably bods-dagre's
        # compare-versions check) throw ``TypeError: Invalid argument
        # expected string`` if they see anything else. Coerce all
        # publication-detail strings up front.
        pub[dst] = str(v)
    publisher: dict[str, str] = {}
    if row.get("publicationdetails_publisher_name"):
        publisher["name"] = str(row["publicationdetails_publisher_name"])
    if row.get("publicationdetails_publisher_url"):
        publisher["url"] = str(row["publicationdetails_publisher_url"])
    if publisher:
        pub["publisher"] = publisher
    return pub


def _source_block(row: dict[str, Any]) -> dict[str, Any]:
    src: dict[str, Any] = {}
    if row.get("source_type"):
        src["type"] = row["source_type"]
    if row.get("source_url"):
        src["url"] = row["source_url"]
    return src


def reconstruct_entity_statement(
    conn: sqlite3.Connection, link: str
) -> dict[str, Any]:
    e = _row_dict(
        conn.execute("SELECT * FROM entity_statement WHERE _link = ?", (link,)).fetchone()
    )

    identifiers = [
        {
            **({"id": r["id"]} if r["id"] else {}),
            **({"scheme": r["scheme"]} if r["scheme"] else {}),
            **({"schemeName": r["schemename"]} if r["schemename"] else {}),
            **({"uri": r["uri"]} if r["uri"] else {}),
        }
        for r in conn.execute(
            "SELECT id, scheme, schemename, uri FROM entity_recordDetails_identifiers "
            "WHERE _link_entity_statement = ?",
            (link,),
        ).fetchall()
        if r["id"]
    ]

    addresses = [
        {
            **({"type": r["type"]} if r["type"] else {}),
            **({"address": r["address"]} if r["address"] else {}),
            **({"postCode": r["postcode"]} if r["postcode"] else {}),
            **(
                {"country": r["country_code"]}
                if r["country_code"]
                else ({"country": r["country_name"]} if r["country_name"] else {})
            ),
        }
        for r in conn.execute(
            "SELECT type, address, postcode, country_name, country_code "
            "FROM entity_recordDetails_addresses "
            "WHERE _link_entity_statement = ?",
            (link,),
        ).fetchall()
    ]

    rd: dict[str, Any] = {
        "entityType": {
            "type": e.get("recorddetails_entitytype_type") or "registeredEntity",
            **(
                {"details": e["recorddetails_entitytype_details"]}
                if e.get("recorddetails_entitytype_details")
                else {}
            ),
        },
        "name": e.get("recorddetails_name") or "",
    }
    if e.get("recorddetails_jurisdiction_code") or e.get("recorddetails_jurisdiction_name"):
        rd["incorporatedInJurisdiction"] = {
            "name": e.get("recorddetails_jurisdiction_name")
            or e.get("recorddetails_jurisdiction_code"),
            "code": e.get("recorddetails_jurisdiction_code") or "",
        }
    if e.get("recorddetails_foundingdate"):
        rd["foundingDate"] = e["recorddetails_foundingdate"]
    if e.get("recorddetails_dissolutiondate"):
        rd["dissolutionDate"] = e["recorddetails_dissolutiondate"]
    if identifiers:
        rd["identifiers"] = identifiers
    if addresses:
        rd["addresses"] = addresses

    return {
        "statementId": e["statementid"],
        **({"recordId": e["recordid"]} if e.get("recordid") else {}),
        "recordType": "entity",
        "recordStatus": e.get("recordstatus") or "new",
        **({"statementDate": e["statementdate"]} if e.get("statementdate") else {}),
        "recordDetails": rd,
        **({"publicationDetails": _publication_block(e)} if _publication_block(e) else {}),
        **({"source": _source_block(e)} if _source_block(e) else {}),
    }


def reconstruct_person_statement(
    conn: sqlite3.Connection, link: str
) -> dict[str, Any]:
    p = _row_dict(
        conn.execute("SELECT * FROM person_statement WHERE _link = ?", (link,)).fetchone()
    )

    names = [
        {
            **({"type": r["type"]} if r["type"] else {}),
            **({"fullName": r["fullname"]} if r["fullname"] else {}),
            **({"familyName": r["familyname"]} if r["familyname"] else {}),
            **({"givenName": r["givenname"]} if r["givenname"] else {}),
        }
        for r in conn.execute(
            "SELECT type, fullname, familyname, givenname "
            "FROM person_recordDetails_names "
            "WHERE _link_person_statement = ?",
            (link,),
        ).fetchall()
    ]

    nationalities = [
        {
            **({"name": r["name"]} if r["name"] else {}),
            **({"code": r["code"]} if r["code"] else {}),
        }
        for r in conn.execute(
            "SELECT name, code FROM person_recordDetails_nationalities "
            "WHERE _link_person_statement = ?",
            (link,),
        ).fetchall()
        if r["name"] or r["code"]
    ]

    rd: dict[str, Any] = {
        "personType": p.get("recorddetails_persontype") or "knownPerson",
    }
    if names:
        rd["names"] = names
    if nationalities:
        rd["nationalities"] = nationalities
    if p.get("recorddetails_birthdate"):
        rd["birthDate"] = p["recorddetails_birthdate"]

    return {
        "statementId": p["statementid"],
        **({"recordId": p["recordid"]} if p.get("recordid") else {}),
        "recordType": "person",
        "recordStatus": p.get("recordstatus") or "new",
        **({"statementDate": p["statementdate"]} if p.get("statementdate") else {}),
        "recordDetails": rd,
        **({"publicationDetails": _publication_block(p)} if _publication_block(p) else {}),
        **({"source": _source_block(p)} if _source_block(p) else {}),
    }


def reconstruct_relationship_statement(
    conn: sqlite3.Connection, link: str
) -> dict[str, Any]:
    r = _row_dict(
        conn.execute(
            "SELECT * FROM relationship_statement WHERE _link = ?", (link,)
        ).fetchone()
    )

    interests = []
    for i in conn.execute(
        "SELECT * FROM relationship_recordDetails_interests "
        "WHERE _link_relationship_statement = ?",
        (link,),
    ).fetchall():
        interest: dict[str, Any] = {}
        if i["type"]:
            interest["type"] = i["type"]
        if i["directorindirect"]:
            interest["directOrIndirect"] = i["directorindirect"]
        if i["beneficialownershiporcontrol"] is not None:
            interest["beneficialOwnershipOrControl"] = bool(
                i["beneficialownershiporcontrol"]
            )
        if i["details"]:
            interest["details"] = i["details"]
        if i["startdate"]:
            interest["startDate"] = i["startdate"]
        # share_minimum / share_maximum / enddate exist on UK only
        for col, key in (
            ("share_minimum", "share"),
            ("enddate", "endDate"),
        ):
            try:
                v = i[col]
            except (IndexError, KeyError):
                continue
            if v is None:
                continue
            if col == "share_minimum":
                share = {"minimum": v}
                try:
                    if i["share_maximum"] is not None:
                        share["maximum"] = i["share_maximum"]
                except (IndexError, KeyError):
                    pass
                interest["share"] = share
            else:
                interest[key] = v
        if interest:
            interests.append(interest)

    rd: dict[str, Any] = {
        "subject": {"describedByEntityStatement": r["recorddetails_subject"]},
    }
    # interestedParty can be entity, person, or anonymous/unknown by reason.
    if r.get("recorddetails_interestedparty"):
        # We don't always know if the target is an entity or person from
        # this row alone. Mark as describedByEntityStatement; consumers
        # can re-resolve. The runtime loader patches this when it sees
        # the target appears in person_statement.
        rd["interestedParty"] = {
            "describedByEntityStatement": r["recorddetails_interestedparty"]
        }
    elif r.get("recorddetails_interestedparty_reason"):
        rd["interestedParty"] = {
            "unspecifiedReason": r["recorddetails_interestedparty_reason"],
            **(
                {"description": r["recorddetails_interestedparty_description"]}
                if r.get("recorddetails_interestedparty_description")
                else {}
            ),
        }

    if interests:
        rd["interests"] = interests

    return {
        "statementId": r["statementid"],
        **({"recordId": r["recordid"]} if r.get("recordid") else {}),
        "recordType": "relationship",
        "recordStatus": r.get("recordstatus") or "new",
        **({"statementDate": r["statementdate"]} if r.get("statementdate") else {}),
        "recordDetails": rd,
        **({"publicationDetails": _publication_block(r)} if _publication_block(r) else {}),
        **({"source": _source_block(r)} if _source_block(r) else {}),
    }


def patch_relationship_targets(
    conn: sqlite3.Connection, statements: list[dict[str, Any]]
) -> None:
    """Resolve every relationship's interestedParty to entity vs person.

    The reconstruction step defaults to ``describedByEntityStatement``
    because it's a simple column read. Here we look at the actual
    target's type and rewrite to ``describedByPersonStatement`` where
    appropriate.
    """
    has_persons = has_table(conn, "person_statement")
    if not has_persons:
        return
    for stmt in statements:
        if stmt.get("recordType") != "relationship":
            continue
        ip = stmt["recordDetails"].get("interestedParty") or {}
        target = ip.get("describedByEntityStatement")
        if not target:
            continue
        is_person = conn.execute(
            "SELECT 1 FROM person_statement WHERE statementid = ? LIMIT 1",
            (target,),
        ).fetchone()
        if is_person:
            stmt["recordDetails"]["interestedParty"] = {
                "describedByPersonStatement": target
            }


# ---------------------------------------------------------------------
# Top-level extraction
# ---------------------------------------------------------------------


def extract_for_root(
    conn: sqlite3.Connection, root_link: str, *, max_hops: int
) -> list[dict[str, Any]]:
    e_links, p_links, r_links = walk_subgraph(conn, root_link, max_hops=max_hops)
    statements: list[dict[str, Any]] = []
    for link in sorted(e_links):
        statements.append(reconstruct_entity_statement(conn, link))
    for link in sorted(p_links):
        statements.append(reconstruct_person_statement(conn, link))
    for link in sorted(r_links):
        statements.append(reconstruct_relationship_statement(conn, link))
    patch_relationship_targets(conn, statements)
    return statements


def write_jsonl(path: Path, statements: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for stmt in statements:
            fh.write(json.dumps(stmt, separators=(",", ":")) + "\n")


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Extract per-LEI BODS subgraphs from Open Ownership SQLite dumps."
    )
    parser.add_argument(
        "--gleif",
        required=True,
        type=Path,
        help="Path to gleif_version_0_4.db",
    )
    parser.add_argument(
        "--uk",
        type=Path,
        help="Path to uk_version_0_4.db. Optional — when omitted, only "
        "GLEIF subgraphs are extracted.",
    )
    parser.add_argument(
        "--leis",
        nargs="+",
        required=True,
        help="One or more 20-character LEIs.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[2] / "data" / "cache" / "bods_data",
        help="Output root (default: data/cache/bods_data/).",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=3,
        help="Maximum BFS depth from each LEI (default 3).",
    )
    args = parser.parse_args(argv)

    if not args.gleif.is_file():
        print(f"GLEIF db not found: {args.gleif}", file=sys.stderr)
        return 1
    if args.uk and not args.uk.is_file():
        print(f"UK PSC db not found: {args.uk}", file=sys.stderr)
        return 1

    print(f"Opening GLEIF db: {args.gleif}")
    gleif_conn = sqlite3.connect(args.gleif)
    gleif_conn.row_factory = sqlite3.Row
    print("  Ensuring indexes (one-off, may take minutes on first run)…")
    ensure_indexes(gleif_conn, has_persons=False)

    uk_conn: sqlite3.Connection | None = None
    if args.uk:
        print(f"Opening UK PSC db: {args.uk}")
        uk_conn = sqlite3.connect(args.uk)
        uk_conn.row_factory = sqlite3.Row
        print("  Ensuring indexes (one-off, may take minutes on first run)…")
        ensure_indexes(uk_conn, has_persons=True)

    for lei in args.leis:
        lei = lei.strip().upper()
        print(f"\n=== {lei} ===")

        # GLEIF subgraph.
        gleif_link = find_entity_link_by_identifier(gleif_conn, lei, LEI_SCHEME)
        if gleif_link is None:
            print("  GLEIF: no entity statement found")
        else:
            statements = extract_for_root(
                gleif_conn, gleif_link, max_hops=args.max_hops
            )
            out_path = args.output / "gleif" / f"{lei}.jsonl"
            write_jsonl(out_path, statements)
            print(
                f"  GLEIF: {len(statements)} statements → {out_path.relative_to(args.output.parent.parent.parent)}"
            )

        # UK PSC subgraph (only if jurisdiction = GB).
        if uk_conn is None:
            continue
        gb_coh = (
            get_gb_coh_for_lei(gleif_conn, lei) if gleif_link is not None else None
        )
        if not gb_coh:
            print("  UK PSC: skipped (no GB-COH on GLEIF record)")
            continue
        uk_link = find_entity_link_by_identifier(uk_conn, gb_coh, GB_COH_SCHEME)
        if uk_link is None:
            print(f"  UK PSC: no entity statement for GB-COH {gb_coh}")
            continue
        statements = extract_for_root(uk_conn, uk_link, max_hops=args.max_hops)
        out_path = args.output / "uk" / f"{gb_coh}.jsonl"
        write_jsonl(out_path, statements)
        print(
            f"  UK PSC: {len(statements)} statements → {out_path.relative_to(args.output.parent.parent.parent)}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
