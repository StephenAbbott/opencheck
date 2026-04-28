"""Diagnose why the relationship walk in ``extract_bods_subgraphs.py``
returned no relationships for the curated LEIs.

Hypothesis: Open Ownership's processed BODS dataset stores multiple
entity-statement versions per company (one per snapshot), and the
relationship statements point at versions other than the one our
``LIMIT 1`` lookup happens to pick.

Run this against the same local SQLite file you used for extraction:

    python scripts/diagnose_bods_walk.py --uk /path/to/uk_version_0_4.db

It prints, per LEI / GB-COH:

* How many entity_statement rows match the identifier
* The list of distinct statementIds those rows carry
* How many relationships point at any of those statementIds (subject)
* How many point at any of those statementIds (interestedParty)
* A sample of related-party statementIds + their recordtypes

That tells us whether the data has the relationships we expect, and
whether they all reference one canonical statementId or many.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


SUBJECTS = [
    # (description, GB-COH or "" if not GB)
    ("Daily Mail and General Trust", "00184594"),
    ("BP P.L.C.", "00102498"),
    ("Bank Saderat PLC", "01126618"),
    ("BIFFA PLC", "10336040"),
    ("Hornsea 1 Limited", "07640868"),
    ("Care UK Social Care", "07068789"),
    ("Taqa Bratani Limited", "00031014"),
    ("Newcastle United FC", "04152338"),
    ("Melli Bank PLC", "05975475"),
]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--uk", required=True, type=Path)
    args = parser.parse_args(argv)

    if not args.uk.is_file():
        print(f"db not found: {args.uk}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(args.uk)
    conn.row_factory = sqlite3.Row

    for label, coh in SUBJECTS:
        if not coh:
            continue
        print(f"\n=== {label}  GB-COH={coh} ===")

        # All entity_statements with this GB-COH
        rows = conn.execute(
            "SELECT _link_entity_statement FROM entity_recordDetails_identifiers "
            "WHERE id = ? AND scheme = ?",
            (coh, "GB-COH"),
        ).fetchall()
        print(f"  entity_recordDetails_identifiers rows matching: {len(rows)}")

        if not rows:
            continue
        links = [r[0] for r in rows]

        # Pull all matching entity_statements
        placeholders = ",".join("?" for _ in links)
        sids = [
            r[0]
            for r in conn.execute(
                f"SELECT statementid FROM entity_statement WHERE _link IN ({placeholders})",
                links,
            ).fetchall()
        ]
        sids = list(set(sids))
        print(f"  distinct entity_statement.statementid: {len(sids)}")
        for s in sids[:5]:
            print(f"    - {s}")
        if len(sids) > 5:
            print(f"    … +{len(sids) - 5} more")

        if not sids:
            continue
        ph = ",".join("?" for _ in sids)

        n_subj = conn.execute(
            f"SELECT COUNT(*) FROM relationship_statement "
            f"WHERE recorddetails_subject IN ({ph})",
            sids,
        ).fetchone()[0]
        n_ip = conn.execute(
            f"SELECT COUNT(*) FROM relationship_statement "
            f"WHERE recorddetails_interestedparty IN ({ph})",
            sids,
        ).fetchone()[0]
        print(f"  relationships where this is subject: {n_subj}")
        print(f"  relationships where this is interestedParty: {n_ip}")

        # If there ARE relationships, sample a few of the OTHER endpoints
        if n_subj > 0:
            rows = conn.execute(
                f"SELECT recorddetails_interestedparty FROM relationship_statement "
                f"WHERE recorddetails_subject IN ({ph}) LIMIT 5",
                sids,
            ).fetchall()
            print("  sample interestedParty statementIds (subject side):")
            for r in rows:
                target = r[0] or "<unspecified>"
                kind = _classify(conn, target) if r[0] else "(no target — unspecified reason)"
                print(f"    - {target}  [{kind}]")
        if n_ip > 0:
            rows = conn.execute(
                f"SELECT recorddetails_subject FROM relationship_statement "
                f"WHERE recorddetails_interestedparty IN ({ph}) LIMIT 5",
                sids,
            ).fetchall()
            print("  sample subject statementIds (interestedParty side):")
            for r in rows:
                target = r[0] or "<null>"
                kind = _classify(conn, target) if r[0] else ""
                print(f"    - {target}  [{kind}]")

    return 0


def _classify(conn: sqlite3.Connection, sid: str) -> str:
    """Tell us whether a given statementId is in entity_, person_, or
    neither table."""
    if conn.execute(
        "SELECT 1 FROM entity_statement WHERE statementid = ? LIMIT 1", (sid,)
    ).fetchone():
        return "entity"
    if conn.execute(
        "SELECT 1 FROM person_statement WHERE statementid = ? LIMIT 1", (sid,)
    ).fetchone():
        return "person"
    return "??? not in entity or person tables"


if __name__ == "__main__":
    sys.exit(main())
