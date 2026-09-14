"""Build the Ukraine ЄДР SQLite index from the data.gov.ua bulk XML export.

Why a bulk index and not an API
-------------------------------
There is no government API for the ЄДР. The Ministry of Justice publishes a
weekly ZIP of XML on data.gov.ua and that is the only official channel.

    Dataset: https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7
    UO.zip   327 MB  →  UO.xml  3.16 GB  →  2,017,706 legal entities
    Licence: CC BY 4.0, Ministry of Justice of Ukraine

Usage
-----
    curl -L -o UO.zip "https://data.gov.ua/dataset/03cc1239-3988-4451-aa0d-aadb77448714/resource/d40cc921-39bb-44fd-be06-dc02589f45c6/download/uo.zip"
    python scripts/build_edr_ukraine_index.py --zip UO.zip --out edr_ukraine.sqlite

Then point the adapter at it:

    EDR_UKRAINE_DB_FILE=/abs/path/to/edr_ukraine.sqlite

Tiers
-----
``--tier graph`` (the default) keeps entities that take part in an ownership
graph: those with a beneficial-ownership record (a named owner OR a filed
statement that there is none), a corporate founder, or a state authority —
plus any entity another company's founder list points at, so no edge dangles.
``--tier full`` keeps all 2,017,706, for offline analysis.

Sizes are measured against the 8 Sep 2026 export, not estimated. See the
counts the script prints at the end; the graph tier is roughly 800k entities
and about 1 GB, which is an offline artefact, not something to download on a
web host's cold start.

Traps this script exists to absorb
----------------------------------
* The XML declares **windows-1251**. ``iterparse`` honours the declaration —
  do not decode the bytes yourself.
* **EDRPOU is not a primary key.** 24,885 codes appear more than once and 150
  records carry a blank code. The live (``зареєстровано``) record wins.
  Resolving that needs lookahead — the live record is often the *second* of a
  pair — so the file is read **twice**: once to decide which occurrence of each
  code wins, once to write it. Deleting superseded rows in a single pass was
  the first design and it degrades badly: the deletes run before the indexes
  exist, so each of the ~12k supersessions scans five growing tables.
* Values are parsed by the adapter's own functions, imported here rather than
  reimplemented, so the index and the adapter can never drift on the grammar.
  Raw strings are kept alongside the parsed columns because BODS carries the
  register's own wording verbatim in ``description``.
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import sqlite3
import sys
import time
import zipfile
from pathlib import Path
from xml.etree.ElementTree import iterparse

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.edr_ukraine import (  # noqa: E402
    ACTIVE_STAN,
    normalise_edrpou,
    parse_beneficiary,
    parse_capital,
    parse_founder,
    parse_role_holder,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_edr_ukraine_index")

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE entity (
  edrpou TEXT PRIMARY KEY,
  name TEXT, short_name TEXT, opf TEXT, stan TEXT,
  active INTEGER, capital REAL, registration TEXT
);
CREATE TABLE beneficiary (
  edrpou TEXT, seq INTEGER, kind TEXT, name TEXT, citizenship TEXT,
  influence TEXT, pct_direct REAL, pct_indirect REAL, note TEXT,
  reason_text TEXT, raw TEXT
);
CREATE TABLE founder (
  edrpou TEXT, seq INTEGER, name TEXT, code TEXT, citizenship TEXT,
  amount_uah REAL
);
CREATE TABLE signer (edrpou TEXT, seq INTEGER, name TEXT, role TEXT);
CREATE TABLE member (edrpou TEXT, seq INTEGER, name TEXT, role TEXT);
CREATE TABLE executive_power (edrpou TEXT, seq INTEGER, name TEXT, code TEXT);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""

INDEXES = """
CREATE INDEX ix_ben ON beneficiary(edrpou);
CREATE INDEX ix_fnd ON founder(edrpou);
CREATE INDEX ix_fnd_code ON founder(code);
CREATE INDEX ix_sgn ON signer(edrpou);
CREATE INDEX ix_mem ON member(edrpou);
CREATE INDEX ix_exec ON executive_power(edrpou);
CREATE INDEX ix_entity_name ON entity(name);
"""


def _text(elem, tag: str) -> str:
    return (elem.findtext(tag) or "").strip()


def _children(elem, path: str) -> list[str]:
    return [t for t in ((c.text or "").strip() for c in elem.findall(path)) if t]


def _subject_stream(zip_path: Path):
    """Yield each ``SUBJECT`` element from UO.xml inside the zip, then clear it."""
    zf = zipfile.ZipFile(str(zip_path))
    member_name = zf.namelist()[0]
    with zf.open(member_name) as raw_stream:
        stream = io.BufferedReader(raw_stream, buffer_size=1 << 22)
        for _, elem in iterparse(stream, events=("end",)):
            if elem.tag == "SUBJECT":
                yield elem
                elem.clear()


def resolve_winners(
    zip_path: Path,
) -> tuple[dict[str, int], set[str], int, int]:
    """Decide which occurrence of each EDRPOU is the one to keep.

    The live (``зареєстровано``) record wins; among equals the first wins. The
    live one is frequently the *second* of a pair, so this cannot be decided
    while writing — hence a cheap first pass that reads nothing but the code
    and the status.

    It also collects every EDRPOU named as a *corporate founder*, so the graph
    tier can keep the entities its edges point at even when those entities
    carry no ownership data of their own.

    Returns ``(winner_occurrence_by_code, edge_targets, duplicates, blanks)``.
    """
    winner: dict[str, int] = {}
    occurrence: dict[str, int] = {}
    won_live: set[str] = set()
    targets: set[str] = set()
    duplicates = 0
    blanks = 0
    for elem in _subject_stream(zip_path):
        for raw in _children(elem, "FOUNDERS/FOUNDER"):
            code = parse_founder(raw).get("code")
            if code:
                targets.add(code)
        edrpou = normalise_edrpou(_text(elem, "EDRPOU"))
        if not edrpou:
            blanks += 1
            continue
        n = occurrence.get(edrpou, 0)
        occurrence[edrpou] = n + 1
        if n:
            duplicates += 1
        live = _text(elem, "STAN") == ACTIVE_STAN
        if edrpou not in winner:
            winner[edrpou] = n
            if live:
                won_live.add(edrpou)
        elif live and edrpou not in won_live:
            # A live record supersedes the terminated one already chosen.
            winner[edrpou] = n
            won_live.add(edrpou)
    return winner, targets, duplicates, blanks


def build(zip_path: Path, out_path: Path, tier: str) -> dict[str, int]:
    if out_path.exists():
        out_path.unlink()
    con = sqlite3.connect(str(out_path))
    con.executescript(SCHEMA)

    t0 = time.time()
    stats = {
        "subjects": 0,
        "blank_edrpou": 0,
        "duplicate_edrpou": 0,
        "entities": 0,
        "beneficiaries_named": 0,
        "beneficiaries_absent": 0,
        "beneficiaries_unparsed": 0,
        "founders": 0,
        "corporate_founders": 0,
        "signers": 0,
        "members": 0,
        "executive_power": 0,
    }
    log.info("pass 1/2: resolving duplicate EDRPOU codes")
    winner, edge_targets, duplicates, blanks = resolve_winners(zip_path)
    stats["duplicate_edrpou"] = duplicates
    stats["blank_edrpou"] = blanks
    log.info(
        "  %s distinct codes, %s duplicate rows, %s blank, "
        "%s corporate-founder edge targets (%.0fs)",
        f"{len(winner):,}", f"{duplicates:,}", f"{blanks:,}",
        f"{len(edge_targets):,}", time.time() - t0,
    )
    occurrence: dict[str, int] = {}

    ent: list[tuple] = []
    ben: list[tuple] = []
    fnd: list[tuple] = []
    sgn: list[tuple] = []
    mem: list[tuple] = []
    exe: list[tuple] = []

    def flush() -> None:
        con.executemany(
            "INSERT OR REPLACE INTO entity VALUES (?,?,?,?,?,?,?,?)", ent
        )
        con.executemany(
            "INSERT INTO beneficiary VALUES (?,?,?,?,?,?,?,?,?,?,?)", ben
        )
        con.executemany("INSERT INTO founder VALUES (?,?,?,?,?,?)", fnd)
        con.executemany("INSERT INTO signer VALUES (?,?,?,?)", sgn)
        con.executemany("INSERT INTO member VALUES (?,?,?,?)", mem)
        con.executemany("INSERT INTO executive_power VALUES (?,?,?,?)", exe)
        for batch in (ent, ben, fnd, sgn, mem, exe):
            batch.clear()

    log.info("pass 2/2: writing the index")
    for elem in _subject_stream(zip_path):
            stats["subjects"] += 1
            edrpou = normalise_edrpou(_text(elem, "EDRPOU"))
            if not edrpou:
                continue

            # Only the occurrence pass 1 chose is written — no row is ever
            # inserted and then taken back.
            n = occurrence.get(edrpou, 0)
            occurrence[edrpou] = n + 1
            if winner.get(edrpou) != n:
                continue

            stan = _text(elem, "STAN")
            active = stan == ACTIVE_STAN

            bens = [parse_beneficiary(v) for v in _children(elem, "BENEFICIARIES/BENEFICIARY")]
            fnds = [parse_founder(v) for v in _children(elem, "FOUNDERS/FOUNDER")]
            sgns = [parse_role_holder(v) for v in _children(elem, "SIGNERS/SIGNER")]
            mems = [parse_role_holder(v) for v in _children(elem, "MEMBERS/MEMBER")]

            ep = elem.find("EXECUTIVE_POWER")
            ep_name = _text(ep, "NAME") if ep is not None else ""
            ep_code = normalise_edrpou(_text(ep, "CODE")) if ep is not None else ""

            # An entity whose only beneficial-ownership record is a filed
            # ABSENCE still makes a beneficial-ownership declaration — "no
            # qualifying person exists", with a stated reason. Excluding those
            # would drop 88,247 declarations and keep only the half of the
            # register that names somebody.
            has_bo_record = any(b["kind"] in ("named", "absence") for b in bens)
            corporate = [f for f in fnds if f.get("code")]

            # Keep an entity that takes part in an ownership graph — by having
            # a beneficial owner, a corporate founder or a state authority —
            # and any entity another company's founder list points AT, so no
            # edge dangles. Terminated companies are dropped unless something
            # still points at them.
            keep = tier == "full" or edrpou in edge_targets or (
                active and (has_bo_record or corporate or ep_name)
            )
            if not keep:
                continue

            stats["entities"] += 1
            ent.append(
                (
                    edrpou,
                    _text(elem, "NAME"),
                    _text(elem, "SHORT_NAME"),
                    _text(elem, "OPF"),
                    stan,
                    1 if active else 0,
                    parse_capital(_text(elem, "AUTHORIZED_CAPITAL")),
                    _text(elem, "REGISTRATION"),
                )
            )
            for i, b in enumerate(bens):
                if b["kind"] == "named":
                    stats["beneficiaries_named"] += 1
                elif b["kind"] == "absence":
                    stats["beneficiaries_absent"] += 1
                else:
                    stats["beneficiaries_unparsed"] += 1
                ben.append(
                    (
                        edrpou, i, b["kind"], b.get("name"), b.get("citizenship"),
                        b.get("influence"), b.get("pct_direct"), b.get("pct_indirect"),
                        b.get("note"), b.get("reason_text"), b.get("raw"),
                    )
                )
            for i, f in enumerate(fnds):
                stats["founders"] += 1
                if f.get("code"):
                    stats["corporate_founders"] += 1
                fnd.append(
                    (edrpou, i, f["name"], f.get("code"), f.get("citizenship"),
                     f.get("amount_uah"))
                )
            for i, s in enumerate(sgns):
                stats["signers"] += 1
                sgn.append((edrpou, i, s["name"], s.get("role")))
            for i, m in enumerate(mems):
                stats["members"] += 1
                mem.append((edrpou, i, m["name"], m.get("role")))
            if ep_name:
                stats["executive_power"] += 1
                exe.append((edrpou, 0, ep_name, ep_code or None))

            if stats["subjects"] % 200_000 == 0:
                flush()
                con.commit()
                log.info(
                    "  %s subjects → %s kept (%.0fs)",
                    f"{stats['subjects']:,}", f"{stats['entities']:,}",
                    time.time() - t0,
                )

    flush()
    con.commit()

    con.executescript(INDEXES)
    con.executemany(
        "INSERT OR REPLACE INTO meta VALUES (?,?)",
        [
            ("source", "data.gov.ua ЄДР UO.zip"),
            ("source_file", zip_path.name),
            ("tier", tier),
            ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            ("licence", "CC-BY-4.0 — Ministry of Justice of Ukraine"),
            *[(f"count_{k}", str(v)) for k, v in stats.items()],
        ],
    )
    con.commit()
    con.execute("VACUUM")
    con.close()

    stats["seconds"] = int(time.time() - t0)
    stats["megabytes"] = int(os.path.getsize(out_path) / 1e6)
    return stats


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--zip", required=True, type=Path, help="path to UO.zip")
    ap.add_argument("--out", required=True, type=Path, help="SQLite index to write")
    ap.add_argument(
        "--tier",
        choices=("graph", "full"),
        default="graph",
        help="graph = ownership-bearing entities only (default); full = all 2.0M",
    )
    args = ap.parse_args()
    if not args.zip.exists():
        raise SystemExit(f"not found: {args.zip}")

    stats = build(args.zip, args.out, args.tier)
    log.info("wrote %s (%s MB, %ss)", args.out, stats["megabytes"], stats["seconds"])
    for key in (
        "subjects", "entities", "duplicate_edrpou", "blank_edrpou",
        "beneficiaries_named", "beneficiaries_absent", "beneficiaries_unparsed",
        "founders", "corporate_founders", "signers", "members", "executive_power",
    ):
        log.info("  %-24s %s", key, f"{stats[key]:,}")
    if stats["beneficiaries_unparsed"]:
        log.warning(
            "  %s beneficiary strings could not be parsed — expected 1 in the "
            "8 Sep 2026 export; a jump means the grammar has changed",
            f"{stats['beneficiaries_unparsed']:,}",
        )


if __name__ == "__main__":
    main()
