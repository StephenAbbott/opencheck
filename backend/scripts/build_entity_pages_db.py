"""Build (or monthly-refresh) entity_pages.sqlite from GLEIF Golden Copy files.

The database behind the SEO entity pages (``opencheck/entity_pages.py``).
Two modes:

* ``--full`` (default) — download the latest LEI2 + RR + REPEX Golden Copy
  **full** files (~480MB + ~23MB + ~59MB zipped CSV; ~3.4M LEI records,
  ~490k relationship records, ~6.3M reporting exceptions) and rebuild from
  scratch.
* ``--delta {LastMonth,LastWeek,LastDay,IntraDay}`` — download the matching
  delta files and upsert into an existing DB. ``--delta LastMonth`` is the
  monthly refresh: GLEIF's 31-day delta carries every new LEI issued and
  every record revised in the preceding month.

File discovery uses GLEIF's Golden Copy publish API
(``https://goldencopy.gleif.org/api/v2/golden-copies/publishes``) — free, no
registration, new publishes at 02:00/10:00/18:00 UTC daily. Offline / test
use: pass ``--lei2-file`` and ``--rr-file`` pointing at local CSVs (zipped
or plain) to skip the network entirely.

The build is streaming (csv → executemany batches), so memory stays flat
regardless of file size. A full build writes to a temp file and renames at
the end — an interrupted run never clobbers the live DB.

Phase 178 turned the file into a GLEIF mirror (see ``entity_pages.py``): each
entity row also carries ``detail_json`` — every Level 1 field the BODS mapper
reads — and the RR and REPEX files land in their own tables. The page columns
are unchanged, so entity pages, sitemaps and the browse hubs render exactly as
before. Applying a delta to a v1 file upgrades it in place.

The detail is loaded as JSON text and then, in ``compress_detail_column``,
deflated row-by-row against a dictionary sampled from the whole file and stored
in ``meta`` (697 → 149 bytes a row on the 2026-09-07 publish; the file goes
from 4.8 GB to 1.8 GB after the ``VACUUM`` a full build ends with). A delta
leaves its rows as text and the same pass compresses them with the dictionary
the file already carries. A full build of that publish took 12 minutes end to
end on a 2-vCPU container (downloads included; the compression pass is about
a quarter of it), and ``gzip -9`` of the result is 879 MB.

Usage (from backend/):
    uv run python scripts/build_entity_pages_db.py --out data/entity_pages.sqlite
    uv run python scripts/build_entity_pages_db.py --out data/entity_pages.sqlite --delta LastMonth
    uv run python scripts/build_entity_pages_db.py --out /tmp/sample.sqlite --sample 50000
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sqlite3
import sys
import tempfile
import zipfile
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from opencheck.entity_pages import (  # noqa: E402
    DETAIL_ZDICT_META_KEY,
    EXCEPTION_CATEGORIES,
    RR_STANDING_REGISTRATION_STATUSES,
    SCHEMA,
    SCHEMA_V2_ENTITY_COLUMNS,
    SCHEMA_VERSION,
    build_detail_zdict,
    compress_detail,
    decode_zdict,
    encode_zdict,
    slugify_name,
)

PUBLISHES_API = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes"

# Exact LEI2 CSV column names (LEI_3.1 CDF; verified against a live delta
# file 2026-08-03). The reader is name-based, so column reordering upstream
# is harmless; a renamed column fails loudly in _require().
COL_LEI = "LEI"
COL_NAME = "Entity.LegalName"
COL_TRANSLIT = "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.1"
COL_CITY = "Entity.LegalAddress.City"
COL_REGION = "Entity.LegalAddress.Region"
COL_COUNTRY = "Entity.LegalAddress.Country"
COL_JURISDICTION = "Entity.LegalJurisdiction"
COL_STATUS = "Entity.EntityStatus"
COL_LEGAL_FORM = "Entity.LegalForm.EntityLegalFormCode"
COL_SUCCESSOR = "Entity.SuccessorEntity.1.SuccessorLEI"
COL_FIRST_REG = "Registration.InitialRegistrationDate"
COL_LAST_UPDATE = "Registration.LastUpdateDate"
COL_REG_STATUS = "Registration.RegistrationStatus"

# Phase 178: the Level 1 detail columns (all optional in the reader — an older
# CDF without one of them just yields a record without that field).
COL_NAME_LANG = "Entity.LegalName.xmllang"
COL_OTHER_NAME = "Entity.OtherEntityNames.OtherEntityName.{n}"
COL_OTHER_NAME_LANG = "Entity.OtherEntityNames.OtherEntityName.{n}.xmllang"
COL_OTHER_NAME_TYPE = "Entity.OtherEntityNames.OtherEntityName.{n}.type"
COL_TRANSLIT_N = "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.{n}"
COL_TRANSLIT_LANG = (
    "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.{n}.xmllang"
)
COL_TRANSLIT_TYPE = (
    "Entity.TransliteratedOtherEntityNames.TransliteratedOtherEntityName.{n}.type"
)
COL_RA_ID = "Entity.RegistrationAuthority.RegistrationAuthorityID"
COL_RA_OTHER = "Entity.RegistrationAuthority.OtherRegistrationAuthorityID"
COL_RA_ENTITY_ID = "Entity.RegistrationAuthority.RegistrationAuthorityEntityID"
COL_CATEGORY = "Entity.EntityCategory"
COL_SUBCATEGORY = "Entity.EntitySubCategory"
COL_LEGAL_FORM_OTHER = "Entity.LegalForm.OtherLegalForm"
COL_CREATION = "Entity.EntityCreationDate"
COL_EXPIRATION_DATE = "Entity.EntityExpirationDate"
COL_EXPIRATION_REASON = "Entity.EntityExpirationReason"
COL_SUCCESSOR_N = "Entity.SuccessorEntity.{n}.SuccessorLEI"
COL_SUCCESSOR_NAME_N = "Entity.SuccessorEntity.{n}.SuccessorEntityName"
COL_NEXT_RENEWAL = "Registration.NextRenewalDate"
COL_MANAGING_LOU = "Registration.ManagingLOU"
COL_VALIDATION_SOURCES = "Registration.ValidationSources"
COL_VA_ID = "Registration.ValidationAuthority.ValidationAuthorityID"
COL_VA_OTHER = "Registration.ValidationAuthority.OtherValidationAuthorityID"
COL_VA_ENTITY_ID = "Registration.ValidationAuthority.ValidationAuthorityEntityID"
COL_OVA_ID = (
    "Registration.OtherValidationAuthorities.OtherValidationAuthority.{n}.ValidationAuthorityID"
)
COL_OVA_OTHER = (
    "Registration.OtherValidationAuthorities.OtherValidationAuthority.{n}"
    ".OtherValidationAuthorityID"
)
COL_OVA_ENTITY_ID = (
    "Registration.OtherValidationAuthorities.OtherValidationAuthority.{n}"
    ".ValidationAuthorityEntityID"
)
COL_CONFORMITY = "ConformityFlag"
# The two address blocks share a column layout under different prefixes.
ADDR_LEGAL = "Entity.LegalAddress"
ADDR_HQ = "Entity.HeadquartersAddress"
# ``addressLines`` in the live API is the first line plus the additional
# lines; the number, the number within the building and the mail routing are
# their own keys there, and ``map_gleif`` reads only ``addressLines`` — so
# folding them in would put text in the BODS address that the live record
# does not (a live parity sweep across 30 LEIs caught a Spanish record whose
# ``addressNumber`` repeated half the street line).
ADDR_LINE_COLS = (
    "FirstAddressLine",
    "AdditionalAddressLine.1",
    "AdditionalAddressLine.2",
    "AdditionalAddressLine.3",
)
ADDR_EXTRA_COLS = (
    ("AddressNumber", "addressNumber"),
    ("AddressNumberWithinBuilding", "addressNumberWithinBuilding"),
    ("MailRouting", "mailRouting"),
)
# GLEIF allows five repeats of every repeating group in the CSV.
_REPEATS = range(1, 6)

RR_START = "Relationship.StartNode.NodeID"       # the child
RR_END = "Relationship.EndNode.NodeID"           # the parent
RR_TYPE = "Relationship.RelationshipType"
RR_STATUS = "Relationship.RelationshipStatus"
RR_REG_STATUS = "Registration.RegistrationStatus"
RR_LAST_UPDATE = "Registration.LastUpdateDate"
RR_PERIOD_START = "Relationship.Period.{n}.startDate"
RR_PERIOD_END = "Relationship.Period.{n}.endDate"
RR_PERIOD_TYPE = "Relationship.Period.{n}.periodType"
# Delta files carry a DeletedAt column; a non-empty value retracts the record.
COL_DELETED_AT = "DeletedAt"

REPEX_LEI = "LEI"
REPEX_CATEGORY = "Exception.Category"
REPEX_REASON = "Exception.Reason.{n}"
REPEX_REFERENCE = "Exception.Reference.{n}"

RR_DIRECT = "IS_DIRECTLY_CONSOLIDATED_BY"
RR_ULTIMATE = "IS_ULTIMATELY_CONSOLIDATED_BY"

BATCH = 5_000


def _require(row: dict, col: str) -> None:
    if col not in row:
        raise SystemExit(
            f"Expected column {col!r} missing from the CSV — has the GLEIF "
            "CDF version changed? Compare the header against the COL_* "
            "constants in this script."
        )


def _open_csv(path: Path) -> Iterator[dict[str, str]]:
    """DictReader over a CSV file, transparently unwrapping a .zip."""
    if path.suffix == ".zip":
        zf = zipfile.ZipFile(path)
        names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not names:
            raise SystemExit(f"No CSV inside {path}")
        stream = io.TextIOWrapper(zf.open(names[0]), encoding="utf-8-sig")
    else:
        stream = open(path, encoding="utf-8-sig")  # noqa: SIM115 — handed to DictReader; lives as long as iteration
    return csv.DictReader(stream)


def _download(url: str, dest_dir: Path, label: str) -> Path:
    import httpx

    dest = dest_dir / url.rsplit("/", 1)[-1]
    print(f"downloading {label}: {url}")
    with httpx.stream("GET", url, timeout=1800.0, follow_redirects=True) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in resp.iter_bytes():
                fh.write(chunk)
    print(f"  -> {dest} ({dest.stat().st_size:,} bytes)")
    return dest


def _latest_publish() -> dict:
    import httpx

    resp = httpx.get(PUBLISHES_API, params={"page[size]": 1}, timeout=60.0)
    resp.raise_for_status()
    return resp.json()["data"][0]


def _iso_utc(value: str | None) -> str | None:
    """A CDF timestamp in the form the live API serialises it: UTC, second
    precision, ``Z`` suffix. The CSV carries the LOU's local offset
    (``2026-09-06T00:00:00+01:00``) and sometimes milliseconds; the API shows
    the same instant as ``2026-09-05T23:00:00Z``. The mapper takes the date
    part of ``lastUpdateDate`` for ``statementDate``, so the two channels must
    agree on the instant or a store-served statement would be dated a day
    off. Anything that does not parse is kept verbatim."""
    if not value:
        return None
    raw = value.strip()
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw
    if parsed.tzinfo is None:
        return raw
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _v(row: dict[str, str], col: str) -> str | None:
    """A trimmed CSV value, or ``None`` when the column is absent or blank."""
    value = row.get(col)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _address_detail(row: dict[str, str], prefix: str) -> dict:
    """One GLEIF address block from the CSV columns under ``prefix`` — only the
    parts the file actually carries."""
    lines = [v for v in (_v(row, f"{prefix}.{c}") for c in ADDR_LINE_COLS) if v]
    out: dict = {}
    lang = _v(row, f"{prefix}.xmllang")
    if lang:
        out["language"] = lang
    if lines:
        out["lines"] = lines
    for col, key in ADDR_EXTRA_COLS:
        value = _v(row, f"{prefix}.{col}")
        if value:
            out[key] = value
    for key in ("City", "Region", "Country", "PostalCode"):
        value = _v(row, f"{prefix}.{key}")
        if value:
            out[key[0].lower() + key[1:]] = value
    return out


def _name_list(row: dict[str, str], name_col: str, lang_col: str, type_col: str) -> list[dict]:
    names: list[dict] = []
    for n in _REPEATS:
        name = _v(row, name_col.format(n=n))
        if not name:
            continue
        entry: dict = {"name": name}
        lang = _v(row, lang_col.format(n=n))
        if lang:
            entry["language"] = lang
        kind = _v(row, type_col.format(n=n))
        if kind:
            entry["type"] = kind
        names.append(entry)
    return names


def _authority(row: dict[str, str], id_col: str, other_col: str) -> dict | None:
    """``{"id": "RA000585", "other": None}`` — the live API's registeredAt /
    validatedAt shape — or ``None`` when neither column is filled."""
    ra_id = _v(row, id_col)
    other = _v(row, other_col)
    if not ra_id and not other:
        return None
    return {"id": ra_id, "other": other}


def entity_detail(row: dict[str, str]) -> dict:
    """The ``detail_json`` payload for one LEI2 row: every Level 1 field the BODS
    mapper reads that is not already a page column, keyed the way
    ``gleif_record_from_row`` expects. Only values present in the file are
    written — an absent key means "the Golden Copy did not carry it", never a
    default."""
    d: dict = {}
    lang = _v(row, COL_NAME_LANG)
    if lang:
        d["legalName"] = {"language": lang}
    other = _name_list(row, COL_OTHER_NAME, COL_OTHER_NAME_LANG, COL_OTHER_NAME_TYPE)
    if other:
        d["otherNames"] = other
    translit = _name_list(row, COL_TRANSLIT_N, COL_TRANSLIT_LANG, COL_TRANSLIT_TYPE)
    if translit:
        d["transliteratedOtherNames"] = translit
    legal_addr = _address_detail(row, ADDR_LEGAL)
    if legal_addr:
        d["legalAddress"] = legal_addr
    hq_addr = _address_detail(row, ADDR_HQ)
    if hq_addr:
        d["headquartersAddress"] = hq_addr
    registered_at = _authority(row, COL_RA_ID, COL_RA_OTHER)
    if registered_at:
        d["registeredAt"] = registered_at
    registered_as = _v(row, COL_RA_ENTITY_ID)
    if registered_as:
        d["registeredAs"] = registered_as
    for col, key in (
        (COL_CATEGORY, "category"),
        (COL_SUBCATEGORY, "subCategory"),
        (COL_LEGAL_FORM_OTHER, "legalFormOther"),
        (COL_MANAGING_LOU, "managingLou"),
        (COL_VALIDATION_SOURCES, "corroborationLevel"),
        (COL_VA_ENTITY_ID, "validatedAs"),
        (COL_CONFORMITY, "conformityFlag"),
    ):
        value = _v(row, col)
        if value:
            d[key] = value
    for col, key in ((COL_CREATION, "creationDate"), (COL_NEXT_RENEWAL, "nextRenewalDate")):
        value = _iso_utc(_v(row, col))
        if value:
            d[key] = value
    expiration = {
        k: v
        for k, v in (("date", _iso_utc(_v(row, COL_EXPIRATION_DATE))),
                     ("reason", _v(row, COL_EXPIRATION_REASON)))
        if v
    }
    if expiration:
        d["expiration"] = expiration
    successors: list[dict] = []
    for n in _REPEATS:
        lei = _v(row, COL_SUCCESSOR_N.format(n=n))
        name = _v(row, COL_SUCCESSOR_NAME_N.format(n=n))
        if lei or name:
            successors.append({k: v for k, v in (("lei", lei), ("name", name)) if v})
    if successors:
        d["successorEntities"] = successors
    validated_at = _authority(row, COL_VA_ID, COL_VA_OTHER)
    if validated_at:
        d["validatedAt"] = validated_at
    others: list[dict] = []
    for n in _REPEATS:
        authority = _authority(row, COL_OVA_ID.format(n=n), COL_OVA_OTHER.format(n=n))
        entity_id = _v(row, COL_OVA_ENTITY_ID.format(n=n))
        if authority or entity_id:
            entry: dict = {}
            if authority:
                entry["validatedAt"] = authority
            if entity_id:
                entry["validatedAs"] = entity_id
            others.append(entry)
    if others:
        d["otherValidationAuthorities"] = others
    return d


def _entity_tuple(row: dict[str, str]) -> tuple:
    lei = row[COL_LEI].strip().upper()
    name = row.get(COL_NAME, "").strip() or lei
    slug = slugify_name(name, row.get(COL_TRANSLIT, "").strip() or None)
    detail = entity_detail(row)
    return (
        lei,
        name,
        slug,
        row.get(COL_STATUS, "").strip() or None,
        row.get(COL_REG_STATUS, "").strip() or None,
        row.get(COL_JURISDICTION, "").strip() or None,
        row.get(COL_LEGAL_FORM, "").strip() or None,
        row.get(COL_CITY, "").strip() or None,
        row.get(COL_REGION, "").strip() or None,
        row.get(COL_COUNTRY, "").strip() or None,
        _iso_utc(row.get(COL_FIRST_REG)),
        _iso_utc(row.get(COL_LAST_UPDATE)),
        row.get(COL_SUCCESSOR, "").strip() or None,
        json.dumps(detail, ensure_ascii=False, separators=(",", ":")) if detail else None,
    )


_UPSERT = """
INSERT INTO entities (
    lei, name, slug, entity_status, registration_status, jurisdiction,
    legal_form, city, region, country, first_registered, last_updated,
    successor_lei, detail_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(lei) DO UPDATE SET
    name=excluded.name, slug=excluded.slug,
    entity_status=excluded.entity_status,
    registration_status=excluded.registration_status,
    jurisdiction=excluded.jurisdiction, legal_form=excluded.legal_form,
    city=excluded.city, region=excluded.region, country=excluded.country,
    first_registered=excluded.first_registered,
    last_updated=excluded.last_updated, successor_lei=excluded.successor_lei,
    detail_json=excluded.detail_json
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Create the tables, and bring a v1 file up to v2 in place: ``CREATE TABLE
    IF NOT EXISTS`` adds the Level 2 tables, but SQLite has no ``ADD COLUMN IF
    NOT EXISTS``, so the entity columns a v1 file lacks are added here."""
    conn.executescript(SCHEMA)
    existing = {r[1] for r in conn.execute("PRAGMA table_info(entities)").fetchall()}
    for column in SCHEMA_V2_ENTITY_COLUMNS:
        if column not in existing:
            conn.execute(f"ALTER TABLE entities ADD COLUMN {column} TEXT")
    conn.commit()


def load_lei2(conn: sqlite3.Connection, path: Path, sample: int | None = None) -> int:
    reader = _open_csv(path)
    count = 0
    batch: list[tuple] = []
    first = True
    for row in reader:
        if first:
            for col in (COL_LEI, COL_NAME, COL_REG_STATUS, COL_LAST_UPDATE):
                _require(row, col)
            first = False
        if not row.get(COL_LEI, "").strip():
            continue
        batch.append(_entity_tuple(row))
        count += 1
        if len(batch) >= BATCH:
            conn.executemany(_UPSERT, batch)
            batch.clear()
            if count % 200_000 == 0:
                conn.commit()
                print(f"  {count:,} entity records…")
        if sample is not None and count >= sample:
            break
    if batch:
        conn.executemany(_UPSERT, batch)
    conn.commit()
    return count


_RR_UPSERT = """
INSERT INTO relationships (
    child_lei, relationship_type, parent_lei, relationship_status,
    registration_status, period_start, period_end, last_updated
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(child_lei, relationship_type) DO UPDATE SET
    parent_lei=excluded.parent_lei,
    relationship_status=excluded.relationship_status,
    registration_status=excluded.registration_status,
    period_start=excluded.period_start, period_end=excluded.period_end,
    last_updated=excluded.last_updated
"""

# The parent columns are a *view* of the relationships table: the standing
# record of each kind, or NULL. Recomputed for every child an RR load touched
# (or for every entity on a full build), so a delta that retires a record —
# or deletes it — clears the column instead of leaving a parent that the live
# API no longer serves.
_PARENT_SQL = """
UPDATE entities SET {column} = (
    SELECT parent_lei FROM relationships r
    WHERE r.child_lei = entities.lei
      AND r.relationship_type = ?
      AND upper(coalesce(r.registration_status, 'PUBLISHED')) IN ({statuses})
      AND upper(coalesce(r.relationship_status, 'ACTIVE')) = 'ACTIVE'
)
"""


def _relationship_period(row: dict[str, str]) -> tuple[str | None, str | None]:
    """The RELATIONSHIP_PERIOD start/end among the up-to-five period slots."""
    for n in _REPEATS:
        if (_v(row, RR_PERIOD_TYPE.format(n=n)) or "").upper() == "RELATIONSHIP_PERIOD":
            return _v(row, RR_PERIOD_START.format(n=n)), _v(row, RR_PERIOD_END.format(n=n))
    return None, None


def recompute_parent_columns(conn: sqlite3.Connection, leis: set[str] | None = None) -> None:
    """Derive ``direct_parent_lei`` / ``ultimate_parent_lei`` from the
    relationships table for ``leis`` (``None`` = every entity)."""
    statuses = ",".join(f"'{s}'" for s in sorted(RR_STANDING_REGISTRATION_STATUSES))
    for column, rtype in (("direct_parent_lei", RR_DIRECT), ("ultimate_parent_lei", RR_ULTIMATE)):
        sql = _PARENT_SQL.format(column=column, statuses=statuses)
        if leis is None:
            conn.execute(sql, (rtype,))
        else:
            wanted = sorted(leis)
            for i in range(0, len(wanted), 500):
                chunk = wanted[i : i + 500]
                marks = ",".join("?" * len(chunk))
                conn.execute(f"{sql} WHERE lei IN ({marks})", (rtype, *chunk))
    conn.commit()


def load_rr(conn: sqlite3.Connection, path: Path, *, full: bool = True) -> int:
    """Load the RR file into ``relationships`` and derive the parent columns.

    Every record lands in the table with its statuses; the parent columns are
    then recomputed from the standing records (see
    ``RR_STANDING_REGISTRATION_STATUSES``). A delta row with ``DeletedAt`` set
    removes the record. Returns the number of direct/ultimate records applied.
    """
    reader = _open_csv(path)
    count = 0
    batch: list[tuple] = []
    deletes: list[tuple[str, str]] = []
    touched: set[str] = set()
    first = True

    def flush() -> None:
        if batch:
            conn.executemany(_RR_UPSERT, batch)
            batch.clear()
        if deletes:
            conn.executemany(
                "DELETE FROM relationships WHERE child_lei = ? AND relationship_type = ?",
                deletes,
            )
            deletes.clear()

    for row in reader:
        if first:
            for col in (RR_START, RR_END, RR_TYPE):
                _require(row, col)
            first = False
        child = (_v(row, RR_START) or "").upper()
        parent = (_v(row, RR_END) or "").upper()
        rtype = (_v(row, RR_TYPE) or "").upper()
        if not child or not parent or rtype not in (RR_DIRECT, RR_ULTIMATE):
            continue
        touched.add(child)
        if _v(row, COL_DELETED_AT):
            deletes.append((child, rtype))
        else:
            start_date, end_date = _relationship_period(row)
            batch.append(
                (
                    child,
                    rtype,
                    parent,
                    (_v(row, RR_STATUS) or None),
                    (_v(row, RR_REG_STATUS) or None),
                    _iso_utc(start_date),
                    _iso_utc(end_date),
                    _iso_utc(_v(row, RR_LAST_UPDATE)),
                )
            )
        count += 1
        if len(batch) + len(deletes) >= BATCH:
            flush()
    flush()
    conn.commit()
    recompute_parent_columns(conn, None if full else touched)
    return count


_REPEX_UPSERT = """
INSERT INTO reporting_exceptions (lei, kind, reason, reasons_json, reference)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(lei, kind) DO UPDATE SET
    reason=excluded.reason, reasons_json=excluded.reasons_json,
    reference=excluded.reference
"""

#: GLEIF category → the relation kind the table keys on. Anything else is
#: stored verbatim (and ignored by the reader) rather than dropped.
_KIND_BY_CATEGORY = {category: kind for kind, category in EXCEPTION_CATEGORIES.items()}


def load_repex(conn: sqlite3.Connection, path: Path) -> int:
    """Load the REPEX (reporting exceptions) file. One row per (LEI, kind);
    up to five reasons and references, kept in file order. A delta row with
    ``DeletedAt`` set removes the exception."""
    reader = _open_csv(path)
    count = 0
    batch: list[tuple] = []
    deletes: list[tuple[str, str]] = []
    first = True

    def flush() -> None:
        if batch:
            conn.executemany(_REPEX_UPSERT, batch)
            batch.clear()
        if deletes:
            conn.executemany(
                "DELETE FROM reporting_exceptions WHERE lei = ? AND kind = ?", deletes
            )
            deletes.clear()

    for row in reader:
        if first:
            for col in (REPEX_LEI, REPEX_CATEGORY):
                _require(row, col)
            first = False
        lei = (_v(row, REPEX_LEI) or "").upper()
        category = (_v(row, REPEX_CATEGORY) or "").upper()
        if not lei or not category:
            continue
        kind = _KIND_BY_CATEGORY.get(category, category)
        if _v(row, COL_DELETED_AT):
            deletes.append((lei, kind))
        else:
            reasons = [r for r in (_v(row, REPEX_REASON.format(n=n)) for n in _REPEATS) if r]
            references = [
                r for r in (_v(row, REPEX_REFERENCE.format(n=n)) for n in _REPEATS) if r
            ]
            batch.append(
                (
                    lei,
                    kind,
                    reasons[0] if reasons else None,
                    json.dumps(reasons) if len(reasons) > 1 else None,
                    "; ".join(references) if references else None,
                )
            )
        count += 1
        if len(batch) + len(deletes) >= BATCH:
            flush()
    flush()
    conn.commit()
    return count


#: How many rows the dictionary is sampled from. 256 rows of ~700 bytes
#: already exceed zlib's 32 KiB window; the last 32 KiB are kept.
_ZDICT_SAMPLE_ROWS = 256


def compress_detail_column(conn: sqlite3.Connection, *, batch: int = 20_000) -> int:
    """Compress every ``detail_json`` still held as TEXT into a zlib BLOB
    against the file's shared dictionary (see ``entity_pages.build_detail_zdict``).

    Runs after every load: a full build writes the detail as text first and
    compresses here, once the whole file can be sampled for the dictionary; a
    delta upsert leaves its new and revised rows as text, and this pass picks
    them up with the dictionary the file already carries — the dictionary is
    fixed for the life of the file, since every BLOB in it was written against
    it. Returns the number of rows compressed.
    """
    stored = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (DETAIL_ZDICT_META_KEY,)
    ).fetchone()
    zdict = decode_zdict(stored[0]) if stored else None
    if zdict is None:
        total = conn.execute(
            "SELECT COUNT(*) FROM entities WHERE detail_json IS NOT NULL"
        ).fetchone()[0]
        if not total:
            return 0
        step = max(1, total // _ZDICT_SAMPLE_ROWS)
        samples = [
            r[0]
            for r in conn.execute(
                "SELECT detail_json FROM entities WHERE detail_json IS NOT NULL "
                "AND typeof(detail_json) = 'text' AND (rowid % ?) = 0 LIMIT ?",
                (step, _ZDICT_SAMPLE_ROWS),
            )
        ]
        if not samples:
            return 0
        zdict = build_detail_zdict(samples)
        write_meta(conn, **{DETAIL_ZDICT_META_KEY: encode_zdict(zdict)})

    done = 0
    last = 0
    while True:
        rows = conn.execute(
            "SELECT rowid, detail_json FROM entities WHERE rowid > ? "
            "AND typeof(detail_json) = 'text' ORDER BY rowid LIMIT ?",
            (last, batch),
        ).fetchall()
        if not rows:
            break
        conn.executemany(
            "UPDATE entities SET detail_json = ? WHERE rowid = ?",
            [(compress_detail(text, zdict), rowid) for rowid, text in rows],
        )
        conn.commit()
        done += len(rows)
        last = rows[-1][0]
        if done % 500_000 < batch:
            print(f"  {done:,} detail rows compressed…")
    return done


def write_meta(conn: sqlite3.Connection, **values: str) -> None:
    conn.executemany(
        "INSERT INTO meta (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        list(values.items()),
    )
    conn.commit()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--out", required=True, help="Path of the SQLite DB to write.")
    ap.add_argument(
        "--delta",
        choices=["LastMonth", "LastWeek", "LastDay", "IntraDay"],
        help="Upsert a delta into an existing DB instead of a full rebuild.",
    )
    ap.add_argument("--lei2-file", type=Path, help="Local LEI2 CSV (.csv or .zip); skips download.")
    ap.add_argument("--rr-file", type=Path, help="Local RR CSV (.csv or .zip); skips download.")
    ap.add_argument(
        "--repex-file", type=Path, help="Local REPEX CSV (.csv or .zip); skips download."
    )
    ap.add_argument("--sample", type=int, help="Stop after N entity records (dev/testing).")
    ap.add_argument("--skip-rr", action="store_true", help="Skip relationship (parent) data.")
    ap.add_argument(
        "--skip-repex", action="store_true", help="Skip reporting-exception data."
    )
    args = ap.parse_args()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    publish: dict | None = None
    publish_date = ""
    needs_download = (
        args.lei2_file is None
        or (args.rr_file is None and not args.skip_rr)
        or (args.repex_file is None and not args.skip_repex)
    )
    if needs_download:
        publish = _latest_publish()
        publish_date = publish["publish_date"]
        print(f"latest GLEIF Golden Copy publish: {publish_date}")

    with tempfile.TemporaryDirectory(prefix="gleif-gc-") as tmp:
        tmp_dir = Path(tmp)

        def file_for(kind: str, local: Path | None) -> Path:
            if local is not None:
                return local
            assert publish is not None
            section = publish[kind]
            entry = (
                section["delta_files"][args.delta]["csv"]
                if args.delta
                else section["full_file"]["csv"]
            )
            return _download(entry["url"], tmp_dir, f"{kind} {args.delta or 'full'}")

        lei2_path = file_for("lei2", args.lei2_file)

        in_place = bool(args.delta and out.exists())
        if in_place:
            conn = sqlite3.connect(out)
            ensure_schema(conn)  # no-op on an up-to-date DB; upgrades a v1 file
        else:
            if args.delta:
                print("note: --delta but no existing DB; building fresh from the delta.")
            build_path = out.with_suffix(".building")
            build_path.unlink(missing_ok=True)
            conn = sqlite3.connect(build_path)
            ensure_schema(conn)

        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA journal_mode=MEMORY")

        n_entities = load_lei2(conn, lei2_path, sample=args.sample)
        print(f"entities loaded/updated: {n_entities:,}")

        if not args.skip_rr:
            rr_path = file_for("rr", args.rr_file)
            n_rel = load_rr(conn, rr_path, full=not in_place)
            print(f"relationship records applied: {n_rel:,}")

        if not args.skip_repex:
            repex_path = file_for("repex", args.repex_file)
            n_ex = load_repex(conn, repex_path)
            print(f"reporting exceptions applied: {n_ex:,}")

        n_compressed = compress_detail_column(conn)
        print(f"detail rows compressed: {n_compressed:,}")

        def _count(table: str) -> str:
            return str(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

        write_meta(
            conn,
            built_at=datetime.now(UTC).isoformat(timespec="seconds"),
            # Empty when built from local files — consumers (sitemap lastmod,
            # page footer) only render it when it is a real date.
            source_publish_date=publish_date,
            # Phase 178: the full publish timestamp is the mirror's watermark —
            # a delta refresh (Phase 180) picks the file whose window covers
            # the gap from here.
            source_publish_datetime=publish_date,
            source="publish API" if publish else "local files",
            mode=args.delta or "full",
            schema_version=SCHEMA_VERSION,
            detail_encoding="zlib+zdict",
            record_count=_count("entities"),
            relationship_count=_count("relationships"),
            exception_count=_count("reporting_exceptions"),
        )
        total = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
        conn.execute("PRAGMA optimize")
        if not in_place:
            # The detail was written as text and compressed in place, which
            # leaves the freed space inside the file; a full build hands over
            # a compact one. (A delta touches too few rows to be worth it.)
            conn.execute("VACUUM")
        conn.close()

        if not in_place:
            # Atomic hand-over: the live DB is never a half-written file.
            out.with_suffix(".building").replace(out)

    print(json.dumps({"db": str(out), "entities": total, "publish": publish_date}))


if __name__ == "__main__":
    main()
