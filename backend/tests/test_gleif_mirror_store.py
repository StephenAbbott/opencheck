"""Phase 178 — the entity-pages store as a GLEIF mirror.

Two things are pinned here. First, the **builder** turns the Golden Copy
files (LEI2 + RR + REPEX, with their real CDF 3.1 column names) into a store
that holds every Level 1 field the BODS mapper reads, the relationship records
with their statuses, and the reporting exceptions. Second — the point of the
phase — **a store row rendered through ``gleif_record_from_row`` is
indistinguishable from the live API record for everything ``map_gleif``
reads**, and never carries a field the Golden Copy did not. The parity
fixture is a real record captured from ``api.gleif.org`` on 2026-09-07
(``2138001EXFNP9E7AYB46``) alongside the same LEI's row from that day's
LastDay delta, so the two channels are compared on identical data.
"""

from __future__ import annotations

import csv
import json
import shutil
import sqlite3
import sys
import zlib
from pathlib import Path
from typing import Any

import pytest

from opencheck import entity_pages as ep
from opencheck import subsidiaries as subs
from opencheck.bods import mapper
from opencheck.config import get_settings

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from build_entity_pages_db import (  # noqa: E402
    _iso_utc,
    compress_detail_column,
    ensure_schema,
    entity_detail,
    load_lei2,
    load_repex,
    load_rr,
    write_meta,
)

FIXTURES = Path(__file__).parent / "fixtures" / "gleif_mirror"
EASY = "2138001EXFNP9E7AYB46"
PARENT = "2138000000000000P178"
TOP = "2138000000000000T178"
ORPHAN = "2138000000000000O178"
RETIRED_CHILD = "2138000000000000R178"

_LIVE = json.loads((FIXTURES / "live_record_2138001EXFNP9E7AYB46.json").read_text())
_LEI2 = json.loads((FIXTURES / "lei2_row_2138001EXFNP9E7AYB46.json").read_text())
LEI2_HEADER: list[str] = _LEI2["header"]
RR_HEADER: list[str] = json.loads((FIXTURES / "rr_header.json").read_text())
REPEX_HEADER: list[str] = json.loads((FIXTURES / "repex_header.json").read_text())


def _lei2_row(**values: str) -> list[str]:
    """A full-width LEI2 row from a handful of named CDF columns."""
    row = [""] * len(LEI2_HEADER)
    for col, value in values.items():
        row[LEI2_HEADER.index(col)] = value
    return row


def _rr_row(child: str, parent: str, rtype: str, *, reg: str, status: str = "ACTIVE",
            start: str = "2020-01-01T00:00:00Z", deleted: str = "") -> list[str]:
    values = {
        "Relationship.StartNode.NodeID": child,
        "Relationship.StartNode.NodeIDType": "LEI",
        "Relationship.EndNode.NodeID": parent,
        "Relationship.EndNode.NodeIDType": "LEI",
        "Relationship.RelationshipType": rtype,
        "Relationship.RelationshipStatus": status,
        "Relationship.Period.1.startDate": start,
        "Relationship.Period.1.periodType": "RELATIONSHIP_PERIOD",
        "Relationship.Period.2.startDate": "2024-01-01T00:00:00Z",
        "Relationship.Period.2.endDate": "2024-12-31T00:00:00Z",
        "Relationship.Period.2.periodType": "ACCOUNTING_PERIOD",
        "Registration.RegistrationStatus": reg,
        "Registration.LastUpdateDate": "2026-09-06T16:15:35.777Z",
        "DeletedAt": deleted,
    }
    row = [""] * len(RR_HEADER)
    for col, value in values.items():
        row[RR_HEADER.index(col)] = value
    return row


def _repex_row(lei: str, category: str, *reasons: str, reference: str = "",
               deleted: str = "") -> list[str]:
    values = {"LEI": lei, "Exception.Category": category, "DeletedAt": deleted}
    for i, reason in enumerate(reasons, start=1):
        values[f"Exception.Reason.{i}"] = reason
    if reference:
        values["Exception.Reference.1"] = reference
    row = [""] * len(REPEX_HEADER)
    for col, value in values.items():
        row[REPEX_HEADER.index(col)] = value
    return row


LEI2_ROWS = [
    _LEI2["row"],  # EASY POWER, verbatim from the 2026-09-07 LastDay delta
    _lei2_row(**{
        "LEI": PARENT, "Entity.LegalName": "Mirror Parent Holdings Ltd",
        "Entity.LegalAddress.City": "LONDON", "Entity.LegalAddress.Country": "GB",
        "Entity.LegalJurisdiction": "GB", "Entity.EntityStatus": "ACTIVE",
        "Entity.LegalForm.EntityLegalFormCode": "H0PO",
        "Entity.RegistrationAuthority.RegistrationAuthorityID": "RA000585",
        "Entity.RegistrationAuthority.RegistrationAuthorityEntityID": "01234567",
        "Registration.InitialRegistrationDate": "2019-05-05T00:00:00Z",
        "Registration.LastUpdateDate": "2026-06-15T08:00:00.000Z",
        "Registration.RegistrationStatus": "ISSUED",
    }),
    _lei2_row(**{
        "LEI": TOP, "Entity.LegalName": "Mirror Top plc",
        "Entity.LegalAddress.City": "LONDON", "Entity.LegalAddress.Country": "GB",
        "Entity.LegalJurisdiction": "GB", "Entity.EntityStatus": "ACTIVE",
        "Entity.LegalForm.EntityLegalFormCode": "B6ES",
        "Entity.EntityExpirationDate": "",
        "Entity.SuccessorEntity.1.SuccessorLEI": "",
        "Entity.SuccessorEntity.1.SuccessorEntityName": "",
        "Registration.InitialRegistrationDate": "2012-06-06T00:00:00Z",
        "Registration.LastUpdateDate": "2026-08-01T00:00:00+01:00",
        "Registration.RegistrationStatus": "ISSUED",
    }),
    _lei2_row(**{
        "LEI": ORPHAN, "Entity.LegalName": "Mirror Orphan GmbH",
        "Entity.LegalAddress.City": "BERLIN", "Entity.LegalAddress.Country": "DE",
        "Entity.LegalJurisdiction": "DE", "Entity.EntityStatus": "INACTIVE",
        "Entity.EntityExpirationDate": "2025-12-31T00:00:00+01:00",
        "Entity.EntityExpirationReason": "DISSOLVED",
        "Entity.SuccessorEntity.1.SuccessorLEI": TOP,
        "Entity.SuccessorEntity.1.SuccessorEntityName": "Mirror Top plc",
        "Registration.InitialRegistrationDate": "2018-02-02T00:00:00Z",
        "Registration.LastUpdateDate": "2025-11-11T08:00:00.000Z",
        "Registration.RegistrationStatus": "RETIRED",
    }),
    _lei2_row(**{
        "LEI": RETIRED_CHILD, "Entity.LegalName": "Mirror Retired Link Ltd",
        "Entity.LegalAddress.City": "LEEDS", "Entity.LegalAddress.Country": "GB",
        "Entity.LegalJurisdiction": "GB", "Entity.EntityStatus": "ACTIVE",
        "Registration.LastUpdateDate": "2026-01-01T00:00:00Z",
        "Registration.RegistrationStatus": "ISSUED",
    }),
]

RR_ROWS = [
    # EASY → PARENT (direct, PUBLISHED) and EASY → TOP (ultimate, LAPSED —
    # still served live, see RR_STANDING_REGISTRATION_STATUSES).
    _rr_row(EASY, PARENT, "IS_DIRECTLY_CONSOLIDATED_BY", reg="PUBLISHED"),
    _rr_row(EASY, TOP, "IS_ULTIMATELY_CONSOLIDATED_BY", reg="LAPSED"),
    # PARENT → TOP direct + ultimate.
    _rr_row(PARENT, TOP, "IS_DIRECTLY_CONSOLIDATED_BY", reg="PUBLISHED"),
    _rr_row(PARENT, TOP, "IS_ULTIMATELY_CONSOLIDATED_BY", reg="PUBLISHED"),
    # A retired record and an inactive relationship: held, never standing.
    _rr_row(RETIRED_CHILD, TOP, "IS_DIRECTLY_CONSOLIDATED_BY", reg="RETIRED"),
    _rr_row(RETIRED_CHILD, TOP, "IS_ULTIMATELY_CONSOLIDATED_BY", reg="PUBLISHED",
            status="INACTIVE"),
]

REPEX_ROWS = [
    # TOP files an exception for both kinds; two reasons on the ultimate one.
    _repex_row(TOP, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NON_CONSOLIDATING"),
    _repex_row(TOP, "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT", "NATURAL_PERSONS",
               "NON_CONSOLIDATING", reference="Section 399 Companies Act 2006"),
    # RETIRED_CHILD has no standing direct parent and an exception for it.
    _repex_row(RETIRED_CHILD, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NO_LEI"),
]


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> Path:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(rows)
    return path


@pytest.fixture(scope="module")
def db_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    tmp = tmp_path_factory.mktemp("mirror")
    out = tmp / "entity_pages.sqlite"
    conn = sqlite3.connect(out)
    ensure_schema(conn)
    assert load_lei2(conn, _write_csv(tmp / "lei2.csv", LEI2_HEADER, LEI2_ROWS)) == 5
    assert load_rr(conn, _write_csv(tmp / "rr.csv", RR_HEADER, RR_ROWS)) == 6
    assert load_repex(conn, _write_csv(tmp / "repex.csv", REPEX_HEADER, REPEX_ROWS)) == 3
    # What the builder does last: every reader test below runs against the
    # compressed BLOBs a built file actually holds, not the transient text.
    assert compress_detail_column(conn) == 5
    write_meta(
        conn,
        source_publish_date="2026-09-07 16:00:00",
        source_publish_datetime="2026-09-07 16:00:00",
        schema_version=ep.SCHEMA_VERSION,
    )
    conn.close()
    return out


@pytest.fixture
def store(db_path: Path) -> ep.EntityStore:
    return ep.EntityStore(db_path)


@pytest.fixture
def configured_store(db_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OPENCHECK_ENTITY_PAGES_DB_FILE", str(db_path))
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    get_settings.cache_clear()
    ep.reset_store_for_tests()
    yield ep.get_store()
    get_settings.cache_clear()
    ep.reset_store_for_tests()


# ---------------------------------------------------------------------------
# The builder
# ---------------------------------------------------------------------------


def test_builder_writes_schema_v2_and_meta(store: ep.EntityStore) -> None:
    assert store.schema_version == "2"
    assert store.has_relationships and store.has_exceptions and store.has_detail
    meta = store.meta()
    assert meta["source_publish_datetime"] == "2026-09-07 16:00:00"


def test_builder_keeps_the_page_columns_and_adds_detail(store: ep.EntityStore) -> None:
    row = store.get(EASY)
    assert row is not None
    # Page columns exactly as Phase 88 read them …
    assert row.name.startswith("EASY POWER")
    assert row.city == "ATHENS" and row.country == "GR" and row.jurisdiction == "GR"
    assert row.legal_form == "W2NK" and row.registration_status == "ISSUED"
    # … and the timestamp in the live API's serialisation, not the LOU's offset.
    assert row.last_updated == "2026-09-05T23:00:00Z"
    # … plus the detail the mapper needs.
    assert row.detail is not None
    assert row.detail["registeredAs"] == "007132601000"
    assert row.detail["registeredAt"] == {"id": "RA000685", "other": None}
    assert row.detail["otherNames"][0]["name"] == "EASY POWER AE"
    assert row.detail["headquartersAddress"]["postalCode"] == "145 75"


def test_builder_only_writes_what_the_file_carries() -> None:
    """An absent key means "not in the Golden Copy", so blanks must not
    become empty structures the reader would render as facts."""
    sparse = dict(
        zip(LEI2_HEADER, _lei2_row(**{"LEI": "X", "Entity.LegalName": "X Ltd"}), strict=True)
    )
    assert entity_detail(sparse) == {}
    with_hq = dict(zip(LEI2_HEADER, _lei2_row(**{
        "LEI": "X", "Entity.LegalName": "X Ltd", "Entity.HeadquartersAddress.City": "OSLO",
    }), strict=True))
    assert entity_detail(with_hq) == {"headquartersAddress": {"city": "OSLO"}}


def test_address_number_is_its_own_key_as_the_live_api_has_it() -> None:
    """``map_gleif`` reads ``addressLines`` only, so a number folded into the
    lines would put text in the BODS address that the live record does not
    (a Spanish record in a live sweep repeated half its street that way)."""
    row = dict(zip(LEI2_HEADER, _lei2_row(**{
        "LEI": "X", "Entity.LegalName": "X SL",
        "Entity.LegalAddress.FirstAddressLine": "LA FLAUTA MAGICA 37, POLIG. IND. ALAMEDA",
        "Entity.LegalAddress.AddressNumber": "37, POLIG. IND. ALAMEDA",
        "Entity.LegalAddress.City": "Málaga",
    }), strict=True))
    detail = entity_detail(row)
    assert detail["legalAddress"] == {
        "lines": ["LA FLAUTA MAGICA 37, POLIG. IND. ALAMEDA"],
        "addressNumber": "37, POLIG. IND. ALAMEDA",
        "city": "Málaga",
    }
    rendered = ep.gleif_record_from_row(
        ep.EntityRow(
            lei="X", name="X SL", slug="x-sl", entity_status=None,
            registration_status=None, jurisdiction=None, legal_form=None, city=None,
            region=None, country=None, first_registered=None, last_updated=None,
            successor_lei=None, direct_parent_lei=None, ultimate_parent_lei=None,
            detail=detail,
        )
    )
    assert rendered["attributes"]["entity"]["legalAddress"] == {
        "addressLines": ["LA FLAUTA MAGICA 37, POLIG. IND. ALAMEDA"],
        "addressNumber": "37, POLIG. IND. ALAMEDA",
        "city": "Málaga",
    }


def test_detail_is_a_dictionary_compressed_blob(db_path: Path, store: ep.EntityStore) -> None:
    """The size fix that lets the widened file boot: the detail rows are zlib
    BLOBs against one shared dictionary held in ``meta``, and the reader
    inflates them back to the identical JSON."""
    conn = sqlite3.connect(db_path)
    types = {r[0] for r in conn.execute("SELECT typeof(detail_json) FROM entities")}
    assert types == {"blob"}
    zdict = ep.decode_zdict(store.meta()[ep.DETAIL_ZDICT_META_KEY])
    assert zdict and len(zdict) <= 32 * 1024
    blob = conn.execute("SELECT detail_json FROM entities WHERE lei = ?", (EASY,)).fetchone()[0]
    conn.close()
    text = ep.decompress_detail(blob, zdict)
    assert json.loads(text) == store.get(EASY).detail
    # Round trip through the same primitives the builder uses.
    assert ep.decompress_detail(ep.compress_detail(text, zdict), zdict) == text
    # A blob written against a dictionary cannot be read without it — which is
    # why the dictionary lives inside the file, beside the rows.
    with pytest.raises(zlib.error):
        ep.decompress_detail(blob, None)


def test_reader_accepts_plain_text_detail(tmp_path: Path) -> None:
    """A hand-made row (tests, a file whose compression pass did not run)
    still reads: TEXT is parsed as-is, BLOB is inflated."""
    db = tmp_path / "text.sqlite"
    conn = sqlite3.connect(db)
    ensure_schema(conn)
    conn.execute(
        "INSERT INTO entities (lei, name, slug, detail_json) VALUES (?, ?, ?, ?)",
        (EASY, "Text Row", "text-row", json.dumps({"registeredAs": "123"})),
    )
    conn.commit()
    conn.close()
    row = ep.EntityStore(db).get(EASY)
    assert row is not None and row.detail == {"registeredAs": "123"}


def test_shared_dictionary_beats_per_row_compression() -> None:
    """The reason for the dictionary, pinned on the real record: with it the
    compressor writes the key names and shared values as back references."""
    text = json.dumps(entity_detail(dict(zip(LEI2_HEADER, LEI2_ROWS[0], strict=True))))
    others = [
        json.dumps(entity_detail(dict(zip(LEI2_HEADER, r, strict=True)))) for r in LEI2_ROWS[1:]
    ]
    zdict = ep.build_detail_zdict(others)
    with_dict = len(ep.compress_detail(text, zdict))
    without = len(zlib.compress(text.encode()))
    assert with_dict < without < len(text)


def test_exceptions_table_is_keyed_by_kind(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    kinds = {r[0] for r in conn.execute("SELECT DISTINCT kind FROM reporting_exceptions")}
    sql = conn.execute(
        "SELECT sql FROM sqlite_master WHERE name = 'reporting_exceptions'"
    ).fetchone()[0]
    conn.close()
    assert kinds == {"direct", "ultimate"}
    assert "WITHOUT ROWID" in sql


def test_relationships_table_holds_every_record_with_its_statuses(
    store: ep.EntityStore,
) -> None:
    direct = store.relationship(EASY, "direct")
    assert direct is not None
    assert direct.parent_lei == PARENT and direct.standing
    assert direct.period_start == "2020-01-01T00:00:00Z" and direct.period_end is None
    assert direct.last_updated == "2026-09-06T16:15:35Z"  # milliseconds dropped, as live
    ultimate = store.relationship(EASY, "ultimate")
    assert ultimate is not None and ultimate.registration_status == "LAPSED"
    assert ultimate.standing  # a lapsed registration is still served live
    retired = store.relationship(RETIRED_CHILD, "direct")
    assert retired is not None and not retired.standing
    inactive = store.relationship(RETIRED_CHILD, "ultimate")
    assert inactive is not None and not inactive.standing


def test_parent_columns_are_derived_from_standing_records(store: ep.EntityStore) -> None:
    easy = store.get(EASY)
    assert easy is not None
    assert easy.direct_parent_lei == PARENT
    assert easy.ultimate_parent_lei == TOP  # LAPSED registration counts
    retired = store.get(RETIRED_CHILD)
    assert retired is not None
    assert retired.direct_parent_lei is None  # RETIRED record
    assert retired.ultimate_parent_lei is None  # INACTIVE relationship


def test_children_by_either_relation(store: ep.EntityStore) -> None:
    direct, n_direct = store.children(TOP, kind="direct")
    assert n_direct == 1 and [r.lei for r in direct] == [PARENT]
    ultimate, n_ultimate = store.children(TOP, kind="ultimate")
    assert n_ultimate == 2 and {r.lei for r in ultimate} == {EASY, PARENT}
    # The Phase 88 signature is unchanged.
    assert store.children(TOP)[1] == 1


def test_reporting_exceptions_by_kind(store: ep.EntityStore) -> None:
    ex = store.exceptions(TOP)
    assert set(ex) == {"direct", "ultimate"}
    assert ex["direct"].reason == "NON_CONSOLIDATING" and ex["direct"].reasons == [
        "NON_CONSOLIDATING"
    ]
    assert ex["ultimate"].reasons == ["NATURAL_PERSONS", "NON_CONSOLIDATING"]
    assert ex["ultimate"].reference == "Section 399 Companies Act 2006"
    record = ex["ultimate"].record()
    assert record["type"] == "reporting-exceptions"
    assert record["attributes"] == {
        "lei": TOP,
        "category": "ULTIMATE_ACCOUNTING_CONSOLIDATION_PARENT",
        "reason": "NATURAL_PERSONS",
        "reasons": ["NATURAL_PERSONS", "NON_CONSOLIDATING"],
        "reference": "Section 399 Companies Act 2006",
    }
    assert store.exceptions(EASY) == {}


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-09-06T00:00:00+01:00", "2026-09-05T23:00:00Z"),
        ("2026-09-06T16:15:35.777Z", "2026-09-06T16:15:35Z"),
        ("2026-07-01T08:00:00.000Z", "2026-07-01T08:00:00Z"),
        ("2020-01-01T00:00:00Z", "2020-01-01T00:00:00Z"),
        ("2026-09-06", "2026-09-06"),  # date-only: kept verbatim
        ("", None),
        (None, None),
    ],
)
def test_timestamps_are_serialised_as_the_live_api_does(raw: Any, expected: Any) -> None:
    assert _iso_utc(raw) == expected


# ---------------------------------------------------------------------------
# Parity with the live record — the point of the phase
# ---------------------------------------------------------------------------


def _prune(value: Any) -> Any:
    """Drop the nulls and empties the live API pads records with, so the two
    shapes can be compared on what they *assert*."""
    if isinstance(value, dict):
        out = {k: _prune(v) for k, v in value.items()}
        return {k: v for k, v in out.items() if v not in (None, [], {})}
    if isinstance(value, list):
        return [_prune(v) for v in value]
    return value


def _subset(store_value: Any, live_value: Any, path: str = "") -> list[str]:
    """Every key/value the store emits must exist, equal, in the live record."""
    problems: list[str] = []
    if isinstance(store_value, dict):
        if not isinstance(live_value, dict):
            return [f"{path}: store has an object, live has {live_value!r}"]
        for key, sv in store_value.items():
            if key not in live_value:
                problems.append(f"{path}/{key}: invented (absent live)")
            else:
                problems.extend(_subset(sv, live_value[key], f"{path}/{key}"))
    elif isinstance(store_value, list):
        if not isinstance(live_value, list) or len(store_value) != len(live_value):
            return [f"{path}: list differs ({store_value!r} vs {live_value!r})"]
        for i, (sv, lv) in enumerate(zip(store_value, live_value, strict=True)):
            problems.extend(_subset(sv, lv, f"{path}[{i}]"))
    elif store_value != live_value:
        problems.append(f"{path}: {store_value!r} != {live_value!r}")
    return problems


#: Every path ``map_gleif`` / ``_gleif_entity_statement`` reads off a record.
MAPPER_READS = (
    ("attributes", "lei"),
    ("attributes", "entity", "legalName", "name"),
    ("attributes", "entity", "jurisdiction"),
    ("attributes", "entity", "registeredAs"),
    ("attributes", "entity", "registeredAt", "id"),
    ("attributes", "entity", "otherNames"),
    ("attributes", "entity", "transliteratedOtherNames"),
    ("attributes", "entity", "creationDate"),
    ("attributes", "entity", "status"),
    ("attributes", "entity", "legalForm", "id"),
    ("attributes", "entity", "legalAddress"),
    ("attributes", "entity", "headquartersAddress"),
    ("attributes", "registration", "lastUpdateDate"),
    ("attributes", "registration", "status"),
)


def _at(record: dict, path: tuple[str, ...]) -> Any:
    node: Any = record
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def test_store_record_matches_live_on_every_field_the_mapper_reads(
    store: ep.EntityStore,
) -> None:
    from_store = ep.gleif_record_from_row(store.get(EASY))
    live = _prune(_LIVE)
    for path in MAPPER_READS:
        assert _prune(_at(from_store, path)) == _at(live, path), "/".join(path)


def test_store_record_never_invents_a_field(store: ep.EntityStore) -> None:
    """Everything the store emits is in the live record with the same value.
    (The converse is not required: the cross-reference ids — bic, mic, ocid,
    qcc, spglobal — come from GLEIF's mapping files, not the Golden Copy.)"""
    from_store = ep.gleif_record_from_row(store.get(EASY))
    problems = _subset(_prune(from_store), _prune(_LIVE))
    assert problems == []
    for absent in ("bic", "mic", "ocid", "qcc", "spglobal"):
        assert absent not in from_store["attributes"]


def test_map_gleif_cannot_tell_a_store_record_from_a_live_one(store: ep.EntityStore) -> None:
    """The decisive check: the BODS entity statement is the same either way."""

    def entity_statement(record: dict) -> dict:
        bundle = {"source_id": "gleif", "lei": EASY, "record": record}
        statements = mapper.map_gleif(bundle).statements
        entity = next(s for s in statements if s["recordDetails"].get("entityType"))
        # Provenance is expected to differ (snapshot vs live); strip it.
        entity = dict(entity)
        entity.pop("source", None)
        entity.pop("publicationDetails", None)
        return entity

    from_store = entity_statement(ep.gleif_record_from_row(store.get(EASY)))
    from_live = entity_statement(_LIVE)
    # The one documented difference: the live API decorates a record with the
    # cross-reference ids from GLEIF's *mapping* files (OpenCorporates, QCC,
    # S&P Global, BIC, MIC), which are not in the Golden Copy. Those become
    # extra identifiers on the live statement and nothing else — Phase 181
    # may load the mapping files; until then the store honestly omits them.
    mapping_file_schemes = {"OpenCorporates", "QCC Code", "S&P CIQ Company ID", "BIC", "MIC"}
    live_ids = from_live["recordDetails"]["identifiers"]
    from_live["recordDetails"]["identifiers"] = [
        i for i in live_ids if i["scheme"] not in mapping_file_schemes
    ]
    assert len(live_ids) > len(from_live["recordDetails"]["identifiers"])  # the gap is real
    assert from_store == from_live
    # And it is a rich statement, not two empty ones agreeing with each other.
    ids = {i["scheme"]: i["id"] for i in from_store["recordDetails"]["identifiers"]}
    assert ids["XI-LEI"] == EASY
    assert "007132601000" in ids.values()  # registeredAs → the Greek registry id
    assert from_store["recordDetails"]["alternateNames"]
    assert len(from_store["recordDetails"]["addresses"]) == 2


def test_v1_row_renders_as_before(store: ep.EntityStore) -> None:
    """A row without detail (a pre-178 file) renders exactly the Phase 146 shape."""
    row = ep.EntityRow(
        lei=PARENT, name="Mirror Parent Holdings Ltd", slug="mirror-parent-holdings-ltd",
        entity_status="ACTIVE", registration_status="ISSUED", jurisdiction="GB",
        legal_form="H0PO", city="LONDON", region=None, country="GB",
        first_registered=None, last_updated=None, successor_lei=None,
        direct_parent_lei=TOP, ultimate_parent_lei=TOP,
    )
    record = ep.gleif_record_from_row(row)
    entity = record["attributes"]["entity"]
    assert "registeredAs" not in entity and "headquartersAddress" not in entity
    assert entity["legalAddress"] == {"city": "LONDON", "country": "GB"}
    assert record["attributes"]["registration"] == {"status": "ISSUED"}


# ---------------------------------------------------------------------------
# Delta upsert, v1 upgrade, v1 tolerance
# ---------------------------------------------------------------------------


def test_delta_retires_and_deletes(db_path: Path, tmp_path: Path) -> None:
    db2 = tmp_path / "delta.sqlite"
    shutil.copy(db_path, db2)
    conn = sqlite3.connect(db2)
    ensure_schema(conn)
    rr_delta = _write_csv(
        tmp_path / "rr-delta.csv", RR_HEADER,
        [
            # EASY's direct parent record is retired …
            _rr_row(EASY, PARENT, "IS_DIRECTLY_CONSOLIDATED_BY", reg="RETIRED"),
            # … and its ultimate record is deleted outright.
            _rr_row(EASY, TOP, "IS_ULTIMATELY_CONSOLIDATED_BY", reg="LAPSED",
                    deleted="2026-09-07T10:00:00Z"),
        ],
    )
    assert load_rr(conn, rr_delta, full=False) == 2
    repex_delta = _write_csv(
        tmp_path / "repex-delta.csv", REPEX_HEADER,
        [_repex_row(TOP, "DIRECT_ACCOUNTING_CONSOLIDATION_PARENT", "NON_CONSOLIDATING",
                    deleted="2026-09-07T10:00:00Z")],
    )
    assert load_repex(conn, repex_delta) == 1
    lei2_delta = _write_csv(tmp_path / "lei2-delta.csv", LEI2_HEADER, [LEI2_ROWS[0]])
    assert load_lei2(conn, lei2_delta) == 1
    # The revised row lands as text; the pass compresses just that one, with
    # the dictionary the file already carries — never a new one.
    zdict_before = conn.execute(
        "SELECT value FROM meta WHERE key = ?", (ep.DETAIL_ZDICT_META_KEY,)
    ).fetchone()[0]
    assert compress_detail_column(conn) == 1
    assert conn.execute(
        "SELECT value FROM meta WHERE key = ?", (ep.DETAIL_ZDICT_META_KEY,)
    ).fetchone()[0] == zdict_before
    assert conn.execute(
        "SELECT COUNT(*) FROM entities WHERE typeof(detail_json) = 'text'"
    ).fetchone()[0] == 0
    conn.close()

    s2 = ep.EntityStore(db2)
    easy = s2.get(EASY)
    assert easy is not None
    assert easy.detail is not None and easy.detail["registeredAs"] == "007132601000"
    assert easy.direct_parent_lei is None  # retired → column cleared
    assert easy.ultimate_parent_lei is None  # deleted → column cleared
    assert s2.relationship(EASY, "ultimate") is None
    assert s2.relationship(EASY, "direct") is not None  # held, not standing
    assert set(s2.exceptions(TOP)) == {"ultimate"}
    # Untouched rows keep their parents.
    parent = s2.get(PARENT)
    assert parent is not None and parent.direct_parent_lei == TOP


def _v1_schema() -> str:
    """The Phase 88 schema, verbatim — no detail column, no Level 2 tables."""
    return """
    CREATE TABLE entities (
        lei TEXT PRIMARY KEY, name TEXT NOT NULL, slug TEXT NOT NULL DEFAULT '',
        entity_status TEXT, registration_status TEXT, jurisdiction TEXT,
        legal_form TEXT, city TEXT, region TEXT, country TEXT,
        first_registered TEXT, last_updated TEXT, successor_lei TEXT,
        direct_parent_lei TEXT, ultimate_parent_lei TEXT
    );
    CREATE INDEX idx_entities_direct_parent
        ON entities(direct_parent_lei) WHERE direct_parent_lei IS NOT NULL;
    CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
    INSERT INTO meta VALUES ('source_publish_date', '2026-08-03 08:00:00');
    """


def test_v1_file_is_read_as_not_held(tmp_path: Path) -> None:
    db = tmp_path / "v1.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(_v1_schema())
    conn.execute(
        "INSERT INTO entities (lei, name, slug, entity_status, registration_status, "
        "jurisdiction, city, country, direct_parent_lei, ultimate_parent_lei) "
        "VALUES (?, ?, ?, 'ACTIVE', 'ISSUED', 'GB', 'LONDON', 'GB', ?, ?)",
        (EASY, "Old Row Ltd", "old-row-ltd", PARENT, TOP),
    )
    conn.commit()
    conn.close()
    s1 = ep.EntityStore(db)
    assert s1.schema_version == "1"
    assert not s1.has_detail and not s1.has_relationships and not s1.has_exceptions
    row = s1.get(EASY)
    assert row is not None and row.detail is None
    assert s1.relationship(EASY, "direct") is None
    assert s1.exceptions(EASY) == {}
    # The ultimate column existed in v1, so the ultimate children query works
    # there too — without the index, but correctly.
    assert s1.children(TOP, kind="ultimate")[1] == 1


def test_v1_file_is_upgraded_in_place_by_a_delta(tmp_path: Path) -> None:
    db = tmp_path / "v1-upgrade.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(_v1_schema())
    conn.commit()
    ensure_schema(conn)  # what the builder runs before an in-place delta
    columns = {r[1] for r in conn.execute("PRAGMA table_info(entities)")}
    assert "detail_json" in columns
    assert load_lei2(conn, _write_csv(tmp_path / "d.csv", LEI2_HEADER, [LEI2_ROWS[0]])) == 1
    assert load_repex(conn, _write_csv(tmp_path / "x.csv", REPEX_HEADER, REPEX_ROWS[:1])) == 1
    assert compress_detail_column(conn) == 1  # a v1 file has no dictionary yet: sampled here
    conn.close()
    s2 = ep.EntityStore(db)
    assert s2.has_detail and s2.has_exceptions
    row = s2.get(EASY)
    assert row is not None and row.detail is not None
    assert row.detail["registeredAs"] == "007132601000"


# ---------------------------------------------------------------------------
# The consumers: anchor snapshot and subsidiary network
# ---------------------------------------------------------------------------


def test_anchor_snapshot_bundle_carries_exceptions_and_full_record(configured_store) -> None:
    from opencheck.sources.gleif import GleifAdapter

    bundle = GleifAdapter()._snapshot_bundle(TOP)
    assert bundle is not None and bundle["snapshot_fallback"] is True
    # TOP has no parents of either kind, but filed exceptions for both.
    assert bundle["direct_parent"] is None and bundle["ultimate_parent"] is None
    assert bundle["direct_parent_exception"]["attributes"]["reason"] == "NON_CONSOLIDATING"
    assert bundle["ultimate_parent_exception"]["attributes"]["reason"] == "NATURAL_PERSONS"
    # The mapper turns them into the bridge statements exactly as it does live.
    statements = mapper.map_gleif(bundle).statements
    details = [
        i["details"]
        for s in statements
        for i in (s["recordDetails"].get("interests") or [])
    ]
    assert any("NATURAL_PERSONS" in d or "natural person" in d.lower() for d in details)

    easy = GleifAdapter()._snapshot_bundle(EASY)
    assert easy is not None
    assert easy["record"]["attributes"]["entity"]["registeredAs"] == "007132601000"
    # A subject WITH a parent of a kind carries no exception for that kind.
    assert easy["direct_parent"]["attributes"]["lei"] == PARENT
    assert easy["direct_parent_exception"] is None
    assert easy["ultimate_parent"]["attributes"]["lei"] == TOP


async def test_subsidiary_snapshot_serves_the_ultimate_relation(configured_store) -> None:
    """Phase 146 had to declare the ultimate children unavailable under a
    GLEIF refusal; the ultimate index lets the store stand in for both."""
    from unittest.mock import patch

    from tests.test_subsidiaries import _FakeClient, _FakeCM, _FakeResponse

    client = _FakeClient(
        children={"direct": _FakeResponse(429), "ultimate": _FakeResponse(429)}
    )
    with patch.object(subs, "build_client", lambda: _FakeCM(client)):
        res = await subs.assemble_subsidiaries(TOP, include_bods=False)

    assert res["direct_available"] is True and res["ultimate_available"] is True
    assert res["snapshot_fallback"] is True and res["snapshot_date"] == "2026-09-07"
    assert res["direct_total"] == 1 and res["ultimate_total"] == 2
    relations = {r["lei"]: r["relation"] for r in res["children"]}
    assert relations == {PARENT: "both", EASY: "ultimate"}
    detail = res["degraded_detail"]
    assert "snapshot" in detail and "could not be checked" not in detail


# ---------------------------------------------------------------------------
# Boot download (Render's disk is ephemeral)
# ---------------------------------------------------------------------------


def test_boot_download_inflates_the_gzip_as_it_streams(
    db_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The mirror is downloaded gzipped and inflated straight to the live path:
    no archive is ever written beside the plain file, and the plain file only
    appears, complete, under its final name."""
    import gzip

    import httpx

    archive = gzip.compress(db_path.read_bytes())
    transport = httpx.MockTransport(lambda request: httpx.Response(200, content=archive))

    def fake_stream(method: str, url: str, **kwargs: Any):
        return httpx.Client(transport=transport).stream(method, url)

    monkeypatch.setattr(httpx, "stream", fake_stream)
    monkeypatch.setenv(
        "OPENCHECK_ENTITY_PAGES_DB_URL", "https://example.test/entity_pages.sqlite.gz"
    )
    monkeypatch.delenv("OPENCHECK_ENTITY_PAGES_DB_FILE", raising=False)
    get_settings.cache_clear()
    target = tmp_path / "boot" / "entity_pages.sqlite"
    monkeypatch.setattr(ep, "_db_path", lambda: target)
    try:
        result = ep.warm_entity_pages_db()
    finally:
        get_settings.cache_clear()
    assert result["entity_pages"].startswith(f"downloaded: {target}")
    assert target.read_bytes() == db_path.read_bytes()
    assert sorted(p.name for p in target.parent.iterdir()) == ["entity_pages.sqlite"]
    # And it is a usable mirror, dictionary and all.
    assert ep.EntityStore(target).get(EASY).detail is not None
