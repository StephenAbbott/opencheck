"""Romania — ONRC bulk index and ANAF live service.

The fixtures are shaped from the real 2 September 2026 export and real ANAF v9
responses, so the traps they pin are the ones the live data actually contains:
two registration-number formats for the same company, a date of birth with a
spurious time on it, a representative that is a company rather than a person,
and a fiscal code that reaches ANAF without any index at all.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from opencheck.bods import map_anaf_romania, map_onrc_romania
from opencheck.config import get_settings
from opencheck.findings import finding_anaf_romania, finding_onrc_romania
from opencheck.sources import onrc_romania
from opencheck.sources.anaf_romania import (
    COVERAGE_NO_INDEX,
    MAX_BATCH,
    AnafRomaniaAdapter,
    _normalise_ro_id,
)
from opencheck.sources.onrc_romania import (
    RO_ONRC_RA_CODE,
    RO_RA_CODES,
    RO_TAX_RA_CODE,
    OnrcRomaniaAdapter,
    names_agree,
    normalise_cui,
    normalise_registration_number,
    parse_ro_date,
    resolve_cui,
    to_new_format,
)

# ---------------------------------------------------------------------------
# Identifier grammar
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("14399840", "14399840"),
        ("RO14399840", "14399840"),
        ("ro 14399840", "14399840"),
        # ANAF takes ``cui`` as a JSON number, so the padded and unpadded
        # forms are the same query and the unpadded one round-trips.
        ("0000361", "361"),
        ("361", "361"),
    ],
)
def test_normalise_cui(raw: str, expected: str) -> None:
    assert normalise_cui(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "J40/1116/1991",
        "4325/A/2003",          # NGO register (RA000718)
        "CSC06FDIR/120135",     # ASF instruments registry (RA000498)
        "1",                    # one digit is not a CUI
        "12345678901",          # eleven is too many
    ],
)
def test_normalise_cui_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalise_cui(raw)


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("J40/1116/1991", "J40/1116/1991"),
        ("j40/1116/1991", "J40/1116/1991"),
        ("J40 / 1116 / 1991", "J40/1116/1991"),
        ("J2002000372404", "J2002000372404"),
        ("F40/22/1991", "F40/22/1991"),
        ("C40/53/2005", "C40/53/2005"),
    ],
)
def test_normalise_registration_number(raw: str, expected: str) -> None:
    assert normalise_registration_number(raw) == expected


@pytest.mark.parametrize("raw", ["", "14399840", "4325/A/2003", "CSC06FDIR/120135"])
def test_normalise_registration_number_rejects(raw: str) -> None:
    with pytest.raises(ValueError):
        normalise_registration_number(raw)


@pytest.mark.parametrize(
    "old,prefix",
    [
        # Every one of these was verified against the real export: the derived
        # prefix found exactly one company and its name matched GLEIF's.
        ("J40/15812/2017", "J2017015812" "40"),
        ("J23/6841/2022", "J2022006841" "23"),
        ("J20/3/2023", "J2023000003" "20"),
        ("J12/2551/2012", "J2012002551" "12"),
        ("J5/1227/2014", "J2014001227" "05"),
    ],
)
def test_to_new_format(old: str, prefix: str) -> None:
    assert to_new_format(old) == prefix
    assert len(to_new_format(old) or "") == 13


def test_to_new_format_ignores_already_new_and_junk() -> None:
    assert to_new_format("J2017015812405") is None
    assert to_new_format("14399840") is None
    assert to_new_format("") is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("13/02/1991", "1991-02-13"),
        ("1/2/1991", "1991-02-01"),
        # THE TRAP: a minority of ONRC birth dates carry a spurious time. It
        # is a data-entry artefact, not a precision claim, and must not reach
        # a BODS birthDate.
        ("19/06/1967 14:45:06", "1967-06-19"),
        ("", None),
        ("not a date", None),
        ("32/01/1991", None),
        ("13/13/1991", None),
    ],
)
def test_parse_ro_date(raw: str, expected: str | None) -> None:
    assert parse_ro_date(raw) == expected


def test_names_agree_across_register_spellings() -> None:
    assert names_agree("Formosa SRL", "FORMOSA S.R.L.")
    assert names_agree("T.Q.SERVICES SRL", "T.Q.SERVICES S.R.L.")
    assert names_agree("ETIQUETAS MONTLLO ROMANIA SRL", "Etiquetas Montllo România S.R.L.")
    # A rename, not a spelling: GLEIF held the old name for CUI 4467425 while
    # the registers had moved on. Refusing this is the point of the gate.
    assert not names_agree("UNLIMITED WHOLESALE & RETAIL ROM", "TRIPOP PRODCOM S.R.L.")
    assert not names_agree("", "FORMOSA S.R.L.")


def test_ra_codes_exclude_registers_that_cannot_reach_a_cui() -> None:
    """RA000718 (NGO) and RA000498 (ASF) are deliberately absent.

    Their numbers are real identifiers from other Romanian registers, but an
    association's fiscal code is not on its LEI record and cannot be derived
    from ``4325/A/2003``. Including them would dispatch a source that can only
    fail.
    """
    assert RO_RA_CODES == {RO_ONRC_RA_CODE, RO_TAX_RA_CODE}
    assert "RA000718" not in RO_RA_CODES
    assert "RA000498" not in RO_RA_CODES


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("14399840", "14399840"),
        ("RO14399840", "14399840"),
        ("J40/1116/1991", "J40/1116/1991"),
        ("J2002000372404", "J2002000372404"),
    ],
)
def test_deriver_accepts_both_kinds(raw: str, expected: str) -> None:
    assert _normalise_ro_id(raw) == expected


@pytest.mark.parametrize("raw", ["4325/A/2003", "CSC06FDIR/120135", ""])
def test_deriver_rejects_other_registers(raw: str) -> None:
    with pytest.raises(ValueError):
        _normalise_ro_id(raw)


# ---------------------------------------------------------------------------
# Index fixture
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE company (
    registration_number TEXT PRIMARY KEY, registration_prefix TEXT, cui TEXT,
    name TEXT, legal_form TEXT, registered_on TEXT, status_code TEXT,
    status TEXT, country TEXT, county TEXT, locality TEXT, address TEXT,
    postal_code TEXT, website TEXT, parent_country TEXT
);
CREATE TABLE representative (
    registration_number TEXT, seq INTEGER, name TEXT, role TEXT,
    role_slug TEXT, is_entity INTEGER, birth_date TEXT, birth_locality TEXT,
    birth_county TEXT, birth_country TEXT, locality TEXT, county TEXT,
    country TEXT
);
-- The builder writes this on every index; the fixture omitted it for three
-- phases, which is why nothing noticed that no index read declared its
-- provenance. A fixture missing a table the builder always creates cannot
-- fail a test about that table.
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""


@pytest.fixture()
def index(tmp_path: Path):
    """A three-company index in the real schema, pointed at by the setting."""
    path = tmp_path / "onrc.sqlite"
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            # Filed in the OLD format, as GLEIF holds it.
            ("J40/1116/1991", None, "412052", "TRANSIDEAL SRL", "SRL",
             "1991-03-14", "1048", "funcțiune", "România", "Bucureşti",
             "Bucureşti Sectorul 5", "Str. CAP. IVAN ANGHELACHE, 10", "064191",
             None, None),
            # Filed in the NEW format. GLEIF holds the OLD one — J40/15812/2017
            # — so only the prefix join can reach it.
            ("J2017015812405", "J2017015812" "40", "38218844",
             "IMAFLUX DESIGN SRL", "SRL", "2017-09-01", "1048", "funcțiune",
             "România", "Bucureşti", "Bucureşti Sectorul 3", "Str. Exemplu, 1",
             "030000", None, None),
            ("J40/9999/2001", None, "", "NO FISCAL CODE SRL", "SRL",
             "2001-01-01", "1084", "radiată", "România", "Cluj", "Cluj-Napoca",
             None, None, None, None),
        ],
    )
    conn.executemany(
        "INSERT INTO representative VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("J40/1116/1991", 0, "POPESCU ION", "administrator",
             "administrator", 0, "1967-06-19", "Municipiul Aiud", "Alba",
             "România", "Bucureşti", "Bucureşti", "România"),
            # The two-row corporate-directorship shape: a firm as
            # administrator, with its natural-person representative beside it.
            ("J40/1116/1991", 1, "CATEDRAL INSOLV IPURL",
             "administrator judiciar", "judicial_administrator", 1, None,
             None, None, None, "Cluj-Napoca", "Cluj", "România"),
            ("J40/1116/1991", 2, "GABOR MARIA",
             "reprezentant al persoanei juridice", "entity_representative", 0,
             "1972-03-15", "Cluj-Napoca", "Cluj", "România", "Cluj-Napoca",
             "Cluj", "România"),
        ],
    )
    conn.executemany(
        "INSERT INTO meta VALUES (?,?)",
        [
            ("dataset", "firme-02-09-2026"),
            # Deliberately LATER than the export date, so a test can tell which
            # of the two a snapshot is dated from. They are the same day in a
            # same-day build, which would make the two keys indistinguishable.
            ("built_at", "2026-09-15"),
            ("companies", "3"),
            ("licence", "CC-BY-4.0"),
        ],
    )
    conn.commit()
    conn.close()

    get_settings.cache_clear()
    import os

    os.environ["ONRC_ROMANIA_DB_FILE"] = str(path)
    onrc_romania.reset_connection()
    yield path
    os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
    get_settings.cache_clear()
    onrc_romania.reset_connection()


@pytest.fixture()
def no_index(tmp_path):
    """No index **anywhere** — neither named nor at the data-root default.

    Unsetting ``ONRC_ROMANIA_DB_FILE`` alone stopped meaning this once
    ``db_path`` gained its data-root fallback: a developer (or a boot
    download) who leaves an index in ``data/`` makes every "no index" test
    fail, on a real file rather than a bug. So the data root moves too.
    """
    import os

    os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
    previous_root = os.environ.get("OPENCHECK_DATA_ROOT")
    os.environ["OPENCHECK_DATA_ROOT"] = str(tmp_path / "empty-data-root")
    get_settings.cache_clear()
    onrc_romania.reset_connection()
    yield
    if previous_root is None:
        os.environ.pop("OPENCHECK_DATA_ROOT", None)
    else:
        os.environ["OPENCHECK_DATA_ROOT"] = previous_root
    get_settings.cache_clear()
    onrc_romania.reset_connection()


# ---------------------------------------------------------------------------
# resolve_cui
# ---------------------------------------------------------------------------


def test_resolve_cui_exact_match(index: Path) -> None:
    assert resolve_cui("J40/1116/1991") == "412052"


def test_resolve_cui_recovers_a_renumbered_company(index: Path) -> None:
    """The 6.8-point case: GLEIF holds the old number, ONRC holds the new one."""
    assert resolve_cui("J40/15812/2017") == "38218844"


def test_resolve_cui_checks_the_name_when_given_one(index: Path) -> None:
    assert resolve_cui("J40/1116/1991", legal_name="TRANSIDEAL S.R.L.") == "412052"
    # A former name on the LEI record must not attach the lookup to a company
    # the user did not ask about.
    assert resolve_cui("J40/1116/1991", legal_name="SOMETHING ELSE SRL") is None


def test_resolve_cui_returns_none_without_a_fiscal_code(index: Path) -> None:
    """2.0% of rows carry no CUI at all. That is not resolvable, not an error."""
    assert resolve_cui("J40/9999/2001") is None


def test_resolve_cui_without_an_index(no_index) -> None:
    assert resolve_cui("J40/1116/1991") is None
    assert onrc_romania.index_available() is False


# ---------------------------------------------------------------------------
# ONRC adapter and mapper
# ---------------------------------------------------------------------------


async def test_onrc_fetch_returns_a_stub_without_an_index(no_index) -> None:
    bundle = await OnrcRomaniaAdapter().fetch("J40/1116/1991", legal_name="Transideal")
    assert bundle["is_stub"] is True
    assert bundle["representatives"] == []


async def test_onrc_fetch_reads_the_index(index: Path) -> None:
    bundle = await OnrcRomaniaAdapter().fetch("J40/1116/1991", legal_name="Transideal")
    assert bundle["is_stub"] is False
    assert bundle["cui"] == "412052"
    assert bundle["name"] == "TRANSIDEAL SRL"
    assert len(bundle["representatives"]) == 3


async def test_onrc_fetch_finds_a_renumbered_company(index: Path) -> None:
    bundle = await OnrcRomaniaAdapter().fetch("J40/15812/2017")
    assert bundle["is_stub"] is False
    # The bundle carries the register's own spelling, not GLEIF's.
    assert bundle["registration_number"] == "J2017015812405"


async def test_onrc_mapper_emits_company_people_and_roles(index: Path) -> None:
    bundle = await OnrcRomaniaAdapter().fetch("J40/1116/1991")
    statements = list(map_onrc_romania(bundle))
    kinds = [s["recordType"] for s in statements]
    assert kinds.count("entity") == 2       # company + the IPURL firm
    assert kinds.count("person") == 2
    assert kinds.count("relationship") == 3

    company = statements[0]
    schemes = {
        i["scheme"]: i["id"]
        for i in company["recordDetails"]["identifiers"]
    }
    assert schemes["RO-ONRC"] == "J40/1116/1991"
    assert schemes["RO-CUI"] == "412052"

    interests = [
        s["recordDetails"]["interests"][0]
        for s in statements
        if s["recordType"] == "relationship"
    ]
    types = {i["type"] for i in interests}
    assert types == {"seniorManagingOfficial", "controlByLegalFramework"}
    # Nothing here is a beneficial ownership claim — ONRC publishes none.
    assert all(i["beneficialOwnershipOrControl"] is False for i in interests)
    # The register's own word for the role survives the mapping.
    assert any(i.get("details") == "administrator judiciar" for i in interests)


async def test_onrc_mapper_publishes_the_full_birth_date(index: Path) -> None:
    """Stephen's call, 14 Sep 2026: publish the date at the precision filed.

    The register publishes it openly under CC BY 4.0, and a full date is what
    gives Romanian person matching a corroborating attribute.
    """
    bundle = await OnrcRomaniaAdapter().fetch("J40/1116/1991")
    people = [s for s in map_onrc_romania(bundle) if s["recordType"] == "person"]
    dates = {p["recordDetails"].get("birthDate") for p in people}
    assert "1967-06-19" in dates


async def test_onrc_mapper_yields_nothing_for_a_stub(no_index) -> None:
    bundle = await OnrcRomaniaAdapter().fetch("J40/1116/1991")
    assert list(map_onrc_romania(bundle)) == []


async def test_onrc_finding(index: Path) -> None:
    bundle = await OnrcRomaniaAdapter().fetch("J40/1116/1991")
    sentence = finding_onrc_romania(bundle)
    assert sentence is not None
    assert sentence.startswith("Funcțiune")
    assert "3 legal representatives" in sentence
    assert sentence.endswith(".")


# ---------------------------------------------------------------------------
# ANAF — live service
# ---------------------------------------------------------------------------

_ANAF_RECORD: dict[str, Any] = {
    "date_generale": {
        "data": "2026-09-14",
        "cui": 14399840,
        "denumire": "DANTE INTERNATIONAL SA",
        "adresa": "MUNICIPIUL BUCUREŞTI, SECTOR 2, STR. GARA HERĂSTRĂU, NR.6",
        "nrRegCom": "J2002000372404",
        "stare_inregistrare": "INREGISTRAT din data 29.08.2006",
        "data_inregistrare": "2002-01-23",
        "cod_CAEN": "4754",
        "forma_juridica": "SOCIETATE COMERCIALĂ PE ACŢIUNI",
        "forma_organizare": "PERSOANA JURIDICA",
        "statusRO_e_Factura": False,
    },
    "inregistrare_scop_Tva": {
        "scpTVA": True,
        "perioade_TVA": [{"data_inceput_ScpTVA": "2002-02-01", "data_sfarsit_ScpTVA": ""}],
    },
    "stare_inactiv": {
        "statusInactivi": False,
        "dataInactivare": "",
        "dataReactivare": "",
        "dataPublicare": "",
        "dataRadiere": "",
    },
    "adresa_sediu_social": {
        "sdenumire_Strada": "Şos. Virtuţii",
        "snumar_Strada": "148",
        "sdenumire_Localitate": "Sector 6 Mun. Bucureşti",
        "sdenumire_Judet": "MUNICIPIUL BUCUREŞTI",
        "scod_Postal": "60787",
        "sdetalii_Adresa": "spatiul E47",
    },
    "adresa_domiciliu_fiscal": {
        "ddenumire_Strada": "Str. Gara Herăstrău",
        "dnumar_Strada": "6",
        "ddenumire_Localitate": "Sector 2 Mun. Bucureşti",
        "ddenumire_Judet": "MUNICIPIUL BUCUREŞTI",
        "ddetalii_Adresa": "Cladirea Globalworth Square",
    },
}


class _FakeResponse:
    def __init__(self, payload: Any, status_code: int = 200, text: str = "") -> None:
        self._payload = payload
        self.status_code = status_code
        self._text = text

    @property
    def is_success(self) -> bool:
        return 200 <= self.status_code < 300

    def json(self) -> Any:
        if self._payload is None:
            raise ValueError("not JSON")
        return self._payload


class _FakeClient:
    """Captures the POST body so the batching contract can be asserted."""

    def __init__(self, payload: Any, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.posts: list[list[dict[str, Any]]] = []

    async def __aenter__(self) -> "_FakeClient":
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def post(self, url: str, *, content: bytes, headers: dict[str, str]):
        self.posts.append(json.loads(content.decode("utf-8")))
        return _FakeResponse(self.payload, self.status_code)


@pytest.fixture()
def anaf_client(monkeypatch):
    """Patch ``build_client`` in the adapter and hand back the fake."""

    def _install(payload: Any, status_code: int = 200) -> _FakeClient:
        client = _FakeClient(payload, status_code)
        monkeypatch.setattr(
            "opencheck.sources.anaf_romania.build_client", lambda: client
        )
        return client

    return _install


async def test_anaf_fetch_by_fiscal_code_needs_no_index(no_index, anaf_client) -> None:
    """47.8% of RO LEI holders take this path — the index is irrelevant here."""
    client = anaf_client({"found": [_ANAF_RECORD], "notFound": []})
    bundle = await AnafRomaniaAdapter().fetch("14399840", legal_name="Dante")
    assert bundle["is_stub"] is False
    assert bundle["cui"] == "14399840"
    assert bundle["registration_number"] == "J2002000372404"
    # The request is exactly ANAF's documented shape.
    assert client.posts == [[{"cui": 14399840, "data": bundle["record"]["date_generale"]["data"]}]] or (
        client.posts[0][0]["cui"] == 14399840
    )


async def test_anaf_says_so_when_no_index_can_resolve_a_j_number(no_index) -> None:
    """The honest-degradation case: not asked, and the card says why."""
    bundle = await AnafRomaniaAdapter().fetch("J40/1116/1991", legal_name="Transideal")
    assert bundle["record"] is None
    assert bundle["coverage_note"] == COVERAGE_NO_INDEX
    assert bundle["is_stub"] is False  # a note card, not a placeholder
    assert list(map_anaf_romania(bundle)) == []
    assert "Not queried" in (finding_anaf_romania(bundle) or "")


async def test_anaf_resolves_a_j_number_through_the_index(index, anaf_client) -> None:
    client = anaf_client({"found": [_ANAF_RECORD], "notFound": []})
    bundle = await AnafRomaniaAdapter().fetch("J40/1116/1991", legal_name="TRANSIDEAL SRL")
    assert bundle["coverage_note"] is None
    # The index turned the register number into the fiscal code ANAF was asked.
    assert client.posts[0][0]["cui"] == 412052


async def test_anaf_not_found_is_not_a_stub(no_index, anaf_client) -> None:
    anaf_client({"found": [], "notFound": [99999999]})
    bundle = await AnafRomaniaAdapter().fetch("99999999")
    assert bundle["not_found"] is True
    assert list(map_anaf_romania(bundle)) == []
    assert finding_anaf_romania(bundle) == "No taxpayer record under this fiscal code."


async def test_anaf_batches_at_the_documented_ceiling(no_index, anaf_client) -> None:
    """ANAF states 100 CUIs per request; 250 must become three requests."""
    client = anaf_client({"found": [], "notFound": []})
    await AnafRomaniaAdapter().fetch_many([str(n) for n in range(1000, 1250)])
    assert [len(p) for p in client.posts] == [MAX_BATCH, MAX_BATCH, 50]


async def test_anaf_records_a_degradation_on_an_html_body(no_index, anaf_client, monkeypatch) -> None:
    """A WAF page must never read as 'the register had nothing to say'."""
    from opencheck import degradation

    anaf_client(None)  # .json() raises, as an HTML body would
    with degradation.recording() as recorded:
        bundle = await AnafRomaniaAdapter().fetch("14399840")
    assert bundle["not_found"] is True
    assert any(d.source_id == "anaf_romania" for d in recorded)


async def test_anaf_mapper_emits_the_company_with_both_identifiers(no_index, anaf_client) -> None:
    anaf_client({"found": [_ANAF_RECORD], "notFound": []})
    bundle = await AnafRomaniaAdapter().fetch("14399840")
    statements = list(map_anaf_romania(bundle))
    assert len(statements) == 1
    stmt = statements[0]
    schemes = {i["scheme"]: i["id"] for i in stmt["recordDetails"]["identifiers"]}
    # ANAF repeats ONRC's number rather than asserting one of its own, and it
    # is emitted under ONRC's scheme so the reconciler can corroborate.
    assert schemes == {"RO-CUI": "14399840", "RO-ONRC": "J2002000372404"}
    assert stmt["recordDetails"]["foundingDate"] == "2002-01-23"
    assert "dissolutionDate" not in stmt["recordDetails"]


async def test_anaf_mapper_reads_the_striking_off_date(no_index, anaf_client) -> None:
    record = json.loads(json.dumps(_ANAF_RECORD))
    record["stare_inactiv"]["dataRadiere"] = "2019-04-30"
    anaf_client({"found": [record], "notFound": []})
    bundle = await AnafRomaniaAdapter().fetch("14399840")
    stmt = next(iter(map_anaf_romania(bundle)))
    assert stmt["recordDetails"]["dissolutionDate"] == "2019-04-30"


async def test_anaf_inactive_is_pending_not_dissolved(no_index, anaf_client) -> None:
    """A taxpayer declared inactive still exists. Never ``terminal``."""
    record = json.loads(json.dumps(_ANAF_RECORD))
    record["stare_inactiv"]["statusInactivi"] = True
    record["stare_inactiv"]["dataInactivare"] = "2024-02-01"
    anaf_client({"found": [record], "notFound": []})
    bundle = await AnafRomaniaAdapter().fetch("14399840")
    stmt = next(iter(map_anaf_romania(bundle)))
    assert "dissolutionDate" not in stmt["recordDetails"]
    descriptions = " ".join(
        a.get("description", "") for a in stmt.get("annotations", [])
    )
    assert "terminal process" in descriptions
    assert "On the inactive-taxpayer register" in (finding_anaf_romania(bundle) or "")


async def test_anaf_finding_leads_with_registration(no_index, anaf_client) -> None:
    anaf_client({"found": [_ANAF_RECORD], "notFound": []})
    bundle = await AnafRomaniaAdapter().fetch("14399840")
    sentence = finding_anaf_romania(bundle)
    assert sentence is not None
    assert sentence.startswith("Registered since 2002-01-23")
    assert "registered for VAT" in sentence
    assert len(sentence) <= 140


# ---------------------------------------------------------------------------
# The builder and the adapter must derive the prefix the same way (Phase 211)
# ---------------------------------------------------------------------------


def _load_builder():
    """Import ``scripts/build_onrc_romania_index.py`` by path."""
    import importlib.util

    path = (
        Path(__file__).resolve().parent.parent
        / "scripts"
        / "build_onrc_romania_index.py"
    )
    spec = importlib.util.spec_from_file_location("_build_onrc_index", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("J40/15812/2017", "J201701581240"),   # old → converted
        ("J2017015812405", "J201701581240"),   # new → its own first thirteen
        ("J 40 / 15812 / 2017", "J201701581240"),
        ("14399840", None),                    # a fiscal code is not a number
        ("", None),
    ],
)
def test_prefix_for_derives_from_either_format(raw: str, expected: str | None) -> None:
    """Both spellings of one company must land on the same key.

    ``to_new_format`` answers only half the question — it returns None for a
    number already in the new format — and using it on its own is what left
    new-format rows unjoinable.
    """
    assert onrc_romania.prefix_for(raw) == expected


def test_the_builder_stores_the_prefix_the_adapter_looks_up(tmp_path: Path) -> None:
    """End-to-end through the real builder, then the real adapter.

    This is the test the Phase 207 shape could not have: the fixture index in
    this file hand-wrote ``registration_prefix`` the *correct* way while the
    builder wrote it the *wrong* way, so every adapter test passed against an
    index no builder would ever produce. Nothing compared the two sides.

    The company here is filed under its NEW number and looked up by its OLD
    one, which is the 38.6% of Romanian LEI lookups that silently missed.
    """
    builder = _load_builder()
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "od_firme.csv").write_text(
        "﻿DENUMIRE^CUI^COD_INMATRICULARE^DATA_INMATRICULARE^FORMA_JURIDICA"
        "^ADR_TARA^ADR_JUDET^ADR_LOCALITATE^ADR_DEN_STRADA\r\n"
        "IMAFLUX DESIGN SRL^38218844^J2017015812405^01/09/2017^SRL"
        "^România^Bucureşti^Sector 3^Str. Exemplu\r\n",
        encoding="utf-8",
    )
    (csv_dir / "od_reprezentanti_legali.csv").write_text(
        "﻿COD_INMATRICULARE^PERSOANA_IMPUTERNICITA^CALITATE^DATA_NASTERE\r\n"
        "J2017015812405^POPESCU ION^administrator^19/06/1967\r\n",
        encoding="utf-8",
    )

    out = tmp_path / "onrc.sqlite"
    builder.build(csv_dir, out, dataset="firme-02-09-2026", nomen_dataset="")

    import os

    get_settings.cache_clear()
    os.environ["ONRC_ROMANIA_DB_FILE"] = str(out)
    onrc_romania.reset_connection()
    try:
        # The old-format number GLEIF actually holds for this company.
        assert onrc_romania.resolve_cui("J40/15812/2017") == "38218844"
        row = onrc_romania.company_row("J40/15812/2017")
        assert row is not None
        assert row["name"] == "IMAFLUX DESIGN SRL"
    finally:
        os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
        get_settings.cache_clear()
        onrc_romania.reset_connection()


def test_the_builder_keeps_only_the_indexes_something_queries() -> None:
    """131 MB of the 1,242 MB index served no query at all.

    ``idx_company_cui`` was never filtered on, and ``idx_company_name`` backs
    only a leading-wildcard LIKE, which SQLite cannot answer from an index.
    """
    builder = _load_builder()
    assert "idx_company_prefix" in builder.INDEXES
    assert "idx_rep_number" in builder.INDEXES
    assert "idx_company_cui" not in builder.INDEXES
    assert "idx_company_name" not in builder.INDEXES


# ---------------------------------------------------------------------------
# Dispatch is gated on the index file
# ---------------------------------------------------------------------------


def test_onrc_is_not_announced_without_an_index(no_index) -> None:
    """No file, no source card — not an empty one.

    A registered source that announces itself and then says it has nothing
    counts itself in "N of N sources answered" while answering nothing.
    """
    adapter = onrc_romania.OnrcRomaniaAdapter()
    assert adapter.covers_lei("315700V12MDD9PTKU295") is False


def test_onrc_is_announced_once_an_index_exists(index: Path) -> None:
    adapter = onrc_romania.OnrcRomaniaAdapter()
    assert adapter.covers_lei("315700V12MDD9PTKU295") is True


async def test_a_company_outside_the_index_gets_a_coverage_note(index: Path) -> None:
    """Present index, absent company — say why rather than look empty.

    With the index scoped to the LEI population this is a real case: 199 of
    8,677 dispatchable Romanian LEIs land here, most of them sole traders.
    """
    from opencheck.findings import finding_onrc_romania

    bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("J40/55555/2019")
    # The INPI shape, and load-bearing: ``_lookup_pipeline`` drops every
    # is_stub result above the registry branch, so a True here meant this
    # card never rendered at all. Phase 212.
    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert bundle["coverage_note"]
    sentence = finding_onrc_romania(bundle)
    assert sentence and "not in the indexed extract" in sentence.lower()


async def test_the_prefix_join_works_in_both_directions(tmp_path: Path) -> None:
    """GLEIF's spelling and ONRC's can differ either way round.

    The builder-side fix alone passes a test that only looks up an old-format
    number, because converting old→new is what ``to_new_format`` already did.
    The *adapter* side is what a company filed under its OLD number and held by
    GLEIF under its NEW one needs — 2.19M of ONRC's 2.86M rows are old-format
    and ~23% of Romanian LEI records carry the new spelling, so this pairing is
    ordinary, not exotic.
    """
    builder = _load_builder()
    csv_dir = tmp_path / "csv"
    csv_dir.mkdir()
    (csv_dir / "od_firme.csv").write_text(
        "﻿DENUMIRE^CUI^COD_INMATRICULARE^DATA_INMATRICULARE^FORMA_JURIDICA\r\n"
        # Filed OLD in the register…
        "CALLINVEST SRL^30687535^J12/2551/2012^05/06/2012^SRL\r\n"
        # …and NEW, for the other direction.
        "IMAFLUX DESIGN SRL^38218844^J2017015812405^01/09/2017^SRL\r\n",
        encoding="utf-8",
    )
    (csv_dir / "od_reprezentanti_legali.csv").write_text(
        "﻿COD_INMATRICULARE^PERSOANA_IMPUTERNICITA^CALITATE^DATA_NASTERE\r\n",
        encoding="utf-8",
    )
    out = tmp_path / "onrc.sqlite"
    builder.build(csv_dir, out, dataset="firme-02-09-2026", nomen_dataset="")

    import os

    get_settings.cache_clear()
    os.environ["ONRC_ROMANIA_DB_FILE"] = str(out)
    onrc_romania.reset_connection()
    try:
        # old in GLEIF -> new in ONRC
        assert onrc_romania.resolve_cui("J40/15812/2017") == "38218844"
        # new in GLEIF -> old in ONRC  (the adapter-side half)
        assert onrc_romania.resolve_cui("J2012002551125") == "30687535"
    finally:
        os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
        get_settings.cache_clear()
        onrc_romania.reset_connection()


# ---------------------------------------------------------------------------
# The boot download (Phase 211 follow-up)
# ---------------------------------------------------------------------------


def test_the_reader_and_the_downloader_agree_on_the_path(tmp_path: Path, monkeypatch) -> None:
    """`_connect` and `warm_index` must resolve to the same file.

    They did not at first: the download wrote to the data-root default while
    the reader required `ONRC_ROMANIA_DB_FILE` and returned None without it,
    so a perfectly good downloaded index was ignored and the source stayed
    dark — failing closed, and silently, which is the worst shape.
    """
    monkeypatch.setattr("opencheck.cache.data_root", lambda: tmp_path)
    get_settings.cache_clear()
    import os

    os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
    try:
        assert onrc_romania.db_path() == tmp_path / "onrc_romania.sqlite"
    finally:
        get_settings.cache_clear()
        onrc_romania.reset_connection()


def test_an_index_at_the_default_path_is_read_without_the_env_var(
    tmp_path: Path, monkeypatch, index: Path
) -> None:
    """A downloaded index works with no `ONRC_ROMANIA_DB_FILE` set at all."""
    import os
    import shutil

    default_dir = tmp_path / "data"
    default_dir.mkdir()
    shutil.copy(index, default_dir / "onrc_romania.sqlite")

    monkeypatch.setattr("opencheck.cache.data_root", lambda: default_dir)
    os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
    get_settings.cache_clear()
    onrc_romania.reset_connection()
    try:
        assert onrc_romania.index_available() is True
        assert onrc_romania.resolve_cui("J40/1116/1991") == "412052"
    finally:
        get_settings.cache_clear()
        onrc_romania.reset_connection()


def test_warm_index_without_a_url_is_a_state_not_a_failure(monkeypatch) -> None:
    """An empty URL disables the download rather than erroring."""
    import os

    os.environ["ONRC_ROMANIA_DB_URL"] = ""
    get_settings.cache_clear()
    try:
        result = onrc_romania.warm_index()
        assert "no URL" in result["onrc_romania"]
    finally:
        os.environ.pop("ONRC_ROMANIA_DB_URL", None)
        get_settings.cache_clear()
        onrc_romania.reset_connection()


def test_warm_index_never_raises_when_the_asset_is_unreachable(monkeypatch) -> None:
    """No index is a state the pipeline handles; a boot crash is not.

    `covers_lei` returns False and the source is simply not announced.
    """
    def _boom(*args, **kwargs):
        raise RuntimeError("release asset unreachable")

    monkeypatch.setattr("opencheck.entity_pages.asset_check", _boom)
    monkeypatch.setattr("opencheck.entity_pages.download_db", _boom)
    result = onrc_romania.warm_index()
    assert result["onrc_romania"].startswith("failed:")


def test_the_configured_asset_url_points_at_a_gzipped_sqlite() -> None:
    """A URL typo fails closed and silently — pin the shape, not the bytes."""
    url = get_settings().onrc_romania_db_url
    assert url.startswith("https://github.com/StephenAbbott/opencheck/releases/download/")
    assert url.endswith(".sqlite.gz"), "download_db only inflates a .gz"


# ---------------------------------------------------------------------------
# Provenance: a card full of register rows must not read as a placeholder
# ---------------------------------------------------------------------------
#
# ``SourceHit.liveness`` and the provenance recorder are two different
# declarations, and ONRC set only the first — on ``search`` at that, never on
# the ``fetch`` the lookup actually calls. The recorder defaults to ``stub``,
# whose frontend label is "Placeholder data — no live source was contacted",
# so every ONRC card in production badged real Trade Register data as a
# placeholder. Found by reading a live lookup response, not by the suite:
# nothing here had ever asserted what a Romanian card claims about itself.


async def test_a_real_index_read_declares_a_snapshot_not_a_stub(index: Path) -> None:
    """The bug itself. Fails on the unfixed adapter with liveness 'stub'."""
    from opencheck import provenance

    with provenance.recording() as recorder:
        bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("J40/1116/1991")
        resolved = recorder.resolve(is_stub=bundle["is_stub"])

    assert bundle["is_stub"] is False
    assert resolved.liveness == "snapshot"
    assert resolved.label == "Snapshot"
    assert "ONRC" in (resolved.detail or "")


async def test_the_snapshot_is_dated_from_the_export_not_the_build(index: Path) -> None:
    """``record_snapshot`` asks for the upstream extract date.

    The fixture's ``built_at`` (15 Sep) is deliberately later than its
    ``dataset`` slug (02-09-2026), so reading the wrong key fails here. Which
    one is right is not a style question: it is the export date that decides
    whether a row is stale, and rebuilding an old dump tomorrow does not make
    its rows a day old.
    """
    from opencheck import provenance

    with provenance.recording() as recorder:
        await onrc_romania.OnrcRomaniaAdapter().fetch("J40/1116/1991")
        resolved = recorder.resolve()

    assert resolved.retrieved_at is not None
    assert resolved.retrieved_at.date().isoformat() == "2026-09-02"
    assert "firme-02-09-2026" in (resolved.detail or "")


def test_the_export_date_falls_back_when_the_slug_is_absent(tmp_path: Path) -> None:
    """The shipped 4.3 MB asset predates ``built_at``; others may lack ``dataset``.

    A reader that insisted on either key would report nothing for a real file,
    so both are tried and neither is required.
    """
    assert onrc_romania.export_date({"dataset": "firme-02-09-2026"}) is not None
    assert onrc_romania.export_date({"built_at": "2026-09-15"}) is not None
    assert onrc_romania.export_date({}) is None
    # A malformed slug is data, not a crash.
    assert onrc_romania.export_date({"dataset": "firme-99-99-2026"}) is None


async def test_an_index_with_no_meta_still_declares_a_snapshot(
    tmp_path: Path, monkeypatch
) -> None:
    """An undated index is still a bulk read, not a placeholder.

    Losing the date must not cost the liveness claim — that trade would put
    the card back on "Placeholder data" for the sake of a missing row.
    """
    import os

    from opencheck import provenance

    path = tmp_path / "no-meta.sqlite"
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    conn.execute("DROP TABLE meta")
    conn.execute(
        "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        ("J40/1116/1991", None, "412052", "TRANSIDEAL SRL", "SRL",
         "1991-03-14", "1048", "funcțiune", "România", "Bucureşti",
         "Bucureşti Sectorul 5", "Str. Exemplu, 1", "064191", None, None),
    )
    conn.commit()
    conn.close()

    os.environ["ONRC_ROMANIA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    onrc_romania.reset_connection()
    try:
        with provenance.recording() as recorder:
            await onrc_romania.OnrcRomaniaAdapter().fetch("J40/1116/1991")
            resolved = recorder.resolve()
        assert resolved.liveness == "snapshot"
        assert resolved.retrieved_at is None, "no date is better than a made-up one"
    finally:
        os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
        get_settings.cache_clear()
        onrc_romania.reset_connection()


async def test_a_company_absent_from_the_index_claims_no_read(index: Path) -> None:
    """A coverage-note card claims no read, even though it is not a stub.

    Phase 212 flipped ``is_stub`` to False so the card survives the pipeline's
    blanket stub drop. The provenance claim must not ride along with it: the
    recorder is told nothing on a miss, so it still resolves to ``stub``. The
    card is real; the read it would describe never happened.
    """
    from opencheck import provenance

    with provenance.recording() as recorder:
        bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("J40/55555/2024")
        resolved = recorder.resolve(is_stub=bundle["is_stub"])

    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert resolved.liveness == "stub"


async def test_no_index_claims_no_read(no_index) -> None:
    """No file, nothing declared."""
    from opencheck import provenance

    with provenance.recording() as recorder:
        bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("J40/1116/1991")
        resolved = recorder.resolve(is_stub=bundle["is_stub"])

    assert resolved.liveness == "stub"


# ---------------------------------------------------------------------------
# A fiscal code is the other half of the population (Phase 212)
# ---------------------------------------------------------------------------
#
# GLEIF files a CUI for 4,854 of the 8,677 dispatchable Romanian LEI records
# and a J-number for 3,821. Until this phase ONRC only understood the second
# kind, so it reached 41.8% of the population and was announced-and-silent for
# the rest: `sources_applicable` named it, and no card and no error followed.


async def test_a_fiscal_code_reaches_the_company(index: Path) -> None:
    """The case that produced nothing at all before.

    ALMO SRL in production: GLEIF holds `1468817`, the register files it under
    `J1991000698388`, and no ONRC card was rendered.
    """
    bundle = await onrc_romania.OnrcRomaniaAdapter().fetch(
        "412052", legal_name="TRANSIDEAL SRL"
    )
    assert bundle["is_stub"] is False
    assert bundle.get("not_found") is None
    assert bundle["registration_number"] == "J40/1116/1991"
    assert bundle["cui"] == "412052"
    assert bundle["representatives"], "the representatives are the point of ONRC"


async def test_both_identifier_shapes_reach_the_same_company(index: Path) -> None:
    """A CUI and a J-number for one company must not disagree."""
    adapter = onrc_romania.OnrcRomaniaAdapter()
    by_cui = await adapter.fetch("38218844")
    by_number = await adapter.fetch("J40/15812/2017")
    assert by_cui["registration_number"] == by_number["registration_number"]
    assert by_cui["cui"] == by_number["cui"] == "38218844"


def test_a_fiscal_code_on_several_rows_is_decided_by_status(tmp_path: Path) -> None:
    """A CUI is NOT unique in the register — 469 of them sit on >1 row.

    Always the same company under successive registration numbers. Restricted
    to `funcțiune` the shipped index holds 6,764 rows and 6,764 distinct
    fiscal codes, so status decides it exactly. Pinned here on the real shape:
    one struck-off registration, one live one.
    """
    import os

    path = tmp_path / "dupes.sqlite"
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("J40/11171/2011", None, "29114704", "CONVERSION MEDIA SRL", "SRL",
             "2011-01-01", "1084", "radiată", "România", "Bucureşti",
             "Bucureşti", None, None, None, None),
            ("J23/4771/2016", None, "29114704", "CONVERSION MEDIA SRL", "SRL",
             "2016-01-01", "1048", "funcțiune", "România", "Ilfov",
             "Voluntari", None, None, None, None),
        ],
    )
    conn.commit()
    conn.close()

    os.environ["ONRC_ROMANIA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    onrc_romania.reset_connection()
    try:
        row = onrc_romania.company_by_cui("29114704")
        assert row is not None
        assert row["registration_number"] == "J23/4771/2016"
        assert row["status"] == onrc_romania.ACTIVE_STATUS
    finally:
        os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
        get_settings.cache_clear()
        onrc_romania.reset_connection()


def test_an_undecidable_fiscal_code_is_refused_not_guessed(tmp_path: Path) -> None:
    """Two live registrations for one CUI — refuse, as the prefix join does."""
    import os

    path = tmp_path / "ambiguous.sqlite"
    conn = sqlite3.connect(str(path))
    conn.executescript(_SCHEMA)
    conn.executemany(
        "INSERT INTO company VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ("J40/1/2011", None, "555", "TWO LIVE SRL", "SRL", "2011-01-01",
             "1048", "funcțiune", "România", "B", "B", None, None, None, None),
            ("J40/2/2012", None, "555", "TWO LIVE SRL", "SRL", "2012-01-01",
             "1048", "funcțiune", "România", "B", "B", None, None, None, None),
        ],
    )
    conn.commit()
    conn.close()

    os.environ["ONRC_ROMANIA_DB_FILE"] = str(path)
    get_settings.cache_clear()
    onrc_romania.reset_connection()
    try:
        assert onrc_romania.company_by_cui("555") is None
    finally:
        os.environ.pop("ONRC_ROMANIA_DB_FILE", None)
        get_settings.cache_clear()
        onrc_romania.reset_connection()


async def test_an_unresolvable_fiscal_code_says_which_miss_it_was(index: Path) -> None:
    """Two misses, two sentences.

    Saying "not in the indexed extract" for a fiscal code that merely failed to
    pick out one registration would report a company as absent when it may be
    sitting there under several historical numbers.
    """
    from opencheck.findings import finding_onrc_romania

    bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("99999999")
    assert bundle["is_stub"] is False
    assert bundle["not_found"] is True
    assert bundle["coverage_note"] == onrc_romania.COVERAGE_CUI_UNRESOLVED
    sentence = finding_onrc_romania(bundle) or ""
    assert "fiscal code" in sentence.lower()
    assert "not in the indexed extract" not in sentence.lower()


async def test_a_miss_maps_to_no_bods_statements(index: Path) -> None:
    """The mapper reads the same flag the card does, or a miss becomes a node."""
    from opencheck.bods import map_onrc_romania

    bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("99999999")
    assert list(map_onrc_romania(bundle)) == []


async def test_no_index_is_still_a_stub_and_is_dropped(no_index) -> None:
    """Nothing consulted, nothing to show. Distinct from a miss.

    Without an index `covers_lei` is False and the source is never dispatched,
    so this bundle should never reach a card — `is_stub` True is what makes
    the pipeline drop it if it somehow does.
    """
    bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("J40/1116/1991")
    assert bundle["is_stub"] is True
    assert "not_found" not in bundle
