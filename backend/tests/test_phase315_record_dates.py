"""Phase 315 — ``statementDate`` from the dates the sources already publish.

Phase C of the BODS dates audit (Notion "📆 Audit of dates captured across
OpenCheck", §5–§6). Every field used here was confirmed on a real production
payload on 9 Oct 2026 before it was wired, and the rule throughout is the
same: a **record-level** date the source keeps for the record — a
last-modified stamp, the latest filing, the declaration that names the party —
never a periodic declaration that later register changes could postdate, and
never a date after the day it is read.

Two production 500s found by that survey are pinned here too: PRH's YTJ v3
returns ``mainBusinessLine`` as an object, and CRO's CKAN types
``nace_v2_code`` as a number.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest

import opencheck.bods as bods_pkg
from opencheck import provenance
from opencheck.bods.statements import dated_by_cut, latest_record_date, record_date
from opencheck.provenance import Provenance
from tests.test_onrc_romania import index  # noqa: F401  (fixture)

TODAY = dt.datetime.now(dt.timezone.utc).date().isoformat()
#: Mapped under a live provenance that would date everything today unless the
#: mapper passed a date of its own — so a statement NOT dated today proves it.
_LIVE = Provenance(liveness="live", retrieved_at=dt.datetime.now(dt.timezone.utc))


def _map(mapper: Any, bundle: dict[str, Any]) -> list[dict[str, Any]]:
    with provenance.mapping_provenance(_LIVE):
        return list(mapper(bundle))


def _dates(statements: list[dict[str, Any]], record_type: str | None = None) -> set[str]:
    return {
        s["statementDate"]
        for s in statements
        if record_type is None or s["recordType"] == record_type
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestRecordDate:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("2026-09-02", "2026-09-02"),
            ("2026-09-02T02:22:05", "2026-09-02"),
            ("2026-09-22T17:41:10+02:00", "2026-09-22"),
            ("2026-09-30T04:00:47.019306", "2026-09-30"),
            ("2026-09", None),
            ("02.09.2026", None),
            ("", None),
            (None, None),
            (1791546473000, None),  # epoch ms is the mapper's helper's job
        ],
    )
    def test_reads_only_a_full_iso_day(self, raw, expected):
        assert record_date(raw) == expected

    def test_a_future_date_is_not_a_declaration(self):
        tomorrow = (dt.date.today() + dt.timedelta(days=1)).isoformat()
        assert record_date(tomorrow) is None
        assert latest_record_date(["2026-01-01", tomorrow]) == "2026-01-01"

    def test_latest_ignores_unreadable_values(self):
        assert latest_record_date(["2024-03-11", None, "x", "2025-01-02T00:00:00Z"]) == "2025-01-02"
        assert latest_record_date([]) is None

    def test_dated_by_cut_stamps_every_statement_or_none(self):
        stmts = [{"statementDate": TODAY}, {"statementDate": TODAY}]
        assert _dates(list(dated_by_cut(iter(stmts), "2026-10-05"))) == {"2026-10-05"}
        stmts = [{"statementDate": TODAY}]
        assert _dates(list(dated_by_cut(iter(stmts), None))) == {TODAY}


# ---------------------------------------------------------------------------
# FtM — OpenSanctions / EveryPolitician ``last_change``
# ---------------------------------------------------------------------------


def _ftm_bundle() -> dict[str, Any]:
    owner = {
        "id": "NK-owner", "schema": "Company", "caption": "Owner Ltd",
        "properties": {"name": ["Owner Ltd"]},
        "first_seen": "2024-01-01T00:00:00", "last_seen": "2026-09-29T00:00:00",
        "last_change": "2025-06-30T10:00:00",
    }
    edge = {
        "id": "NK-edge", "schema": "Ownership",
        "properties": {"owner": [owner], "asset": ["NK-subject"]},
        "first_seen": "2024-01-01T00:00:00", "last_seen": "2026-09-29T00:00:00",
        "last_change": "2025-11-04T15:57:36",
    }
    return {
        "entity": {
            "id": "NK-subject", "schema": "Company", "caption": "Subject PLC",
            "properties": {"name": ["Subject PLC"], "ownershipOwner": [edge]},
            "first_seen": "2024-06-30T00:00:00",
            "last_seen": "2026-09-29T02:23:29",
            "last_change": "2026-09-02T02:22:05",
        }
    }


class TestFtm:
    def test_each_record_is_dated_by_its_own_last_change(self):
        out = _map(bods_pkg.map_opensanctions, _ftm_bundle())
        by_name = {
            (s["recordDetails"].get("name") or s["recordType"]): s["statementDate"]
            for s in out
        }
        assert by_name["Subject PLC"] == "2026-09-02"
        assert by_name["Owner Ltd"] == "2025-06-30"
        # The edge is its own record, with its own date.
        assert by_name["relationship"] == "2025-11-04"

    def test_last_seen_is_never_used(self):
        bundle = _ftm_bundle()
        del bundle["entity"]["last_change"]
        out = _map(bods_pkg.map_opensanctions, bundle)
        subject = next(s for s in out if s["recordDetails"].get("name") == "Subject PLC")
        assert subject["statementDate"] == TODAY  # falls back; 2026-09-29 is a crawl

    def test_openaleph_has_no_record_date_and_falls_back(self):
        bundle = _ftm_bundle()
        for key in ("first_seen", "last_seen", "last_change"):
            bundle["entity"].pop(key)
        out = _map(bods_pkg.map_openaleph, {"entity": {
            "id": "oa-1", "schema": "Company", "properties": {"name": ["X"]}}})
        assert _dates(out) == {TODAY}

    def test_everypolitician_person(self):
        out = _map(bods_pkg.map_everypolitician, {"entity": {
            "id": "Q717674", "schema": "Person", "properties": {"name": ["Gerrit Zalm"]},
            "last_change": "2026-10-08T14:47:24",
        }})
        assert _dates(out, "person") == {"2026-10-08"}


# ---------------------------------------------------------------------------
# Wikidata P813, OpenCorporates updated_at
# ---------------------------------------------------------------------------


def test_wikidata_owner_edge_is_dated_by_its_latest_reference():
    bundle = {"summary": {
        "qid": "Q154950", "label": "Shell plc",
        "controlling_owners": [{
            "qid": "Q1", "name": "Owner Co", "bods_kind": "entity",
            "entity_type": "registeredEntity",
            "references": [
                {"url": "https://a", "retrieved": "2023-01-01T00:00:00Z"},
                {"url": "https://b", "retrieved": "2024-03-11T00:00:00Z"},
            ],
        }, {
            "qid": "Q2", "name": "Unreferenced Co", "bods_kind": "entity",
            "entity_type": "registeredEntity", "references": [],
        }],
    }}
    out = _map(bods_pkg.map_wikidata, bundle)
    rels = {s["statementId"]: s["statementDate"] for s in out if s["recordType"] == "relationship"}
    assert sorted(rels.values()) == ["2024-03-11", TODAY]


def test_opencorporates_entity_is_dated_by_oc_updated_at():
    bundle = {"ocid": "gb/04366849", "company": {
        "name": "SHELL PLC", "jurisdiction_code": "gb", "company_number": "04366849",
        "updated_at": "2026-08-14T03:12:00+00:00",
    }}
    out = _map(bods_pkg.map_opencorporates, bundle)
    assert out[0]["statementDate"] == "2026-08-14"


# ---------------------------------------------------------------------------
# US registers: DC DLCP, New York DOS (real fixtures)
# ---------------------------------------------------------------------------


def test_dlcp_entity_by_its_modification_and_owners_by_the_report():
    from tests.test_dlcp_dc import _bundle

    out = _map(bods_pkg.map_dlcp_dc, _bundle())
    assert _dates(out, "entity") == {"2026-09-18"}  # DCS_LAST_MOD_DTTM
    assert _dates(out, "person") == {"2026-04-02"}  # DATE_LAST_REPORT_FILED
    assert _dates(out, "relationship") == {"2026-04-02"}


class TestNyDos:
    def test_entity_by_latest_filing_and_ceo_by_the_filing_naming_them(self):
        from tests.test_ny_dos import _bundle

        out = _map(bods_pkg.map_ny_dos, _bundle("corning"))
        assert _dates(out) == {"2024-12-02"}

    def test_a_truncated_history_claims_no_latest_filing(self):
        from tests.test_ny_dos import _bundle

        out = _map(bods_pkg.map_ny_dos, _bundle("corning", truncated=True))
        assert _dates(out, "entity") == {TODAY}


# ---------------------------------------------------------------------------
# BO registers: CAC Nigeria, UR Latvia, RPVS Slovakia
# ---------------------------------------------------------------------------


def _cac_psc(name: str, notified: str, status: str = "ACTIVE") -> dict[str, Any]:
    return {
        "psc_id": name, "owner_name": name, "owner_kind": "person",
        "notified": notified, "psc_status": status, "nationality": "NG",
    }


def test_cac_current_owner_by_notification_departed_owner_not_dated():
    bundle = {"record": {
        "rc": "123", "company": "DANGOTE CEMENT PLC", "status": "ACTIVE",
        "pscs": [
            _cac_psc("ALIKO DANGOTE", "2021-12-22"),
            _cac_psc("ALIKO DANGOTE", "2023-05-07"),
            _cac_psc("FORMER OWNER", "2019-01-01", status="INACTIVE"),
        ],
    }}
    out = _map(bods_pkg.map_cac_nigeria, bundle)
    rels = {s["recordStatus"]: s["statementDate"] for s in out if s["recordType"] == "relationship"}
    assert rels["new"] == "2023-05-07"
    assert rels["closed"] == TODAY  # no cessation date: falls back, not to 2019


def test_latvia_beneficial_owners_and_members_are_dated():
    bundle = {
        "lv_regcode": "40003229495",
        "entity": {"name": "SIA Example", "regcode": "40003229495"},
        "beneficial_owners": [{
            "id": 7, "forename": "Jānis", "surname": "Bērziņš", "nationality": "LV",
            "registered_on": "2019-05-02T00:00:00",
            "last_modified_at": "2019-05-02T08:41:39",
        }],
        "members": [{
            "id": 9, "name": "Holding SIA", "entity_type": "LEGAL_ENTITY",
            "legal_entity_registration_number": "40000000000",
            "date_from": "2015-01-01T00:00:00",
            "registered_on": "2015-01-02T00:00:00",
            "last_modified_at": "2024-09-23T11:00:00",
        }],
    }
    out = _map(bods_pkg.map_ur_latvia, bundle)
    rels = sorted(
        s["statementDate"] for s in out if s["recordType"] == "relationship"
    )
    assert rels == ["2019-05-02", "2024-09-23"]
    assert "2019-05-02" in _dates(out, "person")


def test_rpvs_kuv_dated_by_its_validity_window():
    bundle = {
        "sk_ico": "31320155", "name": "Partner a.s.", "link": "https://rpvs.gov.sk",
        "active_kuvs": [
            {"Id": 1, "Meno": "Ján", "Priezvisko": "Novák",
             "PlatnostOd": "2017-03-13T00:00:00+01:00", "PlatnostDo": None},
            {"Id": 2, "Meno": "Eva", "Priezvisko": "Nová",
             "PlatnostOd": "2017-12-19T00:00:00+01:00",
             "PlatnostDo": "2020-07-06T23:59:59.9+02:00"},
        ],
    }
    out = _map(bods_pkg.map_rpvs_slovakia, bundle)
    assert sorted(_dates(out, "relationship")) == ["2017-03-13", "2020-07-06"]
    assert _dates(out, "entity") == {TODAY}  # the partner record carries none


# ---------------------------------------------------------------------------
# Live European registers: INPI, PRH, Zefix, CRO
# ---------------------------------------------------------------------------


def test_inpi_record_updated_at_dates_every_statement():
    bundle = {"siren": "352730394", "company": {
        "updatedAt": "2026-09-22T17:41:10+02:00",
        "identite": {"entreprise": {"denomination": "BOLLORE PARTICIPATIONS"}},
        "formality": {"content": {"personneMorale": {
            "composition": {"pouvoirs": [{
                "typeDePersonne": "INDIVIDU", "roleEntreprise": "73",
                "individu": {"descriptionPersonne": {"nom": "BOLLORE", "prenoms": ["Cyrille"]}},
            }]},
        }}},
    }}
    out = _map(bods_pkg.map_inpi, bundle)
    assert len(out) == 3
    assert _dates(out) == {"2026-09-22"}


def _prh_v3() -> dict[str, Any]:
    """The YTJ v3 shape, as returned for Neste Oyj on 9 Oct 2026 (trimmed)."""
    return {
        "businessId": {"value": "1852302-9", "registrationDate": "2003-09-17"},
        "names": [
            {"name": "Neste Oyj", "type": "1", "registrationDate": "2015-06-01"},
            {"name": "Neste Oil Oyj", "type": "1", "registrationDate": "2005-03-08",
             "endDate": "2015-06-01"},
        ],
        "mainBusinessLine": {"type": "19200", "typeCodeSet": "TOIMI4",
                             "registrationDate": "2026-01-01"},
        "companyForms": [{"type": "17", "descriptions": [
            {"languageCode": "1", "description": "Julkinen osakeyhtiö"},
            {"languageCode": "3", "description": "Public limited company"}],
            "registrationDate": "2005-03-08"}],
        "addresses": [{"type": 1, "street": "Keilaranta", "buildingNumber": "21",
                       "postCode": "02150", "postOffices": [
                           {"city": "ESBO", "languageCode": "2"},
                           {"city": "ESPOO", "languageCode": "1"}]}],
        "lastModified": "2026-10-06T12:37:33",
    }


class TestPrh:
    def test_the_v3_shape_maps_instead_of_raising(self):
        out = _map(bods_pkg.map_prh, {"ytunnus": "1852302-9", "company": _prh_v3()})
        (entity,) = out
        details = entity["recordDetails"]
        assert details["name"] == "Neste Oyj"
        assert {"id": "19200", "scheme": "FI-TOL",
                "schemeName": "Finnish Standard Industrial Classification (TOL 2008)"} in details["identifiers"]
        assert details["addresses"][0]["address"] == "Keilaranta 21 02150 ESPOO"
        assert entity["statementDate"] == "2026-10-06"

    def test_the_legacy_list_shape_still_maps(self):
        company = _prh_v3()
        company["mainBusinessLine"] = [{"code": "62010", "endDate": None}]
        company["companyForm"] = "OYJ"
        out = _map(bods_pkg.map_prh, {"ytunnus": "1852302-9", "company": company})
        assert any(i["id"] == "62010" for i in out[0]["recordDetails"]["identifiers"])


def test_zefix_dated_by_latest_sogc_publication():
    bundle = {"company": {
        "uid": "CHE105909036", "name": "Nestlé S.A.", "legalSeat": "Vevey",
        "sogcDate": "2026-06-30",
        "sogcPub": [{"sogcDate": "2026-07-01"}, {"sogcDate": "2019-01-01"}],
    }}
    out = _map(bods_pkg.map_zefix, bundle)
    assert out[0]["statementDate"] == "2026-07-01"


class TestCro:
    def _bundle(self, **extra: Any) -> dict[str, Any]:
        return {"crn": "633425", "company": {
            "company_num": 633425, "company_name": "EXAMPLE DAC",
            "company_type": "DAC - Designated Activity Company",
            "company_status": "Normal", "nace_v2_code": 6499,
            "company_reg_date": "2018-09-05T00:00:00",
            "last_ar_date": "2025-12-31T00:00:00",
            "company_name_eff_date": "2026-05-25T00:00:00",
        }, **extra}

    def test_a_numeric_nace_code_maps_instead_of_raising(self):
        out = _map(bods_pkg.map_cro, self._bundle())
        assert {"id": "6499", "scheme": "NACE2",
                "schemeName": "NACE Rev. 2 activity code"} in out[0]["recordDetails"]["identifiers"]

    def test_dated_by_the_extract_not_the_annual_return(self):
        # The name changed in May 2026, after the 2025 annual return, so the
        # AR date would assert the old picture; the extract refresh is CRO's
        # claim for the row as served.
        out = _map(bods_pkg.map_cro, self._bundle(extract_modified="2026-09-30T04:00:47.019306"))
        assert out[0]["statementDate"] == "2026-09-30"
        assert _map(bods_pkg.map_cro, self._bundle())[0]["statementDate"] == TODAY

    async def test_the_extract_date_is_read_once_and_never_fails_a_fetch(self, monkeypatch):
        from opencheck.sources import cro

        calls: list[str] = []

        class _Resp:
            is_success = True

            def json(self):
                return {"result": {"last_modified": "2026-09-30T04:00:47.019306"}}

        class _Client:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

            async def get(self, url):
                calls.append(url)
                return _Resp()

        monkeypatch.setattr(cro, "build_client", lambda: _Client())
        monkeypatch.setattr(cro, "_extract_meta", None)
        monkeypatch.setattr(cro.CroAdapter, "info", property(
            lambda self: type("I", (), {"live_available": True})()))
        adapter = cro.CroAdapter()
        assert await adapter._extract_modified() == "2026-09-30T04:00:47.019306"
        assert await adapter._extract_modified() == "2026-09-30T04:00:47.019306"
        assert len(calls) == 1 and "resource_show" in calls[0]

        class _Broken(_Client):
            async def get(self, url):
                raise RuntimeError("CKAN down")

        monkeypatch.setattr(cro, "build_client", lambda: _Broken())
        monkeypatch.setattr(cro, "_extract_meta", None)
        assert await adapter._extract_modified() is None


# ---------------------------------------------------------------------------
# Zambia, and the single-snapshot sources stated explicitly
# ---------------------------------------------------------------------------


def test_zambia_entity_by_its_latest_dataset_update():
    bundle = {
        "lei": "2549008ZVFBSUO8W2L37", "tpins": ["1001602517"],
        "gleif_legal_name": "KANSANSHI MINING PLC",
        "built": "2026-10-07T13:18:59+00:00",
        "datasets": {
            "zra-revenue-payments-2024": {"last_updated": "2025-07-14T13:21:38Z"},
            "mining-rights": {"last_updated": "2024-04-24T08:00:00Z"},
        },
        "match": {"method": "tpin_via_name"},
    }
    out = _map(bods_pkg.map_eiti_zambia, bundle)
    assert out[0]["statementDate"] == "2025-07-14"


def test_anaf_states_its_as_of_date():
    bundle = {"cui": "14399840", "record": {
        "date_generale": {"cui": 14399840, "denumire": "OMV PETROM SA",
                          "data": "2026-10-01", "nrRegCom": "J40/8302/1997"},
    }}
    out = _map(bods_pkg.map_anaf_romania, bundle)
    assert out and out[0]["statementDate"] == "2026-10-01"


@pytest.mark.parametrize(
    "mapper_name,key",
    [
        ("map_asp_moldova", "snapshot_date"),
        ("map_apr_serbia", "snapshot_date"),
        ("map_onrc_romania", "export_date"),
        ("map_eiti_soe", "source_snapshot"),
        ("map_eiti_assessment", "source_snapshot"),
    ],
)
def test_single_snapshot_mappers_state_the_cut(mapper_name, key, monkeypatch):
    """Each wraps its inner mapper in ``dated_by_cut`` on the bundle's cut."""
    from opencheck.bods import mapper as mapper_mod

    inner = getattr(mapper_mod, "_" + mapper_name)
    outer = getattr(mapper_mod, mapper_name)
    seen: dict[str, Any] = {}

    def fake_inner(bundle):
        seen["bundle"] = bundle
        yield {"statementId": "s", "recordType": "entity", "statementDate": TODAY}

    module = __import__(inner.__module__, fromlist=["_"])
    monkeypatch.setattr(module, "_" + mapper_name, fake_inner)
    out = list(outer({key: "2026-09-02T00:00:00Z"}))
    assert _dates(out) == {"2026-09-02"}


async def test_the_onrc_bundle_carries_its_export_cut(index):
    """The inner mapper gets the cut from the bundle, so the adapter puts it
    there: the same slug date the snapshot declares as ``source_as_of``."""
    from opencheck.sources import onrc_romania

    bundle = await onrc_romania.OnrcRomaniaAdapter().fetch("J40/1116/1991")
    assert bundle["export_date"] == "2026-09-02"
    assert _dates(list(bods_pkg.map_onrc_romania(bundle))) == {"2026-09-02"}


def test_the_eiti_soe_bundle_carries_its_snapshot():
    import inspect

    from opencheck.sources import eiti_soe

    assert '"source_snapshot": _index_snapshot' in inspect.getsource(
        eiti_soe.EitiSoeAdapter._build_bundle
    )
