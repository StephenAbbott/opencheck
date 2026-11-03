"""RPVS (Slovakia): beneficial owners' nationality and country of residence.

From the country-of-residence ticket's mapper sweep (3 Nov 2026). RPVS
publishes, per beneficial owner (KUV), ``StatnaPrislusnost`` (nationality) and
``Adresa`` → ``Stat`` (the country of their address). The adapter never asked
for either, and the mapper read keys RPVS does not have (``Obcanstvo``,
``statObcanstva``, a string ``Adresa``), so no RPVS owner ever carried a
nationality or a residence. ``$expand`` is capped at depth 2 from ``Partneri``,
so the countries are read from the owners collection, which is paged.

The owner rows below are the live answer for partner 7049 on 3 Nov 2026,
cut to two rows, with the street fields replaced.
"""

from __future__ import annotations

import re
from typing import Any

import httpx
import pytest
import respx

from opencheck import degradation
from opencheck.bods import map_rpvs_slovakia, validate_shape
from opencheck.cache import Cache
from opencheck.sources.rpvs_slovakia import (
    KUV_COUNTRIES_NOT_READ,
    RpvsSlovakiaAdapter,
    kuv_countries_from,
)

ICO = "35763469"
PARTNER_ID = 7049
RESOLVE = re.compile(r"https://rpvs\.gov\.sk/opendatav2/PartneriVerejnehoSektora.*")
DETAIL = re.compile(r"https://rpvs\.gov\.sk/opendatav2/Partneri\(7049\).*")
OWNERS = re.compile(r"https://rpvs\.gov\.sk/opendatav2/KonecniUzivateliaVyhod\?.*")
NEXT = "https://rpvs.gov.sk/opendatav2/KonecniUzivateliaVyhod?$skip=20&page=2"

_GERMANY = {
    "Id": 87, "Meno": "Nemecká spolková republika", "StatistickyKod": "276",
    "PlatnostOd": "1993-01-01T00:00:00+01:00", "PlatnostDo": None,
}
_SLOVAKIA = {
    "Id": 1, "Meno": "Slovenská republika", "StatistickyKod": "703",
    "PlatnostOd": "1993-01-01T00:00:00+01:00", "PlatnostDo": None,
}


def _kuv(kuv_id: int, first: str, last: str, *, ended: bool = False) -> dict[str, Any]:
    return {
        "Id": kuv_id, "Meno": first, "Priezvisko": last, "DatumNarodenia": None,
        "TitulPred": None, "TitulZa": None, "JeVerejnyCinitel": False,
        "ObchodneMeno": None, "Ico": None,
        "PlatnostOd": "2017-02-01T00:00:00+01:00",
        "PlatnostDo": "2017-07-13T23:59:59.9+02:00" if ended else None,
    }


def _owner_row(kuv_id: int, nationality: dict, residence: dict | None) -> dict[str, Any]:
    row = {**_kuv(kuv_id, "x", "y"), "StatnaPrislusnost": nationality}
    row["Adresa"] = {
        "Id": kuv_id - 1, "MenoUlice": "Street", "OrientacneCislo": "1",
        "SupisneCislo": None, "Mesto": "Town", "MestoKod": None, "Psc": "00000",
        "Identifikator": None, "Stat": residence,
    }
    return row


DETAIL_RAW = {
    "Id": PARTNER_ID,
    "CisloVlozky": PARTNER_ID,
    "PartneriVerejnehoSektora": [
        {"Id": 1, "ObchodneMeno": "Slovak Telekom, a.s.", "Ico": ICO,
         "PlatnostOd": "2017-02-01T00:00:00+01:00", "PlatnostDo": None},
    ],
    "KonecniUzivateliaVyhod": [
        _kuv(18862, "Clemens", "Reinhard"),
        _kuv(18900, "Jana", "Nováková"),
    ],
    "OpravneneOsoby": [],
}


@pytest.fixture(autouse=True)
def _live(monkeypatch, tmp_path):
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    from opencheck.config import get_settings

    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _mock_resolve_and_detail() -> None:
    respx.get(RESOLVE).mock(
        return_value=httpx.Response(
            200, json={"value": [{"Id": 1, "Partner": {"Id": PARTNER_ID}}]}
        )
    )
    respx.get(DETAIL).mock(return_value=httpx.Response(200, json=DETAIL_RAW))


def _people(bundle: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stmts = list(map_rpvs_slovakia(bundle))
    assert validate_shape(stmts) == []
    return {
        s["recordDetails"]["names"][0]["fullName"]: s
        for s in stmts
        if s["recordType"] == "person"
    }


def test_owner_rows_are_reduced_to_countries_with_no_street() -> None:
    reduced = kuv_countries_from([_owner_row(18862, _GERMANY, _SLOVAKIA)])
    assert reduced == {
        "18862": {
            "nationality": {"name": "Nemecká spolková republika", "numeric": "276"},
            "residence": {"name": "Slovenská republika", "numeric": "703"},
        }
    }
    assert "Street" not in repr(reduced) and "Town" not in repr(reduced)


@respx.mock
async def test_owners_carry_nationality_and_residence_across_pages() -> None:
    _mock_resolve_and_detail()
    respx.get(OWNERS).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "value": [_owner_row(18862, _GERMANY, _GERMANY)],
                    "@odata.nextLink": NEXT,
                },
            ),
            httpx.Response(200, json={"value": [_owner_row(18900, _SLOVAKIA, _GERMANY)]}),
        ]
    )
    bundle = await RpvsSlovakiaAdapter().fetch(ICO)
    assert set(bundle["kuv_countries"]) == {"18862", "18900"}

    people = _people(bundle)
    reinhard = people["Clemens Reinhard"]["recordDetails"]
    assert reinhard["nationalities"] == [{"name": "Germany", "code": "DE"}]
    assert reinhard["addresses"] == [
        {"type": "residence", "country": {"name": "Germany", "code": "DE"}}
    ]
    novakova = people["Jana Nováková"]["recordDetails"]
    assert novakova["nationalities"] == [{"name": "Slovakia", "code": "SK"}]
    assert novakova["addresses"][0]["country"]["code"] == "DE"

    # Cached with the countries and without any street.
    cached = Cache().get_payload(f"rpvs_slovakia/partner-v2/{PARTNER_ID}")
    assert cached is not None and "kuv_countries" in cached[0]
    assert "Street" not in repr(cached[0])


@respx.mock
async def test_a_failed_owners_read_lists_the_owners_without_countries_and_says_so() -> None:
    _mock_resolve_and_detail()
    respx.get(OWNERS).mock(return_value=httpx.Response(503))
    with degradation.recording() as degraded:
        bundle = await RpvsSlovakiaAdapter().fetch(ICO)
    assert bundle["kuv_countries"] is None
    people = _people(bundle)
    assert len(people) == 2
    for person in people.values():
        assert not person["recordDetails"].get("nationalities")
        assert not person["recordDetails"].get("addresses")
    (record,) = [d for d in degraded if d.source_id == "rpvs_slovakia"]
    assert record.check == degradation.CHECK_SOURCE_READ
    assert record.detail == KUV_COUNTRIES_NOT_READ
    # Not cached, so the next lookup reads the countries again.
    assert Cache().get_payload(f"rpvs_slovakia/partner-v2/{PARTNER_ID}") is None


def test_an_unknown_numeric_code_keeps_the_registers_name_and_no_code() -> None:
    bundle = {
        "source_id": "rpvs_slovakia", "hit_id": ICO, "sk_ico": ICO,
        "partner_id": PARTNER_ID, "cislo_vlozky": PARTNER_ID,
        "name": "Slovak Telekom, a.s.", "status": "active",
        "kuvs": [_kuv(1, "A", "B")], "active_kuvs": [_kuv(1, "A", "B")],
        "pvs_entries": DETAIL_RAW["PartneriVerejnehoSektora"], "opravnene_osoby": [],
        "kuv_countries": {
            "1": {
                "nationality": {"name": "Kosovo", "numeric": ""},
                "residence": {"name": "Neznámy štát", "numeric": "999"},
            }
        },
        "link": "https://rpvs.gov.sk/rpvs/7049", "is_stub": False,
    }
    (person,) = _people(bundle).values()
    assert person["recordDetails"]["nationalities"] == [{"name": "Kosovo"}]
    assert person["recordDetails"]["addresses"] == [
        {"type": "residence", "country": {"name": "Neznámy štát"}}
    ]


def test_a_bundle_cached_before_the_ticket_maps_as_before() -> None:
    bundle = {
        "source_id": "rpvs_slovakia", "hit_id": ICO, "sk_ico": ICO,
        "partner_id": PARTNER_ID, "name": "Slovak Telekom, a.s.", "status": "active",
        "kuvs": [_kuv(1, "A", "B")], "active_kuvs": [_kuv(1, "A", "B")],
        "pvs_entries": [], "opravnene_osoby": [],
        "link": "https://rpvs.gov.sk/rpvs/7049", "is_stub": False,
    }
    (person,) = _people(bundle).values()
    assert not person["recordDetails"].get("addresses")
