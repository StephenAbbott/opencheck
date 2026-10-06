"""Phase 296 — one register-link table, and the subject header's register record.

Pins four things:

* the frontend mirror (``frontend/src/lib/registerLinks.ts``) is the same table
  as ``opencheck.register_links`` — parsed, not duplicated, so a fifth copy
  cannot start drifting;
* every entry is wired to a real adapter and to the derived key its own
  ``LookupDeriver`` stores the number under;
* the templates produce the URLs that were opened against the live registers
  on 6 Oct 2026, with the GLEIF samples they were opened with;
* the anchor event carries ``register_record`` from the RA code and number
  alone, and the replay folds it back.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pytest_httpx import HTTPXMock

from opencheck import register_links as R
from opencheck.config import get_settings
from opencheck.routers import lookup as L
from opencheck.sources import REGISTRY

_TS = Path(__file__).resolve().parents[2] / "frontend" / "src" / "lib" / "registerLinks.ts"


def _frontend_table() -> dict:
    source = _TS.read_text(encoding="utf-8")
    m = re.search(
        r"// BEGIN REGISTER_LINKS\nexport const REGISTER_LINKS: Record<string, RegisterLinkEntry> = (\{.*?\n\});\n// END REGISTER_LINKS",
        source,
        re.S,
    )
    assert m, "could not find the REGISTER_LINKS block in registerLinks.ts — has its shape changed?"
    return json.loads(m.group(1))


def _backend_table() -> dict:
    out = {}
    for source_id, link in R.REGISTER_LINKS.items():
        entry = {
            "register": link.register,
            "identifierKey": link.identifier_key,
            "record": [list(rule) for rule in link.record],
        }
        if link.history:
            entry["history"] = [list(rule) for rule in link.history]
        out[source_id] = entry
    return out


# --- one table ----------------------------------------------------------------


def test_frontend_mirror_is_the_backend_table() -> None:
    assert _frontend_table() == _backend_table()


def test_every_entry_is_a_registered_adapter_and_its_own_derived_key() -> None:
    for source_id, link in R.REGISTER_LINKS.items():
        assert source_id in REGISTRY, f"{source_id} is not a registered source"
        if source_id == "companies_house":
            # GB is derived on jurisdiction in _build_derived, not by a deriver.
            assert link.identifier_key == "gb_coh"
            continue
        keys = {d.derived_key for d in REGISTRY[source_id].lookup_derivers}
        assert link.identifier_key in keys, (
            f"{source_id}: {link.identifier_key!r} is not one of its derived keys {keys}"
        )


def test_identifier_keys_are_unique() -> None:
    keys = [link.identifier_key for link in R.REGISTER_LINKS.values()]
    assert len(keys) == len(set(keys))


def test_rules_are_anchored_and_substitute_the_number() -> None:
    for source_id, link in R.REGISTER_LINKS.items():
        for pattern, template in (*link.record, *link.history):
            assert pattern.startswith("^") and pattern.endswith("$"), (source_id, pattern)
            re.compile(pattern)
            assert template.count("{id}") == 1, (source_id, template)
            assert template.startswith("https://"), (source_id, template)


# --- the URLs opened against the live registers -------------------------------

# (source, number as the deriver normalises GLEIF's registeredAs, URL). Every
# one was loaded in a headless browser on 6 Oct 2026 and showed the company,
# except the four marked carried-over, which refuse datacentre clients.
VERIFIED = [
    ("companies_house", "03751777", "https://find-and-update.company-information.service.gov.uk/company/03751777"),
    ("kvk", "83235035", "https://www.kvk.nl/bestellen/?kvknummer=83235035"),
    ("zefix", "CHE107367494", "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE107367494"),  # RA000548
    ("zefix", "CHE112229805", "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE112229805"),  # RA000549
    ("ariregister", "16905950", "https://ariregister.rik.ee/eng/company/16905950"),
    ("nz_companies", "4767319", "https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/4767319"),
    ("nz_companies", "9429039305992", "https://www.nzbn.govt.nz/mynzbn/nzbndetails/9429039305992/"),
    ("brreg", "932862581", "https://virksomhet.brreg.no/nb/oppslag/enheter/932862581"),
    ("prh", "2798110-1", "https://tietopalvelu.ytj.fi/yritys/2798110-1"),
    ("ur_latvia", "40203552355", "https://info.ur.gov.lv/#/legal-entity/40203552355"),
    ("ares", "24505285", "https://ares.gov.cz/ekonomicke-subjekty?ico=24505285"),
    ("bce_belgium", "0790703913", "https://kbopub.economie.fgov.be/kbopub/toonondernemingps.html?ondernemingsnummer=0790703913"),
    ("corporations_canada", "17197489", "https://ised-isde.canada.ca/cc/lgcy/fdrlCrpDtls.html?corpId=17197489"),
    ("abr_australia", "46211338152", "https://abr.business.gov.au/ABN/View?abn=46211338152"),
    # carried over unchanged, not re-verified
    ("inpi", "327048260", "https://data.inpi.fr/entreprises/327048260"),
    ("cro", "823392", "https://core.cro.ie/company/823392"),
    ("cvr_denmark", "13521735", "https://datacvr.virk.dk/enhed/virksomhed/13521735"),
    ("jar_lithuania", "304420944", "https://www.registrucentras.lt/jar/p/index.php?kod=304420944"),
]


@pytest.mark.parametrize("source_id,number,url", VERIFIED)
def test_verified_record_urls(source_id: str, number: str, url: str) -> None:
    assert R.record_url(source_id, number) == url


def test_formatted_numbers_are_compacted_before_matching() -> None:
    # A hit id as the register displays it, rather than as the deriver
    # normalises it.
    assert R.record_url("zefix", "CHE-469.102.316") == (
        "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE469102316"
    )
    assert R.record_url("corporations_canada", "1719748-9").endswith("corpId=17197489")
    # …but a register whose number keeps its hyphen keeps it.
    assert R.record_url("prh", "2798110-1").endswith("/2798110-1")


@pytest.mark.parametrize(
    "source_id,number",
    [
        ("corporations_canada", "1719748"),  # no check digit: the page renders empty
        ("companies_house", "3751777"),  # 7 characters: not a company number
        ("kvk", "8323503"),
        ("zefix", "123456789"),
        ("nz_companies", "not-a-number"),
        ("ny_dos", "49779"),  # no per-entity page
        ("firmenbuch", "355240m"),  # no entry: search form only
        ("sudreg_croatia", "030020445"),
        ("bolagsverket", "5562701382"),
        ("gemi_greece", "156284901000"),
        ("companies_house", ""),
    ],
)
def test_no_address_means_no_link(source_id: str, number: str) -> None:
    assert R.record_url(source_id, number) is None


def test_history_overrides_where_the_tab_wants_another_page() -> None:
    assert R.history_url("companies_house", "00358949") == (
        "https://find-and-update.company-information.service.gov.uk/company/00358949/filing-history"
    )
    assert R.history_url("ny_dos", "49779") == (
        "https://data.ny.gov/resource/63wc-4exh.json?corpid_num=49779"
    )
    # No override: the record page.
    assert R.history_url("cvr_denmark", "24256790") == (
        "https://datacvr.virk.dk/enhed/virksomhed/24256790"
    )


# --- the anchor's register record ----------------------------------------------


def _ctx(ra: str, derived: dict[str, str]) -> L._LookupCtx:
    ctx = L._LookupCtx(lei="5493000IBP32UQZ0KL24")
    ctx.registered_at = ra
    ctx.derived = derived
    return ctx


def test_ra_index_is_read_from_the_derivers_not_copied() -> None:
    for deriver in L._RA_DERIVERS:
        for ra in deriver.ra_codes:
            assert ra in L._IDENTIFIER_KEY_BY_RA
    for ra in ("RA000585", "RA000586", "RA000587"):
        assert L._IDENTIFIER_KEY_BY_RA[ra] == "gb_coh"
    assert "RA000591" not in L._IDENTIFIER_KEY_BY_RA  # The Pensions Regulator


def test_scottish_company_links_to_companies_house() -> None:
    rec = L._register_record(_ctx("RA000587", {"gb_coh": "SC651281"}))
    assert rec == {
        "source_id": "companies_house",
        "register": "Companies House",
        "identifier": "SC651281",
        "url": "https://find-and-update.company-information.service.gov.uk/company/SC651281",
        "ra_code": "RA000587",
    }


def test_cantonal_swiss_company_gets_the_uid_page() -> None:
    # GLEIF's own link only fires for RA000548; OpenCheck's reaches RA000549.
    rec = L._register_record(_ctx("RA000549", {"che_uid": "CHE469102316"}))
    assert rec and rec["url"] == "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE469102316"
    assert rec["source_id"] == "zefix"


def test_french_sirene_company_links_to_inpi() -> None:
    rec = L._register_record(_ctx("RA000189", {"siren": "327048260"}))
    assert rec and rec["url"] == "https://data.inpi.fr/entreprises/327048260"


@pytest.mark.parametrize(
    "ra,derived",
    [
        ("RA000014", {"au_acn": "123456789"}),  # ACN: no public page by ACN
        ("RA000628", {"us_ny_dos_id": "49779"}),  # DOS: no per-entity page
        ("RA000017", {"at_fn": "355240m"}),  # Firmenbuch: no entry
        ("RA999999", {}),  # no registration authority
        ("", {"gb_coh": "03751777"}),  # curated bundle: RA code unknown
        ("RA000585", {}),  # number failed to derive
    ],
)
def test_no_record_when_there_is_no_address(ra: str, derived: dict) -> None:
    assert L._register_record(_ctx(ra, derived)) is None


def test_register_lookup_record_has_no_ra_code() -> None:
    rec = R.register_record("kvk", "83235035")
    assert rec == {
        "source_id": "kvk",
        "register": "KvK Handelsregister",
        "identifier": "83235035",
        "url": "https://www.kvk.nl/bestellen/?kvknummer=83235035",
    }


def test_replay_folds_the_record_back() -> None:
    from opencheck.routers.lookup import fold_lookup_events

    lei = "5493000IBP32UQZ0KL24"
    record = R.register_record("kvk", "83235035", ra_code="RA000463")
    resp = fold_lookup_events(
        lei,
        [
            ("gleif_done", {"lei": lei, "legal_name": "X B.V.", "jurisdiction": "NL",
                            "derived_identifiers": {"kvk_number": "83235035"},
                            "register_record": record}),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert resp.register_record == record


def test_an_event_recorded_before_phase_296_folds_to_none() -> None:
    from opencheck.routers.lookup import fold_lookup_events

    lei = "5493000IBP32UQZ0KL24"
    resp = fold_lookup_events(
        lei,
        [
            ("gleif_done", {"lei": lei, "legal_name": "X", "jurisdiction": "NL",
                            "derived_identifiers": {}}),
            ("done", {"bods_issues": [], "license_notices": []}),
        ],
    )
    assert resp.register_record is None


@pytest.mark.httpx_mock(assert_all_responses_were_requested=False)
async def test_live_anchor_builds_the_record_from_gleif_alone(
    tmp_path: Path, monkeypatch, httpx_mock: HTTPXMock
) -> None:
    """From a live GLEIF record — no adapter involved at all."""
    monkeypatch.setenv("OPENCHECK_ALLOW_LIVE", "true")
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    lei = "254900JP7NJEXZ9JFG77"
    api = "https://api.gleif.org/api/v1"
    httpx_mock.add_response(
        url=f"{api}/lei-records/{lei}",
        json={
            "data": {
                "id": lei,
                "type": "lei-records",
                "attributes": {
                    "lei": lei,
                    "entity": {
                        "legalName": {"name": "Venture Spirit Sàrl"},
                        "jurisdiction": "CH",
                        "registeredAt": {"id": "RA000549", "other": None},
                        "registeredAs": "CHE-469.102.316",
                    },
                    "registration": {"status": "ISSUED"},
                },
            }
        },
        is_reusable=True,
    )
    for rel_path in (
        "direct-parent",
        "direct-parent-reporting-exception",
        "ultimate-parent",
        "ultimate-parent-reporting-exception",
    ):
        httpx_mock.add_response(
            url=f"{api}/lei-records/{lei}/{rel_path}", status_code=404, is_reusable=True
        )
    httpx_mock.add_response(
        url=re.compile(rf"{re.escape(api)}/lei-records/{lei}/direct-children.*"),
        json={"data": [], "meta": {"pagination": {"total": 0}}},
        is_reusable=True,
    )
    httpx_mock.add_response(
        url=re.compile(r"https://query\.wikidata\.org/.*"),
        json={"results": {"bindings": []}},
        is_reusable=True,
    )
    try:
        ctx, _ = await L._resolve_ctx(lei)
    finally:
        get_settings.cache_clear()
    assert L._register_record(ctx) == {
        "source_id": "zefix",
        "register": "Swiss UID register (FSO)",
        "identifier": "CHE469102316",
        "url": "https://www.uid.admin.ch/Detail.aspx?uid_id=CHE469102316",
        "ra_code": "RA000549",
    }
