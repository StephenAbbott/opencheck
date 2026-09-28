"""Phase 257 — three adapter schemes renamed to their org-id.guide codes.

``SK-RPO`` → ``SK-ICO``, ``BR-RFB`` → ``BR-CNPJ``, ``CA-CORP`` → ``CA-CC``.
Each was named after the register or authority a number is read from rather
than after the number, and libcovebods reported all three as
``entity_identifiers_not_known_scheme`` (the five-network subsidiaries check,
28 Sept 2026). The scheme follows the adapter (Phase 239), so the mapper and
the GLEIF RA table move together — and with them the FullCheck register hops,
which ``register_hops`` derives from that table.

The GLEIF numbers below are the shapes GLEIF files under each RA code, read
from the live API on 28 Sept 2026: RA000681 writes the CNPJ punctuated
(``33.856.394/0001-33``), RA000526 the bare eight-digit IČO, RA000072 the
corporation number with its check digit after a hyphen (``1709920-7``).
"""

from __future__ import annotations

from typing import Any

import pytest

from opencheck.bods.mapper import (
    _GLEIF_RA_TO_ORG_ID,
    gleif_registration_scheme,
    map_cnpj_brazil,
    map_corporations_canada,
    map_gleif,
    map_rpo_slovakia,
    map_rpvs_slovakia,
)
from opencheck.reconcile import _identifier_keys
from opencheck.register_hops import hop_for, hop_schemes
from opencheck.watchlist import diff_snapshots

RENAMES = {
    # old: (new, RA code, adapter)
    "SK-RPO": ("SK-ICO", "RA000526", "rpo_slovakia"),
    "BR-RFB": ("BR-CNPJ", "RA000681", "cnpj_brazil"),
    "CA-CORP": ("CA-CC", "RA000072", "corporations_canada"),
}


def _schemes(stmt: dict[str, Any]) -> dict[str, str]:
    return {i["scheme"]: i["id"] for i in stmt["recordDetails"].get("identifiers") or []}


def _entities(stmts: Any) -> list[dict[str, Any]]:
    return [s for s in stmts if s["recordType"] == "entity"]


def _gleif_entity(lei: str, jurisdiction: str, ra: str, registered_as: str) -> dict[str, Any]:
    bundle = {
        "lei": lei,
        "record": {
            "attributes": {
                "lei": lei,
                "entity": {
                    "legalName": {"name": "TEST ENTITY"},
                    "jurisdiction": jurisdiction,
                    "registeredAs": registered_as,
                    "registeredAt": {"id": ra, "other": None},
                },
                "registration": {"status": "ISSUED", "lastUpdateDate": "2026-09-24T00:00:00Z"},
            }
        },
    }
    return _entities(map_gleif(bundle).statements)[0]


# ---------------------------------------------------------------------------
# The RA table and the GLEIF side
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("old,new_ra", list(RENAMES.items()))
def test_ra_table_names_the_org_id_code(old: str, new_ra: tuple[str, str, str]) -> None:
    new, ra, _ = new_ra
    assert _GLEIF_RA_TO_ORG_ID[ra][0] == new
    assert gleif_registration_scheme(ra, None)[0] == new
    assert all(scheme != old for scheme, _ in _GLEIF_RA_TO_ORG_ID.values())


def test_a_canadian_province_is_still_named_by_its_ra_code() -> None:
    # Only the federal register became CA-CC; Ontario's numbers say nothing
    # about it and keep the RA-code fallback.
    assert gleif_registration_scheme("RA000076", "CA-ON")[0] == "RA000076"


# ---------------------------------------------------------------------------
# The adapter side
# ---------------------------------------------------------------------------


def test_rpo_slovakia_writes_sk_ico() -> None:
    stmts = list(map_rpo_slovakia({"sk_ico": "31320155", "name": "Acme s.r.o.", "is_stub": False}))
    ids = _schemes(_entities(stmts)[0])
    assert ids["SK-ICO"] == "31320155"
    assert "SK-RPO" not in ids


def test_rpvs_legal_person_kuv_writes_sk_ico() -> None:
    bundle = {
        "sk_ico": "35763469",
        "name": "Slovak Telekom, a.s.",
        "partner_id": 1,
        "link": "https://rpvs.gov.sk/rpvs/Partner/1",
        "is_stub": False,
        "active_kuvs": [{"Id": 9, "ObchodneMeno": "Deutsche Telekom AG", "Ico": "123456"}],
    }
    kuv = next(s for s in _entities(map_rpvs_slovakia(bundle)) if s["recordDetails"]["name"] == "Deutsche Telekom AG")
    assert _schemes(kuv) == {"SK-ICO": "00123456"}
    # The RPVS partner itself keeps its register-specific scheme.
    partner = next(s for s in _entities(map_rpvs_slovakia(bundle)) if s["recordDetails"]["name"].startswith("Slovak"))
    assert "SK-RPVS" in _schemes(partner)


def test_cnpj_brazil_writes_br_cnpj() -> None:
    stmts = list(map_cnpj_brazil({
        "br_cnpj": "33856394000133",
        "is_stub": False,
        "company": {"name": "PERNOD RICARD BRASIL INDUSTRIA E COMERCIO LTDA"},
        "partners": [],
    }))
    ids = _schemes(_entities(stmts)[0])
    assert ids["BR-CNPJ"] == "33856394000133"
    assert "BR-RFB" not in ids


def test_corporations_canada_writes_ca_cc_and_says_so_on_the_row() -> None:
    from opencheck.sources.corporations_canada import CorporationsCanadaAdapter

    stmts = list(map_corporations_canada({
        "corp_id": "17099207",
        "legal_name": "Aluminum AA3104 Ltd.",
        "corporation": {"corporate_name": "Aluminum AA3104 Ltd."},
        "directors": [],
        "is_stub": False,
    }))
    ids = _schemes(_entities(stmts)[0])
    assert ids["CA-CC"] == "17099207"
    assert "CA-CORP" not in ids
    hit = CorporationsCanadaAdapter._entity_hit({"status": "Active"}, "17099207")
    assert hit.summary.startswith("CA-CC 17099207")


def test_the_lookup_hit_summary_names_ca_cc() -> None:
    from opencheck.routers.hit_builders import _bh_corporations_canada

    class _Ctx:
        legal_name = "Aluminum AA3104 Ltd."

    hit = _bh_corporations_canada({"corporation": {}}, "17099207", _Ctx())  # type: ignore[arg-type]
    assert hit.summary == "CA-CC 17099207"


# ---------------------------------------------------------------------------
# GLEIF and the register corroborate on one scheme
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ra,jur,registered_as,register_stmt,key",
    [
        (
            "RA000681", "BR", "33.856.394/0001-33",
            lambda: _entities(map_cnpj_brazil({
                "br_cnpj": "33856394000133", "is_stub": False,
                "company": {"name": "PERNOD RICARD BRASIL"}, "partners": [],
            }))[0],
            "BR-CNPJ:33856394000133",
        ),
        (
            "RA000526", "SK", "31340989",
            lambda: _entities(map_rpo_slovakia({"sk_ico": "31340989", "name": "M line, s. r. o.", "is_stub": False}))[0],
            "SK-ICO:31340989",
        ),
        (
            "RA000072", "CA", "1709920-7",
            lambda: _entities(map_corporations_canada({
                "corp_id": "17099207", "legal_name": "Aluminum AA3104 Ltd.",
                "corporation": {}, "directors": [], "is_stub": False,
            }))[0],
            "CA-CC:17099207",
        ),
    ],
)
def test_gleif_and_the_register_share_a_scheme_scoped_key(ra, jur, registered_as, register_stmt, key) -> None:
    gleif = _gleif_entity("2549001K7XWV7CAGRB80", jur, ra, registered_as)
    assert key in _identifier_keys(gleif)
    assert key in _identifier_keys(register_stmt())


# ---------------------------------------------------------------------------
# FullCheck register hops
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("old,new_ra", list(RENAMES.items()))
def test_register_hop_follows_the_new_scheme(old: str, new_ra: tuple[str, str, str]) -> None:
    new, _, adapter = new_ra
    hop = hop_for(new)
    assert hop is not None and hop.source_id == adapter
    assert hop_for(old) is None


def test_country_aliases() -> None:
    hops = hop_schemes()
    assert hops["REG-SK"].source_id == "rpo_slovakia"
    assert hops["REG-BR"].source_id == "cnpj_brazil"
    # The federal register cannot stand in for Canada: most Canadian
    # companies are provincial and their numbers say nothing about it.
    assert "REG-CA" not in hops or hops["REG-CA"].source_id != "corporations_canada"


def test_gleif_check_digit_form_still_reaches_corporations_canada() -> None:
    hop = hop_for("CA-CC")
    assert hop is not None
    assert hop.normalise("1709920-7") == "17099207"


# ---------------------------------------------------------------------------
# Frozen payloads: a watchlist baseline taken before the rename
# ---------------------------------------------------------------------------


def _snapshot(ids: dict[str, str]) -> dict[str, Any]:
    return {
        "legal_name": "Aluminum AA3104 Ltd.",
        "jurisdiction": "CA",
        "register_status": {"liveness": "live", "raw": "Active"},
        "identifiers": ids,
        "signals": [],
        "coverage": {},
        "degraded_sources": [],
        "verdict": "",
    }


@pytest.mark.parametrize("old,new_ra", list(RENAMES.items()))
def test_a_renamed_scheme_is_not_an_identifier_change(old: str, new_ra: tuple[str, str, str]) -> None:
    new = new_ra[0]
    jur = new.split("-")[0]
    before = _snapshot({"LEI": "2549001K7XWV7CAGRB80", old: "17099207", f"REGISTER:{jur}": "17099207"})
    after = _snapshot({"LEI": "2549001K7XWV7CAGRB80", new: "17099207", f"REGISTER:{jur}": "17099207"})
    assert [c for c in diff_snapshots(before, after) if c["kind"] == "identifier"] == []
