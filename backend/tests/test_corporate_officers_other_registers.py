"""Corporate officers in other registers' mappers (follow-up to Phase 293).

Phase 293 made a Companies House ``corporate-*`` officer an entity. This file
pins the same rule where the register itself says an officer is a legal
person, using shapes observed live on 6 Oct 2026:

* Companies House officer-appointments (BackgroundCheck): ``is_corporate_officer``
  and ``corporate-*`` roles.
* Norway (Brønnøysund): a role held by an ``enhet`` — OBOS EIENDOMSFORVALTNING
  AS as forretningsfører of ABELLUND BORETTSLAG (950386223). It was dropped.
* Estonia (e-Äriregister): an 8-digit registry code in the officers table —
  2C Ventures Fund 1 GP OÜ (16769949), general partner of 2C Ventures Fund I
  usaldusfond (16864903). It was mapped as a person.
* Brazil (Receita Federal QSA): SHELL BRAZIL HOLDING BV, "Sócio Pessoa
  Jurídica Domiciliado no Exterior", ``pais`` "Países Baixos (Holanda)". It was
  an entity, but published as Brazilian.
"""

from __future__ import annotations

from opencheck.bods import map_companies_house, validate_shape
from opencheck.bods.mapper import (
    _map_companies_house_officer,
    map_ariregister,
    map_brreg,
    map_cnpj_brazil,
)
from opencheck.related_targets import officer_only_entity_ids
from opencheck.sources.ariregister import _parse_officers


def _by_type(stmts: list[dict], kind: str) -> list[dict]:
    return [s for s in stmts if s["recordType"] == kind]


def _rels(stmts: list[dict]) -> list[dict]:
    return _by_type(stmts, "relationship")


# ---------------------------------------------------------------------------
# Companies House — officer-appointments path
# ---------------------------------------------------------------------------

_BELIZE_IDENT = {
    "identification_type": "non-eea",
    "legal_authority": "INTERNATIONAL BUSINESS COMPANIES ACT 1990, BELIZE",
    "legal_form": "LIMITED",
    "place_registered": "REGISTRAR OF INTERNATIONAL BUSINESS COMPANIES",
    "registration_number": "36,265",
}


def _appointment(number: str, role: str, ident: dict | None = None) -> dict:
    item = {
        "appointed_to": {"company_number": number, "company_name": f"CO {number}"},
        "officer_role": role,
        "appointed_on": "2009-06-08",
        "address": {"locality": "Belize City", "country": "Belize"},
    }
    if ident is not None:
        item["identification"] = ident
    return item


def _officer_bundle(items: list[dict], *, flag: bool | None = True) -> dict:
    envelope: dict = {"name": "ADVANCE DEVELOPMENTS LIMITED", "items": items}
    if flag is not None:
        envelope["is_corporate_officer"] = flag
    return {"officer_id": "ADVDEV1", "appointments": envelope}


def test_ch_appointments_corporate_officer_is_an_entity() -> None:
    stmts = _map_companies_house_officer(
        _officer_bundle([
            _appointment("OC346224", "corporate-llp-designated-member", _BELIZE_IDENT),
            _appointment("OC346225", "corporate-llp-designated-member", _BELIZE_IDENT),
        ])
    ).statements
    assert not _by_type(stmts, "person")
    officer = [
        s for s in _by_type(stmts, "entity")
        if s["recordDetails"]["name"] == "ADVANCE DEVELOPMENTS LIMITED"
    ]
    assert len(officer) == 1
    rd = officer[0]["recordDetails"]
    assert rd["jurisdiction"]["code"] == "BZ"
    assert {"id": "36265", "scheme": "REG-BZ"}.items() <= rd["identifiers"][0].items()
    for rel in _rels(stmts):
        assert rel["recordDetails"]["interestedParty"] == officer[0]["statementId"]
    assert validate_shape(stmts) == []


def test_ch_appointments_corporate_roles_alone_are_enough() -> None:
    """``is_corporate_officer`` is missing from some cached payloads; the
    ``corporate-*`` roles say the same thing."""
    stmts = _map_companies_house_officer(
        _officer_bundle(
            [_appointment("OC346224", "corporate-director", _BELIZE_IDENT)], flag=None
        )
    ).statements
    assert not _by_type(stmts, "person")


def test_ch_appointments_natural_person_unchanged() -> None:
    stmts = _map_companies_house_officer(
        _officer_bundle([_appointment("01234567", "director")], flag=False)
    ).statements
    assert len(_by_type(stmts, "person")) == 1


def test_ch_appointments_entity_matches_the_company_path() -> None:
    """The same corporate member reached through the company's officers list
    and through its officer id is one statement."""
    member = {
        "name": "ADVANCE DEVELOPMENTS LIMITED",
        "officer_role": "corporate-llp-designated-member",
        "appointed_on": "2009-06-08",
        "address": {"locality": "Belize City", "country": "Belize"},
        "identification": _BELIZE_IDENT,
        "links": {"officer": {"appointments": "/officers/ADVDEV1/appointments"}},
    }
    company = list(map_companies_house({
        "source_id": "companies_house",
        "company_number": "OC346224",
        "profile": {"company_name": "METASTAR INVEST LLP", "type": "llp"},
        "officers": {"items": [member]},
        "pscs": {"items": []},
        "related_companies": {},
    }))
    via_company = {
        s["statementId"] for s in _by_type(company, "entity")
        if s["recordDetails"]["name"] == "ADVANCE DEVELOPMENTS LIMITED"
    }
    via_officer = {
        s["statementId"]
        for s in _by_type(
            _map_companies_house_officer(
                _officer_bundle([
                    _appointment("OC346224", "corporate-llp-designated-member", _BELIZE_IDENT)
                ])
            ).statements,
            "entity",
        )
        if s["recordDetails"]["name"] == "ADVANCE DEVELOPMENTS LIMITED"
    }
    assert via_company and via_company == via_officer


def test_ch_appointments_uk_corporate_officer_gets_gb_coh() -> None:
    uk_ident = {
        "identification_type": "uk-limited-company",
        "registration_number": "1234567",
        "place_registered": "COMPANIES HOUSE",
    }
    stmts = _map_companies_house_officer(
        _officer_bundle([_appointment("09999999", "corporate-director", uk_ident)])
    ).statements
    officer = next(
        s for s in _by_type(stmts, "entity")
        if s["recordDetails"]["name"] == "ADVANCE DEVELOPMENTS LIMITED"
    )
    ids = officer["recordDetails"]["identifiers"]
    assert ids[0]["scheme"] == "GB-COH" and ids[0]["id"] == "01234567"


# ---------------------------------------------------------------------------
# Norway — roles held by an ``enhet``
# ---------------------------------------------------------------------------

def _brreg_bundle(roles: list[dict]) -> dict:
    return {
        "source_id": "brreg",
        "orgnr": "950386223",
        "entity": {"organisasjonsnummer": "950386223", "navn": "ABELLUND BORETTSLAG"},
        "roles": roles,
        "legal_name": "ABELLUND BORETTSLAG",
        "is_stub": False,
    }


_OBOS = {
    "erSlettet": False,
    "navn": ["OBOS EIENDOMSFORVALTNING AS"],
    "organisasjonsform": {"beskrivelse": "Aksjeselskap", "kode": "AS"},
    "organisasjonsnummer": "934261585",
}


def _ffor(unit: dict) -> dict:
    return {
        "avregistrert": False,
        "enhet": unit,
        "type": {"beskrivelse": "Forretningsfører", "kode": "FFØR"},
    }


def test_brreg_enhet_role_holder_is_an_entity() -> None:
    stmts = list(map_brreg(_brreg_bundle([
        _ffor(_OBOS),
        {
            "type": {"kode": "LEDE", "beskrivelse": "Styrets leder"},
            "person": {"navn": {"fornavn": "Kari", "etternavn": "Nordmann"},
                       "fodselsdato": "1970-01-01"},
        },
    ])))
    obos = [
        s for s in _by_type(stmts, "entity")
        if s["recordDetails"]["name"] == "OBOS EIENDOMSFORVALTNING AS"
    ]
    assert len(obos) == 1
    rd = obos[0]["recordDetails"]
    assert rd["jurisdiction"]["code"] == "NO"
    assert rd["identifiers"] == [{
        "id": "934261585",
        "scheme": "NO-BRC",
        "schemeName": "Brønnøysundregistrene Enhetsregisteret",
    }]
    rel = next(
        r for r in _rels(stmts)
        if r["recordDetails"]["interestedParty"] == obos[0]["statementId"]
    )
    assert rel["recordDetails"]["interests"][0]["details"] == "Forretningsfører"
    assert len(_by_type(stmts, "person")) == 1
    assert validate_shape(stmts) == []


def test_brreg_same_enhet_twice_is_one_entity() -> None:
    stmts = list(map_brreg(_brreg_bundle([_ffor(_OBOS), _ffor(_OBOS)])))
    names = [s["recordDetails"]["name"] for s in _by_type(stmts, "entity")]
    assert names.count("OBOS EIENDOMSFORVALTNING AS") == 1
    assert len(_rels(stmts)) == 2


def test_brreg_enhet_naming_the_subject_is_not_a_second_entity() -> None:
    stmts = list(map_brreg(_brreg_bundle([
        _ffor({**_OBOS, "organisasjonsnummer": "950386223"})
    ])))
    assert len(_by_type(stmts, "entity")) == 1
    assert not _rels(stmts)


# ---------------------------------------------------------------------------
# Estonia — a registry code in the officers table
# ---------------------------------------------------------------------------

_EE_OFFICERS_HTML = """<table>
<tr><th>Name</th><th>Personal identification code</th><th>Role</th><th>Start - end</th></tr>
<tr><td>2C Ventures Fund 1 GP OÜ</td><td>16769949</td><td>General partner</td><td>20.11.2023</td></tr>
<tr><td>Hendrik Reimand</td><td>38001010000</td><td>Limited partner</td><td>20.11.2023</td></tr>
<tr><td>John Smith</td><td>01.02.1970</td><td>Liquidator</td><td>20.11.2023</td></tr>
</table>"""


def test_ariregister_registry_code_marks_a_legal_person() -> None:
    officers = {o["nimi_arinimi"] or o["eesnimi"]: o for o in _parse_officers(_EE_OFFICERS_HTML)}
    gp = officers["2C Ventures Fund 1 GP OÜ"]
    assert gp["isiku_tyyp"] == "J"
    assert gp["isikukood_registrikood"] == "16769949"
    assert gp["eesnimi"] == ""
    # A personal code, or a foreign person's date of birth, stays a person.
    assert all(
        o["isiku_tyyp"] == "F" for k, o in officers.items() if k != "2C Ventures Fund 1 GP OÜ"
    )


def _ee_bundle(officers: list[dict]) -> dict:
    return {
        "source_id": "ariregister",
        "registry_code": "16864903",
        "ee_registry_code": "16864903",
        "name": "2C Ventures Fund I usaldusfond",
        "legal_name": "2C Ventures Fund I usaldusfond",
        "officers": officers,
        "shareholders": [],
        "beneficial_owners": [],
        "is_stub": False,
    }


def test_ariregister_corporate_general_partner_is_an_entity() -> None:
    stmts = list(map_ariregister(_ee_bundle(_parse_officers(_EE_OFFICERS_HTML))))
    gp = [
        s for s in _by_type(stmts, "entity")
        if s["recordDetails"]["name"] == "2C Ventures Fund 1 GP OÜ"
    ]
    assert len(gp) == 1
    rd = gp[0]["recordDetails"]
    assert rd["jurisdiction"]["code"] == "EE"
    assert rd["identifiers"][0] == {
        "id": "16769949",
        "scheme": "EE-ARIREGISTER",
        "schemeName": "Estonian e-Business Register",
    }
    persons = {s["recordDetails"]["names"][0]["fullName"] for s in _by_type(stmts, "person")}
    assert "2C Ventures Fund 1 GP OÜ" not in " ".join(persons)
    assert validate_shape(stmts) == []
    # A general partner's interest is an officer role, so the Phase 293
    # screening gate (a corroborating country) applies to it.
    assert gp[0]["statementId"] in officer_only_entity_ids(stmts)


# ---------------------------------------------------------------------------
# Brazil — QSA partners domiciled abroad
# ---------------------------------------------------------------------------

def _br_bundle(partners: list[dict]) -> dict:
    return {
        "br_cnpj": "10456016000167",
        "company": {"name": "SHELL BRASIL PETROLEO LTDA"},
        "partners": partners,
        "is_stub": False,
    }


def _br_partner(name: str, kind: str, role: str, country: str | None) -> dict:
    return {
        "name": name, "cnpj": None, "role": role, "kind": kind,
        "entry_date": None, "country": country,
    }


def _br_entity(stmts: list[dict], name: str) -> dict:
    return next(s for s in _by_type(stmts, "entity") if s["recordDetails"]["name"] == name)


def test_cnpj_foreign_corporate_partner_takes_the_filed_country() -> None:
    stmts = list(map_cnpj_brazil(_br_bundle([
        _br_partner(
            "SHELL BRAZIL HOLDING BV", "entity",
            "Sócio Pessoa Jurídica Domiciliado no Exterior", "Países Baixos (Holanda)",
        ),
    ])))
    jur = _br_entity(stmts, "SHELL BRAZIL HOLDING BV")["recordDetails"]["jurisdiction"]
    assert jur == {"name": "Netherlands", "code": "NL"}
    assert validate_shape(stmts) == []


def test_cnpj_code3_partner_that_is_a_legal_person_is_an_entity() -> None:
    stmts = list(map_cnpj_brazil(_br_bundle([
        _br_partner(
            "CORTEVA AGRISCIENCE HOLDING SPAIN, S.L.", "foreign",
            "Sócio Pessoa Jurídica Domiciliado no Exterior", "ESPANHA",
        ),
        _br_partner(
            "JOHN DOE", "foreign",
            "Sócio Pessoa Física Residente ou Domiciliado no Exterior", "ESTADOS UNIDOS",
        ),
    ])))
    assert (
        _br_entity(stmts, "CORTEVA AGRISCIENCE HOLDING SPAIN, S.L.")
        ["recordDetails"]["jurisdiction"]["code"] == "ES"
    )
    persons = [s["recordDetails"]["names"][0]["fullName"] for s in _by_type(stmts, "person")]
    assert persons == ["JOHN DOE"]


def test_cnpj_domestic_and_unresolved_countries() -> None:
    stmts = list(map_cnpj_brazil(_br_bundle([
        _br_partner("HOLDING BRASILEIRA SA", "entity", "Sócio", None),
        _br_partner("OBSCURE HOLDINGS", "entity", "Sócio", "TERRA DO NUNCA"),
    ])))
    assert (
        _br_entity(stmts, "HOLDING BRASILEIRA SA")["recordDetails"]["jurisdiction"]["code"]
        == "BR"
    )
    assert "jurisdiction" not in _br_entity(stmts, "OBSCURE HOLDINGS")["recordDetails"]


# ---------------------------------------------------------------------------
# Phase 295 decisions (Stephen, 6 Oct 2026)
# ---------------------------------------------------------------------------
#
# 1. Management roles that were otherInfluenceOrControl (Norway DAGL/FFØR,
#    Austria Geschäftsführer/Prokurist) or appointmentOfBoard (Czech statutory
#    body) are seniorManagingOfficial, which brings a corporate holder under
#    the Phase 293 officer screening gate.
# 2. A corporate officer gets no jurisdiction the register did not file:
#    Cyprus files none; Romania files one sometimes.

def test_brreg_corporate_forretningsforer_is_under_the_officer_gate() -> None:
    stmts = list(map_brreg(_brreg_bundle([_ffor(_OBOS)])))
    obos = next(
        s for s in _by_type(stmts, "entity")
        if s["recordDetails"]["name"] == "OBOS EIENDOMSFORVALTNING AS"
    )
    assert _rels(stmts)[0]["recordDetails"]["interests"][0]["type"] == "seniorManagingOfficial"
    assert obos["statementId"] in officer_only_entity_ids(stmts)


def test_firmenbuch_management_roles_are_senior_managing_officials() -> None:
    from opencheck.sources.firmenbuch import _role_to_interest

    for code in ("GF", "GFI", "PK", "PKI", "PR", "PRI"):
        assert _role_to_interest(code, "")[0] == "seniorManagingOfficial", code
    assert _role_to_interest("", "Geschäftsführer")[0] == "seniorManagingOfficial"
    assert _role_to_interest("", "Prokurist")[0] == "seniorManagingOfficial"
    # Roles not in the decision keep their type.
    assert _role_to_interest("KOMPL", "")[0] == "otherInfluenceOrControl"


def test_brreg_roles_outside_the_decision_keep_their_type() -> None:
    from opencheck.bods.mapper import _BRREG_ROLE_MAP

    assert _BRREG_ROLE_MAP["DAGL"][0] == "seniorManagingOfficial"
    assert _BRREG_ROLE_MAP["FFØR"][0] == "seniorManagingOfficial"
    assert _BRREG_ROLE_MAP["INNH"][0] == "otherInfluenceOrControl"
    assert _BRREG_ROLE_MAP["REPR"][0] == "otherInfluenceOrControl"


def test_cyprus_corporate_official_has_no_assumed_jurisdiction() -> None:
    from opencheck.bods.mapper import map_cyprus_drcor

    stmts = list(map_cyprus_drcor({
        "reg_no": "123456",
        "organisation": {"organisation_name": "TEST HOLDINGS LTD", "organisation_type_code": "HE"},
        "officials": [
            {"person_or_organisation_name": "OFFSHORE SECRETARIAL SERVICES LIMITED",
             "official_position": "Secretary"},
            {"person_or_organisation_name": "ANDREAS GEORGIOU", "official_position": "Director"},
        ],
    }))
    secretary = next(
        s for s in _by_type(stmts, "entity")
        if s["recordDetails"]["name"] == "OFFSHORE SECRETARIAL SERVICES LIMITED"
    )
    assert "jurisdiction" not in secretary["recordDetails"]
    assert validate_shape(stmts) == []


def test_romania_corporate_representative_takes_only_the_filed_country() -> None:
    from opencheck.bods.mapper import map_onrc_romania
    from opencheck.bods.mappers.romania import _ro_country_code

    assert [_ro_country_code(x) for x in ("ROMANIA", "CIPRU", "REGATUL UNIT", "", None)] == [
        "RO", "CY", "GB", "", "",
    ]
    stmts = list(map_onrc_romania({
        "registration_number": "J40/1234/2010",
        "company": {"name": "TEST SRL"},
        "representatives": [
            {"name": "ADMIN SERVICES LTD", "role_slug": "administrator",
             "is_entity": True, "country": "CIPRU"},
            {"name": "INSOLVENCY IPURL", "role_slug": "administrator",
             "is_entity": True, "country": None},
        ],
    }))
    by_name = {s["recordDetails"]["name"]: s["recordDetails"] for s in _by_type(stmts, "entity")}
    assert by_name["ADMIN SERVICES LTD"]["jurisdiction"] == {"name": "Cyprus", "code": "CY"}
    assert "jurisdiction" not in by_name["INSOLVENCY IPURL"]
