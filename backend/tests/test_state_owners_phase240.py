"""Phase 240 — state owners typed as state bodies, and one holding per state.

The DQ-5 finding of the Opus 5.5 check: Equinor's "FINANSDEPARTEMENTET"
(OpenSanctions), "Ministry of Trade, Industry and Fisheries" and "Ministry of
Energy of Norway" (both Wikidata) each held 67%, all typed
``registeredEntity``; Rosneft's "Government of Russia" and "Federal Agency for
State Property Management" likewise. So STATE_CONTROLLED never fired, and the
listed owners summed past 200%.

Three causes, each pinned here on the live data's shapes (read 24 Sept 2026):

* Wikidata types its ministries by subclass ("ministry of trade", "Ministry of
  Norway", "Federal Agency", "executive branch"), so a direct-P31 match missed
  every one; and two of Equinor's three 67% statements carry a P582 end time
  the query never read.
* OpenSanctions files FINANSDEPARTEMENTET as a ``Company`` tagged ``gov.soe``;
  its LEI is a RESIDENT_GOVERNMENT_ENTITY in GLEIF.
* Two sources naming one stake by different bodies read as two holders.
"""

from __future__ import annotations

from opencheck.bods import state_bodies
from opencheck.bods.mappers.ftm import _ftm_entity_statement, _ftm_state_body_basis
from opencheck.bods.mappers.wikidata import map_wikidata
from opencheck.risk import (
    GLEIF_GOVERNMENT_DETAILS_PREFIX,
    INCLUDING_ENDED,
    STATE_CONTROLLED,
    _state_controlled_signals,
    assess_bundle,
    merge_state_controlled,
)
from opencheck.routers.lookup import _merge_signals
from opencheck.sources.wikidata import (
    _CLASS_ROOTS,
    _classify_owner,
    _ownership_classes,
    _parse_class_roots,
    _parse_ownership,
    _wikidata_date,
)

_ENT = "http://www.wikidata.org/entity/"
TODAY = "2026-09-24"


def _own_row(st, owner, label, *, cls=None, prop=None, start=None, end=None,
             country=None, via="P127"):
    r: dict = {
        "st": {"value": f"http://www.wikidata.org/entity/statement/{st}"},
        "owner": {"value": _ENT + owner},
        "ownerLabel": {"value": label},
        "via": {"value": via},
    }
    if cls:
        r["ownerClass"] = {"value": _ENT + cls}
    if prop is not None:
        r["proportion"] = {"value": prop}
    if start:
        r["start"] = {"value": start}
    if end:
        r["end"] = {"value": end}
    if country:
        r["ownerCountry"] = {"value": country}
    return r


# Equinor ASA (Q1776022), P127 as read from Wikidata on 24 Sept 2026.
EQUINOR_ROWS = [
    _own_row("s-moe", "Q2607880", "Ministry of Energy of Norway", cls="Q19973795",
             prop="0.67", start="1978-01-01T00:00:00Z", end="2021-12-31T00:00:00Z",
             country="NO"),
    _own_row("s-moe", "Q2607880", "Ministry of Energy of Norway", cls="Q109644271",
             prop="0.67", start="1978-01-01T00:00:00Z", end="2021-12-31T00:00:00Z",
             country="NO"),
    _own_row("s-gpf", "Q11969389", "Government Pension Fund Norway", cls="Q270791",
             prop="0.0345", country="NO"),
    _own_row("s-gpf", "Q11969389", "Government Pension Fund Norway", cls="Q699386",
             prop="0.0345", country="NO"),
    _own_row("s-ntf", "Q18177740", "Ministry of Trade, Industry and Fisheries",
             cls="Q1243341", prop="0.67", start="2022-01-01T00:00:00Z", country="NO"),
    _own_row("s-ntf", "Q18177740", "Ministry of Trade, Industry and Fisheries",
             cls="Q109644271", prop="0.67", start="2022-01-01T00:00:00Z", country="NO"),
    _own_row("s-ind", "Q12715675", "Ministry of Industry", cls="Q109644271",
             start="1972-01-01T00:00:00Z", end="1978-01-01T00:00:00Z", country="NO"),
]

# The P279* roots each class reaches — the shape _CLASS_ROOTS_QUERY returns,
# as read from WDQS on 24 Sept 2026.
EQUINOR_CLASS_ROOTS = {
    "Q19973795": ["Q192350", "Q327333"],   # ministry of energy
    "Q109644271": ["Q192350", "Q327333"],  # Ministry of Norway
    "Q1243341": ["Q192350", "Q327333"],    # ministry of trade
    "Q270791": ["Q270791", "Q4830453", "Q783794"],  # state-owned enterprise
    "Q699386": ["Q327333", "Q783794"],     # statutory corporation
}

# Rosneft (Q1141123).
ROSNEFT_ROWS = [
    _own_row("r-gov", "Q1140115", "Government of Russia", cls="Q35798", country="RU"),
    _own_row("r-fa", "Q1659060", "Federal Agency for State Property Management",
             cls="Q43229", via="P749", country="RU"),
    _own_row("r-fa", "Q1659060", "Federal Agency for State Property Management",
             cls="Q14944295", via="P749", country="RU"),
    _own_row("r-rng", "Q4397843", "Rosneftegaz", cls="Q4830453", via="P749", country="RU"),
    _own_row("r-rng", "Q4397843", "Rosneftegaz", cls="Q6881511", via="P749", country="RU"),
]
ROSNEFT_CLASS_ROOTS = {
    "Q35798": ["Q327333", "Q35798"],     # executive branch
    "Q14944295": ["Q327333", "Q35798"],  # Federal Agency
    "Q43229": [],                        # organization
    "Q4830453": ["Q4830453"],            # business
    "Q6881511": [],                      # enterprise
}


# --- Wikidata: classification ----------------------------------------------


def test_equinor_ministries_classify_as_state_bodies_through_subclasses():
    by = {o["qid"]: o for o in _parse_ownership(EQUINOR_ROWS, EQUINOR_CLASS_ROOTS, today=TODAY)}
    for qid in ("Q2607880", "Q18177740", "Q12715675"):
        assert by[qid]["entity_type"] == "stateBody", qid
    # A state-owned enterprise and a statutory corporation are companies, even
    # though Wikidata files both under government classes.
    assert by["Q11969389"]["entity_type"] == "registeredEntity"


def test_without_class_roots_the_direct_classes_still_decide():
    # A failed roots query degrades to the pre-Phase-240 behaviour, never worse.
    by = {o["qid"]: o for o in _parse_ownership(EQUINOR_ROWS, None, today=TODAY)}
    assert by["Q18177740"]["entity_type"] == "registeredEntity"


def test_rosneft_government_and_agency_are_state_bodies_rosneftegaz_is_not():
    by = {o["qid"]: o for o in _parse_ownership(ROSNEFT_ROWS, ROSNEFT_CLASS_ROOTS, today=TODAY)}
    assert by["Q1140115"]["entity_type"] == "stateBody"
    assert by["Q1659060"]["entity_type"] == "stateBody"
    assert by["Q4397843"]["entity_type"] == "registeredEntity"  # the GLIE holding company


def test_business_beats_government_class():
    assert _classify_owner({"Q327333", "Q783794"}, "Statkraft") == "company"
    assert _classify_owner({"Q192350"}, "Ministry") == "statebody"
    assert _classify_owner({"Q3624078"}, "Norway") == "state"
    assert _classify_owner({"Q1061648"}, "Norges Bank Investment Management") == "glie"
    assert _classify_owner({"Q13417114"}, "House of Bourbon") == "family"


def test_class_roots_parsing_answers_every_asked_class():
    rows = [
        {"cls": {"value": _ENT + "Q1243341"}, "root": {"value": _ENT + "Q192350"}},
        {"cls": {"value": _ENT + "Q1243341"}, "root": {"value": _ENT + "Q327333"}},
    ]
    out = _parse_class_roots(rows, {"Q1243341", "Q43229"})
    assert out == {"Q1243341": ["Q192350", "Q327333"], "Q43229": []}
    assert _ownership_classes(EQUINOR_ROWS) >= {"Q19973795", "Q109644271", "Q270791"}


def test_root_table_carries_the_checked_qids():
    # The Phase 62 table named "Lautenschläger" as the sovereign-wealth-fund
    # class; these are the QIDs checked against Wikidata on 24 Sept 2026.
    assert {"Q192350", "Q327333", "Q35798", "Q7188", "Q1061648", "Q270791"} <= _CLASS_ROOTS
    assert "Q1808582" not in _CLASS_ROOTS
    assert "Q2659904" not in _CLASS_ROOTS  # government organization: SOEs sit under it


# --- Wikidata: dates and the >200% ------------------------------------------


def test_ended_statements_are_dated_not_concurrent():
    by = {o["qid"]: o for o in _parse_ownership(EQUINOR_ROWS, EQUINOR_CLASS_ROOTS, today=TODAY)}
    assert by["Q18177740"]["end_date"] is None
    assert by["Q18177740"]["start_date"] == "2022-01-01"
    assert by["Q2607880"]["end_date"] == "2021-12-31"
    assert by["Q12715675"]["end_date"] == "1978-01-01"
    current = [o for o in by.values() if o["end_date"] is None and o["share_percent"]]
    assert sum(o["share_percent"] for o in current) < 100


def test_wikidata_date():
    assert _wikidata_date("2021-12-31T00:00:00Z") == "2021-12-31"
    assert _wikidata_date("+1978-00-00T00:00:00Z") == "1978-01-01"  # rounded, as BODS asks
    assert _wikidata_date("not a date") is None
    assert _wikidata_date(None) is None


def _wikidata_bundle(owners):
    return {
        "source_id": "wikidata",
        "qid": "Q1776022",
        "bindings": [{}],
        "summary": {
            "qid": "Q1776022",
            "label": "Equinor ASA",
            "is_entity": True,
            "controlling_owners": owners,
        },
    }


def test_mapper_dates_interests_and_sets_owner_jurisdiction():
    owners = _parse_ownership(EQUINOR_ROWS, EQUINOR_CLASS_ROOTS, today=TODAY)
    bods = list(map_wikidata(_wikidata_bundle(owners)))
    by_name = {
        s["recordDetails"]["name"]: s for s in bods if s["recordType"] == "entity"
    }
    ntf = by_name["Ministry of Trade, Industry and Fisheries"]
    assert ntf["recordDetails"]["entityType"]["type"] == "stateBody"
    assert ntf["recordDetails"]["jurisdiction"] == {"name": "Norway", "code": "NO"}
    rels = {
        s["recordDetails"]["interestedParty"]: s
        for s in bods if s["recordType"] == "relationship"
    }
    moe = by_name["Ministry of Energy of Norway"]["statementId"]
    interest = rels[moe]["recordDetails"]["interests"][0]
    assert interest["endDate"] == "2021-12-31"
    assert "ended 2021-12-31" in interest["details"]
    assert "endDate" not in rels[ntf["statementId"]]["recordDetails"]["interests"][0]


# --- OpenSanctions / FtM ------------------------------------------------------


# FINANSDEPARTEMENTET as OpenSanctions serves it (NK-jTRpKp7CNjp8y6naJLRdeT).
FINANSDEP = {
    "id": "NK-jTRpKp7CNjp8y6naJLRdeT",
    "schema": "Company",
    "properties": {
        "name": ["FINANSDEPARTEMENTET", "Norges regjering", "Government of Norway"],
        "leiCode": ["549300L0BT3FJTN9MX24"],
        "registrationNumber": ["972 417 807"],
        "topics": ["gov.soe", "corp.public"],
        "jurisdiction": ["no"],
        "legalForm": ["Staten"],
        "sector": ["government"],
    },
}


def test_ftm_public_body_and_government_topics_are_state_bodies():
    assert _ftm_state_body_basis("PublicBody", {}) == "FollowTheMoney schema PublicBody"
    assert _ftm_state_body_basis("Company", {"topics": ["gov.national"]}).endswith("gov.national")
    stmt = _ftm_entity_statement(
        {"id": "x", "schema": "PublicBody", "properties": {"name": ["Donetsk People's Republic"]}},
        "opensanctions", None,
    )
    assert stmt["recordDetails"]["entityType"] == {
        "type": "stateBody", "details": "FollowTheMoney schema PublicBody",
    }


def test_ftm_gov_soe_alone_is_not_a_state_body():
    # gov.soe describes a state-owned *enterprise*; BODS models those as
    # registeredEntity. That OpenSanctions also tags ministries with it is
    # why the GLEIF category below is needed.
    assert _ftm_state_body_basis("Company", FINANSDEP["properties"]) is None
    stmt = _ftm_entity_statement(FINANSDEP, "opensanctions", None)
    assert stmt["recordDetails"]["entityType"]["type"] == "registeredEntity"


# --- GLEIF category via the mirror -------------------------------------------


def _gleif_lookup(leis):
    known = {"549300L0BT3FJTN9MX24": {"category": "RESIDENT_GOVERNMENT_ENTITY",
                                      "subCategory": "STATE_GOVERNMENT"},
             "OW6OFBNCKXC4US5C7523": {"category": "GENERAL", "subCategory": None}}
    return {lei: known[lei] for lei in leis if lei in known}


def _os_equinor_bundle():
    subject = _ftm_entity_statement(
        {"id": "NK-jsAdAZqk3fG7KumbmDM5LW", "schema": "Company",
         "properties": {"name": ["Equinor"], "leiCode": ["OW6OFBNCKXC4US5C7523"],
                        "jurisdiction": ["no"]}},
        "opensanctions", None,
    )
    owner = _ftm_entity_statement(FINANSDEP, "opensanctions", None)
    sub = _ftm_entity_statement(
        {"id": "NK-orsted", "schema": "Company", "properties": {"name": ["Ørsted"]}},
        "opensanctions", None,
    )
    danish_state = _ftm_entity_statement(
        {"id": "NK-dk", "schema": "PublicBody",
         "properties": {"name": ["Danish Ministry of Finance"], "jurisdiction": ["dk"]}},
        "opensanctions", None,
    )

    def rel(rid, subj, ip, pct):
        return {
            "statementId": rid, "recordId": rid, "recordType": "relationship",
            "recordDetails": {
                "isComponent": False,
                "subject": subj["statementId"],
                "interestedParty": ip["statementId"],
                "interests": [{"type": "shareholding", "share": {"exact": pct}}],
            },
        }

    return [
        subject, owner, sub, danish_state,
        rel("os-r1", subject, owner, 67.0),
        rel("os-r2", sub, subject, 10.0),       # Equinor owns 10% of Ørsted
        rel("os-r3", sub, danish_state, 50.1),  # the Danish state owns Ørsted
    ]


def test_gleif_government_category_retypes_the_owner_and_annotates_it():
    bods = _os_equinor_bundle()
    out = state_bodies.classify_government_entities(bods, lookup=_gleif_lookup)
    owner = next(s for s in out if s["recordDetails"].get("name") == "FINANSDEPARTEMENTET")
    assert owner["recordDetails"]["entityType"] == {
        "type": "stateBody",
        "details": "Resident government entity in GLEIF (state government)",
    }
    (ann,) = owner["annotations"]
    assert ann["statementPointerTarget"] == "/recordDetails/entityType/type"
    assert ann["motivation"] == "transformation"
    assert "registeredEntity" in ann["description"]
    # The input is untouched — stored bundles are shared between requests.
    assert next(
        s for s in bods if s["recordDetails"].get("name") == "FINANSDEPARTEMENTET"
    )["recordDetails"]["entityType"]["type"] == "registeredEntity"
    # A GENERAL-category LEI (the subject) is left alone.
    subject = next(s for s in out if s["recordDetails"].get("name") == "Equinor")
    assert subject["recordDetails"]["entityType"]["type"] == "registeredEntity"


def test_no_mirror_changes_nothing():
    bods = _os_equinor_bundle()
    assert state_bodies.classify_government_entities(bods, lookup=lambda leis: {}) is bods


def test_details_prefix_is_the_one_the_caveat_reads():
    assert state_bodies.DETAILS_PREFIX == GLEIF_GOVERNMENT_DETAILS_PREFIX


def test_only_owners_above_the_subject_count():
    # The Danish state owns Ørsted, which Equinor owns 10% of: that is not
    # state control of Equinor, and must not fire on its own.
    bods = state_bodies.classify_government_entities(_os_equinor_bundle(), lookup=lambda leis: {})
    assert _state_controlled_signals("opensanctions", "NK-jsAdAZqk3fG7KumbmDM5LW", bods) == []


# --- The merged signal ---------------------------------------------------------


def _signals_for_equinor():
    os_bods = state_bodies.classify_government_entities(
        _os_equinor_bundle(), lookup=_gleif_lookup
    )
    # assess_bundle reads OpenSanctions' hit id from the raw bundle.
    os_sigs = assess_bundle(
        "opensanctions", {"entity_id": "NK-jsAdAZqk3fG7KumbmDM5LW"}, os_bods,
        hit_id="NK-jsAdAZqk3fG7KumbmDM5LW",
    )
    owners = _parse_ownership(EQUINOR_ROWS, EQUINOR_CLASS_ROOTS, today=TODAY)
    wd_bods = list(map_wikidata(_wikidata_bundle(owners)))
    wd_sigs = assess_bundle("wikidata", {}, wd_bods, hit_id="Q1776022")
    return [s.to_dict() for s in os_sigs], [s.to_dict() for s in wd_sigs]


def test_each_source_fires_state_controlled_for_equinor():
    os_sigs, wd_sigs = _signals_for_equinor()
    assert STATE_CONTROLLED in {s["code"] for s in os_sigs}
    assert STATE_CONTROLLED in {s["code"] for s in wd_sigs}


def test_equinor_carries_one_state_controlled_with_one_merged_state_holder():
    os_sigs, wd_sigs = _signals_for_equinor()
    merged = [s for s in _merge_signals(os_sigs, wd_sigs) if s["code"] == STATE_CONTROLLED]
    assert len(merged) == 1
    ev = merged[0]["evidence"]
    (norway,) = ev["state_holdings"]
    assert norway["state"] == "NO"
    assert norway["current"] is True
    assert norway["shares"] == ["67%"]
    assert norway["sources"] == ["opensanctions", "wikidata"]
    names = {m["name"] for m in ev["matches"]}
    assert {"FINANSDEPARTEMENTET", "Ministry of Trade, Industry and Fisheries"} <= names
    # Every state node is badged — the matches carry each statement id.
    assert len({m["statement_id"] for m in ev["matches"]}) == len(ev["matches"]) == 4
    summary = merged[0]["summary"]
    assert "Norway, 67% — FINANSDEPARTEMENTET (OpenSanctions)" in summary
    assert "formerly Ministry of Energy of Norway (Wikidata)" in summary
    assert "state body per its GLEIF entity category" in summary
    # A current holding exists for the group, so nothing rests on ended links.
    assert INCLUDING_ENDED not in summary
    assert "includes_ended_relationships" not in ev


def test_merge_is_order_independent():
    os_sigs, wd_sigs = _signals_for_equinor()
    a = [s for s in _merge_signals(os_sigs, wd_sigs) if s["code"] == STATE_CONTROLLED][0]
    b = [s for s in _merge_signals(wd_sigs, os_sigs) if s["code"] == STATE_CONTROLLED][0]
    assert a["evidence"]["state_holdings"] == b["evidence"]["state_holdings"]


def test_rosneft_carries_one_russian_state_holding():
    owners = _parse_ownership(ROSNEFT_ROWS, ROSNEFT_CLASS_ROOTS, today=TODAY)
    bundle = _wikidata_bundle(owners)
    bundle["qid"] = bundle["summary"]["qid"] = "Q1141123"
    bundle["summary"]["label"] = "Rosneft"
    sigs = [s.to_dict() for s in assess_bundle(
        "wikidata", {}, list(map_wikidata(bundle)), hit_id="Q1141123"
    )]
    (sig,) = [s for s in _merge_signals(sigs) if s["code"] == STATE_CONTROLLED]
    (russia,) = sig["evidence"]["state_holdings"]
    assert russia["state"] == "RU"
    assert set(sig["evidence"]["state_owners"]) == {
        "Government of Russia", "Federal Agency for State Property Management",
    }
    assert "Russian Federation — " in sig["summary"]


def test_a_state_with_only_ended_holdings_is_qualified():
    rows = [r for r in EQUINOR_ROWS if "s-moe" in r["st"]["value"]]
    owners = _parse_ownership(rows, EQUINOR_CLASS_ROOTS, today=TODAY)
    sigs = assess_bundle("wikidata", {}, list(map_wikidata(_wikidata_bundle(owners))),
                         hit_id="Q1776022")
    (sig,) = [s for s in sigs if s.code == STATE_CONTROLLED]
    assert INCLUDING_ENDED in sig.summary
    assert sig.evidence["includes_ended_relationships"] is True


def test_merge_state_controlled_ignores_signals_without_matches():
    new = {"code": STATE_CONTROLLED, "source_id": "x", "hit_id": "h", "evidence": {}}
    assert merge_state_controlled({"evidence": {}}, new) is new


def test_a_publishers_own_bods_is_not_retyped():
    bods = _os_equinor_bundle()
    assert state_bodies.classify_government_entities(
        bods, source_id="meip", lookup=_gleif_lookup
    ) is bods


def test_default_lookup_reads_the_golden_copy_mirror(monkeypatch):
    from types import SimpleNamespace

    from opencheck import entity_pages

    rows = {
        "549300L0BT3FJTN9MX24": SimpleNamespace(
            detail={"category": "RESIDENT_GOVERNMENT_ENTITY", "subCategory": "STATE_GOVERNMENT"}
        ),
        "OW6OFBNCKXC4US5C7523": SimpleNamespace(detail={"category": "GENERAL"}),
    }
    store = SimpleNamespace(get_many=lambda leis: {lei: rows[lei] for lei in leis if lei in rows})
    monkeypatch.setattr(entity_pages, "get_store", lambda: store)
    out = state_bodies.classify_government_entities(_os_equinor_bundle())
    types = {
        s["recordDetails"]["name"]: s["recordDetails"]["entityType"]["type"]
        for s in out if s["recordType"] == "entity"
    }
    assert types["FINANSDEPARTEMENTET"] == "stateBody"
    assert types["Equinor"] == "registeredEntity"


async def test_the_lookup_deepen_path_retypes_before_the_risk_rules(monkeypatch):
    # The wiring, not just the helper: a source bundle deepened by the lookup
    # pipeline reaches the risk rules with the government owner already a
    # state body, so its own risk_signals carry STATE_CONTROLLED.
    from opencheck import provenance as _provenance
    from opencheck.bods import state_bodies as sb
    from opencheck.routers import lookup

    async def fake_fetch(adapter, hit_id, **kwargs):
        return {"entity_id": hit_id}, _provenance.STUB_PROVENANCE

    monkeypatch.setattr(lookup, "_fetch_with_provenance", fake_fetch)
    monkeypatch.setattr(lookup, "_bods_data_override", lambda *a: None)
    monkeypatch.setattr(lookup, "_mapper_for", lambda sid: (lambda raw: _os_equinor_bundle()))
    monkeypatch.setattr(sb, "_default_lookup", _gleif_lookup)

    deep = await lookup._safe_deepen("opensanctions", "NK-jsAdAZqk3fG7KumbmDM5LW")
    owner = next(s for s in deep["bods"] if s["recordDetails"].get("name") == "FINANSDEPARTEMENTET")
    assert owner["recordDetails"]["entityType"]["type"] == "stateBody"
    assert "STATE_CONTROLLED" in {s["code"] for s in deep["risk_signals"]}
