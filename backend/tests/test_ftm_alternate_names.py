"""FtM aliases → BODS names (Phase 176).

`map_ftm` used to read exactly one name per party — `properties.name[0]`,
falling back to the caption — and drop every other name the record carried.
The round trip proved that was a defect rather than a choice: `bods/ftm.py`
maps BODS → FtM carrying `alternateNames` → `alias` and every person name
after the first → `alias`, so a bundle exported to FtM and back lost its
aliases in one direction only.

Measured against live OpenSanctions on 2026-09-07, which is what settled the
design questions these tests pin:

    Novo Nordisk    0 extra names        Maduro     41
    Shell PLC       3                    Putin      93 (+32 weakAlias)
    Rosneft        37                    Kadyrov   103

* **No cap.** A sanctioned person really does carry ~100 published names.
  Truncating a name list in a screening tool hides matches and leaves "which
  ones did we keep?" unanswerable; volume is a display problem.
* **`weakAlias` stays out**, as Phase 174 decided for the read side: upstream
  files a name there precisely when it should not be trusted alone, and a
  BODS statement is a published assertion that this *is* a name of the party.
* **Deduplication is on the comparable form**, storing what the source wrote.
"""

from __future__ import annotations

import pytest

from opencheck import names as names_mod
from opencheck.bods.ftm import map_to_ftm
from opencheck.bods.mappers.ftm import _FTM_NAME_TYPE, map_ftm
from opencheck.cross_check import _person_full_name
from opencheck.icij_check import _person_name

# The BODS v0.4 nameType codelist, read from schema/codelists/nameType.csv on
# the data-standard 0.4.0 branch. `individual` and `alias` are NOT in it —
# a widely-copied summary says otherwise, and that summary is what put a dead
# `type == "individual"` preference into two screening helpers.
_BODS_NAME_TYPES = frozenset(
    {"legal", "translation", "transliteration", "former", "alternative", "birth"}
)


def _entity_payload(**props):
    return {
        "id": "NK-entity",
        "schema": "Company",
        "caption": "Test Co",
        "properties": {"name": ["Test Co"], **props},
    }


def _person_payload(**props):
    return {
        "id": "NK-person",
        "schema": "Person",
        "caption": "Jane Smith",
        "properties": {"name": ["Jane Smith"], **props},
    }


def _map(payload):
    bundle = map_ftm(
        payload, source_id="opensanctions", source_url_builder=lambda i: f"u/{i}"
    )
    return bundle.statements[0]["recordDetails"]


# ---------------------------------------------------------------------------
# The mapping table itself
# ---------------------------------------------------------------------------


def test_every_read_name_property_has_a_bods_type() -> None:
    """A property we read but cannot land is dropped on the way into BODS —
    the same failure class as not reading it. ftm.py enforces this at import;
    this says so where a reader will look."""
    assert set(names_mod.FTM_NAME_PROPS) <= set(_FTM_NAME_TYPE)


def test_name_types_are_in_the_bods_codelist() -> None:
    assert set(_FTM_NAME_TYPE.values()) <= _BODS_NAME_TYPES


# ---------------------------------------------------------------------------
# Entity side — untyped alternateNames
# ---------------------------------------------------------------------------


def test_entity_carries_alias_previous_name_and_abbreviation() -> None:
    rd = _map(
        _entity_payload(
            alias=["Testco", "T-Co"],
            previousName=["Test Holdings"],
            abbreviation=["TC"],
        )
    )
    assert rd["name"] == "Test Co"
    assert rd["alternateNames"] == ["Testco", "T-Co", "Test Holdings", "TC"]


def test_entity_excludes_weak_alias() -> None:
    rd = _map(_entity_payload(alias=["Testco"], weakAlias=["Tesco"]))
    assert rd["alternateNames"] == ["Testco"]
    assert "Tesco" not in rd["alternateNames"]


def test_entity_keeps_extra_primary_names_as_alternates() -> None:
    """FtM records name variants under `name`; only the first becomes the
    BODS name, and the rest are names of the party all the same."""
    rd = _map(_entity_payload(name=["Test Co", "Test Company Limited"]))
    assert rd["name"] == "Test Co"
    assert rd["alternateNames"] == ["Test Company Limited"]


def test_entity_dedupes_on_the_comparable_form_keeping_the_source_spelling() -> None:
    rd = _map(_entity_payload(name=["Test Co", "TEST  CO"], alias=["test co", "Testco"]))
    assert rd["alternateNames"] == ["Testco"]


def test_entity_with_no_other_names_has_no_alternate_names_key() -> None:
    """The common case, and by far the most frequent in production: an
    ordinary company's OpenSanctions record is a GLEIF mirror with one name.
    Absence must stay absence rather than an empty array."""
    rd = _map(_entity_payload())
    assert "alternateNames" not in rd


def test_entity_alias_count_is_not_capped() -> None:
    aliases = [f"Alias {i}" for i in range(120)]
    rd = _map(_entity_payload(alias=aliases))
    assert len(rd["alternateNames"]) == 120


# ---------------------------------------------------------------------------
# Person side — typed names[]
# ---------------------------------------------------------------------------


def test_person_names_are_typed_by_source_property() -> None:
    rd = _map(
        _person_payload(
            alias=["Janie"],
            previousName=["Jane Doe"],
            abbreviation=["JS"],
        )
    )
    assert rd["names"] == [
        {"type": "legal", "fullName": "Jane Smith"},
        {"type": "alternative", "fullName": "Janie"},
        {"type": "former", "fullName": "Jane Doe"},
        {"type": "alternative", "fullName": "JS"},
    ]


def test_person_legal_name_stays_first() -> None:
    rd = _map(_person_payload(alias=["Aaaa Aaaa"] * 1))
    assert rd["names"][0] == {"type": "legal", "fullName": "Jane Smith"}


def test_person_excludes_weak_alias() -> None:
    rd = _map(_person_payload(alias=["Janie"], weakAlias=["J"]))
    assert [n["fullName"] for n in rd["names"]] == ["Jane Smith", "Janie"]


def test_person_dedupes_against_the_legal_name() -> None:
    rd = _map(_person_payload(alias=["jane  smith", "Janie"]))
    assert [n["fullName"] for n in rd["names"]] == ["Jane Smith", "Janie"]


def test_person_transliteration_survives_alongside_aliases() -> None:
    """A Cyrillic primary name still gains its deterministic Latin form, and
    the source's own aliases follow it."""
    payload = {
        "id": "NK-ru",
        "schema": "Person",
        "caption": "Владимир Путин",
        "properties": {"name": ["Владимир Путин"], "alias": ["Vova"]},
    }
    rd = _map(payload)
    types = [n["type"] for n in rd["names"]]
    assert types[0] == "legal"
    assert "transliteration" in types
    assert types[-1] == "alternative"
    assert rd["names"][-1]["fullName"] == "Vova"


# ---------------------------------------------------------------------------
# The round trip that proved this was a defect
# ---------------------------------------------------------------------------


def test_ftm_bods_ftm_round_trip_preserves_aliases() -> None:
    original = _entity_payload(alias=["Testco"], previousName=["Test Holdings"])
    bundle = map_ftm(
        original, source_id="opensanctions", source_url_builder=lambda i: "u"
    )
    back = map_to_ftm(list(bundle))
    entities = [e for e in back if e.get("schema") != "Ownership"]
    assert entities, "round trip produced no entity"
    aliases = set(entities[0]["properties"].get("alias") or [])
    assert {"Testco", "Test Holdings"} <= aliases


# ---------------------------------------------------------------------------
# The latent defect this change walked into
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("pick", [_person_full_name, _person_name])
def test_screening_reads_the_legal_name_whatever_the_order(pick) -> None:
    """Both helpers preferred ``type == "individual"`` — a code that is not in
    the BODS v0.4 codelist and that OpenCheck never emits — so the preference
    was dead and they fell through to "first entry with a fullName". That was
    right only because the legal name happened to be first. Ask for the type.
    """
    rd = {
        "names": [
            {"type": "alternative", "fullName": "The Bagman"},
            {"type": "legal", "fullName": "Jane Smith"},
        ]
    }
    assert pick(rd) == "Jane Smith"


@pytest.mark.parametrize("pick", [_person_full_name, _person_name])
def test_screening_still_honours_legacy_individual_from_third_party_bods(pick) -> None:
    rd = {
        "names": [
            {"type": "alternative", "fullName": "The Bagman"},
            {"type": "individual", "fullName": "Jane Smith"},
        ]
    }
    assert pick(rd) == "Jane Smith"


@pytest.mark.parametrize("pick", [_person_full_name, _person_name])
def test_screening_falls_back_to_the_first_usable_name(pick) -> None:
    rd = {"names": [{"type": "alternative", "fullName": "Only Name"}]}
    assert pick(rd) == "Only Name"


# ---------------------------------------------------------------------------
# Cross-script names — the bug this file's dedup key exists to avoid
# ---------------------------------------------------------------------------


def test_a_latin_alias_of_a_cyrillic_name_survives() -> None:
    """The first version of this deduplicated on ``names.normalise_name``,
    which folds Cyrillic and Greek into Latin — so "Газпром" and "Gazprom"
    became one key and the Latin form was deleted as a duplicate of its own
    original. That is the single most useful pair in the list. Three existing
    tests caught it (a Wikidata multilingual entity, an Aeroflot
    transliteration, an ABR trading name); ``names.display_name_key`` folds
    case and spacing only.
    """
    rd = _map(
        _entity_payload(name=["Газпром"], alias=["Gazprom", "Γκαζπρόμ", "GAZPROM"])
    )
    assert rd["alternateNames"] == ["Gazprom", "Γκαζπρόμ"]


def test_display_name_key_folds_case_and_spacing_but_never_script() -> None:
    key = names_mod.display_name_key
    assert key("Rosneft  Oil COMPANY") == key("rosneft oil company")
    assert key("Газпром") != key("Gazprom")
    assert key(None) == ""
