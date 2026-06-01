"""Regression tests: previous/other names → BODS alternateNames.

- GLEIF ``entity.otherNames`` / ``transliteratedOtherNames`` → alternateNames
- Companies House ``profile.previous_company_names`` → alternateNames

Modelled on THE ARSENAL FOOTBALL CLUB LIMITED (LEI 213800M5PDIESHN4W786),
which has both fields populated.
"""

from __future__ import annotations

from typing import Any

from opencheck.bods.mapper import map_companies_house, map_gleif


def _gleif_bundle(other_names: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "lei": "213800M5PDIESHN4W786",
        "record": {
            "attributes": {
                "lei": "213800M5PDIESHN4W786",
                "entity": {
                    "legalName": {"name": "THE ARSENAL FOOTBALL CLUB LIMITED"},
                    "jurisdiction": "GB",
                    "otherNames": other_names,
                    "transliteratedOtherNames": [],
                },
                "registration": {"lastUpdateDate": "2024-01-01T00:00:00Z"},
            }
        },
    }


def _ch_bundle(previous: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "company_number": "00109244",
        "profile": {
            "company_number": "00109244",
            "company_name": "THE ARSENAL FOOTBALL CLUB LIMITED",
            "date_of_creation": "1910-04-26",
            "previous_company_names": previous,
        },
        "pscs": {},
        "officers": {},
    }


def _entity(stmts: Any) -> dict[str, Any]:
    return next(s for s in stmts if s["recordType"] == "entity")


# ---------------------------------------------------------------------------
# GLEIF
# ---------------------------------------------------------------------------


class TestGleifOtherNames:
    def test_other_names_mapped(self) -> None:
        b = map_gleif(_gleif_bundle([
            {"name": "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY",
             "language": "en", "type": "PREVIOUS_LEGAL_NAME"},
        ]))
        e = _entity(b.statements)
        assert e["recordDetails"]["alternateNames"] == [
            "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY"
        ]

    def test_no_other_names_omits_field(self) -> None:
        b = map_gleif(_gleif_bundle([]))
        e = _entity(b.statements)
        assert "alternateNames" not in e["recordDetails"]

    def test_primary_name_excluded_from_alternates(self) -> None:
        b = map_gleif(_gleif_bundle([
            {"name": "THE ARSENAL FOOTBALL CLUB LIMITED", "type": "PREVIOUS_LEGAL_NAME"},
            {"name": "GUNNERS LTD", "type": "TRADING_OR_OPERATING_NAME"},
        ]))
        e = _entity(b.statements)
        assert e["recordDetails"]["alternateNames"] == ["GUNNERS LTD"]


# ---------------------------------------------------------------------------
# Companies House
# ---------------------------------------------------------------------------


class TestCompaniesHousePreviousNames:
    def test_previous_names_mapped(self) -> None:
        b = map_companies_house(_ch_bundle([
            {"name": "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY",
             "effective_from": "1991-05-09", "ceased_on": "2023-04-18"},
            {"name": "THE WOOLWICH ARSENAL FOOTBALL AND ATHLETIC COMPANY LIMITED",
             "effective_from": "1910-04-26", "ceased_on": "1915-05-10"},
        ]))
        e = _entity(b.statements)
        assert e["recordDetails"]["alternateNames"] == [
            "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY",
            "THE WOOLWICH ARSENAL FOOTBALL AND ATHLETIC COMPANY LIMITED",
        ]

    def test_current_name_deduped(self) -> None:
        # Companies House lists the current name among previous names when it
        # was reused; it must not appear in alternateNames.
        b = map_companies_house(_ch_bundle([
            {"name": "THE ARSENAL FOOTBALL CLUB LIMITED", "ceased_on": "1915-05-10"},
            {"name": "OLD NAME LTD", "ceased_on": "1910-04-26"},
        ]))
        e = _entity(b.statements)
        assert e["recordDetails"]["alternateNames"] == ["OLD NAME LTD"]

    def test_no_previous_names_omits_field(self) -> None:
        b = map_companies_house(_ch_bundle([]))
        e = _entity(b.statements)
        assert "alternateNames" not in e["recordDetails"]
