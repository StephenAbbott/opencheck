"""Seed demo fixtures into ``data/cache/demos/``.

OpenCheck's two-tier cache (``demos/`` then ``live/``) means anything
shipped under ``data/cache/demos/`` overrides the Phase 0 stub path —
the app can demo cleanly with no API keys, no network access.

This script writes hand-curated fixtures using public data so the
in-repo demo set stays small, deterministic, and reviewable. The
fixture payloads are intentionally minimal — just enough to drive the
BODS mapper, the cross-source reconciler, and every risk rule the
project advertises.

Demo subjects
-------------

* **BP p.l.c.** — clean cross-source entity story.
  Bridges via LEI ``213800LBDB8WB3QGVN21`` (GLEIF ↔ Companies House)
  and Wikidata Q-ID ``Q152057``. UK-only, simple, no risk flags fire.

* **Vladimir Putin** — PEP, multi-source story.
  Bridges via Wikidata Q-ID ``Q7747`` across Wikidata, OpenSanctions
  and EveryPolitician. Triggers ``PEP`` from three independent sources
  and ``SANCTIONED`` from the OpenSanctions ``role.pep`` + ``sanction``
  topic combination.

* **Rosneft** — sanctioned entity, AMLA story.
  Real LEI ``253400JSI04G42PAAS27``. Triggers ``SANCTIONED`` (OS topic)
  and ``NON_EU_JURISDICTION`` (RU jurisdiction outside EU+EEA) — the
  AMLA CDD RTS jurisdiction-condition signal.

Run
---

    python -m backend.scripts.seed_demo_fixtures

Or directly:

    cd backend && python scripts/seed_demo_fixtures.py

Idempotent — re-running overwrites existing demo files.
"""

from __future__ import annotations

import json
from pathlib import Path

# Make ``opencheck`` importable when this script is run directly.
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.sources.companies_house import _slug as ch_slug
from opencheck.sources.everypolitician import _slug as ep_slug
from opencheck.sources.gleif import _slug as gleif_slug
from opencheck.sources.openaleph import _slug as al_slug  # noqa: F401
from opencheck.sources.opensanctions import _slug as os_slug
from opencheck.sources.wikidata import _slug as wd_slug


# ---------------------------------------------------------------------
# Resolve project paths
# ---------------------------------------------------------------------


def _project_root() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "data").is_dir():
            return parent
    return here.parents[2]


DEMOS_ROOT = _project_root() / "data" / "cache" / "demos"


def _write(cache_key: str, payload: dict | list | None) -> Path:
    target = DEMOS_ROOT / f"{cache_key}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as fh:
        json.dump({"_cached_at": 0, "payload": payload}, fh, indent=2)
    return target


# ---------------------------------------------------------------------
# BP — clean entity demo
# ---------------------------------------------------------------------


def seed_bp() -> list[Path]:
    """BP p.l.c. — entity, UK-incorporated, real public IDs."""
    written: list[Path] = []

    bp_lei = "213800LBDB8WB3QGVN21"
    bp_coh = "00102498"
    bp_qid = "Q152057"

    # GLEIF search — single hit
    written.append(
        _write(
            f"gleif/search/{gleif_slug('BP')}",
            {
                "data": [
                    {
                        "id": bp_lei,
                        "type": "lei-records",
                        "attributes": {
                            "lei": bp_lei,
                            "entity": {
                                "legalName": {"name": "BP P.L.C."},
                                "jurisdiction": "GB",
                                "registeredAs": bp_coh,
                                "category": "GENERAL",
                            },
                            "registration": {"status": "ISSUED"},
                        },
                    }
                ]
            },
        )
    )
    # GLEIF deepen — record + no parents
    written.append(
        _write(
            f"gleif/lei/{bp_lei}",
            {
                "data": {
                    "id": bp_lei,
                    "type": "lei-records",
                    "attributes": {
                        "lei": bp_lei,
                        "entity": {
                            "legalName": {"name": "BP P.L.C."},
                            "jurisdiction": "GB",
                            "registeredAs": bp_coh,
                        },
                        "registration": {"status": "ISSUED"},
                    },
                }
            },
        )
    )

    # Companies House search
    written.append(
        _write(
            f"companies_house/search/companies/{ch_slug('BP')}",
            {
                "items": [
                    {
                        "company_number": bp_coh,
                        "title": "BP P.L.C.",
                        "company_status": "active",
                        "address_snippet": "1 St James's Square, London, SW1Y 4PD",
                    }
                ]
            },
        )
    )
    # Companies House deepen — profile + minimal officers/PSCs
    written.append(
        _write(
            f"companies_house/company/{bp_coh}",
            {
                "company_number": bp_coh,
                "company_name": "BP P.L.C.",
                "company_status": "active",
                "type": "plc",
                "date_of_creation": "1909-04-14",
                "registered_office_address": {
                    "address_line_1": "1 St James's Square",
                    "locality": "London",
                    "postal_code": "SW1Y 4PD",
                    "country": "United Kingdom",
                },
            },
        )
    )
    written.append(
        _write(
            f"companies_house/company/{bp_coh}/officers",
            {"items": [], "active_count": 0, "resigned_count": 0},
        )
    )
    written.append(
        _write(
            f"companies_house/company/{bp_coh}/pscs",
            {"items": [], "active_count": 0, "ceased_count": 0},
        )
    )

    # Wikidata search — wbsearchentities response shape
    written.append(
        _write(
            f"wikidata/search/entity/{wd_slug('BP')}",
            {
                "search": [
                    {
                        "id": bp_qid,
                        "title": bp_qid,
                        "label": "BP",
                        "description": "British multinational oil and gas company",
                        "match": {"type": "label", "language": "en", "text": "BP"},
                    }
                ]
            },
        )
    )
    # Wikidata fetch — SPARQL bindings shape
    written.append(
        _write(
            f"wikidata/fetch/{bp_qid}",
            {
                "head": {
                    "vars": [
                        "qid", "label", "description", "instance",
                        "instanceLabel", "country", "countryLabel",
                        "lei", "openCorporates", "isin", "inception",
                    ]
                },
                "results": {
                    "bindings": [
                        {
                            "qid": {"type": "uri", "value": f"http://www.wikidata.org/entity/{bp_qid}"},
                            "label": {"type": "literal", "value": "BP"},
                            "description": {"type": "literal", "value": "British multinational oil and gas company"},
                            "instance": {"type": "uri", "value": "http://www.wikidata.org/entity/Q6881511"},
                            "instanceLabel": {"type": "literal", "value": "enterprise"},
                            "country": {"type": "uri", "value": "http://www.wikidata.org/entity/Q145"},
                            "countryLabel": {"type": "literal", "value": "United Kingdom"},
                            "lei": {"type": "literal", "value": bp_lei},
                            "inception": {"type": "literal", "value": "+1909-04-14T00:00:00Z"},
                        }
                    ]
                },
            },
        )
    )
    return written


# ---------------------------------------------------------------------
# Vladimir Putin — PEP demo
# ---------------------------------------------------------------------


def seed_putin() -> list[Path]:
    """Vladimir Putin — multi-source PEP story bridged on Q7747."""
    written: list[Path] = []

    qid = "Q7747"
    os_id = "Q7747-pep"  # OpenSanctions PEPs dataset uses Q-ID-derived slugs.

    # Wikidata search
    written.append(
        _write(
            f"wikidata/search/person/{wd_slug('Vladimir Putin')}",
            {
                "search": [
                    {
                        "id": qid,
                        "title": qid,
                        "label": "Vladimir Putin",
                        "description": "President of Russia",
                        "match": {"type": "label", "language": "en", "text": "Vladimir Putin"},
                    }
                ]
            },
        )
    )
    # Wikidata fetch — SPARQL bindings (multi-row OPTIONAL groups)
    written.append(
        _write(
            f"wikidata/fetch/{qid}",
            {
                "head": {
                    "vars": [
                        "qid", "label", "description", "instance",
                        "instanceLabel", "dob", "dod", "citizenship",
                        "citizenshipLabel", "position", "positionLabel",
                        "positionStart", "positionEnd",
                    ]
                },
                "results": {
                    "bindings": [
                        # Row 1: human + Russian citizen + currently-held position (no end)
                        {
                            "qid": {"type": "uri", "value": f"http://www.wikidata.org/entity/{qid}"},
                            "label": {"type": "literal", "value": "Vladimir Putin"},
                            "description": {"type": "literal", "value": "President of Russia"},
                            "instance": {"type": "uri", "value": "http://www.wikidata.org/entity/Q5"},
                            "instanceLabel": {"type": "literal", "value": "human"},
                            "dob": {"type": "literal", "value": "1952-10-07T00:00:00Z"},
                            "citizenship": {"type": "uri", "value": "http://www.wikidata.org/entity/Q159"},
                            "citizenshipLabel": {"type": "literal", "value": "Russia"},
                            "position": {"type": "uri", "value": "http://www.wikidata.org/entity/Q11696"},
                            "positionLabel": {"type": "literal", "value": "President of Russia"},
                            "positionStart": {"type": "literal", "value": "2012-05-07T00:00:00Z"},
                        },
                        # Row 2: also held PM (ended) — proves end-date filtering works
                        {
                            "qid": {"type": "uri", "value": f"http://www.wikidata.org/entity/{qid}"},
                            "label": {"type": "literal", "value": "Vladimir Putin"},
                            "instance": {"type": "uri", "value": "http://www.wikidata.org/entity/Q5"},
                            "position": {"type": "uri", "value": "http://www.wikidata.org/entity/Q899"},
                            "positionLabel": {"type": "literal", "value": "Prime Minister of Russia"},
                            "positionStart": {"type": "literal", "value": "2008-05-08T00:00:00Z"},
                            "positionEnd": {"type": "literal", "value": "2012-05-07T00:00:00Z"},
                        },
                    ]
                },
            },
        )
    )

    # OpenSanctions search — Person schema, with PEP topic
    written.append(
        _write(
            f"opensanctions/search/Person/{os_slug('Vladimir Putin')}",
            {
                "results": [
                    {
                        "id": os_id,
                        "schema": "Person",
                        "caption": "Vladimir Putin",
                        "properties": {
                            "name": ["Vladimir Putin"],
                            "wikidataId": [qid],
                            "topics": ["role.pep"],
                            "country": ["ru"],
                        },
                        "topics": ["role.pep", "sanction"],
                        "datasets": ["peps", "ru_rupep"],
                    }
                ]
            },
        )
    )
    # OpenSanctions deepen
    written.append(
        _write(
            f"opensanctions/entity/{os_slug(os_id)}",
            {
                "id": os_id,
                "schema": "Person",
                "caption": "Vladimir Putin",
                "properties": {
                    "name": ["Vladimir Putin"],
                    "wikidataId": [qid],
                    "topics": ["role.pep", "sanction"],
                    "birthDate": ["1952-10-07"],
                    "position": ["President of Russia"],
                    "country": ["ru"],
                },
                "topics": ["role.pep", "sanction"],
                "datasets": ["peps", "ru_rupep"],
            },
        )
    )

    # EveryPolitician (via OS PEPs dataset) — same FtM shape as OS,
    # different cache namespace.
    written.append(
        _write(
            f"everypolitician/search/{ep_slug('Vladimir Putin')}",
            {
                "results": [
                    {
                        "id": os_id,
                        "schema": "Person",
                        "caption": "Vladimir Putin",
                        "properties": {
                            "name": ["Vladimir Putin"],
                            "wikidataId": [qid],
                            "position": ["President of Russia"],
                            "country": ["ru"],
                        },
                    }
                ]
            },
        )
    )
    written.append(
        _write(
            f"everypolitician/entity/{ep_slug(os_id)}",
            {
                "id": os_id,
                "schema": "Person",
                "caption": "Vladimir Putin",
                "properties": {
                    "name": ["Vladimir Putin"],
                    "wikidataId": [qid],
                    "position": ["President of Russia"],
                    "birthDate": ["1952-10-07"],
                },
            },
        )
    )

    return written


# ---------------------------------------------------------------------
# Rosneft — sanctioned + non-EU AMLA demo
# ---------------------------------------------------------------------


def seed_rosneft() -> list[Path]:
    """Rosneft Oil Company — sanctioned, RU-incorporated."""
    written: list[Path] = []

    lei = "253400JSI04G42PAAS27"
    qid = "Q1141123"
    os_id = "NK-rosneft"

    # GLEIF search + record (RU jurisdiction → AMLA NON_EU_JURISDICTION)
    written.append(
        _write(
            f"gleif/search/{gleif_slug('Rosneft')}",
            {
                "data": [
                    {
                        "id": lei,
                        "type": "lei-records",
                        "attributes": {
                            "lei": lei,
                            "entity": {
                                "legalName": {"name": "Rosneft Oil Company"},
                                "jurisdiction": "RU",
                            },
                            "registration": {"status": "ISSUED"},
                        },
                    }
                ]
            },
        )
    )
    written.append(
        _write(
            f"gleif/lei/{lei}",
            {
                "data": {
                    "id": lei,
                    "type": "lei-records",
                    "attributes": {
                        "lei": lei,
                        "entity": {
                            "legalName": {"name": "Rosneft Oil Company"},
                            "jurisdiction": "RU",
                        },
                        "registration": {"status": "ISSUED"},
                    },
                }
            },
        )
    )

    # OpenSanctions: sanctioned topic
    written.append(
        _write(
            f"opensanctions/search/LegalEntity/{os_slug('Rosneft')}",
            {
                "results": [
                    {
                        "id": os_id,
                        "schema": "Company",
                        "caption": "Rosneft Oil Company",
                        "properties": {
                            "name": ["Rosneft Oil Company"],
                            "leiCode": [lei],
                            "wikidataId": [qid],
                            "jurisdiction": ["ru"],
                        },
                        "topics": ["sanction"],
                        "datasets": ["us_ofac_sdn", "eu_fsf"],
                    }
                ]
            },
        )
    )
    written.append(
        _write(
            f"opensanctions/entity/{os_slug(os_id)}",
            {
                "id": os_id,
                "schema": "Company",
                "caption": "Rosneft Oil Company",
                "properties": {
                    "name": ["Rosneft Oil Company"],
                    "leiCode": [lei],
                    "wikidataId": [qid],
                    "jurisdiction": ["ru"],
                    "topics": ["sanction"],
                },
                "topics": ["sanction"],
                "datasets": ["us_ofac_sdn", "eu_fsf"],
            },
        )
    )

    # Wikidata search + fetch
    written.append(
        _write(
            f"wikidata/search/entity/{wd_slug('Rosneft')}",
            {
                "search": [
                    {
                        "id": qid,
                        "title": qid,
                        "label": "Rosneft",
                        "description": "Russian state-controlled oil company",
                        "match": {"type": "label", "language": "en", "text": "Rosneft"},
                    }
                ]
            },
        )
    )
    written.append(
        _write(
            f"wikidata/fetch/{qid}",
            {
                "head": {
                    "vars": [
                        "qid", "label", "description", "instance",
                        "instanceLabel", "country", "countryLabel",
                        "lei",
                    ]
                },
                "results": {
                    "bindings": [
                        {
                            "qid": {"type": "uri", "value": f"http://www.wikidata.org/entity/{qid}"},
                            "label": {"type": "literal", "value": "Rosneft"},
                            "description": {"type": "literal", "value": "Russian state-controlled oil company"},
                            "instance": {"type": "uri", "value": "http://www.wikidata.org/entity/Q4830453"},
                            "instanceLabel": {"type": "literal", "value": "business"},
                            "country": {"type": "uri", "value": "http://www.wikidata.org/entity/Q159"},
                            "countryLabel": {"type": "literal", "value": "Russia"},
                            "lei": {"type": "literal", "value": lei},
                        }
                    ]
                },
            },
        )
    )

    return written


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------


def main() -> None:
    DEMOS_ROOT.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    written.extend(seed_bp())
    written.extend(seed_putin())
    written.extend(seed_rosneft())

    print(f"Wrote {len(written)} demo fixtures under {DEMOS_ROOT}:")
    for path in written:
        rel = path.relative_to(DEMOS_ROOT)
        print(f"  - {rel}")


if __name__ == "__main__":
    main()
