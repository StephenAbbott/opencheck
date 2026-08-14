"""Phase 101 — register-supplied statementDate, second wave.

Phase 100 wired GLEIF, Companies House PSC and SEC EDGAR. This extends the same
rule to every remaining source that publishes a genuine record-level
declaration date.

The rule, restated because it is easy to get wrong: a valid `statementDate` is
when the register *asserted or last revised* the record. It is not when the
underlying fact began. Founding dates, incorporation dates, appointment dates,
role start dates, status-effective dates and reporting periods are all the
first clock — "when was it true" — and belong on `interests[].startDate` or
`foundingDate`, never here.

Three of the four sources originally proposed for this phase turned out to have
no usable field, which is recorded here so nobody re-litigates it:

* **Estonia (ariregister)** — the scraped printable page exposes `Registered`,
  a founding date. No last-changed label found.
* **Denmark (cvr_denmark)** — Datafordeler CVR is bitemporal, but the adapter's
  six GraphQL queries request only `virkningFra`/`virkningTil`, which are
  *validity* time ("when it was true"), not transaction time. The
  `registreringFra`-style twin is not requested, so there is nothing to wire
  until the queries change.
* **Brazil (cnpj_brazil)** — probed live 2026-08-14 against both OpenCNPJ and
  BrasilAPI. Neither returns any update stamp; every `data_*` field is an event
  or founding date. `data_situacao_cadastral` is the date the *cadastral status*
  took effect, which is a trap, not a declaration date.
"""

from __future__ import annotations

from datetime import date

from opencheck.bods.mapper import (
    map_ares,
    map_brreg,
    map_krs_poland,
    map_ted_eu,
    map_ur_latvia,
)

TODAY = date.today().isoformat()


def _by_type(bundle, record_type):
    return [s for s in bundle if s["recordType"] == record_type]


# ---------------------------------------------------------------------------
# Poland — KRS dataOstatniegoWpisu ("date of the last entry")
# ---------------------------------------------------------------------------


class TestKrsPoland:
    def _bundle(self, last_change="2023-09-14"):
        return {
            "pl_krs": "0000037568",
            "name": "EXAMPLE SP. Z O.O.",
            "registration_date": "1998-02-11",
            "last_change_date": last_change,
            "directors": [],
            "shareholders": [],
        }

    def test_statement_date_is_the_last_court_entry(self):
        stmt = _by_type(list(map_krs_poland(self._bundle())), "entity")[0]
        assert stmt["statementDate"] == "2023-09-14"

    def test_founding_date_is_not_used_as_the_declaration_date(self):
        stmt = _by_type(list(map_krs_poland(self._bundle())), "entity")[0]
        assert stmt["statementDate"] != "1998-02-11"
        assert stmt["recordDetails"].get("foundingDate") == "1998-02-11"

    def test_missing_last_entry_falls_back(self):
        stmt = _by_type(list(map_krs_poland(self._bundle(None))), "entity")[0]
        assert stmt["statementDate"] == TODAY


# ---------------------------------------------------------------------------
# Latvia — UR officer last_modified_at
# ---------------------------------------------------------------------------


class TestUrLatvia:
    def _bundle(self, last_modified="2024-05-20", registered="2015-01-02"):
        return {
            "lv_regcode": "40003032949",
            "entity": {"name": "EXAMPLE SIA", "type": "SIA"},
            "officers": [
                {
                    "id": "1",
                    "name": "Jānis Bērziņš",
                    "entity_type": "NATURAL_PERSON",
                    "governing_body": "BOARD",
                    "position": "Member",
                    "registered_on": registered,
                    "last_modified_at": last_modified,
                }
            ],
        }

    def test_officer_statement_date_is_the_revision_date(self):
        out = list(map_ur_latvia(self._bundle()))
        people = _by_type(out, "person")
        assert people and people[0]["statementDate"] == "2024-05-20"

    def test_relationship_carries_it_too(self):
        rels = _by_type(list(map_ur_latvia(self._bundle())), "relationship")
        assert rels and rels[0]["statementDate"] == "2024-05-20"

    def test_last_modified_no_longer_leaks_into_start_date(self):
        """Regression: last_modified_at was a fallback for interest.startDate.

        That is the Phase 100 error inverted — a declaration date standing in
        for when the role began. With no registered_on the interest should have
        no startDate at all rather than borrowing the revision date.
        """
        rels = _by_type(
            list(map_ur_latvia(self._bundle(registered=None))), "relationship"
        )
        assert rels
        interest = rels[0]["recordDetails"]["interests"][0]
        assert "startDate" not in interest
        # ...but the revision date is not lost; it is the declaration date.
        assert rels[0]["statementDate"] == "2024-05-20"

    def test_registered_on_still_drives_start_date(self):
        rels = _by_type(list(map_ur_latvia(self._bundle())), "relationship")
        assert rels[0]["recordDetails"]["interests"][0]["startDate"] == "2015-01-02"

    def test_falls_back_to_registered_on_when_never_revised(self):
        out = list(map_ur_latvia(self._bundle(last_modified=None)))
        assert _by_type(out, "relationship")[0]["statementDate"] == "2015-01-02"


# ---------------------------------------------------------------------------
# Czechia — ARES datumAktualizace (live-verified 2026-08-14)
# ---------------------------------------------------------------------------


class TestAres:
    def _bundle(self, last_updated="2026-07-01"):
        return {
            "cz_ico": "27082440",
            "entity": {
                "name": "EXAMPLE A.S.",
                "incorporation_date": "2003-08-26",
                "last_updated": last_updated,
            },
            "owners": [],
            "directors": [],
        }

    def test_statement_date_is_the_record_refresh(self):
        stmt = _by_type(list(map_ares(self._bundle())), "entity")[0]
        assert stmt["statementDate"] == "2026-07-01"

    def test_datum_vzniku_is_the_founding_date_not_the_declaration(self):
        """datumVzniku reads like an entry date but is the company's creation."""
        stmt = _by_type(list(map_ares(self._bundle())), "entity")[0]
        assert stmt["recordDetails"].get("foundingDate") == "2003-08-26"
        assert stmt["statementDate"] != "2003-08-26"

    def test_missing_refresh_date_falls_back(self):
        stmt = _by_type(list(map_ares(self._bundle(None))), "entity")[0]
        assert stmt["statementDate"] == TODAY


# ---------------------------------------------------------------------------
# Norway — brreg rollegrupper[].sistEndret (live-verified 2026-08-14)
# ---------------------------------------------------------------------------


class TestBrreg:
    def _bundle(self, sist_endret="2020-11-02"):
        return {
            "orgnr": "923609016",
            "legal_name": "EXAMPLE AS",
            "entity": {"navn": "EXAMPLE AS", "organisasjonsnummer": "923609016"},
            "roles": [
                {
                    "type": {"kode": "DAGL", "beskrivelse": "Daglig leder"},
                    "_group_type": {"kode": "DAGL"},
                    "_group_last_changed": sist_endret,
                    "person": {
                        "navn": {"fornavn": "Kari", "etternavn": "Nordmann"}
                    },
                }
            ],
        }

    def test_person_statement_date_is_the_group_revision(self):
        people = _by_type(list(map_brreg(self._bundle())), "person")
        assert people and people[0]["statementDate"] == "2020-11-02"

    def test_relationship_carries_it_too(self):
        rels = _by_type(list(map_brreg(self._bundle())), "relationship")
        assert rels and rels[0]["statementDate"] == "2020-11-02"

    def test_missing_sist_endret_falls_back(self):
        rels = _by_type(list(map_brreg(self._bundle(None))), "relationship")
        assert rels and rels[0]["statementDate"] == TODAY


# ---------------------------------------------------------------------------
# TED — notice publication-date
# ---------------------------------------------------------------------------


class TestTedEu:
    def _bundle(self):
        return {
            "lei": "213800LH1BZH3DI6G760",
            "legal_name": "EXAMPLE SA",
            "total_notice_count": 2,
            "confirmed_wins": 1,
            "notices": [
                {"url": "https://ted.europa.eu/n/1", "publication_date": "2025-03-01"},
                {"url": "https://ted.europa.eu/n/2", "publication_date": "2026-01-15"},
            ],
        }

    def test_statement_date_is_the_latest_notice_publication(self):
        stmt = _by_type(list(map_ted_eu(self._bundle())), "entity")[0]
        assert stmt["statementDate"] == "2026-01-15"

    def test_no_notices_falls_back(self):
        bundle = self._bundle()
        bundle["notices"] = []
        stmt = _by_type(list(map_ted_eu(bundle)), "entity")[0]
        assert stmt["statementDate"] == TODAY


# ---------------------------------------------------------------------------
# Cross-source canary
# ---------------------------------------------------------------------------


def test_no_source_writes_its_register_date_into_publication_date():
    """publicationDetails describes OpenCheck's publication, always.

    The Phase 100 regression, restated for every source wired in this phase.
    """
    cases = [
        (map_krs_poland, TestKrsPoland()._bundle(), "2023-09-14"),
        (map_ur_latvia, TestUrLatvia()._bundle(), "2024-05-20"),
        (map_ares, TestAres()._bundle(), "2026-07-01"),
        (map_brreg, TestBrreg()._bundle(), "2020-11-02"),
        (map_ted_eu, TestTedEu()._bundle(), "2026-01-15"),
    ]
    for mapper, bundle, register_date in cases:
        statements = list(mapper(bundle))
        assert statements, f"{mapper.__name__} produced nothing"
        for stmt in statements:
            pub = stmt["publicationDetails"]
            assert pub["publicationDate"] != register_date, (
                f"{mapper.__name__} leaked the register date into publicationDate"
            )
            assert pub["publisher"]["name"] == "OpenCheck"


def test_each_source_actually_reaches_statement_date():
    """The other half: the date must land, not merely be absent elsewhere."""
    cases = [
        (map_krs_poland, TestKrsPoland()._bundle(), "2023-09-14"),
        (map_ur_latvia, TestUrLatvia()._bundle(), "2024-05-20"),
        (map_ares, TestAres()._bundle(), "2026-07-01"),
        (map_brreg, TestBrreg()._bundle(), "2020-11-02"),
        (map_ted_eu, TestTedEu()._bundle(), "2026-01-15"),
    ]
    for mapper, bundle, register_date in cases:
        dates = {s["statementDate"] for s in mapper(bundle)}
        assert register_date in dates, f"{mapper.__name__} did not use {register_date}"
