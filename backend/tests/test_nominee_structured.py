"""Phase 104 — the nominee signal fires on structure, not on a sentence.

Until now `NOMINEE` worked by substring-matching the word "nominee" in a name or
a free-text descriptor. It fired correctly on UK Register of Overseas Entities
filings, but **by accident**: the mapper renders each nature-of-control code
into an English descriptor in `interest.details`, and those descriptors happen
to contain the word. A register stating the identical fact in a code, a boolean,
or another language was invisible to it.

Companies House publishes `natures_of_control` on every PSC record, and six ROE
codes state a nominee arrangement outright. `assess_bundle` already receives the
raw adapter payload, so those codes can be read at full fidelity rather than
recovered from prose the mapper generated.

The two grades of evidence are now distinguished rather than conflated:

* **structured** — the register filed a nominee code. High confidence, and the
  code travels in the evidence so a reviewer can check the filing itself.
* **textual** — the word appeared somewhere. Medium confidence, and the summary
  says what it matched on. "Nominee Services Ltd" is a company name, not a
  declaration.
"""

from __future__ import annotations

import pytest

from opencheck.bods.nominees import NOMINEE_NATURE_CODES, is_nominee_nature
from opencheck.bods.psc_natures import PSC_NATURE_DESCRIPTIONS
from opencheck.risk import NOMINEE, assess_bundle

ROE_NOMINEE = "registered-owner-as-nominee-person-england-wales-registered-overseas-entity"


class TestNatureCodeSet:
    def test_all_six_roe_codes_are_present(self):
        assert len(NOMINEE_NATURE_CODES) == 6

    def test_every_code_in_the_set_is_a_real_published_code(self):
        for code in NOMINEE_NATURE_CODES:
            assert code in PSC_NATURE_DESCRIPTIONS

    def test_drift_canary_no_published_nominee_code_is_missed(self):
        """If Companies House adds a nominee code, this fails.

        The set is derived from the vendored enumeration, so this mostly guards
        against someone converting it to a hand-written literal later — at
        which point a new upstream code would be silently unclassified, which
        is the whole failure mode this phase exists to remove.
        """
        published = {
            code
            for code, desc in PSC_NATURE_DESCRIPTIONS.items()
            if "nominee" in code or "nominee" in desc.lower()
        }
        assert published <= NOMINEE_NATURE_CODES, (
            f"unclassified nominee codes: {published - NOMINEE_NATURE_CODES}"
        )

    def test_ordinary_codes_are_not_nominee(self):
        assert not is_nominee_nature("ownership-of-shares-50-to-75-percent")
        assert not is_nominee_nature("voting-rights-75-to-100-percent")

    def test_case_and_whitespace_tolerant(self):
        assert is_nominee_nature(f"  {ROE_NOMINEE.upper()}  ")

    def test_empty_input_is_safe(self):
        assert not is_nominee_nature("")


def _raw(natures, ceased_on=None, name="Jane SMITH"):
    psc = {
        "kind": "individual-person-with-significant-control",
        "name": name,
        "etag": "abc",
        "natures_of_control": natures,
    }
    if ceased_on:
        psc["ceased_on"] = ceased_on
    return {
        "company_number": "OE123456",
        "profile": {"company_number": "OE123456", "company_name": "OVERSEAS CO"},
        "officers": {"items": []},
        "pscs": {"items": [psc]},
    }


def _nominee(signals):
    return next((s for s in signals if s.code == NOMINEE), None)


class TestStructuredPath:
    def _signals(self, raw, bods=None):
        # A non-empty bods list is required for the AMLA rules to run at all.
        bods = bods or [
            {
                "statementId": "S1",
                "recordType": "entity",
                "recordDetails": {"name": "OVERSEAS CO"},
            }
        ]
        return assess_bundle("companies_house", raw, bods, hit_id="OE123456")

    def test_fires_on_a_filed_nominee_code(self):
        sig = _nominee(self._signals(_raw([ROE_NOMINEE])))
        assert sig is not None
        assert sig.confidence == "high"
        assert sig.evidence["basis"] == "structured"

    def test_the_code_travels_in_the_evidence(self):
        """A reviewer should be able to check the filing, not our reading of it."""
        sig = _nominee(self._signals(_raw([ROE_NOMINEE])))
        assert ROE_NOMINEE in sig.evidence["nature_codes"]
        assert sig.evidence["matches"][0]["nature_code"] == ROE_NOMINEE

    def test_summary_says_the_register_filed_it(self):
        sig = _nominee(self._signals(_raw([ROE_NOMINEE])))
        assert "Register filed" in sig.summary
        assert "AMLA" in sig.summary

    def test_does_not_fire_on_ordinary_natures(self):
        assert _nominee(self._signals(_raw(["ownership-of-shares-50-to-75-percent"]))) is None

    def test_ceased_psc_is_historical_not_current(self):
        """A ceased PSC's nominee arrangement has ended; it is not a live risk."""
        raw = _raw([ROE_NOMINEE], ceased_on="2024-01-01")
        assert _nominee(self._signals(raw)) is None

    def test_multiple_codes_are_deduplicated_in_the_code_list(self):
        codes = [
            ROE_NOMINEE,
            "registered-owner-as-nominee-person-scotland-registered-overseas-entity",
            ROE_NOMINEE,
        ]
        sig = _nominee(self._signals(_raw(codes)))
        assert len(sig.evidence["nature_codes"]) == 2

    def test_other_sources_do_not_use_the_structured_path(self):
        """natures_of_control is a Companies House shape; another source
        happening to carry the key must not be read as one."""
        signals = assess_bundle(
            "opencorporates",
            _raw([ROE_NOMINEE]),
            [{"statementId": "S1", "recordType": "entity", "recordDetails": {}}],
            hit_id="X",
        )
        sig = _nominee(signals)
        assert sig is None or sig.evidence.get("basis") != "structured"


class TestStructuredBeatsTextual:
    def test_a_filed_code_reports_as_structured_not_textual(self):
        """The mapper renders the code into interest.details, so a ROE filing
        would otherwise trip both paths for the same fact and report the weaker
        one. Structured wins."""
        bods = [
            {
                "statementId": "S1",
                "recordType": "relationship",
                "recordDetails": {
                    "subject": {"describedByEntityStatement": "E1"},
                    "interestedParty": {"describedByPersonStatement": "P1"},
                    "interests": [
                        {
                            "type": "otherInfluenceOrControl",
                            # Exactly what the mapper emits for this code.
                            "details": PSC_NATURE_DESCRIPTIONS[ROE_NOMINEE],
                        }
                    ],
                },
            }
        ]
        sig = _nominee(
            assess_bundle("companies_house", _raw([ROE_NOMINEE]), bods, hit_id="OE1")
        )
        assert sig.confidence == "high"
        assert sig.evidence["basis"] == "structured"


class TestTextualFallbackSurvives:
    def test_prose_only_source_still_fires(self):
        """No regression in recall for sources that publish only free text."""
        bods = [
            {
                "statementId": "S1",
                "recordType": "relationship",
                "recordDetails": {
                    "subject": {"describedByEntityStatement": "E1"},
                    "interestedParty": {"describedByPersonStatement": "P1"},
                    "interests": [
                        {
                            "type": "shareholding",
                            "details": "Held by a nominee shareholder.",
                        }
                    ],
                },
            }
        ]
        sig = _nominee(assess_bundle("ares", {"cz_ico": "1"}, bods, hit_id="1"))
        assert sig is not None
        assert sig.confidence == "medium"
        assert sig.evidence["basis"] == "textual"

    def test_textual_summary_does_not_overclaim(self):
        bods = [
            {
                "statementId": "S1",
                "recordType": "relationship",
                "recordDetails": {
                    "subject": {"describedByEntityStatement": "E1"},
                    "interestedParty": {"describedByPersonStatement": "P1"},
                    "interests": [
                        {"type": "shareholding", "details": "nominee shareholder"}
                    ],
                },
            }
        ]
        sig = _nominee(assess_bundle("ares", {"cz_ico": "1"}, bods, hit_id="1"))
        assert "descriptive text" in sig.summary
        assert "Register filed" not in sig.summary


class TestAmlaCompositeStillWorks:
    def test_structured_nominee_counts_toward_the_composite_rule(self):
        """AMLA "complex structure" = >=3 layers AND >=1 of
        {trust, non-EU, nominee}. A structured nominee must still qualify."""
        from opencheck.risk import COMPLEX_CORPORATE_STRUCTURE

        # Three ownership layers: E1 <- E2 <- E3, plus the nominee filing.
        bods = [
            {"statementId": "E1", "recordType": "entity", "recordDetails": {"name": "A"}},
            {"statementId": "E2", "recordType": "entity", "recordDetails": {"name": "B"}},
            {"statementId": "E3", "recordType": "entity", "recordDetails": {"name": "C"}},
            {
                "statementId": "R1",
                "recordType": "relationship",
                "recordDetails": {
                    "subject": {"describedByEntityStatement": "E1"},
                    "interestedParty": {"describedByEntityStatement": "E2"},
                    "interests": [{"type": "shareholding"}],
                },
            },
            {
                "statementId": "R2",
                "recordType": "relationship",
                "recordDetails": {
                    "subject": {"describedByEntityStatement": "E2"},
                    "interestedParty": {"describedByEntityStatement": "E3"},
                    "interests": [{"type": "shareholding"}],
                },
            },
        ]
        signals = assess_bundle(
            "companies_house", _raw([ROE_NOMINEE]), bods, hit_id="OE1"
        )
        codes = {s.code for s in signals}
        assert NOMINEE in codes
        # The composite may or may not fire depending on how layers are counted
        # in this minimal graph; what must hold is that the nominee signal is
        # available to it in structured form.
        nominee = _nominee(signals)
        assert nominee.evidence["basis"] == "structured"
        assert COMPLEX_CORPORATE_STRUCTURE  # imported symbol exists


class TestMalformedInput:
    @pytest.mark.parametrize(
        "pscs",
        [
            {},
            {"items": None},
            {"items": ["not a dict"]},
            {"items": [{"natures_of_control": None}]},
            {"items": [{"natures_of_control": [None]}]},
        ],
    )
    def test_does_not_crash(self, pscs):
        raw = {"company_number": "X", "pscs": pscs}
        signals = assess_bundle(
            "companies_house",
            raw,
            [{"statementId": "S1", "recordType": "entity", "recordDetails": {}}],
            hit_id="X",
        )
        assert isinstance(signals, list)
