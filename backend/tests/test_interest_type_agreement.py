"""The same role must carry the same interest type whichever source said it.

Phase 196. OpenCheck keeps one position→interestType table per source and
nothing compared them, so OpenCorporates published every director as
``appointmentOfBoard`` — the **power to appoint or remove directors** — while
Companies House published the same person from the same register as
``seniorManagingOfficial``. It survived for as long as both tables have, and
was found by accident when OpenCorporates officers started arriving at all
(Phase 195). This is the test that would have caught it.
"""

from __future__ import annotations

import pytest

from opencheck.bods.mapper import _MANAGING_OFFICIAL_ROLES
from opencheck.bods.mappers.wikidata import _oc_match_position

#: Companies House writes an officer role as a hyphenated slug; OpenCorporates
#: writes a free-text position. These are the same job under both spellings —
#: only the roles Companies House treats as a managing official, since that is
#: the claim the two tables have to agree about.
_SAME_ROLE: dict[str, str] = {
    "director": "director",
    "corporate-director": "director",
    "nominee-director": "nominee director",
    "llp-member": "member",
    "llp-designated-member": "managing member",
    "general-partner-in-a-limited-partnership": "general partner",
    "managing-officer": "manager",
}


@pytest.mark.parametrize("ch_role,oc_position", sorted(_SAME_ROLE.items()))
def test_interest_types_agree_across_sources(ch_role: str, oc_position: str) -> None:
    assert ch_role in _MANAGING_OFFICIAL_ROLES, (
        f"{ch_role} is no longer a Companies House managing official — "
        "this table needs updating, not the assertion relaxing"
    )
    oc_type = _oc_match_position(oc_position)
    # A nominee is the one legitimate divergence: OpenCorporates publishes the
    # nominee arrangement itself, which BODS has a code for and the Companies
    # House officer role does not distinguish.
    expected = "nominee" if "nominee" in oc_position else "seniorManagingOfficial"
    assert oc_type == expected, (
        f"Companies House calls {ch_role!r} a seniorManagingOfficial; "
        f"OpenCorporates calls {oc_position!r} a {oc_type!r}"
    )


def test_appointment_of_board_is_not_a_job_title() -> None:
    """``appointmentOfBoard`` is the power to appoint or remove directors — a
    UK PSC statutory condition, which is why ``statements.py`` maps the PSC
    nature onto it. No officer position may claim it: being on the board is
    not the same as choosing who is."""
    from opencheck.bods.mappers.wikidata import _OC_POSITION_TO_INTEREST_TYPE

    offenders = [
        position
        for position, itype in _OC_POSITION_TO_INTEREST_TYPE.items()
        if itype == "appointmentOfBoard"
    ]
    assert offenders == [], offenders


def test_the_psc_nature_that_does_mean_it_still_maps_there() -> None:
    """The code is not wrong, only its use was. The Companies House nature
    that genuinely asserts the power keeps it."""
    from opencheck.bods.statements import _INTEREST_PREFIX

    assert _INTEREST_PREFIX["right-to-appoint-and-remove-directors"] == (
        "appointmentOfBoard"
    )


# ---------------------------------------------------------------------------
# The two remaining over-claims, closed in Phase 197
#
# Companies House keeps both of these out of `_MANAGING_OFFICIAL_ROLES` and
# emits no statement for either. OpenCorporates typed both
# `seniorManagingOfficial` — the code a regime reaches for when a company has
# no identifiable beneficial owner and its senior managers are named instead.
# ---------------------------------------------------------------------------


def test_a_secretary_is_not_a_senior_managing_official() -> None:
    """An administrative officer. Kept visible — OpenCorporates is sometimes
    the only source that names one — without the claim that they run it."""
    assert _oc_match_position("company secretary") == "otherInfluenceOrControl"
    assert "secretary" not in _MANAGING_OFFICIAL_ROLES


def test_a_limited_partner_holds_profit_rights_not_management() -> None:
    """The one partner who by legal definition takes no part in management —
    taking part is what costs them their limited liability. What they hold is
    a capital contribution entitling them to a share of profits."""
    assert _oc_match_position("limited partner") == "rightsToProfitOrIncome"
    assert "limited-partner-in-a-limited-partnership" not in _MANAGING_OFFICIAL_ROLES
    # The partner who does manage is unaffected.
    assert _oc_match_position("general partner") == "seniorManagingOfficial"


def test_the_longest_position_match_wins_not_the_first_listed() -> None:
    """"partner" precedes "limited partner" in the table, so insertion order
    would read "Limited Partner (Class A)" — a string the exact match cannot
    catch — as the partner who runs the firm rather than the one who by
    definition does not. Harmless while a family shared one type; a defect the
    moment it stopped."""
    assert _oc_match_position("Limited Partner (Class A)") == "rightsToProfitOrIncome"
    assert _oc_match_position("Joint Company Secretary") == "otherInfluenceOrControl"


def test_every_mapped_position_is_a_real_bods_interest_type() -> None:
    """A codelist value is not a preference: a wrong one is a conformance
    failure that the schema alone will not catch, since it only checks the
    value is IN the list."""
    from opencheck.bods.mappers.wikidata import _OC_POSITION_TO_INTEREST_TYPE
    from opencheck.bods.validator import _VALID_INTEREST_TYPES

    unknown = {
        itype
        for itype in _OC_POSITION_TO_INTEREST_TYPE.values()
        if itype not in _VALID_INTEREST_TYPES
    }
    assert unknown == set()
