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
