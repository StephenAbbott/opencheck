"""The Israel Corporations Authority's change log — what each request code is.

The ``ica-changes`` file on data.gov.il (CC BY) lists every request the
Authority has registered for a company or partnership in the past year: the
corporation's number, the request type (a numeric code and the register's
Hebrew words) and the day its status was updated. It says **that** something
was recorded, never **what** it changed to — no person, no share count, no old
or new name, no new status.

This module is the one place a request code is read: its English label, the
group it belongs to, and whether a watchlist re-runs on it. The index builder
(``scripts/build_ica_israel_index.py``), the History emitter
(``timeline/ica_israel.py``), the watchlist diff
(``scripts/diff_ica_israel_index.py``) and the watchlist tick all import it,
so the four cannot disagree.

The 97 codes are the ones the file carried on 11 Nov 2026 (counts per year on
the Notion ticket). An unlisted code reads as the register's Hebrew words
alone, in the ``other`` group — never material, never notable.

Stephen's decisions (11 Nov 2026): the material set below; charges are not
material; share filings rank at the ownership tier on History and are worded
"recorded, parties not published" (12 Nov 2026: the day is the
request's latest status update, not its filing day); nothing here reaches QuickCheck.
"""

from __future__ import annotations

#: Request code → English label. The register's Hebrew words always travel
#: beside it (``label_for``).
REQUEST_LABELS: dict[int, str] = {
    # Shares
    3: "Share transfer",
    2: "Share allotment",
    22030: "Annual report: share allotment",
    22124: "Annual report: shareholders updated",
    153124: "Party details: shareholders updated",
    # Capital
    21: "Registered capital increased",
    22157: "Annual report: capital increased",
    42160: "Nominal value changed",
    42161: "Share split",
    42165: "Allotted capital reduced",
    42158: "Registered capital reduced",
    42159: "Share class changed",
    42162: "Capital type changed",
    22165: "Annual report: allotted capital reduced",
    22158: "Annual report: registered capital reduced",
    42: "Capital structure changed",
    # Directors
    4031: "Director removed",
    4032: "Director added",
    22031: "Annual report: director removed",
    22032: "Annual report: director added",
    # Officers and managers
    25: "Officers updated",
    22163: "Annual report: officer added",
    22164: "Annual report: officer removed",
    22168: "Annual report: officer updated",
    22146: "Annual report: managers updated",
    4146: "Directors update: managers updated",
    153146: "Party details: managers updated",
    153168: "Party details: officer updated",
    # Name
    24: "Change of name",
    83: "Change of name (partnership)",
    218: "Change of name (public-benefit company)",
    40166: "Shareholders' liability changed: name",
    # Status: winding-up, liquidation, revival
    146: "Fast-track voluntary winding-up",
    152: "Automatic fast-track voluntary liquidation",
    144: "Voluntary winding-up",
    145: "Company liquidated",
    49: "Company liquidated",
    151: "Liquidation by the court (insolvency)",
    150: "Winding-up by the court (insolvency)",
    51: "Liquidation by the court",
    50: "Winding-up by the court",
    147: "Objection to a fast-track winding-up",
    148: "Order to open proceedings",
    149: "Corporation in rehabilitation",
    61: "Stay-of-proceedings order added or updated",
    62: "Compromise or arrangement added or updated",
    63: "Stay of proceedings, receivership or temporary winding-up cancelled",
    52: "Winding-up cancelled",
    45: "Company revived",
    286: "Revival requested within a year of strike-off",
    46: "Legal status changed from temporarily active",
    48: "Company winding-up",
    54: "Summary winding-up",
    56: "Foreign company removed",
    58: "Approval of the company cancelled",
    85: "Voluntary winding-up of a partnership",
    86: "Winding-up of a partnership by the court",
    96: "Liquidation of a partnership by the court",
    133: "Winding-up of a partnership cancelled",
    90: "Partnership revived",
    # Merger
    55: "Voluntary merger (absorbing company)",
    59: "Voluntary merger (target company)",
    57: "Merger by the court (absorbing company)",
    60: "Merger by the court (target company)",
    73: "Merger error corrected",
    # Charges
    68: "Charge registered",
    70: "Charge removed",
    69: "Charge updated",
    72: "Charge removal cancelled",
    71: "Charge moved from one company to another",
    # Attachment
    74: "Attachment order added, updated or removed",
    # Classification and form
    39: "Company reclassified",
    37: "Reporting corporation",
    38: "Non-reporting corporation",
    40153: "Shareholders' liability changed to limited",
    40154: "Shareholders' liability changed to unlimited",
    41: "Converted to a public-benefit company",
    219: "Public-benefit company's purpose changed",
    # Corrections and court
    153206: "Party details: wrong ID number for a director corrected",
    153207: "Party details: wrong ID number for a shareholder corrected",
    153208: "Party details: wrong ID number for an officer corrected",
    64: "Company registration corrected",
    77: "Foreign company registration corrected",
    78: "Partnership registration corrected",
    120: "Legal, general: court response or note on the extract",
    47: "Solvency declaration",
    84: "Partnership changes",
    # Articles
    43: "Articles changed",
    22171: "Annual report: articles provision changed",
    # Routine
    22: "Annual report",
    1000: "Violation update",
    1001: "Fee charges update",
    275: "Address or digital address updated",
    23: "Change of address",
    22441: "Annual report: digital address updated",
    22156: "Annual report: address updated",
    22302: "Annual report: line of business updated",
}

#: Group → the request codes in it. Every code in ``REQUEST_LABELS`` is in
#: exactly one group (pinned by a test).
GROUPS: dict[str, frozenset[int]] = {
    "shares": frozenset({3, 2, 22030, 22124, 153124}),
    "capital": frozenset({21, 22157, 42160, 42161, 42165, 42158, 42159, 42162, 22165, 22158, 42}),
    "directors": frozenset({4031, 4032, 22031, 22032}),
    "officers": frozenset({25, 22163, 22164, 22168, 22146, 4146, 153146, 153168}),
    "name": frozenset({24, 83, 218, 40166}),
    "status": frozenset({
        146, 152, 144, 145, 49, 151, 150, 51, 50, 147, 148, 149, 61, 62, 63, 52,
        45, 286, 46, 48, 54, 56, 58, 85, 86, 96, 133, 90,
    }),
    "merger": frozenset({55, 59, 57, 60, 73}),
    "charges": frozenset({68, 70, 69, 72, 71}),
    "attachment": frozenset({74}),
    "classification": frozenset({39, 37, 38, 40153, 40154, 41, 219}),
    "corrections": frozenset({153206, 153207, 153208, 64, 77, 78, 120, 47, 84}),
    "articles": frozenset({43, 22171}),
    "routine": frozenset({22, 1000, 1001, 275, 23, 22441, 22156, 22302}),
}

#: The groups a watchlist re-runs on (Stephen, 11 Nov 2026: the recommended
#: set; charges are not material).
MATERIAL_GROUPS: tuple[str, ...] = (
    "shares", "capital", "directors", "name", "status", "merger",
    "attachment", "classification",
)

MATERIAL_CODES: frozenset[int] = frozenset().union(*(GROUPS[g] for g in MATERIAL_GROUPS))

#: Director codes that add a director (the rest of the group remove one).
DIRECTOR_ADDED: frozenset[int] = frozenset({4032, 22032})

_GROUP_OF: dict[int, str] = {code: group for group, codes in GROUPS.items() for code in codes}


def group_of(code: int | None) -> str:
    """The code's group, ``other`` for a code the table does not know."""
    if code is None:
        return "other"
    return _GROUP_OF.get(int(code), "other")


def is_material(code: int | None) -> bool:
    return code is not None and int(code) in MATERIAL_CODES


def label_for(code: int | None, hebrew: str | None) -> str:
    """``Share transfer (העברת מניות)``; the Hebrew alone for an unlisted
    code; the English alone where the file gave no words."""
    english = REQUEST_LABELS.get(int(code)) if code is not None else None
    hebrew = (hebrew or "").strip() or None
    if english and hebrew:
        return f"{english} ({hebrew})"
    return english or hebrew or f"Request type {code}"


__all__ = [
    "DIRECTOR_ADDED",
    "GROUPS",
    "MATERIAL_CODES",
    "MATERIAL_GROUPS",
    "REQUEST_LABELS",
    "group_of",
    "is_material",
    "label_for",
]
