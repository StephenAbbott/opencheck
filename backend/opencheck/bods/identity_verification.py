"""Companies House identity verification → a BODS annotation (Phase 203).

Companies House is verifying the identity of every director and every
individual person with significant control; the transition runs to November
2026. The public data API carries the outcome on each officer and PSC list item
as ``identity_verification_details``. Measured on the live register
(Shell PLC, 04366849, 10 Sept 2026), the block arrives in three shapes, not
two:

* **not yet verified** — only ``appointment_verification_statement_due_on``
  (PSCs also carry ``appointment_verification_statement_date``, the day their
  14-day window opens);
* **verified through an authorised corporate service provider (ACSP)** —
  ``identity_verified_on``, ``authorised_corporate_service_provider_name``,
  ``anti_money_laundering_supervisory_bodies``, ``preferred_name``, and the
  statement dates ``appointment_verification_start_on`` / ``_end_on``;
* **verified directly with Companies House** (GOV.UK One Login) — **only** the
  statement dates. The register publishes when the verification statement was
  supplied for the role, and neither the date of verification nor a verifier.

What counts as verified
-----------------------
Verifying an identity and linking it to a role are two steps (the personal
code), and Companies House's own service shows its "Verified" label *against
each role*. So the rule mirrors the register, per appointment:

    verified  ⇔  appointment_verification_start_on is present
                 and appointment_verification_end_on is absent or later than today

``9999-12-31`` is the register's "still in place". A ``due_on`` date alone is
not a verification, and neither is ``identity_verified_on`` without a statement
— the register would not show Verified against that role.

What is deliberately not published
----------------------------------
* **Nothing for an unverified person.** Absence is not asserted: until November
  a missing statement is the ordinary state of a compliant director, and people
  reached through other sources carry no Companies House data at all.
* **``preferred_name``.** A second name for the person adds nothing to the
  verification claim, and an annotation should not be the place a name first
  appears in the output.
* **A verification date for the direct route.** It is not published. The first
  statement date is carried instead, named as what it is.

The "by whom" for the direct route is the one inference here: the scheme has
exactly two routes, so a verification with no ACSP named was made with
Companies House. The sentence says so in those terms.

The BODS construct is a ``commenting`` annotation with a custom
``identityVerification`` object — the Annotation schema says "custom properties
can be included within the Annotation object to provide structured data where
required". ``source.type: "verified"`` was considered and rejected (Stephen,
10 Sept 2026): it marks the whole statement as verified, when the register
verified the identity, not the service address or the nationality.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from .annotations import commenting

#: The custom property name on the annotation. Read by the frontend
#: (``frontend/src/lib/identityVerification.ts``) — change both together.
ANNOTATION_KEY = "identityVerification"

#: ``route`` values. Closed vocabulary; the frontend accepts only these.
ROUTE_ACSP = "authorisedCorporateServiceProvider"
ROUTE_COMPANIES_HOUSE = "companiesHouse"

_RECORDED_BY = {
    "name": "Companies House",
    "uri": "https://www.gov.uk/government/organisations/companies-house",
}

_MONTHS = (
    "January", "February", "March", "April", "May", "June", "July",
    "August", "September", "October", "November", "December",
)


@dataclass(frozen=True)
class Verification:
    """One role's verification, as the register states it."""

    statement_on: str
    route: str
    identity_verified_on: str | None = None
    verifier: str | None = None
    supervisors: tuple[str, ...] = ()

    @property
    def richness(self) -> int:
        """How much the register said: an ACSP record names who and when."""
        return (self.route == ROUTE_ACSP) + bool(self.identity_verified_on)


def _iso(value: Any) -> str | None:
    """A ``YYYY-MM-DD`` string, or None. Anything else is not a date we repeat."""
    if not isinstance(value, str):
        return None
    text = value.strip()
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


def read_verification(
    item: dict[str, Any] | None, *, today: date | None = None
) -> Verification | None:
    """The verification a Companies House officer or PSC item records, if any.

    Returns ``None`` for every shape that is not a verification in place: no
    block, a due date only, a statement that has been removed, or a malformed
    date.
    """
    details = (item or {}).get("identity_verification_details")
    if not isinstance(details, dict):
        return None
    start = _iso(details.get("appointment_verification_start_on"))
    if not start:
        return None
    end_raw = details.get("appointment_verification_end_on")
    if end_raw not in (None, ""):
        end = _iso(end_raw)
        if end is None:
            # A value we cannot read is not evidence the statement is in place.
            return None
        if date.fromisoformat(end) <= (today or date.today()):
            return None

    verifier = details.get("authorised_corporate_service_provider_name")
    verifier = verifier.strip() if isinstance(verifier, str) else ""
    supervisors = tuple(
        s.strip()
        for s in details.get("anti_money_laundering_supervisory_bodies") or []
        if isinstance(s, str) and s.strip()
    )
    if verifier:
        return Verification(
            statement_on=start,
            route=ROUTE_ACSP,
            identity_verified_on=_iso(details.get("identity_verified_on")),
            verifier=verifier,
            supervisors=supervisors,
        )
    return Verification(statement_on=start, route=ROUTE_COMPANIES_HOUSE)


def _human_date(iso: str) -> str:
    d = date.fromisoformat(iso)
    return f"{d.day} {_MONTHS[d.month - 1]} {d.year}"


def _and_join(items: tuple[str, ...]) -> str:
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _who_clause(v: Verification) -> str:
    """The "by whom" sentence, in the register's terms."""
    if v.route == ROUTE_ACSP:
        when = (
            f" on {_human_date(v.identity_verified_on)}"
            if v.identity_verified_on
            else ""
        )
        supervised = (
            f", an authorised corporate service provider supervised for "
            f"anti-money laundering by {_and_join(v.supervisors)}"
            if v.supervisors
            else ", an authorised corporate service provider"
        )
        return f"The identity was verified{when} by {v.verifier}{supervised}."
    return (
        "The register names no authorised corporate service provider, so the "
        "identity was verified with Companies House directly; Companies House "
        "does not publish the date of verification for that route."
    )


def _structure(v: Verification) -> dict[str, Any]:
    out: dict[str, Any] = {
        "status": "verified",
        "recordedBy": dict(_RECORDED_BY),
        "route": v.route,
    }
    if v.verifier:
        verifier: dict[str, Any] = {"name": v.verifier}
        if v.supervisors:
            verifier["antiMoneyLaunderingSupervisoryBodies"] = list(v.supervisors)
        out["verifiedBy"] = verifier
    if v.identity_verified_on:
        out["identityVerifiedOn"] = v.identity_verified_on
    return out


def role_annotation(
    target: str, v: Verification, *, url: str | None = None
) -> dict[str, Any]:
    """The annotation for one role: pinned at the relationship's interested party."""
    annotation = commenting(
        target,
        (
            "Companies House records an identity verification statement for "
            f"this role, supplied on {_human_date(v.statement_on)}. "
            + _who_clause(v)
        ),
    )
    structure = _structure(v)
    structure["verificationStatementSuppliedOn"] = v.statement_on
    annotation[ANNOTATION_KEY] = structure
    if url:
        annotation["url"] = url
    return annotation


def person_annotation(
    target: str, roles: list[Verification], *, url: str | None = None
) -> dict[str, Any] | None:
    """The annotation for a person, derived from every verified role in the output.

    One person statement can carry several appointments (Phase 193), and a
    director's statement may be in place on one board before another. The
    person is verified if any of their roles is. The verifier and verification
    date come from the richest record (an ACSP record names both); the date
    given for the direct route is the **earliest** statement, which is the
    latest the identity can have been verified — and is named as a statement
    date, never as a verification date.
    """
    if not roles:
        return None
    best = max(roles, key=lambda r: (r.richness, -_ordinal(r.statement_on)))
    first = min(r.statement_on for r in roles)
    annotation = commenting(
        target,
        (
            "Companies House records this person's identity as verified. "
            + _who_clause(best)
            + " The first identity verification statement for a role in this "
            f"data was supplied on {_human_date(first)}."
        ),
    )
    structure = _structure(best)
    structure["firstVerificationStatementOn"] = first
    annotation[ANNOTATION_KEY] = structure
    if url:
        annotation["url"] = url
    return annotation


def _ordinal(iso: str) -> int:
    return date.fromisoformat(iso).toordinal()


def role_verification_from_annotation(
    annotation: dict[str, Any],
) -> Verification | None:
    """Read a role annotation back — the mapper's post-pass derives the person
    from the relationships it has already written, so the two cannot disagree."""
    structure = annotation.get(ANNOTATION_KEY)
    if not isinstance(structure, dict) or structure.get("status") != "verified":
        return None
    statement_on = _iso(structure.get("verificationStatementSuppliedOn"))
    route = structure.get("route")
    if not statement_on or route not in (ROUTE_ACSP, ROUTE_COMPANIES_HOUSE):
        return None
    verifier = structure.get("verifiedBy") or {}
    return Verification(
        statement_on=statement_on,
        route=route,
        identity_verified_on=_iso(structure.get("identityVerifiedOn")),
        verifier=verifier.get("name") or None,
        supervisors=tuple(verifier.get("antiMoneyLaunderingSupervisoryBodies") or ()),
    )
