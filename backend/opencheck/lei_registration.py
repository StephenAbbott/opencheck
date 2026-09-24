"""The LEI's own registration status — whether the LEI record is kept up (Phase 242).

GLEIF publishes two statuses on every Level 1 record, and they answer
different questions:

* ``entity.status`` (ACTIVE / INACTIVE) — does the legal entity still exist?
  That is register status, read by ``bods.liveness`` and shown by
  ``subject_profile.register_status``. Unchanged by this module.
* ``registration.status`` (ISSUED / LAPSED / RETIRED / ...) — is the LEI
  *record* maintained? A LAPSED LEI belongs to a company that may be
  perfectly alive and has simply not paid its issuer to re-check its reference
  data. ``bods/mapper.py`` deliberately never reads it as liveness.

Before this phase the second was rendered only on the SEO entity pages, so
American Foreign Policy Council (``549300W96W2VKSMVDF81``) — LAPSED since its
renewal fell due on 19 Oct 2017 — read as an ordinary live LEI in the lookup,
the MCP profile and the report. This module reads the block once, from the
GLEIF anchor the pipeline already holds (live API, Golden Copy mirror or
snapshot — all three carry ``registration``), and the result rides on the
``subject_profile`` event as ``lei_registration``, frozen with the run.

Rules, each one a decision (Stephen, 24 Sept 2026):

* **Only the dates GLEIF publishes.** GLEIF has no "last validated" field.
  A LAPSED LEI's date is ``nextRenewalDate`` — the renewal that fell due and
  was not made, which is exactly when it lapsed. ``lastUpdateDate`` is when
  GLEIF last edited the record (AFPC's reads 8 Apr 2026, nine years after it
  lapsed), so it is carried as "record last updated" and never called a
  validation. No date is estimated.
* **A fact, not a finding.** Nothing here reaches ``risk.py`` or the verdict,
  and :data:`BANNED_WORDS` keeps the sentences from implying one.
* **ISSUED is stated, not flagged.** The chip beside the LEI renders only for
  a status other than ISSUED (``flag``); the identity row, the MCP profile and
  the report always carry the status.
"""

from __future__ import annotations

from typing import Any

#: GLEIF LEI-CDF ``RegistrationStatus`` → the word a reader sees. The labels
#: say what GLEIF's own code list says each status means, and no more.
LABELS: dict[str, str] = {
    "ISSUED": "Issued",
    "LAPSED": "Lapsed",
    "RETIRED": "Retired",
    "MERGED": "Merged",
    "ANNULLED": "Annulled",
    "DUPLICATE": "Duplicate",
    "CANCELLED": "Cancelled",
    "TRANSFERRED": "Transferred",
    "PENDING_TRANSFER": "Pending transfer",
    "PENDING_ARCHIVAL": "Pending archival",
    "PENDING_VALIDATION": "Pending validation",
}

#: What each status means, as a clause after "GLEIF records this LEI as <label>".
#: LAPSED's clause is built with its date in :func:`sentence`.
_MEANING: dict[str, str] = {
    "ISSUED": "its reference data is maintained by its issuer",
    "RETIRED": "the LEI is no longer maintained, usually because the entity ceased to operate",
    "MERGED": "the entity merged into another and the LEI is no longer maintained",
    "ANNULLED": "the LEI was issued in error or withdrawn by its issuer",
    "DUPLICATE": "another LEI was issued for the same entity",
    "CANCELLED": "the registration was abandoned before the LEI was issued",
    "TRANSFERRED": "the LEI moved to another issuer",
    "PENDING_TRANSFER": "the LEI is moving to another issuer",
    "PENDING_ARCHIVAL": "the LEI is about to be transferred and archived by its issuer",
    "PENDING_VALIDATION": "the issuer has not yet validated the registration",
}

#: The distinction the sentence always ends on, because it is the reading
#: this phase exists to stop: a lapsed LEI is not a dissolved company.
NOT_ENTITY_STATUS = "This is the status of the LEI record, not of the company."

#: Words a registration sentence must never contain — the status is a
#: record-keeping fact, and each of these would turn it into a judgement.
BANNED_WORDS: tuple[str, ...] = (
    "risk", "suspicious", "invalid", "expired", "dissolved", "defunct",
    "non-compliant", "noncompliant", "red flag", "warning",
)


def _date(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw[:10] if len(raw) >= 10 else None


def from_gleif_record(record: dict[str, Any] | None) -> dict[str, Any] | None:
    """The ``lei_registration`` payload from a GLEIF ``lei-records`` resource.

    ``None`` when the record carries no ``registration.status`` (a curated
    Open Ownership bundle with no live call behind it) — absence is never
    ISSUED. Shape::

        {"status": "LAPSED", "label": "Lapsed", "flag": True,
         "since": "2017-10-19", "next_renewal_date": "2017-10-19",
         "last_update_date": "2026-04-08", "initial_registration_date": "2016-10-21",
         "managing_lou": "5493001KJTIIGC8Y1R12", "source_id": "gleif",
         "sentence": "GLEIF records this LEI as lapsed: ..."}
    """
    attrs = (record or {}).get("attributes") or {}
    reg = attrs.get("registration") or {}
    status = str(reg.get("status") or "").strip().upper()
    if not status:
        return None
    next_renewal = _date(reg.get("nextRenewalDate"))
    out: dict[str, Any] = {
        "status": status,
        "label": LABELS.get(status, status.replace("_", " ").capitalize()),
        "flag": status != "ISSUED",
        # When the status took effect, where GLEIF's own data says so. Only
        # LAPSED has such a date: a lapse happens on the missed renewal date.
        "since": next_renewal if status == "LAPSED" else None,
        "next_renewal_date": next_renewal,
        "last_update_date": _date(reg.get("lastUpdateDate")),
        "initial_registration_date": _date(reg.get("initialRegistrationDate")),
        "managing_lou": str(reg.get("managingLou") or "") or None,
        "source_id": "gleif",
    }
    out["sentence"] = sentence(out)
    return out


def format_date(iso: str | None) -> str:
    """"19 Oct 2017", "16 Sept 2026" — the house date form (``lib/subjectProfile.ts``)."""
    if not iso or len(iso) < 10:
        return iso or ""
    months = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sept", "Oct", "Nov", "Dec")
    try:
        y, m, d = int(iso[:4]), int(iso[5:7]), int(iso[8:10])
        return f"{d} {months[m - 1]} {y}"
    except (ValueError, IndexError):
        return iso


def short_line(reg: dict[str, Any] | None) -> str | None:
    """The row value: "Lapsed — renewal was due 19 Oct 2017" / "Issued — renews 21 Mar 2027"."""
    if not reg or not reg.get("status"):
        return None
    label = reg.get("label") or reg["status"]
    status = reg["status"]
    renewal = reg.get("next_renewal_date")
    if status == "LAPSED" and renewal:
        return f"{label} — renewal was due {format_date(renewal)}"
    if status == "ISSUED" and renewal:
        return f"{label} — renews {format_date(renewal)}"
    return label


def sentence(reg: dict[str, Any] | None) -> str | None:
    """One or two plain sentences for the MCP profile and the report."""
    if not reg or not reg.get("status"):
        return None
    status = reg["status"]
    label = str(reg.get("label") or status).lower()
    renewal = reg.get("next_renewal_date")
    if status == "LAPSED":
        meaning = (
            f"its renewal was due on {format_date(renewal)} and has not been made, "
            "so no issuer has re-checked its reference data since then"
            if renewal
            else "its renewal has not been made, so its reference data is not being re-checked"
        )
    elif status == "ISSUED" and renewal:
        meaning = f"{_MEANING['ISSUED']}, and its next renewal is due on {format_date(renewal)}"
    else:
        meaning = _MEANING.get(status, "")
    first = f"GLEIF records this LEI as {label}" + (f": {meaning}." if meaning else ".")
    if status == "ISSUED":
        return first
    updated = reg.get("last_update_date")
    tail = f" GLEIF last updated the record on {format_date(updated)}." if updated else ""
    return f"{first}{tail} {NOT_ENTITY_STATUS}"


def report_value(report: dict[str, Any] | None) -> str | None:
    """The identifiers-table value for the PDF/HTML and Markdown reports.

    Read from the report's frozen ``subject_profile`` — never re-fetched, so a
    saved report states what GLEIF recorded on the day of the run. A status
    other than ISSUED gets the full sentence (it has to say that it is not the
    company's status); ISSUED gets the short line.
    """
    profile = (report or {}).get("subject_profile") or {}
    reg = profile.get("lei_registration") if isinstance(profile, dict) else None
    if not isinstance(reg, dict) or not reg.get("status"):
        return None
    return sentence(reg) if reg.get("flag") else short_line(reg)
