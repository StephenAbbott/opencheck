"""Connecticut change emitter — Time Machine events from SOTS filings.

The Connecticut Secretary of the State publishes two dated histories on
data.ct.gov, and the History tab reads both through
``CtSotsAdapter.fetch_timeline_data`` (the lookup's cached rows plus the
Filing History, ``ah3s-bes7``). Measured over the 686 ISSUED ``RA000600``
LEIs on 20 Nov 2026: a median of 19 filings a business (3 without annual
reports), at most 195, and 73% of all filings are annual reports.

* **Name changes** — the Name Change History, one ``LEGAL_NAME_CHANGE`` per
  recorded change, dated by the register's change date. 150 of 172 rename
  dates are also the day of an amendment filing ("Amend Name", "Name Change
  Amendment", a plain "Amendment"); that filing is the same change and is
  folded into it, never shown twice. An amendment on any other day stays an
  administrative row.
* **Status changes** — the filings that end, restore or move a business
  (dissolution, withdrawal, cancellation, reinstatement, revocation of a
  dissolution, forfeiture, conversion, domestication): Tier 2, dated by the
  filing's effective date where the register gives one, else its filing date.
  For a **foreign** business a dissolution, revocation, withdrawal or
  cancellation ends only its registration in Connecticut, and is worded so.
  A Notice of Intent to Dissolve/Revoke is a warning, not a change of status:
  Tier 3.
* **Mergers** — a merger or consolidation certificate never names the other
  business. On a domestic business the register records as Merged or
  Consolidation, the latest such certificate is a ``SUCCESSION``; on the
  survivor it changed nothing about the survivor's status and stays Tier 3.
  A business the register records as Merged or Consolidation with no such
  certificate on its own record (it was filed on the survivor) still gets
  the ``SUCCESSION`` row, from the Business Master: dated by its dissolution
  date where it gives one, else undated — never left to end on its last
  annual report as if it were still trading.
* **Annual reports** — one grouped Tier 3 row per business (Stephen,
  20 Nov 2026), dated by the latest report, never one row per year.
* **Everything else** — a Tier 3 administrative row: formations, agent and
  address changes, amendments, interim notices.

Personal data: the Filing History's organizer and incorporator columns are
never read (the adapter's ``$select`` is an allowlist and the schema refuses
them). Two categories are published with a broken dash ("Mass Agent Change
\\ufffd Address"), and some rows hold the literal string "None"; both are
cleaned here.
"""

from __future__ import annotations

from typing import Any

from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_SOURCE = "ct_sots"

#: Annual-report categories: grouped into one row.
_ANNUAL_REPORT_TYPES = frozenset(
    {"Annual Report", "Amend Annual Report", "Corrected Annual Report"}
)

#: Filings that can record a change of name. One filed (or effective) on the
#: day the Name Change History records a rename is that rename and is folded
#: into it; any other is kept as an administrative row. A plain "Amendment"
#: counts: Eder-Goodman, LLC's rename of 1 April 2026 was filed as an
#: Amendment / Certificate of Amendment effective that day.
_NAME_FILING_TYPES = frozenset(
    {"Amend Name", "Amnd Name", "Certificate of Amendment - Name Change"}
)
_NAME_CATEGORIES = frozenset({"Name Change Amendment", "Amendment"})

#: Status-changing filings, by filing type: (domestic words, foreign words).
#: Foreign words are None where the filing means the same for both.
_STATUS_BY_FILING: dict[str, tuple[str, str | None]] = {
    "Certificate of Dissolution/Revocation": (
        "Dissolved by the Secretary of the State",
        "Registration in Connecticut revoked by the Secretary of the State",
    ),
    "Certificate of Dissolution": ("Dissolved", "Registration in Connecticut ended"),
    "Statement of Dissolution": ("Dissolved", "Registration in Connecticut ended"),
    "Dissolution": ("Dissolved", "Registration in Connecticut ended"),
    "Dissolved": ("Dissolved", "Registration in Connecticut ended"),
    "Certificate of Withdrawal": (
        "Withdrawn", "Withdrew its registration in Connecticut",
    ),
    "Statement of Withdrawal Registration": (
        "Withdrawn", "Withdrew its registration in Connecticut",
    ),
    "Withdrawal of Certificate of Authority": (
        "Withdrawn", "Withdrew its registration in Connecticut",
    ),
    "Certificate of Cancellation": ("Cancelled", "Registration in Connecticut cancelled"),
    "Cancellation of Registration": ("Cancelled", "Registration in Connecticut cancelled"),
    "Statement of Cancellation": ("Cancelled", "Registration in Connecticut cancelled"),
    "Cancellation": ("Cancelled", "Registration in Connecticut cancelled"),
    "Certificate of Reinstatement": ("Reinstated", None),
    "Reinstatement": ("Reinstated", None),
    "Revocation of Dissolution": ("Dissolution revoked", None),
    "Forfeited": ("Forfeited", None),
    "Forfeiture": ("Forfeited", None),
}
#: The same, by category, for a row whose filing type is blank or "None".
_STATUS_BY_CATEGORY: dict[str, str] = {
    "Dissolution": "Dissolution",
    "Withdrawal": "Certificate of Withdrawal",
    "Cancellation": "Cancellation",
    "Reinstatement": "Reinstatement",
    "Revocation of dissolution": "Revocation of Dissolution",
    "Forfeited": "Forfeited",
    "Forfeiture": "Forfeiture",
}

_MERGER_FILINGS = frozenset(
    {"Certificate of Merger", "Merger", "Statement of Merger"}
)
_CONSOLIDATION_FILINGS = frozenset({"Certificate of Consolidation", "Consolidation"})
_MERGER_CATEGORIES = frozenset({"Merger"})
_CONSOLIDATION_CATEGORIES = frozenset({"Certificate of Consolidation", "Consolidation"})

_CONVERSION_FILINGS = frozenset({"Certificate of Conversion", "Statement of Conversion"})
_DOMESTICATION_FILINGS = frozenset(
    {
        "Certificate of Domestication",
        "Statement of Domestication",
        "Certificate of Redomestication",
        "Statement of Redomestication",
    }
)

MERGED_WORDS = "Merged into another business; the register does not name it"
CONSOLIDATED_WORDS = "Consolidated into another business; the register does not name it"
#: Merged / Consolidation in the Business Master, no certificate on the record.
MERGED_STATUS_WORDS = (
    "Merged into another business, as the register records; no merger filing "
    "is on this business's record, and the register does not name the other business"
)
CONSOLIDATED_STATUS_WORDS = (
    "Consolidated into another business, as the register records; no "
    "consolidation filing is on this business's record, and the register does "
    "not name the other business"
)


def text(value: Any) -> str:
    """A field as the register meant it: trimmed, the literal "None" as
    blank, and the broken dash two categories carry as a hyphen."""
    from ..sources.ct_sots import clean_field

    out = clean_field(value).replace("�", "-")
    out = " ".join(out.split())
    return "" if out == "None" else out


def _filing_date(filing: dict[str, Any]) -> str | None:
    from ..sources.ct_sots import iso_date

    return iso_date(filing.get("effective_date")) or iso_date(filing.get("filing_date"))


def _filing_days(filing: dict[str, Any]) -> set[str]:
    from ..sources.ct_sots import iso_date

    return {
        d
        for d in (iso_date(filing.get("filing_date")), iso_date(filing.get("effective_date")))
        if d
    }


def _is_annual_report(category: str, filing_type: str) -> bool:
    return category in _ANNUAL_REPORT_TYPES or (
        not category and filing_type in _ANNUAL_REPORT_TYPES
    )


def _is_name_filing(category: str, filing_type: str) -> bool:
    return filing_type in _NAME_FILING_TYPES or category in _NAME_CATEGORIES


def _merger_kind(category: str, filing_type: str) -> str | None:
    if filing_type in _CONSOLIDATION_FILINGS or (
        not filing_type and category in _CONSOLIDATION_CATEGORIES
    ):
        return "consolidation"
    if filing_type in _MERGER_FILINGS or (not filing_type and category in _MERGER_CATEGORIES):
        return "merger"
    return None


def _status_words(category: str, filing_type: str, *, domestic: bool) -> str | None:
    key = filing_type if filing_type in _STATUS_BY_FILING else None
    if key is None and not filing_type:
        key = _STATUS_BY_CATEGORY.get(category)
    if key is None:
        return None
    domestic_words, foreign_words = _STATUS_BY_FILING[key]
    return domestic_words if domestic or foreign_words is None else foreign_words


def _sort_key(filing: dict[str, Any]) -> tuple[str, str]:
    return (_filing_date(filing) or "", text(filing.get("name")))


def _event(
    business_id: str,
    filing: dict[str, Any],
    *,
    raw_change_type: str,
    raw_field: str | None,
    change_type: ChangeType | None,
    tier: Tier,
    value_new: str | None = None,
) -> ChangeEvent:
    return ChangeEvent(
        source_id=_SOURCE,
        subject_id=business_id,
        record_type=RecordType.ENTITY,
        raw_change_type=raw_change_type,
        raw_field=raw_field,
        value_new=value_new,
        raw_payload_ref=text(filing.get("name")) or None,
        change_type=change_type,
        tier=tier,
        event_date=_filing_date(filing),
        date_basis=DateBasis.EFFECTIVE,
        date_confidence=DateConfidence.HIGH,
    )


def _name_events(business_id: str, bundle: dict[str, Any]) -> tuple[list[ChangeEvent], set[str]]:
    """One event per recorded rename, and the days they fell on."""
    from ..sources.ct_sots import name_changes_of

    out: list[ChangeEvent] = []
    days: set[str] = set()
    for change in name_changes_of(bundle):
        old, new = text(change.get("old")), text(change.get("new"))
        if not new or old == new:
            continue
        if change.get("on"):
            days.add(change["on"])
        out.append(
            ChangeEvent(
                source_id=_SOURCE,
                subject_id=business_id,
                record_type=RecordType.ENTITY,
                raw_change_type="Name change",
                raw_field="business_name",
                value_old=old or None,
                value_new=new,
                change_type=ChangeType.LEGAL_NAME_CHANGE,
                tier=Tier.IDENTITY_STATUS,
                event_date=change.get("on"),
                date_basis=DateBasis.EFFECTIVE,
                date_confidence=DateConfidence.HIGH,
            )
        )
    return out, days


def annual_report_summary(reports: list[dict[str, Any]]) -> str:
    """"23 annual reports filed, for 2003 to 2026" — the grouped row's words."""
    from ..sources.ct_sots import iso_date

    years: list[str] = []
    for r in reports:
        year = text(r.get("report_year"))
        if not (year.isdigit() and len(year) == 4):
            year = (iso_date(r.get("filing_date")) or "")[:4]
        if year:
            years.append(year)
    n = len(reports)
    head = f"{n:,} annual report{'s' if n != 1 else ''} filed"
    if not years:
        return head
    first, last = min(years), max(years)
    return f"{head}, for {first}" if first == last else f"{head}, for {first} to {last}"


def ct_sots_change_events(bundle: dict[str, Any] | None) -> list[ChangeEvent]:
    """Build Time Machine ChangeEvents from ``CtSotsAdapter.fetch_timeline_data``."""
    from ..sources.ct_sots import summarise

    if not bundle or bundle.get("is_stub") or not bundle.get("master"):
        return []
    summary = summarise(bundle)
    business_id = summary.get("business_id") or ""
    domestic = bool(summary.get("domestic"))
    status = summary.get("status") or ""

    events, rename_days = _name_events(business_id, bundle)

    filings = sorted(
        (f for f in bundle.get("filings") or [] if isinstance(f, dict)), key=_sort_key
    )
    # The certificate that ended a business absorbed into another: the latest
    # merger or consolidation, and only when the register says it ended so.
    absorbed_by: dict[str, Any] | None = None
    if domestic and status in ("Merged", "Consolidation"):
        wanted = "consolidation" if status == "Consolidation" else "merger"
        for filing in filings:
            if _merger_kind(text(filing.get("type")), text(filing.get("filing_type"))) == wanted:
                absorbed_by = filing

    if domestic and status in ("Merged", "Consolidation") and absorbed_by is None:
        dated = summary.get("dissolved_on")
        events.append(
            ChangeEvent(
                source_id=_SOURCE,
                subject_id=business_id,
                record_type=RecordType.ENTITY,
                raw_change_type=status,
                raw_field="status",
                value_new=(
                    CONSOLIDATED_STATUS_WORDS if status == "Consolidation" else MERGED_STATUS_WORDS
                ),
                change_type=ChangeType.SUCCESSION,
                tier=Tier.IDENTITY_STATUS,
                event_date=dated,
                date_basis=DateBasis.EFFECTIVE,
                date_confidence=DateConfidence.MEDIUM if dated else DateConfidence.LOW,
            )
        )

    reports: list[dict[str, Any]] = []
    for filing in filings:
        category = text(filing.get("type"))
        filing_type = text(filing.get("filing_type"))
        label = filing_type or category or "Filing"
        field = category or None

        if _is_annual_report(category, filing_type):
            reports.append(filing)
            continue
        if _is_name_filing(category, filing_type) and _filing_days(filing) & rename_days:
            continue
        if filing is absorbed_by:
            events.append(
                _event(
                    business_id, filing,
                    raw_change_type=label, raw_field=field,
                    change_type=ChangeType.SUCCESSION, tier=Tier.IDENTITY_STATUS,
                    value_new=CONSOLIDATED_WORDS if status == "Consolidation" else MERGED_WORDS,
                )
            )
            continue
        words = _status_words(category, filing_type, domestic=domestic)
        if words:
            events.append(
                _event(
                    business_id, filing,
                    raw_change_type=label, raw_field=field,
                    change_type=ChangeType.STATUS_CHANGED, tier=Tier.IDENTITY_STATUS,
                    value_new=words,
                )
            )
            continue
        if filing_type in _CONVERSION_FILINGS or (not filing_type and category == "Conversion"):
            events.append(
                _event(
                    business_id, filing,
                    raw_change_type=label, raw_field=field,
                    change_type=ChangeType.LEGAL_FORM_CHANGE, tier=Tier.IDENTITY_STATUS,
                    value_new="Converted; the register does not say into what",
                )
            )
            continue
        if filing_type in _DOMESTICATION_FILINGS or (
            not filing_type and category == "Domestication"
        ):
            events.append(
                _event(
                    business_id, filing,
                    raw_change_type=label, raw_field=field,
                    change_type=ChangeType.JURISDICTION_CHANGE, tier=Tier.IDENTITY_STATUS,
                    value_new=f"{label} filed",
                )
            )
            continue
        events.append(
            _event(
                business_id, filing,
                raw_change_type=label, raw_field=field,
                change_type=None, tier=Tier.ADMIN_NOISE,
            )
        )

    if reports:
        # Dated by the latest report; no single filing number names the row.
        latest = {**reports[-1], "name": None}
        events.append(
            _event(
                business_id, latest,
                raw_change_type=annual_report_summary(reports),
                raw_field="Annual Report",
                change_type=None, tier=Tier.ADMIN_NOISE,
            )
        )
    return events


__all__ = ["annual_report_summary", "ct_sots_change_events", "text"]
