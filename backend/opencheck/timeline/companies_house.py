"""Companies House change emitter — maps filing-history events to ``ChangeEvent``.

The second emitter for the Time Machine model, and the proof that the model is
source-agnostic. Companies House is *event-typed*, not field-diffed: its
filing-history API returns discrete filings with a ``category`` (and a ``type``
code like ``PSC01``), each with a real filing/effective ``date``. So where the
GLEIF emitter diffs ``valueOld``/``valueNew``, this one maps a controlled
*filing vocabulary* into the same ``ChangeType`` codelist.

Two consequences worth noting versus GLEIF:

- CH dates are **effective**, not merely recorded → ``DateBasis.EFFECTIVE`` and
  ``DateConfidence.HIGH``. (GLEIF was ``RECORDED`` / ``MEDIUM``.)
- A CH filing is an event, not a field transition, so ``value_old`` /
  ``value_new`` are left ``None``; ``raw_payload_ref`` points at the filing.

Reference: Companies House filing-history API ``category`` enum and PSC
transaction codes (PSC01–PSC09).
"""

from __future__ import annotations

from .model import (
    ChangeEvent,
    ChangeType,
    DateBasis,
    DateConfidence,
    RecordType,
    Tier,
)

_T1 = Tier.OWNERSHIP_CONTROL
_T2 = Tier.IDENTITY_STATUS
_T3 = Tier.ADMIN_NOISE

# PSC (persons-with-significant-control) transaction codes → (change_type, tier).
# PSC01-03 notify a new PSC; PSC07 ceases one; PSC08/09 are PSC *statements*
# (e.g. "company has no PSC"); PSC04-06 change a PSC's details — most often the
# nature/level of control, though a later parser may find some are address-only.
_PSC_TYPES: dict[str, tuple[ChangeType, Tier]] = {
    "PSC01": (ChangeType.OWNER_ADDED, _T1),
    "PSC02": (ChangeType.OWNER_ADDED, _T1),
    "PSC03": (ChangeType.OWNER_ADDED, _T1),
    "PSC04": (ChangeType.CONTROL_NATURE_CHANGED, _T1),
    "PSC05": (ChangeType.CONTROL_NATURE_CHANGED, _T1),
    "PSC06": (ChangeType.CONTROL_NATURE_CHANGED, _T1),
    "PSC07": (ChangeType.OWNER_REMOVED, _T1),
    "PSC08": (ChangeType.REPORTING_EXCEPTION_CHANGED, _T1),
    "PSC09": (ChangeType.REPORTING_EXCEPTION_CHANGED, _T1),
}

# Non-PSC filing categories → (change_type, tier). Anything not listed (accounts,
# confirmation-statement, officers, capital, gazette, incorporation, mortgage,
# resolution, …) is administrative noise for an *ownership* timeline and falls
# through to Tier 3 — kept, suppressed by default. Officer appointments and
# share-capital filings are deliberately Tier 3 here; they could become a
# filterable layer later, but they are not beneficial-ownership changes.
_CATEGORY_TYPES: dict[str, tuple[ChangeType, Tier]] = {
    "change-of-name": (ChangeType.LEGAL_NAME_CHANGE, _T2),
    "reregistration": (ChangeType.LEGAL_FORM_CHANGE, _T2),  # e.g. PLC → Ltd
    "address": (ChangeType.ADDRESS_CHANGE, _T2),
    "dissolution": (ChangeType.STATUS_CHANGED, _T2),
    "liquidation": (ChangeType.STATUS_CHANGED, _T2),
    "insolvency": (ChangeType.STATUS_CHANGED, _T2),
    "restoration": (ChangeType.STATUS_CHANGED, _T2),
}

# Categories whose changes are about an ownership relationship, not the entity.
_RELATIONSHIP_CATEGORIES = {"persons-with-significant-control"}


def _iso_date(value: str | None) -> str | None:
    if not value:
        return None
    return value[:10]


def _self_link(item: dict) -> str | None:
    links = item.get("links")
    if isinstance(links, dict) and links.get("self"):
        return links["self"]
    return item.get("transaction_id")


def classify_companies_house_filing(
    item: dict, *, company_id: str = ""
) -> ChangeEvent:
    """Map one Companies House filing-history item to a ``ChangeEvent``.

    ``item`` is a filing-history entry (``category``, ``type``, ``date``,
    optional ``action_date``, ``links``/``transaction_id``). ``company_id`` is
    the stable subject recordId (defaults to the company number).
    """
    category = (item.get("category") or "").lower()
    ftype = (item.get("type") or "").upper()

    if category in _RELATIONSHIP_CATEGORIES:
        record_type = RecordType.RELATIONSHIP
        # Dispatch on the PSC transaction code; unknown PSC codes are still
        # PSC-related, so surface them as a control change rather than noise.
        change_type, tier = _PSC_TYPES.get(
            ftype, (ChangeType.CONTROL_NATURE_CHANGED, _T1)
        )
    else:
        record_type = RecordType.ENTITY
        change_type, tier = _CATEGORY_TYPES.get(category, (None, _T3))

    # CH gives a real effective date where known (action_date), else the filing
    # date — both are effective-basis, high confidence.
    event_date = _iso_date(item.get("action_date")) or _iso_date(item.get("date"))

    return ChangeEvent(
        source_id="companies_house",
        subject_id=company_id or item.get("company_number") or "",
        record_type=record_type,
        raw_change_type=ftype or category,
        raw_field=category or None,
        value_old=None,  # CH filings are events, not field transitions
        value_new=None,
        raw_payload_ref=_self_link(item),
        change_type=change_type,
        tier=tier,
        event_date=event_date,
        date_basis=DateBasis.EFFECTIVE,
        date_confidence=DateConfidence.HIGH,
    )


__all__ = ["classify_companies_house_filing"]
