"""Which Companies House filing notified a PSC — declarant and filing provenance.

From the "Declarant and filing provenance in BODS statements" ticket (Open
Ownership's *Basic disclosure information*: who disclosed, and when). A PSC
record says *when* the register was told (``notified_on``, ``ceased_on``);
the filing history says *on what*: the form (PSC01/02/03 to notify, PSC07 to
cease), its transaction id, the day it was filed and the document. This
module matches the two and writes the answer as one ``commenting``
annotation per event on the PSC's relationship statement.

Stephen's decisions (3 Nov 2026):

* **An annotation, not the ``source`` block.** ``source`` stays the register
  record OpenCheck read; the annotation's ``url`` is the filing's document,
  its ``description`` one sentence naming the form, transaction and filing
  day (both survive the RDF export), and :data:`FILING_PROPERTY` carries the
  same facts structured, for the UI. Annotations are outside the
  content-addressed digest, so no ``statementId`` moves.
* **Every company in the bundle**, one paged filing-history call each (the
  adapter's ``filing_history``).
* **No match is said, never signalled.** A missing PSC01 is routine: a PSC
  given at incorporation is on the incorporation filing (NEWINC), and until
  26 June 2017 a company gave its PSCs, and changes to them, on its next
  confirmation statement. Both routes are named; anything else is "no filing
  names this notification". Nothing here reaches
  ``RECORD_CONSISTENCY``, the risk engine or the discrepancy note.

Matching is by date **and** name. The date alone is not enough: Monzo Bank
Limited (09446231) has one PSC02 dated 6 April 2016, and it names
Passion Capital Fs (Gp) Llp, not the Passion Capital Fs Lp notified the same
day. Names are compared in the shared comparable forms
(``names.person_name_key`` drops "Mrs", which the PSC record carries and the
filing does not). Companies House publishes no presenter, so the declarant
is always the company, the statutory filer, and nothing more is claimed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..names import despace, normalise_name, org_comparable_name, person_name_key
from .annotations import commenting, pointer

#: The annotation property carrying the filing, structured. An extension
#: field, like Phase 305's ``gleifLegalEntityEvent``; read back by
#: :func:`filing_of` and by ``frontend/src/lib/filingProvenance.ts``.
FILING_PROPERTY = "companiesHouseFiling"

#: The one filing-history request per company: the three categories a PSC
#: can be notified on.
FILING_CATEGORIES = "persons-with-significant-control,incorporation,confirmation-statement"

#: Notification and cessation forms. PSC03 is the "other registrable
#: person" (a legal person that is not a company); a limited liability
#: partnership files the same forms with an ``LL`` prefix (OC302604 files
#: LLPSC01 and LLPSC07).
NOTIFICATION_FORMS = frozenset(
    {"PSC01", "PSC02", "PSC03", "LLPSC01", "LLPSC02", "LLPSC03"}
)
CESSATION_FORMS = frozenset({"PSC07", "LLPSC07"})

#: UK legal-form words a company's name can change between while it stays
#: the company a filing named (Bellis Finco Limited re-registered as Bellis
#: Finco Plc after its PSC02). Compared only on the name's tail.
_UK_FORM_TAILS = (
    ("public", "limited", "company"),
    ("limited", "liability", "partnership"),
    ("limited", "partnership"),
    ("limited",),
    ("ltd",),
    ("plc",),
    ("llp",),
    ("lp",),
    ("unlimited",),
    ("cic",),
)

#: The PSC register's start; the first day a confirmation statement (which
#: replaced the annual return) could carry PSCs; and the day PSC changes
#: became event-driven filings (PSC01–PSC07 within 14 days). Before that, a
#: company gave its PSCs, and any change to them, on its next confirmation
#: statement — HSBC Bank plc's HSBC Holdings, notified 26 Oct 2016, is on no
#: PSC form.
REGIME_START = "2016-04-06"
CONFIRMATION_STATEMENTS_FROM = "2016-06-30"
EVENT_DRIVEN_FROM = "2017-06-26"
CONFIRMATION_STATEMENT_FORMS = frozenset({"CS01", "LLCS01"})

#: How a notification was found — the closed vocabulary of ``match``.
MATCH_FILING = "filing"
MATCH_INCORPORATION = "incorporation"
MATCH_CONFIRMATION_STATEMENT = "confirmation_statement"
MATCH_NONE = "none"
MATCHES = (MATCH_FILING, MATCH_INCORPORATION, MATCH_CONFIRMATION_STATEMENT, MATCH_NONE)

#: Companies House names no presenter; the statutory filer is the company.
DECLARANT_COMPANY = "company"

_SITE = "https://find-and-update.company-information.service.gov.uk"

_DECLARANT_SENTENCE = (
    "Companies House records the company as the filer and does not publish "
    "who presented it."
)


@dataclass(frozen=True)
class FilingHistory:
    """A company's filing history as far as it was read.

    ``complete`` is False when the history could not be read whole (an
    error, a capped list, a status other than available): a match may still
    be found in what was read, but an absence is not claimed.
    """

    items: tuple[dict[str, Any], ...]
    complete: bool


def history_of(payload: Any) -> FilingHistory | None:
    """The filing history the adapter put on a company bundle, or ``None``
    when none was read (an older cached bundle, a failed call)."""
    if not isinstance(payload, dict) or payload.get("opencheck_unavailable"):
        return None
    items = tuple(i for i in payload.get("items") or [] if isinstance(i, dict))
    status = payload.get("filing_history_status")
    total = payload.get("total_count")
    complete = (
        not payload.get("opencheck_page_cap_reached")
        and status in (None, "filing-history-available")
        and (not isinstance(total, int) or len(items) >= total)
    )
    return FilingHistory(items=items, complete=complete)


def document_url(number: str, filing: dict[str, Any]) -> str:
    """The register's own page for the filing's document, else the company's
    filing history (an old filing can have no document)."""
    txid = filing.get("transaction_id")
    links = filing.get("links") or {}
    if txid and links.get("document_metadata"):
        return f"{_SITE}/company/{number}/filing-history/{txid}/document?format=pdf&download=0"
    return f"{_SITE}/company/{number}/filing-history"


def _values(filing: dict[str, Any]) -> dict[str, Any]:
    values = filing.get("description_values")
    return values if isinstance(values, dict) else {}


def _is_person(psc: dict[str, Any]) -> bool:
    return "individual" in (psc.get("kind") or "").lower()


def _psc_names(psc: dict[str, Any]) -> list[str]:
    names = [psc.get("name") or ""]
    elements = psc.get("name_elements") or {}
    if isinstance(elements, dict):
        parts = [elements.get(k) for k in ("forename", "middle_name", "surname")]
        names.append(" ".join(p for p in parts if p))
    return [n for n in names if n.strip()]


def _without_form(name: str) -> str:
    tokens = normalise_name(name).lower().split()
    for tail in _UK_FORM_TAILS:
        if len(tokens) > len(tail) and tuple(tokens[-len(tail):]) == tail:
            return " ".join(tokens[: -len(tail)])
    return " ".join(tokens)


def _names_nearly_agree(psc: dict[str, Any], filed: Any) -> bool:
    """The second tier, used only when it picks out exactly one filing: a
    company named without its legal form (it re-registered since), or a
    person filed without a middle name ("Lee Madine" for Lee Micheal Madine)
    — at least two of the filed names, the surname among them, all in the
    PSC's own name."""
    filed = str(filed or "").strip()
    if not filed:
        return False
    if not _is_person(psc):
        own = {_without_form(n) for n in _psc_names(psc)}
        return _without_form(filed) in own - {""}
    filed_tokens = set(person_name_key(filed).split())
    if len(filed_tokens) < 2:
        return False
    surname = normalise_name(((psc.get("name_elements") or {}).get("surname")) or "").lower()
    for name in _psc_names(psc):
        tokens = set(person_name_key(name).split())
        if filed_tokens <= tokens and (not surname or set(surname.split()) <= filed_tokens):
            return True
    return False


def _names_agree(psc: dict[str, Any], filed: Any) -> bool:
    filed = str(filed or "").strip()
    if not filed:
        return False
    if _is_person(psc):
        key = person_name_key(filed)
        return bool(key) and any(person_name_key(n) == key for n in _psc_names(psc))
    filed_key = org_comparable_name(filed)
    for name in _psc_names(psc):
        own = org_comparable_name(name)
        if own and (own == filed_key or despace(own) == despace(filed_key)):
            return True
    return False


def _earliest(filings: list[dict[str, Any]]) -> dict[str, Any]:
    return min(filings, key=lambda f: (str(f.get("date") or ""), str(f.get("transaction_id") or "")))


def _filing_facts(
    number: str, filing: dict[str, Any], *, event: str, match: str
) -> dict[str, Any]:
    facts: dict[str, Any] = {
        "event": event,
        "match": match,
        "declarant": DECLARANT_COMPANY,
        "type": filing.get("type"),
        "transactionId": filing.get("transaction_id"),
        "date": filing.get("date"),
        "documentUrl": document_url(number, filing),
    }
    if filing.get("description"):
        facts["description"] = filing["description"]
    return {k: v for k, v in facts.items() if v is not None}


def _named_filing(
    psc: dict[str, Any],
    history: FilingHistory,
    forms: frozenset[str],
    date_key: str,
    day: str,
) -> dict[str, Any] | None:
    """The filing of one of *forms* dated *day* that names *psc*: the
    earliest exact name match, else the one near match if there is exactly
    one. A date never matches on its own."""
    dated = [f for f in history.items if f.get("type") in forms and _values(f).get(date_key) == day]
    exact = [f for f in dated if _names_agree(psc, _values(f).get("psc_name"))]
    if exact:
        return _earliest(exact)
    near = [f for f in dated if _names_nearly_agree(psc, _values(f).get("psc_name"))]
    return near[0] if len(near) == 1 else None


def _made_up_to(filing: dict[str, Any]) -> str:
    return str(_values(filing).get("made_up_date") or filing.get("date") or "")


def _next_confirmation_statement(history: FilingHistory, notified: str) -> dict[str, Any] | None:
    """The first confirmation statement made up to on or after *notified* —
    the one that carried a PSC given before changes became event-driven.
    Legacy annual returns share the category and are never read."""
    statements = [
        f
        for f in history.items
        if f.get("type") in CONFIRMATION_STATEMENT_FORMS
        and _made_up_to(f) >= max(notified, CONFIRMATION_STATEMENTS_FROM)
    ]
    if not statements:
        return None
    return min(statements, key=lambda f: (_made_up_to(f), str(f.get("transaction_id") or "")))


def match_notification(
    psc: dict[str, Any], history: FilingHistory, *, number: str, date_of_creation: str | None
) -> dict[str, Any] | None:
    """The filing that notified *psc*, as :data:`FILING_PROPERTY` facts, or
    ``None`` when nothing can be said (no notification date; an incomplete
    history with no match)."""
    notified = psc.get("notified_on")
    if not notified:
        return None
    found = _named_filing(psc, history, NOTIFICATION_FORMS, "notification_date", notified)
    if found is not None:
        return _filing_facts(number, found, event="notification", match=MATCH_FILING)
    if date_of_creation and notified == date_of_creation:
        incorporation = [
            f
            for f in history.items
            if f.get("category") == "incorporation" and f.get("date") == date_of_creation
        ]
        if incorporation:
            return _filing_facts(
                number, _earliest(incorporation), event="notification", match=MATCH_INCORPORATION
            )
    if REGIME_START <= notified < EVENT_DRIVEN_FROM:
        statement = _next_confirmation_statement(history, notified)
        if statement is not None:
            return _filing_facts(
                number, statement, event="notification", match=MATCH_CONFIRMATION_STATEMENT
            )
    if not history.complete:
        return None
    return {"event": "notification", "match": MATCH_NONE, "declarant": DECLARANT_COMPANY}


def match_cessation(
    psc: dict[str, Any], history: FilingHistory, *, number: str
) -> dict[str, Any] | None:
    """The PSC07 that ceased *psc*, or a stated absence, or ``None``."""
    ceased = psc.get("ceased_on")
    if not ceased:
        return None
    found = _named_filing(psc, history, CESSATION_FORMS, "cessation_date", ceased)
    if found is not None:
        return _filing_facts(number, found, event="cessation", match=MATCH_FILING)
    if not history.complete:
        return None
    return {"event": "cessation", "match": MATCH_NONE, "declarant": DECLARANT_COMPANY}


def _transaction(facts: dict[str, Any]) -> str:
    txid = facts.get("transactionId")
    return f", transaction {txid}" if txid else ""


def describe(facts: dict[str, Any], psc: dict[str, Any]) -> str:
    """The annotation's sentence. Dates are ISO days, as filed."""
    match = facts.get("match")
    form = facts.get("type") or "an unnamed form"
    filed = facts.get("date") or "an unrecorded day"
    if facts.get("event") == "cessation":
        if match == MATCH_FILING:
            return (
                f"Cessation notified to Companies House on form {form}, filed {filed}"
                f"{_transaction(facts)}. {_DECLARANT_SENTENCE}"
            )
        return (
            "No PSC07 in Companies House's filing history names this cessation "
            f"of {psc.get('ceased_on')}."
        )
    if match == MATCH_FILING:
        return (
            f"Notified to Companies House on form {form}, filed {filed}"
            f"{_transaction(facts)}. {_DECLARANT_SENTENCE}"
        )
    if match == MATCH_INCORPORATION:
        return (
            f"Given to Companies House at incorporation, on the incorporation filing "
            f"({form}, filed {filed}{_transaction(facts)}); a PSC named at incorporation "
            f"has no separate notification form. {_DECLARANT_SENTENCE}"
        )
    if match == MATCH_CONFIRMATION_STATEMENT:
        return (
            "No PSC notification form names this party. Until 26 June 2017 a company "
            "gave its PSCs, and changes to them, on its next confirmation statement; "
            f"the first made up to on or after {psc.get('notified_on')} is {form}, "
            f"filed {filed}{_transaction(facts)}. {_DECLARANT_SENTENCE}"
        )
    return (
        "No filing in Companies House's filing history names this notification of "
        f"{psc.get('notified_on')} (PSC notifications, incorporation and confirmation "
        "statements were read)."
    )


def _annotation(facts: dict[str, Any], psc: dict[str, Any], target: str) -> dict[str, Any]:
    annotation = commenting(target, describe(facts, psc))
    if facts.get("documentUrl"):
        annotation["url"] = facts["documentUrl"]
    annotation[FILING_PROPERTY] = facts
    return annotation


def psc_annotations(
    psc: dict[str, Any],
    payload: Any,
    *,
    number: str,
    date_of_creation: str | None,
) -> list[dict[str, Any]]:
    """The filing annotations for one PSC's relationship statement: the
    notification on ``/recordDetails/interests``, a cessation on the first
    interest's ``endDate`` (every interest of a ceased PSC carries one). Empty
    when the filing history was not read."""
    history = history_of(payload)
    if history is None:
        return []
    out: list[dict[str, Any]] = []
    notified = match_notification(psc, history, number=number, date_of_creation=date_of_creation)
    if notified is not None:
        out.append(_annotation(notified, psc, pointer("recordDetails", "interests")))
    ceased = match_cessation(psc, history, number=number)
    if ceased is not None:
        out.append(
            _annotation(ceased, psc, pointer("recordDetails", "interests", 0, "endDate"))
        )
    return out


def filing_of(statement: dict[str, Any], event: str = "notification") -> dict[str, Any] | None:
    """Read a relationship statement's filing facts back, or ``None``."""
    for annotation in statement.get("annotations") or []:
        facts = annotation.get(FILING_PROPERTY) if isinstance(annotation, dict) else None
        if isinstance(facts, dict) and facts.get("event") == event:
            return facts
    return None


__all__ = [
    "CESSATION_FORMS",
    "DECLARANT_COMPANY",
    "FILING_CATEGORIES",
    "FILING_PROPERTY",
    "FilingHistory",
    "MATCHES",
    "NOTIFICATION_FORMS",
    "describe",
    "document_url",
    "filing_of",
    "history_of",
    "match_cessation",
    "match_notification",
    "psc_annotations",
]
