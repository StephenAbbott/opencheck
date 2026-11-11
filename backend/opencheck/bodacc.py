"""BODACC — France's official bulletin of civil and commercial notices, read live.

The Bulletin officiel des annonces civiles et commerciales (DILA) publishes,
for every company on a French trade register, the notices its register and
courts send: collective-procedure judgments, removals from the register,
modifications, sales of a business, accounts filed. Open data under the
Licence Ouverte 2.0, through DILA's opendatasoft portal
(``bodacc-datadila.opendatasoft.com``, dataset ``annonces-commerciales``):
keyless, refreshed about 06:15 UTC each publication day, about one second a
query, 5 million calls a day across the domain.

One reader, three callers: the History tab (``timeline/bodacc.py``), the
watchlist's daily BODACC tier (``watchlist.bodacc_tick``) and the Sirene card's
QuickCheck clause (``sources/sirene.py``). Stephen's decisions (11 Nov 2026,
the BODACC ticket) and the traps, which will be re-derived otherwise:

* **One SIREN per query.** ``registre`` lists every SIREN a notice names — the
  buyer *and* the seller of a sale — in both spellings; a batched
  ``registre in (...)`` with ``group_by`` undercounts. Query
  ``registre="<siren>"`` and read the role from the notice itself: the subject
  is in ``listepersonnes``, the seller of a sale in the previous owner /
  operator lists. A notice that only mentions the SIREN is left out
  (decision F).
* **No person, ever (decision B).** Everything is copied by allowlist into a
  reduced notice. A sole trader's name, the directors (``administration``),
  liquidators and administrators (``complementJugement``), addresses, prices
  and activity text never leave this module. A company name is kept only for a
  legal person (``typePersonne`` ``pm``): the subject's name on a change of
  name, and the buyer of a business the subject sold.
* **Personal judgments are dropped (decision G):** personal bankruptcy and
  management bans name a person, not the company.
* **Corrections are applied.** A ``rectificatif`` replaces the notice it
  names in ``parutionavisprecedent``; an ``annulation`` withdraws it. The
  earlier notice's id is rebuilt as publication letter + issue number +
  notice number (``A202301482862``). ``parution`` is an issue number, never a
  date.
* **A failure is never an empty answer.** :class:`BodaccUnavailable` carries a
  reason in the degradation vocabulary; only a successful read is cached (a
  day, per SIREN).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import date
from typing import Any

import httpx

from .cache import Cache
from .http import build_client

log = logging.getLogger(__name__)

SOURCE_ID = "bodacc"
API_URL = (
    "https://bodacc-datadila.opendatasoft.com/api/explore/v2.1/catalog/"
    "datasets/annonces-commerciales/records"
)
NOTICE_URL = "https://www.bodacc.fr/pages/annonces-commerciales-detail/?q.id=id:{id}"
#: Where a reader finds every notice naming a SIREN (the History tab's chip;
#: ``bodaccSearchUrl`` in ``lib/historyMode.ts`` is the same address).
SEARCH_URL = (
    "https://bodacc-datadila.opendatasoft.com/explore/dataset/"
    "annonces-commerciales/table/?q={siren}"
)
LICENCE = "Licence Ouverte 2.0"
ATTRIBUTION = "BODACC — Direction de l'information légale et administrative (bodacc.fr)"

PAGE_SIZE = 100
#: 1,000 notices. The most measured over 322 French LEIs was 189 (and ORPEA's
#: 190); a SIREN past the cap is read newest first and flagged ``truncated``.
MAX_PAGES = 10
CACHE_KEY = "bodacc/notices-v1/{siren}"
CACHE_DAYS = 1.0
TIMEOUT_S = 20.0
RETRIES = 2

_FIELDS = (
    "id,publicationavis,parution,dateparution,numeroannonce,typeavis,familleavis,"
    "tribunal,listepersonnes,jugement,modificationsgenerales,radiationaurcs,depot,"
    "acte,listeprecedentproprietaire,listeprecedentexploitant,parutionavisprecedent"
)

# --------------------------------------------------------------------------- #
# Kinds — the one reading of a notice, shared by History, the watchlist and
# the QuickCheck clause.
# --------------------------------------------------------------------------- #

KIND_INSOLVENCY_OPENED = "insolvency_opened"
KIND_INSOLVENCY_CLOSED = "insolvency_closed"
KIND_INSOLVENCY_PLAN = "insolvency_plan"
KIND_INSOLVENCY_OTHER = "insolvency_other"
KIND_CONCILIATION = "conciliation"
KIND_PROFESSIONAL_RECOVERY = "professional_recovery"
KIND_DEREGISTERED = "deregistered"
KIND_DISSOLUTION = "dissolution"
KIND_UNIVERSAL_TRANSFER = "universal_transfer"
KIND_NAME_CHANGE = "name_change"
KIND_SEAT_TRANSFER = "seat_transfer"
KIND_MODIFICATION = "modification"
KIND_BUSINESS_ACQUIRED = "business_acquired"
KIND_BUSINESS_SOLD = "business_sold"
KIND_ESTABLISHMENT_CREATED = "establishment_created"
KIND_REGISTERED = "registered"
KIND_ACCOUNTS_FILED = "accounts_filed"
KIND_WITHDRAWN = "notice_withdrawn"
KIND_OTHER = "other"

#: The collective-procedure kinds — the QuickCheck clause reads these.
COLLECTIVE_KINDS = frozenset({
    KIND_INSOLVENCY_OPENED, KIND_INSOLVENCY_CLOSED, KIND_INSOLVENCY_PLAN, KIND_INSOLVENCY_OTHER,
})

ROLE_SUBJECT = "subject"
ROLE_SELLER = "seller"

#: Decision G — judgments that name a person, not the company.
_PERSONAL_JUDGMENTS = ("faillite personnelle", "interdiction de gérer", "interdiction de diriger")


class BodaccUnavailable(Exception):  # noqa: N818 — the RikUnavailable shape
    """BODACC did not answer. ``reason`` is in the degradation vocabulary."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


# --------------------------------------------------------------------------- #
# Small readers
# --------------------------------------------------------------------------- #

def _digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalise_siren(value: Any) -> str | None:
    """Nine digits, or None. Spaces are the issuers' grouping (``542 051 180``)."""
    digits = _digits(value)
    return digits if len(digits) == 9 else None


def _fix_text(value: Any) -> str:
    """The text, with the UTF-8-read-as-Latin-1 mojibake some notices carry repaired."""
    text = str(value or "").strip()
    if "Ã" in text:
        try:
            return text.encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            return text
    return text


def _json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if not value:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _people(block: Any) -> list[dict[str, Any]]:
    data = _json(block)
    if not isinstance(data, dict):
        return []
    people = data.get("personne")
    if isinstance(people, dict):
        return [people]
    if isinstance(people, list):
        return [p for p in people if isinstance(p, dict)]
    return []


def _person_siren(person: dict[str, Any]) -> str | None:
    imm = person.get("numeroImmatriculation")
    if isinstance(imm, dict):
        return normalise_siren(imm.get("numeroIdentification"))
    return None


def _legal_person_name(person: dict[str, Any]) -> str | None:
    """A legal person's name — never a natural person's (decision B)."""
    if person.get("typePersonne") != "pm":
        return None
    name = _fix_text(person.get("denomination"))
    return name or None


def _iso_day(value: Any) -> str | None:
    text = str(value or "")[:10]
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


# --------------------------------------------------------------------------- #
# Classification
# --------------------------------------------------------------------------- #

def _procedure(nature: str) -> str:
    if "sauvegarde accélérée" in nature:
        return "accelerated safeguard"
    if "sauvegarde" in nature:
        return "safeguard"
    if "redressement" in nature:
        return "receivership (redressement judiciaire)"
    if "liquidation" in nature:
        return "judicial liquidation"
    if "rétablissement professionnel" in nature:
        return "professional recovery"
    return "collective proceedings"


def _cap(text: str) -> str:
    return text[:1].upper() + text[1:]


def classify_judgment(nature_raw: str) -> tuple[str, str] | None:
    """``(kind, English label)`` for a collective-procedure judgment, or None
    for a personal judgment (decision G)."""
    nature = _fix_text(nature_raw).lower()
    if any(p in nature for p in _PERSONAL_JUDGMENTS):
        return None
    proc = _procedure(nature)
    if "résolution du plan" in nature:
        return KIND_INSOLVENCY_OPENED, f"Plan cancelled; {proc} opened"
    if "conversion" in nature:
        return KIND_INSOLVENCY_OPENED, f"Converted to {proc}"
    if "extension" in nature:
        return KIND_INSOLVENCY_OPENED, f"{_cap(proc)} extended to this company"
    if "reprise" in nature:
        return KIND_INSOLVENCY_OPENED, f"{_cap(proc)} resumed"
    if "ouverture" in nature and "jugement" in nature and "créances" not in nature:
        return KIND_INSOLVENCY_OPENED, f"{_cap(proc)} opened"
    if "clôture" in nature or "mettant fin" in nature:
        if "insuffisance d'actif" in nature:
            return KIND_INSOLVENCY_CLOSED, f"{_cap(proc)} closed for insufficient assets"
        if "extinction du passif" in nature:
            return KIND_INSOLVENCY_CLOSED, f"{_cap(proc)} closed, debts settled"
        return KIND_INSOLVENCY_CLOSED, f"{_cap(proc)} closed"
    if "plan de cession" in nature:
        return KIND_INSOLVENCY_PLAN, "Sale plan adopted by the court"
    if "plan" in nature:
        if "modifiant" in nature:
            return KIND_INSOLVENCY_PLAN, f"{_cap(proc)} plan modified"
        return KIND_INSOLVENCY_PLAN, f"{_cap(proc)} plan adopted"
    if "collocation" in nature:
        return KIND_INSOLVENCY_OTHER, "Ranking of claims filed"
    if "projet de répartition" in nature and "état des créances" not in nature:
        return KIND_INSOLVENCY_OTHER, "Distribution plan filed"
    if "état des créances" in nature:
        return KIND_INSOLVENCY_OTHER, "Statement of claims filed"
    if "liste des créances" in nature:
        return KIND_INSOLVENCY_OTHER, "List of claims arising after the opening filed"
    if "cour d'appel" in nature:
        return KIND_INSOLVENCY_OTHER, "Appeal court ruling in collective proceedings"
    return KIND_INSOLVENCY_OTHER, "Other court ruling in collective proceedings"


#: Phrases of ``modificationsgenerales.descriptif`` → what changed, in English.
#: The descriptif itself is never carried (it can be free text).
_MODIFICATION_WORDS: tuple[tuple[str, str], ...] = (
    ("administration", "management"),
    ("capital", "capital"),
    ("activité", "activity"),
    ("nom commercial", "trading name"),
    ("enseigne", "sign"),
    ("forme juridique", "legal form"),
    ("cessation d'activité", "activity ceased"),
    ("date de clôture", "financial year end"),
    ("durée", "duration"),
)


def classify_modification(descriptif_raw: str) -> tuple[str, str]:
    text = _fix_text(descriptif_raw).lower()
    if "dissolution" in text:
        return KIND_DISSOLUTION, "Dissolution"
    if "transmission universelle" in text:
        return KIND_UNIVERSAL_TRANSFER, (
            "Universal transfer of assets (transmission universelle du patrimoine)"
        )
    if "dénomination" in text:
        return KIND_NAME_CHANGE, "Change of name"
    if "transfert du siège" in text or "transfert de l'établissement principal" in text:
        return KIND_SEAT_TRANSFER, "Registered office transferred"
    changed = [words for phrase, words in _MODIFICATION_WORDS if phrase in text]
    # "activité" also matches "cessation d'activité": keep the more precise one.
    if "activity ceased" in changed and "activity" in changed:
        changed.remove("activity")
    if changed:
        return KIND_MODIFICATION, _cap("Change of " + ", ".join(changed)).replace(
            "Change of activity ceased", "Activity ceased"
        )
    return KIND_MODIFICATION, "Other modification"


_SALE_WORDS: tuple[tuple[str, str], ...] = (
    ("achat d'un fonds", "business purchased"),
    ("achat d'un établissement principal", "main establishment purchased"),
    ("achat d'un établissement secondaire", "secondary establishment purchased"),
    ("autre achat", "other purchase, contribution or allotment"),
    ("location-gérance", "business let under a management lease"),
    ("apport", "business contributed"),
)


def sale_words(category_raw: str) -> str:
    text = _fix_text(category_raw).lower()
    for phrase, words in _SALE_WORDS:
        if phrase in text:
            return words
    return "sale or transfer of a business"


# --------------------------------------------------------------------------- #
# The allowlist
# --------------------------------------------------------------------------- #

def _previous_id(record: dict[str, Any]) -> str | None:
    prev = _json(record.get("parutionavisprecedent"))
    if isinstance(prev, list):
        prev = prev[0] if prev else None
    if not isinstance(prev, dict):
        return None
    pub = str(prev.get("nomPublication") or "").strip()
    letter = pub[-1:].upper() if pub[-1:].upper() in {"A", "B", "C"} else ""
    parution = _digits(prev.get("numeroParution"))
    numero = _digits(prev.get("numeroAnnonce"))
    if not (letter and parution and numero):
        return None
    return f"{letter}{parution}{int(numero)}"


def reduce_record(record: dict[str, Any], siren: str) -> dict[str, Any] | None:
    """One notice, copied by allowlist, or None when it is not about ``siren``
    as the subject or the seller, or is a personal judgment."""
    nid = str(record.get("id") or "").strip()
    published = _iso_day(record.get("dateparution"))
    if not nid or not published:
        return None
    family = str(record.get("familleavis") or "").strip()
    subject_people = _people(record.get("listepersonnes"))
    subject = next((p for p in subject_people if _person_siren(p) == siren), None)
    sellers = _people(record.get("listeprecedentproprietaire")) + _people(
        record.get("listeprecedentexploitant")
    )
    if subject is not None:
        role = ROLE_SUBJECT
    elif family == "vente" and any(_person_siren(p) == siren for p in sellers):
        role = ROLE_SELLER
    else:
        return None  # a mention only (decision F)

    notice: dict[str, Any] = {
        "id": nid,
        "url": NOTICE_URL.format(id=nid),
        "publication": str(record.get("publicationavis") or "").strip() or None,
        "published": published,
        "notice_type": str(record.get("typeavis") or "annonce").strip(),
        "family": family,
        "role": role,
        "subject_type": (subject or {}).get("typePersonne") if role == ROLE_SUBJECT else None,
        "court": _fix_text(record.get("tribunal")) or None,
        "effective": None,
        "kind": KIND_OTHER,
        "label": "Other notice",
        "detail": None,
        "counterparty": None,
        "previous_id": _previous_id(record),
    }

    if family in {"collective", "conciliation", "retablissement_professionnel"}:
        judgment = _json(record.get("jugement")) or {}
        nature = _fix_text(judgment.get("nature")) if isinstance(judgment, dict) else ""
        notice["effective"] = _iso_day(judgment.get("date")) if isinstance(judgment, dict) else None
        notice["detail"] = nature or None
        if family == "collective":
            classified = classify_judgment(nature)
            if classified is None:
                return None
            notice["kind"], notice["label"] = classified
        elif family == "conciliation":
            notice["kind"] = KIND_CONCILIATION
            notice["label"] = (
                "Conciliation agreement approved by the court"
                if "homologation" in nature.lower()
                else "Conciliation ruling"
            )
        else:
            notice["kind"] = KIND_PROFESSIONAL_RECOVERY
            notice["label"] = "Professional recovery procedure"
    elif family == "radiation":
        notice["kind"] = KIND_DEREGISTERED
        rad = _json(record.get("radiationaurcs")) or {}
        comment = _fix_text(rad.get("commentaire")).lower() if isinstance(rad, dict) else ""
        notice["label"] = (
            "Struck off by the registry (radiation d'office)"
            if "office" in comment
            else "Removed from the register (radiation)"
        )
    elif family == "modification":
        mod = _json(record.get("modificationsgenerales")) or {}
        descriptif = mod.get("descriptif") if isinstance(mod, dict) else ""
        notice["kind"], notice["label"] = classify_modification(descriptif or "")
        if notice["kind"] == KIND_NAME_CHANGE and subject is not None:
            notice["detail"] = _legal_person_name(subject)
    elif family == "vente":
        acte = _json(record.get("acte")) or {}
        vente = acte.get("vente") if isinstance(acte, dict) else None
        category = vente.get("categorieVente") if isinstance(vente, dict) else ""
        words = sale_words(category or "")
        if role == ROLE_SELLER:
            notice["kind"] = KIND_BUSINESS_SOLD
            notice["label"] = "Business sold"
            buyer = next(
                (p for p in subject_people if p.get("typePersonne") == "pm"), None
            )
            if buyer is not None:
                notice["counterparty"] = {
                    "siren": _person_siren(buyer),
                    "name": _legal_person_name(buyer),
                }
            elif subject_people:
                notice["counterparty"] = {"siren": None, "name": None}
        else:
            notice["kind"] = KIND_BUSINESS_ACQUIRED
            notice["label"] = _cap(words)
    elif family == "creation":
        notice["kind"], notice["label"] = KIND_ESTABLISHMENT_CREATED, "Establishment created"
    elif family == "immatriculation":
        notice["kind"], notice["label"] = KIND_REGISTERED, "Registered (immatriculation)"
    elif family == "dpc":
        depot = _json(record.get("depot")) or {}
        closing = _iso_day(depot.get("dateCloture")) if isinstance(depot, dict) else None
        notice["kind"] = KIND_ACCOUNTS_FILED
        notice["label"] = (
            f"Accounts filed for the year ending {closing}" if closing else "Accounts filed"
        )
        notice["effective"] = None
    return notice


def apply_corrections(notices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Replace each corrected notice by its rectificatif; an annulation
    withdraws the notice it names and stays as a ``notice_withdrawn`` row."""
    replaced: set[str] = set()
    out: list[dict[str, Any]] = []
    for n in notices:
        kind = n.get("notice_type") or "annonce"
        prev = n.get("previous_id")
        if kind == "rectificatif" and prev:
            replaced.add(prev)
            out.append({**n, "corrects": prev})
        elif kind == "annulation" and prev:
            replaced.add(prev)
            out.append({**n, "corrects": prev, "kind": KIND_WITHDRAWN,
                        "label": "Earlier notice withdrawn", "detail": None,
                        "counterparty": None})
        else:
            out.append(n)
    return [n for n in out if n["id"] not in replaced]


def reduce_page(records: list[dict[str, Any]], siren: str) -> list[dict[str, Any]]:
    return [n for r in records if (n := reduce_record(r, siren)) is not None]


def notices_payload(records: list[dict[str, Any]], siren: str, *, total: int) -> dict[str, Any]:
    notices = apply_corrections(reduce_page(records, siren))
    notices.sort(key=lambda n: (n["published"], n["id"]), reverse=True)
    return {
        "siren": siren,
        "notices": notices,
        "total": total,
        "truncated": total > len(records),
        "search_url": SEARCH_URL.format(siren=siren),
    }


# --------------------------------------------------------------------------- #
# Fetch
# --------------------------------------------------------------------------- #

def query_params(siren: str, offset: int, *, since: str | None = None) -> dict[str, Any]:
    where = f'registre="{siren}"'
    if since:
        where += f" and dateparution >= date'{since}'"
    return {
        "where": where,
        "order_by": "dateparution desc, id desc",
        "limit": PAGE_SIZE,
        "offset": offset,
        "select": _FIELDS,
    }


def _failure(exc: BaseException) -> BodaccUnavailable:
    if isinstance(exc, httpx.TimeoutException):
        return BodaccUnavailable("timeout", "BODACC did not answer in time.")
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        if code == 429:
            return BodaccUnavailable("rate_limited", "BODACC is limiting requests.")
        return BodaccUnavailable("upstream_error", f"BODACC answered HTTP {code}.")
    return BodaccUnavailable("upstream_error", "BODACC could not be reached.")


def _page_body(resp: httpx.Response) -> tuple[list[dict[str, Any]], int]:
    resp.raise_for_status()
    body = resp.json()
    if not isinstance(body, dict) or not isinstance(body.get("results"), list):
        raise BodaccUnavailable("upstream_error", "BODACC answered with an unreadable body.")
    return body["results"], int(body.get("total_count") or 0)


async def _read_async(
    client: httpx.AsyncClient, siren: str, since: str | None
) -> tuple[list[dict[str, Any]], int]:
    records: list[dict[str, Any]] = []
    total = 0
    for page in range(MAX_PAGES):
        for attempt in range(RETRIES + 1):
            try:
                resp = await client.get(
                    API_URL, params=query_params(siren, page * PAGE_SIZE, since=since),
                    timeout=TIMEOUT_S,
                )
                rows, total = _page_body(resp)
                break
            except BodaccUnavailable:
                raise
            except (httpx.TransportError, httpx.HTTPStatusError, ValueError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", 0)
                if attempt >= RETRIES or (status and status < 500 and status != 429):
                    raise _failure(exc) from exc
                await asyncio.sleep(0.5 * (attempt + 1))
        records += rows
        if len(rows) < PAGE_SIZE or len(records) >= total:
            break
    return records, total


def _read_sync(
    client: httpx.Client, siren: str, since: str | None
) -> tuple[list[dict[str, Any]], int]:
    import time

    records: list[dict[str, Any]] = []
    total = 0
    for page in range(MAX_PAGES):
        for attempt in range(RETRIES + 1):
            try:
                resp = client.get(
                    API_URL, params=query_params(siren, page * PAGE_SIZE, since=since),
                    timeout=TIMEOUT_S,
                )
                rows, total = _page_body(resp)
                break
            except BodaccUnavailable:
                raise
            except (httpx.TransportError, httpx.HTTPStatusError, ValueError) as exc:
                status = getattr(getattr(exc, "response", None), "status_code", 0)
                if attempt >= RETRIES or (status and status < 500 and status != 429):
                    raise _failure(exc) from exc
                time.sleep(0.5 * (attempt + 1))
        records += rows
        if len(rows) < PAGE_SIZE or len(records) >= total:
            break
    return records, total


async def notices_for(siren_raw: Any, *, cache: Cache | None = None) -> dict[str, Any]:
    """Every notice about ``siren`` (subject or seller), newest first.

    Raises :class:`BodaccUnavailable` when BODACC did not answer (never
    cached) and ``ValueError`` for something that is not a SIREN.
    """
    siren = normalise_siren(siren_raw)
    if siren is None:
        raise ValueError(f"{siren_raw!r} is not a SIREN")
    cache = cache or Cache()
    key = CACHE_KEY.format(siren=siren)
    hit = cache.get_payload(key, max_age_days=CACHE_DAYS)
    if hit is not None and isinstance(hit[0], dict):
        return hit[0]
    async with build_client() as client:
        records, total = await _read_async(client, siren, None)
    payload = notices_payload(records, siren, total=total)
    cache.put(key, payload)
    return payload


def recent_notices_sync(
    siren_raw: Any, *, since: str, client: httpx.Client
) -> list[dict[str, Any]]:
    """Notices about ``siren`` published on or after ``since`` (the watchlist).

    Corrections are applied within the window only; a rectificatif of an older
    notice is kept as itself. Raises :class:`BodaccUnavailable`.
    """
    siren = normalise_siren(siren_raw)
    if siren is None:
        return []
    records, total = _read_sync(client, siren, since)
    return notices_payload(records, siren, total=total)["notices"]


def latest_collective(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """The newest collective-procedure notice about the subject, or None."""
    for n in (payload or {}).get("notices") or []:
        if n.get("role") == ROLE_SUBJECT and n.get("kind") in COLLECTIVE_KINDS:
            return n
    return None


#: The keys a reduced notice may carry. Pinned by the no-personal-data test.
NOTICE_KEYS = frozenset({
    "id", "url", "publication", "published", "notice_type", "family", "role",
    "subject_type", "court", "effective", "kind", "label", "detail",
    "counterparty", "previous_id", "corrects",
})

__all__ = [
    "ATTRIBUTION",
    "BodaccUnavailable",
    "COLLECTIVE_KINDS",
    "LICENCE",
    "NOTICE_KEYS",
    "SEARCH_URL",
    "apply_corrections",
    "classify_judgment",
    "classify_modification",
    "latest_collective",
    "notices_for",
    "notices_payload",
    "recent_notices_sync",
    "reduce_record",
]
