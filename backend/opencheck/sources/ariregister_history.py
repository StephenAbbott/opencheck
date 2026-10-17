"""Ended ownership from the e-Business Register's own history.

Every one of RIK's daily open-data files holds current rows only (zero
``lopp_kpv`` across all ten, measured 16 Oct 2026). The history is in the
credentialed SOAP service the Time Machine already reads —
``detailandmed_v2`` with ``ainult_kehtivad=0`` (every row the registry card
has ever carried) and ``tegelikudKasusaajad_v2`` (the beneficial-owner
history). This module turns those two responses into the **ended**
shareholdings and beneficial-owner entries the ownership graph draws.

What it keeps and what it never lets out:

* **Ended rows only.** Every current row in the history is already in the
  index: its ``kirje_id`` is the bulk row's (Bolt Technology OÜ 183/183
  shareholders, Eesti Energia AS 11/11 beneficial owners, 16 Oct 2026).
* **Shareholders and beneficial owners only.** Officers and supervisory
  boards stay serving-only (Phase 317); founders are the off-card file's
  (the off-card phase).
* **Personal identification codes never leave this module.** A SOAP row
  carries the 11-digit code, not the files' ``isikukood_hash``. The code is
  used here to join an ended row to the same person's *current* row
  (``kirje_id`` → the index row's hash), so a holder who is still on any
  list is the same node; and to group a former-only person's rows. It is
  then dropped. Nothing derived from it is published — a hash of a code is
  reversible by enumeration, so a former-only person is keyed by their
  first ``kirje_id``, never by their code.
* **A failure is never cached** (Phase 262). A successful read is cached per
  registry code for a day, after stripping, so no code reaches the disk.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

from .. import degradation
from ..cache import Cache

logger = logging.getLogger(__name__)

#: Shareholder roles a history row can carry: an OÜ shareholder off the card
#: (``O``, the ``osanikud`` list), an AS shareholder the register records
#: (``S``, the off-card file), and the card's own shareholder role
#: (``OSAN``, on the card since 1 Sept 2023).
SHAREHOLDER_ROLES = frozenset({"O", "S", "OSAN"})

#: How long a successful history read is kept, per registry code.
CACHE_MAX_AGE_DAYS = 1.0
_CACHE_KEY = "ariregister/history-v1/{code}"

#: Why the history was not read — the degradation reason vocabulary where
#: one applies.
NOT_READ_OFFLINE = "offline"
NOT_READ_NO_CREDENTIALS = "no_credentials"
NOT_READ_FAILED = "upstream_error"
#: The contract's one-query slot was busy, or its daily allowance spent
#: (``ariregister_rest``): nothing was sent.
NOT_READ_LIMITED = "rate_limited"

#: How an ended row's party was identified, said on the record.
JOIN_REGISTER = "register"        # the same person's current row, by the register's own record
JOIN_NAME_BIRTH = "name_birth"    # a current row with the same name and date of birth
JOIN_FORMER = "former"            # no current row: a former-only party
JOIN_ROW = "row"                  # no code and no birth date: the row on its own

_PERSONAL_CODE = re.compile(r"^[1-6]\d{10}$")

#: Fields a history row keeps. Everything else (street-level addresses,
#: the card's region and number, the codes) stays here.
_KEEP_SHAREHOLDER = (
    "kirje_id", "isiku_tyyp", "isiku_roll", "isiku_roll_tekstina", "eesnimi",
    "nimi_arinimi", "isikukood_registrikood", "valis_kood", "valis_kood_riik",
    "valis_kood_riik_tekstina", "synniaeg", "aadress_riik", "aadress_riik_tekstina",
    "osaluse_protsent", "osaluse_suurus", "osaluse_valuuta", "osaluse_omandiliik",
    "osaluse_omandiliik_tekstina", "kontrolli_allikas", "kontrolli_allikas_tekstina",
    "kontrolli_allika_kpv", "algus_kpv", "lopp_kpv",
)
_KEEP_BO = (
    "kirje_id", "eesnimi", "nimi", "valis_kood_riik", "valis_kood_riik_tekstina",
    "synniaeg", "aadress_riik", "aadress_riik_tekstina", "kontrolli_teostamise_viis",
    "kontrolli_teostamise_viis_tekstina", "lahknevusteade_esitatud", "lopetamise_liik",
    "algus_kpv", "lopp_kpv",
)


def _ln(el: ET.Element) -> str:
    return el.tag.rsplit("}", 1)[-1]


def _rows(root: ET.Element | None, list_name: str, item_tag: str = "item") -> list[dict[str, str]]:
    """The children of the first ``list_name`` block, as text dicts."""
    if root is None:
        return []
    for el in root.iter():
        if _ln(el) == list_name:
            return [
                {_ln(f): (f.text or "").strip() for f in item if (f.text or "").strip()}
                for item in el if _ln(item) == item_tag
            ]
    return []


def _parse(xml_text: str | None) -> ET.Element | None:
    if not xml_text:
        return None
    try:
        return ET.fromstring(xml_text)
    except ET.ParseError:
        return None


def _day(raw: str | None) -> str:
    """``YYYY-MM-DD`` from the SOAP form ``YYYY-MM-DDZ`` (or the bulk
    files' ``DD.MM.YYYY``); '' when absent."""
    text = (raw or "").strip()
    m = re.match(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})$", text)
    return f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}" if m else ""


def _party_code(row: dict[str, str], code_field: str) -> str:
    """The key that names one party across rows of the same response: a
    personal code, a registry code, or a foreign code with its country.
    Used for grouping and joining here only — never returned."""
    code = (row.get(code_field) or "").strip()
    if code:
        return code
    foreign = (row.get("valis_kood") or "").strip()
    if foreign:
        return f"V:{(row.get('valis_kood_riik') or '').strip().upper()}:{foreign}"
    return ""


def _name_birth(row: dict[str, str], surname_field: str) -> str:
    born = _day(row.get("synniaeg"))
    if not born:
        return ""
    name = " ".join(
        (row.get("eesnimi") or "").split() + (row.get(surname_field) or "").split()
    ).casefold()
    return f"{name}|{born}" if name else ""


def _index_people(record: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    """From the index record: ``kirje_id`` → hash over every list, and
    name + birth date → hash for the rows that give a birth date."""
    by_kirje: dict[str, str] = {}
    by_name_birth: dict[str, str] = {}
    for key, surname in (
        ("shareholders", "nimi_arinimi"), ("officers", "nimi_arinimi"),
        ("off_card_persons", "nimi_arinimi"), ("beneficial_owners", "nimi"),
    ):
        for row in record.get(key) or []:
            h = row.get("isikukood_hash")
            if not h:
                continue
            kirje = str(row.get("kirje_id") or "")
            if kirje:
                by_kirje[kirje] = h
            nb = _name_birth({k: str(v) for k, v in row.items() if v is not None}, surname)
            if nb:
                by_name_birth.setdefault(nb, h)
    return by_kirje, by_name_birth


def history_from_responses(
    detail_xml: str | None, bo_xml: str | None, record: dict[str, Any],
) -> dict[str, Any]:
    """The ended shareholdings and beneficial-owner entries in two SOAP
    responses, joined to the index record's people and stripped of every
    personal code. ``record`` is ``entity_record(code)``."""
    detail = _parse(detail_xml)
    bo_root = _parse(bo_xml)
    by_kirje, by_name_birth = _index_people(record)

    card_rows = _rows(detail, "kaardile_kantud_isikud") + _rows(detail, "kaardivalised_isikud")
    bo_rows = _rows(bo_root, "kasusaajad", "kasusaaja")

    # A party's code → the hash of its current row, from every current row
    # in either response whose kirje_id the index holds.
    code_to_hash: dict[str, str] = {}
    for row, field in [(r, "isikukood_registrikood") for r in card_rows] + [(r, "isikukood") for r in bo_rows]:
        if _day(row.get("lopp_kpv")):
            continue
        h = by_kirje.get(row.get("kirje_id") or "")
        code = _party_code(row, field)
        if h and code:
            code_to_hash.setdefault(code, h)

    former_groups: dict[str, str] = {}   # party code → its first kirje_id

    def identify(row: dict[str, str], field: str, surname: str) -> dict[str, str]:
        """The identity fields a stripped row carries in place of its code."""
        if (row.get("isiku_tyyp") or "F") == "J":
            return {}
        code = _party_code(row, field)
        if code and code in code_to_hash:
            return {"isikukood_hash": code_to_hash[code], "history_join": JOIN_REGISTER}
        nb = _name_birth(row, surname)
        if nb and nb in by_name_birth:
            return {"isikukood_hash": by_name_birth[nb], "history_join": JOIN_NAME_BIRTH}
        group = code or nb
        if group:
            first = former_groups.setdefault(group, row.get("kirje_id") or "")
            return {"history_key": f"hist-{first}", "history_join": JOIN_FORMER}
        return {"history_key": f"hist-{row.get('kirje_id') or ''}", "history_join": JOIN_ROW}

    shareholders: list[dict[str, Any]] = []
    for row in card_rows:
        if (row.get("isiku_roll") or "") not in SHAREHOLDER_ROLES or not _day(row.get("lopp_kpv")):
            continue
        out: dict[str, Any] = {k: row[k] for k in _KEEP_SHAREHOLDER if k in row}
        out.update(identify(row, "isikukood_registrikood", "nimi_arinimi"))
        if (row.get("isiku_tyyp") or "F") != "J":
            # A natural person's codes are used above and dropped here: the
            # Estonian personal code, and a foreign one (a Lithuanian
            # asmens kodas is 11 digits too). The country stays, as the
            # bulk files publish it. A legal person's codes are register
            # numbers and stay — a foreign one is the cross-border hop.
            out.pop("isikukood_registrikood", None)
            out.pop("valis_kood", None)
        for d in ("algus_kpv", "lopp_kpv", "kontrolli_allika_kpv", "synniaeg"):
            if out.get(d):
                out[d] = _day(out[d])
        shareholders.append(out)

    owners: list[dict[str, Any]] = []
    for row in bo_rows:
        if not _day(row.get("lopp_kpv")):
            continue
        out = {k: row[k] for k in _KEEP_BO if k in row}
        out.update(identify(row, "isikukood", "nimi"))
        for d in ("algus_kpv", "lopp_kpv", "synniaeg"):
            if out.get(d):
                out[d] = _day(out[d])
        out["lahknevusteade_esitatud"] = row.get("lahknevusteade_esitatud") == "true"
        owners.append(out)

    entries = [
        {"kande_nr": e.get("kande_nr"), "kande_kpv": _day(e.get("kande_kpv")), "kandeliik": e.get("kandeliik")}
        for e in _rows(detail, "kanded")
    ]
    return {
        "read": True,
        "reason": None,
        "former_shareholders": shareholders,
        "former_beneficial_owners": owners,
        "card_entries": entries,
    }


def not_read(reason: str) -> dict[str, Any]:
    return {
        "read": False,
        "reason": reason,
        "former_shareholders": [],
        "former_beneficial_owners": [],
        "card_entries": [],
    }


_NOT_READ_DETAIL = {
    NOT_READ_NO_CREDENTIALS: "no open-data API credentials are configured",
    NOT_READ_FAILED: "the register's history service did not answer",
    NOT_READ_LIMITED: "the register's query allowance was not available",
}


async def read_history(adapter: Any, registry_code: str, record: dict[str, Any]) -> dict[str, Any]:
    """The history for one company: from the day's cache, else the two SOAP
    calls ``fetch_timeline_data`` makes. Records a ``source_read``
    degradation when it cannot be read, so the graph being current-only is
    said rather than silent; an offline instance (``allow_live`` off) reads
    nothing and says nothing, as every live source does."""
    from ..config import get_settings

    settings = get_settings()
    if not settings.allow_live:
        return not_read(NOT_READ_OFFLINE)
    cache = Cache()
    key = _CACHE_KEY.format(code=registry_code)
    hit = cache.get_payload(key, max_age_days=CACHE_MAX_AGE_DAYS)
    if hit is not None and isinstance(hit[0], dict) and hit[0].get("read"):
        return hit[0]

    unavailable: dict[str, str] = {}
    if not (settings.ariregister_username and settings.ariregister_password):
        reason = NOT_READ_NO_CREDENTIALS
        data = None
    else:
        reason = NOT_READ_FAILED
        try:
            data = await adapter.fetch_timeline_data(registry_code)
        except Exception as exc:  # noqa: BLE001 — a history failure never fails the lookup
            logger.warning("ariregister: history read failed for %s: %s", registry_code, type(exc).__name__)
            data = None
        if data:
            unavailable = dict(data.get("unavailable") or {})
    if not data or not data.get("detail_xml"):
        cause = unavailable.get("detail")
        if cause in _LIMITED_CAUSES:
            reason = NOT_READ_LIMITED
        degradation.record(
            "ariregister",
            "The registry-card history was not read "
            f"({_why(reason, cause)}); former shareholders and former "
            "beneficial owners are not shown.",
            reason=_REASON[reason] if cause is None else _rest_reason(cause),
            check=degradation.CHECK_SOURCE_READ,
        )
        return not_read(reason)
    history = history_from_responses(data.get("detail_xml"), data.get("bo_xml"), record)
    if not data.get("bo_xml"):
        # The card history answered and the BO history did not: the ended
        # holdings are shown, the former beneficial owners are not.
        history["bo_read"] = False
        cause = unavailable.get("bo")
        degradation.record(
            "ariregister",
            "The beneficial-owner history was not read "
            f"({_why(NOT_READ_FAILED, cause)}); former beneficial owners are not shown.",
            reason=_rest_reason(cause) if cause else degradation.REASON_UPSTREAM_ERROR,
            check=degradation.CHECK_SOURCE_READ,
        )
    else:
        cache.put(key, history)
    return history


#: The ``ariregister_rest`` causes for which nothing was sent.
_LIMITED_CAUSES = frozenset({"busy", "budget"})

_REASON = {
    NOT_READ_NO_CREDENTIALS: degradation.REASON_NOT_CONFIGURED,
    NOT_READ_FAILED: degradation.REASON_UPSTREAM_ERROR,
    NOT_READ_LIMITED: degradation.REASON_RATE_LIMITED,
}


def _why(reason: str, cause: str | None) -> str:
    """The bracketed clause: the shared client's fixed sentence when it
    said why, else the history module's own."""
    if cause:
        from . import ariregister_rest as rik

        return rik.DETAILS.get(cause, _NOT_READ_DETAIL[NOT_READ_FAILED])
    return _NOT_READ_DETAIL[reason]


def _rest_reason(cause: str) -> str:
    from . import ariregister_rest as rik

    return rik.RikUnavailable(cause).reason if cause in rik.DETAILS else degradation.REASON_UPSTREAM_ERROR


__all__ = [
    "CACHE_MAX_AGE_DAYS", "JOIN_FORMER", "JOIN_NAME_BIRTH", "JOIN_REGISTER", "JOIN_ROW",
    "NOT_READ_FAILED", "NOT_READ_LIMITED", "NOT_READ_NO_CREDENTIALS", "NOT_READ_OFFLINE", "SHAREHOLDER_ROLES",
    "history_from_responses", "not_read", "read_history",
]
