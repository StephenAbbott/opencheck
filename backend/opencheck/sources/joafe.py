"""The Journal officiel des associations et fondations d'entreprise (JOAFE) —
the History tab's reader for a French association's notices.

DILA publishes every JOAFE notice since 1901 and every deposit of an
association's or foundation's annual accounts on its Opendatasoft portal
(dataset ``jo_associations``, Licence Ouverte; the same as the daily XML at
``echanges.dila.gouv.fr/OPENDATA/ASSOCIATIONS/``). Keyless; the portal's quota
is six million calls a day across all callers. Measured on 11 Nov 2026
(the JOAFE History ticket): 5,448,942 notices and 230,798 accounts deposits;
one entity's query answers in 0.8 s at the median, 7.9 s at p95.

This is a timeline reader, not a registered source: nothing here reaches the
lookup, the BODS or a signal. Stephen's decisions (11 Nov 2026), A–F on the
ticket, all as recommended:

* **A — the join.** A notice names the association by its RNA number; an
  accounts deposit by its SIREN. The SIREN comes from GLEIF; the RNA numbers
  from the RNA index (:func:`.rna_index.rnas_for_siren`), a local read. One
  query asks for both: ``numero_rna in (…) or dca_siren = "…"``.
* **B — addresses.** A small association's seat is often a member's home, and
  the seat lines are free text that name people ("chez M …", 386,438 notices
  match "chez"). Only the commune and postcode of a seat transfer are kept;
  the street lines, the purpose text (``objet``), the founders list and the
  whole ``contenu`` are read in memory and dropped before anything is cached
  or returned (:data:`FIELDS` is an allowlist; :func:`_reduce` builds every
  kept row from named keys only).
* **D — foundations.** A fondation d'entreprise's or fonds de dotation's
  notices are keyed on its RNF number, which no row links to a SIREN; their
  accounts carry the SIREN. So they reach History through their accounts only.
* **E — live, cached a day** under ``joafe/notices-v1/<siren>``. A failure is
  ``status: "unavailable"``, never cached, never read as "no notices".

Notices older than the RNA (2009) carry no W number and are not reached.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..secret_scrub import describe_exception
from . import rna_index

logger = logging.getLogger(__name__)

HOST = "journal-officiel-datadila.opendatasoft.com"
RECORDS_URL = f"https://{HOST}/api/explore/v2.1/catalog/datasets/jo_associations/records"
DATASET_PAGE = "https://www.data.gouv.fr/datasets/associations-joafe"
LICENCE = "Licence Ouverte / Open Licence 2.0"

#: Bump when the cached payload's shape changes.
CACHE_VERSION = "v1"
_CACHE_NS = "joafe"

#: One page; the portal's maximum. Measured notices per entity: p50 1, p95 22,
#: max 24 — a second page is rare, and :data:`MAX_PAGES` bounds it.
PAGE_SIZE = 100
MAX_PAGES = 5

#: More than this many RNA numbers for one SIREN is not a join, it is noise.
MAX_RNAS = 10

#: What is asked of the portal. ``contenu`` is asked for and never kept: the
#: few facts read from it (a seat transfer's commune and postcode, whether the
#: purpose changed) are taken by :func:`_reduce` and the rest dropped.
FIELDS: tuple[str, ...] = (
    "id", "source", "dateparution", "datedeclaration", "annonce_type_facette",
    "etatavis", "reference_avis_initial", "numero_rna", "dca_siren",
    "dca_datecloture", "dca_datevalidation", "dca_rectificatif_version",
    "titre", "titre_ancien", "titre_nouveau", "parution_numero",
    "numero_annonce", "contenu",
)

#: The keys a reduced row may carry — and nothing else (pinned by a test).
ROW_KEYS: frozenset[str] = frozenset({
    "id", "source", "form", "kind", "state", "published", "declared",
    "rna", "siren", "accounts_period_end", "accounts_validated",
    "accounts_version", "title", "title_old", "title_new", "seat_new",
    "seat_old", "purpose_changed", "corrects", "number",
})

_SIREN = re.compile(r"^\d{9}$")


class JoafeUnavailable(Exception):
    """The portal did not answer usably."""


def _day(value: Any) -> str | None:
    text = str(value or "").strip()[:10]
    return text if re.match(r"^\d{4}-\d{2}-\d{2}$", text) else None


def _seat(block: Any) -> dict[str, str] | None:
    """A seat's commune and postcode — never a street line, a building, a
    "chez …" line or a country. ``None`` when neither is given."""
    if not isinstance(block, dict):
        return None
    commune = str(block.get("commune") or block.get("ville") or "").strip()
    postcode = str(block.get("codePostal") or "").strip()
    if not re.match(r"^\d{5}$", postcode):
        postcode = ""
    if not commune and not postcode:
        return None
    return {k: v for k, v in (("commune", commune), ("postcode", postcode)) if v}


def _address_key(block: Any) -> tuple[str, ...]:
    """The whole seat address, folded, for one comparison in memory: a
    modification notice repeats the seat in ``transfereNouvelleAdresse`` when
    the seat did not move (a rectified CARPA notice, 10 Nov 2026), and only
    the street can tell a move within one commune from none. Never kept."""
    if not isinstance(block, dict):
        return ()
    # Sorted values, not keys: older notices name the commune ``ville`` on one
    # side and ``commune`` on the other.
    return tuple(sorted(
        " ".join(str(v).split()).casefold()
        for k, v in block.items()
        if k != "pays" and isinstance(v, (str, int)) and str(v).strip()
    ))


def _corrects(value: Any) -> dict[str, str] | None:
    """The notice a rectification or cancellation names: its publication day
    and its number in that issue (the issue number is written two ways)."""
    try:
        ref = json.loads(value) if isinstance(value, str) else value
    except ValueError:
        return None
    if not isinstance(ref, dict):
        return None
    day, number = _day(ref.get("dateParution")), str(ref.get("numAnnonce") or "").strip()
    return {"published": day, "number": number} if day and number else None


def _reduce(raw: dict[str, Any]) -> dict[str, Any]:
    """One portal row → the facts History uses, built from named keys only."""
    facette = str(raw.get("annonce_type_facette") or "")
    parts = facette.split("/")
    form, kind, state = (parts + ["", "", ""])[:3] if len(parts) >= 3 else ("", "", facette)
    row: dict[str, Any] = {
        "id": str(raw.get("id") or ""),
        "source": raw.get("source"),
        "form": form or None,
        "kind": kind or None,
        "state": state or raw.get("etatavis") or None,
        "published": _day(raw.get("dateparution")),
        "declared": _day(raw.get("datedeclaration")),
        "rna": raw.get("numero_rna") or None,
        "siren": raw.get("dca_siren") or None,
        "title": (raw.get("titre") or "").strip() or None,
        "title_old": (raw.get("titre_ancien") or "").strip() or None,
        "title_new": (raw.get("titre_nouveau") or "").strip() or None,
    }
    if raw.get("numero_annonce") is not None:
        row["number"] = str(raw["numero_annonce"])
    if raw.get("source") == "dca":
        row["accounts_period_end"] = _day(raw.get("dca_datecloture"))
        row["accounts_validated"] = _day(raw.get("dca_datevalidation"))
        version = raw.get("dca_rectificatif_version")
        row["accounts_version"] = int(version) if isinstance(version, int) else 0
    corrects = _corrects(raw.get("reference_avis_initial"))
    if corrects:
        row["corrects"] = corrects
    content = raw.get("contenu")
    if isinstance(content, str) and content:
        try:
            content = json.loads(content)
        except ValueError:
            content = None
    if isinstance(content, dict):
        body = next((v for v in content.values() if isinstance(v, dict)), {})
        modification = body.get("modification") if isinstance(body, dict) else None
        if isinstance(modification, dict):
            moved = None
            if _address_key(modification.get("transfereNouvelleAdresse")) != _address_key(
                modification.get("siegeSocial")
            ):
                moved = _seat(modification.get("transfereNouvelleAdresse"))
            if moved:
                row["seat_new"] = moved
                old = _seat(modification.get("siegeSocial"))
                if old:
                    row["seat_old"] = old
            if str(modification.get("nouvelObjet") or "").strip():
                row["purpose_changed"] = True
            if not row["title_new"] and str(modification.get("nouveauTitre") or "").strip():
                row["title_new"] = str(modification["nouveauTitre"]).strip()
                row["title_old"] = row["title_old"] or (
                    str(modification.get("ancienTitre") or "").strip() or None
                )
    reduced = {k: v for k, v in row.items() if v is not None}
    stray = set(reduced) - ROW_KEYS
    if stray:  # pragma: no cover - built from named keys above
        raise ValueError(f"joafe row carries unexpected keys: {sorted(stray)}")
    return reduced


def where_clause(siren: str, rnas: list[str]) -> str:
    """The portal query for one SIREN and its RNA numbers."""
    conds = [f'dca_siren="{siren}"']
    if rnas:
        conds.append("numero_rna in (" + ", ".join(f'"{r}"' for r in rnas) + ")")
    return " or ".join(conds)


async def _query(where: str) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    truncated = False
    async with build_client() as client:
        offset = 0
        for _ in range(MAX_PAGES):
            resp = await client.get(
                RECORDS_URL,
                params={
                    "select": ",".join(FIELDS),
                    "where": where,
                    "order_by": "dateparution",
                    "limit": PAGE_SIZE,
                    "offset": offset,
                },
                timeout=30.0,
            )
            if resp.status_code != 200:
                raise JoafeUnavailable(f"HTTP {resp.status_code}")
            try:
                body = resp.json()
            except ValueError as exc:
                raise JoafeUnavailable("unreadable body") from exc
            if not isinstance(body, dict) or not isinstance(body.get("results"), list):
                raise JoafeUnavailable("unexpected body")
            page = body["results"]
            rows.extend(_reduce(r) for r in page if isinstance(r, dict))
            total = int(body.get("total_count") or 0)
            offset += len(page)
            if not page or offset >= total:
                break
        else:
            truncated = True
    return rows, truncated


async def notices(siren: str) -> dict[str, Any]:
    """Every JOAFE notice and accounts deposit for one SIREN.

    ``status``: ``read`` (``rows`` is the answer, possibly empty), or
    ``unavailable`` (``reason``; nothing was cached). ``rna_status``: ``read``
    (``rnas`` from the index), ``no_index`` (accounts only — the notices of
    the association could not be keyed) or ``error``.
    """
    siren = str(siren or "").strip()
    if not _SIREN.match(siren):
        raise ValueError(f"not a SIREN: {siren!r}")
    cache = Cache()
    key = f"{_CACHE_NS}/notices-{CACHE_VERSION}/{siren}"
    held = cache.get_payload(key, max_age_days=1)
    if held is not None and isinstance(held[0], dict):
        return held[0]
    if not get_settings().allow_live:
        return {"status": "unavailable", "reason": "not_configured", "siren": siren}

    try:
        rnas = rna_index.rnas_for_siren(siren)
        rna_status = "read" if rnas is not None else "no_index"
    except Exception as exc:  # noqa: BLE001 - a broken index is said, not raised
        logger.warning("joafe: RNA index read failed for %s: %s", siren, describe_exception(exc))
        rnas, rna_status = None, "error"
    rnas = (rnas or [])[:MAX_RNAS]

    try:
        rows, truncated = await _query(where_clause(siren, rnas))
    except (httpx.HTTPError, OSError, JoafeUnavailable, ValueError) as exc:
        logger.warning("joafe: query failed for %s: %s", siren, describe_exception(exc))
        reason = "timeout" if isinstance(exc, httpx.TimeoutException) else "upstream_error"
        return {"status": "unavailable", "reason": reason, "siren": siren, "rnas": rnas}

    payload = {
        "status": "read",
        "siren": siren,
        "rnas": rnas,
        "rna_status": rna_status,
        "rows": rows,
        "truncated": truncated,
    }
    if rna_status == "read":
        # Without the index the answer is accounts only, and a later call
        # with the index must not be served this one.
        cache.put(key, payload)
    return payload


__all__ = [
    "DATASET_PAGE",
    "FIELDS",
    "LICENCE",
    "RECORDS_URL",
    "ROW_KEYS",
    "JoafeUnavailable",
    "notices",
    "where_clause",
]
