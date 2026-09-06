"""EITI Company Assessment adapter — ESG category.

This is a **third** EITI product, distinct from the two OpenCheck already has.
``eiti`` targets the EITI API and surfaces company payments to governments;
``eiti_soe`` targets the state-owned-enterprise roster. This adapter targets the
**Company Assessment**, which arrived with the new EITI global database
(``eiti-database.eiti.org``, 2026) and was absent from the SOE database
entirely.

The Company Assessment evaluates EITI *supporting companies* — the ~99
BHP/Anglo/Shell-class multinationals that fund and endorse the EITI — against
nine expectations. Two of them are the reason this adapter exists:

* **Expectation 6 — "Company disclose beneficial ownership."** A per-company,
  per-year result, with links to the disclosure where EITI captured them. This
  is the closest thing to a beneficial ownership signal EITI publishes.
* **Expectation 2 — "Company publish a list of controlled subsidiaries."**
  Plus the declared list itself: 1,230 parent→child rows across 51 EITI
  implementing countries.

What this source is *not*
-------------------------
It is **not beneficial ownership data**. Expectation 6 records a company's
*disclosure posture* — whether it says it discloses, and where — not who owns
it. The distinction matters and is carried through every layer here: no risk
signal, no person statements, no ownership relationships. The advocacy point
this data supports is that these companies have committed to disclosing
beneficial ownership and overwhelmingly do it as a PDF in an annual report
rather than as structured data, which is a weaker form of transparency than the
commitment implies.

The declared subsidiaries are **names and countries only**. EITI publishes no
identifier for them, no percentage and no share class.

.. important::
   **The declared subsidiary names must never enter the BODS graph** — no
   entity statements, no relationship statements, no nodes, no edges. They are
   evidence rendered on a card. Matching 1,230 free-text names to companies is
   a different and much harder problem than the 99 parents, and asserting an
   unmatched name as a graph node would claim an identity OpenCheck has not
   established. ``map_eiti_assessment`` emits one entity statement for the
   subject and nothing else; ``tests/test_eiti_assessment.py`` pins that.

Offline by design
-----------------
Identity resolution happens **once, offline**, in
``scripts/build_eiti_assessment_index.py``, which resolves the supporting
companies to LEIs and refuses to write an index until a human has reviewed
every match. At runtime this adapter loads the committed, LEI-keyed artifact
and answers ``fetch_by_lei`` as a dict lookup — no network on the hot path, and
no live-enrichment path at all.

The review gate is not ceremony. A name matcher over multinational-enterprise
names produces confident nonsense: "Anglo American" ranks the ANGLO AMERICAN
FOUNDATION first, "Equinor" once matched a company sports club, and stripping
legal-form suffixes makes a group head exactly equal to its own subsidiaries
("Glencore" → GLENCORE AG). Review caught a match on TECK GmbH, an unrelated
German company, that had passed as an exact name match.

Identifier corroboration
------------------------
This adapter asserts **no identifiers at all**, which is deliberate:

* The **LEI is derived** by OpenCheck at build time, so the corroboration rule
  in ``CLAUDE.md`` bars asserting it.
* EITI's ``legal_entity_id`` column exists but is populated for **3 companies
  of 10,116**, so it is not a usable source of LEIs.
* EITI's ``eiti_id_company`` is a **UUIDv5 over a name-derived key** — a
  deduplication key that EITI regenerated wholesale (v4 → v5) when it launched
  this database, not a registry number. Publishing it as an identifier would
  invite consumers to store something EITI can legitimately change.

An empty identifier set is the honest outcome here, not an oversight.

Licence: EITI content-use policy — free republication with credit to
"EITI International Secretariat, eiti.org".
"""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .. import provenance
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo

log = logging.getLogger(__name__)

#: Committed, LEI-keyed index (built by scripts/build_eiti_assessment_index.py).
#: Overridable via env for tests / alternative snapshots.
_INDEX_PATH = Path(
    os.environ.get("EITI_ASSESSMENT_INDEX_PATH", "")
    or (Path(__file__).resolve().parent.parent / "data" / "eiti_assessment_index.json.gz")
)

#: Lazy module-level singleton (LEI -> record). Tests may set this directly.
_index: dict[str, dict[str, Any]] | None = None
#: Upstream extract date of the committed index (``meta.source_harvest``) —
#: when EITI's data was actually read, not when git wrote the file here.
_source_snapshot: str | None = None

#: BODS shorthand for the expectation that carries the beneficial ownership
#: assessment. Named once so the frontend, the mapper and the findings template
#: cannot drift apart on a string literal.
BO_EXPECTATION = "exp_6"
#: The controlled-subsidiaries expectation.
SUBSIDIARY_EXPECTATION = "exp_2"


def _get_index() -> dict[str, dict[str, Any]]:
    """Load the committed LEI-keyed index (cached in a module singleton)."""
    global _index, _source_snapshot
    if _index is None:
        try:
            with gzip.open(_INDEX_PATH, "rt", encoding="utf-8") as f:
                data = json.load(f)
            raw = data.get("index") or {}
            _index = {
                str(k).strip().upper(): v
                for k, v in raw.items()
                if len(str(k).strip()) == 20
            }
            meta = data.get("meta") or {}
            _source_snapshot = meta.get("source_harvest") or meta.get("built")
            log.info(
                "EITI Company Assessment index loaded: %s of %s supporting "
                "companies resolved to an LEI, %s declared subsidiaries (%s harvest)",
                meta.get("parents_accepted", len(_index)),
                meta.get("parents_total"),
                meta.get("subsidiaries_indexed"),
                meta.get("source_harvest"),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("EITI Company Assessment index unavailable: %s", exc)
            _index = {}
    return _index


def _snapshot_dt() -> datetime | None:
    """The index's upstream harvest date as a datetime, or None."""
    if not _source_snapshot:
        return None
    try:
        dt = datetime.fromisoformat(str(_source_snapshot).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _load_and_declare() -> dict[str, dict[str, Any]]:
    """Load the index and declare the payload as curated, never live.

    Without this the adapter records nothing on a match and the bundle would
    resolve to 'stub', under-claiming: the data is real, it is simply a
    snapshot. The declared date is EITI's harvest date, not the file's mtime —
    an mtime only records when git wrote the file to this machine, which says
    nothing about how current the data is.
    """
    index = _get_index()
    provenance.record_curated(
        "EITI Company Assessment snapshot committed to the repository",
        harvested_at=_snapshot_dt(),
    )
    return index


def _reset_index_for_tests() -> None:
    """Test helper — drop the cached singleton so a fresh index is loaded."""
    global _index, _source_snapshot
    _index = None
    _source_snapshot = None


class EitiAssessmentAdapter(SourceAdapter):
    """EITI Company Assessment adapter — ESG category."""

    id = "eiti_assessment"

    #: LEI-keyed source, dispatched directly in routers/lookup.py alongside the
    #: other LEI-keyed offline sources (eiti_soe, eiti_bo, cac_nigeria), not via
    #: an RA-code deriver. No network on the hot path, so the budget is small.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="EITI Company Assessment",
            homepage="https://eiti-database.eiti.org/",
            description=(
                "EITI's assessment of its supporting companies against nine "
                "expectations, including whether the company discloses its "
                "beneficial owners and publishes a list of controlled "
                "subsidiaries — plus the declared subsidiary list itself. "
                "Records a company's disclosure posture, not its ownership. "
                "Snapshot of the EITI global database, resolved to LEIs "
                "offline and reviewed by hand."
            ),
            license="EITI content-use policy (free reuse with attribution)",
            attribution="EITI International Secretariat, eiti.org",
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=bool(_get_index()),
            category="esg",
            is_national_register=False,
            country=None,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # Identifier-keyed (LEI) source; free-text search intentionally empty.
        return []

    def covers_lei(self, lei: str) -> bool:
        """Whether the committed assessment snapshot holds this LEI.

        The lookup pipeline asks before dispatching, so a company this file
        cannot describe is never announced as a source being queried and never
        counted in "N of N sources answered". The set is EITI's supporting
        companies — a few dozen multinationals — not every extractive company,
        so this governs whether the source is *applicable* and says nothing
        about the company.

        Reads the index without declaring provenance: nothing has been fetched.
        """
        return (lei or "").strip().upper() in _get_index()

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """Return the assessment bundle for a LEI, or ``None`` when absent."""
        lei_norm = (lei or "").strip().upper()
        index = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return None
        return self._build_bundle(lei_norm, record)

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        lei_norm = (hit_id or "").strip().upper()
        index = await asyncio.to_thread(_load_and_declare)
        record = index.get(lei_norm)
        if record is None:
            return {"source_id": self.id, "hit_id": hit_id, "is_stub": True}
        return self._build_bundle(lei_norm, record)

    def _build_bundle(self, lei: str, record: dict[str, Any]) -> dict[str, Any]:
        # Corroboration rule: assert NOTHING. The LEI is OpenCheck-derived, and
        # neither identifier EITI publishes is usable as one — see the module
        # docstring. `eiti_published` below carries them for display only.
        return {
            "source_id": self.id,
            "hit_id": lei,
            "lei": lei,
            "is_stub": False,
            "identifiers": {},
            "name": record.get("name") or "",
            "hq_country": record.get("hq_country"),
            "hq_city": record.get("hq_city"),
            "sectors": record.get("sectors") or [],
            "company_type": record.get("company_type"),
            "business_activity": record.get("business_activity"),
            "eiti_published": record.get("eiti_published") or {},
            "match": record.get("match") or {},
            "assessments": record.get("assessments") or {},
            "subsidiaries": record.get("subsidiaries") or [],
            "source_snapshot": _source_snapshot,
        }


# --------------------------------------------------------------------------
# Reading helpers — shared by the hit builder, the mapper and the findings
# template so the three cannot disagree about what the data says.
# --------------------------------------------------------------------------


def latest_year(bundle: dict[str, Any]) -> str | None:
    """The most recent assessment year present, as a string, or None."""
    years = [y for y in (bundle.get("assessments") or {}) if str(y).strip()]
    return max(years, key=lambda y: (len(y), y)) if years else None


def expectation(bundle: dict[str, Any], shorthand: str, year: str | None = None):
    """One expectation's fields for *year* (default: the latest year)."""
    year = year or latest_year(bundle)
    if not year:
        return None
    return ((bundle.get("assessments") or {}).get(year) or {}).get(shorthand)


def bo_disclosure_result(bundle: dict[str, Any], year: str | None = None) -> str | None:
    """EITI's verbatim expectation-6 result, e.g. "Expectation met".

    Returned as EITI wrote it. "Not available" means EITI did not assess, which
    is not a finding against the company, and the frontend must not render it
    in a risk tone — see ``RISK`` note in the ESG card.
    """
    exp = expectation(bundle, BO_EXPECTATION, year)
    return (exp or {}).get("result") or None


def bo_disclosure_url(bundle: dict[str, Any], year: str | None = None) -> str | None:
    """The best available link to the company's beneficial ownership disclosure.

    EITI populated these for the 2023 cohort and **not** for 2025, so a lookup
    on the latest year alone silently shows nothing for most companies. This
    falls back through earlier years rather than pretending no disclosure link
    exists; the caller should say which year the link came from.
    """
    years = sorted((bundle.get("assessments") or {}), reverse=True)
    if year:
        years = [year]
    for y in years:
        exp = ((bundle.get("assessments") or {}).get(y) or {}).get(BO_EXPECTATION) or {}
        for field in ("bo_url", "bo_disclosure_url", "url"):
            value = (exp.get(field) or "").strip()
            if value:
                return value
    return None
