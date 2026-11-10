"""ESTMA — payments to governments by extractive companies (Canada), ESG.

The Extractive Sector Transparency Measures Act requires extractive companies
active in Canada to report, each year, the payments they make to governments
in Canada and abroad — taxes, royalties, fees, production entitlements,
bonuses, dividends and infrastructure payments — by payee and by project.
Natural Resources Canada collects the reports; **Statistics Canada**
republishes every one as a single keyless SDMX-CSV, rebuilt weekly
(dataflow ``STC:DF_ESTMA(1.0)``). That file is what this adapter reads.

An **index-tier, LEI-keyed** ESG source, the ``cyprus_drcor`` arrangement for
the file and the ``eiti_zambia`` one for the join:

* ``scripts/build_estma_index.py`` turns the CSV into a SQLite index
  (entities, reports, payment lines), rebuilt weekly by
  ``.github/workflows/refresh-estma-index.yml`` and shipped as the
  ``estma-index`` release asset, downloaded at boot (``ESTMA_DB_URL``).
* The file carries no LEI and no business number: only the ESTMA ID
  (``E405435``), stable across renames. The join to GLEIF is the reviewed
  crosswalk ``opencheck/data/estma_lei_crosswalk.csv`` — exact-name GLEIF
  matches, a hand-checked normalised-name pass and five rows Stephen settled
  (10 Nov 2026). Only rows marked ``accepted`` are read; a retired LEI is
  not. One LEI can name two ESTMA IDs (Minto Explorations Ltd. and Minto
  Metals Corp. are one company renamed); both are shown, apart.

What is asserted
----------------
The ESTMA ID is the dataset's own key for every row it files, so it is
asserted (``ca_estma_id``, BODS scheme ``CA-ESTMA``). The LEI is OpenCheck's
match and is **not** — the mapper carries it as an ``identifying``
annotation with the match's basis (the ``eiti_assessment`` precedent).

What is never said
------------------
* **A payee view and a project view are the same payments twice.** A
  report's total is the sum of its payee lines, and the project lines are
  shown apart; the two are never added.
* **Two reports are never added**, even for one year (a stub period beside
  a full year, or a refiled report).
* **"Included in X's report" is not "paid nothing"** and is not ownership:
  an ESTMA reporting entity may report on behalf of another, which is a
  reporting arrangement. No BODS relationship is drawn.
* **ESTMA and EITI figures are never added together** — different reports,
  currencies and periods.

Licence: Statistics Canada Open Licence (Stephen's decision, 10 Nov 2026 —
assumed for the republished file, not confirmed with NRCan).
"""

from __future__ import annotations

import asyncio
import csv
import logging
import sqlite3
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import provenance
from ..config import get_settings
from .base import SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.estma import EstmaBundle

logger = logging.getLogger(__name__)

#: ``meta.schema_version`` the reader accepts (written by the builder).
INDEX_SCHEMA_VERSION = "1"

DATASET_URL = "https://www150.statcan.gc.ca/n1/pub/71-607-x/2022008/STC_DF_ESTMA-eng.csv"
#: Statistics Canada's page for the dataset (the CSV's landing page).
DATASET_PAGE = "https://www150.statcan.gc.ca/n1/pub/71-607-x/71-607-x2022008-eng.htm"
NRCAN_URL = (
    "https://natural-resources.canada.ca/minerals-mining/services-mining-industry/"
    "extractive-sector-transparency/links-estma-reports"
)
LICENCE = "StatCan-Open-Licence"
LICENCE_URL = "https://www.statcan.gc.ca/en/reference/licence"

#: The committed, reviewed ESTMA ID ↔ LEI crosswalk.
CROSSWALK_PATH = Path(__file__).resolve().parent.parent / "data" / "estma_lei_crosswalk.csv"

#: How many payees and projects one report carries in the bundle; the rest
#: are counted (``payee_count`` / ``project_count``), never dropped silently.
TOP_LINES = 10

#: What each crosswalk ``method`` means, in words a reader can weigh.
MATCH_BASIS: dict[str, str] = {
    "exact name, one live LEI": (
        "the ESTMA entity name equals the legal name of exactly one LEI record"
    ),
    "exact name, several LEIs, chosen by hand": (
        "the ESTMA entity name equals the legal name of several LEI records; "
        "this one was chosen by hand"
    ),
    "name match after suffix, ampersand or bilingual-name normalisation, checked by hand": (
        "the names agree once legal-form suffixes, ampersands and bilingual "
        "forms are set aside; checked by hand"
    ),
    "checked by Stephen": "confirmed by hand by OpenCheck's maintainer",
}


# ---------------------------------------------------------------------------
# The crosswalk
# ---------------------------------------------------------------------------

_CROSSWALK: dict[str, list[dict[str, str]]] | None = None


def _load_crosswalk() -> dict[str, list[dict[str, str]]]:
    """LEI → the accepted crosswalk rows naming it (cached)."""
    global _CROSSWALK
    if _CROSSWALK is None:
        by_lei: dict[str, list[dict[str, str]]] = defaultdict(list)
        try:
            with open(CROSSWALK_PATH, encoding="utf-8", newline="") as fh:
                for row in csv.DictReader(fh):
                    lei = (row.get("lei") or "").strip().upper()
                    if row.get("decision") == "accepted" and len(lei) == 20:
                        by_lei[lei].append(row)
        except OSError as exc:  # pragma: no cover - the file is committed
            logger.warning("estma: crosswalk unavailable: %s", exc)
        _CROSSWALK = dict(by_lei)
    return _CROSSWALK


def estma_ids_for_lei(lei: str) -> list[str]:
    """The ESTMA IDs the reviewed crosswalk ties to an LEI (may be empty)."""
    return [r["estma_id"] for r in _load_crosswalk().get((lei or "").strip().upper(), [])]


# ---------------------------------------------------------------------------
# The index
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``ESTMA_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().estma_db_file
    return Path(configured) if configured else data_root() / "estma.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().estma_db_file:
            logger.warning("estma: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    meta = provenance.index_meta(conn)
    if meta.get("schema_version") != INDEX_SCHEMA_VERSION:
        logger.warning(
            "estma: index at %s has an unexpected schema (want %s); ignoring",
            path, INDEX_SCHEMA_VERSION,
        )
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection (after a download, or in tests)."""
    global _CROSSWALK
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass
    _CROSSWALK = None


def index_available() -> bool:
    """True when a readable index of the expected schema is present."""
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    conn = _shared_conn()
    return provenance.index_meta(conn) if conn is not None else {}


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The day Statistics Canada published the file the index was built from."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot on its two clocks (Phase 314):
    StatCan's publication day is the source's, ``built_at`` ours."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"Statistics Canada ESTMA dataset, published {cut}"
            if cut else "Statistics Canada ESTMA dataset"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state
    this module models (``covers_lei`` is False, the source not announced)."""
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        record_asset_stamp,
    )
    from ..entity_pages import (
        read_meta as read_file_meta,
    )

    settings = get_settings()
    path = db_path()
    url = settings.estma_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"estma": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"estma": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"estma": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("estma: asset check/download failed: %s", exc)
        return {"estma": f"failed: {exc}"}


def _entity_row(conn: sqlite3.Connection, estma_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT estma_id, name, reports_payments FROM entity WHERE estma_id = ?", (estma_id,)
    ).fetchone()
    return dict(row) if row is not None else None


def _lines(conn: sqlite3.Connection, report_id: int, kind: str) -> list[dict[str, Any]]:
    """One report's payee or project lines, grouped by payee/project, largest
    first. Each carries its payment types as filed."""
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for r in conn.execute(
        "SELECT code, name, country_code, country_name, payment_type, payment_type_label, amount"
        " FROM payment WHERE report_id = ? AND kind = ?",
        (report_id, kind),
    ):
        key = (r["code"], r["country_code"] or "")
        line = grouped.setdefault(
            key,
            {
                "name": r["name"],
                "country_code": r["country_code"],
                "country_name": r["country_name"],
                "total_cad": 0.0,
                "by_payment_type": [],
            },
        )
        line["total_cad"] += float(r["amount"])
        line["by_payment_type"].append(
            {"code": r["payment_type"], "label": r["payment_type_label"], "cad": float(r["amount"])}
        )
    out = sorted(grouped.values(), key=lambda x: (-x["total_cad"], x["name"]))
    for line in out:
        line["total_cad"] = round(line["total_cad"], 2)
        line["by_payment_type"].sort(key=lambda x: -x["cad"])
    return out


def _report(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
    report: dict[str, Any] = {
        "year": row["year"],
        "period_start": row["period_start"],
        "period_end": row["period_end"],
        "report_type": row["report_type"],
        "report_type_label": row["report_type_label"],
        "url": row["url"],
        "included_in": (
            {"estma_id": row["included_in_id"], "name": row["included_in_name"]}
            if row["included_in_id"] else None
        ),
    }
    if report["included_in"] is not None:
        return report
    payees = _lines(conn, row["report_id"], "payee")
    projects = _lines(conn, row["report_id"], "project")
    by_type: dict[str, dict[str, Any]] = {}
    by_country: dict[str, dict[str, Any]] = {}
    for p in payees:
        for t in p["by_payment_type"]:
            entry = by_type.setdefault(t["code"], {"code": t["code"], "label": t["label"], "cad": 0.0})
            entry["cad"] += t["cad"]
        c = by_country.setdefault(
            p["country_code"] or "",
            {"code": p["country_code"], "name": p["country_name"], "cad": 0.0, "payees": 0},
        )
        c["cad"] += p["total_cad"]
        c["payees"] += 1
    report.update(
        {
            # OpenCheck's arithmetic, labelled as such on the card: the sum
            # of the report's payee lines. The project lines are the same
            # payments seen by project and are never added to it.
            "payee_total_cad": round(sum(p["total_cad"] for p in payees), 2),
            "payee_count": len(payees),
            "project_count": len(projects),
            "by_payment_type": sorted(
                ({**t, "cad": round(t["cad"], 2)} for t in by_type.values()),
                key=lambda t: -t["cad"],
            ),
            "by_country": sorted(
                ({**c, "cad": round(c["cad"], 2)} for c in by_country.values()),
                key=lambda c: -c["cad"],
            ),
            "payees": payees[:TOP_LINES],
            "projects": projects[:TOP_LINES],
        }
    )
    return report


def entity_bundle(estma_id: str) -> dict[str, Any] | None:
    """Everything the index holds for one ESTMA ID, or None."""
    conn = _shared_conn()
    if conn is None:
        return None
    entity = _entity_row(conn, estma_id)
    if entity is None:
        return None
    rows = conn.execute(
        "SELECT * FROM report WHERE estma_id = ?"
        " ORDER BY year DESC, period_end DESC, report_type, url",
        (estma_id,),
    ).fetchall()
    reports = [_report(conn, r) for r in rows]
    # The entities whose payments this one reports, in its latest year that
    # names any: a reporting arrangement, never ownership.
    includes: dict[str, Any] | None = None
    latest = conn.execute(
        "SELECT MAX(year) FROM report WHERE included_in_id = ?", (estma_id,)
    ).fetchone()[0]
    if latest:
        names = conn.execute(
            "SELECT DISTINCT r.estma_id, e.name FROM report r JOIN entity e USING (estma_id)"
            " WHERE r.included_in_id = ? AND r.year = ? ORDER BY e.name",
            (estma_id, latest),
        ).fetchall()
        includes = {"year": latest, "entities": [{"estma_id": n[0], "name": n[1]} for n in names]}
    return {
        "estma_id": entity["estma_id"],
        "name": entity["name"],
        "reports_payments": bool(entity["reports_payments"]),
        "reports": reports,
        "includes": includes,
    }


def _reset_for_tests() -> None:
    reset_connection()


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class EstmaAdapter(SourceAdapter):
    """ESTMA payments to governments (Statistics Canada's file) — ESG."""

    id = "estma"

    #: LEI-keyed, dispatched in routers/lookup.py beside the other LEI-keyed
    #: offline sources (eiti_zambia, eiti_assessment), not by RA code.
    lookup_timeout_s = 10.0

    @property
    def info(self) -> SourceInfo:
        meta = read_meta()
        n = sum(len(v) for v in _load_crosswalk().values())
        return SourceInfo(
            id=self.id,
            name="ESTMA payments to governments (Canada)",
            homepage=NRCAN_URL,
            description=(
                "Payments to governments in Canada and abroad — taxes, royalties, "
                "fees, production entitlements, bonuses, dividends and "
                "infrastructure payments — reported each year by extractive "
                "companies under Canada's Extractive Sector Transparency Measures "
                "Act, from Statistics Canada's weekly republication of every "
                "report. The file is keyed by the ESTMA ID and carries no LEI; "
                f"OpenCheck links {n} ESTMA entities to LEIs through a "
                "hand-reviewed name crosswalk."
                + (f" Published {meta['cut']}." if meta.get("cut") else "")
            ),
            license=LICENCE,
            attribution=(
                "Source: Statistics Canada, Extractive Sector Transparency Measures "
                "Act (ESTMA) data (dataflow STC:DF_ESTMA), collected by Natural "
                "Resources Canada. Reproduced under the Statistics Canada Open Licence."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            category="esg",
            # No country, like every ESG source: knowability reads
            # SourceInfo.country as "a register OpenCheck reads for owners",
            # and ESTMA is a payments disclosure, not a register.
            country=None,
        )

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        # LEI-keyed source; free-text search intentionally empty.
        return []

    def covers_lei(self, lei: str) -> bool:
        """Whether the crosswalk names this LEI and the index is present.

        Absence means OpenCheck could not tie the LEI to an ESTMA entity, not
        that the company made no payments. Declares no provenance."""
        return bool(estma_ids_for_lei(lei)) and index_available()

    async def fetch_by_lei(self, lei: str) -> dict[str, Any] | None:
        """The ESTMA bundle for an LEI, or ``None`` if the crosswalk names none."""
        lei_norm = (lei or "").strip().upper()
        if not estma_ids_for_lei(lei_norm):
            return None
        bundle = await asyncio.to_thread(self._build, lei_norm)
        return None if bundle.get("is_stub") else bundle

    async def fetch(self, hit_id: str) -> dict[str, Any]:
        """Fetch by LEI hit id (deepen / retry path)."""
        return await asyncio.to_thread(self._build, (hit_id or "").strip().upper())

    def _build(self, lei: str) -> dict[str, Any]:
        rows = _load_crosswalk().get(lei, [])
        meta = read_meta()
        if not rows or not meta:
            return {"source_id": self.id, "hit_id": lei, "is_stub": True}
        declare_snapshot(meta)
        entities: list[dict[str, Any]] = []
        for row in rows:
            entity = entity_bundle(row["estma_id"])
            if entity is None:
                continue
            entity["match"] = {
                "method": row.get("method") or "",
                "basis": MATCH_BASIS.get(row.get("method") or "", "a match made by OpenCheck"),
                "gleif_name": row.get("gleif_name") or "",
                "note": row.get("note") or "",
            }
            entities.append(entity)
        if not entities:
            return {"source_id": self.id, "hit_id": lei, "is_stub": True}
        # The reporting entity first, then by most recent year.
        entities.sort(
            key=lambda e: (
                not e["reports_payments"],
                -int(max((r["year"] for r in e["reports"]), default="0") or 0),
            )
        )
        years = sorted({r["year"] for e in entities for r in e["reports"]}, reverse=True)
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": lei,
            "lei": lei,
            "is_stub": False,
            "gleif_legal_name": entities[0]["match"]["gleif_name"],
            "entities": entities,
            "latest_year": years[0] if years else None,
            # Corroboration rule: the ESTMA ID is the dataset's own key.
            "identifiers": {"ca_estma_id": entities[0]["estma_id"]},
            "cut": meta.get("cut"),
            "built_at": meta.get("built_at"),
            "dataset_url": DATASET_URL,
            "dataset_page": DATASET_PAGE,
            "nrcan_url": NRCAN_URL,
            "licence": LICENCE,
            "licence_url": LICENCE_URL,
        }
        validate_raw(self.id, EstmaBundle, bundle)
        return bundle
