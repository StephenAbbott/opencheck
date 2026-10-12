"""Lithuanian Register of Legal Entities (JAR) adapter — a weekly index (Phase 325).

Registrų centras (the Centre of Registers) keeps the Juridinių asmenų
registras (JAR), Lithuania's statutory register of every legal entity, and
publishes the whole of it as daily open data under CC BY 4.0:

    https://www.registrucentras.lt/atviri-duomenys-ir-statistika/jar-pirminiai-duomenys-raw-data

Why an index, and why it is built elsewhere
-------------------------------------------

Until Phase 325 this adapter scraped the register's name search
(``/jar/p/index.php?pav=``). From production it never worked: JAR's site, and
the open-data downloads with it, sit behind a Cloudflare JavaScript challenge
that every datacentre address receives (Render, GitHub Actions, the cloud) and
no server-side client can pass. The national portal that republishes the
files (``data.gov.lt`` / ``get.data.gov.lt``) is geo-fenced to Lithuania —
tested 12 Oct 2026 from a UK residential connection and a real browser, both
refused. So every Lithuanian lookup showed a card holding only GLEIF's name.

A residential connection is not challenged. ``scripts/build_jar_lithuania_index.py``
(standard library only) downloads the registered and deregistered entity
files there, checks them, and packs a SQLite index;
``scripts/refresh_jar_lithuania_index.sh`` replaces the ``jar-lithuania-index``
release asset; and :func:`warm_index` downloads it at boot — the ONRC /
ChileCompra arrangement. The index holds the **whole register** (≈550,000
entities, 58 MB, 17.6 MB gzipped), not an LEI-scoped slice, because ``LT-JAR``
is a FullCheck register hop and ``opencheck_register_lookup`` reaches
Lithuanian companies that hold no LEI.

What the index answers, measured 12 Oct 2026
--------------------------------------------

* All 5,083 GLEIF records filed under ``RA000430`` carry a bare nine-digit
  code; 5,082 resolve (3,422 of 3,423 ISSUED). The miss is a note card.
* No name gate. Where GLEIF and JAR disagree on a name it is GLEIF holding an
  old one (UAB "Delela" is now UAB Starwell): the code is an exact identifier
  from GLEIF's own record, and gating would lose exactly the renamed
  companies — the ONRC CUI precedent (Phase 212).
* **Names the register withholds.** The open data puts the legal form's name
  in the name column for every sole proprietorship, general and limited
  partnership and their branches (170,258 entities), because those names
  usually carry the owner's own name. The index stores no name for them; the
  bundle carries GLEIF's legal name instead, marked ``name_withheld``, and the
  mapper annotates where the name came from.
* Status: one code per registered entity, classified in :data:`STATUSES`.
  Deregistered entities carry their deregistration date, which becomes
  ``dissolutionDate``.

No beneficial owners and no officers: neither is in the open data
(participants moved to JANGIS, a legitimate-interest system).

Nothing here touches the response :class:`~opencheck.cache.Cache`. A miss is
an answer about one build of the index, and must not outlive it.
"""

from __future__ import annotations

import logging
import re
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.jar_lithuania import JarLithuaniaBundle

logger = logging.getLogger(__name__)

#: GLEIF Registration Authority code for Lithuania's JAR.
LT_RA_CODE: str = "RA000430"

#: The table layout this adapter reads. ``scripts/build_jar_lithuania_index.py``
#: writes the same value into ``meta``; a test pins the two equal, and an index
#: carrying any other value is ignored rather than misread.
INDEX_SCHEMA_VERSION = "1"

#: Where the data is published — the page a reader can follow. The per-entity
#: search page (``?kod=``) is CAPTCHA-gated and challenged from datacentres.
DATASET_URL = (
    "https://www.registrucentras.lt/atviri-duomenys-ir-statistika/"
    "jar-pirminiai-duomenys-raw-data"
)

COVERAGE_NOT_IN_INDEX = (
    "This entity code is not in the Register of Legal Entities' open data as "
    "indexed — neither among registered nor deregistered entities. GLEIF may "
    "hold a code the register does not use, or the entity may be newer than "
    "the index."
)

# Lithuanian legal entity code: exactly 9 digits.
_CODE_RE = re.compile(r"^\d{9}$")


# ---------------------------------------------------------------------------
# The register's vocabularies
# ---------------------------------------------------------------------------

#: JAR's legal status codes (``stat_kodas``), as found in the registered-entity
#: file on 12 Oct 2026: ``(liveness class, English gloss, search-rank word)``.
#:
#: * ``live`` — code 0, *Teisinis stat neįregistruotas*: no legal status is
#:   registered against the entity, i.e. no liquidation, bankruptcy or
#:   reorganisation (230,558 of 235,248).
#: * ``pending`` — a process that ends the entity is under way: liquidation,
#:   bankruptcy, being reorganised, merged or divided **out**.
#: * ``unknown`` — the label does not say whether this entity ends: taking
#:   part in someone else's reorganisation, changing legal form, restructuring,
#:   and two cross-border-merger states (480 entities). The label still shows
#:   on the card; nothing is written to BODS rather than a guess.
#:
#: An unlisted code is ``unknown`` too. The rank word is a summary segment
#: ``search_rank`` recognises, where one fits.
STATUSES: dict[str, tuple[str, str, str | None]] = {
    "0": ("live", "no legal status registered", None),
    "1": ("pending", "being reorganised", None),
    "2": ("unknown", "taking part in a reorganisation", None),
    "3": ("unknown", "changing legal form", None),
    "4": ("unknown", "under restructuring", None),
    "5": ("pending", "in bankruptcy proceedings", "bankrupt"),
    "6": ("pending", "bankrupt", "bankrupt"),
    "7": ("pending", "in liquidation", "in liquidation"),
    "8": ("unknown", "taking part in a separation", None),
    "9": ("pending", "liquidation initiated", "in liquidation"),
    "16": ("unknown", "European company formation by merger initiated", None),
    "23": ("unknown", "in a cross-border merger", None),
    "26": ("pending", "in liquidation following bankruptcy", "in liquidation"),
    "27": ("pending", "being merged across borders", None),
    "28": ("unknown", "taking part in a cross-border merger", None),
    "30": ("pending", "being divided across borders", None),
}

#: English for the commonest legal forms (``form_kodas``). A gloss, shown
#: beside the register's own words, never instead of them; an unlisted form
#: shows in Lithuanian alone.
FORM_GLOSSES: dict[str, str] = {
    "110": "state enterprise",
    "130": "municipal enterprise",
    "210": "general partnership",
    "220": "limited partnership",
    "260": "gardeners' community",
    "270": "community association",
    "310": "private limited company",
    "311": "branch of a private limited company",
    "320": "public limited company",
    "410": "agricultural company",
    "490": "branch of a foreign legal entity",
    "520": "representative office of a foreign legal entity",
    "570": "public institution",
    "630": "association",
    "640": "charity and support foundation",
    "700": "traditional religious community",
    "710": "cooperative",
    "760": "trade union",
    "810": "sole proprietorship",
    "850": "European economic interest grouping",
    "860": "European company",
    "870": "central bank",
    "950": "budget institution",
    "960": "small partnership",
}

#: Legal forms that are public bodies funded from a state or municipal budget
#: — typed ``stateBody``. Everything else in JAR was created by registration
#: and is a ``registeredEntity``, **including state and municipal
#: enterprises**: an SOE is not a government body (the Phase 240 rule).
STATE_BODY_FORMS = frozenset({"580", "581", "680", "681", "870", "950", "951", "975"})


# ---------------------------------------------------------------------------
# Identifier helpers
# ---------------------------------------------------------------------------


def normalise_code(code: str | int) -> str:
    """Return the canonical 9-digit Lithuanian legal entity code string."""
    return str(code).strip().zfill(9)


def is_valid_lt_code(code: str) -> bool:
    """True when ``code`` is a valid 9-digit Lithuanian entity code."""
    return bool(_CODE_RE.match(normalise_code(code)))


def _entity_url(code: str) -> str:
    """The register's public page for an entity code (CAPTCHA-gated)."""
    return f"https://www.registrucentras.lt/jar/p/index.php?kod={code}"


def form_label(name: str | None, code: str | None) -> str | None:
    """The register's legal-form words, with an English gloss where known."""
    name = (name or "").strip()
    if not name:
        return None
    gloss = FORM_GLOSSES.get(str(code or ""))
    return f"{name} ({gloss})" if gloss else name


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``JAR_LITHUANIA_DB_FILE`` or the data-root default — one function, so
    the reader, the boot download and the sweep's ``requires_files`` agree
    (the ONRC lesson: they once did not, and a good index sat unread)."""
    from ..cache import data_root

    configured = get_settings().jar_lithuania_db_file
    return Path(configured) if configured else data_root() / "jar_lithuania.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().jar_lithuania_db_file:
            logger.warning("jar_lithuania: index not found at %s", path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'schema_version'"
        ).fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning("jar_lithuania: index at %s has an unexpected schema; ignoring", path)
        conn.close()
        return None
    return conn


def _shared_conn() -> sqlite3.Connection | None:
    if _SHARED.get("conn") is None:
        _SHARED["conn"] = _connect()
    return _SHARED.get("conn")


def reset_connection() -> None:
    """Forget the cached connection (after a download, or in tests)."""
    conn = _SHARED.pop("conn", None)
    if conn is not None:
        try:
            conn.close()
        except sqlite3.Error:  # pragma: no cover - best effort
            pass


def index_available() -> bool:
    """True when a readable index of the expected schema is present."""
    return _shared_conn() is not None


def read_meta() -> dict[str, str]:
    """The index's ``meta`` table, or ``{}`` when there is no index."""
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The register's cut — the ``formavimo_data`` every row of the files
    carries — as the day the data is "as of". Never the build time."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot of the register's open data.

    Without this the provenance recorder collects nothing and resolves to its
    ``stub`` default, and real register rows badge as "Placeholder data" —
    the ariregister (Phase 45) and ONRC (PR #275) bug. The cut is the
    register's clock; ``built_at`` is ours (Phase 314).
    """
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"Register of Legal Entities open data, cut {cut}"
            if cut else "Register of Legal Entities open data"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises — no index is a state this
    module models (``covers_lei`` is False and the source is not announced).

    The index cannot be built here: the register's files are behind a
    challenge every datacentre receives.
    """
    from ..entity_pages import (
        ASSET_STAMP_KEY,
        asset_check,
        download_db,
        read_meta as read_file_meta,
        record_asset_stamp,
    )

    settings = get_settings()
    path = db_path()
    url = settings.jar_lithuania_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {"jar_lithuania": f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {"jar_lithuania": f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {"jar_lithuania": f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("jar_lithuania: asset check/download failed: %s", exc)
        return {"jar_lithuania": f"failed: {exc}"}


_ENTITY_SQL = (
    "SELECT e.code, e.name, e.address, e.registered_on, e.form_code,"
    " f.name AS form_name, e.status_code, s.name AS status_name,"
    " e.status_since, e.deregistered_on"
    " FROM entity e"
    " LEFT JOIN form f ON f.code = e.form_code"
    " LEFT JOIN status s ON s.code = e.status_code"
)


def entity_record(code: str) -> dict[str, Any] | None:
    """The index's row for one entity code, with form and status names joined
    in; None when the index does not hold the code (or there is no index)."""
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute(_ENTITY_SQL + " WHERE e.code = ?", (code,)).fetchone()
    return dict(row) if row is not None else None


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class JarLithuaniaAdapter(SourceAdapter):
    """Source adapter for the Lithuanian Register of Legal Entities (JAR)."""

    id = "jar_lithuania"

    lookup_derivers = (
        LookupDeriver(frozenset({LT_RA_CODE}), "lt_code", normalise_code),
    )
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="JAR — Lithuanian Register of Legal Entities",
            homepage="https://www.registrucentras.lt/jar/",
            description=(
                "Lithuania's Register of Legal Entities (JAR), kept by "
                "Registrų centras and published as daily open data: every "
                "registered and deregistered entity's name, code, registered "
                "address, legal form, registration date, legal status and "
                "deregistration date. Read from a weekly index of the open "
                "data. The register withholds the names of sole "
                "proprietorships and partnerships; it publishes no officers "
                "or beneficial owners."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains data from the Lithuanian Register of Legal Entities "
                "(JAR), published by Registrų centras, available under CC BY 4.0. "
                "Source: registrucentras.lt."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="LT",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, not on the LEI.

        Without an index the source would be announced for every Lithuanian
        lookup and then answer nothing — counted in "N of N sources answered"
        while answering nothing (the ``cac_nigeria`` / ``onrc_romania`` gate).
        Whether the *entity* is in the index is ``fetch``'s answer: a note
        card when it is not.
        """
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        try:
            rows = conn.execute(
                _ENTITY_SQL + " WHERE e.name LIKE ? LIMIT 10",
                (f"%{query.strip()}%",),
            ).fetchall()
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        code = row["code"]
        segments = [f"LT {code}"]
        form = (row.get("form_name") or "").strip()
        if form:
            segments.append(form)
        if row.get("deregistered_on"):
            segments.append("deregistered")
        else:
            rank_word = STATUSES.get(str(row.get("status_code") or ""), (None, None, None))[2]
            if rank_word:
                segments.append(rank_word)
        return SourceHit(
            source_id="jar_lithuania",
            hit_id=code,
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"LT {code}",
            summary=" · ".join(segments),
            identifiers={"lt_code": code},
            raw={"code": code},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, code: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a code. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "hit_id": code,
            "lt_code": code,
            "name": legal_name or "",
            "link": DATASET_URL,
            "is_stub": True,
        }

    def _not_in_index(self, code: str, legal_name: str, meta: dict[str, str]) -> dict[str, Any]:
        """The index answered, and does not hold the code — a note card.

        ``is_stub`` False and ``not_found`` True, the INPI / ONRC shape:
        ``_lookup_pipeline`` drops every ``is_stub`` result above the registry
        branch, so a True here would announce the source and then show
        nothing. Not cached and not a degradation: nothing failed.
        """
        return {
            "source_id": self.id,
            "hit_id": code,
            "lt_code": code,
            "name": legal_name or "",
            "link": DATASET_URL,
            "is_stub": False,
            "not_found": True,
            "coverage_note": COVERAGE_NOT_IN_INDEX,
            "cut": meta.get("cut"),
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the index's record for one Lithuanian entity code.

        ``hit_id`` is the nine-digit code GLEIF files under ``registeredAs``
        (or a register-hop / ``opencheck_register_lookup`` caller passes);
        ``legal_name`` is GLEIF's name, used only where the register
        withholds one.
        """
        code = normalise_code(hit_id)
        if not is_valid_lt_code(code):
            logger.warning("jar_lithuania: invalid code %r", hit_id)
            return self._stub(str(hit_id or ""), legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(code, legal_name)
        try:
            row = entity_record(code)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Lithuanian register index could not be read; the card "
                "shows nothing from the register. This is not a confirmation "
                "that the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(code, legal_name)
        meta = read_meta()
        if row is None:
            return self._not_in_index(code, legal_name, meta)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        register_name = (row.get("name") or "").strip() or None
        status_code = str(row.get("status_code") or "") or None
        deregistered_on = row.get("deregistered_on")
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "hit_id": code,
            "lt_code": code,
            "name": register_name or legal_name or "",
            "register_name": register_name,
            "name_withheld": register_name is None,
            "address": row.get("address"),
            "legal_form": row.get("form_name"),
            "legal_form_code": row.get("form_code"),
            "registered_on": row.get("registered_on"),
            "status": None if deregistered_on else row.get("status_name"),
            "status_code": None if deregistered_on else status_code,
            "status_since": None if deregistered_on else row.get("status_since"),
            "deregistered": bool(deregistered_on),
            "deregistered_on": deregistered_on,
            "cut": meta.get("cut"),
            "link": DATASET_URL,
            "is_stub": False,
        }
        validate_raw("jar_lithuania", JarLithuaniaBundle, bundle)
        return bundle
