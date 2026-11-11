"""Argentina — the Registro Nacional de Sociedades (RNS) and the Inspección
General de Justicia (IGJ), read from the Ministry of Justice's open data.

An **index-tier** register source, the ``ica_israel`` shape: the lookup reads
a SQLite index of two ``datos.jus.gob.ar`` datasets (CC BY 4.0), downloaded at
boot from the ``ar-sociedades-index`` release asset (``AR_SOCIEDADES_DB_URL``)
and replaced when the asset changes. Dispatch is gated on the index being
present (``covers_lei``); a number the index does not hold is a note card,
never a failure. The index is rebuilt by
``.github/workflows/refresh-ar-sociedades-index.yml`` running
``scripts/build_ar_sociedades_index.py``.

One source, two registers (Stephen's decision A, 11 Nov 2026)
-------------------------------------------------------------
* **RNS** is the base: every legal person nationwide (1,270,807 CUITs on
  18 Sept 2026), keyed on the CUIT, with name, legal form, contract (founding)
  date, legal and fiscal addresses and the ARCA activity. It carries **no
  status and no people**.
* **IGJ** (the City of Buenos Aires register) adds, for the companies it
  registered, its own number (the *correlativo*), the deregistration flag
  with its reason, the balance sheets and assemblies filed, and a count of
  the people named in its filings.

Joining them, measured on the 337 ISSUED LEIs that file a CUIT (11 Nov 2026):
313 are in the RNS, 238 in the IGJ.

What is never read or said
--------------------------
* **IGJ people are a count, never a party** (decision B): the file is
  cumulative (rows are added, never removed), undated, gives no specific
  role, and carries a national ID number and a gender on every row. "SOCIO"
  there is not a shareholder (Google Argentina SRL's are natural persons).
  The index stores only the count.
* **Natural-person forms are not in the index** (decision C): sole traders,
  auctioneers, brokers, customs agents (IGJ forms 210–260 bar 226). Their
  entity name is a person's name.
* **A foreign art. 123 registration that carries a subsidiary's CUIT is
  ignored** (decision D): Toyota Motor Corporation and Banco do Brasil are
  filed under their Argentine subsidiaries' CUITs. A foreign-form IGJ row is
  read for a CUIT only when the RNS itself calls the CUIT a foreign company
  or branch.
* **An IGJ row without the deregistration flag is not "active"**: the flag
  marks removal; its absence says nothing about whether the company trades.

Identifiers
-----------
GLEIF files the CUIT under RA000009 (ARCA, ``33-67913936-9``), RA000010 (the
RNS; bare or hyphenated) and RA000800 (CNV). About 30 RA000010 records file an
IGJ correlativo instead (Bioseeds S.A. ``1917865``): those are looked up in the
IGJ table and **name-gated** against GLEIF's name, because a correlativo is a
short number that other registers' numbers collide with. A CUIT is never
name-gated (two of 313 differ, both renames). The CUIT is written
``30-50000173-5`` under ``AR-CUIT`` (org-id.guide) on the GLEIF and register
statements alike.
"""

from __future__ import annotations

import logging
import re
import sqlite3
import unicodedata
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .. import degradation, provenance
from ..config import get_settings
from ..identifiers import argentine_cuit, format_cuit
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.registro_sociedades_argentina import RegistroSociedadesArgentinaBundle

logger = logging.getLogger(__name__)

SOURCE_ID = "registro_sociedades_argentina"

#: GLEIF Registration Authority codes that carry an Argentine CUIT: ARCA
#: (formerly AFIP, RA000009), the Registro Nacional de Sociedades (RA000010)
#: and the Comisión Nacional de Valores (RA000800). Verified against all 972
#: GLEIF records with an Argentine legal address, 11 Nov 2026.
AR_RA_CODES: frozenset[str] = frozenset({"RA000009", "RA000010", "RA000800"})
#: The RNS's own code: the only one under which a correlativo was filed.
AR_RNS_RA_CODE = "RA000010"

#: org-id.guide's scheme for the CUIT, and what both mappers write.
AR_CUIT_SCHEME = "AR-CUIT"
AR_CUIT_SCHEME_NAME = "CUIT — Clave Única de Identificación Tributaria (Argentina)"
#: The IGJ's own number, said in a note and never asserted as an identifier
#: scheme (org-id.guide has no code for it).
IGJ_KEY_PREFIX = "IGJ:"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

RNS_DATASET_URL = "https://datos.jus.gob.ar/dataset/registro-nacional-de-sociedades"
IGJ_DATASET_URL = (
    "https://datos.jus.gob.ar/dataset/"
    "entidades-constituidas-en-la-inspeccion-general-de-justicia-igj"
)

#: IGJ forms whose entity is a natural person (decision C) — never indexed.
#: 226 (consorcios de propiedad horizontal) is a building's owners'
#: association, not a person, and stays.
NATURAL_PERSON_FORMS: frozenset[int] = frozenset({210, 220, 222, 225, 230, 240, 250, 260})
#: IGJ forms that are not a company of their own: foreign registrations
#: (80, 105, 176), unregistered companies (160), joint ventures and
#: collaboration contracts (150, 151, 155, 170) and trusts (175, 178).
#: Skipped when a CUIT is resolved to an IGJ entry.
IGJ_NOT_A_COMPANY_FORMS: frozenset[int] = frozenset({80, 105, 150, 151, 155, 160, 170, 175, 176, 178})
#: The foreign forms among them, read only when the RNS calls the CUIT foreign.
IGJ_FOREIGN_FORMS: frozenset[int] = frozenset({80, 105, 176})
#: The RNS forms of a foreign company's Argentine registration.
RNS_FOREIGN_FORMS: frozenset[str] = frozenset(
    {"SUCURSAL EMPRESA EXTRANJERA", "SOCIEDAD EXTRANJERA", "ASOCIACION CIVIL EXTRANJERA"}
)

#: English glosses of the RNS's legal forms (the register's own words are
#: kept beside them).
FORM_GLOSSES: dict[str, str] = {
    "SOCIEDAD ANONIMA": "public limited company (sociedad anónima)",
    "SOCIEDAD ANONIMA UNIPERSONAL": "single-member public limited company",
    "SOCIEDAD DE RESPONSABILIDAD LIMITADA": "limited liability company",
    "SOCIEDAD POR ACCION SIMPLIFICADA": "simplified joint-stock company",
    "SOCIEDAD EN COMANDITA SIMPLE": "limited partnership",
    "SOCIEDAD EN COMANDITA POR ACCIONES": "partnership limited by shares",
    "SOCIEDAD COLECTIVA": "general partnership",
    "SOCIEDAD LEY 19550 CAPITULO I SECCION IV": "informal company (General Companies Law, chapter I section IV)",
    "CAPITAL E INDUSTRIA": "capital-and-industry partnership",
    "SOCIEDAD DEL ESTADO": "state company",
    "SOCIEDAD ANONIMA CON PARTICIPACION ESTATAL MAYORITARIA": "public limited company with majority state ownership",
    "ECONOMIA MIXTA": "mixed-economy company",
    "GARANTIA RECIPROCA": "mutual guarantee company",
    "SUCURSAL EMPRESA EXTRANJERA": "branch of a foreign company",
    "SOCIEDAD EXTRANJERA": "foreign company",
    "SOCIEDAD BINACIONAL FUERA DE JURISDICCION": "binational company",
    "ASOCIACION CIVIL": "civil association",
    "ASOCIACION CIVIL EXTRANJERA": "foreign civil association",
    "ASOCIACION COOPERADORA": "cooperating association",
    "ASOCIACION MUTUAL": "mutual association",
    "SIMPLE ASOCIACION": "simple association",
    "FUNDACION": "foundation",
    "COOPERATIVA": "cooperative",
    "COOPERATIVA EFECTORA": "social-economy cooperative",
    "INSTITUTO DE VIDA CONSAGRADA": "institute of consecrated life",
}

#: RNS forms that are a state body or a state company. Phase 240: a state
#: company stays ``registeredEntity``; none of these is a ``stateBody``.
STATE_FORMS: frozenset[str] = frozenset(
    {"SOCIEDAD DEL ESTADO", "SOCIEDAD ANONIMA CON PARTICIPACION ESTATAL MAYORITARIA"}
)

COVERAGE_NOT_IN_INDEX = (
    "This number is not among the legal persons in the Registro Nacional de "
    "Sociedades or the companies registered by the IGJ, as indexed. The "
    "national register is refreshed about monthly, so a new company may not "
    "be in it yet."
)
COVERAGE_NAME_MISMATCH = (
    "GLEIF files an IGJ number for this company, and the IGJ entry with that "
    "number names a different entity, so it was not used. The number GLEIF "
    "files is probably another register's."
)

#: An IGJ deregistration reason, by the words of its description. The IGJ
#: publishes about 100 codes, many with no description; the words decide.
_MOVED_WORDS = ("JURISDICCION", "JURIS.", "JURISD")
_TRANSFORMED_WORDS = ("TRANSFORMACION",)
_DUPLICATE_WORDS = ("DUPLICIDAD",)
_TERMINAL_WORDS = (
    "CANCELACION", "CANCEL.", "CANCEL ", "CANC.", "DISOLUCION SIN LIQUIDACION",
    "RETIRO PERSONERIA", "EXTINCION", "LIQUIDACION JUDICIAL",
)


def baja_class(deregistered: Any, words: str | None) -> str:
    """What an IGJ entry's deregistration says.

    ``none`` (no flag), ``moved`` (to another jurisdiction — not an ending),
    ``transformed`` (into another form, often under a new entry),
    ``duplicate`` (the IGJ's own duplicate entry), ``terminal`` (cancelled,
    dissolved by merger or division, liquidated), or ``unstated`` (the flag
    is set and the reason has no words, or words this reading does not
    know). Order matters: "CANCELACION A CAMBIO DE JURISDICCION" is a move.
    """
    if not deregistered:
        return "none"
    text = (words or "").upper()
    if any(w in text for w in _MOVED_WORDS):
        return "moved"
    if any(w in text for w in _TRANSFORMED_WORDS):
        return "transformed"
    if any(w in text for w in _DUPLICATE_WORDS):
        return "duplicate"
    if any(w in text for w in _TERMINAL_WORDS):
        return "terminal"
    return "unstated"


#: Legal-form words removed before two names are compared.
_FORM_TOKENS = frozenset(
    {
        "S", "A", "SA", "SAU", "SRL", "SAS", "SCA", "SC", "SE", "SH", "L", "R", "U",
        "SOCIEDAD", "ANONIMA", "UNIPERSONAL", "RESPONSABILIDAD", "LIMITADA",
        "POR", "ACCIONES", "ACCION", "SIMPLIFICADA", "EN", "COMANDITA", "DE", "LA", "Y",
        "THE", "AND", "INC", "LTD", "LIMITED", "CORP",
    }
)


def name_key(name: str | None) -> frozenset[str]:
    """The distinctive tokens of a company name: upper case, accents and
    punctuation gone, legal-form words removed (IGJ files ``TOYOTA
    ARGENTINA``; the RNS files ``TOYOTA ARGENTINA S A``)."""
    text = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode()
    tokens = re.findall(r"[A-Z0-9]+", text.upper().replace("&", " Y "))
    return frozenset(t for t in tokens if t not in _FORM_TOKENS)


def names_agree(a: str | None, b: str | None) -> bool:
    """One name's distinctive tokens are all in the other's."""
    ka, kb = name_key(a), name_key(b)
    if not ka or not kb:
        return False
    return ka <= kb or kb <= ka


def normalise_ar_key(raw: str) -> str:
    """The dispatch key for a number GLEIF files under an Argentine RA.

    A CUIT with a valid check digit → its 11 digits. A 6–7 digit number →
    ``IGJ:<n>``, an IGJ correlativo (RA000010 only, in practice). Anything
    else (a provincial number, free text, a 4-digit code) raises
    ``ValueError`` and the source is not dispatched.
    """
    cuit = argentine_cuit(raw)
    if cuit:
        return cuit
    text = str(raw or "").strip()
    if text.upper().startswith(IGJ_KEY_PREFIX):
        # Already a key of ours: the number was zero-stripped, so any length.
        digits = text[len(IGJ_KEY_PREFIX):].strip()
        if re.fullmatch(r"\d{1,7}", digits) and int(digits) > 0:
            return f"{IGJ_KEY_PREFIX}{int(digits)}"
    elif re.fullmatch(r"\d{6,7}", text):
        return f"{IGJ_KEY_PREFIX}{int(text)}"
    raise ValueError(f"not a CUIT or an IGJ number: {raw!r}")


def normalise_cuit_key(raw: str) -> str:
    """A CUIT only — the FullCheck register hop on ``AR-CUIT``."""
    cuit = argentine_cuit(raw)
    if not cuit:
        raise ValueError(f"not a CUIT: {raw!r}")
    return cuit


def cuit_spellings(raw: str) -> tuple[str, ...]:
    """Both spellings GLEIF stores a CUIT in (``registeredAs`` is an exact
    match): ``30500001735`` and ``30-50000173-5``. Anything that is not a
    valid CUIT is searched as typed."""
    cuit = argentine_cuit(raw)
    if not cuit:
        return (str(raw or "").strip(),)
    return (cuit, format_cuit(cuit))


def form_label(form: str | None) -> str | None:
    text = (form or "").strip()
    if not text:
        return None
    return FORM_GLOSSES.get(text, text.capitalize())


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``AR_SOCIEDADES_DB_FILE`` or the data-root default — one function, so
    the reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().ar_sociedades_db_file
    return Path(configured) if configured else data_root() / "ar_sociedades.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().ar_sociedades_db_file:
            logger.warning("%s: index not found at %s", SOURCE_ID, path)
        return None
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        version = conn.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    except sqlite3.Error:
        version = None
    if not version or version[0] != INDEX_SCHEMA_VERSION:
        logger.warning(
            "%s: index at %s has an unexpected schema (want %s); ignoring",
            SOURCE_ID, path, INDEX_SCHEMA_VERSION,
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
    conn = _shared_conn()
    if conn is None:
        return {}
    try:
        return {str(k): str(v) for k, v in conn.execute("SELECT key, value FROM meta")}
    except sqlite3.Error:  # pragma: no cover
        return {}


def cut_date(meta: dict[str, str] | None = None) -> datetime | None:
    """The RNS file's date — the register's cut (the IGJ month is said beside it)."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: the RNS cut is the register's
    clock, ``built_at`` ours (Phase 314)."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    month = meta.get("igj_month") or ""
    detail = "datos.jus.gob.ar Registro Nacional de Sociedades and IGJ"
    if cut:
        detail += f", RNS cut {cut}"
    if month:
        detail += f", IGJ {month}"
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=detail,
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises."""
    from ..entity_pages import ASSET_STAMP_KEY, asset_check, download_db, record_asset_stamp
    from ..entity_pages import read_meta as read_file_meta

    settings = get_settings()
    path = db_path()
    url = settings.ar_sociedades_db_url
    if not url:
        state = "present" if path.exists() else "absent"
        return {SOURCE_ID: f"{state}: {path} (no URL)"}
    try:
        last_modified: str | None = None
        if path.exists():
            decision, last_modified = asset_check(url, path)
            if decision == "keep":
                if last_modified and read_file_meta(path).get(ASSET_STAMP_KEY) is None:
                    record_asset_stamp(path, last_modified)
                return {SOURCE_ID: f"already present: {path}"}
            outcome = "replaced"
        else:
            outcome = "downloaded"
        downloaded, elapsed = download_db(url, path, last_modified=last_modified)
        reset_connection()
        return {SOURCE_ID: f"{outcome}: {path} ({downloaded} bytes in {elapsed:.1f}s)"}
    except Exception as exc:  # noqa: BLE001 — no index is a state, not a crash
        logger.warning("%s: asset check/download failed: %s", SOURCE_ID, exc)
        return {SOURCE_ID: f"failed: {exc}"}


def _vocab(conn: sqlite3.Connection, kind: str, code: Any) -> str | None:
    if code in (None, ""):
        return None
    row = conn.execute(
        "SELECT words FROM vocab WHERE kind = ? AND code = ?", (kind, str(code))
    ).fetchone()
    return row[0] if row and row[0] else None


def rns_record(cuit: str) -> dict[str, Any] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM rns WHERE cuit = ?", (int(cuit),)).fetchone()
    return dict(row) if row is not None else None


def igj_record(correlativo: int) -> dict[str, Any] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM igj WHERE correlativo = ?", (correlativo,)).fetchone()
    return dict(row) if row is not None else None


def igj_for_cuit(cuit: str, rns: dict[str, Any] | None) -> tuple[dict[str, Any] | None, int]:
    """The IGJ entry for a CUIT, and how many other company entries share it.

    The IGJ's CUIT is not unique: renames, transformations and unregistered
    entries repeat it, and a foreign art. 123 registration carries its
    subsidiary's (decision D — ignored). So: skip the forms that are not a
    company of their own (foreign ones unless the RNS calls the CUIT foreign)
    and the IGJ's own duplicates; then prefer an entry not ended, then one
    whose name agrees with the RNS, then the newest (highest number).
    """
    conn = _shared_conn()
    if conn is None:
        return None, 0
    foreign_ok = (rns or {}).get("form") in RNS_FOREIGN_FORMS
    rows = [dict(r) for r in conn.execute("SELECT * FROM igj WHERE cuit = ?", (int(cuit),))]
    candidates = []
    for row in rows:
        form = row.get("form_code")
        if form in IGJ_NOT_A_COMPANY_FORMS and not (foreign_ok and form in IGJ_FOREIGN_FORMS):
            continue
        words = _vocab(conn, "igj_baja", row.get("baja_code"))
        cls = baja_class(row.get("deregistered"), words)
        if cls == "duplicate":
            continue
        row["baja_words"], row["baja_class"] = words, cls
        candidates.append(row)
    if not candidates:
        return None, 0
    rns_name = (rns or {}).get("name")
    candidates.sort(
        key=lambda r: (
            r["baja_class"] != "terminal",
            bool(rns_name) and names_agree(r.get("name"), rns_name),
            r.get("correlativo") or 0,
        ),
        reverse=True,
    )
    return candidates[0], len(candidates) - 1


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class RegistroSociedadesArgentinaAdapter(SourceAdapter):
    """Source adapter for Argentina's RNS and IGJ open data (local SQLite index)."""

    id = SOURCE_ID

    lookup_derivers = (LookupDeriver(AR_RA_CODES, "ar_cuit", normalise_ar_key),)
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="Argentina — Registro Nacional de Sociedades and IGJ",
            homepage="https://datos.jus.gob.ar/",
            description=(
                "Argentina's national register of legal persons (Registro "
                "Nacional de Sociedades) and the companies registered by the "
                "Inspección General de Justicia in the City of Buenos Aires, "
                "published as open data by the Ministry of Justice and read "
                "from an index of both: name, CUIT, legal form, founding "
                "date, addresses and main activity; for an IGJ company its "
                "deregistration and its reason, balance sheets and assemblies "
                "filed. No shareholders or beneficial owners are published; "
                "the people named in IGJ filings are counted, never listed."
            ),
            license="CC-BY-4.0",
            attribution=(
                "Contains information from the Registro Nacional de Sociedades "
                "and the Inspección General de Justicia, Ministerio de Justicia "
                "de la Nación (datos.jus.gob.ar), CC BY 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="AR",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, not on the LEI (the JAR gate)."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not (query or "").strip():
            return []
        conn = _shared_conn()
        if conn is None:
            return []
        cuit = argentine_cuit(query)
        tokens = re.findall(r"\w+", query)
        try:
            if cuit:
                rows = conn.execute("SELECT * FROM rns WHERE cuit = ?", (int(cuit),)).fetchall()
            elif tokens:
                match = " ".join(f'"{t}"' for t in tokens)
                rows = conn.execute(
                    "SELECT rns.* FROM rns_fts JOIN rns ON rns.cuit = rns_fts.rowid"
                    " WHERE rns_fts MATCH ? ORDER BY (upper(rns.name) = upper(?)) DESC,"
                    " length(rns.name), rns.cuit LIMIT 10",
                    (match, query.strip()),
                ).fetchall()
            else:
                rows = []
        except sqlite3.Error:
            return []
        if not rows:
            return []
        declare_snapshot()
        return [self._search_hit(dict(row)) for row in rows]

    @staticmethod
    def _search_hit(row: dict[str, Any]) -> SourceHit:
        cuit = f"{int(row['cuit']):011d}"
        segments = [f"AR {format_cuit(cuit)}"]
        form = form_label(row.get("form"))
        if form:
            segments.append(form)
        return SourceHit(
            source_id=SOURCE_ID,
            hit_id=cuit,
            kind=SearchKind.ENTITY,
            name=row.get("name") or f"CUIT {format_cuit(cuit)}",
            summary=" · ".join(segments),
            identifiers={"ar_cuit": cuit},
            raw={"cuit": cuit, "form": row.get("form")},
            is_stub=False,
            liveness="snapshot",
        )

    def _stub(self, key: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a number. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "key": key,
            "name": legal_name or "",
            "legal_name": legal_name,
            "link": RNS_DATASET_URL,
            "is_stub": True,
        }

    def _note_card(
        self, key: str, legal_name: str, meta: dict[str, str], note: str, *, reason: str
    ) -> dict[str, Any]:
        """The index answered and holds nothing usable — a note card (the
        JAR / INPI shape). Not a degradation: nothing failed."""
        bundle = self._stub(key, legal_name)
        bundle.update(
            {
                "is_stub": False,
                "not_found": True,
                "not_found_reason": reason,
                "coverage_note": note,
                "cut": meta.get("cut"),
                "igj_month": meta.get("igj_month"),
            }
        )
        return bundle

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the index's record for one CUIT or IGJ number.

        ``hit_id`` is the dispatch key (``30500001735`` or ``IGJ:1917865``)
        or any spelling of a CUIT; ``legal_name`` is GLEIF's name — the gate
        for an IGJ number, and the card's name when nothing is held.
        """
        try:
            key = normalise_ar_key(hit_id)
        except ValueError:
            logger.warning("%s: not a CUIT or an IGJ number: %r", self.id, hit_id)
            return self._stub(str(hit_id or ""), legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(key, legal_name)
        try:
            meta = read_meta()
            if key.startswith(IGJ_KEY_PREFIX):
                igj = igj_record(int(key[len(IGJ_KEY_PREFIX):]))
                if igj is None:
                    return self._note_card(key, legal_name, meta, COVERAGE_NOT_IN_INDEX, reason="not_in_index")
                if legal_name and not names_agree(igj.get("name"), legal_name):
                    return self._note_card(key, legal_name, meta, COVERAGE_NAME_MISMATCH, reason="name_mismatch")
                words = _vocab(conn, "igj_baja", igj.get("baja_code"))
                igj["baja_words"], igj["baja_class"] = words, baja_class(igj.get("deregistered"), words)
                cuit = f"{igj['cuit']:011d}" if igj.get("cuit") else None
                rns = rns_record(cuit) if cuit else None
                others = 0
            else:
                cuit = key
                rns = rns_record(cuit)
                igj, others = igj_for_cuit(cuit, rns)
                if rns is None and igj is None:
                    return self._note_card(key, legal_name, meta, COVERAGE_NOT_IN_INDEX, reason="not_in_index")
            activity_words = _vocab(conn, "activity", (rns or {}).get("activity"))
            igj_form_words = _vocab(conn, "igj_form", (igj or {}).get("form_code"))
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Argentine register index could not be read; the card shows "
                "nothing from the register. This is not a confirmation that the "
                "register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(key, legal_name)

        # Declared only once a row is in hand: a miss is not a read of data.
        declare_snapshot(meta)
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "key": key,
            "cuit": cuit,
            "name": (rns or {}).get("name") or (igj or {}).get("name") or legal_name or "",
            "legal_name": legal_name,
            "rns": _rns_block(rns, activity_words) if rns else None,
            "igj": _igj_block(igj, igj_form_words, others) if igj else None,
            "status_class": _status_class(igj),
            "cut": meta.get("cut"),
            "igj_month": meta.get("igj_month"),
            "link": RNS_DATASET_URL if rns else IGJ_DATASET_URL,
            "is_stub": False,
        }
        validate_raw(self.id, RegistroSociedadesArgentinaBundle, bundle)
        return bundle


def _status_class(igj: dict[str, Any] | None) -> str:
    """``terminal`` only from an IGJ cancellation; nothing here is ``live``
    (the RNS publishes no status, and an unflagged IGJ entry is not a
    statement that the company trades)."""
    if igj and igj.get("baja_class") == "terminal":
        return "terminal"
    return "unknown"


def _address(row: dict[str, Any], prefix: str) -> dict[str, Any] | None:
    parts = {
        k: row.get(f"{prefix}_{k}")
        for k in ("street", "number", "floor", "unit", "locality", "province", "postcode", "state")
        if row.get(f"{prefix}_{k}") not in (None, "")
    }
    return parts if set(parts) - {"state"} else None


def _rns_block(rns: dict[str, Any], activity_words: str | None) -> dict[str, Any]:
    return {
        "name": rns.get("name"),
        "form": rns.get("form"),
        "founded": rns.get("founded"),
        "updated": rns.get("updated"),
        "activity_code": rns.get("activity"),
        "activity": activity_words,
        "activities": rns.get("activities") or 0,
        "legal_address": _address(rns, "legal"),
        "fiscal_address": _address(rns, "fiscal"),
    }


def _igj_block(igj: dict[str, Any], form_words: str | None, others: int) -> dict[str, Any]:
    address = {
        k: igj.get(k)
        for k in ("street", "number", "locality", "province", "postcode")
        if igj.get(k) not in (None, "")
    }
    return {
        "correlativo": igj.get("correlativo"),
        "name": igj.get("name"),
        "form_code": igj.get("form_code"),
        "form": form_words,
        "deregistered": bool(igj.get("deregistered")),
        "baja_code": igj.get("baja_code"),
        "baja_words": igj.get("baja_words"),
        "baja_class": igj.get("baja_class") or "none",
        "people_named": igj.get("people") or 0,
        "balances": igj.get("balances") or 0,
        "last_balance": igj.get("last_balance"),
        "last_balance_filed": igj.get("last_balance_filed"),
        "last_assembly": igj.get("last_assembly"),
        "address": address or None,
        "other_entries": others,
    }
