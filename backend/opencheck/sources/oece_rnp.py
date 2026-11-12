"""Peru — the Registro Nacional de Proveedores (RNP): the legal make-up each
supplier to the Peruvian state declares to OECE.

The Organismo Especializado para las Contrataciones Públicas Eficientes
(OECE, formerly OSCE) keeps the RNP, the register every company must join
before it can sell to the Peruvian state. Each supplier declares its legal
make-up — its partners or shareholders (*socios / accionistas / titular*),
its legal representatives and the members of its governing body — and OECE
publishes the latest approved declaration of every supplier as one open-data
file, *Personas declaradas en la conformación jurídica de proveedores*
(``conformacion_juridica.csv``, ~227 MB, pipe-delimited, Latin-1), attached
to an anonymous-readable OECE Confluence page and refreshed on the first days
of each month.

An **index-tier** source, the ``registro_sociedades_argentina`` shape: the
lookup reads a SQLite index downloaded at boot from the ``oece-rnp-index``
release asset (``OECE_RNP_DB_URL``) and replaced when the asset changes.
Dispatch is gated on the index being present (``covers_lei``); a RUC the
index does not hold is a note card, never a failure. The index is rebuilt by
``.github/workflows/refresh-oece-rnp-index.yml`` running
``scripts/build_oece_rnp_index.py``. The Notion ticket chose a self-building
index on Render (the ``asp_moldova`` shape); the release asset was chosen in
the build because the file is 227 MB and Render's free tier restarts often —
a boot-time rebuild would download and parse it on most days. Confluence
answers a datacentre (container, Render and a GitHub runner), so the
workflow needs nothing from a laptop.

What the file is, and what it is not
------------------------------------
* **A supplier's self-declaration to a procurement regulator**, not a
  company register and not a beneficial-ownership register. Every statement
  carries a note saying so, and no interest asserts
  ``beneficialOwnershipOrControl``.
* **No percentages.** A partner or shareholder is a ``shareholding`` with no
  ``share``; the file says who is a member, not how much they hold.
* **No supplier name and no status.** The file is keyed on the supplier's
  RUC and names only the declared people and companies; the supplier's name
  is GLEIF's (or the register hop's filing's), and nothing is said about
  whether the supplier trades.
* **One snapshot.** Each supplier's latest approved declaration; no history.
  ``FECHA_INICIO_VIGENCIA`` is when the declared entry took effect in the
  RNP (most entries read 2016–2017, when the current system started), so it
  is said in the interest's details and never used as a ``startDate``.

Personal data (Ley 29733)
-------------------------
1,441,000 of 1,493,478 rows (2 Nov 2026 cut) carry a full DNI, and others a
carnet de extranjería or passport number. **No document number of a person
is stored in the index, the bundle or a statement**, and nothing derived
from one: an 8-digit DNI is enumerable, so even a hash would be reversible.
A person is therefore one node per supplier (keyed on the folded name within
that supplier's filing, the Moldova rule) and never merged across suppliers.
A corporate member's 11-digit RUC is a company identifier, not personal
data, and is kept as ``PE-RUC`` — except a RUC starting ``10``, which is a
natural person's RUC built on their DNI and is dropped.

The file's ``TIPO_DOCUMENTO`` does not say reliably whether a member is a
person or a company (Henkel AG & Co. KGaA is filed under a carnet de
extranjería; Lima Airport Partners' corporate shareholders under DNIs), so
:func:`classify_party` decides from the document's shape and the name's
legal-form words.

Licence
-------
The dataset page on datosabiertos.gob.pe reads "License Not Specified"; the
publisher's own metadata document attached to the same dataset states
*Open Data Commons Attribution License*. Stephen accepted ODC-By on that
basis (16 Sept 2026); OECE has been asked to fix the portal field.
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
from ..identifiers import peruvian_ruc, ruc_check_ok
from ..config import get_settings
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.oece_rnp import OeceRnpBundle

logger = logging.getLogger(__name__)

SOURCE_ID = "oece_rnp"

#: GLEIF Registration Authority codes under which LEI issuers file a
#: Peruvian RUC: SUNAT's RUC register (RA000689), SUNARP's Public Register of
#: Commerce (RA000482 — 94 of its 132 PE records file the RUC, the rest a
#: SUNARP partida), the SMV (RA000979) and the SBS (RA000980). Read off all
#: 654 GLEIF records with a Peruvian legal address, 12 Nov 2026. A value is
#: a RUC only when it is 11 digits with a valid check digit (``normalise_ruc``),
#: so a partida or a fund code under the same authority dispatches nothing.
PE_RA_CODES: frozenset[str] = frozenset({"RA000689", "RA000482", "RA000979", "RA000980"})
PE_SUNAT_RA_CODE = "RA000689"

#: org-id.guide's scheme for the RUC — the one OECE's own OCDS parties use.
PE_RUC_SCHEME = "PE-RUC"
PE_RUC_SCHEME_NAME = "RUC — Registro Único de Contribuyentes (SUNAT, Peru)"

#: ``meta.schema_version`` the reader accepts.
INDEX_SCHEMA_VERSION = "1"

DATASET_URL = (
    "https://www.datosabiertos.gob.pe/dataset/"
    "personas-declaradas-en-la-conformaci%C3%B3n-jur%C3%ADdica-de-proveedores-en-el-registro-nacional-de-0"
)
CONFLUENCE_PAGE_URL = "https://osce-gob-pe.atlassian.net/wiki/spaces/PNDA/pages/106889267"
#: The supplier's public profile (Ficha Única del Proveedor) — linked, never
#: fetched (its certificate chain is incomplete).
PROFILE_URL = "https://apps.osce.gob.pe/perfilprov-ui/"

LICENSE_ID = "ODC-BY-1.0"

#: The file's three kinds of entry → (code stored in the index, English).
ROLES: dict[str, tuple[str, str]] = {
    "SOCIO_ACCIONISTA_TITULAR": ("S", "partner, shareholder or owner"),
    "REPRESENTANTE_LEGAL": ("R", "legal representative"),
    "ORGANO_ADMINISTRACION": ("O", "member of the governing body"),
}
ROLE_CODES: dict[str, str] = {code: label for code, label in ROLES.values()}

#: Legal forms (``DE_FORMA_SOCIETARIA``) whose members hold shares or
#: participations — a ``SOCIO_ACCIONISTA_TITULAR`` row there is a
#: ``shareholding``. Anywhere else (an association, a cooperative, a
#: foundation, a foreign company's branch) the row is a member, an associate
#: or the foreign parent's own owner, and is mapped as
#: ``otherInfluenceOrControl`` with the register's words.
SHARE_FORMS: frozenset[str] = frozenset(
    {
        "SOCIEDAD ANONIMA CERRADA",
        "SOCIEDAD ANONIMA",
        "SOCIEDAD ANONIMA ABIERTA",
        "SOCIEDAD COMERCIAL DE RESPONSABILIDAD LIMITADA",
        "EMPRESA INDIVIDUAL DE RESPONSABILIDAD LIMITADA",
        "SOCIEDAD CIVIL DE RESPONSABILIDAD LIMITADA",
        "SOCIEDAD CIVIL",
        "SOCIEDAD COLECTIVA",
        "SOCIEDAD EN COMANDITA POR ACCIONES",
        "SOCIEDAD EN COMANDITA SIMPLE",
    }
)
#: The one form whose owner is, by law (Decreto Ley 21621), a single natural
#: person holding the whole business — the *titular*.
EIRL_FORM = "EMPRESA INDIVIDUAL DE RESPONSABILIDAD LIMITADA"

#: English glosses of the file's legal forms; the register's words are kept
#: beside them.
FORM_GLOSSES: dict[str, str] = {
    "SOCIEDAD ANONIMA CERRADA": "closely held corporation (S.A.C.)",
    "SOCIEDAD ANONIMA": "corporation (S.A.)",
    "SOCIEDAD ANONIMA ABIERTA": "publicly held corporation (S.A.A.)",
    "SOCIEDAD COMERCIAL DE RESPONSABILIDAD LIMITADA": "limited liability company (S.R.L.)",
    EIRL_FORM: "individual limited liability enterprise (E.I.R.L.)",
    "SOCIEDAD CIVIL DE RESPONSABILIDAD LIMITADA": "civil limited liability partnership",
    "SOCIEDAD CIVIL": "civil partnership",
    "SOCIEDAD COLECTIVA": "general partnership",
    "SOCIEDAD EN COMANDITA POR ACCIONES": "partnership limited by shares",
    "SOCIEDAD EN COMANDITA SIMPLE": "limited partnership",
    "ASOCIACION": "association",
    "EMPRESA EXTRANJERA": "foreign company",
    "EMPRESA COMUNAL": "communal enterprise",
    "COMUNIDAD CAMPESINA Y NATIVA": "peasant or native community",
    "COOPERATIVA": "cooperative",
    "COMITE": "committee",
    "COLEGIOS PROFESIONALES": "professional association",
    "FUNDACION": "foundation",
    "UNIVERSIDADES": "university",
    "INSTITUTOS SUPERIORES TECNOLOGICOS PERUANOS": "higher technological institute",
}

COVERAGE_NOT_IN_INDEX = (
    "This RUC is not among the suppliers whose legal make-up the Registro "
    "Nacional de Proveedores publishes, as indexed. Only companies registered "
    "to sell to the Peruvian state are in it, so most companies are not."
)

#: An entry that names a class of people rather than a party ("OTROS").
PLACEHOLDER_NAMES: frozenset[str] = frozenset(
    {
        "OTROS", "OTRO", "VARIOS", "OTROS ACCIONISTAS", "ACCIONISTAS VARIOS",
        "ACCIONISTAS MINORITARIOS", "OTROS ACCIONISTAS MINORITARIOS",
        "ACCIONISTAS", "ACCIONISTA", "SOCIOS", "SOCIOS VARIOS", "OTROS SOCIOS",
        "NO ESPECIFICADO", "NO APLICA", "NINGUNO", "-", ".", "X",
        "ACCIONES EN TESORERIA", "ACCIONES DE TESORERIA", "ACCIONISTAS DIVERSOS",
        "DIVERSOS", "DIVERSOS ACCIONISTAS", "MINORITARIOS", "OTROS MINORITARIOS",
        "PUBLICO EN GENERAL", "ACCIONISTAS PUBLICOS",
    }
)

#: Legal-form abbreviations. They mark a company only as the name's last
#: word ("QUIMPAC CORP SAC"): inside a name they are surnames ("LUIS
#: EDUARDO SAC ALARCON").
SUFFIX_TOKENS: frozenset[str] = frozenset(
    {
        "SA", "SAC", "SAA", "SRL", "EIRL", "SL", "SLU", "SAU", "SAS", "SPA",
        "SAB", "SACI", "SAPI", "CV", "SE", "LTD", "LTDA", "LLC", "LLP", "LP",
        "INC", "CORP", "GMBH", "AG", "KG", "KGAA", "BV", "NV", "PLC", "PTE",
        "PTY", "AB", "OY", "AS", "SARL", "SICAV", "CO", "CIA",
    }
)

#: Whole words (after dots and commas are removed) that mark a member as a
#: company, a fund or a public body wherever they appear in the name.
ORG_TOKENS: frozenset[str] = frozenset(
    {
        "LIMITED", "LIMITADA", "INCORPORATED", "CORPORATION", "CORPORACION",
        "SOCIETE", "COMPANY", "COMPANIA", "EMPRESA", "EMPRESAS", "SOCIEDAD",
        "HOLDING", "HOLDINGS", "GROUP", "GRUPO", "INVERSIONES", "INVESTMENTS",
        "INVESTMENT", "CAPITAL", "PARTNERS", "ENTERPRISES", "ASOCIADOS",
        "FONDO", "FUND", "FUNDS", "TRUST", "FIDEICOMISO", "FIDUCIARIA",
        "ASOCIACION", "FUNDACION", "COOPERATIVA", "CONSORCIO", "BANCO", "BANK",
        "AFP", "SEGUROS", "MUNICIPALIDAD", "GOBIERNO", "MINISTERIO",
        "UNIVERSIDAD", "INSTITUTO", "CONGREGACION", "ARZOBISPADO", "OBISPADO",
        "COMUNIDAD", "INTERNATIONAL", "INTERNACIONAL", "SUCURSAL", "SERVICIOS",
        "SERVICES", "FINANCIAL", "CONSULTING", "CONSULTORES", "CONSULTORIA",
        "CONSTRUCTORA", "INGENIERIA", "INGENIEROS", "TRADING", "COMERCIAL",
        "SOLUCIONES", "LOGISTICA", "INDUSTRIAL", "INDUSTRIAS", "PARTICIPACOES",
    }
)

#: A legal-form abbreviation written onto the end of the name with dots and
#: no space ("Siemens.s.a.", "TRADING INC-").
_GLUED_SUFFIX = re.compile(
    r"[.,\s](S\.?A\.?[CU]?|S\.?A\.?A|S\.?R\.?L|E\.?I\.?R\.?L|S\.?L\.?U?)\.?\s*-?$"
)


def _looks_like_organisation(clean: str) -> bool:
    tokens = name_tokens(clean)
    if tokens & ORG_TOKENS:
        return True
    words = re.sub(r"[.,;:()\"']", "", clean.replace("Ñ", "N")).replace("&", " ").replace("-", " ").split()
    if words and words[-1] in SUFFIX_TOKENS:
        return True
    return bool(_GLUED_SUFFIX.search(clean))


# ---------------------------------------------------------------------------
# Identifier grammar
# ---------------------------------------------------------------------------

def normalise_ruc(raw: str) -> str:
    """The dispatch key: a Peruvian RUC, 11 digits. Raises ``ValueError`` for
    anything else — a SUNARP partida, a fund code — so the source is not
    dispatched for it."""
    ruc = peruvian_ruc(raw)
    if not ruc:
        raise ValueError(f"not a Peruvian RUC: {raw!r}")
    return ruc


def is_personal_ruc(ruc: str) -> bool:
    """A RUC starting ``10`` is a natural person's, built on their DNI."""
    return ruc.startswith("10")


# ---------------------------------------------------------------------------
# Parties
# ---------------------------------------------------------------------------


def fold(text: str | None) -> str:
    """Upper case, accents removed, single spaces."""
    decomposed = unicodedata.normalize("NFKD", str(text or ""))
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.upper().split())


def name_tokens(name: str | None) -> set[str]:
    folded = fold(name).replace("Ñ", "N")
    return set(re.sub(r"[.,;:()\"']", "", folded).replace("&", " ").split())


def classify_party(name: str | None, document: str | None) -> tuple[str, str | None]:
    """``(kind, ruc)`` for one declared member.

    ``kind`` is ``E`` (a company or public body), ``A`` (an arrangement:
    a deceased person's undivided estate, "SUCESIÓN DE …", or a marital
    partnership, "SOCIEDAD CONYUGAL …" — neither a person nor a company),
    ``P`` (a person) or ``U`` (an entry naming a class of people — "OTROS"). ``ruc`` is the member's
    RUC when it is a company's: 11 digits, a valid check digit and not a
    natural person's ``10…`` RUC. Never a person's document number.
    """
    clean = fold(name)
    if not clean or re.sub(r"[.\-\s]", "", clean) == "" or clean in PLACEHOLDER_NAMES:
        return "U", None
    if re.match(r"(SUCESION|SUCESIONES|SOCIEDAD CONYUGAL)\b", clean.replace("Ó", "O")):
        return "A", None
    number = re.sub(r"\D", "", str(document or ""))
    ruc = number if ruc_check_ok(number) and not is_personal_ruc(number) else None
    if ruc and ruc[:2] in {"20", "15", "17"}:
        return "E", ruc
    if _looks_like_organisation(clean):
        return "E", None
    return "P", None


def form_label(form: str | None) -> str | None:
    text = (form or "").strip()
    if not text:
        return None
    return FORM_GLOSSES.get(text, text.capitalize())


# ---------------------------------------------------------------------------
# Index access
# ---------------------------------------------------------------------------


def db_path() -> Path:
    """``OECE_RNP_DB_FILE`` or the data-root default — one function, so the
    reader, the boot download and the sweep's ``requires_files`` agree."""
    from ..cache import data_root

    configured = get_settings().oece_rnp_db_file
    return Path(configured) if configured else data_root() / "oece_rnp.sqlite"


_SHARED: dict[str, sqlite3.Connection | None] = {}


def _connect() -> sqlite3.Connection | None:
    path = db_path()
    if not path.exists():
        if get_settings().oece_rnp_db_file:
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
    """The file's ``FECHA_CORTE`` — the register's cut."""
    meta = read_meta() if meta is None else meta
    try:
        return datetime.strptime(meta.get("cut") or "", "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


def declare_snapshot(meta: dict[str, str] | None = None) -> None:
    """Declare an index read as a snapshot: OECE's cut is the register's
    clock, ``built_at`` ours (Phase 314)."""
    meta = read_meta() if meta is None else meta
    cut = meta.get("cut") or ""
    provenance.record_snapshot(
        retrieved_at=provenance.parse_moment(meta.get("built_at")),
        source_as_of=cut_date(meta),
        detail=(
            f"OECE Registro Nacional de Proveedores, conformación jurídica, cut {cut}"
            if cut else "OECE Registro Nacional de Proveedores, conformación jurídica"
        ),
    )


def warm_index() -> dict[str, Any]:
    """Download the index at boot when absent; replace it when the release
    asset changed; keep it otherwise. Never raises."""
    from ..entity_pages import ASSET_STAMP_KEY, asset_check, download_db, record_asset_stamp
    from ..entity_pages import read_meta as read_file_meta

    settings = get_settings()
    path = db_path()
    url = settings.oece_rnp_db_url
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


def supplier_record(ruc: str) -> dict[str, Any] | None:
    conn = _shared_conn()
    if conn is None:
        return None
    row = conn.execute("SELECT * FROM supplier WHERE ruc = ?", (int(ruc),)).fetchone()
    return dict(row) if row is not None else None


def parties_of(ruc: str) -> list[dict[str, Any]]:
    """The declared entries of one supplier, in file order."""
    conn = _shared_conn()
    if conn is None:
        return []
    rows = conn.execute(
        "SELECT seq, role, kind, name, party_ruc, since FROM party WHERE ruc = ? ORDER BY seq",
        (int(ruc),),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        entry = dict(row)
        party_ruc = entry.pop("party_ruc")
        entry["ruc"] = f"{int(party_ruc):011d}" if party_ruc else None
        out.append(entry)
    return out


def supplier_count_for_member(member_ruc: str) -> int:
    """How many suppliers declare this company as a member (the reverse
    side, for the finding of a company that is itself a corporate member)."""
    conn = _shared_conn()
    if conn is None:
        return 0
    row = conn.execute(
        "SELECT COUNT(DISTINCT ruc) FROM party WHERE party_ruc = ?", (int(member_ruc),)
    ).fetchone()
    return int(row[0] or 0) if row else 0


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class OeceRnpAdapter(SourceAdapter):
    """Source adapter for the RNP's declared legal make-up of suppliers."""

    id = SOURCE_ID

    lookup_derivers = (LookupDeriver(PE_RA_CODES, "pe_ruc", normalise_ruc),)
    lookup_pass_legal_name = True

    @property
    def info(self) -> SourceInfo:
        return SourceInfo(
            id=self.id,
            name="OECE — Registro Nacional de Proveedores, declared legal make-up of suppliers (Peru)",
            homepage="https://www.gob.pe/oece",
            description=(
                "The partners and shareholders, legal representatives and "
                "governing-body members that each supplier to the Peruvian "
                "state declares to the Registro Nacional de Proveedores, kept "
                "by OECE (formerly OSCE) and published monthly as open data. "
                "A supplier's self-declaration to a procurement regulator, not "
                "a company or beneficial-ownership register: no percentages, "
                "no supplier name or status, and no document numbers of "
                "people (OpenCheck never stores them)."
            ),
            license=LICENSE_ID,
            attribution=(
                "Contains information from the Registro Nacional de "
                "Proveedores (Personas declaradas en la conformación jurídica "
                "de proveedores), Organismo Especializado para las "
                "Contrataciones Públicas Eficientes (OECE), Peru, published "
                "under the Open Data Commons Attribution License. OpenCheck "
                "removes every person's document number and decides from the "
                "name and document whether a member is a person or a company."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=index_available(),
            is_national_register=True,
            country="PE",
        )

    def covers_lei(self, lei: str) -> bool:
        """Gate dispatch on the index file, not on the LEI (the JAR gate)."""
        return index_available()

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        """A RUC search only: the file names no supplier, so there is no
        name to search."""
        if kind != SearchKind.ENTITY:
            return []
        ruc = peruvian_ruc(query)
        if not ruc or _shared_conn() is None:
            return []
        try:
            record = supplier_record(ruc)
        except sqlite3.Error:
            return []
        if record is None:
            return []
        declare_snapshot()
        segments = [f"PE RUC {ruc}"]
        form = form_label(record.get("form"))
        if form:
            segments.append(form)
        return [
            SourceHit(
                source_id=SOURCE_ID,
                hit_id=ruc,
                kind=SearchKind.ENTITY,
                name=f"RNP supplier, RUC {ruc}",
                summary=" · ".join(segments),
                identifiers={"pe_ruc": ruc},
                raw={"ruc": ruc, "form": record.get("form")},
                is_stub=False,
                liveness="snapshot",
            )
        ]

    def _stub(self, key: str, legal_name: str) -> dict[str, Any]:
        """Nothing was read: no index, or not a RUC. Dropped by the pipeline."""
        return {
            "source_id": self.id,
            "ruc": key,
            "name": legal_name or "",
            "legal_name": legal_name,
            "link": DATASET_URL,
            "is_stub": True,
        }

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """Return the RNP's declared legal make-up for one supplier RUC.

        ``legal_name`` is GLEIF's name (or the register hop's filing's): the
        file names no supplier, so it is the only name the card can carry.
        """
        try:
            ruc = normalise_ruc(hit_id)
        except ValueError:
            logger.warning("%s: not a RUC: %r", self.id, hit_id)
            return self._stub(str(hit_id or ""), legal_name)
        conn = _shared_conn()
        if conn is None:
            return self._stub(ruc, legal_name)
        try:
            meta = read_meta()
            record = supplier_record(ruc)
            if record is None:
                card = self._stub(ruc, legal_name)
                card.update(
                    {
                        "is_stub": False,
                        "not_found": True,
                        "coverage_note": COVERAGE_NOT_IN_INDEX,
                        "cut": meta.get("cut"),
                    }
                )
                return card
            parties = parties_of(ruc)
        except sqlite3.Error as exc:
            degradation.record(
                self.id,
                "The Peruvian supplier register index could not be read; the "
                "card shows nothing from it. This is not a confirmation that "
                "the register holds nothing.",
                reason=degradation.reason_for_failure(type(exc).__name__),
            )
            return self._stub(ruc, legal_name)

        declare_snapshot(meta)
        if not legal_name:
            # The file names no supplier; without a name from GLEIF or the
            # filing the mapper emits nothing, and the reader is told so.
            degradation.record(
                self.id,
                "The supplier register publishes no supplier name, and none "
                "was supplied, so its declared members could not be drawn.",
                reason=degradation.REASON_UPSTREAM_ERROR,
                check=degradation.CHECK_SOURCE_READ,
            )
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "ruc": ruc,
            "name": legal_name or "",
            "legal_name": legal_name,
            "form": record.get("form"),
            "form_id": record.get("form_id"),
            "parties": parties,
            "counts": {
                code: sum(1 for p in parties if p["role"] == code) for code in ROLE_CODES
            },
            "cut": meta.get("cut"),
            "link": DATASET_URL,
            "profile_url": PROFILE_URL,
            "is_stub": False,
        }
        validate_raw(self.id, OeceRnpBundle, bundle)
        return bundle
