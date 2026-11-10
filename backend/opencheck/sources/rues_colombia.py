"""Colombia RUES adapter — live, keyed on the NIT (or a matrícula number).

The Registro Único Empresarial y Social (RUES) joins the mercantile registers
of Colombia's 58 chambers of commerce. Confecámaras publishes its basic record
on datos.gov.co as the Socrata dataset ``c82u-588k`` *Personas Naturales,
Personas Jurídicas y Entidades Sin Ánimo de Lucro* — 9,466,228 rows on
10 Nov 2026 (rows last updated 6 Nov 2026; Confecámaras states a monthly
refresh), **CC BY-SA 4.0**.

One row is one *matrícula* (registration) at one chamber: chamber, matrícula
number, name (razón social) and acronym (sigla), NIT and check digit, CIIU
codes, registration / renewal / cancellation dates, legal organisation and
status (``estado_matricula``), and the **legal representative's name, ID type
and ID number**. There are no shareholders and no beneficial owners; the legal
representative is the only person on a company's row.

Personal data
-------------

6,982,674 rows are natural persons (sole traders), keyed on a cédula — a
personal identity number — and the legal representative's identification is
a cédula too. **Neither is ever asked for.** Every query names its columns
(``$select``) and leaves out ``num_identificacion_representante_legal`` and
the natural persons' name columns, and every row is filtered to the legal
person category before it is kept. ``RuesColombiaBundle`` refuses a bundle
carrying a natural person's row or the representative's number. The
representative is carried by **name and role only** (the Estonian
personal-code rule).

The flow
--------

GLEIF files Colombian companies under 58 chamber RA codes (RA000094–RA000150)
and the RUE itself (RA000093); RA000848 is the Superintendencia Financiera,
which files funds and supervised entities. Measured on the 583 ISSUED LEIs
with a Colombian legal address (10 Nov 2026): RA000093 245, RA000101 (Bogotá)
152, RA999999 52, RA000125 (Medellín) 35, RA000848 14, the rest a handful
each. ``registeredAs`` comes in two families:

* **A NIT** — with the check digit (``902033505-1``, ``900.986.856 - 3``,
  ``00000900625884 - 1``, ``8300977829``) or without it (``890933815``).
  :func:`parse_registered_as` keeps a NIT only when the DIAN check digit
  agrees, or when nine bare digits start with 8 or 9 (company NITs do).
* **A matrícula** — Bogotá pads it (``0000759723``, ``02045154``), Medellín
  writes ``21-075155-12`` (the dataset's ``7515512``), Cali appends it
  (``080001177589 / Matrícula 1177589``). The chamber the RA code names is
  **not** reliable (FIBRATECH INTERNATIONAL files ``0000759723`` under
  Bogotá's RA code; matrícula ``759723`` is Barranquilla's), so a matrícula
  is searched across chambers and **name-gated**: exactly one legal-person
  row whose name agrees with GLEIF's, or a note card.

Resolution over the 583 (10 Nov 2026): 440 by NIT, 60 by matrícula, 52 with
no number (RA999999), 31 not in RUES — mostly funds and *patrimonios
autónomos* (trusts held by a fiduciary, not merchants), universities and the
Banco de la República, which no chamber registers.

**No name gate on a NIT.** A check-digit-valid NIT is national and unique;
of the 440, 37 names differ from GLEIF's and every one is the same company
under a fuller or newer name — RUES writes the permitted alternative names
into the razón social ("BANCOLOMBIA S.A, ADEMÁS PODRÁ GIRAR BAJO LA
DENOMINACIÓN BANCO DE COLOMBIA S.A."), or GLEIF holds an old name (BASF
COATINGS COLOMBIA S.A.S → SURVENTIS S.A.S). The Taiwan UBN precedent.

One NIT, several rows
---------------------

A company that moves its domicile gets a new matrícula at the new chamber;
the old one reads "MATRÍCULA CANCELADA POR TRASLADO DE DOMICILIO" and the new
one, at first, "MATRÍCULA NUEVA, CONSTITUCIÓN POR TRASLADO". :func:`summarise`
picks the current row (ACTIVA first, then the new-by-transfer row, then the
latest update) and keeps the others as ``other_registrations``.

Rate limits
-----------

Socrata throttles unauthenticated requests from a shared per-IP pool;
``SOCRATA_APP_TOKEN`` (the one ``ny_dos`` already reads) is sent as
``X-App-Token``. A NIT or matrícula query answers in 0.3–0.5 s (10 Nov 2026).

License: CC BY-SA 4.0, attribution "Confecámaras — Registro Único Empresarial
y Social (RUES), via datos.gov.co".
"""

from __future__ import annotations

import asyncio
import logging
import re
import unicodedata
import weakref
from typing import Any
from urllib.parse import urlencode

from .. import degradation, names
from ..identifiers import colombian_nit, nit_check_digit
from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from ..outbound_rate import TokenBucket, current_budget
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo
from .schemas import validate_raw
from .schemas.rues_colombia import RuesColombiaBundle

_LOG = logging.getLogger(__name__)

_CACHE_NS = "rues_colombia"

#: The RUE itself and the 58 chambers of commerce (GLEIF RA list, read live
#: 10 Nov 2026: every RA whose jurisdiction is CO), plus the Superintendencia
#: Financiera, which files supervised companies' NITs beside its funds'.
RUE_RA_CODE = "RA000093"
SUPERFINANCIERA_RA_CODE = "RA000848"
CO_CHAMBER_RA_CODES: frozenset[str] = frozenset(
    f"RA{n:06d}" for n in range(94, 151)
)
CO_RA_CODES: frozenset[str] = (
    CO_CHAMBER_RA_CODES | {RUE_RA_CODE, SUPERFINANCIERA_RA_CODE}
)

#: org-id.guide's code for the RUES; its identifier is the NIT.
CO_RUE_SCHEME = "CO-RUE"
CO_RUE_SCHEME_NAME = "NIT — Registro Único Empresarial y Social (RUES), Colombia"

DATASET = "c82u-588k"
_SODA_BASE = "https://www.datos.gov.co/resource"
DATASET_PAGE = (
    "https://www.datos.gov.co/Comercio-Industria-y-Turismo/"
    "Personas-Naturales-Personas-Jur-dicas-y-Entidades-/c82u-588k"
)

#: The legal-person category. Everything else is a natural person.
LEGAL_PERSON_CATEGORY = "SOCIEDAD ó PERSONA JURIDICA PRINCIPAL ó ESAL"
_NATURAL_PERSON = "PERSONA NATURAL"

#: Columns asked for. Deliberately absent: the natural persons' name columns
#: (``primer_apellido`` …), ``numero_identificacion`` (a natural person's
#: cédula; for a company it repeats ``nit``), ``inscripcion_proponente`` and
#: ``num_identificacion_representante_legal`` — a person's identity number.
_SELECT = (
    "codigo_camara,camara_comercio,matricula,razon_social,sigla,"
    "codigo_clase_identificacion,clase_identificacion,nit,digito_verificacion,"
    "cod_ciiu_act_econ_pri,cod_ciiu_act_econ_sec,fecha_matricula,"
    "fecha_renovacion,ultimo_ano_renovado,fecha_vigencia,fecha_cancelacion,"
    "codigo_tipo_sociedad,tipo_sociedad,codigo_organizacion_juridica,"
    "organizacion_juridica,codigo_categoria_matricula,categoria_matricula,"
    "codigo_estado_matricula,estado_matricula,clase_identificacion_rl,"
    "representante_legal,fecha_actualizacion"
)

_ROW_LIMIT = 100
_SEARCH_LIMIT = 20
#: One lookup costs one query (NIT) or two (matrícula, then its NIT).
_LOOKUP_CALL_BUDGET = 6
_RATE_PER_MINUTE = 120.0
_BURST = 4
_LOOKUP_TIMEOUT_S = 30.0
_CACHE_MAX_AGE_DAYS = 1.0

STATUS_ACTIVE = "ACTIVA"
STATUS_NEW_BY_TRANSFER = "MATRÍCULA NUEVA, CONSTITUCIÓN POR TRASLADO"
STATUS_MOVED = "MATRÍCULA CANCELADA POR TRASLADO DE DOMICILIO"
#: Statuses that end the company's registration (not its move elsewhere).
TERMINAL_STATUSES: tuple[str, ...] = (
    "CANCELADA",
    "MATRÍCULA CANCELADA LEY 1429",
    "MATRÍCULA CANCELADA POR FUSIÓN",
)
LIVE_STATUSES: tuple[str, ...] = (STATUS_ACTIVE, STATUS_NEW_BY_TRANSFER)

#: Words with which a razón social starts listing the other names the company
#: may use ("PUDIENDO UTILIZAR …", "Y PODRÁ IDENTIFICARSE …", "SIGLA …").
_ALIAS_CLAUSE = re.compile(
    r"\s*[,;]?\s*\b(?:Y\s+)?(?:PUDIENDO|PODR[AÁ]|TAMBI[EÉ]N\s+PODR[AÁ]|"
    r"QUE\s+PODR[AÁ]|PERO\s+PODR[AÁ]|ADEM[AÁ]S|SIGLA)\b.*$",
    re.IGNORECASE | re.DOTALL,
)

COVERAGE_NOT_IN_REGISTER = (
    "RUES holds no company with the number on the LEI record. Funds, trusts "
    "held by a fiduciary (patrimonios autónomos), universities and other "
    "bodies no chamber of commerce registers are not in it."
)
COVERAGE_NAME_MISMATCH = (
    "The matrícula number on the LEI record belongs to a different company in "
    "RUES, so no record is attached."
)
COVERAGE_AMBIGUOUS = (
    "The matrícula number on the LEI record matches more than one company in "
    "RUES (each chamber numbers its own), so no record is attached."
)

_BUCKETS: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, TokenBucket]" = (
    weakref.WeakKeyDictionary()
)


def _bucket() -> TokenBucket:
    loop = asyncio.get_running_loop()
    bucket = _BUCKETS.get(loop)
    if bucket is None:
        bucket = TokenBucket(_RATE_PER_MINUTE, capacity=_BURST, name="rues_colombia")
        _BUCKETS[loop] = bucket
    return bucket


# ---------------------------------------------------------------------------
# Numbers
# ---------------------------------------------------------------------------


def parse_registered_as(value: Any) -> tuple[str, str]:
    """``("nit", base)`` or ``("matricula", number)`` from GLEIF's value.

    Raises ``ValueError`` for anything else (an empty value, a foreign
    code). See the module docstring for the shapes.
    """
    text = " ".join(str(value or "").split())
    if not text:
        raise ValueError("empty")
    explicit = re.search(r"matr[ií]cula\D*(\d+)", text, re.IGNORECASE)
    if explicit:
        number = explicit.group(1).lstrip("0")
        if number:
            return ("matricula", number)
    nit = colombian_nit(text)
    if nit:
        return ("nit", nit.split("-")[0])
    medellin = re.fullmatch(r"\d{2}-(\d{6})-(\d{2})", text)
    if medellin:
        return ("matricula", str(int(medellin.group(1) + medellin.group(2))))
    compact = text.replace(".", "")
    if compact.isdigit():
        stripped = compact.lstrip("0")
        if not stripped:
            raise ValueError(f"not a RUES number: {value!r}")
        if len(stripped) == 9 and stripped[0] in "89":
            return ("nit", stripped)
        if len(stripped) <= 10:
            return ("matricula", stripped)
    raise ValueError(f"not a RUES number: {value!r}")


def normalise_rues_key(value: Any) -> str:
    """The lookup key: ``900396759`` for a NIT, ``M7515512`` for a matrícula.

    Also accepts a key already in that form, so a register hop or a
    ``/deepen`` call can pass it back.
    """
    text = " ".join(str(value or "").split())
    if re.fullmatch(r"M[1-9]\d{0,11}", text):
        return text
    return _from_parsed(parse_registered_as(text))


def _from_parsed(parsed: tuple[str, str]) -> str:
    kind, number = parsed
    return number if kind == "nit" else f"M{number}"


# ---------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------


def clean_field(value: Any) -> str:
    return " ".join(str(value or "").split())


def ymd(value: Any) -> str | None:
    """``YYYY-MM-DD`` from RUES's ``YYYYMMDD``; None for 0 / 99991231."""
    text = clean_field(value)
    match = re.fullmatch(r"(\d{4})(\d{2})(\d{2})", text)
    if not match:
        return None
    year, month, day = (int(g) for g in match.groups())
    if year < 1800 or year >= 9999 or not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def update_day(value: Any) -> str | None:
    """``YYYY-MM-DD`` from ``fecha_actualizacion`` (``2026/10/23 08:10:16…``)."""
    match = re.match(r"(\d{4})[/-](\d{2})[/-](\d{2})", clean_field(value))
    return "-".join(match.groups()) if match else None


def is_legal_person(row: dict[str, Any]) -> bool:
    category = clean_field(row.get("categoria_matricula"))
    org = clean_field(row.get("organizacion_juridica")).upper()
    return category == LEGAL_PERSON_CATEGORY and org != _NATURAL_PERSON


def legal_name(razon_social: str) -> tuple[str, str | None]:
    """``(name, alias clause)``: the razón social cut where it starts listing
    the other names the company may use. The clause is returned as filed."""
    text = clean_field(razon_social)
    match = _ALIAS_CLAUSE.search(text)
    if not match or match.start() == 0:
        return text, None
    head = text[: match.start()].rstrip(" ,;-")
    return (head or text), text[match.start():].strip(" ,;") or None


def _fold(text: str) -> str:
    return (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().upper()
    )


def names_agree(gleif_name: str | None, row: dict[str, Any]) -> bool:
    """Does the row's name (or sigla) agree with GLEIF's legal name?"""
    wanted = clean_field(gleif_name)
    if not wanted:
        return True
    candidates = [clean_field(row.get("razon_social")), clean_field(row.get("sigla"))]
    candidates.append(legal_name(candidates[0])[0])
    for candidate in candidates:
        if not candidate:
            continue
        a, b = names.org_comparable_name(_fold(wanted)), names.org_comparable_name(_fold(candidate))
        if a and a == b:
            return True
        if a and names.despace(a) == names.despace(b):
            return True
        if names.distinctive_token_agreement(_fold(wanted), _fold(candidate)):
            return True
        if names.name_similarity(_fold(wanted), _fold(candidate)) >= names.NAME_MATCH_THRESHOLD:
            return True
    return False


def _rank(row: dict[str, Any]) -> tuple[int, str]:
    status = clean_field(row.get("estado_matricula"))
    order = {STATUS_ACTIVE: 0, STATUS_NEW_BY_TRANSFER: 1}.get(status, 2)
    # Latest update first within a tier: negate by sorting reversed below.
    return (order, clean_field(row.get("fecha_actualizacion")))


def current_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The row that describes the company now (see the module docstring)."""
    if not rows:
        return None
    best_tier = min(_rank(r)[0] for r in rows)
    tier = [r for r in rows if _rank(r)[0] == best_tier]
    return max(tier, key=lambda r: clean_field(r.get("fecha_actualizacion")))


def summarise(bundle: dict[str, Any]) -> dict[str, Any]:
    """Everything the mapper, the finding and the hit builder read."""
    rows = [r for r in bundle.get("rows") or [] if isinstance(r, dict)]
    row = current_row(rows) or {}
    name, alias_clause = legal_name(clean_field(row.get("razon_social")))
    nit = clean_field(row.get("nit"))
    dv = clean_field(row.get("digito_verificacion"))
    nit_display = f"{nit}-{dv}" if nit and dv.isdigit() else nit
    registered = [d for d in (ymd(r.get("fecha_matricula")) for r in rows) if d]
    others = [
        {
            "chamber": clean_field(r.get("camara_comercio")),
            "matricula": clean_field(r.get("matricula")),
            "status": clean_field(r.get("estado_matricula")),
            "registered_on": ymd(r.get("fecha_matricula")),
            "cancelled_on": ymd(r.get("fecha_cancelacion")),
        }
        for r in rows
        if r is not row
    ]
    rep_class = clean_field(row.get("clase_identificacion_rl")).upper()
    representative = clean_field(row.get("representante_legal"))
    return {
        "nit": nit,
        "nit_display": nit_display,
        "name": name,
        "razon_social": clean_field(row.get("razon_social")),
        "alias_clause": alias_clause,
        "sigla": clean_field(row.get("sigla")),
        "chamber": clean_field(row.get("camara_comercio")),
        "matricula": clean_field(row.get("matricula")),
        "status": clean_field(row.get("estado_matricula")),
        "registered_on": ymd(row.get("fecha_matricula")),
        "first_registered_on": min(registered) if registered else None,
        "renewed_on": ymd(row.get("fecha_renovacion")),
        "last_year_renewed": clean_field(row.get("ultimo_ano_renovado")) or None,
        "cancelled_on": ymd(row.get("fecha_cancelacion")),
        "legal_form": clean_field(row.get("organizacion_juridica")),
        "company_type": clean_field(row.get("tipo_sociedad")),
        "ciiu": clean_field(row.get("cod_ciiu_act_econ_pri")) or None,
        "updated_on": update_day(row.get("fecha_actualizacion")),
        "representative": representative or None,
        "representative_is_entity": rep_class == "NIT",
        "other_registrations": others,
    }


def record_url(nit: str) -> str:
    """The SODA query for the company's rows (RUES's own search is a form)."""
    return f"{_SODA_BASE}/{DATASET}.json?" + urlencode(
        {"$select": _SELECT, "nit": nit}
    )


class RuesColombiaAdapter(SourceAdapter):
    """Source adapter for Colombia's RUES (Confecámaras, datos.gov.co)."""

    id = "rues_colombia"

    lookup_derivers = (
        LookupDeriver(CO_RA_CODES, "co_rues", normalise_rues_key),
    )
    lookup_pass_legal_name = True
    lookup_timeout_s = _LOOKUP_TIMEOUT_S

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Colombia RUES — chambers of commerce (Confecámaras)",
            homepage=DATASET_PAGE,
            description=(
                "Colombian company records from the Registro Único Empresarial "
                "y Social (RUES), the joined mercantile registers of Colombia's "
                "chambers of commerce, looked up by NIT: name, legal form, "
                "status, registration and renewal dates, the chamber and "
                "matrícula, and the legal representative named on the record."
            ),
            license="CC-BY-SA-4.0",
            attribution=(
                "Personas Naturales, Personas Jurídicas y Entidades Sin Ánimo "
                "de Lucro — Confecámaras, Registro Único Empresarial y Social "
                "(RUES), via datos.gov.co, licensed under CC BY-SA 4.0."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
            country="CO",
        )

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    async def _query(self, where: str, *, limit: int = _ROW_LIMIT) -> list[dict[str, Any]] | None:
        """Legal-person rows for one SODA query, or None when it failed."""
        budget = current_budget(self.id, _LOOKUP_CALL_BUDGET)
        if budget is not None and not budget.take():
            degradation.record(
                self.id,
                (
                    f"RUES per-lookup call budget of {_LOOKUP_CALL_BUDGET} "
                    "Socrata requests reached; the record was not fetched"
                ),
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        params = {"$select": _SELECT, "$where": where, "$limit": str(limit)}
        headers = {"Accept": "application/json"}
        token = getattr(get_settings(), "socrata_app_token", None)
        if token:
            headers["X-App-Token"] = token

        await _bucket().acquire()
        async with build_client() as client:
            response = await client.get(
                f"{_SODA_BASE}/{DATASET}.json", params=params, headers=headers
            )
        if response.status_code == 429:
            degradation.record(
                self.id, "datos.gov.co returned HTTP 429",
                reason=degradation.REASON_RATE_LIMITED,
            )
            return None
        if not response.is_success:
            degradation.record(self.id, f"datos.gov.co returned HTTP {response.status_code}")
            return None
        try:
            payload = response.json()
        except ValueError:
            degradation.record(self.id, "datos.gov.co returned a non-JSON body")
            return None
        if not isinstance(payload, list):
            degradation.record(self.id, "datos.gov.co returned an unexpected shape")
            return None
        return [r for r in payload if isinstance(r, dict) and is_legal_person(r)]

    # ------------------------------------------------------------------
    # Search — legal persons only, by the start of the name
    # ------------------------------------------------------------------

    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        if kind != SearchKind.ENTITY or not self.info.live_available:
            return []
        cleaned = clean_field(query)
        if len(cleaned) < 3:
            return []
        try:
            key = normalise_rues_key(cleaned)
        except ValueError:
            key = None
        if key and not key.startswith("M"):
            rows = await self._query(f"nit='{key}'")
        else:
            literal = cleaned.upper().replace("'", "''")
            rows = await self._query(
                f"starts_with(razon_social, '{literal}') AND "
                f"categoria_matricula='{LEGAL_PERSON_CATEGORY}'",
                limit=_SEARCH_LIMIT * 3,
            )
        if not rows:
            return []
        by_nit: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            nit = clean_field(row.get("nit"))
            if nit:
                by_nit.setdefault(nit, []).append(row)
        hits: list[SourceHit] = []
        for nit, group in list(by_nit.items())[:_SEARCH_LIMIT]:
            s = summarise({"rows": group})
            hits.append(
                SourceHit(
                    source_id=self.id,
                    hit_id=nit,
                    kind=SearchKind.ENTITY,
                    name=s["name"] or nit,
                    summary=f"{CO_RUE_SCHEME} {s['nit_display']}"
                    + (f" · {s['status']}" if s["status"] else ""),
                    identifiers={"co_nit": s["nit_display"]},
                    raw={"nit": nit, "status": s["status"], "chamber": s["chamber"]},
                )
            )
        return hits

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        """RUES's legal-person rows for one NIT (or a name-gated matrícula)."""
        try:
            key = normalise_rues_key(hit_id)
        except ValueError:
            return self._bundle(key=str(hit_id or ""), legal_name=legal_name)

        if key.startswith("M"):
            return await self._fetch_matricula(key, legal_name)
        rows = await self._rows_for_nit(key)
        if rows is None:
            return self._bundle(key=key, legal_name=legal_name)
        if not rows:
            return self._bundle(key=key, legal_name=legal_name, not_in_register=True)
        return self._bundle(key=key, legal_name=legal_name, rows=rows, matched_by="nit")

    async def _fetch_matricula(self, key: str, gleif_name: str) -> dict[str, Any]:
        number = key[1:]
        cache_key = f"{_CACHE_NS}/matricula/{number}"
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], list):
            candidates = cached[0]
        else:
            if not self.info.live_available:
                return self._bundle(key=key, legal_name=gleif_name)
            fetched = await self._query(f"matricula='{number}'")
            if fetched is None:
                return self._bundle(key=key, legal_name=gleif_name)
            candidates = fetched
            self._cache.put(cache_key, candidates)
        if not candidates:
            return self._bundle(key=key, legal_name=gleif_name, not_in_register=True)
        agreeing = [r for r in candidates if names_agree(gleif_name, r)]
        nits = {clean_field(r.get("nit")) for r in agreeing if clean_field(r.get("nit"))}
        if not agreeing:
            registered = legal_name(clean_field(candidates[0].get("razon_social")))[0]
            return self._bundle(
                key=key, legal_name=gleif_name, name_mismatch=True,
                registered_name=registered if len(candidates) == 1 else None,
            )
        if len(nits) > 1 or (not nits and len(agreeing) > 1):
            return self._bundle(key=key, legal_name=gleif_name, ambiguous=True)
        nit = next(iter(nits), "")
        rows = await self._rows_for_nit(nit) if nit else None
        if not rows:
            rows = agreeing
        return self._bundle(key=key, legal_name=gleif_name, rows=rows, matched_by="matricula")

    async def _rows_for_nit(self, nit: str) -> list[dict[str, Any]] | None:
        cache_key = f"{_CACHE_NS}/nit/{nit}"
        cached = self._cache.get_payload(cache_key, max_age_days=_CACHE_MAX_AGE_DAYS)
        if cached is not None and isinstance(cached[0], list):
            return cached[0]
        if not self.info.live_available:
            return None
        rows = await self._query(f"nit='{nit}'")
        if rows is None:
            return None
        self._cache.put(cache_key, rows)
        return rows

    # ------------------------------------------------------------------

    def _bundle(
        self,
        *,
        key: str,
        legal_name: str,
        rows: list[dict[str, Any]] | None = None,
        matched_by: str | None = None,
        not_in_register: bool = False,
        name_mismatch: bool = False,
        ambiguous: bool = False,
        registered_name: str | None = None,
    ) -> dict[str, Any]:
        has_record = bool(rows)
        answered_empty = not_in_register or name_mismatch or ambiguous
        bundle: dict[str, Any] = {
            "source_id": self.id,
            "key": key,
            "legal_name": legal_name,
            "rows": [dict(r) for r in rows or []],
            "matched_by": matched_by,
            "is_stub": not has_record and not answered_empty,
        }
        if answered_empty:
            bundle["not_found"] = True
        if not_in_register:
            bundle["not_in_register"] = True
            bundle["coverage_note"] = COVERAGE_NOT_IN_REGISTER
        if name_mismatch:
            bundle["name_mismatch"] = True
            bundle["coverage_note"] = COVERAGE_NAME_MISMATCH
            if registered_name:
                bundle["registered_name"] = registered_name
        if ambiguous:
            bundle["ambiguous"] = True
            bundle["coverage_note"] = COVERAGE_AMBIGUOUS
        if has_record:
            validate_raw(self.id, RuesColombiaBundle, bundle)
        return bundle


__all__ = [
    "CO_RA_CODES",
    "CO_RUE_SCHEME",
    "CO_RUE_SCHEME_NAME",
    "COVERAGE_AMBIGUOUS",
    "COVERAGE_NAME_MISMATCH",
    "COVERAGE_NOT_IN_REGISTER",
    "DATASET",
    "LEGAL_PERSON_CATEGORY",
    "LIVE_STATUSES",
    "RuesColombiaAdapter",
    "STATUS_ACTIVE",
    "STATUS_MOVED",
    "TERMINAL_STATUSES",
    "clean_field",
    "colombian_nit",
    "current_row",
    "is_legal_person",
    "legal_name",
    "names_agree",
    "nit_check_digit",
    "normalise_rues_key",
    "parse_registered_as",
    "record_url",
    "summarise",
    "ymd",
]
