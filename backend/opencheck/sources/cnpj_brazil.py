"""Brazil — Receita Federal CNPJ register adapter.

The Cadastro Nacional da Pessoa Jurídica (CNPJ), maintained by the Receita
Federal do Brasil, is Brazil's national register of legal entities and is
published as full open data. It is unusually rich: every record carries the
**QSA** (Quadro de Sócios e Administradores) — the company's partners and
administrators — which maps to BODS ownership/control relationships.

This adapter is **key-less** and two-tier:

1. **OpenCNPJ** (``api.opencnpj.org``) — a purpose-built, fast serving of the
   Receita Federal CNPJ open dataset. Primary source.
2. **BrasilAPI** (``brasilapi.com.br``) — a popular community API over the same
   public data. Fallback when OpenCNPJ errors or has no record.

Both return the same substance with slightly different field names/encodings;
``_normalise()`` collapses either into one provider-agnostic bundle so the BODS
mapper (``map_cnpj_brazil``) never sees provider differences.

The flow with GLEIF:

  1. GLEIF returns ``registeredAt.id == "RA000681"`` (Receita Federal CNPJ) and
     ``registeredAs == "33.000.167/0001-01"`` for Brazilian entities.
  2. routers/lookup.py derives ``derived["br_cnpj"]`` (14 digits) and calls
     ``fetch()`` here.

QSA / PII note: partner *names* are public Receita Federal open data; CPF is
already masked at source (``***NNNNNN**``) and age is given only as a band.
Legal-entity (PJ) partners expose their full 14-digit CNPJ, which we surface as
a cross-source identifier. Mapping this is consistent with how the brreg /
UK PSC / INPI adapters handle open officer/owner data.

Authentication: none (both providers are public, key-less).
GLEIF RA code: RA000681 (National Registry for Legal Entity — Receita Federal).
License: Brazilian public open data (Receita Federal CNPJ — Dados Públicos).
Attribution: "Contains data from the Receita Federal do Brasil CNPJ open data,
  served via OpenCNPJ (opencnpj.org) / BrasilAPI (brasilapi.com.br)."
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..cache import Cache
from ..config import get_settings
from ..http import build_client
from .base import LookupDeriver, SearchKind, SourceAdapter, SourceHit, SourceInfo

_LOG = logging.getLogger(__name__)

# Provider endpoints (both take the bare 14-digit CNPJ).
_OPENCNPJ_URL = "https://api.opencnpj.org/{cnpj}"
_BRASILAPI_URL = "https://brasilapi.com.br/api/cnpj/v1/{cnpj}"

_CACHE_NS = "cnpj_brazil"

# GLEIF Registration Authority code for the Receita Federal CNPJ register.
BR_RA_CODE: str = "RA000681"

_CNPJ_RE = re.compile(r"^\d{14}$")


def normalise_cnpj(cnpj: str | int) -> str:
    """Return the canonical 14-digit CNPJ (digits only, zero-padded).

    GLEIF stores the punctuated form ``33.000.167/0001-01``; both APIs expect
    the bare ``33000167000101``.
    """
    digits = re.sub(r"\D", "", str(cnpj))
    return digits.zfill(14) if digits else ""


def is_valid_cnpj(cnpj: str) -> bool:
    return bool(_CNPJ_RE.match(normalise_cnpj(cnpj)))


def _company_url(cnpj: str) -> str:
    """A public human-readable page for the CNPJ (OpenCNPJ web view)."""
    return f"https://opencnpj.org/{cnpj}"


def _txt(value: Any) -> str:
    """Coerce a provider field to a trimmed string.

    The providers are inconsistent: a field may arrive as a string, ``None``, a
    number, or a ``{codigo, descricao}`` object (OpenCNPJ encodes a QSA
    partner's ``pais`` this way, which previously crashed ``.strip()``).
    Objects collapse to their descriptive label; None / lists become ``""``.
    """
    if value is None:
        return ""
    if isinstance(value, dict):
        return str(
            value.get("descricao") or value.get("nome") or value.get("text") or ""
        ).strip()
    if isinstance(value, (list, tuple)):
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _partner_kind(identificador: Any) -> str:
    """Classify a QSA partner as 'entity' (PJ), 'person' (PF) or 'foreign'.

    OpenCNPJ uses text labels ("Pessoa Jurídica" / "Pessoa Física" /
    "...Exterior"); BrasilAPI uses numeric codes (1 = PJ, 2 = PF, 3 = foreign).
    """
    if isinstance(identificador, str):
        low = identificador.lower()
        if "exterior" in low or "estrangeir" in low:
            return "foreign"
        if "jur" in low:  # "jurídica"
            return "entity"
        return "person"
    try:
        code = int(identificador)
    except (TypeError, ValueError):
        return "person"
    if code == 1:
        return "entity"
    if code == 3:
        return "foreign"
    return "person"


def _legal_nature(raw: Any) -> str | None:
    """Coerce the legal-nature field (string or {codigo, descricao}) to text."""
    if isinstance(raw, dict):
        return (raw.get("descricao") or raw.get("descricao_natureza_juridica") or None)
    s = (raw or "").strip() if isinstance(raw, str) else None
    return s or None


def _norm_partner(s: dict[str, Any]) -> dict[str, Any] | None:
    """Normalise one QSA entry from either provider into a common shape."""
    name = _txt(s.get("nome_socio") or s.get("nome"))
    if not name:
        return None
    doc_raw = _txt(s.get("cnpj_cpf_socio") or s.get("cnpj_cpf_do_socio"))
    doc = re.sub(r"\D", "", doc_raw)
    masked = "*" in doc_raw
    kind = _partner_kind(s.get("identificador_socio") or s.get("identificador_de_socio"))
    entry = _txt(s.get("data_entrada_sociedade"))[:10] or None
    return {
        "name": name,
        # Full 14-digit CNPJ for a PJ partner is usable as an identifier;
        # a masked CPF (PF) is not.
        "cnpj": doc if (kind == "entity" and len(doc) == 14 and not masked) else None,
        "role": _txt(s.get("qualificacao_socio")) or None,
        "kind": kind,
        "entry_date": entry,
        # OpenCNPJ encodes pais as {codigo, descricao}; BrasilAPI as a string.
        "country": _txt(s.get("pais")) or None,
    }


class CnpjBrazilAdapter(SourceAdapter):
    """Source adapter for the Brazilian CNPJ register (Receita Federal)."""

    id = "cnpj_brazil"

    lookup_derivers = (
        LookupDeriver(frozenset({BR_RA_CODE}), "br_cnpj", normalise_cnpj),
    )
    lookup_pass_legal_name = True

    def __init__(self) -> None:
        self._cache = Cache()

    @property
    def info(self) -> SourceInfo:
        settings = get_settings()
        return SourceInfo(
            id=self.id,
            name="Receita Federal — CNPJ register (Brazil)",
            homepage="https://www.gov.br/receitafederal/",
            description=(
                "Brazilian company data from the Receita Federal CNPJ register "
                "(open data), including the QSA — partners and administrators. "
                "Served key-lessly via OpenCNPJ with a BrasilAPI fallback."
            ),
            license="BR-Open-Data",
            attribution=(
                "Contains data from the Receita Federal do Brasil CNPJ open "
                "data, served via OpenCNPJ (opencnpj.org) and BrasilAPI "
                "(brasilapi.com.br)."
            ),
            supports=[SearchKind.ENTITY],
            requires_api_key=False,
            live_available=settings.allow_live,
            is_national_register=True,
        )

    # Identifier-keyed: the providers look up by CNPJ only (no name search).
    async def search(self, query: str, kind: SearchKind) -> list[SourceHit]:
        return []

    # ------------------------------------------------------------------
    # Fetch — CNPJ lookup with OpenCNPJ → BrasilAPI fallback
    # ------------------------------------------------------------------

    async def fetch(self, hit_id: str, *, legal_name: str = "") -> dict[str, Any]:
        cnpj = normalise_cnpj(hit_id)

        def _bundle(company, partners, is_stub) -> dict[str, Any]:
            return {
                "source_id": self.id,
                "br_cnpj": cnpj,
                "company": company,
                "partners": partners or [],
                "legal_name": legal_name,
                "link": _company_url(cnpj),
                "is_stub": is_stub,
            }

        if not is_valid_cnpj(cnpj):
            return _bundle(None, [], True)

        cache_key = f"{_CACHE_NS}/company/{cnpj}"
        if not self.info.live_available and not self._cache.has(cache_key):
            return _bundle(None, [], True)

        cached = self._cache.get_payload(cache_key)
        if cached is not None:
            return cached[0]

        raw = await self._fetch_raw(cnpj)
        if raw is None:
            # Both providers failed/empty. The entity IS registered (GLEIF
            # confirmed RA000681), so surface a non-stub card using the GLEIF
            # name rather than hiding it — mirrors the CRO / JAR adapters.
            bundle = _bundle({"name": legal_name} if legal_name else None, [], False)
            self._cache.put(cache_key, bundle)
            return bundle

        bundle = self._normalise(cnpj, raw, legal_name)
        self._cache.put(cache_key, bundle)
        return bundle

    async def _fetch_raw(self, cnpj: str) -> dict[str, Any] | None:
        """Try OpenCNPJ, then BrasilAPI. Return the first JSON object found."""
        for url in (_OPENCNPJ_URL.format(cnpj=cnpj), _BRASILAPI_URL.format(cnpj=cnpj)):
            try:
                async with build_client() as client:
                    resp = await client.get(url, headers={"Accept": "application/json"})
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("cnpj_brazil: HTTP error %s: %s", url, exc)
                continue
            if resp.status_code == 404:
                continue  # not in this provider's snapshot — try the next
            if not resp.is_success:
                _LOG.warning("cnpj_brazil: HTTP %s from %s", resp.status_code, url)
                continue
            try:
                data = resp.json()
            except ValueError:
                continue
            if isinstance(data, dict) and (data.get("razao_social") or data.get("nome")):
                return data
        return None

    def _normalise(self, cnpj: str, raw: dict[str, Any], legal_name: str) -> dict[str, Any]:
        """Collapse an OpenCNPJ or BrasilAPI payload into the common bundle."""
        name = _txt(raw.get("razao_social") or raw.get("nome")) or _txt(legal_name)
        addr_parts = [
            _txt(raw.get(k))
            for k in ("logradouro", "numero", "bairro", "municipio", "uf", "cep")
        ]
        company = {
            "name": name,
            "trade_name": _txt(raw.get("nome_fantasia")) or None,
            "status": _txt(
                raw.get("situacao_cadastral") or raw.get("descricao_situacao_cadastral")
            ) or None,
            "legal_nature": _legal_nature(raw.get("natureza_juridica")),
            "founding_date": _txt(raw.get("data_inicio_atividade"))[:10] or None,
            "capital_social": raw.get("capital_social"),
            "address": ", ".join(p for p in addr_parts if p) or None,
        }
        qsa = raw.get("QSA") or raw.get("qsa") or []
        partners = [p for p in (_norm_partner(s) for s in qsa if isinstance(s, dict)) if p]
        return {
            "source_id": self.id,
            "br_cnpj": cnpj,
            "company": company,
            "partners": partners,
            "legal_name": legal_name,
            "link": _company_url(cnpj),
            "is_stub": False,
        }
