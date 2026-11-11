"""Pydantic schema for the Argentine RNS + IGJ fetch bundles.

Only the fields the BODS mapper (``map_registro_sociedades_argentina``) reads
are declared; everything else passes through via ``extra="allow"``. The
bundle is assembled by ``RegistroSociedadesArgentinaAdapter.fetch`` from the
local SQLite index built by ``scripts/build_ar_sociedades_index.py``.

**No person data, by construction** (Stephen's decision B, 11 Nov 2026): the
IGJ's people file carries a name, a national ID number and a gender on every
row. The index holds a count only, and this schema refuses a bundle in which
any of those columns reappears, at any depth.
"""

from __future__ import annotations

from typing import Any

from pydantic import model_validator

from . import _Base

#: The IGJ ``autoridades`` columns that must never reach a bundle.
FORBIDDEN_KEYS: frozenset[str] = frozenset(
    {
        "apellido_nombre", "numero_documento", "tipo_documento",
        "descripcion_tipo_documento", "genero_autoridad", "people", "persons",
    }
)


def _forbidden_paths(value: Any, path: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            here = f"{path}.{key}" if path else str(key)
            if key in FORBIDDEN_KEYS:
                found.append(here)
            found.extend(_forbidden_paths(item, here))
    elif isinstance(value, list):
        for i, item in enumerate(value):
            found.extend(_forbidden_paths(item, f"{path}[{i}]"))
    return found


class RegistroSociedadesArgentinaBundle(_Base):
    """Top-level shape returned by RegistroSociedadesArgentinaAdapter.fetch."""

    #: The dispatch key: 11 CUIT digits, or ``IGJ:<correlativo>``.
    key: str
    cuit: str | None = None
    name: str | None = None
    legal_name: str | None = None
    rns: dict[str, Any] | None = None
    igj: dict[str, Any] | None = None
    status_class: str | None = None
    #: The RNS file's date, and the IGJ month read.
    cut: str | None = None
    igj_month: str | None = None
    link: str | None = None
    not_found: bool = False
    coverage_note: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _no_person_data(cls, data: Any) -> Any:
        paths = _forbidden_paths(data)
        if paths:
            raise ValueError(f"person data in an Argentine bundle: {', '.join(paths)}")
        return data
