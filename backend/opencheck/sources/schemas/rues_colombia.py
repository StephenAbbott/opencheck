"""Pydantic schema for Colombia RUES rows from datos.gov.co (SODA 2.1).

One row shape, dataset ``c82u-588k``. SODA omits a column that is null on a
row, so every field but the NIT is optional; ``extra="allow"`` keeps columns
RUES adds later.

The bundle is also a privacy check: a natural person's row or a legal
representative's identification number must never reach it (see the
adapter's docstring), and the validator refuses both.
"""

from __future__ import annotations

from pydantic import model_validator

from . import _Base

#: Columns that would carry a person's identity number or a natural person's
#: own name. The adapter never asks for them.
_FORBIDDEN_COLUMNS = frozenset(
    {
        "num_identificacion_representante_legal",
        "numero_identificacion",
        "primer_apellido",
        "segundo_apellido",
        "primer_nombre",
        "segundo_nombre",
    }
)


class RuesRow(_Base):
    """A ``c82u-588k`` row — one matrícula at one chamber of commerce."""

    nit: str
    codigo_camara: str | None = None
    camara_comercio: str | None = None
    matricula: str | None = None
    razon_social: str | None = None
    sigla: str | None = None
    digito_verificacion: str | None = None
    fecha_matricula: str | None = None
    fecha_renovacion: str | None = None
    fecha_cancelacion: str | None = None
    organizacion_juridica: str | None = None
    categoria_matricula: str | None = None
    estado_matricula: str | None = None
    clase_identificacion_rl: str | None = None
    representante_legal: str | None = None
    fecha_actualizacion: str | None = None

    @model_validator(mode="after")
    def _no_personal_data(self) -> "RuesRow":
        extra = set((self.model_extra or {}).keys())
        leaked = sorted(extra & _FORBIDDEN_COLUMNS)
        if leaked:
            raise ValueError(f"personal-data column(s) in a RUES row: {leaked}")
        if (self.organizacion_juridica or "").strip().upper() == "PERSONA NATURAL":
            raise ValueError("a natural person's row reached a RUES bundle")
        return self


class RuesColombiaBundle(_Base):
    """Top-level shape returned by ``RuesColombiaAdapter.fetch``."""

    key: str
    rows: list[RuesRow] = []
    matched_by: str | None = None
    legal_name: str | None = None
