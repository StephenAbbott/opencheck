"""Build the Argentine companies index from datos.jus.gob.ar (RNS + IGJ).

Two datasets of Argentina's Ministry of Justice, both CC BY 4.0 and keyless,
published on the ``datos.jus.gob.ar`` CKAN portal:

* **Registro Nacional de Sociedades** (RNS) — every legal person on the
  national register, nationwide, keyed on the CUIT: name, legal form,
  founding (contract) date, last update, legal and fiscal addresses and the
  ARCA activity codes. One CSV in a yearly ZIP, refreshed about monthly.
* **Inspección General de Justicia** (IGJ) — the City of Buenos Aires
  register: every entity it has registered, keyed on its own ``numero
  correlativo``, with the CUIT where it holds one, the legal form, the
  deregistration flag with its reason, the registered address, the balance
  sheets and assemblies filed and the people named in its filings. A
  half-yearly ZIP holding monthly full cuts of five CSVs; the newest month
  is read.

Usage
-----
    python3 scripts/build_ar_sociedades_index.py --download --out ar_sociedades.sqlite --gzip
    python3 scripts/build_ar_sociedades_index.py --rns-zip rns.zip --igj-zip igj.zip \
        --out ar_sociedades.sqlite

What is kept, and what is never kept (Stephen's decisions, 11 Nov 2026)
-----------------------------------------------------------------------
* RNS: one row per CUIT (the file repeats the company on every activity
  row), the main active activity, both addresses.
* IGJ: every entity **except the natural-person forms** (sole traders,
  auctioneers, brokers, customs agents, unregistered individuals — forms
  210–260 bar 226): their "name" is a person's name.
* IGJ people (``autoridades``): **a count per entity, nothing else.** The
  file carries a national ID number and a gender on every row and records
  no date or role; the index holds neither a name nor a number.
* Deregistration is kept as the IGJ code; the adapter reads the reason.

The builder refuses a changed header, a short file, too many malformed rows,
or a ZIP that is not the dataset, and writes nothing when it refuses.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from opencheck.identifiers import argentine_cuit  # noqa: E402
from opencheck.sources.registro_sociedades_argentina import (  # noqa: E402
    INDEX_SCHEMA_VERSION,
    NATURAL_PERSON_FORMS,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("build_ar_sociedades_index")

USER_AGENT = "OpenCheck/1.0 (+https://opencheck.world; index builder)"
API = "https://datos.jus.gob.ar/api/3/action"
RNS_DATASET = "registro-nacional-de-sociedades"
IGJ_DATASET = "entidades-constituidas-en-la-inspeccion-general-de-justicia-igj"

#: The yearly RNS ZIP ("Registro Nacional de Sociedades - 2026"); the
#: associations file, the bimonthly extracts and the sample are other names.
RNS_RESOURCE_RE = re.compile(r"^Registro Nacional de Sociedades\s*-\s*(\d{4})\s*$")
#: The half-yearly IGJ ZIP ("IGJ - 2026 semestre 2").
IGJ_RESOURCE_RE = re.compile(r"^IGJ\s*-\s*(\d{4})\s+semestre\s+(\d)\s*$", re.IGNORECASE)

RNS_MEMBER_RE = re.compile(r"registro-nacional-sociedades-(\d{8})\.csv$")
IGJ_MEMBER_RE = re.compile(r"igj-(entidades|domicilios|autoridades|balances|asambleas)-(\d{6})\.csv$")

RNS_HEADER = (
    "cuit", "razon_social", "fecha_hora_contrato_social", "tipo_societario",
    "fecha_hora_actualizacion", "numero_inscripcion", "dom_fiscal_provincia",
    "dom_fiscal_localidad", "dom_fiscal_calle", "dom_fiscal_numero",
    "dom_fiscal_piso", "dom_fiscal_departamento", "dom_fiscal_cp",
    "dom_fiscal_estado_domicilio", "dom_legal_provincia", "dom_legal_localidad",
    "dom_legal_calle", "dom_legal_numero", "dom_legal_piso",
    "dom_legal_departamento", "dom_legal_cp", "dom_legal_estado_domicilio",
    "actividad_codigo", "actividad_descripcion", "actividad_orden",
    "actividad_estado", "actividad_vigencia",
)
IGJ_HEADERS = {
    "entidades": (
        "numero_correlativo", "tipo_societario", "descripcion_tipo_societario",
        "razon_social", "dada_de_baja", "codigo_baja", "detalle_baja", "cuit",
    ),
    "domicilios": (
        "numero_correlativo", "tipo_societario", "descripcion_tipo_societario",
        "razon_social", "tipo_domicilio", "descripcion_tipo_domicilio", "calle",
        "numero", "localidad", "provincia", "codigo_postal",
    ),
    # Read for a count only. The name, document and gender columns are never
    # copied anywhere.
    "autoridades": (
        "numero_correlativo", "apellido_nombre", "tipo_administrador",
        "descripcion_tipo_administrador", "tipo_documento",
        "descripcion_tipo_documento", "numero_documento", "genero_autoridad",
    ),
    "balances": (
        "numero_correlativo", "tipo_societario", "descripcion_tipo_societario",
        "razon_social", "fecha_balance", "fecha_presentacion", "capital_informado",
    ),
    "asambleas": (
        "numero_correlativo", "tipo_societario", "descripcion_tipo_societario",
        "razon_social", "tipo_asamblea", "descripcion_tipo_asamblea",
        "numero_asamblea", "fecha_realizacion", "fecha_presentacion",
    ),
}

#: Below these the input is not the register (18 Sept 2026: 1,270,807 CUITs;
#: Sept 2026 IGJ cut: 1,076,717 entities, ~1,020,000 once persons go).
MIN_RNS_CUITS = 1_000_000
MIN_IGJ_ENTITIES = 800_000
#: RNS rows whose CUIT is not 11 digits with a valid check digit.
MAX_RNS_MALFORMED = 2_000

SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
CREATE TABLE rns (
  cuit INTEGER PRIMARY KEY, name TEXT, form TEXT, founded TEXT, updated TEXT,
  activity TEXT, activities INTEGER,
  legal_province TEXT, legal_locality TEXT, legal_street TEXT, legal_number TEXT,
  legal_floor TEXT, legal_unit TEXT, legal_postcode TEXT, legal_state TEXT,
  fiscal_province TEXT, fiscal_locality TEXT, fiscal_street TEXT, fiscal_number TEXT,
  fiscal_floor TEXT, fiscal_unit TEXT, fiscal_postcode TEXT, fiscal_state TEXT
);
CREATE TABLE igj (
  correlativo INTEGER PRIMARY KEY, form_code INTEGER, name TEXT, cuit INTEGER,
  deregistered INTEGER NOT NULL, baja_code TEXT,
  people INTEGER, balances INTEGER, last_balance TEXT, last_balance_filed TEXT,
  last_assembly TEXT,
  street TEXT, number TEXT, locality TEXT, province TEXT, postcode TEXT
);
CREATE TABLE vocab (kind TEXT NOT NULL, code TEXT NOT NULL, words TEXT, PRIMARY KEY (kind, code));
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
"""
INDEXES = """
CREATE INDEX igj_cuit ON igj(cuit) WHERE cuit IS NOT NULL;
CREATE VIRTUAL TABLE rns_fts USING fts5(name, content='rns', content_rowid='cuit', tokenize='unicode61 remove_diacritics 2');
INSERT INTO rns_fts(rns_fts) VALUES ('rebuild');
"""


class RefusedInput(Exception):
    """The input is not the dataset as this builder knows it."""


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def _iso_day(raw: Any) -> str | None:
    """``1912-04-30-00:00`` / ``2026-09-18 10:12`` / ``20260918`` → ISO day."""
    text = str(raw or "").strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text) or re.fullmatch(r"(\d{4})(\d{2})(\d{2})", text)
    if not m:
        return None
    y, mo, d = (int(g) for g in m.groups())
    if not (1800 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


def _int(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _reader(zf: zipfile.ZipFile, member: str, header: tuple[str, ...]) -> csv.DictReader:
    fh = io.TextIOWrapper(zf.open(member), encoding="utf-8-sig", newline="")
    reader = csv.DictReader(fh)
    got = tuple(reader.fieldnames or ())
    missing = [h for h in header if h not in got]
    if missing:
        raise RefusedInput(f"{member}: columns missing: {', '.join(missing)}")
    return reader


def rns_member(zf: zipfile.ZipFile) -> tuple[str, str]:
    """The ZIP's one RNS CSV and its cut (``YYYY-MM-DD`` from the name)."""
    found = sorted(
        (m.group(1), name) for name in zf.namelist() if (m := RNS_MEMBER_RE.search(name))
    )
    if not found:
        raise RefusedInput(f"no registro-nacional-sociedades-YYYYMMDD.csv in {zf.namelist()[:5]}")
    stamp, name = found[-1]
    return name, f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:]}"


def igj_members(zf: zipfile.ZipFile) -> tuple[dict[str, str], str]:
    """The newest month's five IGJ CSVs and that month (``YYYY-MM``)."""
    by_month: dict[str, dict[str, str]] = defaultdict(dict)
    for name in zf.namelist():
        m = IGJ_MEMBER_RE.search(name)
        if m:
            by_month[m.group(2)][m.group(1)] = name
    complete = [month for month, files in by_month.items() if set(files) == set(IGJ_HEADERS)]
    if not complete:
        raise RefusedInput(f"no complete IGJ month (five CSVs) in {sorted(zf.namelist())[:10]}")
    month = max(complete)
    return by_month[month], f"{month[:4]}-{month[4:]}"


def _address(row: dict[str, str], prefix: str) -> list[str | None]:
    return [
        _text(row.get(f"{prefix}_{part}"))
        for part in ("provincia", "localidad", "calle", "numero", "piso", "departamento", "cp", "estado_domicilio")
    ]


def load_rns(con: sqlite3.Connection, zf: zipfile.ZipFile, member: str, stats: dict[str, int]) -> None:
    seen: dict[int, list[Any]] = {}
    best: dict[int, tuple[int, str]] = {}
    activities: dict[int, int] = defaultdict(int)
    vocab: dict[str, str] = {}
    malformed = 0
    for row in _reader(zf, member, RNS_HEADER):
        cuit = argentine_cuit(row.get("cuit"))
        if cuit is None:
            malformed += 1
            continue
        key = int(cuit)
        if key not in seen:
            seen[key] = [
                key, _text(row.get("razon_social")), _text(row.get("tipo_societario")),
                _iso_day(row.get("fecha_hora_contrato_social")),
                _iso_day(row.get("fecha_hora_actualizacion")), None, 0,
                *_address(row, "dom_legal"), *_address(row, "dom_fiscal"),
            ]
        code = _text(row.get("actividad_codigo"))
        if code and (row.get("actividad_estado") or "").strip() == "AC":
            activities[key] += 1
            words = _text(row.get("actividad_descripcion"))
            if words:
                vocab.setdefault(code, words)
            order = _int(row.get("actividad_orden"))
            rank = order if order and order > 0 else 999
            if key not in best or rank < best[key][0]:
                best[key] = (rank, code)
    for key, values in seen.items():
        values[5] = best.get(key, (0, None))[1]
        values[6] = activities.get(key, 0)
    con.executemany(f"INSERT INTO rns VALUES ({','.join('?' * 23)})", seen.values())
    con.executemany(
        "INSERT OR REPLACE INTO vocab VALUES ('activity', ?, ?)", sorted(vocab.items())
    )
    stats["rns_cuits"] = len(seen)
    stats["rns_malformed"] = malformed


def load_igj(con: sqlite3.Connection, zf: zipfile.ZipFile, members: dict[str, str], stats: dict[str, int]) -> None:
    entities: dict[int, list[Any]] = {}
    forms: dict[str, str] = {}
    bajas: dict[str, str] = {}
    persons = bad_cuit = 0
    for row in _reader(zf, members["entidades"], IGJ_HEADERS["entidades"]):
        number = _int(row.get("numero_correlativo"))
        form = _int(row.get("tipo_societario"))
        if number is None:
            continue
        if form in NATURAL_PERSON_FORMS:
            persons += 1
            continue
        if form is not None and row.get("descripcion_tipo_societario"):
            forms.setdefault(str(form), _text(row["descripcion_tipo_societario"]) or "")
        baja = _text(row.get("codigo_baja"))
        if baja and _text(row.get("detalle_baja")):
            bajas.setdefault(baja, _text(row["detalle_baja"]) or "")
        raw_cuit = _text(row.get("cuit"))
        cuit = argentine_cuit(raw_cuit)
        if raw_cuit and cuit is None:
            bad_cuit += 1
        entities[number] = [
            number, form, _text(row.get("razon_social")), int(cuit) if cuit else None,
            1 if (row.get("dada_de_baja") or "").strip().upper() == "S" else 0, baja,
            0, 0, None, None, None, None, None, None, None, None,
        ]
    for row in _reader(zf, members["autoridades"], IGJ_HEADERS["autoridades"]):
        number = _int(row.get("numero_correlativo"))
        if number in entities:
            entities[number][6] += 1
    for row in _reader(zf, members["balances"], IGJ_HEADERS["balances"]):
        number = _int(row.get("numero_correlativo"))
        if number not in entities:
            continue
        rec = entities[number]
        rec[7] += 1
        period = _iso_day(row.get("fecha_balance"))
        if period and (rec[8] is None or period > rec[8]):
            rec[8], rec[9] = period, _iso_day(row.get("fecha_presentacion"))
    for row in _reader(zf, members["asambleas"], IGJ_HEADERS["asambleas"]):
        number = _int(row.get("numero_correlativo"))
        if number not in entities:
            continue
        held = _iso_day(row.get("fecha_realizacion"))
        if held and (entities[number][10] is None or held > entities[number][10]):
            entities[number][10] = held
    for row in _reader(zf, members["domicilios"], IGJ_HEADERS["domicilios"]):
        number = _int(row.get("numero_correlativo"))
        if number not in entities or (row.get("tipo_domicilio") or "").strip() != "L":
            continue
        rec = entities[number]
        if rec[11] is None and rec[13] is None:
            rec[11:16] = [
                _text(row.get("calle")), _text(row.get("numero")), _text(row.get("localidad")),
                _text(row.get("provincia")), _text(row.get("codigo_postal")),
            ]
    con.executemany(f"INSERT INTO igj VALUES ({','.join('?' * 16)})", entities.values())
    con.executemany("INSERT OR REPLACE INTO vocab VALUES ('igj_form', ?, ?)", sorted(forms.items()))
    con.executemany("INSERT OR REPLACE INTO vocab VALUES ('igj_baja', ?, ?)", sorted(bajas.items()))
    stats["igj_entities"] = len(entities)
    stats["igj_natural_persons_left_out"] = persons
    stats["igj_cuit_invalid"] = bad_cuit
    stats["igj_with_cuit"] = sum(1 for e in entities.values() if e[3])
    stats["igj_deregistered"] = sum(e[4] for e in entities.values())


def build(
    rns_zip: Path,
    igj_zip: Path,
    out: Path,
    *,
    sources: dict[str, Any] | None = None,
    min_rns: int = MIN_RNS_CUITS,
    min_igj: int = MIN_IGJ_ENTITIES,
) -> dict[str, Any]:
    """Build the index from the two ZIPs as published. ``sources`` is the
    CKAN resource metadata recorded in ``meta`` (``--download`` passes it)."""
    t0 = time.time()
    tmp = out.with_name(out.name + ".building")
    tmp.unlink(missing_ok=True)
    stats: dict[str, Any] = {}
    con = sqlite3.connect(str(tmp))
    try:
        con.executescript(SCHEMA)
        with zipfile.ZipFile(rns_zip) as zf:
            member, rns_cut = rns_member(zf)
            load_rns(con, zf, member, stats)
        log.info("RNS %s: %s CUITs (%s malformed), %.0fs", rns_cut, f"{stats['rns_cuits']:,}", stats["rns_malformed"], time.time() - t0)
        if stats["rns_cuits"] < min_rns:
            raise RefusedInput(f"RNS: only {stats['rns_cuits']:,} CUITs — not the register")
        if stats["rns_malformed"] > MAX_RNS_MALFORMED:
            raise RefusedInput(f"RNS: {stats['rns_malformed']:,} malformed CUITs — the format has changed")
        with zipfile.ZipFile(igj_zip) as zf:
            members, igj_month = igj_members(zf)
            load_igj(con, zf, members, stats)
        log.info("IGJ %s: %s entities, %.0fs", igj_month, f"{stats['igj_entities']:,}", time.time() - t0)
        if stats["igj_entities"] < min_igj:
            raise RefusedInput(f"IGJ: only {stats['igj_entities']:,} entities — not the register")
        con.commit()
        con.executescript(INDEXES)
        meta = [
            ("schema_version", INDEX_SCHEMA_VERSION),
            ("source", "datos.jus.gob.ar — Registro Nacional de Sociedades + IGJ entidades"),
            ("cut", rns_cut),
            ("rns_cut", rns_cut),
            ("igj_month", igj_month),
            ("built_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
            ("licence", "CC BY 4.0 — Ministerio de Justicia de la Nación (datos.jus.gob.ar)"),
            *[(f"count_{k}", str(v)) for k, v in stats.items()],
        ]
        if sources:
            meta.append(("sources", json.dumps(sources, sort_keys=True)))
            igj_published = str((sources.get("igj") or {}).get("last_modified") or "")[:10]
            if igj_published:
                meta.append(("igj_published", igj_published))
        con.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", meta)
        con.commit()
        con.execute("VACUUM")
        con.close()
    except BaseException:
        con.close()
        tmp.unlink(missing_ok=True)
        raise
    os.replace(tmp, out)
    stats.update(rns_cut=rns_cut, igj_month=igj_month)
    stats["seconds"] = int(time.time() - t0)
    stats["megabytes"] = int(os.path.getsize(out) / 1e6)
    return stats


# ---------------------------------------------------------------------------
# Network
# ---------------------------------------------------------------------------


def _get_json(action: str, params: dict[str, Any]) -> dict[str, Any]:
    url = f"{API}/{action}?{urllib.parse.urlencode(params)}"
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=120) as resp:
                body = json.load(resp)
            if not body.get("success"):
                raise RefusedInput(f"{action}: success=false")
            return body["result"]
        except (OSError, ValueError) as exc:
            if attempt == 3:
                raise
            wait = 5 * 2**attempt
            log.warning("%s failed (%s); retrying in %ss", action, type(exc).__name__, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def pick_resource(resources: list[dict[str, Any]], pattern: re.Pattern[str]) -> dict[str, Any]:
    """The newest resource whose name matches (by the numbers in its name)."""
    matches = [
        (tuple(int(g) for g in m.groups()), res)
        for res in resources
        if (m := pattern.match(str(res.get("name") or ""))) and str(res.get("url") or "").endswith(".zip")
    ]
    if not matches:
        raise RefusedInput(f"no resource named like {pattern.pattern}")
    return max(matches, key=lambda pair: pair[0])[1]


def current_sources() -> dict[str, dict[str, str]]:
    """The two resources to read, with their URL and ``last_modified``."""
    out: dict[str, dict[str, str]] = {}
    for key, dataset, pattern in (("rns", RNS_DATASET, RNS_RESOURCE_RE), ("igj", IGJ_DATASET, IGJ_RESOURCE_RE)):
        res = pick_resource(_get_json("package_show", {"id": dataset}).get("resources") or [], pattern)
        out[key] = {
            "name": str(res.get("name") or ""),
            "id": str(res.get("id") or ""),
            "url": str(res.get("url") or ""),
            "last_modified": str(res.get("last_modified") or res.get("metadata_modified") or ""),
        }
    return out


def _download(url: str, dest: Path) -> None:
    for attempt in range(4):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=600) as resp, open(dest, "wb") as fh:
                shutil.copyfileobj(resp, fh, 1 << 20)
            with open(dest, "rb") as fh:
                if fh.read(2) != b"PK":
                    raise RefusedInput(f"{url} did not answer with a ZIP")
            return
        except OSError as exc:
            if attempt == 3:
                raise
            wait = 10 * 2**attempt
            log.warning("download failed (%s); retrying in %ss", type(exc).__name__, wait)
            time.sleep(wait)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="fetch both ZIPs from datos.jus.gob.ar")
    ap.add_argument("--rns-zip", type=Path, help="a local RNS ZIP")
    ap.add_argument("--igj-zip", type=Path, help="a local IGJ ZIP")
    ap.add_argument("--out", required=True, type=Path)
    ap.add_argument("--gzip", action="store_true", help="also write <out>.gz, the release asset's shape")
    ap.add_argument("--sources-out", type=Path, help="write the CKAN resources read, as JSON")
    ap.add_argument(
        "--skip-if-unchanged", type=Path,
        help="a previous --sources-out file: exit 3 without building when both resources are unchanged",
    )
    args = ap.parse_args(argv)

    sources: dict[str, Any] | None = None
    workdir: tempfile.TemporaryDirectory[str] | None = None
    try:
        if args.download:
            sources = current_sources()
            if args.skip_if_unchanged and args.skip_if_unchanged.exists():
                previous = json.loads(args.skip_if_unchanged.read_text(encoding="utf-8"))
                if all(
                    (previous.get(k) or {}).get(f) == sources[k][f]
                    for k in ("rns", "igj") for f in ("url", "last_modified")
                ):
                    log.info("both resources unchanged since the last build: %s", sources)
                    return 3
            workdir = tempfile.TemporaryDirectory(prefix="ar-sociedades-")
            rns_zip, igj_zip = Path(workdir.name) / "rns.zip", Path(workdir.name) / "igj.zip"
            for key, path in (("rns", rns_zip), ("igj", igj_zip)):
                log.info("downloading %s", sources[key]["url"])
                _download(sources[key]["url"], path)
        else:
            if not (args.rns_zip and args.igj_zip):
                ap.error("--rns-zip and --igj-zip, or --download")
            rns_zip, igj_zip = args.rns_zip, args.igj_zip
        stats = build(rns_zip, igj_zip, args.out, sources=sources)
    except RefusedInput as exc:
        log.error("refused: %s", exc)
        return 2
    finally:
        if workdir is not None:
            workdir.cleanup()
    log.info("wrote %s (%s MB, %ss)", args.out, stats["megabytes"], stats["seconds"])
    for key, value in stats.items():
        log.info("  %-30s %s", key, value)
    if args.sources_out and sources:
        args.sources_out.write_text(json.dumps(sources, indent=2, sort_keys=True), encoding="utf-8")
    if args.gzip:
        gz_path = args.out.with_name(args.out.name + ".gz")
        with open(args.out, "rb") as src, gzip.open(gz_path, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        log.info("wrote %s (%s MB)", gz_path, int(os.path.getsize(gz_path) / 1e6))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
