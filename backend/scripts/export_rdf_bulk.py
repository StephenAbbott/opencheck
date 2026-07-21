"""Bulk BODS JSONL → RDF NQuads, streaming, via ``opencheck.bods.rdf``.

Usage (from backend/):

    python scripts/export_rdf_bulk.py \
        --in ../data/estonia/estonia-2026-07-04.jsonl.gz \
        --out ../data/estonia/estonia-2026-07-04.nq.gz

Converts a bulk BODS v0.4 JSONL file (e.g. the Estonia dataset release) into
NQuads using the same statement-per-named-graph projection as
``/export?format=rdf``, processing in chunks so memory stays flat regardless
of corpus size. NQuads is concatenation-safe (rdflib blank-node ids are
random UUIDs, so chunks cannot collide), which is what makes the streaming
approach sound; TriG output is intentionally per-bundle-only.

The per-statement ``bods:license`` is resolved from each statement's source
block via the licensing registry, exactly as in the API export. Statements
without a source block (e.g. the GLEIF layer of the Estonia dataset, built
from Open Ownership's bulk parquet) carry no licence triple — use
``--fallback-license`` to stamp one (e.g. the dataset-level licence from the
release's LICENCES.md) on those statements instead.

Exit codes: 0 = success, 1 = error.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rdflib import Literal  # noqa: E402

from opencheck.bods.rdf import BODS, _build_dataset  # noqa: E402

log = logging.getLogger("export_rdf_bulk")

CHUNK = 5_000


def _open(path: Path, mode: str):
    if path.suffix == ".gz":
        return gzip.open(path, mode + "t", encoding="utf-8")
    return open(path, mode, encoding="utf-8")


def convert(in_path: Path, out_path: Path, fallback_license: str | None = None) -> dict:
    """Stream-convert a BODS JSONL file to NQuads. Returns run counters."""
    counts = {"statements": 0, "triples": 0, "licensed": 0, "fallback_licensed": 0}
    started = time.time()
    with _open(in_path, "r") as src, _open(out_path, "w") as out:
        chunk: list[dict] = []

        def _flush() -> None:
            if not chunk:
                return
            ds, _ = _build_dataset(chunk)
            if fallback_license:
                for g in ds.graphs():
                    stmt_uri = g.identifier
                    if (stmt_uri, BODS.license, None) not in g and len(g):
                        g.add((stmt_uri, BODS.license, Literal(fallback_license)))
                        counts["fallback_licensed"] += 1
            for g in ds.graphs():
                counts["triples"] += len(g)
                counts["licensed"] += sum(
                    1 for _ in g.triples((g.identifier, BODS.license, None))
                )
            out.write(ds.serialize(format="nquads"))
            chunk.clear()

        for line in src:
            line = line.strip()
            if not line:
                continue
            chunk.append(json.loads(line))
            counts["statements"] += 1
            if len(chunk) >= CHUNK:
                _flush()
                if counts["statements"] % 50_000 == 0:
                    log.info("… %s statements, %s triples, %.0fs",
                             f"{counts['statements']:,}", f"{counts['triples']:,}",
                             time.time() - started)
        _flush()
    counts["licensed"] -= counts["fallback_licensed"]
    counts["seconds"] = round(time.time() - started, 1)
    return counts


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--in", dest="in_path", required=True, type=Path,
                        help="Input BODS JSONL (.jsonl or .jsonl.gz)")
    parser.add_argument("--out", dest="out_path", required=True, type=Path,
                        help="Output NQuads (.nq or .nq.gz)")
    parser.add_argument("--fallback-license", default=None,
                        help="Licence URI/label for statements with no source block")
    args = parser.parse_args()

    if not args.in_path.exists():
        log.error("input not found: %s", args.in_path)
        sys.exit(1)
    counts = convert(args.in_path, args.out_path, args.fallback_license)
    log.info(
        "done: %s statements → %s triples in %ss "
        "(%s source-licensed, %s fallback-licensed) → %s",
        f"{counts['statements']:,}", f"{counts['triples']:,}", counts["seconds"],
        f"{counts['licensed']:,}", f"{counts['fallback_licensed']:,}", args.out_path,
    )


if __name__ == "__main__":
    main()
