"""Build a Relate ``.rl`` graph file from a BODS dataset release.

    python3 build_relate_file.py SOURCE.jsonl[.gz] OUT.rl.gz [--batch-size 200]

A convenience beside the release's canonical files (the BODS JSON Lines and
the Neo4j CSV zip): one file that Neo4j Labs' embedded engine, Relate, opens
and queries in Cypher, with no server, no Docker and no APOC. Stephen's
decision (24 Nov 2026, the ticket "Check out relate from Neo4J"): publish it
as a convenience, record the Relate version that wrote it, and keep JSONL
and CSV canonical, because the ``.rl`` format is weeks old.

How it is built, so the graph is the one the Neo4j zip holds:

1. ``bods-neo4j to-cypher`` writes the graph as core Cypher (no APOC, no
   ``LOAD CSV``), the route its tests prove builds the driver route's graph.
2. Each statement is run through the ``relate-db`` Python binding with
   ``DO_NOT_TRACK=1`` (Relate's CLI sends usage telemetry by default; the
   binding showed none, and the variable is set anyway).
3. The node and relationship counts are read back and compared with the
   counts the script's header announces, per label and per edge pattern.
   Any statement that fails, or any count that differs, writes nothing.
4. The file is gzipped, and ``OUT.json`` beside it records the Relate and
   bods-neo4j versions, the counts, the source file's SHA-256 and the
   ``.rl.gz``'s own.

Measured on 24 Nov 2026 with relate-db 1.2.0: the Azerbaijani Laundromat
release loads in 10 s to 25 MB; the Estonia release in about 6 minutes to
173 MB (66 MB gzipped), which opens in about 13 s.

Needs ``relate-db`` and ``bods-neo4j`` installed (neither is a backend
dependency); exits 2 when either is missing.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from importlib import metadata
from pathlib import Path

os.environ["DO_NOT_TRACK"] = "1"

#: The header lines ``bods-neo4j to-cypher`` writes before each block.
_NODE_COUNT = re.compile(r"^// :(\w+) \(([\d,]+)\)$")
_EDGE_COUNT = re.compile(r"^// \((\w+)\)-\[:(\w+)\]->\((\w+)\) \(([\d,]+)\)$")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def statements(script: Path) -> Iterator[str]:
    """The script's statements. A raw newline never occurs inside one of its
    string literals (to-cypher writes ``\\n``), so a line ending ``;`` ends a
    statement."""
    buf: list[str] = []
    with script.open(encoding="utf-8") as fh:
        for line in fh:
            if line.startswith("//"):
                continue
            buf.append(line)
            if line.rstrip().endswith(";"):
                text = "".join(buf).strip().rstrip(";").strip()
                buf = []
                if text:
                    yield text
    if "".join(buf).strip():
        raise ValueError("the script ends inside a statement")


def announced_counts(script: Path) -> tuple[dict[str, int], dict[tuple[str, str, str], int]]:
    """Per-label node counts and per-pattern edge counts from the header."""
    nodes: dict[str, int] = {}
    edges: dict[tuple[str, str, str], int] = {}
    with script.open(encoding="utf-8") as fh:
        for line in fh:
            if not line.startswith("//"):
                continue
            line = line.rstrip("\n")
            if m := _NODE_COUNT.match(line):
                nodes[m[1]] = int(m[2].replace(",", ""))
            elif m := _EDGE_COUNT.match(line):
                key = (m[1], m[2], m[3])
                edges[key] = edges.get(key, 0) + int(m[4].replace(",", ""))
    return nodes, edges


def count_mismatches(
    want_nodes: dict[str, int], got_nodes: dict[str, int],
    want_edges: dict[tuple[str, str, str], int], got_edges: dict[tuple[str, str, str], int],
) -> list[str]:
    """Every label or edge pattern whose count in the graph differs from the
    script's. Entity subtype labels are added by a fix-up the header does not
    count, so only the labels the header names are compared."""
    out = [
        f":{label} announced {n:,}, loaded {got_nodes.get(label, 0):,}"
        for label, n in sorted(want_nodes.items()) if got_nodes.get(label, 0) != n
    ]
    for key in sorted(set(want_edges) | set(got_edges)):
        if want_edges.get(key, 0) != got_edges.get(key, 0):
            a, t, b = key
            out.append(
                f"({a})-[:{t}]->({b}) announced {want_edges.get(key, 0):,}, "
                f"loaded {got_edges.get(key, 0):,}"
            )
    return out


def _base(labels: list[str], base: dict[str, int]) -> str:
    """The node's label the header counts (an entity also carries a subtype
    label, in no promised order)."""
    return next((label for label in labels if label in base), "|".join(sorted(labels)))


def _bods_neo4j() -> str | None:
    for candidate in (
        Path(sys.executable).parent / "bods-neo4j",  # installed beside relate-db
        Path(__file__).resolve().parents[1] / ".venv" / "bin" / "bods-neo4j",
    ):
        if candidate.exists():
            return str(candidate)
    return shutil.which("bods-neo4j")


def _version(dist: str) -> str | None:
    try:
        return metadata.version(dist)
    except metadata.PackageNotFoundError:
        return None


def build(source: Path, out: Path, *, batch_size: int = 200) -> dict:
    """Build ``out`` (a ``.rl.gz``) from ``source`` and return the manifest
    written beside it. Raises ``SystemExit`` on any failure, leaving no
    output behind."""
    try:
        import relate  # type: ignore[import-not-found]
    except ImportError:
        print("relate-db is not installed: pip install relate-db==1.2.0", file=sys.stderr)
        raise SystemExit(2) from None
    cli = _bods_neo4j()
    if cli is None:
        print("bods-neo4j is not installed: pip install "
              "git+https://github.com/StephenAbbott/bods-neo4j.git", file=sys.stderr)
        raise SystemExit(2)

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        jsonl = source
        if source.suffix == ".gz":
            jsonl = tmp / source.stem
            with gzip.open(source, "rb") as src, jsonl.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        script = tmp / "graph.cypher"
        done = subprocess.run(
            [cli, "to-cypher", str(jsonl), "-o", str(script), "--batch-size", str(batch_size)],
            capture_output=True, text=True, check=False,
        )
        if done.returncode != 0:
            print(done.stdout[-2000:], done.stderr[-2000:], file=sys.stderr)
            raise SystemExit(f"bods-neo4j to-cypher failed ({done.returncode})")
        want_nodes, want_edges = announced_counts(script)

        rl = tmp / "graph.rl"
        conn = relate.connect(str(rl))
        started = time.monotonic()
        n = 0
        try:
            for text in statements(script):
                try:
                    conn.execute(text)
                except Exception as exc:  # a refused statement means a different graph
                    raise SystemExit(
                        f"Relate refused statement {n + 1}: {str(exc)[:300]}"
                    ) from exc
                n += 1
            load_s = time.monotonic() - started
            got_nodes = {
                r["l"]: r["c"] for r in conn.execute(
                    "MATCH (n) UNWIND labels(n) AS l RETURN l, count(*) AS c"
                )
            }
            got_edges: dict[tuple[str, str, str], int] = {}
            for r in conn.execute(
                "MATCH (a)-[r]->(b) RETURN labels(a) AS a, type(r) AS t, "
                "labels(b) AS b, count(*) AS c"
            ):
                key = (_base(r["a"], want_nodes), r["t"], _base(r["b"], want_nodes))
                got_edges[key] = got_edges.get(key, 0) + r["c"]
        finally:
            conn.close()
        problems = count_mismatches(want_nodes, got_nodes, want_edges, got_edges)
        if problems:
            raise SystemExit("the .rl graph differs from the script:\n  " + "\n  ".join(problems))

        partial = out.with_name(out.name + ".partial")
        with rl.open("rb") as src, gzip.open(partial, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst)
        rl_bytes = rl.stat().st_size
        partial.replace(out)

    manifest = {
        "file": out.name,
        "format": "Relate graph file (.rl), gzipped: gunzip it, then relate.connect(path)",
        "relate_db_version": _version("relate-db"),
        "bods_neo4j_version": _version("bods-neo4j"),
        "built_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "source_file": source.name,
        "source_sha256": sha256(source),
        "statements_run": n,
        "load_seconds": round(load_s),
        "nodes": sum(want_nodes.values()),
        "relationships": sum(want_edges.values()),
        "nodes_by_label": want_nodes,
        "rl_bytes": rl_bytes,
        "bytes": out.stat().st_size,
        "sha256": sha256(out),
        "note": (
            "A convenience copy of the graph in the release's Neo4j zip, built with "
            "bods-neo4j to-cypher. The BODS JSON Lines file is the canonical record; "
            "Relate is a Neo4j Labs project and the .rl format is new. Licences: see "
            "the release's licence file, which applies to this file unchanged."
        ),
    }
    out.with_name(out.name.removesuffix(".gz").removesuffix(".rl") + ".rl.json").write_text(
        json.dumps(manifest, indent=2) + "\n", encoding="utf-8"
    )
    return manifest


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("source", type=Path, help="the release's BODS JSON Lines file (.gz allowed)")
    ap.add_argument("out", type=Path, help="the .rl.gz to write")
    ap.add_argument("--batch-size", type=int, default=200)
    args = ap.parse_args(argv)
    if not args.out.name.endswith(".rl.gz"):
        ap.error("OUT must end .rl.gz")
    m = build(args.source, args.out, batch_size=args.batch_size)
    print(f"{m['file']}: {m['nodes']:,} nodes, {m['relationships']:,} relationships, "
          f"{m['bytes']:,} bytes, loaded in {m['load_seconds']} s (relate-db "
          f"{m['relate_db_version']})")


if __name__ == "__main__":
    main()
