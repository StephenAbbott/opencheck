#!/usr/bin/env python3
"""Generate the auto-derived parts of OpenCheck's Open Knowledge Format bundle.

OKF (Open Knowledge Format, https://github.com/GoogleCloudPlatform/knowledge-catalog)
is a directory of markdown files with YAML frontmatter that lets humans and AI
agents understand a project, its data sources, and its standards.

This script is OpenCheck's *enrichment agent*: it reads the live source registry
and licensing module and writes one **Data Source** concept per registered
adapter (plus the sources index and the licensing matrix), so those concepts
stay in sync with the code. The narrative concepts (overview, architecture,
glossary, standards, api) are hand-authored and are NOT touched here.

Usage:
    python backend/scripts/generate_okf.py            # (re)generate auto concepts
    python backend/scripts/generate_okf.py --check    # validate OKF conformance only
"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from opencheck import licensing
from opencheck.sources import REGISTRY

OKF_ROOT = Path(__file__).resolve().parents[2] / "okf"
TODAY = date.today().isoformat()
BODS_LINK = "/standards/bods.md"
MATRIX_LINK = "/licensing/matrix.md"


# --- tiny, safe YAML frontmatter emitter (JSON scalars are valid YAML) --------


def _scalar(v) -> str:
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return json.dumps(str(v))  # double-quoted → safe for any content


def _frontmatter(fields: dict) -> str:
    lines = ["---"]
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            items = ", ".join(_scalar(x) for x in v)
            lines.append(f"{k}: [{items}]")
        else:
            lines.append(f"{k}: {_scalar(v)}")
    lines.append("---")
    return "\n".join(lines)


def _doc(fields: dict, body: str) -> str:
    return _frontmatter(fields) + "\n\n" + body.strip() + "\n"


# --- per-source concept -------------------------------------------------------


def _source_concept(adapter) -> str:
    info = adapter.info
    terms = licensing.classify(info.license)
    kinds = [k.value for k in info.supports]
    register_kind = (
        "Official national company / beneficial-ownership register"
        if info.is_national_register
        else "Aggregator, cross-border database or ESG source"
    )
    tags = [
        info.category,
        "national-register" if info.is_national_register else "aggregator",
        info.license,
        f"commercial-{terms.commercial_use}",
    ]
    fields = {
        "type": "Data Source",
        "title": info.name,
        "description": info.description or f"{info.name} data source.",
        "resource": info.homepage,
        "tags": tags,
        "timestamp": TODAY,
        "source_id": info.id,
        "license": info.license,
        "commercial_use": terms.commercial_use,
        "category": info.category,
        "national_register": info.is_national_register,
    }

    lookup_keys = adapter.lookup_keys()
    raw_note = (
        "\n\n> **Raw data:** OpenCheck does not redistribute this source's raw "
        "records (licence permits derived output only). Only the mapped BODS "
        "statements are served; the raw payload is redacted from API responses "
        "and exports."
        if not getattr(adapter, "republish_raw", True)
        else ""
    )

    lead = (info.description or info.name).rstrip(". ")
    body = f"""# Overview

{lead}. {register_kind}.

- **Source id:** `{info.id}`
- **Category:** {info.category} ({"customer due diligence / compliance" if info.category == "cdd" else "environmental, social & governance"})
- **Search kinds:** {", ".join(kinds) or "—"}
- **Requires API key:** {"yes" if info.requires_api_key else "no"}
- **National register:** {"yes" if info.is_national_register else "no"}
{f"- **Lookup keys (LEI-anchored dispatch):** {', '.join(f'`{k}`' for k in lookup_keys)}" if lookup_keys else ""}

# Licensing

- **Licence:** `{info.license}` — {terms.name}
- **Commercial use:** {terms.commercial_use} · **Attribution:** {"required" if terms.attribution_required else "not required"} · **Share-alike:** {"yes" if terms.share_alike else "no"}
- **Attribution line:** {info.attribution}
- {terms.summary}

See the [licensing compatibility matrix]({MATRIX_LINK}) for how this licence combines with others at export time.{raw_note}

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4]({BODS_LINK})
statements by OpenCheck's mapper (`opencheck.bods.map_{info.id}`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- {info.homepage}
"""
    return _doc(fields, body)


def _sources_index() -> str:
    nat, agg, esg = [], [], []
    for sid in sorted(REGISTRY):
        info = REGISTRY[sid].info
        line = f"* [{info.name}](/sources/{sid}.md) - {info.description or info.name}"
        if info.category == "esg":
            esg.append(line)
        elif info.is_national_register:
            nat.append(line)
        else:
            agg.append(line)
    return (
        "# National company / beneficial-ownership registers\n\n"
        + "\n".join(nat)
        + "\n\n# Aggregators & cross-border databases\n\n"
        + "\n".join(agg)
        + "\n\n# ESG / climate sources\n\n"
        + "\n".join(esg)
        + "\n"
    )


def _licensing_matrix() -> str:
    matrix = licensing.full_matrix()
    rows = []
    for s in matrix["sources"]:
        t = s["terms"]
        rows.append(
            f"| {s['name']} (`{s['source_id']}`) | `{t['license']}` | {t['commercial_use']} "
            f"| {'yes' if t['attribution_required'] else 'no'} | {'yes' if t['share_alike'] else 'no'} |"
        )
    fields = {
        "type": "Reference",
        "title": "Licensing compatibility matrix",
        "description": (
            "Per-source licence terms (commercial use, attribution, share-alike) "
            "for combining OpenCheck data in exports. Most-restrictive licence wins."
        ),
        "resource": "/license-matrix",
        "tags": ["licensing", "export", "compliance"],
        "timestamp": TODAY,
    }
    body = (
        "# Source licence matrix\n\n"
        "Generated from the live registry. The OpenCheck `/license-matrix` API "
        "endpoint and the `LICENSES.md` in every export bundle carry the same data.\n\n"
        "| Source | Licence | Commercial | Attribution | Share-alike |\n"
        "|---|---|---|---|---|\n"
        + "\n".join(rows)
        + "\n\n# How combined licensing is assessed\n\n"
        "When a result combines several sources, the **most restrictive** licence "
        "applies to the bundle: a single non-commercial source (e.g. OpenSanctions "
        "`CC-BY-NC-4.0`) makes the whole export non-commercial. OpenCheck computes "
        "this verdict at export time (`opencheck.licensing.assess`).\n\n"
        f"> {matrix['disclaimer']}\n\n"
        "# Citations\n\n"
        "- https://github.com/StephenAbbott/opencheck/blob/main/ATTRIBUTIONS.md\n"
    )
    return _doc(fields, body)


def _licensing_index() -> str:
    return (
        "# Concepts\n\n"
        "* [Licensing compatibility matrix](/licensing/matrix.md) - per-source "
        "licence terms and how they combine in exports.\n"
    )


GENERATED = "  (generated by backend/scripts/generate_okf.py)"


def generate() -> list[Path]:
    written: list[Path] = []
    (OKF_ROOT / "sources").mkdir(parents=True, exist_ok=True)
    (OKF_ROOT / "licensing").mkdir(parents=True, exist_ok=True)

    for sid in sorted(REGISTRY):
        p = OKF_ROOT / "sources" / f"{sid}.md"
        p.write_text(_source_concept(REGISTRY[sid]), encoding="utf-8")
        written.append(p)

    (OKF_ROOT / "sources" / "index.md").write_text(_sources_index(), encoding="utf-8")
    written.append(OKF_ROOT / "sources" / "index.md")
    (OKF_ROOT / "licensing" / "matrix.md").write_text(_licensing_matrix(), encoding="utf-8")
    written.append(OKF_ROOT / "licensing" / "matrix.md")
    (OKF_ROOT / "licensing" / "index.md").write_text(_licensing_index(), encoding="utf-8")
    written.append(OKF_ROOT / "licensing" / "index.md")
    return written


# --- conformance check (OKF v0.1 §9) ------------------------------------------


def check() -> int:
    issues: list[str] = []
    md_files = sorted(OKF_ROOT.rglob("*.md")) if OKF_ROOT.is_dir() else []
    if not md_files:
        print(f"No OKF bundle found at {OKF_ROOT}")
        return 1
    concepts = 0
    for f in md_files:
        if f.name in {"index.md", "log.md"}:
            continue
        concepts += 1
        text = f.read_text(encoding="utf-8")
        rel = f.relative_to(OKF_ROOT)
        if not text.startswith("---"):
            issues.append(f"{rel}: missing YAML frontmatter")
            continue
        end = text.find("\n---", 3)
        if end == -1:
            issues.append(f"{rel}: unterminated frontmatter")
            continue
        fm = text[3:end]
        type_lines = [ln for ln in fm.splitlines() if ln.strip().startswith("type:")]
        if not type_lines or not type_lines[0].split(":", 1)[1].strip():
            issues.append(f"{rel}: missing/empty required `type` field")
    for issue in issues:
        print("  ✗", issue)
    print(
        f"OKF conformance: {concepts} concept docs checked, "
        f"{len(issues)} issue(s)."
    )
    return 1 if issues else 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check", action="store_true", help="validate conformance only")
    args = ap.parse_args()
    if args.check:
        return check()
    written = generate()
    print(f"Wrote {len(written)} generated OKF concepts under {OKF_ROOT}.")
    return check()


if __name__ == "__main__":
    raise SystemExit(main())
