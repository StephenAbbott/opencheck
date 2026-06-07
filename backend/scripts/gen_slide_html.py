"""Phase 7 — Generate slide-ready HTML ownership graphs for the 9 demo entities.

Usage (from repo root):

    make slides

Or manually:

    cd backend && python scripts/gen_slide_html.py [--demo-dir ../data/demo] [--out ../data/demo/slides]

What it produces
----------------
data/demo/slides/opencheck_demo.html  — single self-contained HTML file.

    * All 9 entity graphs embedded as JSON in the page.
    * Cytoscape.js + cytoscape-dagre loaded from CDN.
    * Sidebar entity picker; arrow-key navigation between entities.
    * Edge colours match BODSGraph.tsx (OWNS=blue, CONTROLS=orange,
      MANAGES=purple/dashed, IS_PARTY_TO=teal, UNKNOWN=grey).
    * White background — screenshot-ready for slides.
    * Print CSS: one entity per page.

Node/edge mapping
-----------------
    entity statement  → Cytoscape node (rectangle, label = name)
    person statement  → Cytoscape node (ellipse, label = fullName)
    relationship statement  → one Cytoscape edge per interest
        source = interestedParty recordId  (owner/controller)
        target = subject recordId          (owned/controlled)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DEMO_DIR = _REPO_ROOT / "data" / "demo"
_DEFAULT_OUT_DIR = _REPO_ROOT / "data" / "demo" / "slides"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Interest type → edge family ────────────────────────────────────────────
_OWNS = {
    "shareholding", "rightsToProfitOrIncome", "rightsToSurplusAssetsOnDissolution",
    "rightToProfitOrIncomeFromAssets", "enjoymentAndUseOfAssets",
    "rightsGrantedByContract", "conditionalRightsGrantedByContract",
}
_CONTROLS = {
    "votingRights", "controlViaCompanyRulesOrArticles", "controlByLegalFramework",
    "otherInfluenceOrControl", "appointmentOfBoard",
}
_MANAGES = {"seniorManagingOfficial", "boardMember", "boardChair"}
_IS_PARTY_TO = {"settlor", "trustee", "protector", "beneficiaryOfLegalArrangement", "nominee", "nominator"}


def _edge_family(interest_type: str) -> str:
    if interest_type in _OWNS:
        return "OWNS"
    if interest_type in _CONTROLS:
        return "CONTROLS"
    if interest_type in _MANAGES:
        return "MANAGES"
    if interest_type in _IS_PARTY_TO:
        return "IS_PARTY_TO"
    return "UNKNOWN"


def _resolve_ref(ref) -> str | None:
    """Extract a recordId string from a subject/interestedParty field.

    Handles bare string (BODS v0.4) and legacy dict wrappers.
    Returns None for unspecified records (those with a 'reason' key).
    """
    if not ref:
        return None
    if isinstance(ref, str):
        return ref
    if isinstance(ref, dict):
        if "reason" in ref or "unspecifiedReason" in ref:
            return None  # unspecified party — no target node
        return (
            ref.get("describedByEntityStatement")
            or ref.get("describedByPersonStatement")
            or ref.get("recordId")
        )
    return None


def bods_to_cytoscape(statements: list[dict]) -> dict:
    """Convert a BODS statement list to a Cytoscape elements dict."""
    nodes: dict[str, dict] = {}   # id → node data
    edges: list[dict] = []

    for stmt in statements:
        rt = stmt.get("recordType")
        rid = stmt.get("recordId") or stmt.get("statementId", "")
        rd = stmt.get("recordDetails") or {}

        if rt == "entity":
            nodes[rid] = {
                "id": rid,
                "label": rd.get("name") or rid[:20],
                "type": "entity",
                "entityType": (rd.get("entityType") or {}).get("type", "registeredEntity"),
                "jurisdiction": (rd.get("incorporatedInJurisdiction") or {}).get("code", ""),
            }

        elif rt == "person":
            names = rd.get("names") or []
            full_name = names[0].get("fullName", rid[:20]) if names else rid[:20]
            nodes[rid] = {
                "id": rid,
                "label": full_name,
                "type": "person",
                "entityType": "person",
                "jurisdiction": "",
            }

        elif rt == "relationship":
            subject_id = _resolve_ref(rd.get("subject"))
            ip_id = _resolve_ref(rd.get("interestedParty"))
            if not subject_id or not ip_id:
                continue  # unspecified party — skip edge (preserve as orphan node handled above)

            interests = rd.get("interests") or [{}]
            for idx, interest in enumerate(interests):
                itype = interest.get("type", "unknownInterest")
                family = _edge_family(itype)
                share = interest.get("share", {}) or {}
                exact = share.get("exact")
                min_s = share.get("minimum")
                max_s = share.get("maximum")
                share_label = ""
                if exact is not None:
                    share_label = f"{exact}%"
                elif min_s is not None and max_s is not None:
                    share_label = f"{min_s}–{max_s}%"
                elif min_s is not None:
                    share_label = f"≥{min_s}%"

                edges.append({
                    "id": f"{stmt.get('statementId',rid)}-{idx}",
                    "source": ip_id,
                    "target": subject_id,
                    "family": family,
                    "interestType": itype,
                    "shareLabel": share_label,
                    "directOrIndirect": interest.get("directOrIndirect", ""),
                    "beneficialOwnershipOrControl": interest.get("beneficialOwnershipOrControl", False),
                })

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
        "nodeCount": len(nodes),
        "edgeCount": len(edges),
    }


def load_entity_data(demo_dir: Path, manifest_path: Path) -> list[dict]:
    manifest = json.loads(manifest_path.read_text())
    entities = []
    for ent in manifest["entities"]:
        lei = ent["lei"]
        jsonl_path = demo_dir / f"{lei}.jsonl"
        if not jsonl_path.exists():
            log.warning("Missing %s — skipping", jsonl_path)
            continue
        statements = [json.loads(l) for l in jsonl_path.read_text().splitlines() if l.strip()]
        cy_data = bods_to_cytoscape(statements)
        entities.append({
            "lei": lei,
            "name": ent["name"],
            "ch": ent.get("ch", ""),
            "features": ent.get("features", []),
            "note": ent.get("note", ""),
            "statementCounts": ent.get("statement_counts", {}),
            "graph": cy_data,
        })
        log.info(
            "%-45s  nodes=%d  edges=%d",
            ent["name"][:45],
            cy_data["nodeCount"],
            cy_data["edgeCount"],
        )
    return entities


def _feature_badge(feature: str) -> str:
    colours = {
        "sanctioned": "#be123c",
        "related_sanctioned": "#be123c",
        "complex_ownership_layers": "#0369a1",
        "complex_corporate_structure": "#b91c1c",
        "non_eu_jurisdiction": "#c2410c",
        "trust_or_arrangement": "#4338ca",
    }
    labels = {
        "sanctioned": "SANCTIONED",
        "related_sanctioned": "RELATED SANCTIONED",
        "complex_ownership_layers": "COMPLEX LAYERS",
        "complex_corporate_structure": "COMPLEX STRUCTURE",
        "non_eu_jurisdiction": "NON-EU",
        "trust_or_arrangement": "TRUST/ARRANGEMENT",
    }
    colour = colours.get(feature, "#555")
    label = labels.get(feature, feature.upper().replace("_", " "))
    return f'<span class="badge" style="background:{colour}">{label}</span>'


def _fetch_script(url: str) -> str:
    """Download a JS library, falling back to a CDN URL tag on failure."""
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.read().decode("utf-8")
    except Exception as exc:
        log.warning("Could not inline %s (%s) — will use CDN tag", url, exc)
        return ""


def render_html(entities: list[dict], inline_scripts: dict[str, str] | None = None) -> str:
    entities_json = json.dumps(entities, ensure_ascii=False, separators=(",", ":"))

    # Build <script> tags — inline if content available, else CDN src
    _CDN = {
        "cytoscape": "https://unpkg.com/cytoscape@3.28.1/dist/cytoscape.min.js",
        "dagre": "https://unpkg.com/dagre@0.8.5/dist/dagre.min.js",
        "cytoscape_dagre": "https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js",
    }
    scripts_html = ""
    for key, cdn_url in _CDN.items():
        content = (inline_scripts or {}).get(key, "")
        if content:
            scripts_html += f"\n<script>{content}</script>"
        else:
            scripts_html += f'\n<script src="{cdn_url}"></script>'

    sidebar_items = ""
    for i, ent in enumerate(entities):
        badges = " ".join(_feature_badge(f) for f in ent["features"])
        active = " active" if i == 0 else ""
        sidebar_items += f"""
        <div class="entity-item{active}" data-index="{i}" onclick="selectEntity({i})">
          <div class="entity-name">{ent['name']}</div>
          <div class="entity-badges">{badges}</div>
          <div class="entity-meta">{ent['graph']['nodeCount']} nodes · {ent['graph']['edgeCount']} edges</div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>OpenCheck — Demo Ownership Graphs</title>{scripts_html}
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
       background: #f8f9fa; color: #1a1a1a; height: 100vh; display: flex; flex-direction: column; }}

/* ── Header ── */
.header {{ background: #fff; border-bottom: 1px solid #e5e7eb; padding: 10px 20px;
           display: flex; align-items: center; gap: 12px; flex-shrink: 0; }}
.header h1 {{ font-size: 16px; font-weight: 600; color: #111; }}
.header .subtitle {{ font-size: 12px; color: #6b7280; }}
.nav-btn {{ background: #f3f4f6; border: 1px solid #d1d5db; border-radius: 6px;
            padding: 4px 12px; cursor: pointer; font-size: 13px; }}
.nav-btn:hover {{ background: #e5e7eb; }}
.spacer {{ flex: 1; }}
.entity-counter {{ font-size: 12px; color: #6b7280; }}

/* ── Layout ── */
.main {{ display: flex; flex: 1; min-height: 0; }}

/* ── Sidebar ── */
.sidebar {{ width: 260px; background: #fff; border-right: 1px solid #e5e7eb;
            overflow-y: auto; flex-shrink: 0; }}
.sidebar-title {{ padding: 10px 14px; font-size: 11px; font-weight: 600;
                  text-transform: uppercase; letter-spacing: .05em; color: #6b7280;
                  border-bottom: 1px solid #f3f4f6; }}
.entity-item {{ padding: 10px 14px; cursor: pointer; border-bottom: 1px solid #f3f4f6;
                transition: background .1s; }}
.entity-item:hover {{ background: #f9fafb; }}
.entity-item.active {{ background: #eff6ff; border-left: 3px solid #1d4ed8; }}
.entity-name {{ font-size: 12px; font-weight: 600; color: #111; margin-bottom: 4px; line-height: 1.3; }}
.entity-badges {{ display: flex; flex-wrap: wrap; gap: 3px; margin-bottom: 4px; }}
.badge {{ font-size: 9px; font-weight: 600; color: #fff; padding: 1px 5px;
          border-radius: 3px; letter-spacing: .03em; }}
.entity-meta {{ font-size: 10px; color: #9ca3af; }}

/* ── Graph area ── */
.graph-area {{ flex: 1; display: flex; flex-direction: column; min-width: 0; }}
.graph-header {{ background: #fff; border-bottom: 1px solid #e5e7eb; padding: 10px 16px; flex-shrink: 0; }}
.graph-title {{ font-size: 15px; font-weight: 600; }}
.graph-subtitle {{ font-size: 11px; color: #6b7280; margin-top: 2px; }}
#cy {{ flex: 1; background: #fff; }}

/* ── Legend ── */
.legend {{ background: #fff; border-top: 1px solid #e5e7eb; padding: 8px 16px;
           display: flex; gap: 20px; align-items: center; flex-shrink: 0; flex-wrap: wrap; }}
.legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 11px; color: #374151; }}
.legend-line {{ width: 24px; height: 2px; border-radius: 1px; }}
.legend-node {{ width: 12px; height: 12px; border-radius: 2px; border: 2px solid #555; }}
.legend-person {{ width: 12px; height: 12px; border-radius: 50%; border: 2px solid #555; }}

/* ── Print ── */
@media print {{
  .sidebar, .header, .nav-btn {{ display: none !important; }}
  .graph-area {{ width: 100%; }}
  #cy {{ height: 90vh !important; }}
}}
</style>
</head>
<body>

<div class="header">
  <div>
    <div class="header-h1" style="font-size:15px;font-weight:700;">OpenCheck — Demo Ownership Graphs</div>
    <div class="subtitle">Phase 7 · BODS v0.4 · 9 anchor entities · Cytoscape.js / dagre layout</div>
  </div>
  <div class="spacer"></div>
  <button class="nav-btn" onclick="navigate(-1)">← Prev</button>
  <span class="entity-counter" id="counter">1 / 9</span>
  <button class="nav-btn" onclick="navigate(1)">Next →</button>
</div>

<div class="main">
  <div class="sidebar">
    <div class="sidebar-title">Entities</div>
    {sidebar_items}
  </div>

  <div class="graph-area">
    <div class="graph-header">
      <div class="graph-title" id="graph-title">Loading…</div>
      <div class="graph-subtitle" id="graph-subtitle"></div>
    </div>
    <div id="cy"></div>
    <div class="legend">
      <div class="legend-item"><div class="legend-node"></div> Entity</div>
      <div class="legend-item"><div class="legend-person"></div> Person</div>
      <div class="legend-item"><div class="legend-line" style="background:#1565c0"></div> Owns</div>
      <div class="legend-item"><div class="legend-line" style="background:#e65100"></div> Controls</div>
      <div class="legend-item"><div class="legend-line" style="background:#6a1b9a;border-top:2px dashed #6a1b9a;height:0"></div> Manages</div>
      <div class="legend-item"><div class="legend-line" style="background:#0d9488"></div> Party to</div>
      <div class="legend-item"><div class="legend-line" style="background:#9ca3af"></div> Unknown</div>
      <div style="margin-left:auto;font-size:10px;color:#9ca3af">Arrow = owner/controller → owned/controlled · Use scroll to zoom · Drag to pan</div>
    </div>
  </div>
</div>

<script>
const ENTITIES = {entities_json};
let currentIndex = 0;
let cy = null;

const FAMILY_COLOR = {{
  OWNS: '#1565c0',
  CONTROLS: '#e65100',
  MANAGES: '#6a1b9a',
  IS_PARTY_TO: '#0d9488',
  UNKNOWN: '#9ca3af',
}};

function buildElements(graph) {{
  const elements = [];
  for (const n of graph.nodes) {{
    elements.push({{
      data: {{
        id: n.id,
        label: n.label,
        type: n.type,
        entityType: n.entityType,
        jurisdiction: n.jurisdiction,
      }}
    }});
  }}
  for (const e of graph.edges) {{
    elements.push({{
      data: {{
        id: e.id,
        source: e.source,
        target: e.target,
        family: e.family,
        interestType: e.interestType,
        shareLabel: e.shareLabel,
        label: e.shareLabel || '',
      }}
    }});
  }}
  return elements;
}}

function selectEntity(index) {{
  currentIndex = index;
  const ent = ENTITIES[index];

  // Update sidebar
  document.querySelectorAll('.entity-item').forEach((el, i) => {{
    el.classList.toggle('active', i === index);
  }});

  // Update header
  document.getElementById('graph-title').textContent = ent.name;
  const sc = ent.statementCounts;
  const featureStr = ent.features.map(f => f.replace(/_/g,' ').toUpperCase()).join(' · ');
  document.getElementById('graph-subtitle').textContent =
    `LEI: ${{ent.lei}}${{ent.ch ? '  ·  CH: ' + ent.ch : ''}}  ·  ${{sc.entity||0}} entities · ${{sc.person||0}} persons · ${{sc.relationship||0}} relationships${{featureStr ? '  ·  ' + featureStr : ''}}`;
  document.getElementById('counter').textContent = `${{index+1}} / ${{ENTITIES.length}}`;

  // Destroy previous cy instance
  if (cy) {{ cy.destroy(); cy = null; }}

  const elements = buildElements(ent.graph);

  cy = cytoscape({{
    container: document.getElementById('cy'),
    elements,
    style: [
      {{
        selector: 'node',
        style: {{
          'label': 'data(label)',
          'text-wrap': 'wrap',
          'text-max-width': '120px',
          'font-size': '10px',
          'text-valign': 'center',
          'text-halign': 'center',
          'background-color': '#fff',
          'border-width': 2,
          'border-color': '#555',
          'width': 'label',
          'height': 'label',
          'padding': '8px',
          'shape': 'round-rectangle',
        }}
      }},
      {{
        selector: 'node[type="person"]',
        style: {{
          'shape': 'ellipse',
          'border-color': '#374151',
          'background-color': '#f9fafb',
        }}
      }},
      {{
        selector: 'node[entityType="arrangement"]',
        style: {{
          'shape': 'diamond',
          'border-color': '#4338ca',
        }}
      }},
      {{
        selector: 'edge',
        style: {{
          'width': 1.5,
          'line-color': '#9ca3af',
          'target-arrow-color': '#9ca3af',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'label': 'data(label)',
          'font-size': '9px',
          'text-background-color': '#fff',
          'text-background-opacity': 0.8,
          'text-background-padding': '2px',
          'arrow-scale': 0.8,
        }}
      }},
      ...['OWNS','CONTROLS','MANAGES','IS_PARTY_TO'].map(fam => ({{
        selector: `edge[family="${{fam}}"]`,
        style: {{
          'line-color': FAMILY_COLOR[fam],
          'target-arrow-color': FAMILY_COLOR[fam],
          ...(fam === 'MANAGES' ? {{'line-style': 'dashed'}} : {{}}),
        }}
      }})),
    ],
    layout: {{
      name: 'dagre',
      rankDir: 'TB',
      nodeSep: 40,
      rankSep: 60,
      padding: 20,
      animate: false,
    }},
  }});
}}

function navigate(delta) {{
  const next = (currentIndex + delta + ENTITIES.length) % ENTITIES.length;
  selectEntity(next);
  document.querySelectorAll('.entity-item')[next].scrollIntoView({{block:'nearest'}});
}}

document.addEventListener('keydown', e => {{
  if (e.key === 'ArrowRight' || e.key === 'ArrowDown') navigate(1);
  if (e.key === 'ArrowLeft'  || e.key === 'ArrowUp')   navigate(-1);
}});

// Init
selectEntity(0);
</script>
</body>
</html>"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--demo-dir", type=Path, default=_DEFAULT_DEMO_DIR)
    parser.add_argument("--out", type=Path, default=_DEFAULT_OUT_DIR)
    args = parser.parse_args()

    manifest_path = args.demo_dir / "manifest.json"
    if not manifest_path.exists():
        log.error("manifest.json not found in %s — run 'make build-demo' first", args.demo_dir)
        sys.exit(1)

    log.info("=== Phase 7: Slide HTML generation ===")
    entities = load_entity_data(args.demo_dir, manifest_path)

    # Inline JS libraries so the file works with no internet connection
    log.info("Fetching JS libraries for inlining…")
    inline_scripts = {
        "cytoscape": _fetch_script("https://unpkg.com/cytoscape@3.28.1/dist/cytoscape.min.js"),
        "dagre": _fetch_script("https://unpkg.com/dagre@0.8.5/dist/dagre.min.js"),
        "cytoscape_dagre": _fetch_script("https://unpkg.com/cytoscape-dagre@2.5.0/cytoscape-dagre.js"),
    }
    missing = [k for k, v in inline_scripts.items() if not v]
    if missing:
        log.warning("Could not inline %s — file will need network access", missing)
    else:
        log.info("All JS libraries inlined — file is fully self-contained")

    args.out.mkdir(parents=True, exist_ok=True)
    out_path = args.out / "opencheck_demo.html"
    html = render_html(entities, inline_scripts=inline_scripts)
    out_path.write_text(html, encoding="utf-8")

    size_kb = out_path.stat().st_size // 1024
    log.info("Written: %s (%d KB, %d entities)", out_path, size_kb, len(entities))
    log.info("Open in browser: open %s", out_path)


if __name__ == "__main__":
    main()
