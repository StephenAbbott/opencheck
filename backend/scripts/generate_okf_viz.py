#!/usr/bin/env python3
"""Render the OpenCheck OKF bundle as a self-contained interactive viz.html.

A proof-of-concept OKF *consumer* (mirroring the reference visualiser in the OKF
repo): it parses every concept in ``okf/``, embeds the bundle as JSON, and emits
one standalone HTML file — a force-directed graph of concepts (coloured by type,
edges from cross-links) with a detail panel that renders each concept's
markdown body, a search box, a type filter, and "cited by" backlinks. No backend,
no install on the viewing side; Cytoscape.js and marked are loaded from a CDN.

Usage:
    python backend/scripts/generate_okf_viz.py            # writes okf/viz.html
    python backend/scripts/generate_okf_viz.py --out /tmp/okf.html
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

OKF_ROOT = Path(__file__).resolve().parents[2] / "okf"
_LINK_RE = re.compile(r"\]\(([^)]+)\)")


def _parse_value(v: str):
    v = v.strip()
    if v.startswith("[") and v.endswith("]"):
        inner = v[1:-1].strip()
        return [_parse_value(x) for x in inner.split(",")] if inner else []
    if len(v) >= 2 and v[0] in "\"'" and v[-1] == v[0]:
        try:
            return json.loads(v) if v[0] == '"' else v[1:-1]
        except Exception:
            return v[1:-1]
    if v in ("true", "false"):
        return v == "true"
    return v


def _parse_frontmatter(text: str) -> tuple[dict, str]:
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 3)
    if end == -1:
        return {}, text
    meta: dict = {}
    for line in text[3:end].splitlines():
        if not line.strip() or line.strip().startswith("#") or ":" not in line:
            continue
        k, v = line.split(":", 1)
        meta[k.strip()] = _parse_value(v)
    return meta, text[end + 4:].lstrip("\n")


def _resolve(link: str, from_id: str) -> str | None:
    """Resolve a markdown link target to a concept id, or None."""
    link = link.split("#")[0].strip()
    if not link or link.startswith(("http://", "https://", "mailto:")):
        return None
    if not link.endswith(".md"):
        return None
    if link.startswith("/"):
        target = link[1:]
    else:
        base = from_id.rsplit("/", 1)[0] if "/" in from_id else ""
        parts = (base.split("/") if base else []) + link.split("/")
        stack: list[str] = []
        for p in parts:
            if p in ("", "."):
                continue
            if p == "..":
                if stack:
                    stack.pop()
            else:
                stack.append(p)
        target = "/".join(stack)
    return target[:-3] if target.endswith(".md") else target


def build_bundle() -> dict:
    concepts = []
    ids: set[str] = set()
    for f in sorted(OKF_ROOT.rglob("*.md")):
        if f.name in {"index.md", "log.md"}:
            continue
        cid = str(f.relative_to(OKF_ROOT).with_suffix(""))
        ids.add(cid)
    for f in sorted(OKF_ROOT.rglob("*.md")):
        if f.name in {"index.md", "log.md"}:
            continue
        cid = str(f.relative_to(OKF_ROOT).with_suffix(""))
        meta, body = _parse_frontmatter(f.read_text(encoding="utf-8"))
        links = []
        for m in _LINK_RE.finditer(body):
            tid = _resolve(m.group(1), cid)
            if tid and tid in ids and tid != cid and tid not in links:
                links.append(tid)
        concepts.append(
            {
                "id": cid,
                "type": str(meta.get("type", "Concept")),
                "title": str(meta.get("title", cid.rsplit("/", 1)[-1])),
                "description": str(meta.get("description", "")),
                "resource": meta.get("resource"),
                "tags": meta.get("tags", []) if isinstance(meta.get("tags"), list) else [],
                "body": body,
                "links": links,
            }
        )
    return {"name": "OpenCheck OKF", "concepts": concepts}


_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>__NAME__ — OKF browser</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.30.2/cytoscape.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/marked/marked.min.js"></script>
<style>
  :root { --bg:#f3f3f5; --panel:#fff; --rule:#e5e5e5; --ink:#191d23; --muted:#757575; --accent:#3d30d4; }
  * { box-sizing:border-box; }
  html,body { margin:0; height:100%; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; color:var(--ink); background:var(--bg); }
  #app { display:grid; grid-template-columns: 280px 1fr 420px; height:100vh; }
  #side, #detail { background:var(--panel); border-right:1px solid var(--rule); overflow:auto; padding:14px; }
  #detail { border-right:none; border-left:1px solid var(--rule); }
  #cy { height:100vh; }
  h1 { font-size:15px; margin:0 0 4px; }
  .sub { color:var(--muted); font-size:12px; margin:0 0 12px; }
  input[type=search]{ width:100%; padding:7px 9px; border:1px solid var(--rule); border-radius:6px; font-size:13px; margin-bottom:10px;}
  .types label { display:flex; align-items:center; gap:7px; font-size:12.5px; padding:3px 0; cursor:pointer; }
  .swatch { width:11px;height:11px;border-radius:50%; display:inline-block; }
  .eyebrow { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); margin:14px 0 4px; }
  #detail h2 { font-size:16px; margin:0 0 2px; }
  #detail .desc { color:var(--muted); font-size:13px; margin:0 0 10px; }
  .chip { display:inline-block; font-size:11px; background:var(--bg); border:1px solid var(--rule); border-radius:999px; padding:2px 8px; margin:0 4px 4px 0; }
  .body { font-size:13.5px; line-height:1.6; }
  .body h1{font-size:15px;margin:16px 0 6px;} .body h2{font-size:13.5px;margin:14px 0 5px;}
  .body table{border-collapse:collapse;font-size:12px;} .body td,.body th{border:1px solid var(--rule);padding:4px 7px;text-align:left;}
  .body code{background:var(--bg);padding:1px 4px;border-radius:4px;font-size:12px;}
  .body pre{background:var(--bg);padding:10px;border-radius:8px;overflow:auto;}
  .body a{color:var(--accent);text-decoration:none;} .body a:hover{text-decoration:underline;}
  a.res { color:var(--accent); font-size:12px; text-decoration:none; } a.res:hover{text-decoration:underline;}
  .backlinks a { display:block; font-size:12.5px; color:var(--accent); text-decoration:none; padding:2px 0; }
  .empty { color:var(--muted); font-size:13px; }
</style>
</head>
<body>
<div id="app">
  <div id="side">
    <h1>__NAME__</h1>
    <p class="sub" id="count"></p>
    <input type="search" id="q" placeholder="Search title, id, tags…" />
    <div class="eyebrow">Types</div>
    <div class="types" id="types"></div>
  </div>
  <div id="cy"></div>
  <div id="detail"><p class="empty">Select a concept in the graph, or search on the left.</p></div>
</div>
<script>
const BUNDLE = __BUNDLE__;
const PALETTE = ["#3d30d4","#1f9d4e","#c2410c","#0369a1","#6d28d9","#be123c","#9a3412","#0f766e","#a16207"];
const types = [...new Set(BUNDLE.concepts.map(c => c.type))].sort();
const colorOf = {}; types.forEach((t,i)=> colorOf[t]=PALETTE[i % PALETTE.length]);
const byId = Object.fromEntries(BUNDLE.concepts.map(c=>[c.id,c]));
const active = new Set(types);

const elements = [];
BUNDLE.concepts.forEach(c => elements.push({ data:{ id:c.id, label:c.title, type:c.type } }));
BUNDLE.concepts.forEach(c => c.links.forEach(t => elements.push({ data:{ id:c.id+"->"+t, source:c.id, target:t } })));

const cy = cytoscape({
  container: document.getElementById("cy"),
  elements,
  style: [
    { selector:"node", style:{ "background-color":(e)=>colorOf[e.data("type")]||"#888",
      "label":"data(label)","font-size":"9px","color":"#191d23","text-wrap":"wrap","text-max-width":"110px",
      "text-valign":"bottom","text-margin-y":3,"width":16,"height":16 } },
    { selector:"edge", style:{ "width":1,"line-color":"#cfcfe6","target-arrow-color":"#cfcfe6",
      "target-arrow-shape":"triangle","curve-style":"bezier","arrow-scale":0.7 } },
    { selector:".dim", style:{ "opacity":0.12 } },
    { selector:"node.sel", style:{ "border-width":3,"border-color":"#191d23" } },
  ],
  layout:{ name:"cose", animate:false, padding:30, nodeRepulsion:9000, idealEdgeLength:90 },
});

function renderTypes(){
  const el = document.getElementById("types");
  el.innerHTML = "";
  types.forEach(t => {
    const lab = document.createElement("label");
    lab.innerHTML = `<input type="checkbox" checked data-t="${t}"><span class="swatch" style="background:${colorOf[t]}"></span>${t}`;
    el.appendChild(lab);
  });
  el.querySelectorAll("input").forEach(cb => cb.onchange = () => {
    cb.checked ? active.add(cb.dataset.t) : active.delete(cb.dataset.t);
    applyFilter();
  });
}
function applyFilter(){
  const q = document.getElementById("q").value.trim().toLowerCase();
  cy.nodes().forEach(n => {
    const c = byId[n.id()];
    const matchType = active.has(c.type);
    const matchText = !q || c.title.toLowerCase().includes(q) || c.id.toLowerCase().includes(q)
      || (c.tags||[]).some(tag => String(tag).toLowerCase().includes(q));
    n.style("display", (matchType && matchText) ? "element" : "none");
  });
  cy.edges().forEach(e => {
    e.style("display", (e.source().style("display")!=="none" && e.target().style("display")!=="none") ? "element":"none");
  });
}
function citedBy(id){ return BUNDLE.concepts.filter(c => c.links.includes(id)); }

function select(id){
  const c = byId[id]; if(!c) return;
  cy.nodes().removeClass("sel"); cy.getElementById(id).addClass("sel");
  const d = document.getElementById("detail");
  const tags = (c.tags||[]).map(t=>`<span class="chip">${t}</span>`).join("");
  const res = c.resource ? `<p><a class="res" href="${c.resource}" target="_blank" rel="noreferrer">${c.resource} ↗</a></p>` : "";
  const back = citedBy(id);
  const backHtml = back.length ? `<div class="eyebrow">Cited by</div><div class="backlinks">${back.map(b=>`<a href="#" data-go="${b.id}">${b.title}</a>`).join("")}</div>` : "";
  d.innerHTML = `<span class="chip" style="border-color:${colorOf[c.type]};color:${colorOf[c.type]}">${c.type}</span>
    <h2>${c.title}</h2><p class="desc">${c.description||""}</p>${res}
    <div class="body">${marked.parse(c.body||"")}</div>${backHtml}`;
  // rewire internal links + backlinks to navigate within the viewer
  d.querySelectorAll(".body a").forEach(a => {
    const href = a.getAttribute("href")||"";
    const tid = resolveHref(href, id);
    if(tid && byId[tid]){ a.onclick = (e)=>{ e.preventDefault(); select(tid); }; }
  });
  d.querySelectorAll("[data-go]").forEach(a => a.onclick = (e)=>{ e.preventDefault(); select(a.dataset.go); });
}
function resolveHref(href, fromId){
  if(!href || href.startsWith("http")) return null;
  href = href.split("#")[0];
  let target;
  if(href.startsWith("/")) target = href.slice(1);
  else { const base = fromId.includes("/")? fromId.slice(0, fromId.lastIndexOf("/")) : "";
    const parts = (base?base.split("/"):[]).concat(href.split("/")); const st=[];
    parts.forEach(p=>{ if(p===""||p===".")return; if(p===".."){st.pop();} else st.push(p); }); target=st.join("/"); }
  if(target.endsWith(".md")) target = target.slice(0,-3);
  if(target.endsWith("/index")) target = target.slice(0,-6);
  return target;
}

cy.on("tap","node", e => select(e.target.id()));
document.getElementById("q").addEventListener("input", applyFilter);
document.getElementById("count").textContent = BUNDLE.concepts.length + " concepts · " + types.length + " types";
renderTypes();
</script>
</body>
</html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(OKF_ROOT / "viz.html"), help="output HTML path")
    args = ap.parse_args()
    bundle = build_bundle()
    # Escape "</" so a body containing "</script>" cannot break out of the tag.
    embedded = json.dumps(bundle).replace("</", "<\\/")
    html = _HTML.replace("__BUNDLE__", embedded).replace("__NAME__", bundle["name"])
    out = Path(args.out)
    out.write_text(html, encoding="utf-8")
    edges = sum(len(c["links"]) for c in bundle["concepts"])
    print(f"Wrote {out} — {len(bundle['concepts'])} concepts, {edges} cross-links.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
