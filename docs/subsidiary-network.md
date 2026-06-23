# GLEIF subsidiary network

A lazy, panel-only reveal on the **GLEIF** source that pulls a subject's
**direct and ultimate children** from GLEIF Level 2 (accounting consolidation),
maps them to BODS v0.4, and shows the resulting network as an interactive graph
(small networks) or a table + BODS export (large ones).

It complements the existing GLEIF parent/direct-child relationships shown on the
main lookup: this is the **outward, whole-network** view, fetched on demand
because a large group can run to hundreds or thousands of entities.

- Endpoint: `GET /subsidiaries?lei=<LEI>` (never on the main lookup).
  `?format=bods` additionally returns the BODS statements for the graph/export.
- Gating: `OPENCHECK_ALLOW_LIVE`. No API key — GLEIF is open (CC0).
- Service: `backend/opencheck/subsidiaries.py`; router:
  `backend/opencheck/routers/subsidiaries.py`; mapper:
  `map_gleif_subsidiaries()` in `bods/mapper.py`; UI:
  `frontend/src/components/cdd/SubsidiaryNetwork.tsx`.

## Direct vs ultimate, and the "both" case

GLEIF Level 2 publishes two consolidation relationships per parent/child:

- **direct child** — the entity is *directly* consolidated by the subject;
- **ultimate child** — the entity is *ultimately* consolidated by the subject
  (the group head), possibly several layers down.

The two sets overlap. An entity that the subject consolidates *both* directly and
ultimately appears in **both** GLEIF endpoints. The service fetches both
(`/lei-records/{lei}/direct-children` and `/ultimate-children`), merges by child
LEI, and tags each child `direct`, `ultimate`, or `both`. Counts are taken from
GLEIF's pagination `total`, so they are **exact even when the child list is
capped** (10 pages × 100 per relation).

## BODS mapping — keep both statements, merge the edge

`map_gleif_subsidiaries()` emits a subject entity statement, one entity statement
per distinct child, and a relationship statement **per relation**. A `both` child
therefore carries **two** relationship statements — one `directOrIndirect:
"direct"`, one `"indirect"` — kept distinct in the data and the export.

The graph then merges those two statements into **one annotated edge**
(`bodsToGraph` in `frontend/src/lib/bodsGraph.ts`): the same-pair direct +
ultimate edges are pooled (clean-up B) rather than drawn twice or one being
suppressed (clean-up C, which still drops the *skip-level* ultimate edges that
the direct tree already implies). The merged edge is labelled **"Controls (direct
+ ultimate)"**. So the BODS is faithful to GLEIF (both relationships present) and
the visual is rationalised (one edge, annotated) — the design decision was to
**merge visually only**, never to drop a statement.

## Graph vs table — the readability threshold

`node_estimate = max(direct_total, ultimate_total, distinct_fetched)`.

- **≤ 150 nodes** → `render_mode: "graph"`. The panel offers "Show network
  graph", which fetches the BODS (`format=bods`) and renders it in the standard
  `BodsGraphExplorer` (Cytoscape), loaded lazily.
- **> 150 nodes** → `render_mode: "table"`. A hairball helps no one, so the panel
  shows the children as a table (direct children first, then the indirect
  ultimate-only tail) plus a **Download BODS** button so the network can be
  rendered in the user's own graph tooling.

Either way the children list is shown direct-first-then-tail, and a `truncated`
note appears when the fetched sample is smaller than the exact total.

## Disclosure (three layers)

1. **Invitation** — a "Reveal subsidiary network" strip on the GLEIF card.
   Nothing fires until clicked (the children fetch is several GLEIF calls).
2. **Summary** — direct/ultimate/distinct counts, an indirect-only count, and the
   jurisdiction spread, shown as soon as the summary returns.
3. **Detail** — the interactive graph (small) or the BODS export (large), each
   fetched only when requested.

## Limits and roadmap (v1)

- **Panel-only.** Does not emit an OpenCheck risk signal and is not part of the
  main lookup, AI summary, or PDF — it is an explorer, fetched on demand.
- **Cap.** 10 pages × 100 children per relation are fetched; counts stay exact
  above that and the UI marks the list truncated.
- **Roadmap** — a `COMPLEX_GROUP_STRUCTURE` style signal off the network shape;
  jurisdiction-risk overlays on the children (offshore concentration); and
  cross-referencing children LEIs back through the standard lookup.
