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

## Including it in the main export (opt-in)

By default the subsidiary network is **not** part of the main `/export` bundle —
a large group can add hundreds of statements, so it stays a separate on-demand
view. But a compliance user who wants "the full BODS for this entity, including
its group" can opt in: **`GET /export?lei=<LEI>&subsidiaries=true`** folds the
subsidiary BODS into the bundle for every format (JSON / JSONL / XML / ZIP). The
subject statement is de-duplicated by `statementId` (it is shared with the GLEIF
subject), the merged bundle is re-validated, and the ZIP manifest records
`subsidiary_network_included` + `subsidiary_statement_count`. The Export panel
exposes this as a checkbox. Off by default, gated on `OPENCHECK_ALLOW_LIVE`.

## Limits and roadmap (v1)

- **Lazy by default.** Does not emit an OpenCheck risk signal and is not on the
  main lookup, AI summary, or PDF — it is an explorer, fetched on demand (and now
  optionally folded into `/export`, see above).
- **Cap.** 10 pages × 100 children per relation are fetched; counts stay exact
  above that and the UI marks the list truncated.
- **Roadmap** — a `COMPLEX_GROUP_STRUCTURE` style signal off the network shape;
  jurisdiction-risk overlays on the children (offshore concentration); and
  cross-referencing children LEIs back through the standard lookup.

## The Subsidiaries tab (Phase 185)

Everything above now lives on its own tab, `?mode=subsidiaries`, the fifth
check mode. The GLEIF network moved there from the bottom of FullCheck (where
it sat behind the invitation strip and had no URL), and the tab fetches the
summary on arrival — a reader who opened it has already asked — while the
Cytoscape graph stays behind a click. Every child links to its own
Subsidiaries tab, so a reader can walk down a group; GLEIF's record is one
click further.

The tab also brings together the lists OpenCheck holds from other sources,
served by **`GET /subsidiaries/declared?lei=`** (`opencheck/subsidiaries_declared.py`)
and kept apart per source:

| List | What it measures | Identifier |
|---|---|---|
| GLEIF Level 2 | accounting consolidation (direct + ultimate) | LEI on every row |
| OECD-UNSD MEIP | the register of the 500 largest MNEs' subsidiaries; only the LEI-carrying subset is held | LEI on every row; `total` is the register's own count |
| EITI Company Assessment | what a supporting company declared about its extractive operations | none, by design |
| Global Energy Monitor | entities GEM records as directly owned, with a percentage where it has one | LEI where the GLEIF GEM↔LEI mapping or GEM's own column supplies one |

They disagree, and are meant to: no two measure the same thing and no public
source publishes the whole picture. The tab's first sentence is built from the
numbers (`lib/subsidiariesMode.ts`, `coverageSentence`) rather than asserted.
What the tab *does* settle is which rows can be opened: an LEI is attached
only where a source's own data carries one, and a name that another list
holds an LEI for is offered with a match chip — a name match, never an
identity. Rows with neither say "no LEI published". No live GLEIF name search
is run: the EITI ticket records how "Equinor" matched a company sports club.

**The API surface is unchanged.** `GET /subsidiaries`, `/export?subsidiaries`,
`/expand-layer direction=subsidiaries` and the MCP server all still serve the
GLEIF network only; `/subsidiaries/declared` exists for the tab and is not on
the API page. The OECD's BODS release of MEIP is the next step, on its own
ticket.
