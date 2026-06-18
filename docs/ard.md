# Agentic Resource Discovery (ARD)

OpenCheck publishes an **[ARD](https://agenticresourcediscovery.org/) /
[ai-catalog](https://github.com/Agent-Card/ai-catalog) v1.0** manifest so AI
clients and discovery services can find OpenCheck's capabilities and learn how
to invoke them.

ARD sits *before* invocation: a discovery service crawls the manifest, indexes
the entries, and answers an agent's question *"what's available for this
task?"*. The agent then calls the resource through its own native mechanism —
here, the OpenCheck REST API described by the linked OpenAPI document.

## Where it lives

| Item | Value |
|---|---|
| Manifest (source) | `frontend/public/.well-known/ai-catalog.json` |
| Served at | `https://opencheck.world/.well-known/ai-catalog.json` |
| Spec version | `1.0` |
| Host identifier | `did:web:opencheck.world` |

Vite copies everything under `frontend/public/` to the build output, so the
file is published at the site root `/.well-known/` automatically — no build
change needed.

## What it advertises

Three capability-scoped entries, all pointing at the live OpenAPI document
(`https://api.opencheck.world/openapi.json`). They share one API but carry
distinct `capabilities` and `representativeQueries` so semantic search can match
the right capability to a task:

1. **`…:api:lei-due-diligence`** — LEI-driven CDD: ownership graph + sanctions /
   PEP / debarment / FATF / complex-structure risk signals (`GET /lookup`,
   `GET /lookup-stream`).
2. **`…:api:entity-search`** — name / national-ID → LEI resolution
   (`GET /search`), the entry point that yields the LEI.
3. **`…:api:bods-export`** — BODS v0.4 export of the ownership graph in
   JSON / JSON-Lines / zip (`GET /export`).

`type` is `application/openapi+json`: ARD/ai-catalog is artifact-agnostic, and
OpenCheck's native runtime is a REST API documented by OpenAPI (the official
vendor type `application/vnd.oai.openapi+json` is an equivalent alternative).

## Hosting requirements

The publishing guide requires the manifest to be served over **HTTPS**, with
**`Content-Type: application/json`** (Render serves `.json` correctly by
default) and **`Access-Control-Allow-Origin: *`** so crawlers can fetch it
cross-origin. The CORS header is wired in `render.yaml` on the `opencheck`
static site:

```yaml
headers:
  - path: /.well-known/ai-catalog.json
    name: Access-Control-Allow-Origin
    value: "*"
```

The SPA catch-all rewrite (`/*` → `/index.html`) does **not** intercept this
path: Render serves an existing static file before applying rewrites, and the
manifest is a real file in `dist/.well-known/`.

## Optional: DNS discovery

Not used — the standard `.well-known` path is served directly. If OpenCheck ever
needs to point discovery services at an off-domain location (e.g. an S3
bucket), publish a `TXT` record per the
[publishing guide](https://agenticresourcediscovery.org/how_to_publish/):

```
_catalog._agents.opencheck.world  TXT  "url=https://…/ai-catalog.json"
```

## Updating the manifest

Edit `frontend/public/.well-known/ai-catalog.json` and redeploy the frontend.
When adding or renaming an entry, keep the `identifier` in the domain-anchored
URN form `urn:ai:opencheck.world:<namespace>:<name>` and give each entry **2–5**
natural-language `representativeQueries` (the spec's recommendation for
high-fidelity semantic matching).

After editing, validate it still parses and conforms:

```bash
python3 - <<'PY'
import json, urllib.parse
m = json.load(open("frontend/public/.well-known/ai-catalog.json"))
assert m["specVersion"] == "1.0"
assert m["host"]["identifier"]
for e in m["entries"]:
    for k in ("identifier", "displayName", "type", "url"):
        assert e.get(k), f"missing {k}"
    assert e["identifier"].startswith("urn:ai:opencheck.world:")
print(f"OK — {len(m['entries'])} entries")
PY
```
