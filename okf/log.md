# OpenCheck OKF — update log

## 2026-06-14
* **Initialization**: Created the OpenCheck Open Knowledge Format (OKF v0.1) bundle — [project overview](/overview.md), [architecture](/architecture.md), [glossary](/glossary.md), the [BODS v0.4](/standards/bods.md) and [LEI-anchoring](/standards/lei-anchoring.md) standards, the [API endpoints](/api/), and auto-generated per-[source](/sources/) and [licensing](/licensing/matrix.md) concepts.
* **Tooling**: Added `backend/scripts/generate_okf.py` (the enrichment agent) to regenerate the source and licensing concepts from the live registry, with a `--check` conformance validator.
