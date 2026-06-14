---
okf_version: "0.1"
---

# OpenCheck

* [Project overview](/overview.md) - What OpenCheck is: an open beneficial-ownership and corporate-data aggregator that queries ~30 sources and synthesises Beneficial Ownership Data Standard (BODS) v0.4 output.
* [Architecture](/architecture.md) - The lookup pipeline, source adapters, BODS mapper, reconciler, risk engine, and React frontend.
* [Glossary](/glossary.md) - Beneficial-ownership terminology: BO, UBO, PSC, RLE, LEI, nominee.

# Subdirectories

* [standards](/standards/) - The data standards OpenCheck emits and anchors on — BODS v0.4 and GLEIF / LEI.
* [sources](/sources/) - The ~30 company and beneficial-ownership data sources OpenCheck queries, each with its licence terms.
* [api](/api/) - OpenCheck's HTTP API: search, lookup, deepen, export, licensing.
* [licensing](/licensing/) - The per-source licensing compatibility matrix used at export time.
