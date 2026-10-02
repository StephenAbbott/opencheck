---
type: "Data Source"
title: "ChileCompra \u2014 Mercado P\u00fablico (Chilean public procurement)"
description: "Chile's public procurement platform, run by the Direcci\u00f3n ChileCompra, published monthly as open data. For a company supplier: purchase orders and their value in pesos, tenders bid and won, and the public bodies it sold to, over the last twelve months. Matched on the RUT. Sole traders are not indexed. Procurement activity, not ownership."
resource: "https://www.mercadopublico.cl/"
tags: ["cdd", "aggregator", "CC0-1.0", "commercial-yes"]
timestamp: "2026-10-02"
source_id: "chilecompra"
license: "CC0-1.0"
commercial_use: "yes"
category: "cdd"
national_register: false
---

# Overview

Chile's public procurement platform, run by the Dirección ChileCompra, published monthly as open data. For a company supplier: purchase orders and their value in pesos, tenders bid and won, and the public bodies it sold to, over the last twelve months. Matched on the RUT. Sole traders are not indexed. Procurement activity, not ownership. Aggregator, cross-border database or ESG source.

- **Source id:** `chilecompra`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** no
- **Lookup keys (LEI-anchored dispatch):** `cl_rut`

# Licensing

- **Licence:** `CC0-1.0` — Creative Commons Zero v1.0 (public domain)
- **Commercial use:** yes · **Attribution:** not required · **Share-alike:** no
- **Attribution line:** Fuente: Dirección ChileCompra — monthly open data on tenders and purchase orders from Mercado Público (datos-abiertos.chilecompra.cl).
- Public-domain dedication; no restrictions, no attribution required.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_chilecompra`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.mercadopublico.cl/
