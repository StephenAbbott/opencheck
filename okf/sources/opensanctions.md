---
type: "Data Source"
title: "OpenSanctions"
description: "Sanctions lists, PEPs, debarments, and regulatory actions from the OpenSanctions open-source database."
resource: "https://www.opensanctions.org/"
tags: ["cdd", "aggregator", "CC-BY-NC-4.0", "commercial-no"]
timestamp: "2026-07-24"
source_id: "opensanctions"
license: "CC-BY-NC-4.0"
commercial_use: "no"
category: "cdd"
national_register: false
---

# Overview

Sanctions lists, PEPs, debarments, and regulatory actions from the OpenSanctions open-source database. Aggregator, cross-border database or ESG source.

- **Source id:** `opensanctions`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity, person
- **Requires API key:** yes
- **National register:** no


# Licensing

- **Licence:** `CC-BY-NC-4.0` — Creative Commons Attribution-NonCommercial 4.0
- **Commercial use:** no · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Data from OpenSanctions.org, licensed CC BY-NC 4.0.
- NON-COMMERCIAL only; attribution required; no commercial re-use.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_opensanctions`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.opensanctions.org/
