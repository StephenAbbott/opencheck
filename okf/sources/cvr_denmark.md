---
type: "Data Source"
title: "CVR \u2014 Det Centrale Virksomhedsregister"
description: "Danish Central Business Register (CVR) \u2014 the authoritative register of all Danish businesses, maintained by Erhvervsstyrelsen (the Danish Business Authority). Accessed via the Datafordeler GraphQL API (non-restricted entity data)."
resource: "https://datacvr.virk.dk/"
tags: ["cdd", "national-register", "Danish Open Government Data (CVR brugervilk\u00e5r)", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "cvr_denmark"
license: "Danish Open Government Data (CVR brugervilk\u00e5r)"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Danish Central Business Register (CVR) — the authoritative register of all Danish businesses, maintained by Erhvervsstyrelsen (the Danish Business Authority). Accessed via the Datafordeler GraphQL API (non-restricted entity data). Official national company / beneficial-ownership register.

- **Source id:** `cvr_denmark`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `dk_cvr`

# Licensing

- **Licence:** `Danish Open Government Data (CVR brugervilkår)` — Danish Open Government Data (CVR brugervilkår)
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Indeholder data fra Det Centrale Virksomhedsregister (CVR), Erhvervsstyrelsen / Danish Business Authority. Data distribueret via Datafordelerens CVR GraphQL API.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_cvr_denmark`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://datacvr.virk.dk/
