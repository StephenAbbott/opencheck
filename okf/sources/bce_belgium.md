---
type: "Data Source"
title: "Belgian Crossroads Bank for Enterprises (BCE/KBO)"
description: "Belgium's central business register, read from a monthly index of the KBO Open Data full file: name (NL/FR/DE), status, juridical situation and form, start date and registered office for every active registered entity. No shareholders, officers or beneficial owners \u2014 the Belgian UBO register is held separately and is not open."
resource: "https://kbopub.economie.fgov.be/kbo-open-data/"
tags: ["cdd", "national-register", "Custom-KBO-Reuse", "commercial-conditional"]
timestamp: "2026-10-29"
source_id: "bce_belgium"
license: "Custom-KBO-Reuse"
commercial_use: "conditional"
category: "cdd"
national_register: true
---

# Overview

Belgium's central business register, read from a monthly index of the KBO Open Data full file: name (NL/FR/DE), status, juridical situation and form, start date and registered office for every active registered entity. No shareholders, officers or beneficial owners — the Belgian UBO register is held separately and is not open. Official national company / beneficial-ownership register.

- **Source id:** `bce_belgium`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `be_enterprise_number`

# Licensing

- **Licence:** `Custom-KBO-Reuse` — Belgian KBO/BCE re-use conditions
- **Commercial use:** conditional · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Data from the Belgian Crossroads Bank for Enterprises (BCE/KBO), made available by the FPS Economy, SMEs, Self-Employed and Energy, Belgium.
- Free re-use with notification; commercial use requires an agreement with KBO/BCE.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_bce_belgium`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://kbopub.economie.fgov.be/kbo-open-data/
