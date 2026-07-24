---
type: "Data Source"
title: "Australian Business Register (ABN Lookup)"
description: "Australian company and business data \u2014 ABN, ACN, entity name and type, ABN/GST status, registered state and postcode, and business (trading) names \u2014 from the Australian Business Register's free ABN Lookup web services. Entity-level only; no officer or ownership data."
resource: "https://abr.business.gov.au/"
tags: ["cdd", "national-register", "CC-BY-3.0-AU", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "abr_australia"
license: "CC-BY-3.0-AU"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Australian company and business data — ABN, ACN, entity name and type, ABN/GST status, registered state and postcode, and business (trading) names — from the Australian Business Register's free ABN Lookup web services. Entity-level only; no officer or ownership data. Official national company / beneficial-ownership register.

- **Source id:** `abr_australia`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `au_acn`, `au_abn`

# Licensing

- **Licence:** `CC-BY-3.0-AU` — Creative Commons Attribution 3.0 Australia
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data sourced from the Australian Business Register (ABR), used under CC BY 3.0 AU. The Australian Taxation Office does not endorse this use.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_abr_australia`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://abr.business.gov.au/
