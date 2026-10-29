---
type: "Data Source"
title: "Taiwan company register (MOEA GCIS)"
description: "Taiwan company data from the Ministry of Economic Affairs' GCIS open data platform: the company's name, status, capital and registered address, and its directors and supervisors with the holdings the Company Act requires them to disclose \u2014 including the companies that hold board seats through representatives."
resource: "https://data.gcis.nat.gov.tw/"
tags: ["cdd", "national-register", "OGDL-Taiwan-1.0", "commercial-yes"]
timestamp: "2026-10-30"
source_id: "gcis_taiwan"
license: "OGDL-Taiwan-1.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Taiwan company data from the Ministry of Economic Affairs' GCIS open data platform: the company's name, status, capital and registered address, and its directors and supervisors with the holdings the Company Act requires them to disclose — including the companies that hold board seats through representatives. Official national company / beneficial-ownership register.

- **Source id:** `gcis_taiwan`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `tw_ubn`

# Licensing

- **Licence:** `OGDL-Taiwan-1.0` — Open Government Data License, version 1.0 (Taiwan)
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Ministry of Economic Affairs, Administration of Commerce (經濟部商業發展署) [2026] [公司登記基本資料-應用一; 公司登記董監事資料集]. Released under the Open Government Data License, version 1.0 (https://data.gov.tw/license); users may use it subject to the licence's terms.
- Use for any purpose, commercial included, sub-licensable and compatible with CC BY 4.0. Attribution in the licence's Exhibit form is a condition: without it the licence is void from the start.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_gcis_taiwan`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://data.gcis.nat.gov.tw/
