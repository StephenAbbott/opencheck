---
type: "Data Source"
title: "Hong Kong Companies Registry"
description: "Hong Kong company data from the Companies Registry's open data API on DATA.GOV.HK. Provides core entity details for live local companies \u2014 English and Chinese names, Business Registration Number, company type, registered office and incorporation date."
resource: "https://www.cr.gov.hk/"
tags: ["cdd", "national-register", "DATA.GOV.HK-Terms", "commercial-yes"]
timestamp: "2026-09-10"
source_id: "cr_hongkong"
license: "DATA.GOV.HK-Terms"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Hong Kong company data from the Companies Registry's open data API on DATA.GOV.HK. Provides core entity details for live local companies — English and Chinese names, Business Registration Number, company type, registered office and incorporation date. Official national company / beneficial-ownership register.

- **Source id:** `cr_hongkong`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `hk_brn`

# Licensing

- **Licence:** `DATA.GOV.HK-Terms` — DATA.GOV.HK Terms and Conditions of Use
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Companies Registry of the Government of the Hong Kong Special Administrative Region, made available via DATA.GOV.HK (data.cr.gov.hk).
- Free re-use, distribution and reproduction for commercial and non-commercial purposes, with attribution to the Hong Kong Government, the data provider and DATA.GOV.HK.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_cr_hongkong`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.cr.gov.hk/
