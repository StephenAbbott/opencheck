---
type: "Data Source"
title: "Ministry of Corporate Affairs \u2014 Company Master Data (India)"
description: "India's national company register extract \u2014 CIN, company name, status, class/category, authorised and paid-up capital, registration date, Registrar of Companies, registered office address and NIC industrial classification \u2014 from the Ministry of Corporate Affairs Company Master Data on the Open Government Data Platform. Entity-level only; no officer or ownership data. Exact-match search."
resource: "https://www.data.gov.in/catalog/company-master-data"
tags: ["cdd", "national-register", "GODL-India", "commercial-conditional"]
timestamp: "2026-08-08"
source_id: "mca_india"
license: "GODL-India"
commercial_use: "conditional"
category: "cdd"
national_register: true
---

# Overview

India's national company register extract — CIN, company name, status, class/category, authorised and paid-up capital, registration date, Registrar of Companies, registered office address and NIC industrial classification — from the Ministry of Corporate Affairs Company Master Data on the Open Government Data Platform. Entity-level only; no officer or ownership data. Exact-match search. Official national company / beneficial-ownership register.

- **Source id:** `mca_india`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `in_cin`

# Licensing

- **Licence:** `GODL-India` — GODL-India
- **Commercial use:** conditional · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains Ministry of Corporate Affairs Company Master Data, published by the Open Government Data (OGD) Platform India and used under the Government Open Data License – India. MCA and the OGD Platform do not endorse this use.
- Bespoke or unrecognised licence — verify terms before re-use.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_mca_india`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.data.gov.in/catalog/company-master-data
