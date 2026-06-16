---
type: "Data Source"
title: "Receita Federal \u2014 CNPJ register (Brazil)"
description: "Brazilian company data from the Receita Federal CNPJ register (open data), including the QSA \u2014 partners and administrators. Served key-lessly via OpenCNPJ with a BrasilAPI fallback."
resource: "https://www.gov.br/receitafederal/"
tags: ["cdd", "national-register", "BR-Open-Data", "commercial-conditional"]
timestamp: "2026-06-16"
source_id: "cnpj_brazil"
license: "BR-Open-Data"
commercial_use: "conditional"
category: "cdd"
national_register: true
---

# Overview

Brazilian company data from the Receita Federal CNPJ register (open data), including the QSA — partners and administrators. Served key-lessly via OpenCNPJ with a BrasilAPI fallback. Official national company / beneficial-ownership register.

- **Source id:** `cnpj_brazil`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `br_cnpj`

# Licensing

- **Licence:** `BR-Open-Data` — BR-Open-Data
- **Commercial use:** conditional · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Receita Federal do Brasil CNPJ open data, served via OpenCNPJ (opencnpj.org) and BrasilAPI (brasilapi.com.br).
- Bespoke or unrecognised licence — verify terms before re-use.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_cnpj_brazil`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.gov.br/receitafederal/
