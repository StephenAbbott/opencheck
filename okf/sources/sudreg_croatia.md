---
type: "Data Source"
title: "Sudski registar \u2014 Croatian Court Register"
description: "Croatian company data from the Sudski registar (Court Register) public API, including legal name, MBS and OIB identifiers, legal form, status, founding date, registered seat and share capital."
resource: "https://sudreg.pravosudje.hr"
tags: ["cdd", "national-register", "HR-OpenData", "commercial-yes"]
timestamp: "2026-06-23"
source_id: "sudreg_croatia"
license: "HR-OpenData"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Croatian company data from the Sudski registar (Court Register) public API, including legal name, MBS and OIB identifiers, legal form, status, founding date, registered seat and share capital. Official national company / beneficial-ownership register.

- **Source id:** `sudreg_croatia`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `hr_mbs`

# Licensing

- **Licence:** `HR-OpenData` — HR-OpenData
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Sudski registar (Court Register), Ministry of Justice and Public Administration of the Republic of Croatia, published as open data via data.gov.hr.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_sudreg_croatia`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://sudreg.pravosudje.hr
