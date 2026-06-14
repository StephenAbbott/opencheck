---
type: "Data Source"
title: "INPI \u2014 Registre National des Entreprises"
description: "French company data from the Registre National des Entreprises (RNE), operated by INPI, sourced via the SIREN number."
resource: "https://registre-national-entreprises.inpi.fr/"
tags: ["cdd", "national-register", "Licence Ouverte / Open Licence 2.0", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "inpi"
license: "Licence Ouverte / Open Licence 2.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

French company data from the Registre National des Entreprises (RNE), operated by INPI, sourced via the SIREN number. Official national company / beneficial-ownership register.

- **Source id:** `inpi`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `siren`

# Licensing

- **Licence:** `Licence Ouverte / Open Licence 2.0` — Licence Ouverte / Open Licence 2.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Registre National des Entreprises (RNE), INPI — Licence Ouverte / Open Licence 2.0.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_inpi`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://registre-national-entreprises.inpi.fr/
