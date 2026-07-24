---
type: "Data Source"
title: "KRS \u2014 Polish National Court Register"
description: "Entity data from Poland's National Court Register (Krajowy Rejestr S\u0105dowy / KRS), maintained by the Ministry of Justice. Provides company name, NIP, REGON, legal form, registered address, share capital, board composition, and primary business activity (PKD code). Note: personal data (names, PESEL) is masked in the public API extract."
resource: "https://ekrs.ms.gov.pl/"
tags: ["cdd", "national-register", "PL-OGD", "commercial-yes"]
timestamp: "2026-07-24"
source_id: "krs_poland"
license: "PL-OGD"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Entity data from Poland's National Court Register (Krajowy Rejestr Sądowy / KRS), maintained by the Ministry of Justice. Provides company name, NIP, REGON, legal form, registered address, share capital, board composition, and primary business activity (PKD code). Note: personal data (names, PESEL) is masked in the public API extract. Official national company / beneficial-ownership register.

- **Source id:** `krs_poland`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `pl_krs`

# Licensing

- **Licence:** `PL-OGD` — PL-OGD
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the National Court Register (KRS), Polish Ministry of Justice (Ministerstwo Sprawiedliwości). Source: api-krs.ms.gov.pl.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_krs_poland`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://ekrs.ms.gov.pl/
