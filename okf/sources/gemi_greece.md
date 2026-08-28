---
type: "Data Source"
title: "\u0393\u0395\u039c\u0397 \u2014 Greek General Commercial Registry"
description: "Greek company data from the General Commercial Registry (\u0393\u0395\u039c\u0397) Open Data API (ODC-BY-1.0). Provides entity details \u2014 name, \u0391\u03a6\u039c, legal form, status, registered office and incorporation date \u2014 together with board members and, for private companies and partnerships, partners with their percentage holdings."
resource: "https://www.businessregistry.gr/"
tags: ["cdd", "national-register", "ODC-BY-1.0", "commercial-yes"]
timestamp: "2026-08-28"
source_id: "gemi_greece"
license: "ODC-BY-1.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Greek company data from the General Commercial Registry (ΓΕΜΗ) Open Data API (ODC-BY-1.0). Provides entity details — name, ΑΦΜ, legal form, status, registered office and incorporation date — together with board members and, for private companies and partnerships, partners with their percentage holdings. Official national company / beneficial-ownership register.

- **Source id:** `gemi_greece`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `gr_argemi`

# Licensing

- **Licence:** `ODC-BY-1.0` — ODC-BY-1.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Greek General Commercial Registry (ΓΕΜΗ), published by the Κεντρική Υπηρεσία ΓΕΜΗ / Κεντρική Ένωση Επιμελητηρίων Ελλάδος under ODC-BY-1.0 via opendata.businessportal.gr.
- Open licence; commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_gemi_greece`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.businessregistry.gr/
