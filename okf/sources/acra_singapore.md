---
type: "Data Source"
title: "Singapore ACRA \u2014 Accounting and Corporate Regulatory Authority"
description: "Singapore company data from ACRA's registers on data.gov.sg, looked up by Unique Entity Number: name and former names, entity and company type, status, incorporation date and registered address."
resource: "https://data.gov.sg/collections/2/view"
tags: ["cdd", "national-register", "Singapore-OGL-1.0", "commercial-yes"]
timestamp: "2026-09-10"
source_id: "acra_singapore"
license: "Singapore-OGL-1.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Singapore company data from ACRA's registers on data.gov.sg, looked up by Unique Entity Number: name and former names, entity and company type, status, incorporation date and registered address. Official national company / beneficial-ownership register.

- **Source id:** `acra_singapore`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `sg_uen`

# Licensing

- **Licence:** `Singapore-OGL-1.0` — Singapore Open Data Licence v1.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains information from ACRA Information on Corporate Entities and Entities Registered with ACRA accessed from data.gov.sg, which is made available under the terms of the Singapore Open Data Licence version 1.0 https://data.gov.sg/open-data-licence (the date of access is each record's retrieval date).
- Commercial use permitted with the prescribed attribution statement. Grants no rights over personal data, and none to downstream sub-licensees, who take their own licence from data.gov.sg.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_acra_singapore`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://data.gov.sg/collections/2/view
