---
type: "Data Source"
title: "Florida Division of Corporations (Sunbiz)"
description: "Florida's register of corporations, LLCs, limited partnerships and foreign qualifications, read from a quarterly index of the Division of Corporations' bulk data: name, status with the dated event behind it, filing type, file date, FEI (EIN) number, principal address, registered agent, annual reports, former names and up to six officers. No shareholders or beneficial owners are published."
resource: "https://dos.fl.gov/sunbiz/"
tags: ["cdd", "national-register", "FL-Public-Records", "commercial-yes"]
timestamp: "2026-10-30"
source_id: "sunbiz_florida"
license: "FL-Public-Records"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Florida's register of corporations, LLCs, limited partnerships and foreign qualifications, read from a quarterly index of the Division of Corporations' bulk data: name, status with the dated event behind it, filing type, file date, FEI (EIN) number, principal address, registered agent, annual reports, former names and up to six officers. No shareholders or beneficial owners are published. Official national company / beneficial-ownership register.

- **Source id:** `sunbiz_florida`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `us_fl_doc`

# Licensing

- **Licence:** `FL-Public-Records` — Florida public records (Fla. Const. art. I §24; Fla. Stat. ch. 119)
- **Commercial use:** yes · **Attribution:** not required · **Share-alike:** no
- **Attribution line:** Contains information from the Florida Department of State, Division of Corporations (sunbiz.org) bulk data downloads — Florida public records.
- No licence is stated: the Division of Corporations offers its bulk data free “for informational purposes”, as is. Florida public records are open to anyone for any purpose, and an agency cannot assert copyright in them without statutory authority (Microdecisions, Inc. v. Skinner, 889 So.2d 871, Fla. 2d DCA 2004). OpenCheck attributes the Division anyway (recorded 29 Oct 2026).

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_sunbiz_florida`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://dos.fl.gov/sunbiz/
