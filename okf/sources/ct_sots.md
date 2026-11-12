---
type: "Data Source"
title: "Connecticut Secretary of the State \u2014 Business Registry"
description: "Connecticut business records from the Secretary of the State's registry on data.ct.gov, looked up by business ID: name, type, status, formation, recorded name changes, and the principals \u2014 officers and directors of a corporation, members or managers of an LLC, partners of an LLP \u2014 by name and role."
resource: "https://portal.ct.gov/sots/business-services/bulk-data-and-images"
tags: ["cdd", "national-register", "CT-Open-Data", "commercial-yes"]
timestamp: "2026-11-12"
source_id: "ct_sots"
license: "CT-Open-Data"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Connecticut business records from the Secretary of the State's registry on data.ct.gov, looked up by business ID: name, type, status, formation, recorded name changes, and the principals — officers and directors of a corporation, members or managers of an LLC, partners of an LLP — by name and role. Official national company / beneficial-ownership register.

- **Source id:** `ct_sots`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `us_ct_business_id`

# Licensing

- **Licence:** `CT-Open-Data` — Connecticut open data (data.ct.gov; Public Domain for Business Master)
- **Commercial use:** yes · **Attribution:** not required · **Share-alike:** no
- **Attribution line:** Connecticut Business Registry — Business Master, Principals and Name Change History datasets — Connecticut Secretary of the State, via data.ct.gov.
- data.ct.gov marks the Business Master dataset Public Domain. The Principals and Name Change History datasets, from the same publisher on the same portal, carry no licence field; the Secretary of the State lists all five registry datasets as free bulk data, and Connecticut's Freedom of Information Act makes the records public (Conn. Gen. Stat. § 1-210). data.ct.gov's terms are disclaimers only. OpenCheck attributes the Secretary of the State anyway (recorded 12 Nov 2026).

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_ct_sots`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://portal.ct.gov/sots/business-services/bulk-data-and-images
