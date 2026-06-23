---
type: "Data Source"
title: "Firmenbuch \u2014 Austrian Commercial Register"
description: "Austrian company name, address, status, and officers (managing directors, authorised signatories, supervisory board) from the Firmenbuch (commercial register), via the BMJ High Value Dataset API. Shareholder data requires a paid subscription."
resource: "https://justizonline.gv.at/jop/web/firmenbuchabfrage"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-06-23"
source_id: "firmenbuch"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Austrian company name, address, status, and officers (managing directors, authorised signatories, supervisory board) from the Firmenbuch (commercial register), via the BMJ High Value Dataset API. Shareholder data requires a paid subscription. Official national company / beneficial-ownership register.

- **Source id:** `firmenbuch`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `at_fn`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Austrian Firmenbuch via the BMJ HVD API (CC BY 4.0), © Bundesministerium für Justiz.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_firmenbuch`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://justizonline.gv.at/jop/web/firmenbuchabfrage
