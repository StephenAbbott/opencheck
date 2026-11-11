---
type: "Data Source"
title: "Argentina \u2014 Registro Nacional de Sociedades and IGJ"
description: "Argentina's national register of legal persons (Registro Nacional de Sociedades) and the companies registered by the Inspecci\u00f3n General de Justicia in the City of Buenos Aires, published as open data by the Ministry of Justice and read from an index of both: name, CUIT, legal form, founding date, addresses and main activity; for an IGJ company its deregistration and its reason, balance sheets and assemblies filed. No shareholders or beneficial owners are published; the people named in IGJ filings are counted, never listed."
resource: "https://datos.jus.gob.ar/"
tags: ["cdd", "national-register", "CC-BY-4.0", "commercial-yes"]
timestamp: "2026-11-11"
source_id: "registro_sociedades_argentina"
license: "CC-BY-4.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Argentina's national register of legal persons (Registro Nacional de Sociedades) and the companies registered by the Inspección General de Justicia in the City of Buenos Aires, published as open data by the Ministry of Justice and read from an index of both: name, CUIT, legal form, founding date, addresses and main activity; for an IGJ company its deregistration and its reason, balance sheets and assemblies filed. No shareholders or beneficial owners are published; the people named in IGJ filings are counted, never listed. Official national company / beneficial-ownership register.

- **Source id:** `registro_sociedades_argentina`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `ar_cuit`

# Licensing

- **Licence:** `CC-BY-4.0` — Creative Commons Attribution 4.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains information from the Registro Nacional de Sociedades and the Inspección General de Justicia, Ministerio de Justicia de la Nación (datos.jus.gob.ar), CC BY 4.0.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_registro_sociedades_argentina`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://datos.jus.gob.ar/
