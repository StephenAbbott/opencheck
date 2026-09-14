---
type: "Data Source"
title: "ANAF \u2014 Agen\u021bia Na\u021bional\u0103 de Administrare Fiscal\u0103 (Romania)"
description: "Romanian taxpayer register, queried live by fiscal code (CUI) through ANAF's keyless public web service: registered name, fiscal domicile and registered office, trade-register number, legal form, activity code, registration and de-registration dates, VAT registration history, and the inactive-taxpayer and RO e-Factura registers."
resource: "https://www.anaf.ro/anaf/internet/ANAF/servicii_online/servicii_web_anaf/"
tags: ["cdd", "national-register", "No stated licence \u2014 public register, attribution requested", "commercial-conditional"]
timestamp: "2026-09-14"
source_id: "anaf_romania"
license: "No stated licence \u2014 public register, attribution requested"
commercial_use: "conditional"
category: "cdd"
national_register: true
---

# Overview

Romanian taxpayer register, queried live by fiscal code (CUI) through ANAF's keyless public web service: registered name, fiscal domicile and registered office, trade-register number, legal form, activity code, registration and de-registration dates, VAT registration history, and the inactive-taxpayer and RO e-Factura registers. Official national company / beneficial-ownership register.

- **Source id:** `anaf_romania`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `ro_fiscal_or_reg_id`

# Licensing

- **Licence:** `No stated licence — public register, attribution requested` — No stated licence — public register, attribution requested
- **Commercial use:** conditional · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains information from the taxpayer registers published by the Agenția Națională de Administrare Fiscală (ANAF) through its public web service at webservicesp.anaf.ro. ANAF states no licence for this data; OpenCheck republishes it as public register fact with attribution and does not redistribute it in bulk. OpenCheck is not accredited by ANAF or by the Romanian Ministry of Finance.
- Bespoke or unrecognised licence — verify terms before re-use.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_anaf_romania`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.anaf.ro/anaf/internet/ANAF/servicii_online/servicii_web_anaf/
