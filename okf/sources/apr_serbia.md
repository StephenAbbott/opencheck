---
type: "Data Source"
title: "APR \u2014 Business Registers Agency company register (Serbia)"
description: "Serbia's register of companies (\u0420\u0435\u0433\u0438\u0441\u0442\u0430\u0440 \u043f\u0440\u0438\u0432\u0440\u0435\u0434\u043d\u0438\u0445 \u0434\u0440\u0443\u0448\u0442\u0430\u0432\u0430), published monthly by the Agencija za privredne registre (Business Registers Agency) through its open-data API. Business name, registration number (mati\u010dni broj), legal form, status \u2014 active, in liquidation, in bankruptcy or in forced liquidation \u2014 founding date, municipality and activity code. No addresses and no people: no directors, shareholders or beneficial owners. Deleted companies and entrepreneurs are not published."
resource: "https://www.apr.gov.rs/"
tags: ["cdd", "national-register", "SODL-1.0", "commercial-yes"]
timestamp: "2026-09-17"
source_id: "apr_serbia"
license: "SODL-1.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Serbia's register of companies (Регистар привредних друштава), published monthly by the Agencija za privredne registre (Business Registers Agency) through its open-data API. Business name, registration number (matični broj), legal form, status — active, in liquidation, in bankruptcy or in forced liquidation — founding date, municipality and activity code. No addresses and no people: no directors, shareholders or beneficial owners. Deleted companies and entrepreneurs are not published. Official national company / beneficial-ownership register.

- **Source id:** `apr_serbia`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `rs_mb`

# Licensing

- **Licence:** `SODL-1.0` — Data License — Serbian Open Data Portal (SODL 1.0)
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains data from the Register of Business Entities published by the Agencija za privredne registre (Serbian Business Registers Agency) at https://openapi.apr.gov.rs/api/opendata/companies, under the Serbian Open Data Portal licence (SODL 1.0). Downloaded on the cut date shown on each record; OpenCheck adds a Serbian Latin transliteration of Cyrillic names and maps the register's status to a liveness class.
- Reuse by anyone for commercial and non-commercial purposes, including copying, distribution and merging with other data, free of charge. Attribution names the publishing public body, the download date and address, and any changes made.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_apr_serbia`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.apr.gov.rs/
