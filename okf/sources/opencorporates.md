---
type: "Data Source"
title: "OpenCorporates"
description: "The world's largest open legal-entity database, providing a single unified set of company records from government registries."
resource: "https://opencorporates.com/"
tags: ["cdd", "aggregator", "OC-Terms", "commercial-conditional"]
timestamp: "2026-06-14"
source_id: "opencorporates"
license: "OC-Terms"
commercial_use: "conditional"
category: "cdd"
national_register: false
---

# Overview

The world's largest open legal-entity database, providing a single unified set of company records from government registries. Aggregator, cross-border database or ESG source.

- **Source id:** `opencorporates`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** no


# Licensing

- **Licence:** `OC-Terms` — OpenCorporates Terms & Conditions
- **Commercial use:** conditional · **Attribution:** required · **Share-alike:** yes
- **Attribution line:** Contains company data from OpenCorporates (https://opencorporates.com/). Licensed per-jurisdiction under the source government registry license.
- Bespoke terms; share-alike and bulk-redistribution restrictions — verify before re-use.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

> **Raw data:** OpenCheck does not redistribute this source's raw records (licence permits derived output only). Only the mapped BODS statements are served; the raw payload is redacted from API responses and exports.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_opencorporates`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://opencorporates.com/
