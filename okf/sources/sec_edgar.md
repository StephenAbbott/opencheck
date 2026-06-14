---
type: "Data Source"
title: "SEC EDGAR (Schedule 13D/13G)"
description: "Major shareholders (>5 % beneficial owners) of US-listed companies from mandatory SEC Schedule 13D and 13G filings. Coverage is limited to XML filings submitted from December 2024 onward."
resource: "https://www.sec.gov/search-filings"
tags: ["cdd", "aggregator", "Public Domain", "commercial-yes"]
timestamp: "2026-06-14"
source_id: "sec_edgar"
license: "Public Domain"
commercial_use: "yes"
category: "cdd"
national_register: false
---

# Overview

Major shareholders (>5 % beneficial owners) of US-listed companies from mandatory SEC Schedule 13D and 13G filings. Coverage is limited to XML filings submitted from December 2024 onward. Aggregator, cross-border database or ESG source.

- **Source id:** `sec_edgar`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** no


# Licensing

- **Licence:** `Public Domain` — Public domain
- **Commercial use:** yes · **Attribution:** not required · **Share-alike:** no
- **Attribution line:** SEC EDGAR — public domain, courtesy of the U.S. Securities and Exchange Commission.
- No copyright; free for any use including commercial.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_sec_edgar`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.sec.gov/search-filings
