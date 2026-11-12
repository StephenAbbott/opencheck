---
type: "Data Source"
title: "Find a Tender (UK public procurement)"
description: "The UK government's procurement notice service, published in the Open Contracting Data Standard and collected weekly by the Open Contracting Partnership's Data Registry. For a company: the public contracts awarded to it since 2021, with buyer, value and a link to each notice. Matched on the Companies House number (or a PPON the notices pair with one). Procurement activity, not ownership."
resource: "https://www.find-tender.service.gov.uk/"
tags: ["cdd", "aggregator", "OGL-3.0", "commercial-yes"]
timestamp: "2026-11-12"
source_id: "find_tender_uk"
license: "OGL-3.0"
commercial_use: "yes"
category: "cdd"
national_register: false
---

# Overview

The UK government's procurement notice service, published in the Open Contracting Data Standard and collected weekly by the Open Contracting Partnership's Data Registry. For a company: the public contracts awarded to it since 2021, with buyer, value and a link to each notice. Matched on the Companies House number (or a PPON the notices pair with one). Procurement activity, not ownership. Aggregator, cross-border database or ESG source.

- **Source id:** `find_tender_uk`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** no


# Licensing

- **Licence:** `OGL-3.0` — UK Open Government Licence v3.0
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** Contains public sector information licensed under the Open Government Licence v3.0: Find a Tender service notices (Cabinet Office), as collected by the Open Contracting Partnership's Data Registry.
- Commercial use permitted with attribution.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_find_tender_uk`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.find-tender.service.gov.uk/
