---
type: "Data Source"
title: "National Tax Agency \u2014 Corporate Number (Japan)"
description: "Japan's register of corporate numbers (\u6cd5\u4eba\u756a\u53f7), published by the National Tax Agency and read from a monthly index of its nationwide file: the name as registered, the kind of body, the head-office address, the date and cause of a closed record and a merger's successor, the latest change, and an English name where the body registered one. No officers, shareholders or beneficial owners are published."
resource: "https://www.houjin-bangou.nta.go.jp/"
tags: ["cdd", "national-register", "PDL-1.0", "commercial-yes"]
timestamp: "2026-11-16"
source_id: "nta_japan"
license: "PDL-1.0"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

Japan's register of corporate numbers (法人番号), published by the National Tax Agency and read from a monthly index of its nationwide file: the name as registered, the kind of body, the head-office address, the date and cause of a closed record and a merger's successor, the latest change, and an English name where the body registered one. No officers, shareholders or beneficial owners are published. Official national company / beneficial-ownership register.

- **Source id:** `nta_japan`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** no
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `jp_jcn`

# Licensing

- **Licence:** `PDL-1.0` — Public Data License, version 1.0 (Japan, Digital Agency)
- **Commercial use:** yes · **Attribution:** required · **Share-alike:** no
- **Attribution line:** 出典：国税庁法人番号公表サイト（国税庁）を加工して作成 (https://www.houjin-bangou.nta.go.jp/). Contains information from the National Tax Agency's Corporate Number Publication Site, processed by OpenCheck, under the Public Data License (Version 1.0) (https://www.digital.go.jp/resources/open_data/public_data_license_v1.0).
- The Japanese government's standard terms for open data, compatible with CC BY 4.0: use, copy, adapt and redistribute for any purpose, commercial included, naming the source and saying when the data was edited. The National Tax Agency's corporate number data is published under it.

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_nta_japan`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://www.houjin-bangou.nta.go.jp/
