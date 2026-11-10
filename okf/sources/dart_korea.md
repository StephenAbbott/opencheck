---
type: "Data Source"
title: "Open DART \u2014 Korean disclosure filings (FSS)"
description: "South Korea's DART disclosure system, run by the Financial Supervisory Service, read through the Open DART API: a filer's profile and registration numbers, the largest shareholder and its related persons and the executives from its latest periodic report, and its 5% holding reports. Covers listed companies and other disclosure filers only. Korea has no public beneficial-ownership register."
resource: "https://opendart.fss.or.kr/"
tags: ["cdd", "national-register", "KR-Public-Data", "commercial-yes"]
timestamp: "2026-11-10"
source_id: "dart_korea"
license: "KR-Public-Data"
commercial_use: "yes"
category: "cdd"
national_register: true
---

# Overview

South Korea's DART disclosure system, run by the Financial Supervisory Service, read through the Open DART API: a filer's profile and registration numbers, the largest shareholder and its related persons and the executives from its latest periodic report, and its 5% holding reports. Covers listed companies and other disclosure filers only. Korea has no public beneficial-ownership register. Official national company / beneficial-ownership register.

- **Source id:** `dart_korea`
- **Category:** cdd (customer due diligence / compliance)
- **Search kinds:** entity
- **Requires API key:** yes
- **National register:** yes
- **Lookup keys (LEI-anchored dispatch):** `kr_bizr_no`, `kr_jurir_no`

# Licensing

- **Licence:** `KR-Public-Data` — Korean public data, no restriction on the scope of use (공공데이터법; Open DART terms)
- **Commercial use:** yes · **Attribution:** not required · **Share-alike:** no
- **Attribution line:** Source: Financial Supervisory Service (금융감독원), Open DART (opendart.fss.or.kr). Public data, no restriction on the scope of use (data.go.kr).
- Open DART's terms (art. 16④) defer to the Public Data Act, and data.go.kr lists the FSS's DART APIs with “이용허락범위 제한 없음” — no restriction on the scope of use, commercial use included. The filings are the filers' responsibility (art. 23①); OpenCheck attributes the Financial Supervisory Service anyway (read 10 Nov 2026).

See the [licensing compatibility matrix](/licensing/matrix.md) for how this licence combines with others at export time.

# BODS mapping

Records from this source are mapped to [Beneficial Ownership Data Standard (BODS) v0.4](/standards/bods.md)
statements by OpenCheck's mapper (`opencheck.bods.map_dart_korea`). Cross-source
identifiers (LEI, national company numbers, Wikidata QIDs) are used to reconcile
this source with others.

# Citations

- https://opendart.fss.or.kr/
