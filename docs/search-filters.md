# Search filters

Company-name search on opencheck.world, `GET /search`, `GET /stream` and the
MCP tool `opencheck_search` take optional filters (the advanced-search-filters
ticket, decisions of 3 November 2026).

| Filter | Values | Applied by |
|---|---|---|
| `jurisdiction` | GLEIF's accepted jurisdiction codes — ISO 3166-1 (`DE`) or a sub-division (`US-DE`) | GLEIF; a national register covering that jurisdiction runs as before |
| `legal_form` | ISO 20275 ELF codes (`2HBR`), comma-separated, or `no-elf-code` (8888 and 9999) | GLEIF |
| `entity_status` | `ACTIVE`, `INACTIVE` — the company's status as GLEIF records it | GLEIF |
| `registration_status` | `ISSUED`, `LAPSED`, `RETIRED`, `MERGED`, `ANNULLED`, `DUPLICATE`, `PENDING_TRANSFER`, `PENDING_ARCHIVAL` — the LEI record's status, not the company's | GLEIF |
| `category` | `GENERAL`, `BRANCH`, `FUND`, `SOLE_PROPRIETOR`, `RESIDENT_GOVERNMENT_ENTITY`, `INTERNATIONAL_ORGANIZATION` | GLEIF |
| `nationality` (people) | ISO 3166-1 alpha-2 | OpenSanctions, Wikidata |
| `birth_year` (people) | a year | OpenSanctions, Wikidata |

## A source that cannot apply a filter is left out, and named

With a filter set, a source runs only if it can apply every active filter.
The others are not searched — their unfiltered rows would read as filtered
matches — and the response says so:

```json
"filters": {
  "applied": {"jurisdiction": "DE", "legal_form": ["2HBR"]},
  "applied_by": ["gleif"],
  "left_out": [
    {"source_id": "companies_house", "reason": "outside_jurisdiction", "filters": ["jurisdiction"]},
    {"source_id": "opencorporates", "reason": "cannot_apply", "filters": ["jurisdiction", "legal_form"]}
  ]
}
```

A national register asked only for its own jurisdiction runs as before: every
Companies House record is `GB`. Person filters are applied to each source's own
data — a record with no birth date or no country is not a match. Wikidata's
check is a separate query; if it does not answer, Wikidata reports an error
rather than returning unchecked people.

## The jurisdiction filter is exact

GLEIF's `filter[entity.jurisdiction]` matches each part of a code separately,
so a country whose two letters are also a US state, Canadian province or UAE
emirate picks that sub-division up. Measured on 3 November 2026:

| Filter | GLEIF returns | Of which |
|---|---|---|
| `DE` (Germany) | 405,016 | 145,738 Delaware |
| `PA` (Panama) | 15,625 | 9,291 Pennsylvania |
| `IL` (Israel) | 10,768 | 8,305 Illinois |
| `KY` (Cayman Islands) | 49,675 | 1,490 Kentucky |
| `SH` (Saint Helena) | 165 | 162 Sharjah |

`backend/opencheck/data/gleif_jurisdictions.json` lists the 31 clashing codes,
derived from GLEIF's accepted jurisdictions by
`backend/scripts/build_search_filter_data.py`. For those codes the search
reads 200 records a page, up to five pages, keeps exact rows, and computes the
true total as GLEIF's count minus each clashing sub-division's under the same
filters. A scan that stops before it has ten rows says how far it got and can
continue. A country-specific legal form makes the correction unnecessary; the
8888/9999 placeholders do not. Quoting the value, a `!US-PA` exclusion in the
same filter and repeating the filter all fail at GLEIF.

## Legal forms

GLEIF's parameter is `filter[entity.legalForm]`; `filter[entity.legalForm.id]`
is refused with HTTP 400. The ISO 20275 list has 3,446 active forms (729 of
them US, state by state), so the website asks for a jurisdiction first and
`GET /elf-forms?jurisdiction=DE` lists that jurisdiction's forms — 31 for
Germany, 33 for the UK — most used first by issued LEIs in the Golden Copy
mirror. Abbreviations ("GmbH", "e.V.") are in GLEIF's downloadable code list
only, not its API; `elf_forms.json` is built from that list, and a new version
is a re-run of the build script with `--elf-url`.

The abbreviation shown is the first that starts with the form's displayed
name's first letter (else the shortest), after dropping a parenthesised tail —
`8Z6G` is "KG" though its cell also lists "GmbH & Co. KG"; the UK's Private
Limited Company is "Ltd", not the Welsh "cyf". Every abbreviation and name is
searchable, so typing "GmbH" also offers Germany's KG and UG.
