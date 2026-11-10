---
title: A Korean company's stock-market listing is no longer shown as its legal form — Open DART publishes a market, not a legal form, so the listing moves to a note on the company's statement and GLEIF's legal form is read.
readme: A Korean company's stock-market listing from Open DART is no longer shown as its legal form.
tests: backend +1
---
Found on the first production lookup of Samsung Electronics (`9884007ER46L6N7EI764`) after `dart_korea` shipped: the subject profile's legal form read "listed on KOSPI (stock code 005930)" ahead of GLEIF's JuSikHoeSa, because `map_dart_korea` wrote the listing (`corp_cls` + `stock_code`) into `entityType.details`, which `subject_profile._legal_form` reads as the legal form. Open DART's `company.json` carries no legal form, so the DART entity statement now leaves `entityType.details` empty and says the listing in a `commenting` annotation on `/recordDetails` ("Open DART records this filer as listed on KOSPI (stock code 005930). Open DART publishes no legal form."). The finding sentence reads the listing from the bundle and is unchanged. After deploy: Samsung's profile shows GLEIF's legal form.
