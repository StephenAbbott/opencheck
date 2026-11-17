---
title: SEC EDGAR names the cause when a request times out.
readme: An SEC EDGAR timeout now says it was a timeout instead of an empty error.
tests: backend +1
---
The scheduled source-health sweep of 16 Nov 2026 (run 43916779139) went red on two upstream outages: RPO Slovakia answered 503 and SEC EDGAR did not answer within the read timeout. The same EDGAR outage reached production that afternoon (the BODS quality sweep's `deepen_failed` on `sec_edgar`, and the findings regression's lost `NON_EU_JURISDICTION` for Eli Lilly). Both registers answered normally the next morning, from the cloud and from production. No adapter was wrong, but the EDGAR error read "SEC EDGAR request failed:" with nothing after it, because an httpx timeout carries an empty message. `_get_text` now words the cause with `secret_scrub.describe_exception` ("ReadTimeout: …"), which also scrubs the request URL as CLAUDE.md asks. Timeouts are still not retried: a retry would run past the lookup's 30-second source budget.
