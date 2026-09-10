/**
 * The History tab's values layer (Phase 190).
 *
 * These claims moved here from `components/cdd/HistoryTimeline.test.ts` when
 * the pure helpers moved out of the component: the frontend's node tier is
 * where a sentence, an order or a link is pinned, and every one of these is
 * one of those. What the markup does with them belongs to
 * `HistoryPanel.test.tsx`.
 */
import { describe, it, expect } from "vitest";

import type { HistoryRawChange, HistoryResponse } from "./api";
import {
  basisLabel,
  boardChangesOf,
  boardChangesSummary,
  boardLinkSummary,
  boardRowPersonHref,
  boardRowPersonId,
  boardUncheckedNotice,
  buildTimelineRows,
  corroboratedCount,
  datedSpan,
  historyDegradedNotice,
  historySentence,
  historySourceLabel,
  filingsTruncatedNotice,
  noiseEventsOf,
  recordUrl,
  silentRegisters,
  VISIBLE_ROWS,
} from "./historyMode";

const _LEI = "213800IN6LSRGTZSOS29";

const RESP: HistoryResponse = {
  lei: _LEI,
  company_number: "00358949",
  available: true,
  gleif_record_available: true,
  gleif_events_available: true,
  registry_sources_blocked: false,
  company_number_basis: "live",
  registry_numbers: { companies_house: "00358949" },
  sources: ["gleif", "companies_house"],
  notable_count: 3,
  notable: [
    {
      change_type: "LEGAL_FORM_CHANGE", label: "Legal form changed", tier: 2,
      record_type: "entity", date: "2022-01-11", date_basis: "effective",
      date_confidence: "high", value_old: "B6ES", value_new: "H0PO",
      sources: ["companies_house", "gleif"], corroborating_sources: ["gleif"],
      counterparty: null, interest_start_date: null, interest_end_date: null,
      boosted: false,
    },
    {
      change_type: "LEGAL_NAME_CHANGE", label: "Legal name changed", tier: 2,
      record_type: "entity", date: "2021-12-01", date_basis: "effective",
      date_confidence: "high", value_old: "WM MORRISON SUPERMARKETS P L C",
      value_new: "WM MORRISON SUPERMARKETS LIMITED",
      sources: ["companies_house", "gleif"], corroborating_sources: ["gleif"],
      counterparty: null, interest_start_date: null, interest_end_date: null,
      boosted: false,
    },
    {
      change_type: "OWNER_ADDED", label: "Owner / parent added", tier: 1,
      record_type: "relationship", date: "2023-11-25", date_basis: "recorded",
      date_confidence: "medium", value_old: null,
      value_new: "IS_DIRECTLY_CONSOLIDATED_BY", sources: ["gleif"],
      corroborating_sources: [], counterparty: "549300RKU7UEPSC42U63",
      interest_start_date: "2021-11-01", interest_end_date: null, boosted: false,
    },
  ],
  events: [
    {
      source_id: "gleif", record_type: "entity", raw_change_type: "UPDATE",
      raw_field: "/lei:.../lei:Registration/lei:NextRenewalDate",
      value_old: "2026-01-11", value_new: "2027-01-11", change_type: null,
      label: null, counterparty: null,
      tier: 3, event_date: "2025-11-20", date_basis: "recorded",
    },
    {
      source_id: "companies_house", record_type: "entity", raw_change_type: "CS01",
      raw_field: "confirmation-statement", value_old: null, value_new: null,
      change_type: null, label: null, counterparty: null,
      tier: 3, event_date: "2022-03-01", date_basis: "effective",
    },
    // A notable (tier-2) raw event — must NOT be treated as noise.
    {
      source_id: "gleif", record_type: "entity", raw_change_type: "UPDATE",
      raw_field: "/lei:.../lei:Entity/lei:LegalName", value_old: "x", value_new: "y",
      change_type: "LEGAL_NAME_CHANGE", label: "Legal name changed",
      counterparty: null, tier: 2, event_date: "2021-12-09",
      date_basis: "recorded",
    },
  ],
};

describe("noiseEventsOf", () => {
  it("keeps only Tier-3 events", () => {
    const noise = noiseEventsOf(RESP);
    expect(noise).toHaveLength(2);
    expect(noise.every((e: { tier: number }) => e.tier === 3)).toBe(true);
    // The tier-2 LegalName raw event is excluded.
    expect(noise.some((e: { change_type: string | null }) => e.change_type === "LEGAL_NAME_CHANGE")).toBe(false);
  });
});

describe("buildTimelineRows", () => {
  it("shows only notable rows by default, newest first", () => {
    const rows = buildTimelineRows(RESP, false);
    expect(rows).toHaveLength(3);
    expect(rows.every((r) => r.kind === "notable")).toBe(true);
    expect(rows.map((r) => r.date)).toEqual([
      "2023-11-25", "2022-01-11", "2021-12-01",
    ]);
  });

  it("interleaves noise rows by date when toggled on (newest first)", () => {
    const rows = buildTimelineRows(RESP, true);
    expect(rows).toHaveLength(5); // 3 notable + 2 noise
    expect(rows.map((r) => r.date)).toEqual([
      "2025-11-20", "2023-11-25", "2022-03-01", "2022-01-11", "2021-12-01",
    ]);
    // The 2025-11-20 and 2022-03-01 rows are the noise ones.
    const noiseRows = rows.filter((r) => r.kind === "noise");
    expect(noiseRows.map((r) => r.date)).toEqual(["2025-11-20", "2022-03-01"]);
  });
});

describe("recordUrl", () => {
  it("links GLEIF to the LEI record, with no registry number needed", () => {
    expect(recordUrl("gleif", _LEI)).toBe(`https://search.gleif.org/#/record/${_LEI}`);
  });

  it("links each national register by its own number", () => {
    // Phase 190: before `registry_numbers` only these first two could be
    // linked, and a New Zealand, Estonian or Danish row was shown with no way
    // back to the record that published it.
    // Real numbers from production, so a pattern that only works for a
    // made-up identifier cannot pass here: Fonterra Commodities (NZ),
    // Eesti Energia (EE), Novo Nordisk (DK). Every one of these URLs was
    // opened against the live register.
    const n = {
      companies_house: "00358949",
      nz_companies: "2288120",
      ariregister: "10421629",
      cvr_denmark: "24256790",
    };
    expect(recordUrl("companies_house", _LEI, n)).toBe(
      "https://find-and-update.company-information.service.gov.uk/company/00358949/filing-history",
    );
    // The company record itself, never the register's client-side search
    // page: that returns HTTP 200 with an empty form, so a wrong link here
    // fails open rather than visibly.
    expect(recordUrl("nz_companies", _LEI, n)).toBe(
      "https://app.companiesoffice.govt.nz/companies/app/ui/pages/companies/2288120",
    );
    expect(recordUrl("nz_companies", _LEI, n)).not.toContain("search");
    expect(recordUrl("ariregister", _LEI, n)).toBe(
      "https://ariregister.rik.ee/eng/company/10421629",
    );
    expect(recordUrl("cvr_denmark", _LEI, n)).toBe(
      "https://datacvr.virk.dk/enhed/virksomhed/24256790",
    );
  });

  it("returns null rather than a broken link when the number is absent", () => {
    for (const id of ["companies_house", "nz_companies", "ariregister", "cvr_denmark"]) {
      expect(recordUrl(id, _LEI, {})).toBeNull();
    }
    expect(recordUrl("some_future_source", _LEI, { some_future_source: "1" })).toBeNull();
  });
});

describe("historySourceLabel", () => {
  it("names every register that emits history, Denmark included", () => {
    // cvr_denmark was missing from the label map until Phase 190, so a Danish
    // row chipped as the raw slug.
    expect(historySourceLabel("cvr_denmark")).toBe("CVR (DK)");
    expect(historySourceLabel("gleif")).toBe("GLEIF");
    expect(historySourceLabel("companies_house")).toBe("Companies House");
    expect(historySourceLabel("nz_companies")).toBe("Companies Office (NZ)");
    expect(historySourceLabel("ariregister")).toBe("e-Äriregister (EE)");
  });

  it("falls back to the slug rather than rendering nothing", () => {
    expect(historySourceLabel("brreg")).toBe("brreg");
  });
});

describe("basisLabel", () => {
  it("labels effective vs recorded honestly", () => {
    expect(basisLabel("effective")).toBe("as filed");
    expect(basisLabel("recorded")).toBe("as recorded by GLEIF");
  });
});

describe("historyDegradedNotice (Phase 146)", () => {
  it("says nothing when both GLEIF calls answered", () => {
    expect(historyDegradedNotice(RESP)).toBeNull();
  });

  it("names the compounding failure: no record means no registry histories", () => {
    const notice = historyDegradedNotice({
      ...RESP,
      notable: [],
      events: [],
      available: false,
      gleif_record_available: false,
      gleif_events_available: false,
      registry_sources_blocked: true,
      company_number_basis: null,
      company_number: null,
    });
    expect(notice).toMatch(/could not be checked/);
    expect(notice).toMatch(/Companies House/);
    expect(notice).toMatch(/not a finding/);
  });

  it("distinguishes a missing change log from a missing record", () => {
    const notice = historyDegradedNotice({
      ...RESP,
      gleif_events_available: false,
    });
    expect(notice).toMatch(/change log/);
    // The registry sources ran, so it must not claim they were blocked.
    expect(notice).not.toMatch(/could not be attempted/);
  });

  it("says the registry number came from cache when it did", () => {
    const notice = historyDegradedNotice({
      ...RESP,
      gleif_record_available: false,
      registry_sources_blocked: false,
      company_number_basis: "cached",
    });
    expect(notice).toMatch(/cached copy/);
    expect(notice).not.toMatch(/could not be attempted/);
  });
});

describe("datedSpan and corroboratedCount", () => {
  it("spans the earliest and latest dated entry by year", () => {
    expect(datedSpan(RESP.notable)).toEqual({ from: "2021", to: "2023" });
  });

  it("has no span when nothing is dated, rather than inventing one", () => {
    expect(datedSpan(RESP.notable.map((e) => ({ ...e, date: null })))).toBeNull();
    expect(datedSpan([])).toBeNull();
  });

  it("counts only the entries a second register also recorded", () => {
    // Two of the three carry both sources; the GLEIF-only ownership change
    // does not, and must not be counted as agreed.
    expect(corroboratedCount(RESP.notable)).toBe(2);
  });
});

describe("silentRegisters", () => {
  it("names a register that holds the company but published no history", () => {
    const d: HistoryResponse = {
      ...RESP,
      sources: ["gleif"],
      registry_numbers: { companies_house: "00358949", cvr_denmark: "12345678" },
    };
    expect(silentRegisters(d)).toEqual(["companies_house", "cvr_denmark"]);
  });

  it("says nothing about a register that did answer", () => {
    expect(silentRegisters(RESP)).toEqual([]);
  });
});

describe("historySentence", () => {
  it("counts the registers, the changes and the span, and says what agreement means", () => {
    const s = historySentence(RESP, "WM Morrison Supermarkets Limited");
    expect(s).toMatch(
      /^Two registers publish a change log for WM Morrison Supermarkets Limited — 3 notable changes between 2021 and 2023/,
    );
    expect(s).toContain("2 of them are recorded by more than one register");
    // The tab never claims the history is complete.
    expect(s).not.toMatch(/complete|comprehensive|full history|guarantee/i);
  });

  it("does not compare a single register against nothing", () => {
    const s = historySentence({ ...RESP, sources: ["gleif"] }, "Acme");
    expect(s).toMatch(/^One register publishes a change log for Acme/);
    expect(s).toContain("No second register keeps a history");
  });

  it("says why two registers can disagree when none of them agrees", () => {
    const lone = { ...RESP.notable[2], sources: ["gleif"] };
    const s = historySentence({ ...RESP, notable: [lone] }, "Acme");
    expect(s).toContain("No change is recorded by more than one of them");
    expect(s).toContain("the day it noticed");
  });

  it("calls an empty timeline an absence of records, not a quiet company", () => {
    const none = historySentence({ ...RESP, sources: [], notable: [] }, "Acme");
    expect(none).toBe(
      "No register OpenCheck can ask publishes a change log for Acme. That is an absence of records, not a finding that nothing changed.",
    );
    const covered = historySentence(
      { ...RESP, sources: ["gleif", "companies_house"], notable: [] },
      "Acme",
    );
    expect(covered).toMatch(/^Two registers hold a change log for Acme, and none of them records/);
    expect(covered).toContain("not a finding that nothing changed");
    // No arithmetic on nothing — the Phase 185 lesson, applied here.
    expect(covered).not.toMatch(/0 notable|between .* and/);
    const one = historySentence({ ...RESP, sources: ["gleif"], notable: [] }, "Acme");
    expect(one).toMatch(/^One register holds a change log for Acme, and it records/);
  });

  it("keeps the singular readable for a one-change, one-year history", () => {
    const s = historySentence({ ...RESP, notable: [RESP.notable[1]] }, "Acme");
    expect(s).toContain("1 notable change in 2021");
    expect(s).toContain("1 of them is recorded by more than one register");
  });
});

describe("the row cap", () => {
  it("shows ten rows before collapsing, matching what the tab promises", () => {
    expect(VISIBLE_ROWS).toBe(10);
  });
});

// --------------------------------------------------------------------------
// Board changes — Phase 194
//
// Numbers are Lloyds Bank PLC's, measured over its full 2,404 filings: 246
// appointments and resignations between 1986 and 2026, of which 75 name the
// officer. The other 322 officer filings are particulars changes and belong
// to the administrative stream, which is what makes this one readable.
// --------------------------------------------------------------------------

function board(over: Partial<HistoryRawChange> = {}): HistoryRawChange {
  return {
    source_id: "companies_house",
    record_type: "relationship",
    raw_change_type: "AP01",
    raw_field: "officers/director",
    value_old: null,
    value_new: null,
    change_type: "OFFICER_APPOINTED",
    label: "Officer appointed",
    counterparty: "Mr Kelly Brian Bennett",
    tier: 4,
    event_date: "2026-09-01",
    date_basis: "effective",
    ...over,
  };
}

function withEvents(events: HistoryRawChange[]): HistoryResponse {
  return { ...RESP, events };
}

describe("boardChangesOf", () => {
  it("keeps only Tier-4 events", () => {
    const rows = boardChangesOf(withEvents([board(), board({ tier: 3 }), board({ tier: 1 })]));
    expect(rows).toHaveLength(1);
    expect(rows[0].tier).toBe(4);
  });

  it("is empty rather than undefined when the response carries no events", () => {
    expect(boardChangesOf({ ...RESP, events: [] })).toEqual([]);
  });
});

describe("boardChangesSummary", () => {
  it("counts the rows, spans them, and says how many name anybody", () => {
    const rows = [
      board({ event_date: "2026-09-01" }),
      board({ event_date: "1986-05-08", counterparty: null, change_type: "OFFICER_RESIGNED" }),
      board({ event_date: "1996-07-11", counterparty: null, change_type: "OFFICER_RESIGNED" }),
    ];
    expect(boardChangesSummary(rows)).toBe(
      "3 appointments and resignations between 1986 and 2026 — 1 of which names the officer.",
    );
  });

  it("says none rather than 0 when the register named nobody", () => {
    const rows = [board({ counterparty: null }), board({ counterparty: null })];
    expect(boardChangesSummary(rows)).toBe(
      "2 appointments and resignations in 2026 — none of which name the officer.",
    );
  });

  it("says each when every row names somebody", () => {
    expect(boardChangesSummary([board(), board()])).toBe(
      "2 appointments and resignations in 2026 — each naming the officer.",
    );
  });

  it("reads singular for one row", () => {
    expect(boardChangesSummary([board()])).toBe(
      "1 appointment or resignation in 2026 — naming the officer.",
    );
  });

  it("is null when there is nothing to open", () => {
    expect(boardChangesSummary([])).toBeNull();
  });
});

describe("buildTimelineRows with the board stream", () => {
  it("leaves board rows out until they are asked for", () => {
    const data = withEvents([board()]);
    expect(buildTimelineRows(data, false).some((r) => r.kind === "board")).toBe(false);
    expect(buildTimelineRows(data, false, true).some((r) => r.kind === "board")).toBe(true);
  });

  it("interleaves them with the notable rows, newest first", () => {
    const data = withEvents([board({ event_date: "1990-01-01" })]);
    const rows = buildTimelineRows(data, false, true);
    const dates = rows.map((r) => r.date);
    expect([...dates].sort((a, b) => b.localeCompare(a))).toEqual(dates);
  });

  it("does not pull the administrative stream in with them", () => {
    const data = withEvents([board(), board({ tier: 3, change_type: null, label: null })]);
    const rows = buildTimelineRows(data, false, true);
    expect(rows.filter((r) => r.kind === "board")).toHaveLength(1);
    expect(rows.some((r) => r.kind === "noise")).toBe(false);
  });
});

describe("filingsTruncatedNotice", () => {
  it("says which end is missing, because the register answers newest first", () => {
    const notice = filingsTruncatedNotice({ ...RESP, filings_truncated: true });
    expect(notice).toContain("more filings than this view reads");
    expect(notice).toContain("oldest");
  });

  it("is null when the whole history was read", () => {
    expect(filingsTruncatedNotice({ ...RESP, filings_truncated: false })).toBeNull();
    expect(filingsTruncatedNotice(RESP)).toBeNull();
  });
});

// ---------------------------------------------------------------------------
// Phase 198 — a board row that can point at a person
// ---------------------------------------------------------------------------

const BOARD_ROW: HistoryRawChange = {
  source_id: "companies_house",
  record_type: "relationship",
  raw_change_type: "appointed_on",
  raw_field: "officers/director",
  value_old: null,
  value_new: null,
  change_type: "OFFICER_APPOINTED",
  label: "Officer appointed",
  counterparty: "BENNETT, Kelly Brian",
  tier: 4,
  event_date: "2019-01-01",
  date_basis: "effective",
  party_id: "nW6qko0LQRsgBSdgwvD7NyscZwQ",
  party_statement_id: "opencheck-e7afdaf52cd1644858e47954",
};

describe("boardRowPersonId", () => {
  it("returns the statement id when the graph actually draws that person", () => {
    const known = new Set(["opencheck-e7afdaf52cd1644858e47954"]);
    expect(boardRowPersonId(BOARD_ROW, known)).toBe(
      "opencheck-e7afdaf52cd1644858e47954",
    );
  });

  it("returns null for a person the register keys but the graph does not draw", () => {
    // A resigned director or a secretary: on the board stream, absent from a
    // graph of serving managing officials. Linking to a node that is not
    // there is worse than not linking.
    expect(boardRowPersonId(BOARD_ROW, new Set())).toBeNull();
  });

  it("returns null for a row the source identified nobody on", () => {
    // Every filing-history row, and any officer the register does not key.
    const unkeyed = { ...BOARD_ROW, party_id: null, party_statement_id: null };
    expect(boardRowPersonId(unkeyed, new Set(["anything"]))).toBeNull();
  });

  it("tolerates an API response from before the field existed", () => {
    const { party_id: _p, party_statement_id: _s, ...older } = BOARD_ROW;
    expect(boardRowPersonId(older as HistoryRawChange, new Set())).toBeNull();
  });
});

describe("boardUncheckedNotice", () => {
  it("says so when the officers list was never read", () => {
    const notice = boardUncheckedNotice({ ...RESP, officers_available: false });
    expect(notice).toContain("was not read");
  });

  it("stays quiet when the list was read and simply held nothing", () => {
    expect(boardUncheckedNotice({ ...RESP, officers_available: true })).toBeNull();
  });

  it("stays quiet when board rows arrived regardless of the flag", () => {
    // Belt and braces: a notice claiming nothing was read, above rows that
    // plainly were, would be worse than no notice.
    expect(
      boardUncheckedNotice({
        ...RESP,
        officers_available: false,
        events: [...RESP.events, BOARD_ROW],
      }),
    ).toBeNull();
  });

  it("stays quiet for a response from before the flag existed", () => {
    expect(boardUncheckedNotice(RESP)).toBeNull();
  });
});

describe("boardRowPersonHref", () => {
  it("addresses the FullCheck network focused on that person", () => {
    expect(boardRowPersonHref(_LEI, "opencheck-abc")).toBe(
      `/?lei=${_LEI}&mode=full&focus=opencheck-abc`,
    );
  });

  it("escapes an id rather than pasting it into a query string", () => {
    expect(boardRowPersonHref(_LEI, "a b&c")).toContain("focus=a+b%26c");
  });
});

describe("boardLinkSummary", () => {
  const drawn = new Set(["opencheck-e7afdaf52cd1644858e47954"]);
  const other: HistoryRawChange = {
    ...BOARD_ROW,
    party_statement_id: "opencheck-resigned",
    counterparty: "PRIOR, Jane",
  };

  it("counts the rows that link and says what makes a row linkable", () => {
    const s = boardLinkSummary([BOARD_ROW, other], drawn);
    expect(s).toContain("One row reaches a person");
    expect(s).toContain("currently serving");
  });

  it("pluralises against the count, not the stream", () => {
    const s = boardLinkSummary([BOARD_ROW, { ...BOARD_ROW }, other], drawn);
    expect(s).toContain("2 of these rows reach a person");
  });

  it("says nothing when no row links — there is no gap to explain", () => {
    expect(boardLinkSummary([other], drawn)).toBeNull();
  });

  it("says nothing before the network is known", () => {
    // An empty `known` is "not read yet", not "draws nobody": claiming the
    // rows cannot link would be a statement about the graph we cannot make.
    expect(boardLinkSummary([BOARD_ROW], new Set())).toBeNull();
  });
});
