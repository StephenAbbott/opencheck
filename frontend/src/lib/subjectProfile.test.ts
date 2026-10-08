/**
 * subjectProfile — the status chip and the identity-band rows (Phase 154).
 *
 * The chip carries register status alone and never a risk tone; the rows say
 * what is known and name who said it, never a count.
 */
import { describe, expect, it } from "vitest";

import type { LeiRegistration, LeiSuccessor, SubjectProfile } from "./api";
import {
  LEI_NOT_ENTITY_STATUS,
  SUCCESSOR_FOLLOW_LABEL,
  declaredSentence,
  formatProfileDate,
  formerNamesRow,
  leiRegistrationChip,
  leiRegistrationLine,
  leiSuccessorHref,
  leiSuccessorRow,
  profileRows,
  statusChip,
} from "./subjectProfile";

const NAMES = {
  companies_house: "UK Companies House",
  gleif: "GLEIF",
  opencorporates: "OpenCorporates",
};

const fact = (value: string, sources: string[]) => ({
  value,
  sources,
  independent_sources: sources.length,
  other_values: [],
});

const shell = (overrides: Partial<SubjectProfile> = {}): SubjectProfile => ({
  legal_form: fact("Public limited company", ["companies_house", "gleif"]),
  register_status: {
    liveness: "live",
    since: null,
    raw: "active",
    source_id: "companies_house",
    sources: ["companies_house", "gleif", "opencorporates"],
    independent_sources: 2,
    other_values: [],
  },
  founding_date: fact("2002-02-05", ["companies_house", "gleif"]),
  registered_address: { ...fact("Shell Centre, London, SE1 7NA", ["companies_house"]), country: "GB" },
  jurisdiction: "GB",
  statement_ids: ["a", "b"],
  ...overrides,
});

describe("statusChip", () => {
  it("names the register and keeps a live status neutral — a status is a fact with no valence", () => {
    const chip = statusChip(shell(), NAMES);
    expect(chip).toEqual({
      label: "Active · UK Companies House",
      tone: "neutral",
      detail: "UK Companies House records this company as active.",
    });
  });

  it("warns while a terminal process is under way, with its date", () => {
    const chip = statusChip(
      shell({
        register_status: {
          liveness: "pending",
          since: "2026-01-31",
          raw: "liquidation",
          source_id: "companies_house",
          sources: ["companies_house"],
          independent_sources: 1,
          other_values: [{ source_id: "gleif", value: "live" }],
        },
      }),
      NAMES,
    );
    expect(chip?.tone).toBe("warn");
    expect(chip?.label).toBe("Terminal process under way · UK Companies House");
    expect(chip?.detail).toContain("since 31 Jan 2026");
  });

  it("marks a dissolved company as terminal — never as a risk", () => {
    const chip = statusChip(
      shell({
        register_status: {
          liveness: "terminal",
          since: "2019-04-03",
          raw: "dissolved",
          source_id: "companies_house",
          sources: ["companies_house"],
          independent_sources: 1,
          other_values: [],
        },
      }),
      NAMES,
    );
    expect(chip?.tone).toBe("terminal");
    expect(chip?.label).toBe("Dissolved · UK Companies House");
    expect(chip?.tone).not.toBe("risk");
  });

  it("renders nothing when no register stated a status — absence is not active", () => {
    expect(statusChip(shell({ register_status: null }), NAMES)).toBeNull();
    expect(statusChip(null, NAMES)).toBeNull();
    expect(statusChip(undefined, NAMES)).toBeNull();
  });

  it("falls back to a prettified id when the registry names are not loaded", () => {
    expect(statusChip(shell())?.label).toBe("Active · Companies House");
  });
});

describe("profileRows", () => {
  it("lists the four facts in reading order, each naming its sources in English", () => {
    const rows = profileRows(shell(), NAMES);
    expect(rows.map((r) => r.label)).toEqual([
      "Legal form",
      "Register status",
      "Incorporated",
      "Registered address",
    ]);
    expect(rows[0]).toEqual({
      label: "Legal form",
      value: "Public limited company",
      sources: "Source: UK Companies House and GLEIF",
    });
    expect(rows[1].value).toBe("Active");
    expect(rows[1].sources).toBe("Source: UK Companies House, GLEIF and OpenCorporates");
    expect(rows[2].value).toBe("5 Feb 2002");
    expect(rows[3].value).toBe("Shell Centre, London, SE1 7NA");
    // Never a count: two sources that copy each other would read as two.
    for (const r of rows) expect(r.sources).not.toMatch(/\d+ sources/);
  });

  it("omits a fact no source stated rather than rendering a placeholder", () => {
    const rows = profileRows(shell({ legal_form: null, registered_address: null }), NAMES);
    expect(rows.map((r) => r.label)).toEqual(["Register status", "Incorporated"]);
    expect(profileRows(null)).toEqual([]);
  });

  it("carries the register's own wording on a terminal status when it differs", () => {
    const rows = profileRows(
      shell({
        register_status: {
          liveness: "terminal",
          since: "2019-04-03",
          raw: "struck off",
          source_id: "companies_house",
          sources: ["companies_house"],
          independent_sources: 1,
          other_values: [],
        },
      }),
      NAMES,
    );
    expect(rows[1].value).toBe("Dissolved since 3 Apr 2019 — register status: “struck off”");
  });
});

describe("formatProfileDate", () => {
  it("formats a full ISO date and leaves a bare year or year-month as written", () => {
    expect(formatProfileDate("2002-02-05")).toBe("5 Feb 2002");
    expect(formatProfileDate("2002")).toBe("2002");
    expect(formatProfileDate("2002-02")).toBe("2002-02");
  });
});

// Phase 242 — the LEI record's own status. AFPC's record as GLEIF published it
// on 24 Sept 2026: LAPSED, renewal due 19 Oct 2017, entity still ACTIVE.
const lapsed: LeiRegistration = {
  status: "LAPSED",
  label: "Lapsed",
  flag: true,
  since: "2017-10-19",
  next_renewal_date: "2017-10-19",
  last_update_date: "2026-04-08",
  initial_registration_date: "2016-10-21",
  managing_lou: "5493001KJTIIGC8Y1R12",
  source_id: "gleif",
  sentence:
    "GLEIF records this LEI as lapsed: its renewal was due on 19 Oct 2017 and has not been made, so no issuer has re-checked its reference data since then. GLEIF last updated the record on 8 Apr 2026. This is the status of the LEI record, not of the company.",
};
const issued: LeiRegistration = {
  ...lapsed,
  status: "ISSUED",
  label: "Issued",
  flag: false,
  since: null,
  next_renewal_date: "2027-03-21",
  sentence: "GLEIF records this LEI as issued.",
};

describe("leiRegistrationChip", () => {
  it("says a lapse with the date it took effect, in the context tone", () => {
    const chip = leiRegistrationChip(lapsed);
    expect(chip).toEqual({ label: "LEI lapsed since 19 Oct 2017", tone: "context", detail: lapsed.sentence });
  });

  it("renders nothing for an issued LEI, or when no status was recorded", () => {
    expect(leiRegistrationChip(issued)).toBeNull();
    expect(leiRegistrationChip(null)).toBeNull();
    expect(leiRegistrationChip(undefined)).toBeNull();
  });

  it("gives no date where GLEIF publishes none, and reads a batch row's two fields", () => {
    expect(leiRegistrationChip({ status: "RETIRED" })?.label).toBe("LEI retired");
    expect(leiRegistrationChip({ status: "PENDING_TRANSFER" })?.label).toBe("LEI pending transfer");
    expect(leiRegistrationChip({ status: "LAPSED", since: "2026-09-21" })).toEqual({
      label: "LEI lapsed since 21 Sept 2026",
      tone: "context",
      detail: `GLEIF records this LEI as lapsed since 21 Sept 2026. This is ${LEI_NOT_ENTITY_STATUS}.`,
    });
  });

  it("never borrows the register chip's tones or a risk tone", () => {
    for (const status of ["LAPSED", "RETIRED", "MERGED", "ANNULLED", "DUPLICATE"]) {
      expect(leiRegistrationChip({ status })?.tone).toBe("context");
    }
  });
});

describe("the LEI registration row", () => {
  it("states the status beside register status, and says it is not the company's", () => {
    const rows = profileRows(shell({ lei_registration: lapsed }), NAMES);
    const i = rows.findIndex((r) => r.label === "LEI registration");
    expect(rows[i - 1].label).toBe("Register status");
    expect(rows[i]).toEqual({
      label: "LEI registration",
      value: `Lapsed — renewal was due 19 Oct 2017 · ${LEI_NOT_ENTITY_STATUS}`,
      sources: "Source: GLEIF",
    });
    // The register status row is untouched: a lapsed LEI is not a dissolved company.
    expect(rows[i - 1].value).toBe("Active");
  });

  it("states an issued LEI briefly, and omits the row with no status", () => {
    expect(leiRegistrationLine(issued)).toBe("Issued — renews 21 Mar 2027");
    expect(profileRows(shell(), NAMES).some((r) => r.label === "LEI registration")).toBe(false);
    expect(profileRows(shell({ lei_registration: null }), NAMES).some((r) => r.label === "LEI registration")).toBe(false);
  });
});

// Phase 291: GLEIF's ACTIVE on an LEI no issuer maintains. Bentcard Import LLP
// (54930007FGRO3F0RZ382) — LAPSED since 5 Feb 2015, dissolved by Companies
// House in 2016 — read "Active · GLEIF".
const BENTCARD_SENTENCE =
  "GLEIF holds the entity status as ACTIVE, last declared to the LEI issuer before " +
  "5 Feb 2015; the LEI has not been renewed since, so no issuer has re-checked it. " +
  "This is not a current register reading.";

const bentcard = (sentence: string | null = BENTCARD_SENTENCE): SubjectProfile =>
  shell({
    register_status: {
      liveness: "declared",
      since: "2015-02-05",
      raw: "ACTIVE",
      source_id: "gleif",
      sources: ["gleif"],
      independent_sources: 1,
      other_values: [],
      lei_registration_status: "LAPSED",
      sentence,
    },
  });

describe("a declared status (Phase 291)", () => {
  it("is never labelled Active, carries the 2015 date, and stays neutral — not dissolved, not a risk", () => {
    const chip = statusChip(bentcard(), NAMES);
    expect(chip).toEqual({
      label: "Last declared active (before 5 Feb 2015) · GLEIF",
      tone: "neutral",
      detail: BENTCARD_SENTENCE,
    });
    expect(chip?.label.startsWith("Active")).toBe(false);
  });

  it("puts the server's sentence in the Register status row", () => {
    const row = profileRows(bentcard(), NAMES).find((r) => r.label === "Register status");
    expect(row).toEqual({
      label: "Register status",
      value: BENTCARD_SENTENCE,
      sources: "Source: GLEIF",
    });
  });

  it("builds the same sentence the server does when a payload kept only liveness and date", () => {
    expect(declaredSentence({ raw: "ACTIVE", since: "2015-02-05", sentence: null })).toBe(BENTCARD_SENTENCE);
    expect(statusChip(bentcard(null), NAMES)?.detail).toBe(BENTCARD_SENTENCE);
  });

  it("gives no date where GLEIF publishes none", () => {
    const text = declaredSentence({ raw: "ACTIVE", since: null, sentence: null });
    expect(text).not.toMatch(/\d{4}/);
    expect(text.endsWith("This is not a current register reading.")).toBe(true);
  });

  it("leaves a live GLEIF status on an issued LEI exactly as it was", () => {
    const chip = statusChip(
      shell({
        register_status: {
          liveness: "live",
          since: null,
          raw: "ACTIVE",
          source_id: "gleif",
          sources: ["gleif"],
          independent_sources: 1,
          other_values: [],
        },
      }),
      NAMES,
    );
    expect(chip).toEqual({ label: "Active · GLEIF", tone: "neutral", detail: "GLEIF records this company as active." });
  });
});

// Phase 307: the successor GLEIF names on the LEI record. Diamond Bank PLC
// (029200738G7T8AI6H992) merged into Access Bank PLC (029200328C3N9YI2D660)
// on 17 April 2020 — the shape GLEIF publishes, as read on 8 Oct 2026.
const ACCESS = "029200328C3N9YI2D660";
const successor = (overrides: Partial<LeiSuccessor> = {}): LeiSuccessor => ({
  relation: "successor",
  named: [{ lei: ACCESS, name: "ACCESS BANK PLC" }],
  event: { type: "MERGERS_AND_ACQUISITIONS", status: "COMPLETED", effective_day: "2020-04-17" },
  chain: [{ lei: ACCESS, name: "ACCESS BANK PLC", entity_status: "ACTIVE", registration_status: "ISSUED" }],
  chain_source: "mirror",
  chain_complete: true,
  hops: 1,
  source_id: "gleif",
  sentence:
    "GLEIF names ACCESS BANK PLC (029200328C3N9YI2D660) as this entity's successor, on a merger or acquisition completed on 17 April 2020. Its LEI is issued.",
  ...overrides,
});

describe("the successor row (Phase 307)", () => {
  it("carries the server's sentence and one link, to the record to open next", () => {
    const rows = profileRows(shell({ lei_successor: successor() }), NAMES);
    const i = rows.findIndex((r) => r.label === "Successor");
    expect(i).toBeGreaterThan(0);
    expect(rows[i]).toEqual({
      label: "Successor",
      value: successor().sentence,
      sources: "Source: GLEIF",
      href: `/?lei=${ACCESS}`,
      hrefLabel: SUCCESSOR_FOLLOW_LABEL,
    });
    // After the LEI registration row, before the incorporation date.
    expect(rows.map((r) => r.label).indexOf("Successor")).toBeLessThan(
      rows.map((r) => r.label).indexOf("Incorporated"),
    );
  });

  it("links to the END of a followed chain, never an intermediate hop", () => {
    const end = "549300BBBBBBBBBBBBB2";
    const chained = successor({
      chain: [
        { lei: ACCESS, name: "ACCESS BANK PLC", entity_status: "INACTIVE", registration_status: "RETIRED" },
        { lei: end, name: "END", entity_status: "ACTIVE", registration_status: "LAPSED" },
      ],
      hops: 2,
    });
    expect(leiSuccessorHref(chained)).toBe(`/?lei=${end}`);
  });

  it("still links to the one LEI GLEIF named when the trail was not followed", () => {
    expect(leiSuccessorHref(successor({ chain: [], chain_source: null, chain_complete: false, hops: 0 }))).toBe(
      `/?lei=${ACCESS}`,
    );
  });

  it("gives a name-only successor, or several, no link", () => {
    const nameOnly = successor({
      named: [{ lei: null, name: "CONOCOPHILLIPS CANADA FUNDING COMPANY I" }],
      chain: [],
      chain_source: null,
      chain_complete: false,
      hops: 0,
      sentence: "GLEIF names CONOCOPHILLIPS CANADA FUNDING COMPANY I as this entity's successor, with no LEI.",
    });
    expect(leiSuccessorHref(nameOnly)).toBeNull();
    expect(leiSuccessorRow(nameOnly, NAMES)?.href).toBeUndefined();
    const several = successor({
      named: [{ lei: ACCESS, name: "A" }, { lei: null, name: "B" }],
      chain: [],
      chain_source: null,
      chain_complete: false,
      hops: 0,
    });
    expect(leiSuccessorHref(several)).toBeNull();
  });

  it("states that GLEIF names none for an ended company, with no link (Phase 308)", () => {
    const none = successor({
      relation: "none",
      named: [],
      chain: [],
      chain_source: null,
      chain_complete: true,
      hops: 0,
      sentence: "GLEIF names no successor on this LEI record.",
    });
    expect(leiSuccessorHref(none)).toBeNull();
    expect(leiSuccessorRow(none, NAMES)).toEqual({
      label: "Successor",
      value: "GLEIF names no successor on this LEI record.",
      sources: "Source: GLEIF",
    });
  });

  it("labels a duplicate registration as such, and omits the row when GLEIF names none", () => {
    const dup = successor({
      relation: "duplicate",
      event: null,
      sentence: "GLEIF records this LEI as a duplicate: the same entity is registered as STANBIC IBTC BANK PLC (549300NIVXF92ZIOVW61). This is the status of the LEI record, not of the company.",
    });
    expect(leiSuccessorRow(dup, NAMES)?.label).toBe("Duplicate LEI");
    expect(profileRows(shell(), NAMES).some((r) => r.label === "Successor")).toBe(false);
    expect(profileRows(shell({ lei_successor: null }), NAMES).some((r) => r.label === "Successor")).toBe(false);
    expect(leiSuccessorRow(successor({ named: [] }), NAMES)).toBeNull();
  });
});

describe("formerNamesRow (Phase 309)", () => {
  const barrick = () =>
    shell({
      former_names: [
        { name: "Barrick Gold Corporation", until: "2025-05-06", from: null, sources: ["gleif", "companies_house"] },
        { name: "American Barrick Resources Corporation", until: null, from: null, sources: ["gleif"] },
      ],
      name_changed_on: "2025-05-06",
    });

  it("lists every former name, dated where the register dates it, and names the sources", () => {
    expect(formerNamesRow(barrick(), NAMES)).toEqual({
      label: "Former names",
      value:
        "Barrick Gold Corporation (until 6 May 2025); American Barrick Resources Corporation · legal name changed 6 May 2025",
      sources: "Source: GLEIF and UK Companies House",
    });
  });

  it("brackets a from–until pair as Companies House files it", () => {
    const row = formerNamesRow(
      shell({
        former_names: [
          { name: "THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY", until: "2019-03-29", from: "1991-08-23", sources: ["companies_house"] },
        ],
      }),
      NAMES,
    );
    expect(row?.value).toBe("THE ARSENAL FOOTBALL CLUB PUBLIC LIMITED COMPANY (23 Aug 1991 – 29 Mar 2019)");
    expect(row?.sources).toBe("Source: UK Companies House");
  });

  it("sits after the successor row and before the incorporation date, and is absent with none", () => {
    const labels = profileRows(barrick(), NAMES).map((r) => r.label);
    expect(labels.indexOf("Former names")).toBeGreaterThan(labels.indexOf("LEI registration"));
    expect(labels.indexOf("Former names")).toBeLessThan(labels.indexOf("Incorporated"));
    expect(formerNamesRow(shell(), NAMES)).toBeNull();
    expect(formerNamesRow(shell({ former_names: [] }), NAMES)).toBeNull();
    expect(profileRows(shell(), NAMES).some((r) => r.label === "Former names")).toBe(false);
  });
});
