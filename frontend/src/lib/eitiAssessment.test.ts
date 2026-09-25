/**
 * The EITI Company Assessment, as values.
 *
 * The first two describes are the ones that matter. An assessment EITI did not
 * run must never render as an adverse finding about the company, and a name in
 * two lists must never be described as one matched company — both are claims
 * the card would otherwise make by accident, and both are cheap to pin here.
 */
import { describe, expect, it } from "vitest";

import {
  assessmentTile,
  boDisclosure,
  boDisclosureLabel,
  boDisclosureTone,
  crossReference,
  DECLARED_LISTING_LABEL,
  DECLARED_LISTING_LINK_LABEL,
  declaredListing,
  disclosureLink,
  disclosureSentence,
  latestAssessmentYear,
  methodBasis,
  methodMatchLevel,
  type ComparisonList,
  type EitiAssessmentBundle,
} from "./eitiAssessment";
import { LISTING_LABEL } from "./listing";
// Vite's `?raw`, not `node:fs` — the frontend tsconfig has no Node types.
import EITI_ASSESSMENT_SRC from "./eitiAssessment.ts?raw";

function bundle(over: Partial<EitiAssessmentBundle> = {}): EitiAssessmentBundle {
  return {
    lei: "2138002658CPO9NBH955",
    name: "GLENCORE",
    hq_country: "CHE",
    sectors: ["Mining"],
    company_type: "Public",
    match: {
      method: "gleif_name_exact",
      reviewed: true,
      gleif_legal_name: "GLENCORE PLC",
    },
    assessments: {
      "2023": {
        exp_6: {
          label: "Company disclose beneficial ownership",
          response: "Yes",
          result: "Expectation met",
          bo_url: "https://www.glencore.com/who-we-are/transparency",
          comment: "Glencore publishes a statement of support.",
        },
        exp_2: { label: "Controlled subsidiaries", result: "Expectation met" },
      },
      "2025": {
        exp_6: {
          label: "Company disclose beneficial ownership",
          response: "Yes",
          result: "Expectation met",
        },
        exp_2: { label: "Controlled subsidiaries", result: "Expectation met" },
      },
    },
    subsidiaries: [
      { name: "Katanga Mining Limited", country: "COD", years: ["2023"] },
      { name: "Mopani Copper Mines Plc", country: "ZMB", years: ["2023"] },
      { name: "Prodeco S.A.", country: "COL", years: ["2023"] },
    ],
    ...over,
  };
}

describe("an assessment EITI did not run", () => {
  // The counter-sanctions lesson, in the one place this source could repeat it:
  // "Not available" is a gap in EITI's coverage, and a risk tone would print it
  // as a finding against the company.
  it.each(["Not available", "not available", "Not applicable", ""])(
    "never resolves %o to a risk tone",
    (result) => {
      expect(boDisclosureTone(result)).not.toBe("risk");
      expect(boDisclosureTone(result)).toBe("neutral");
    },
  );

  it("never resolves an unrecognised result to a risk tone", () => {
    // EITI can publish a wording this map has not seen. An unknown assessment
    // is not an adverse one.
    expect(boDisclosureTone("Assessment deferred pending review")).toBe("neutral");
    expect(boDisclosureLabel("Assessment deferred pending review")).toContain(
      "EITI recorded",
    );
  });

  it("labels it as unassessed rather than omitting it", () => {
    expect(boDisclosureLabel("Not available")).toBe("Not assessed");
  });

  it("says so in the sentence instead of falling silent", () => {
    const b = bundle({
      assessments: { "2025": { exp_6: { result: "Not available" } } },
      subsidiaries: [],
    });
    expect(disclosureSentence(b)).toBe(
      "EITI did not assess beneficial ownership disclosure for 2025.",
    );
  });
});

describe("only the three assessed results carry a graded tone", () => {
  it.each([
    ["Expectation met", "ok", "Discloses beneficial ownership"],
    ["Expectation partially met", "warn", "Partially discloses"],
    ["Expectation not met", "risk", "Does not disclose"],
  ])("maps %o", (result, tone, label) => {
    expect(boDisclosureTone(result)).toBe(tone);
    expect(boDisclosureLabel(result)).toBe(label);
  });
});

describe("which year the card is describing", () => {
  it("takes the most recent assessment", () => {
    expect(latestAssessmentYear(bundle())).toBe("2025");
    expect(boDisclosure(bundle())?.year).toBe("2025");
  });

  it("falls back through earlier years for the disclosure link", () => {
    // EITI populated these for 2023 and published none for 2025. Reading the
    // latest year alone shows nothing for most companies.
    expect(disclosureLink(bundle())).toEqual({
      url: "https://www.glencore.com/who-we-are/transparency",
      year: "2023",
    });
  });

  it("dates the link when it came from an earlier assessment", () => {
    const sentence = disclosureSentence(bundle()) ?? "";
    expect(sentence).toContain("published with the 2023 assessment");
    expect(sentence).toContain("not structured data");
  });

  it("says the link is missing rather than showing nothing", () => {
    const b = bundle({
      assessments: {
        "2025": { exp_6: { response: "Yes", result: "Expectation met" } },
      },
    });
    expect(disclosureSentence(b)).toContain(
      "EITI records a disclosure but did not publish the link for 2025",
    );
  });

  it("does not claim a missing link where EITI ran no assessment", () => {
    const b = bundle({
      assessments: { "2025": { exp_6: { result: "Not available" } } },
    });
    expect(disclosureSentence(b)).not.toContain("did not publish the link");
  });
});

describe("cross-referencing the declared names", () => {
  const gleif = (names: string[], available = true): ComparisonList => ({
    id: "gleif",
    label: "GLEIF Level 2",
    measures: "accounting consolidation",
    names,
    available,
  });

  it("counts the overlap in both directions", () => {
    const xref = crossReference(bundle().subsidiaries ?? [], [
      gleif(["Katanga Mining Limited", "Glencore Energy UK Ltd", "Viterra Limited"]),
    ]);
    expect(xref.declared).toBe(3);
    const list = xref.lists[0];
    expect(list.overlap).toBe(1);
    expect(list.eitiOnly).toBe(2);
    // The direction that disappears if you only count from the EITI side.
    expect(list.onlyInList).toBe(2);
  });

  it("matches on a folded name but not across a legal-form suffix", () => {
    const xref = crossReference(
      [
        { name: "Katanga  Mining, Limited" },
        { name: "Mopani Copper Mines Plc" },
      ],
      [gleif(["KATANGA MINING LIMITED", "Mopani Copper Mines"])],
    );
    expect(xref.rows[0].alsoIn).toEqual(["gleif"]);
    // "Mopani Copper Mines" is not "Mopani Copper Mines Plc". Stripping the
    // suffix would match it — and would also match `Teck` to TECK GmbH, which
    // is why this comparison takes the miss.
    expect(xref.rows[1].alsoIn).toEqual([]);
  });

  it("claims no membership at all when the list could not be fetched", () => {
    // The Phase 124 honest-failure rule: a fetch that did not happen must not
    // render as "declared to EITI only" on every row.
    const xref = crossReference(bundle().subsidiaries ?? [], [
      gleif(["Katanga Mining Limited"], false),
    ]);
    expect(xref.rows.every((r) => r.alsoIn.length === 0)).toBe(true);
    expect(xref.lists[0].available).toBe(false);
    expect(xref.lists[0].overlap).toBe(0);
    expect(xref.lists[0].eitiOnly).toBe(0);
  });

  it("takes N lists, so a second comparison source is a column", () => {
    // B4 adds OECD-UNSD MEIP. This is the shape it lands in.
    const meip: ComparisonList = {
      id: "meip",
      label: "OECD-UNSD MEIP",
      measures: "the whole multinational group",
      names: ["Prodeco S.A."],
      leiByKey: { "prodeco s a": "5493001KJTIIGC8Y1R12" },
      available: true,
    };
    const xref = crossReference(bundle().subsidiaries ?? [], [
      gleif(["Katanga Mining Limited"]),
      meip,
    ]);
    expect(xref.lists.map((l) => l.id)).toEqual(["gleif", "meip"]);
    expect(xref.rows[0].alsoIn).toEqual(["gleif"]);
    expect(xref.rows[2].alsoIn).toEqual(["meip"]);
    expect(xref.rows[2].lei).toBe("5493001KJTIIGC8Y1R12");
  });
});

describe("how the LEI on this record was arrived at", () => {
  it("grades an unknown method as the weakest", () => {
    expect(methodMatchLevel("candidates_only")).toBe("low");
    expect(methodMatchLevel(null)).toBe("low");
    expect(methodMatchLevel("gleif_name_exact")).toBe("medium");
    expect(methodMatchLevel("published_lei")).toBe("high");
  });

  it("says what each grade rests on", () => {
    expect(methodBasis("published_lei")).toContain("EITI publishes this LEI");
    expect(methodBasis("gleif_name_exact")).toContain("checked by hand");
  });
});

describe("the summary tile", () => {
  it("counts the declared list", () => {
    expect(assessmentTile(bundle())).toEqual({
      stat: "3",
      unit: "declared subsidiaries",
      sub: "EITI Company Assessment 2025",
    });
  });

  it("still renders, in the same voice, with no list filed", () => {
    const tile = assessmentTile(bundle({ subsidiaries: [] }));
    expect(tile.stat).toBe("—");
    expect(tile.sub).toBe("assessed 2025 · no subsidiary list filed");
  });
});

// ---------------------------------------------------------------------------
// Phase 249 — the listing as declared to EITI
// ---------------------------------------------------------------------------

/** exp_6 rows in the shapes the shipped index carries (read 25 Sept 2026). */
function listingBundle(
  byYear: Record<string, { stock_exchange?: string | null; stock_url?: string | null }>,
): EitiAssessmentBundle {
  const assessments: EitiAssessmentBundle["assessments"] = {};
  for (const [y, v] of Object.entries(byYear)) {
    assessments[y] = { exp_6: { result: "Expectation met", ...v } };
  }
  return bundle({ assessments });
}

describe("declaredListing", () => {
  it("takes the latest year's text and dates a link from an earlier year", () => {
    // Glencore: 2023 has text + the LSE company page, 2025 text only.
    const l = declaredListing(
      listingBundle({
        "2023": {
          stock_exchange: "London Stock Exchange",
          stock_url: "https://www.londonstockexchange.com/stock/GLEN/glencore-plc/company-page",
        },
        "2025": { stock_exchange: "London Stock Exchange" },
      }),
    );
    expect(l).toEqual({
      text: "London Stock Exchange",
      year: "2025",
      quoted: false,
      link: {
        url: "https://www.londonstockexchange.com/stock/GLEN/glencore-plc/company-page",
        year: "2023",
      },
    });
  });

  it("prefers the latest year's link when more than one year carries one", () => {
    const l = declaredListing(
      listingBundle({
        "2023": { stock_exchange: "Oslo Børs", stock_url: "https://www.euronext.com/en/markets/oslo" },
        "2025": { stock_exchange: "Oslo Stock Exchange", stock_url: "https://live.euronext.com/en/product/equities/NO0011157232-XOSL" },
      }),
    );
    expect(l?.link).toEqual({ url: "https://live.euronext.com/en/product/equities/NO0011157232-XOSL", year: "2025" });
  });

  it("keeps the text verbatim — never split into exchanges or tidied", () => {
    const text = "Savannah Energy is Listed under AIM, London Stock Exchange";
    expect(declaredListing(listingBundle({ "2023": { stock_exchange: `  ${text}\n` } }))?.text).toBe(text);
    // Line breaks inside the value survive for the card to render.
    expect(
      declaredListing(listingBundle({ "2023": { stock_exchange: "Prime Market\nTokyo Stock Exchange" } }))?.text,
    ).toBe("Prime Market\nTokyo Stock Exchange");
  });

  it("quotes EITI's 'not applicable' answers rather than reading them as unlisted", () => {
    for (const t of ["Not applicable", "Not Applicable.", "This is not applicable to NNPC limited at the momment"]) {
      const l = declaredListing(listingBundle({ "2023": { stock_exchange: t } }));
      expect(l?.quoted).toBe(true);
      expect(l?.text).toBe(t);
    }
    expect(declaredListing(listingBundle({ "2025": { stock_exchange: "Saudi Exchange" } }))?.quoted).toBe(false);
  });

  it("never turns URL-shaped text into a link — links come from stock_url alone", () => {
    // Chevron's stock_exchange value is a web address.
    const l = declaredListing(
      listingBundle({ "2023": { stock_exchange: "www.chevron.com/investors/financial-information#secfilings" } }),
    );
    expect(l?.text).toBe("www.chevron.com/investors/financial-information#secfilings");
    expect(l?.link).toBeNull();
  });

  it("drops a non-http link instead of rendering it as an href", () => {
    const l = declaredListing(
      listingBundle({ "2023": { stock_exchange: "Nasdaq", stock_url: "javascript:alert(1)" } }),
    );
    expect(l?.link).toBeNull();
  });

  it("shows a link on its own when EITI recorded only a link", () => {
    const l = declaredListing(listingBundle({ "2023": { stock_url: "https://www.thecse.com" } }));
    expect(l).toEqual({ text: null, year: null, quoted: false, link: { url: "https://www.thecse.com", year: "2023" } });
  });

  it("returns null when EITI recorded nothing, rather than an empty line", () => {
    expect(declaredListing(listingBundle({ "2023": {}, "2025": { stock_exchange: "  " } }))).toBeNull();
    expect(declaredListing(null)).toBeNull();
  });

  it("is labelled apart from PermID's line and never calls the link 'filings'", () => {
    expect(DECLARED_LISTING_LABEL).not.toBe(LISTING_LABEL);
    expect(DECLARED_LISTING_LABEL).toMatch(/declared to EITI/);
    expect(DECLARED_LISTING_LINK_LABEL).not.toMatch(/filing/i);
  });

  it("does not read PermID's listing — the two are never merged or reconciled", () => {
    expect(EITI_ASSESSMENT_SRC).not.toMatch(/["']\.\/listing(\.ts)?["']/);
  });
});
