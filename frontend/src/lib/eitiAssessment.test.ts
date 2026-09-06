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
  disclosureLink,
  disclosureSentence,
  latestAssessmentYear,
  methodBasis,
  methodMatchLevel,
  type ComparisonList,
  type EitiAssessmentBundle,
} from "./eitiAssessment";

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
