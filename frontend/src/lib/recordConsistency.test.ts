import { describe, it, expect } from "vitest";

import { FIELD_LABELS, recordConsistencyFields, recordConsistencyRows } from "./recordConsistency";
import type { RiskSignal } from "./api";

function signal(code: string, evidence: Record<string, unknown>): RiskSignal {
  return {
    code,
    confidence: "high",
    summary: "Two sources describe this entity (two independent).",
    source_id: "opencheck",
    hit_id: "21380068P1DRHMJ8KU70",
    evidence,
    kind: "context",
  } as RiskSignal;
}

const EVIDENCE = {
  items: [
    {
      field: "jurisdiction",
      relation: "agree",
      sources: ["companies_house", "gleif"],
      statement_ids: ["s1", "s2"],
      values: ["GB", "GB"],
      said: ["GB", "GB"],
    },
    {
      field: "liveness",
      relation: "disagree",
      sources: ["companies_house", "gleif"],
      statement_ids: ["s1", "s2"],
      values: ["terminal", "live"],
      said: ["dissolved (since 3 Apr 2019)", "ACTIVE"],
    },
  ],
  agreed_fields: ["jurisdiction"],
  disagreed_fields: ["liveness"],
  sources: 2,
  independent_sources: 2,
};

describe("recordConsistencyRows", () => {
  it("renders nothing for any other signal", () => {
    expect(recordConsistencyRows(signal("NON_EU_JURISDICTION", EVIDENCE))).toEqual([]);
  });

  it("reads the register's own words, disagreements first, fields in the house order", () => {
    const rows = recordConsistencyRows(signal("RECORD_CONSISTENCY", EVIDENCE));
    expect(rows.map((r) => [r.fieldLabel, r.relation, r.valueA, r.valueB])).toEqual([
      ["Register status", "disagree", "dissolved (since 3 Apr 2019)", "ACTIVE"],
      ["Jurisdiction", "agree", "GB", "GB"],
    ]);
    expect(rows[0].sourceA).toBe("companies_house");
    expect(rows[0].sourceB).toBe("gleif");
  });

  it("falls back to the compared values when the backend sent no said", () => {
    const rows = recordConsistencyRows(
      signal("RECORD_CONSISTENCY", {
        items: [{ field: "liveness", relation: "disagree", sources: ["a", "b"], values: ["terminal", null] }],
      })
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].valueA).toBe("terminal");
    expect(rows[0].valueB).toBe("—");
  });

  it("drops malformed items and unknown relations", () => {
    const rows = recordConsistencyRows(
      signal("RECORD_CONSISTENCY", {
        items: [
          null,
          { field: "liveness", relation: "one_missing", sources: ["a", "b"] },
          { field: "", relation: "agree", sources: ["a", "b"] },
          { field: "jurisdiction", relation: "agree", sources: ["a"] },
        ],
      })
    );
    expect(rows).toEqual([]);
  });

  it("names a field it has no label for by its code, readably", () => {
    const rows = recordConsistencyRows(
      signal("RECORD_CONSISTENCY", {
        items: [{ field: "legal_form", relation: "agree", sources: ["a", "b"], values: ["x", "x"] }],
      })
    );
    expect(rows[0].fieldLabel).toBe("legal form");
  });

  it("lists every field the backend compares", () => {
    expect(Object.keys(FIELD_LABELS)).toEqual(["liveness", "jurisdiction", "founding_date", "identifier_clash"]);
  });
});

describe("recordConsistencyFields", () => {
  it("reads the backend's lists", () => {
    expect(recordConsistencyFields(signal("RECORD_CONSISTENCY", EVIDENCE))).toEqual({
      agreed: ["jurisdiction"],
      disagreed: ["liveness"],
    });
    expect(recordConsistencyFields(signal("RECORD_CONSISTENCY", {}))).toEqual({ agreed: [], disagreed: [] });
  });
});
