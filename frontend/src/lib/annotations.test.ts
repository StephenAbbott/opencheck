import { beforeEach, describe, expect, it } from "vitest";
import {
  __resetAsFiled,
  annotatedFieldCount,
  annotationsAt,
  asFiledText,
  asFiledToggleLabel,
  getAsFiled,
  parsePointer,
  setAsFiled,
  subscribeAsFiled,
} from "./annotations";

const NOC = {
  statementPointerTarget: "/recordDetails/interests/0/type",
  motivation: "transformation",
  description:
    "Companies House nature-of-control code: ownership-of-shares-75-to-100-percent",
  transformedContent: "shareholding",
};
const BIRTH = {
  statementPointerTarget: "/recordDetails/birthDate",
  motivation: "commenting",
  description: "The register published month and year only.",
};

describe("parsePointer", () => {
  it("splits an RFC6901 pointer into segments", () => {
    expect(parsePointer("/recordDetails/interests/0/type")).toEqual([
      "recordDetails",
      "interests",
      "0",
      "type",
    ]);
  });

  it("treats the whole-statement pointer as an empty path", () => {
    expect(parsePointer("/")).toEqual([]);
    expect(parsePointer("")).toEqual([]);
  });

  it("unescapes ~1 before ~0", () => {
    // A field literally named "a~1b" is encoded "a~01b". Unescaping ~0 first
    // would turn it into "a~1b" → then into "a/b", addressing a fragment that
    // does not exist. Order is the whole correctness argument here.
    expect(parsePointer("/recordDetails/a~01b")).toEqual([
      "recordDetails",
      "a~1b",
    ]);
    expect(parsePointer("/recordDetails/a~1b")).toEqual(["recordDetails", "a/b"]);
  });
});

describe("annotationsAt", () => {
  const stmt = { annotations: [NOC, BIRTH] };

  it("matches the exact field", () => {
    expect(
      annotationsAt(stmt, "recordDetails", "interests", 0, "type"),
    ).toEqual([NOC]);
    expect(annotationsAt(stmt, "recordDetails", "birthDate")).toEqual([BIRTH]);
  });

  it("accepts numeric array indices", () => {
    expect(
      annotationsAt(stmt, "recordDetails", "interests", 0, "type"),
    ).toHaveLength(1);
  });

  it("does not leak an annotation onto a sibling interest", () => {
    // Attributing the register's words about interest 0 to interest 1 would
    // put words in the register's mouth. Exact match, never prefix.
    expect(
      annotationsAt(stmt, "recordDetails", "interests", 1, "type"),
    ).toEqual([]);
  });

  it("does not match a parent or child of the annotated path", () => {
    expect(annotationsAt(stmt, "recordDetails", "interests", 0)).toEqual([]);
    expect(
      annotationsAt(stmt, "recordDetails", "interests", 0, "type", "code"),
    ).toEqual([]);
  });

  it("is safe on statements with no annotations at all", () => {
    expect(annotationsAt({}, "recordDetails", "birthDate")).toEqual([]);
    expect(annotationsAt(null, "recordDetails")).toEqual([]);
    expect(
      annotationsAt({ annotations: "nonsense" }, "recordDetails"),
    ).toEqual([]);
  });
});

describe("asFiledText", () => {
  it("returns the register's words from description", () => {
    expect(asFiledText([NOC])).toContain(
      "ownership-of-shares-75-to-100-percent",
    );
  });

  it("joins multiple annotations rather than dropping all but the first", () => {
    expect(asFiledText([NOC, BIRTH])).toContain("·");
  });

  it("is empty when nothing was annotated", () => {
    expect(asFiledText([])).toBe("");
    expect(asFiledText([{ motivation: "commenting" }])).toBe("");
  });
});

describe("annotatedFieldCount", () => {
  it("counts distinct pointers, not annotations", () => {
    const twoOnOneField = {
      annotations: [NOC, { ...NOC, description: "second note" }],
    };
    expect(annotatedFieldCount([twoOnOneField])).toBe(1);
  });

  it("sums across statements", () => {
    expect(
      annotatedFieldCount([{ annotations: [NOC] }, { annotations: [BIRTH] }]),
    ).toBe(2);
  });

  it("is zero for unannotated bundles — the toggle must not render", () => {
    // Most sources annotate nothing, and the nine companies served from a
    // stored Open Ownership bundle bypass the mapper entirely, so they never
    // carry annotations. A control that changes nothing is worse than none.
    expect(annotatedFieldCount([{}, { recordType: "entity" }])).toBe(0);
    expect(annotatedFieldCount([])).toBe(0);
  });
});

describe("asFiledToggleLabel", () => {
  it("states the destination, matching the graph's toggle convention", () => {
    expect(asFiledToggleLabel(false, 3)).toBe("Show 3 fields as filed");
    expect(asFiledToggleLabel(true, 3)).toBe("Show OpenCheck's reading");
  });

  it("singularises", () => {
    expect(asFiledToggleLabel(false, 1)).toBe("Show 1 field as filed");
  });
});

describe("shared as-filed state", () => {
  beforeEach(() => __resetAsFiled());

  it("defaults to OpenCheck's reading", () => {
    expect(getAsFiled()).toBe(false);
  });

  it("notifies subscribers so every card switches together", () => {
    // The point of module-scoped state: a lookup renders many source cards,
    // and setting "as filed" on one while the next still shows OpenCheck's
    // vocabulary would read as a bug.
    let a = 0;
    let b = 0;
    subscribeAsFiled(() => a++);
    subscribeAsFiled(() => b++);
    setAsFiled(true);
    expect([a, b]).toEqual([1, 1]);
    expect(getAsFiled()).toBe(true);
  });

  it("does not notify when the value is unchanged", () => {
    let n = 0;
    subscribeAsFiled(() => n++);
    setAsFiled(false);
    expect(n).toBe(0);
  });

  it("unsubscribes cleanly", () => {
    let n = 0;
    const off = subscribeAsFiled(() => n++);
    off();
    setAsFiled(true);
    expect(n).toBe(0);
  });
});
