import { describe, expect, it } from "vitest";
import {
  ACRONYMS,
  BANNED_SYNONYMS,
  bodsRecordCount,
  expandOnFirstUse,
  graphPartiesLabel,
  chainSourceCaption,
  mirrorCaption,
  OPENALEPH_TOPIC,
  resultCount,
  sourceLabel,
  setSourceNames,
  sourceList,
  topicLabel,
  topicList,
} from "./vocab";

describe("resultCount", () => {
  it("agrees in number", () => {
    // App.tsx:1511 hardcoded "results" with no singular branch — a plain bug
    // at n=1 that a shared helper cannot reproduce.
    expect(resultCount(0)).toBe("0 results");
    expect(resultCount(1)).toBe("1 result");
    expect(resultCount(2)).toBe("2 results");
  });

  it("never says hit", () => {
    // The same array was "3 results" on one line of SourceBucketCard and
    // "No hits." fifty-four lines later.
    for (const n of [0, 1, 5]) expect(resultCount(n)).not.toMatch(/hit/i);
  });
});

describe("bodsRecordCount", () => {
  it("always qualifies the word", () => {
    // "12 statements" meant BODS records on the source card and narrative
    // claims in the panel directly above it.
    expect(bodsRecordCount(12)).toBe("12 BODS records");
    expect(bodsRecordCount(1)).toBe("1 BODS record");
    for (const n of [1, 12]) expect(bodsRecordCount(n)).toContain("BODS");
  });
});

describe("expandOnFirstUse", () => {
  it("expands once per surface, then leaves the term alone", () => {
    const seen = new Set<string>();
    expect(expandOnFirstUse("BODS", seen)).toBe("BODS (Beneficial Ownership Data Standard)");
    expect(expandOnFirstUse("BODS", seen)).toBe("BODS");
  });

  it("tracks each term separately", () => {
    const seen = new Set<string>();
    expect(expandOnFirstUse("LEI", seen)).toContain("Legal Entity Identifier");
    expect(expandOnFirstUse("PEP", seen)).toContain("politically exposed person");
    expect(expandOnFirstUse("LEI", seen)).toBe("LEI");
  });

  it("starts fresh for a new surface", () => {
    // A reader who lands on a report link has not read the homepage, so
    // "first use" is per page, not per session.
    expect(expandOnFirstUse("BODS", new Set())).toContain("(");
  });

  it("passes an unknown term through untouched", () => {
    expect(expandOnFirstUse("QUICKCHECK", new Set())).toBe("QUICKCHECK");
  });

  it("expands every acronym the report actually shows", () => {
    for (const [term, expansion] of Object.entries(ACRONYMS)) {
      expect(expansion.length, term).toBeGreaterThan(term.length);
      expect(expansion, term).not.toBe(expansion.toUpperCase());
    }
  });
});

describe("sourceLabel", () => {
  it("uses the display name the response carries", () => {
    expect(sourceLabel("companies_house", { companies_house: "UK Companies House" })).toBe(
      "UK Companies House",
    );
  });

  it("never shows a reader a snake_case slug", () => {
    // "via companies_house" rendered in mono on every person match row, and
    // `.join(" and ")` over raw ids built English sentences out of them.
    expect(sourceLabel("companies_house")).toBe("Companies House");
    expect(sourceLabel("cvr_denmark")).toBe("CVR Denmark");
    expect(sourceLabel("ted_eu")).toBe("TED EU");
    expect(sourceLabel("opensanctions")).toBe("Opensanctions");
    for (const id of ["companies_house", "bods_gleif", "jar_lithuania"]) {
      expect(sourceLabel(id)).not.toContain("_");
    }
  });

  it("falls back rather than rendering nothing for an unmapped source", () => {
    expect(sourceLabel("brand_new_source", { other: "Other" })).toBe("Brand New Source");
  });
});

describe("sourceList", () => {
  it("writes an English list", () => {
    const names = { companies_house: "UK Companies House", opensanctions: "OpenSanctions" };
    expect(sourceList(["companies_house"], names)).toBe("UK Companies House");
    expect(sourceList(["companies_house", "opensanctions"], names)).toBe(
      "UK Companies House and OpenSanctions",
    );
  });

  it("uses a serial comma list beyond two", () => {
    expect(sourceList(["a_source", "b_source", "c_source"])).toBe(
      "A Source, B Source and C Source",
    );
  });

  it("deduplicates", () => {
    expect(sourceList(["opensanctions", "opensanctions"])).toBe("Opensanctions");
  });

  it("returns empty for no sources rather than a dangling connective", () => {
    expect(sourceList([])).toBe("");
  });
});

describe("topicLabel", () => {
  it("translates the upstream slugs a reader was shown raw", () => {
    // "disqualified", not "disqualified director": upstream applies the
    // topic to companies as well as to people, and naming the role would
    // invent one.
    expect(topicLabel("corp.disqual")).toBe("disqualified");
    expect(topicLabel("poi")).toBe("person of interest");
    expect(topicLabel("crime.fin")).toBe("financial crime");
  });

  it("prettifies an unmapped topic rather than dropping it", () => {
    // The fact that a collection was tagged is information even when the tag
    // is new to us, so an unknown id is cleaned up, not hidden.
    expect(topicLabel("some.new_topic")).toBe("some new topic");
  });

  it("maps only what upstream already means — no inference", () => {
    // Every entry must be a translation of the slug, not a judgement added on
    // top. A label that introduced a word like "confirmed" or "guilty" would
    // be asserting something FollowTheMoney's topic does not.
    for (const label of Object.values(OPENALEPH_TOPIC)) {
      expect(label).not.toMatch(/\b(confirmed|guilty|proven|verified)\b/i);
    }
  });
});

describe("topicList", () => {
  it("joins readable labels, deduplicated", () => {
    expect(topicList(["poi", "corp.disqual"])).toBe("person of interest, disqualified");
    expect(topicList(["poi", "poi"])).toBe("person of interest");
  });
});

describe("BANNED_SYNONYMS", () => {
  it("names a replacement for every banned term", () => {
    for (const [banned, replacement] of Object.entries(BANNED_SYNONYMS)) {
      expect(replacement, banned).toBeTruthy();
      expect(replacement.toLowerCase(), banned).not.toBe(banned.toLowerCase());
    }
  });

  it("does not ban a word it also recommends", () => {
    // A replacement that is itself banned would make the rule uncloseable.
    const banned = new Set(Object.keys(BANNED_SYNONYMS).map((k) => k.toLowerCase()));
    for (const replacement of Object.values(BANNED_SYNONYMS)) {
      expect(banned.has(replacement.toLowerCase()), replacement).toBe(false);
    }
  });
});

describe("the banned labels the lint enforces", () => {
  it("bans phrases, not words", () => {
    // The first version of this list banned single words and the lint that
    // enforced it reported 90 violations, every one false: "an empty result
    // here is not a clean screen", "a fast screen of the subject", the SSE
    // event named "hit", `Liveness = "stub"`, and `min-h-screen` in a Tailwind
    // class. A word ban would have forced rewrites of correct English.
    for (const banned of Object.keys(BANNED_SYNONYMS)) {
      expect(banned.length, banned).toBeGreaterThan(4);
    }
  });

  it("bans each label that actually regressed", () => {
    // One button said "Screen person" while another said "Run background
    // check" for the same action; an empty state said "No hits." forty lines
    // from one that said "3 results".
    for (const label of ["No hits.", "Screen person", "Run background check", "Look up"]) {
      expect(Object.keys(BANNED_SYNONYMS), label).toContain(label);
    }
  });

  it("points every banned label at something renderable", () => {
    for (const [banned, replacement] of Object.entries(BANNED_SYNONYMS)) {
      expect(replacement, banned).toBeTruthy();
      expect(replacement, banned).not.toBe(banned);
    }
  });
});

describe("graphPartiesLabel", () => {
  it("counts entities, not 'parties'", () => {
    // The chip is computed from the entity split alone. Calling that total
    // "parties" put "7 parties" over a diagram holding eleven nodes.
    expect(graphPartiesLabel(7)).toBe("7 entities");
    expect(graphPartiesLabel(7, 0)).toBe("7 entities");
  });

  it("names the people when the source disclosed any", () => {
    expect(graphPartiesLabel(7, 4)).toBe("7 entities · 4 people");
  });

  it("agrees in number on both halves", () => {
    expect(graphPartiesLabel(1)).toBe("1 entity");
    expect(graphPartiesLabel(1, 1)).toBe("1 entity · 1 person");
    expect(graphPartiesLabel(2, 1)).toBe("2 entities · 1 person");
  });

  it("never says '0 people', which reads as a finding about disclosure", () => {
    expect(graphPartiesLabel(3, 0)).not.toMatch(/people|person/);
    expect(graphPartiesLabel(3, -1)).toBe("3 entities");
  });

  it("never says 'parties'", () => {
    for (const [e, p] of [[1, 0], [1, 1], [5, 2], [12, 9]] as const) {
      expect(graphPartiesLabel(e, p)).not.toMatch(/part(y|ies)/);
    }
  });
});

describe("published registry names", () => {
  it("names a source the way the registry does, with no map in hand", () => {
    // The risk chip's evidence, the ESG cards and the source legend all hold
    // a source id and no `sourceNames` prop, and rendered "Opensanctions"
    // beside cards headed "OpenSanctions".
    setSourceNames({ opensanctions: "OpenSanctions" });
    expect(sourceLabel("opensanctions")).toBe("OpenSanctions");
    expect(sourceList(["opensanctions", "everypolitician"])).toBe(
      "OpenSanctions and Everypolitician"
    );
    setSourceNames({});
  });

  it("still prefers an explicit map, so nothing that threads one changes", () => {
    setSourceNames({ opensanctions: "OpenSanctions" });
    expect(sourceLabel("opensanctions", { opensanctions: "Local override" })).toBe(
      "Local override"
    );
    setSourceNames({});
  });

  it("falls back to prettifying an id the registry does not know", () => {
    setSourceNames({ opensanctions: "OpenSanctions" });
    expect(sourceLabel("some_new_source")).toBe("Some New Source");
    setSourceNames({});
  });
});

describe("mirrorCaption", () => {
  it("names the mirror and the Golden Copy date, and never reads as a failure", () => {
    const text = mirrorCaption("2026-09-07");
    expect(text).toContain("GLEIF mirror");
    expect(text).toContain("Golden Copy of 2026-09-07");
    expect(text).not.toMatch(/refus|unreachable|rate-limit|could not/i);
  });

  it("still says where the rows came from without a date", () => {
    const text = mirrorCaption(null);
    expect(text).toContain("GLEIF mirror");
    expect(text).not.toContain("Golden Copy of");
  });
});

describe("chainSourceCaption", () => {
  it("says nothing at all for a chain walked live — that is the old behaviour", () => {
    expect(chainSourceCaption({ source: "live", related: 4 })).toBeNull();
    expect(chainSourceCaption({ source: "graph_unavailable" })).toBeNull();
    expect(chainSourceCaption(null)).toBeNull();
    expect(chainSourceCaption(undefined)).toBeNull();
  });

  it("names the local copy, its dates, and says the register was still read", () => {
    const text = chainSourceCaption({
      source: "graph",
      related: 4,
      missed: 0,
      snapshot_date: "2026-09-08",
      stream_published_at: "2026-09-08T14:03:21",
    });
    expect(text).toContain("UK PSC register");
    expect(text).toContain("snapshot of 2026-09-08");
    expect(text).toContain("stream to 2026-09-08 14:03");
    expect(text).toContain("read from Companies House");
    // Nothing was refused, so it must not read as a degradation.
    expect(text).not.toMatch(/refus|unreachable|could not|failed|degrad/i);
  });

  it("owns up when the local copy did not know part of the chain", () => {
    const one = chainSourceCaption({ source: "graph", related: 4, missed: 1 });
    expect(one).toContain("1 company in the chain was not in the local copy yet");
    expect(one).toContain("found live");
    const two = chainSourceCaption({ source: "graph", related: 4, missed: 2 });
    expect(two).toContain("2 companies in the chain were not in the local copy yet");
  });

  it("does not boast when the local copy knew the whole chain", () => {
    // The absence of the caveat is the good case; a claim of completeness is
    // a different, stronger thing than "here is where the chain came from",
    // and OpenCheck cannot make it — the register is the only authority on
    // what the chain is. "Every company on it read from Companies House" is
    // a statement about what was fetched, not about the chain being whole.
    const text = chainSourceCaption({ source: "graph", related: 4, missed: 0 });
    expect(text).not.toMatch(/complete|the whole chain|nothing missing|up to date/i);
    expect(text).not.toMatch(/not in the local copy/i);
  });

  it("reads a chain with no accuracy fields as a clean one, not a broken one", () => {
    // Phase 189: `missed` / `extra` are absent unless the graph was asked.
    // A graph chain that carries neither must not grow a caveat by default.
    const text = chainSourceCaption({ source: "graph", related: 3, snapshot_date: "2026-09-09" });
    expect(text).toContain("UK PSC register");
    expect(text).not.toMatch(/not in the local copy/i);
  });

  it("still says where the chain came from without the graph's dates", () => {
    const text = chainSourceCaption({ source: "graph", related: 2 });
    expect(text).toContain("UK PSC register");
    expect(text).not.toContain("snapshot of");
    expect(text).not.toContain("stream to");
  });
});
