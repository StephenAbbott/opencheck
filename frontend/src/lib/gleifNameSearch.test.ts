import { describe, expect, it } from "vitest";
import { nameSearchUrl, searchGleifByName } from "./gleifNameSearch";
import { EXACT_SCAN_MAX_PAGES, EXACT_SCAN_PAGE } from "./searchFilters";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const row = (lei: string, name: string, jurisdiction: string): any => ({
  attributes: {
    lei,
    entity: { legalName: { name }, jurisdiction, legalAddress: { country: jurisdiction.slice(0, 2) }, legalForm: { id: "8888" }, status: "ACTIVE", category: "GENERAL" },
    registration: { status: "ISSUED" },
  },
});

function fakeFetch(handler: (url: URL) => unknown) {
  const calls: URL[] = [];
  const impl = async (url: string) => {
    const u = new URL(url);
    calls.push(u);
    return { ok: true, status: 200, json: async () => handler(u) } as Response;
  };
  return { impl, calls };
}

describe("the GLEIF name-search URL", () => {
  it("carries the filters with GLEIF's own parameter names", () => {
    const url = new URL(nameSearchUrl("holdings", { jurisdiction: "DE", legalForm: ["2HBR", "6QQB"] }, 10, 1));
    expect(url.searchParams.get("filter[entity.names]")).toBe("holdings");
    expect(url.searchParams.get("filter[entity.jurisdiction]")).toBe("DE");
    expect(url.searchParams.get("filter[entity.legalForm]")).toBe("2HBR,6QQB");
    expect(url.searchParams.get("page[size]")).toBe("10");
  });
});

describe("searchGleifByName", () => {
  it("returns GLEIF's total so the count line can say it", async () => {
    const { impl } = fakeFetch(() => ({ data: [row("A".repeat(20), "Alchemy Holdings", "KY")], meta: { pagination: { total: 48_503, lastPage: 4851 } } }));
    const out = await searchGleifByName("holdings", {}, null, impl);
    expect(out.total).toBe(48_503);
    expect(out.results[0].jurisdiction).toBe("KY");
    expect(out.next).toEqual({ page: 2, size: 10, offset: 0 });
  });

  it("makes a clashing jurisdiction exact: rows scanned, total corrected", async () => {
    const page = [
      ...Array.from({ length: 4 }, (_, i) => row(`PA${i}`.padEnd(20, "0"), `Panama ${i}`, "PA")),
      ...Array.from({ length: 6 }, (_, i) => row(`US${i}`.padEnd(20, "0"), `Pennsylvania ${i}`, "US-PA")),
    ];
    const { impl, calls } = fakeFetch((u) => {
      if (u.searchParams.get("filter[entity.jurisdiction]") === "US-PA") return { data: [], meta: { pagination: { total: 292 } } };
      return { data: page, meta: { pagination: { total: 493, lastPage: 1 } } };
    });
    const out = await searchGleifByName("holdings", { jurisdiction: "PA" }, null, impl);
    expect(out.results.map((r) => r.legalName)).toEqual(["Panama 0", "Panama 1", "Panama 2", "Panama 3"]);
    expect(out.total).toBe(201);
    expect(out.scanCapped).toBe(false);
    expect(calls[0].searchParams.get("page[size]")).toBe(String(EXACT_SCAN_PAGE));
  });

  it("stops at its limit, says so, and can keep looking from where it stopped", async () => {
    const pennsylvania = Array.from({ length: 3 }, (_, i) => row(`US${i}`.padEnd(20, "0"), "Pennsylvania", "US-PA"));
    const { impl, calls } = fakeFetch((u) =>
      u.searchParams.get("page[size]") === "1"
        ? { data: [], meta: { pagination: { total: 0 } } }
        : { data: pennsylvania, meta: { pagination: { total: 3_000, lastPage: 99 } } },
    );
    const out = await searchGleifByName("holdings", { jurisdiction: "PA" }, null, impl);
    expect(out.results).toEqual([]);
    expect(out.scanCapped).toBe(true);
    expect(out.next).toEqual({ page: EXACT_SCAN_MAX_PAGES + 1, size: EXACT_SCAN_PAGE, offset: 0 });
    const pagesRead = calls.filter((u) => u.searchParams.get("page[size]") === String(EXACT_SCAN_PAGE)).length;
    expect(pagesRead).toBe(EXACT_SCAN_MAX_PAGES);
  });

  it("does not compute totals again when continuing", async () => {
    const { impl, calls } = fakeFetch(() => ({ data: [row("P".repeat(20), "Panama", "PA")], meta: { pagination: { total: 1, lastPage: 1 } } }));
    const out = await searchGleifByName("holdings", { jurisdiction: "PA" }, { page: 2, size: EXACT_SCAN_PAGE, offset: 0 }, impl);
    expect(out.total).toBeNull();
    expect(calls.every((u) => u.searchParams.get("page[size]") === String(EXACT_SCAN_PAGE))).toBe(true);
  });
});
