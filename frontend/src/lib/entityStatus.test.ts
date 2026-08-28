import { describe, expect, it } from "vitest";
import {
  entityStatusBanner,
  FOLLOW_FORWARD_LABEL,
  isJointVenture,
  JOINT_VENTURE_LABEL,
} from "./entityStatus";

const AMALGAMATED = {
  status: "amalgamated",
  merged_into: "E100001014363",
  merged_into_name: "Delek Logistics Partners LP",
  merged_into_lei: "549300UVYITDIU51P724",
  urls: ["https://example.org/acquisition"],
};

describe("entityStatusBanner", () => {
  it("names the successor and links its lookup for an amalgamated entity", () => {
    const banner = entityStatusBanner(AMALGAMATED);
    expect(banner).not.toBeNull();
    expect(banner!.text).toBe(
      "Global Energy Monitor records this entity as amalgamated into " +
        "Delek Logistics Partners LP.",
    );
    expect(banner!.followHref).toBe("/?lei=549300UVYITDIU51P724");
    expect(banner!.urls).toEqual(["https://example.org/acquisition"]);
  });

  it("falls back to the successor's GEM id, then to 'another entity'", () => {
    const noName = entityStatusBanner({
      status: "amalgamated",
      merged_into: "E100002009178",
    });
    expect(noName!.text).toContain("amalgamated into E100002009178.");
    const bare = entityStatusBanner({ status: "amalgamated" });
    expect(bare!.text).toContain("amalgamated into another entity.");
  });

  it("offers no follow-forward link without a valid successor LEI", () => {
    const banner = entityStatusBanner({
      status: "amalgamated",
      merged_into: "E100002009178",
      merged_into_name: "Abu Dhabi Gas Processing",
    });
    expect(banner!.followHref).toBeNull();
  });

  it("states dissolution in the same voice, with no alarm framing", () => {
    const banner = entityStatusBanner({
      status: "dissolved",
      urls: ["https://example.org/strike-off"],
    });
    expect(banner!.text).toBe(
      "Global Energy Monitor records this entity as dissolved.",
    );
    expect(banner!.followHref).toBeNull();
    expect(banner!.urls).toEqual(["https://example.org/strike-off"]);
  });

  it("mentions a forward pointer when a dissolved record carries one", () => {
    // Seen in the August 2026 data: at least one dissolved (not amalgamated)
    // entity also names a successor.
    const banner = entityStatusBanner({
      status: "dissolved",
      merged_into: "E100001014363",
      merged_into_name: "Delek Logistics Partners LP",
      merged_into_lei: "549300UVYITDIU51P724",
    });
    expect(banner!.text).toBe(
      "Global Energy Monitor records this entity as dissolved; " +
        "its record points forward to Delek Logistics Partners LP.",
    );
    expect(banner!.followHref).toBe("/?lei=549300UVYITDIU51P724");
  });

  it("drops non-http evidence values rather than rendering them", () => {
    const banner = entityStatusBanner({
      status: "dissolved",
      urls: ["https://ok.example", "not a url", "ftp://nope.example"],
    });
    expect(banner!.urls).toEqual(["https://ok.example"]);
  });

  it("is null for active entities, bare JV flags, and missing data", () => {
    expect(entityStatusBanner(null)).toBeNull();
    expect(entityStatusBanner(undefined)).toBeNull();
    expect(entityStatusBanner({})).toBeNull();
    expect(entityStatusBanner({ jv: true })).toBeNull();
  });

  it("rejects a malformed successor LEI instead of linking a broken lookup", () => {
    const banner = entityStatusBanner({
      status: "amalgamated",
      merged_into_name: "X",
      merged_into_lei: "not-an-lei",
    });
    expect(banner!.followHref).toBeNull();
  });
});

describe("isJointVenture", () => {
  it("is true only when GEM flags jv", () => {
    expect(isJointVenture({ jv: true })).toBe(true);
    expect(isJointVenture({ jv: false })).toBe(false);
    expect(isJointVenture({ status: "dissolved" })).toBe(false);
    expect(isJointVenture(null)).toBe(false);
  });
});

describe("labels", () => {
  it("keep GEM's own framing for the follow-forward affordance", () => {
    expect(FOLLOW_FORWARD_LABEL).toBe("Follow the ownership trail forward");
    expect(JOINT_VENTURE_LABEL).toBe("Joint venture");
  });
});
