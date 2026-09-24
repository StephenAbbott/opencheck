import { describe, expect, it } from "vitest";
import { PHONE_QUERY, isPhoneViewport } from "./viewport";

describe("isPhoneViewport (Phase 245)", () => {
  it("asks for the width below Tailwind's sm, so the reorder and the restyle switch together", () => {
    expect(PHONE_QUERY).toBe("(max-width: 639px)");
    const asked: string[] = [];
    const win = {
      matchMedia: (q: string) => {
        asked.push(q);
        return { matches: true };
      },
    };
    expect(isPhoneViewport(win)).toBe(true);
    expect(asked).toEqual([PHONE_QUERY]);
  });

  it("is not a phone where it cannot tell (jsdom, SSR, a throwing matchMedia)", () => {
    expect(isPhoneViewport(undefined)).toBe(false);
    expect(isPhoneViewport({})).toBe(false);
    expect(
      isPhoneViewport({
        matchMedia: () => {
          throw new Error("no");
        },
      }),
    ).toBe(false);
  });
});
