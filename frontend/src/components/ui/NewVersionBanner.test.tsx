/**
 * Phase 331 — the "new version" line appears once a newer build is seen, and
 * not before.
 */
import { act } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";

import { NEW_VERSION_TEXT, NewVersionBanner } from "./NewVersionBanner";
import { checkForNewVersion, resetBuildVersionForTests } from "../../lib/buildVersion";

afterEach(() => {
  cleanup();
  resetBuildVersionForTests();
});

describe("NewVersionBanner", () => {
  it("says nothing until a newer build is seen, then offers a reload", async () => {
    render(<NewVersionBanner />);
    expect(screen.queryByTestId("new-version-banner")).toBeNull();

    const fetcher = vi.fn().mockResolvedValue({ ok: true, json: () => Promise.resolve({ entry: "assets/index-B.js" }) });
    await act(async () => {
      await checkForNewVersion(fetcher, "assets/index-A.js");
    });
    expect(screen.getByRole("status")).toHaveTextContent(NEW_VERSION_TEXT);
    expect(screen.getByRole("button", { name: "Reload now" })).toBeTruthy();
  });
});
