/**
 * The error boundary around each lazy mode panel (Phase 246).
 *
 * Before it, a mode chunk that failed to load — or a panel that threw while
 * rendering — escaped `<Suspense>` with nothing to catch it and React
 * unmounted the whole app. What this pins is the markup: the failure is
 * contained, said in a sentence with a way out, and the rest of the page
 * stays on screen.
 */
import { lazy, Suspense } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { PanelBoundary, PanelLoading } from "./PanelBoundary";

function Boom(): never {
  throw new Error("render failed");
}

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("PanelBoundary", () => {
  it("renders its children when nothing fails", () => {
    render(
      <PanelBoundary label="FullCheck">
        <p>the network</p>
      </PanelBoundary>,
    );
    expect(screen.getByText("the network")).toBeTruthy();
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("contains a panel that throws, and names the tab", () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    render(
      <div>
        <h1>The subject</h1>
        <PanelBoundary label="History">
          <Boom />
        </PanelBoundary>
      </div>,
    );
    expect(screen.getByRole("alert").textContent).toContain("The History tab could not be loaded");
    expect(screen.getByRole("button", { name: "Reload the page" })).toBeTruthy();
    // The page around it survives — the failure used to blank everything.
    expect(screen.getByRole("heading", { name: "The subject" })).toBeTruthy();
  });

  it("contains a lazy chunk that fails to load", async () => {
    vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const Missing = lazy(() =>
      Promise.reject(new TypeError("Failed to fetch dynamically imported module")),
    );
    render(
      <PanelBoundary label="Subsidiaries">
        <Suspense fallback={<PanelLoading label="Subsidiaries" />}>
          <Missing />
        </Suspense>
      </PanelBoundary>,
    );
    expect(screen.getByText("Loading Subsidiaries…")).toBeTruthy();
    const alert = await screen.findByRole("alert");
    expect(alert.textContent).toContain("The Subsidiaries tab could not be loaded");
  });
});
