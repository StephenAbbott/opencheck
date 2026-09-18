/**
 * KnowabilityChainList (Phase 226) — the run's frozen chain first and fixed,
 * fetched statements only for codes the graph added, nothing fetched on a
 * saved report, and every sentence the server's.
 */
import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";

import type { KnowabilityChain, KnowabilityStatement } from "../../lib/api";

const fetchKnowability = vi.fn();
vi.mock("../../lib/api", async (importOriginal) => {
  const mod = await importOriginal<typeof import("../../lib/api")>();
  return { ...mod, fetchKnowability: (...args: unknown[]) => fetchKnowability(...args) };
});

import { KnowabilityChainList } from "./KnowabilityChainList";

const stmt = (code: string, name: string, sentence: string): KnowabilityStatement => ({
  code,
  name,
  sentence,
  sentences: [sentence],
  stated_absence: false,
  review_status: "verified",
  last_verified: "2026-09-16",
  fields: { register: `${name} register` },
  opencheck_reads: [],
  sources: [],
});

const FROZEN: KnowabilityChain = {
  subject: "GB",
  codes: ["GB", "KY"],
  statements: [stmt("GB", "United Kingdom", "Frozen GB sentence."), stmt("KY", "Cayman Islands", "Frozen KY sentence.")],
  as_of: "2026-09-16",
};

beforeEach(() => {
  fetchKnowability.mockReset();
});

describe("KnowabilityChainList", () => {
  it("lists the frozen chain, subject first, and fetches nothing when the graph adds no code", () => {
    render(<KnowabilityChainList frozen={FROZEN} graphCodes={["GB", "KY"]} />);
    expect(screen.getByText("The mapped ownership path runs through 1 other jurisdiction.")).toBeInTheDocument();
    const items = screen.getAllByRole("listitem");
    expect(items).toHaveLength(2);
    expect(items[0]).toHaveTextContent("United Kingdom");
    expect(items[0]).toHaveTextContent("subject");
    expect(items[0]).toHaveTextContent("Frozen GB sentence.");
    expect(items[1]).toHaveTextContent("Frozen KY sentence.");
    expect(fetchKnowability).not.toHaveBeenCalled();
  });

  it("fetches only the codes the expanded graph added, and keeps the frozen sentences", async () => {
    fetchKnowability.mockResolvedValue({
      statements: [stmt("BM", "Bermuda", "Bermuda sentence today."), stmt("KY", "Cayman Islands", "A NEWER KY sentence.")],
      known_codes: [],
      generated_at: null,
      as_of: "2026-09-18",
    });
    render(<KnowabilityChainList frozen={FROZEN} graphCodes={["GB", "KY", "BM"]} />);
    await waitFor(() => expect(screen.getByText("Bermuda sentence today.")).toBeInTheDocument());
    expect(fetchKnowability).toHaveBeenCalledTimes(1);
    expect(fetchKnowability).toHaveBeenCalledWith(["BM"]);
    // The frozen KY sentence stands even though the fetch returned a newer one.
    expect(screen.getByText("Frozen KY sentence.")).toBeInTheDocument();
    expect(screen.queryByText("A NEWER KY sentence.")).toBeNull();
    expect(screen.getByText("The mapped ownership path runs through 2 other jurisdictions.")).toBeInTheDocument();
  });

  it("on a saved report shows the frozen chain only and never calls the server", () => {
    render(<KnowabilityChainList frozen={FROZEN} graphCodes={["GB", "KY", "BM", "VG"]} readOnly />);
    expect(screen.getAllByRole("listitem")).toHaveLength(2);
    expect(fetchKnowability).not.toHaveBeenCalled();
  });

  it("says when the notes could not be loaded, without inventing a sentence", async () => {
    fetchKnowability.mockRejectedValue(new Error("503 Service Unavailable"));
    render(<KnowabilityChainList frozen={FROZEN} graphCodes={["GB", "KY", "BM"]} />);
    await waitFor(() =>
      expect(screen.getByText(/register notes for BM could not be loaded \(503 Service Unavailable\)/)).toBeInTheDocument(),
    );
    expect(screen.getAllByRole("listitem")).toHaveLength(2);
  });

  it("renders nothing with no chain and no graph", () => {
    const { container } = render(<KnowabilityChainList frozen={null} graphCodes={[]} />);
    expect(container).toBeEmptyDOMElement();
  });
});
