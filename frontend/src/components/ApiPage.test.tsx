/**
 * The /api page's MCP section (the API-page MCP guide).
 *
 * `lib/mcpTools.ts` is pinned to the server by
 * `backend/tests/test_api_page_mcp.py`. What that cannot see is whether the
 * page shows it: every tool once, in the handshake's six steps, the
 * budget-spending ones marked, and the reading rules present.
 */
import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ApiPage } from "./ApiPage";
import { MCP_BUDGET_TOOLS, MCP_STEPS, MCP_TOOLS } from "../lib/mcpTools";

function mcpSection() {
  render(<ApiPage />);
  return screen.getByRole("region", { name: "MCP server for AI agents" });
}

describe("ApiPage MCP section", () => {
  it("lists every tool exactly once", () => {
    const section = mcpSection();
    const names = MCP_TOOLS.map((t) => t.name);
    expect(new Set(names).size).toBe(names.length);
    for (const name of names) {
      expect(within(section).getAllByText((_, el) => el?.tagName === "CODE" && el.textContent === name)).toHaveLength(1);
    }
  });

  it("shows the handshake's six steps in order", () => {
    const section = mcpSection();
    const list = within(section).getByRole("list", { name: "Call the tools in this order" });
    const headings = within(list).getAllByRole("heading", { level: 5 }).map((h) => h.textContent);
    expect(headings).toEqual(MCP_STEPS.map((s, i) => `Step ${i + 1}: ${s.title}`));
  });

  it("marks exactly the tools that spend the lookup budget", () => {
    const section = mcpSection();
    const steps = within(section).getByRole("list", { name: "Call the tools in this order" });
    expect(within(steps).getAllByText("Lookup budget")).toHaveLength(MCP_BUDGET_TOOLS.length);
    expect(MCP_BUDGET_TOOLS).toContain("opencheck_lookup");
    expect(MCP_BUDGET_TOOLS).not.toContain("opencheck_knowability");
  });

  it("says how to read the answer and what a 429 means", () => {
    const section = mcpSection();
    expect(within(section).getByText("degraded_sources")).toBeTruthy();
    expect(within(section).getByText(/never clean/)).toBeTruthy();
    expect(within(section).getAllByText("retry_after_s").length).toBeGreaterThan(0);
  });

  it("is linked from the top of the page", () => {
    render(<ApiPage />);
    const nav = screen.getByRole("navigation", { name: "On this page" });
    expect(within(nav).getByRole("link", { name: "MCP server for AI agents" }).getAttribute("href")).toBe("#mcp");
  });
});
