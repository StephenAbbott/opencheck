import { COUNTRY_OPTIONS, RA_CODES, raCodeFor } from "../lib/raCodes";
import { leiInputMessage } from "../lib/api";
import { PERSON_VERB, resultCount } from "../lib/vocab";
import type { SearchForm } from "../hooks/useSearchForm";
import { GleifIcon } from "./icons";

/*
 * The homepage's four-tab search panel — company name, national ID, a pasted
 * LEI, a person's name. Moved out of App.tsx in Phase 246, unchanged; its
 * state lives in `useSearchForm` because App shares it (see that hook).
 */

export function SearchPanel({
  search,
  collapsed,
  onExpand,
  lookupPending,
  onLookup,
  onOpenPerson,
  onPicked,
}: {
  search: SearchForm;
  /** A report is on screen and the panel is folded into the header. */
  collapsed: boolean;
  /** Open the panel (on a phone, where it folds once results are shown). */
  onExpand: () => void;
  lookupPending: boolean;
  onLookup: (lei: string) => void;
  onOpenPerson: (name: string, birthYear?: number) => void;
  /** After a result is picked: the picker unmounts, so focus moves to main. */
  onPicked: () => void;
}) {
  const {
    searchMode,
    setSearchMode,
    leiInput,
    setLeiInput,
    leiInputError,
    setLeiInputError,
    nameQuery,
    setNameQuery,
    personQuery,
    setPersonQuery,
    personBirthYear,
    setPersonBirthYear,
    nationalIdQuery,
    setNationalIdQuery,
    selectedCountry,
    setSelectedCountry,
    setNationalIdTouched,
    nationalIdFormatOk,
    nameSearchMutation,
    nationalIdSearchMutation,
  } = search;
  const searchPanelsCollapsed = collapsed;

  // APG tabs keyboard pattern: Left/Right arrows (wrapping), Home and End move
  // both focus and selection across the search-mode tabs (roving tabindex).
  const SEARCH_TAB_ORDER = ["name", "nationalId", "lei", "person"] as const;
  const SEARCH_TAB_IDS: Record<(typeof SEARCH_TAB_ORDER)[number], string> = {
    name: "tab-name",
    nationalId: "tab-national-id",
    lei: "tab-lei",
    person: "tab-person",
  };
  function onSearchTabKeyDown(e: React.KeyboardEvent<HTMLButtonElement>) {
    const idx = SEARCH_TAB_ORDER.indexOf(searchMode);
    let next: number;
    if (e.key === "ArrowRight") next = (idx + 1) % SEARCH_TAB_ORDER.length;
    else if (e.key === "ArrowLeft") next = (idx + SEARCH_TAB_ORDER.length - 1) % SEARCH_TAB_ORDER.length;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = SEARCH_TAB_ORDER.length - 1;
    else return;
    e.preventDefault();
    const mode = SEARCH_TAB_ORDER[next];
    setSearchMode(mode);
    onExpand();
    document.getElementById(SEARCH_TAB_IDS[mode])?.focus();
  }

  function runLookup(e: React.FormEvent) {
    e.preventDefault();
    const message = leiInputMessage(leiInput);
    setLeiInputError(message);
    if (message) {
      document.getElementById("lei-input")?.focus();
      return;
    }
    onLookup(leiInput);
  }

  function searchByName(e: React.FormEvent) {
    e.preventDefault();
    const q = nameQuery.trim();
    if (!q) return;
    nameSearchMutation.mutate(q);
  }

  return (
    <>
      {/* Phase 245: on a report the panel is folded into the header — a
          "More search options" control there reopens it — so the collapsed
          state renders nothing here. The one-line row that used to stand in
          for it sat between the header and the subject on every report. */}
      <div
        className={
          searchPanelsCollapsed
            ? "hidden"
            : "mb-4 bg-white border border-oo-rule rounded-oo overflow-hidden"
        }
      >
        {/* Tab bar — homepage only.

            It used to stay on the report as a "landmark", with only the
            440 lines of input panels collapsing beneath it. That left four
            tabs and a search affordance above every result, which is the
            v1 page's opening move: search first, answer second. The v2
            report opens on the subject. The one-line prompt below is the
            whole search surface on a result page, and reopening it brings
            the tabs back with it. */}
        {!searchPanelsCollapsed && (
        <div role="tablist" aria-label="Search method" className="flex border-b border-oo-rule">
          <button
            type="button"
            role="tab"
            aria-selected={searchMode === "name"}
            aria-controls={searchMode === "name" ? "panel-name" : undefined}
            id="tab-name"
            tabIndex={searchMode === "name" ? 0 : -1}
            onKeyDown={onSearchTabKeyDown}
            onClick={() => { setSearchMode("name"); onExpand(); }}
            className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-oo-meta font-medium transition-colors bg-white ${
              searchMode === "name"
                ? "text-oo-ink border-b-2 border-oo-blue"
                : "text-oo-muted hover:text-oo-ink"
            }`}
          >
            <GleifIcon aria-hidden style={{ height: "1.1em", width: "auto", flexShrink: 0 }} />
            Company name
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={searchMode === "nationalId"}
            aria-controls={searchMode === "nationalId" ? "panel-national-id" : undefined}
            id="tab-national-id"
            tabIndex={searchMode === "nationalId" ? 0 : -1}
            onKeyDown={onSearchTabKeyDown}
            onClick={() => { setSearchMode("nationalId"); onExpand(); }}
            className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-oo-meta font-medium transition-colors border-l border-oo-rule bg-white ${
              searchMode === "nationalId"
                ? "text-oo-ink border-b-2 border-oo-blue"
                : "text-oo-muted hover:text-oo-ink"
            }`}
          >
            <svg aria-hidden="true" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><path d="M4 7h16M4 12h8m-8 5h16"/></svg>
            National ID
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={searchMode === "lei"}
            aria-controls={searchMode === "lei" ? "panel-lei" : undefined}
            id="tab-lei"
            tabIndex={searchMode === "lei" ? 0 : -1}
            onKeyDown={onSearchTabKeyDown}
            onClick={() => { setSearchMode("lei"); onExpand(); }}
            className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-oo-meta font-medium transition-colors border-l border-oo-rule bg-white ${
              searchMode === "lei"
                ? "text-oo-ink border-b-2 border-oo-blue"
                : "text-oo-muted hover:text-oo-ink"
            }`}
          >
            <svg aria-hidden="true" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><rect x="9" y="2" width="6" height="4" rx="1"/><path d="M8 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V4a2 2 0 0 0-2-2h-2"/><path d="M12 12h4m-4 4h4m-8-4h.01M8 16h.01"/></svg>
            Paste an LEI
          </button>
          <button
            type="button"
            role="tab"
            aria-selected={searchMode === "person"}
            aria-controls={searchMode === "person" ? "panel-person" : undefined}
            id="tab-person"
            tabIndex={searchMode === "person" ? 0 : -1}
            onKeyDown={onSearchTabKeyDown}
            onClick={() => { setSearchMode("person"); onExpand(); }}
            className={`flex-1 flex flex-col items-center justify-center gap-1 px-3 py-2 text-oo-meta font-medium transition-colors border-l border-oo-rule bg-white ${
              searchMode === "person"
                ? "text-oo-ink border-b-2 border-oo-blue"
                : "text-oo-muted hover:text-oo-ink"
            }`}
          >
            <svg aria-hidden="true" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="8" r="3.5"/><path d="M5 20c.8-3.5 3.6-5.5 7-5.5s6.2 2 7 5.5"/></svg>
            Person name
          </button>
        </div>
        )}

        {/* Panels collapse once results are on screen — the tab bar stays
            as a landmark; the prompt row below reopens them. Phase 122
            widened this from mobile-only: 440 lines of search panel above
            a report is the same problem on a laptop as on a phone. */}
        <div className={searchPanelsCollapsed ? "hidden" : ""}>

        {/* ── Name search panel ── */}
        {searchMode === "name" && (
          <div id="panel-name" role="tabpanel" aria-labelledby="tab-name" className="p-4">
            <form onSubmit={searchByName}>
              <div className="flex flex-col sm:flex-row gap-3">
                <input
                  id="name-input"
                  type="search"
                  value={nameQuery}
                  onChange={(e) => setNameQuery(e.target.value)}
                  placeholder="Search by company name"
                  autoComplete="off"
                  aria-label="Company name"
                  className="flex-1 border border-oo-rule rounded px-3 py-2.5 bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                />
                <button
                  type="submit"
                  disabled={nameSearchMutation.isPending || !nameQuery.trim()}
                  aria-busy={nameSearchMutation.isPending}
                  className="w-full sm:w-auto bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                >
                  {nameSearchMutation.isPending ? "Searching…" : "Search"}
                </button>
              </div>
            </form>

            {/* No aria-live here — the role="alert" children announce themselves */}
            <div>
              {nameSearchMutation.isError && (
                <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                  {nameSearchMutation.error?.message ?? "Search failed"}
                </div>
              )}
              {nameSearchMutation.isSuccess && nameSearchMutation.data.length === 0 && (
                <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                  No entities found. Try a shorter or different spelling.
                </div>
              )}
            </div>

            {/* Phase 241: the count is announced once, by a status
                region that is always mounted. The container used to be
                \`aria-live\` itself, so it was inserted with its content —
                which some readers ignore and others read in full, every
                result button after the count. */}
            <p role="status" className="sr-only">
              {nameSearchMutation.data && nameSearchMutation.data.length > 0
                ? `${resultCount(nameSearchMutation.data.length)} — select one to search it`
                : ""}
            </p>
            {nameSearchMutation.data && nameSearchMutation.data.length > 0 && (
              <div className="mt-4">
                <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-3">
                  {resultCount(nameSearchMutation.data.length)} — select one to search it
                </p>
                <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                  {nameSearchMutation.data.map((r) => (
                    <li key={r.lei}>
                      <button
                        type="button"
                        aria-label={`Search ${r.legalName}, LEI ${r.lei}`}
                        onClick={() => {
                          nameSearchMutation.reset();
                          setNameQuery("");
                          onLookup(r.lei);
                          onPicked();
                        }}
                        className="w-full text-left px-4 py-3 hover:bg-oo-bg transition-colors focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oo-blue/40"
                      >
                        <div className="font-head font-bold text-oo-body text-oo-ink leading-snug">
                          {r.legalName}
                        </div>
                        <div className="flex items-center gap-3 mt-1">
                          <span className="font-mono text-[11px] text-oo-blue">
                            {r.lei}
                          </span>
                          <span className="text-[11px] text-oo-muted">{r.country}</span>
                          <span
                            className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                              r.status === "ISSUED"
                                ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                                : "bg-oo-bg text-oo-muted border-oo-rule"
                            }`}
                          >
                            {r.status}
                          </span>
                        </div>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* ── National ID panel ── */}
        {searchMode === "nationalId" && (
          <div id="panel-national-id" role="tabpanel" aria-labelledby="tab-national-id" className="p-4">
            <form
              onSubmit={(e) => {
                e.preventDefault();
                const q = nationalIdQuery.trim();
                if (!q) return;
                const entry = RA_CODES[selectedCountry];
                if (!entry) return;
                nationalIdSearchMutation.mutate(
                  // raCodeFor, not entry.raCode: a GB number beginning SC or
                  // NI belongs to a different Companies House authority, and
                  // scoping it to England & Wales returns nothing at all.
                  { raCode: raCodeFor(selectedCountry, q), id: q },
                  {
                    onSuccess: (results) => {
                      if (results.length === 1) {
                        // Single unambiguous match — go straight to the lookup.
                        nationalIdSearchMutation.reset();
                        setNationalIdQuery("");
                        onLookup(results[0].lei);
                        onPicked();
                      }
                      // Multiple results: show the picker below (same as name search).
                    },
                  },
                );
              }}
            >
              <div className="flex flex-col sm:flex-row sm:gap-3 sm:items-end gap-3">
                <div className="sm:flex-none">
                  <label
                    htmlFor="national-id-country"
                    className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2"
                  >
                    Country
                  </label>
                  <select
                    id="national-id-country"
                    value={selectedCountry}
                    onChange={(e) => {
                      setSelectedCountry(e.target.value);
                      nationalIdSearchMutation.reset();
                      setNationalIdQuery("");
                      setNationalIdTouched(false);
                    }}
                    className="w-full sm:w-auto border border-oo-rule rounded px-3 py-2.5 text-oo-small focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue bg-oo-bg sm:bg-white"
                  >
                    {COUNTRY_OPTIONS.map(({ code, entry }) => (
                      <option key={code} value={code}>
                        {entry.countryName}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="flex-1">
                  <label
                    htmlFor="national-id-input"
                    className="block text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-2"
                  >
                    {RA_CODES[selectedCountry]?.idLabel ?? "Registration number"}
                  </label>
                  <input
                    id="national-id-input"
                    type="text"
                    value={nationalIdQuery}
                    onChange={(e) => setNationalIdQuery(e.target.value)}
                    onBlur={() => setNationalIdTouched(true)}
                    placeholder={RA_CODES[selectedCountry]?.placeholder ?? ""}
                    autoComplete="off"
                    spellCheck={false}
                    aria-label={RA_CODES[selectedCountry]?.idLabel ?? "Registration number"}
                    aria-describedby={!nationalIdFormatOk ? "national-id-format-warn" : undefined}
                    aria-invalid={!nationalIdFormatOk || undefined}
                    className={`w-full border rounded px-3 py-2.5 font-mono focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue ${
                      !nationalIdFormatOk
                        ? "border-amber-400 bg-amber-50/40"
                        : "border-oo-rule bg-oo-bg sm:bg-white"
                    }`}
                  />
                  {!nationalIdFormatOk && (
                    <p
                      id="national-id-format-warn"
                      role="status"
                      className="mt-1.5 text-oo-meta text-amber-700"
                    >
                      Format looks unexpected — expected {RA_CODES[selectedCountry]?.formatHint?.toLowerCase()}.
                      You can still search; GLEIF may store the number differently.
                    </p>
                  )}
                </div>
                <button
                  type="submit"
                  disabled={nationalIdSearchMutation.isPending || !nationalIdQuery.trim()}
                  aria-busy={nationalIdSearchMutation.isPending}
                  className="w-full sm:w-auto sm:flex-none bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                >
                  {nationalIdSearchMutation.isPending ? "Searching…" : "Search"}
                </button>
              </div>
            </form>

            {/* No aria-live here — the role="alert" children announce themselves */}
            <div>
              {nationalIdSearchMutation.isError && (
                <div role="alert" className="mt-4 bg-red-50 border border-red-200 text-red-800 rounded-oo p-3 text-sm">
                  {nationalIdSearchMutation.error?.message ?? "Search failed"}
                </div>
              )}
              {nationalIdSearchMutation.isSuccess && nationalIdSearchMutation.data.length === 0 && (
                <div role="alert" className="mt-4 bg-amber-50 border border-amber-200 text-amber-800 rounded-oo p-3 text-sm">
                  No LEI found for this registration number in GLEIF. The company may not have an LEI, or the number may be recorded differently.{" "}
                  <button
                    type="button"
                    onClick={() => {
                      nationalIdSearchMutation.reset();
                      setNationalIdQuery("");
                      setSearchMode("name");
                      onPicked();
                    }}
                    className="underline hover:no-underline"
                  >
                    Try searching by company name instead →
                  </button>
                </div>
              )}
            </div>

            {/* Phase 241: the count is announced once, by a status
                region that is always mounted. The container used to be
                \`aria-live\` itself, so it was inserted with its content —
                which some readers ignore and others read in full, every
                result button after the count. */}
            <p role="status" className="sr-only">
              {nationalIdSearchMutation.data && nationalIdSearchMutation.data.length > 1
                ? `${resultCount(nationalIdSearchMutation.data.length)} — select one to search it`
                : ""}
            </p>
            {nationalIdSearchMutation.data && nationalIdSearchMutation.data.length > 1 && (
              <div className="mt-4">
                <p className="text-[11px] font-semibold tracking-oo-eyebrow uppercase text-oo-muted mb-3">
                  {resultCount(nationalIdSearchMutation.data.length)} — select one to search it
                </p>
                <ul aria-label="Search results" className="divide-y divide-oo-rule border border-oo-rule rounded-oo overflow-hidden">
                  {nationalIdSearchMutation.data.map((r) => (
                    <li key={r.lei}>
                      <button
                        type="button"
                        aria-label={`Search ${r.legalName}, LEI ${r.lei}`}
                        onClick={() => {
                          nationalIdSearchMutation.reset();
                          setNationalIdQuery("");
                          onLookup(r.lei);
                          onPicked();
                        }}
                        className="w-full text-left px-4 py-3 hover:bg-oo-bg transition-colors focus:outline-none focus:ring-2 focus:ring-inset focus:ring-oo-blue/40"
                      >
                        <div className="font-head font-bold text-oo-body text-oo-ink leading-snug">
                          {r.legalName}
                        </div>
                        <div className="flex items-center gap-3 mt-1">
                          <span className="font-mono text-[11px] text-oo-blue">
                            {r.lei}
                          </span>
                          <span className="text-[11px] text-oo-muted">{r.country}</span>
                          <span
                            className={`text-[10px] font-mono px-1.5 py-0.5 rounded border ${
                              r.status === "ISSUED"
                                ? "bg-emerald-50 text-emerald-700 border-emerald-200"
                                : "bg-oo-bg text-oo-muted border-oo-rule"
                            }`}
                          >
                            {r.status}
                          </span>
                        </div>
                      </button>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}

        {/* ── LEI paste panel ── */}
        {searchMode === "lei" && (
          <form onSubmit={runLookup} noValidate id="panel-lei" role="tabpanel" aria-labelledby="tab-lei" className="p-4">
            <div className="flex flex-col sm:flex-row gap-3">
              <input
                id="lei-input"
                type="text"
                value={leiInput}
                onChange={(e) => {
                  setLeiInput(e.target.value);
                  if (leiInputError) setLeiInputError(null);
                }}
                placeholder="Paste a 20-character LEI"
                aria-invalid={leiInputError ? true : undefined}
                aria-describedby={leiInputError ? "lei-input-error" : undefined}
                spellCheck={false}
                autoComplete="off"
                aria-label="Legal Entity Identifier (20 characters)"
                pattern="[A-Za-z0-9]{20}"
                inputMode="text"
                className="flex-1 border border-oo-rule rounded px-3 py-2.5 font-mono uppercase tracking-wide bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue placeholder:font-sans placeholder:normal-case placeholder:tracking-normal"
                maxLength={20}
              />
              <button
                type="submit"
                disabled={lookupPending || !leiInput.trim()}
                aria-busy={lookupPending}
                className="w-full sm:w-auto bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
              >
                {lookupPending ? "Searching…" : "Search"}
              </button>
            </div>
            {leiInputError && (
              <p id="lei-input-error" role="alert" className="mt-2 text-oo-small text-oo-warn-text">
                {leiInputError}
              </p>
            )}
          </form>
        )}

        {/* ── Person search panel (TENTATIVE, Phase E) ── */}
        {searchMode === "person" && (
          <div id="panel-person" role="tabpanel" aria-labelledby="tab-person" className="p-4">
            <form
              onSubmit={(e) => {
                e.preventDefault();
                const name = personQuery.trim();
                if (name.length < 2) return;
                const by = Number(personBirthYear);
                onOpenPerson(
                  name,
                  Number.isInteger(by) && by >= 1900 && by <= 2100 ? by : undefined
                );
              }}
            >
              <div className="flex flex-col sm:flex-row gap-3">
                <input
                  id="person-input"
                  type="search"
                  value={personQuery}
                  onChange={(e) => setPersonQuery(e.target.value)}
                  placeholder="Search by person name"
                  autoComplete="off"
                  aria-label="Person name"
                  className="flex-1 border border-oo-rule rounded px-3 py-2.5 bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                />
                <input
                  type="text"
                  value={personBirthYear}
                  onChange={(e) => setPersonBirthYear(e.target.value)}
                  placeholder="Birth year (optional)"
                  inputMode="numeric"
                  pattern="[0-9]{4}"
                  maxLength={4}
                  aria-label="Birth year (optional, corroborates name matches)"
                  className="w-full sm:w-44 border border-oo-rule rounded px-3 py-2.5 bg-oo-bg sm:bg-white focus:outline-none focus:ring-2 focus:ring-oo-blue/30 focus:border-oo-blue"
                />
                <button
                  type="submit"
                  disabled={personQuery.trim().length < 2}
                  className="w-full sm:w-auto bg-oo-blue text-white rounded px-5 py-2.5 font-medium hover:bg-oo-burst transition-colors disabled:opacity-50"
                >
                  {PERSON_VERB}
                </button>
              </div>
            </form>
            <p className="text-[11px] text-oo-muted leading-[1.6] mt-3">
              Screens a person by name across every source that holds people
              (Companies House officers, OpenSanctions, EveryPolitician,
              Wikidata, OpenAleph) for PEP, sanctions and offshore-leaks
              signals. Name-based: results are potential matches with their
              evidence shown, never confirmed identities. Adding a birth year
              helps corroborate matches. Tip: for people connected to a
              company, the BackgroundCheck view on the company's report gives
              the same screening with role context attached.
            </p>
          </div>
        )}

        </div>

      </div>
    </>
  );
}
