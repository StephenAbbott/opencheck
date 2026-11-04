import { useEffect, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { searchByNationalId, type GleifSearchResult } from "../lib/gleifNationalId";
import { searchGleifByName, type NameSearchResult, type SearchCursor } from "../lib/gleifNameSearch";
import { fetchLegalForms } from "../lib/api";
import type { EntityFilters, LegalFormList } from "../lib/searchFilters";
import { validateNationalId } from "../lib/raCodes";

/** A name search as the panel shows it: the page(s) so far, the totals from
 *  the first page, and what was asked, so "Show the next 10" asks the same. */
export interface NameSearchState extends NameSearchResult {
  query: string;
  filters: EntityFilters;
}

interface NameSearchVars {
  q: string;
  filters: EntityFilters;
  cursor?: SearchCursor | null;
  previous?: NameSearchState;
}

/*
 * The homepage search panel's state: which tab is open, what is typed in each
 * field, and the two GLEIF searches (by name, by national ID). Moved out of
 * App() in Phase 246. It is a hook rather than state inside SearchPanel
 * because App reads and resets it too — the header field runs a name search,
 * the homepage hides its examples while a search has results, a lookup writes
 * the LEI field, and "back to the homepage" clears the lot.
 */

export type SearchMode = "name" | "nationalId" | "lei" | "person";

export function useSearchForm() {
  const [leiInput, setLeiInput] = useState("");
  // Phase 241: the app's own message for a malformed LEI, in place of the
  // browser's `pattern` tooltip — which is unstyleable, vanishes on its own
  // and is announced inconsistently (the form is `noValidate`).
  const [leiInputError, setLeiInputError] = useState<string | null>(null);
  // Three-mode search: "name" = GLEIF name search; "nationalId" = registration
  // number reverse lookup; "lei" = paste LEI directly.
  // TENTATIVE (Phase E): the "person" tab is under evaluation — Stephen's
  // instinct is to keep person search as a follow-on from entity pages.
  // It is deliberately isolated in its own commit for a clean revert.
  const [searchMode, setSearchMode] = useState<SearchMode>("name");
  const [nameQuery, setNameQuery] = useState("");
  // TENTATIVE person tab inputs (Phase E — see searchMode note above).
  const [personQuery, setPersonQuery] = useState("");
  const [personBirthYear, setPersonBirthYear] = useState("");
  const [nationalIdQuery, setNationalIdQuery] = useState("");
  // ISO 3166-1 alpha-2 country code for the national ID tab; defaults to UK.
  const [selectedCountry, setSelectedCountry] = useState("GB");
  // Tracks whether the national ID input has been blurred at least once.
  // Format warnings are suppressed until the field is touched so they don't
  // fire on every keystroke while the user is still typing.
  const [nationalIdTouched, setNationalIdTouched] = useState(false);

  // ── Name-search mutation ──────────────────────────────────────────────────
  // Queries GLEIF's public API on `entity.names` — every name a record
  // carries, former legal names included (Phase 309: `entity.legalName`
  // could not find a renamed company under its old name at all). The
  // advanced-search filters ride on the same request (`lib/gleifNameSearch`),
  // which also brings back GLEIF's total and, for a jurisdiction code GLEIF
  // also matches inside a US state or Canadian province, makes it exact.
  const [filters, setFiltersState] = useState<EntityFilters>({});
  const [filtersOpen, setFiltersOpen] = useState(false);
  const nameSearchMutation = useMutation<NameSearchState, Error, NameSearchVars>({
    mutationFn: async ({ q, filters: f, cursor, previous }) => {
      const page = await searchGleifByName(q, f, cursor ?? null);
      if (!previous) return { ...page, query: q, filters: f };
      return {
        ...page,
        results: [...previous.results, ...page.results],
        total: previous.total,
        rawTotal: previous.rawTotal,
        recordsChecked: previous.recordsChecked + page.recordsChecked,
        query: q,
        filters: f,
      };
    },
  });

  /** Run a company-name search with the filters as they stand. */
  function runNameSearch(q: string) {
    nameSearchMutation.mutate({ q, filters });
  }

  /** "Show the next 10" / "Keep looking": continue the search on screen. */
  function loadMoreNames() {
    const current = nameSearchMutation.data;
    if (!current?.next) return;
    nameSearchMutation.mutate({
      q: current.query,
      filters: current.filters,
      cursor: current.next,
      previous: current,
    });
  }

  /** Change the filters; a search already on screen re-runs under them, so a
   *  removed chip widens the results the reader is looking at. */
  function setFilters(next: EntityFilters) {
    setFiltersState(next);
    const current = nameSearchMutation.data;
    if (current) nameSearchMutation.mutate({ q: current.query, filters: next });
  }

  // The legal forms of the chosen jurisdiction, for the picker and the chips.
  const [legalForms, setLegalForms] = useState<LegalFormList | null>(null);
  const [legalFormsLoading, setLegalFormsLoading] = useState(false);
  const [legalFormsFailed, setLegalFormsFailed] = useState(false);
  useEffect(() => {
    const jurisdiction = filters.jurisdiction;
    if (!jurisdiction) {
      setLegalForms(null);
      setLegalFormsFailed(false);
      return;
    }
    let live = true;
    setLegalFormsLoading(true);
    setLegalFormsFailed(false);
    fetchLegalForms(jurisdiction)
      .then((list) => {
        if (live) setLegalForms(list);
      })
      .catch(() => {
        if (live) {
          setLegalForms(null);
          setLegalFormsFailed(true);
        }
      })
      .finally(() => {
        if (live) setLegalFormsLoading(false);
      });
    return () => {
      live = false;
    };
  }, [filters.jurisdiction]);

  // ── National-ID search mutation ──────────────────────────────────────────
  // Queries GLEIF's three registration-ID filter fields in parallel using
  // the RA code for the selected country. On single result, auto-navigates;
  // on multiple results, shows the same picker as the name search.
  const nationalIdSearchMutation = useMutation<
    GleifSearchResult[],
    Error,
    { raCode: string; id: string }
  >({
    mutationFn: ({ raCode, id }) => searchByNationalId(raCode, id),
  });

  // Only show the national-ID format warning after the field has been blurred
  // (touched) so partial input during typing doesn't trigger an amber state.
  const nationalIdFormatOk =
    !nationalIdTouched || validateNationalId(selectedCountry, nationalIdQuery);

  /** Back to an empty panel on the name tab — part of App's `resetToHome`. */
  function reset() {
    nameSearchMutation.reset();
    nationalIdSearchMutation.reset();
    setLeiInput("");
    setNameQuery("");
    setNationalIdQuery("");
    setSelectedCountry("GB");
    setNationalIdTouched(false);
    setSearchMode("name");
    setFiltersState({});
    setFiltersOpen(false);
  }

  return {
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
    nationalIdTouched,
    setNationalIdTouched,
    nationalIdFormatOk,
    nameSearchMutation,
    nationalIdSearchMutation,
    runNameSearch,
    loadMoreNames,
    filters,
    setFilters,
    filtersOpen,
    setFiltersOpen,
    legalForms,
    legalFormsLoading,
    legalFormsFailed,
    reset,
  };
}

export type SearchForm = ReturnType<typeof useSearchForm>;
