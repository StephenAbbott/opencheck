import { useState } from "react";
import { useMutation } from "@tanstack/react-query";
import { searchByNationalId, type GleifSearchResult } from "../lib/gleifNationalId";
import { validateNationalId } from "../lib/raCodes";

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
  // Queries GLEIF's public API by legal name. Returns a list of matching
  // entities for the user to pick from; selection hands off to lookupMutation.
  const nameSearchMutation = useMutation<GleifSearchResult[], Error, string>({
    mutationFn: async (q: string) => {
      const url =
        `https://api.gleif.org/api/v1/lei-records` +
        `?filter[entity.legalName]=${encodeURIComponent(q)}&page[size]=10`;
      const resp = await fetch(url, { headers: { Accept: "application/vnd.api+json" } });
      if (!resp.ok) throw new Error(`GLEIF API returned ${resp.status}`);
      const json = await resp.json();
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return (json.data ?? []).map((item: any) => {
        const attrs = item.attributes ?? {};
        const entity = attrs.entity ?? {};
        const reg = attrs.registration ?? {};
        return {
          lei: attrs.lei as string,
          legalName:
            (entity.legalName?.name as string) ??
            (entity.legalName as string) ??
            attrs.lei,
          country: entity.legalAddress?.country ?? "—",
          status: reg.status ?? "—",
        } satisfies GleifSearchResult;
      });
    },
  });

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
    reset,
  };
}

export type SearchForm = ReturnType<typeof useSearchForm>;
