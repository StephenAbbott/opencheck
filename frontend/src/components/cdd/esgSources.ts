/**
 * Who published the data on each Climate & ESG card, and under what licence.
 *
 * Its own module rather than a const inside `EsgPanel` so that a card living in
 * its own file (`EitiAssessmentCard`) can name its source without importing the
 * panel that renders it — which would be a cycle, and would pull the whole panel
 * into that card's test.
 */
export interface EsgSourceMeta {
  org: string;
  href: string;
  licence: string;
}

export const ESG_SOURCE_META: Record<string, EsgSourceMeta> = {
  climatetrace: {
    org: "Global Energy Monitor · Climate TRACE",
    href: "https://climatetrace.org/",
    licence: "CC BY 4.0",
  },
  eiti: {
    org: "EITI International Secretariat",
    href: "https://eiti.org/",
    licence: "open data, attribution",
  },
  eiti_assessment: {
    // Distinct from `eiti` above on purpose: both are the EITI Secretariat, and
    // two tiles labelled with the same organisation are two tiles a reader
    // cannot tell apart. The full attribution is in the card footer.
    org: "EITI · Company Assessment",
    href: "https://eiti-database.eiti.org/",
    licence: "EITI content-use policy, attribution",
  },
  wikirate: {
    org: "Wikirate",
    href: "https://wikirate.org/",
    licence: "CC BY 4.0",
  },
};
