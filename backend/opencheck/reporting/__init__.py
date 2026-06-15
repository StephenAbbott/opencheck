"""PDF report generation for OpenCheck entity profiles.

The report is built as accessible, semantic HTML and converted to a **tagged**
PDF (PDF/UA-1) with WeasyPrint, so the WCAG structure (headings, tables, lists,
image alt-text, language, document title) comes from the markup. Every fact in
the report is drawn from the lookup result and attributed to its source — the
same "nothing unprovable" rule as the narrative feature.

Modules:

- ``diagram``     — render one source's BODS relationships as a BOVS-styled
  print SVG (person/entity icons, ownership/control edge colours, interest
  labels) plus the rows for its text-equivalent table.
- ``html_report`` — assemble the full report HTML from a lookup result (+ an
  optional already-generated narrative).
- ``pdf``         — render the HTML to a tagged PDF (WeasyPrint, lazy import).
"""

from __future__ import annotations

from .diagram import SourceDiagram, source_diagram
from .html_report import build_report_html
from .pdf import PdfUnavailable, build_report_pdf, render_html_to_pdf

__all__ = [
    "SourceDiagram",
    "source_diagram",
    "build_report_html",
    "build_report_pdf",
    "render_html_to_pdf",
    "PdfUnavailable",
]
