"""Render the report HTML to a tagged PDF (PDF/UA-1) with WeasyPrint.

WeasyPrint is imported lazily and declared optional, so the rest of the backend
runs without it; the ``/export/pdf`` route raises a clear 503 when it's absent.
"""

from __future__ import annotations

from typing import Any

from .html_report import build_report_html


class PdfUnavailable(RuntimeError):  # noqa: N818 — public name, kept stable
    """Raised when the PDF toolchain (WeasyPrint) is not installed."""


def render_html_to_pdf(html: str, *, base_url: str = ".") -> bytes:
    try:
        from weasyprint import HTML
    except Exception as exc:  # ImportError or missing system libs
        raise PdfUnavailable(
            "PDF generation is unavailable — WeasyPrint (and Pango/Cairo) is not installed."
        ) from exc
    return HTML(string=html, base_url=base_url).write_pdf(pdf_variant="pdf/ua-1")


def build_report_pdf(report: dict[str, Any], *, narrative: dict[str, Any] | None = None) -> bytes:
    """Build the report HTML for a lookup result and render it to a tagged PDF."""
    html = build_report_html(report, narrative=narrative)
    return render_html_to_pdf(html)
