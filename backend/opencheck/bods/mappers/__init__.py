"""Per-source BODS mappers, one module per country or source family.

``bods/mapper.py`` was 10,824 lines in Phase 168, when the two largest
sections (FtM, Wikidata) moved here, and had regrown to 11,144 by Phase 246,
when every other per-source section followed: one module per country
(``estonia.py``, ``romania.py``, ``us_dc.py`` …) or per non-register source
(``climatetrace.py``, ``eiti.py``, ``sec_edgar.py`` …). The seams are the
banners the file had already drawn. ``mapper.py`` keeps GLEIF and the
passthroughs and re-exports every name defined here, private helpers
included, so ``opencheck.bods``, the 140-odd ``from opencheck.bods.mapper
import …`` sites and ``sources/probes.py``'s ``getattr(mapper,
probe.bods_mapper)`` are unchanged.

**A new source's mapper goes in its own module here**, not in ``mapper.py``:
add ``map_<source_id>`` to a new ``mappers/<country>.py`` and re-export it
from ``mapper.py`` (and ``bods/__init__.py``). A module imports only from
``..statements`` and its siblings, never from ``..mapper`` — ``mapper.py``
imports every module here, so that would be a cycle.
"""
