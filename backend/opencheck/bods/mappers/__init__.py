"""Per-source BODS mappers, one module per source family (Phase 168).

``bods/mapper.py`` was 10,824 lines, and every phase that adds a source
touches it. The sections were already banner-separated by source, so the
seam was drawn where the file itself had drawn it; ``mapper.py`` re-exports
each module's ``map_*`` so `opencheck.bods` and
``sources/probes.py``'s ``getattr(mapper, probe.bods_mapper)`` are unchanged.

The two largest sections moved first. The rest of ``mapper.py`` follows the
same shape and can move the same way.
"""
