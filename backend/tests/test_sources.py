"""Smoke tests for Phase 0 source adapter stubs."""

from __future__ import annotations

import pytest

from opencheck.config import get_settings
from opencheck.sources import REGISTRY, SearchKind


@pytest.fixture(autouse=True)
def _isolated_data_root(monkeypatch, tmp_path):
    """Point the cache at a tmp dir so the shipped demo fixtures
    don't shadow the stub path under test."""
    monkeypatch.setenv("OPENCHECK_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# Adapter modules that deliberately do NOT register in REGISTRY: bulk-data,
# offline, or auxiliary adapters that don't fit the live search/fetch flow.
# Removing an entry here means the module must be registered (or deleted).
_DELIBERATELY_UNREGISTERED = {
    "acra_singapore",   # data.gov.sg bulk CSV — needs scripts/extract_acra.py
    "bods_gleif",       # Open Ownership bulk BODS — wired via lookup pipeline only
    "bods_uk_psc",      # Open Ownership bulk BODS (UK PSC)
    "brightquery",      # paid source, not enabled
    "cyprus_drcor",     # bulk download only
    "opentender",       # retired from the registry
}


def test_every_adapter_module_is_registered() -> None:
    """Discovery replaces the old hand-maintained expected-source list:
    every module under opencheck/sources/ that defines a concrete
    SourceAdapter subclass must either be in REGISTRY or be explicitly
    listed in _DELIBERATELY_UNREGISTERED."""
    import importlib
    import inspect
    import pkgutil

    import opencheck.sources as sources_pkg
    from opencheck.sources.base import SourceAdapter

    adapter_modules: dict[str, type] = {}
    for mod_info in pkgutil.iter_modules(sources_pkg.__path__):
        if mod_info.ispkg or mod_info.name == "base":
            continue
        module = importlib.import_module(f"opencheck.sources.{mod_info.name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(obj, SourceAdapter)
                and obj is not SourceAdapter
                and not inspect.isabstract(obj)
                and obj.__module__ == module.__name__
            ):
                adapter_modules[mod_info.name] = obj

    registered_ids = set(REGISTRY.keys())
    for mod_name, cls in sorted(adapter_modules.items()):
        if mod_name in _DELIBERATELY_UNREGISTERED:
            continue
        adapter_id = getattr(cls, "id", None)
        assert adapter_id in registered_ids, (
            f"sources/{mod_name}.py defines {cls.__name__} (id={adapter_id!r}) "
            "but it is not in REGISTRY — register it in sources/__init__.py "
            "or add it to _DELIBERATELY_UNREGISTERED with a reason"
        )

    # Stale allowlist entries are errors too.
    for mod_name in _DELIBERATELY_UNREGISTERED:
        assert mod_name in adapter_modules, (
            f"_DELIBERATELY_UNREGISTERED lists {mod_name!r} but no such "
            "adapter module exists"
        )
        assert mod_name not in registered_ids, (
            f"{mod_name!r} is registered AND listed as deliberately "
            "unregistered — remove it from _DELIBERATELY_UNREGISTERED"
        )

    # Registry ids follow the module-name convention.
    module_names = set(adapter_modules)
    assert registered_ids <= module_names, (
        f"registry ids without a module: {sorted(registered_ids - module_names)}"
    )


def test_source_info_fields_are_populated() -> None:
    for adapter in REGISTRY.values():
        info = adapter.info
        assert info.id == adapter.id
        assert info.name
        assert info.homepage.startswith("http")
        assert info.license
        assert info.attribution
        assert info.supports, f"{adapter.id} declares no supported kinds"


# Adapters that are entered via a specific identifier (e.g. LEI, ocid) rather
# than free-text search. Their search() method intentionally returns [] because
# they are called directly via fetch() in the LEI-lookup flow (app.py).
_IDENTIFIER_KEYED = {"ariregister", "bolagsverket", "cnpj_brazil", "cvr_denmark", "firmenbuch", "krs_poland", "malta_mbr", "opencorporates", "inpi", "kvk", "rpo_slovakia", "sudreg_croatia", "zefix"}


@pytest.mark.parametrize(
    "source_id",
    [sid for sid in REGISTRY if sid not in _IDENTIFIER_KEYED],
)
async def test_adapter_search_returns_stubs_for_supported_kinds(source_id: str) -> None:
    adapter = REGISTRY[source_id]
    for kind in adapter.info.supports:
        hits = await adapter.search("Rosneft", kind)
        assert hits, f"{source_id} returned no stub hits for {kind}"
        for hit in hits:
            assert hit.source_id == source_id
            assert hit.is_stub is True
            assert hit.name


async def test_entity_only_adapter_rejects_person_search() -> None:
    # GLEIF is entity-only; EveryPolitician is person-only.
    gleif = REGISTRY["gleif"]
    assert await gleif.search("Alice Example", SearchKind.PERSON) == []

    ep = REGISTRY["everypolitician"]
    assert await ep.search("Acme Ltd", SearchKind.ENTITY) == []


async def test_fetch_returns_stub_payload() -> None:
    adapter = REGISTRY["companies_house"]
    payload = await adapter.fetch("00000000")
    assert payload["is_stub"] is True
    assert payload["source_id"] == "companies_house"
    assert payload["hit_id"] == "00000000"
