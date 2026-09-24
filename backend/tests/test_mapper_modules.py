"""The per-source mapper modules (Phase 246).

``bods/mapper.py`` grew from 8,512 lines (Phase 168) back to 11,144 by Phase
246, one new country at a time, because it was where the last source's mapper
was. Phase 246 moved every per-source section to ``bods/mappers/<country or
source>.py``. These tests keep it that way, and keep the address everything
imports from — ``opencheck.bods.mapper`` — pointing at the same objects.
"""

from __future__ import annotations

import ast
import importlib
import pkgutil
from pathlib import Path

import opencheck.bods.mapper as mapper
import opencheck.bods.mappers as mappers_pkg

_MAPPERS_DIR = Path(mappers_pkg.__file__).parent

#: The only source mappers that stay in ``mapper.py``: GLEIF, the anchor every
#: lookup starts from, and the three passthroughs of published BODS.
_STAY_IN_MAPPER = frozenset(
    {"map_gleif", "map_gleif_subsidiaries", "map_bods_gleif", "map_bods_uk_psc", "map_meip"}
)


def _modules():
    for info in pkgutil.iter_modules(mappers_pkg.__path__):
        yield info.name, importlib.import_module(f"opencheck.bods.mappers.{info.name}")


def test_a_new_source_mapper_goes_in_its_own_module() -> None:
    tree = ast.parse(Path(mapper.__file__).read_text(encoding="utf-8"))
    defined = {
        n.name for n in tree.body if isinstance(n, ast.FunctionDef) and n.name.startswith("map_")
    }
    assert defined <= _STAY_IN_MAPPER, (
        f"{sorted(defined - _STAY_IN_MAPPER)} defined in bods/mapper.py — put a new "
        "source's mapper in bods/mappers/<country>.py and re-export it from mapper.py"
    )


#: Phase 168's two modules re-export a chosen list (every ``map_*`` and the
#: helpers something imported at the time), not every name.
_PARTIAL_RE_EXPORT = frozenset({"ftm", "wikidata"})


def test_every_module_name_is_re_exported_from_mapper_as_the_same_object() -> None:
    missing = []
    for mod_name, module in _modules():
        tree = ast.parse(Path(module.__file__).read_text(encoding="utf-8"))
        for node in tree.body:
            names = []
            if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                names = [node.name]
            elif isinstance(node, ast.Assign):
                names = [t.id for t in node.targets if isinstance(t, ast.Name)]
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                names = [node.target.id]
            for name in names:
                if mod_name in _PARTIAL_RE_EXPORT and not name.startswith("map_"):
                    continue
                if getattr(mapper, name, None) is not getattr(module, name):
                    missing.append(f"{mod_name}.{name}")
    assert not missing, f"not re-exported from bods/mapper.py: {missing}"


def test_no_mapper_module_imports_mapper() -> None:
    """``mapper.py`` imports every module here, so an import back is a cycle."""
    offenders = []
    for path in sorted(_MAPPERS_DIR.glob("*.py")):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom) and node.level >= 2:
                if node.module == "mapper" or (
                    node.module is None and any(a.name == "mapper" for a in node.names)
                ):
                    offenders.append(path.name)
    assert not offenders, f"bods/mappers modules importing bods.mapper: {offenders}"
