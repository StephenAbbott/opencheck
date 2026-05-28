"""Phase 3 — Multi-source BODS bundle assembly audit.

This file documents the cross-source entity ID mismatch problem that causes
orphaned (floating) nodes in bods-dagre when the backend concatenates
statements from multiple adapters for the same company.

Root cause
----------
Each adapter mints its own ``statementId`` for the subject company using a
hash of its own source-specific keys (e.g. GLEIF uses the LEI; Companies
House uses the company number).  The backend assembles the final BODS bundle
with a plain ``bods_all.extend(bundle["bods"])`` — no deduplication or ID
normalisation.

The result for a UK company looked up by LEI:

  GLEIF:  entity(id=hash("gleif", "4695GQOBKGQ6GXMZ3J57"))
  CH:     entity(id=hash("ch",   "00102498"))   ← different ID, same company
          person(id=hash("ch", "PSC/john-smith"))
          relationship(subject=CH-entity-id, interestedParty=CH-person-id)

Combined bundle has:
  - 2 entity nodes for the same company (GLEIF's and CH's)
  - 1 person node (PSC)
  - 1 relationship: CH-entity → PSC

bods-dagre draws CH-entity connected to PSC, and GLEIF-entity as a
disconnected floating node.

Scenario with GLEIF parent
--------------------------
When GLEIF also has a parent relationship, the combined bundle contains:

  - GLEIF-child entity   (subject of GLEIF relationship)
  - GLEIF-parent entity  (interestedParty of GLEIF relationship)
  - CH-child entity      (subject of CH relationship, same company as GLEIF-child)
  - CH-PSC person        (interestedParty of CH relationship)

Two disconnected subgraphs:
  Subgraph A:  GLEIF-child → GLEIF-parent
  Subgraph B:  CH-child → CH-PSC-person

bods-dagre renders them as two unconnected clusters.

Tests in this file
------------------
Single-source baseline assertions confirm that each individual adapter
bundle is well-formed.  Multi-source assertions then document the current
(incorrect) assembly behaviour so that any improvement is immediately
caught and the expected state can be updated.

All tests PASS in the current codebase.  Tests prefixed with
``test_CURRENT_`` document a known gap; tests prefixed with
``test_EXPECTED_`` are marked ``xfail`` and will begin passing once the
cross-source normalisation fix (Fix 3) is applied.

Fix 3 (not yet implemented) lives in the assembler / reconciler layer and
should ensure that when GLEIF and a national registry both return the same
company, only one canonical entity statement survives in the combined bundle.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))

from bods_validation_helpers import (  # noqa: E402
    check_duplicate_entity_names,
    check_graph_connectivity,
    check_unreferenced_entities,
    connected_components,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_LEI = "4695GQOBKGQ6GXMZ3J57"
_CH_NUM = "00102498"
_COMPANY_NAME = "SUBSIDIARY LTD"


def _gleif_bundle_no_parent() -> dict:
    """GLEIF bundle for a company with no parent or children."""
    return {
        "lei": _LEI,
        "record": {
            "id": _LEI,
            "attributes": {
                "lei": _LEI,
                "entity": {
                    "legalName": {"name": _COMPANY_NAME},
                    "jurisdiction": "GB",
                    "registeredAs": _CH_NUM,
                    "registeredAt": {"id": "RA000585", "other": None},
                    "legalAddress": {
                        "addressLines": ["1 Test St"],
                        "city": "London",
                        "postalCode": "EC1A 1BB",
                        "country": "GB",
                    },
                },
            },
        },
        "direct_parent": None,
        "ultimate_parent": None,
        "direct_parent_exception": None,
        "ultimate_parent_exception": None,
        "direct_children": [],
    }


def _gleif_bundle_with_parent() -> dict:
    """GLEIF bundle for a company that has a direct parent."""
    bundle = _gleif_bundle_no_parent()
    bundle["direct_parent"] = {
        "id": "PARENTXXXXXXXXXX0001",
        "attributes": {
            "lei": "PARENTXXXXXXXXXX0001",
            "entity": {
                "legalName": {"name": "PARENT HOLDING LTD"},
                "jurisdiction": "GB",
            },
        },
    }
    return bundle


def _ch_bundle_with_psc() -> dict:
    """Companies House bundle for the same UK company, with one individual PSC."""
    return {
        "company_number": _CH_NUM,
        "profile": {
            "company_name": _COMPANY_NAME,
            "company_number": _CH_NUM,
            "type": "private-limited-company",
            "company_status": "active",
            "jurisdiction": "england-wales",
            "date_of_creation": "2000-01-01",
            "registered_office_address": {
                "address_line_1": "1 Test St",
                "locality": "London",
                "postal_code": "EC1A 1BB",
            },
        },
        "officers": {"items": [], "total_results": 0},
        "pscs": {
            "items": [
                {
                    "name": "JANE OWNER",
                    "kind": "individual-person-with-significant-control",
                    "natures_of_control": ["ownership-of-shares-75-to-100-percent"],
                    "notified_on": "2020-01-01",
                    "nationality": "British",
                    "date_of_birth": {"year": 1975, "month": 6},
                    "address": {
                        "address_line_1": "2 Lane",
                        "locality": "London",
                        "country": "England",
                    },
                }
            ],
            "total_results": 1,
        },
        "related_companies": {},
    }


def _ch_bundle_no_psc() -> dict:
    """Companies House bundle for the same UK company, with no PSCs."""
    bundle = _ch_bundle_with_psc()
    bundle["pscs"] = {"items": [], "total_results": 0}
    return bundle


# ---------------------------------------------------------------------------
# Helpers: run mappers, return statement lists
# ---------------------------------------------------------------------------

def _gleif_stmts(bundle: dict) -> list[dict]:
    from opencheck.bods.mapper import map_gleif
    return list(map_gleif(bundle))


def _ch_stmts(bundle: dict) -> list[dict]:
    from opencheck.bods.mapper import map_companies_house
    return list(map_companies_house(bundle))


# ===========================================================================
# Single-source baselines — each individual bundle must be fully connected
# ===========================================================================


def test_gleif_standalone_no_dangling_refs():
    """GLEIF-only bundle (no parent) has no dangling references."""
    stmts = _gleif_stmts(_gleif_bundle_no_parent())
    assert check_graph_connectivity(stmts) == []


def test_gleif_with_parent_standalone_no_dangling_refs():
    """GLEIF-only bundle (with parent relationship) has no dangling references."""
    stmts = _gleif_stmts(_gleif_bundle_with_parent())
    assert check_graph_connectivity(stmts) == []


def test_ch_with_psc_standalone_no_dangling_refs():
    """CH-only bundle (with PSC) has no dangling references."""
    stmts = _ch_stmts(_ch_bundle_with_psc())
    assert check_graph_connectivity(stmts) == []


def test_ch_no_psc_standalone_no_dangling_refs():
    """CH-only bundle (no PSC) has no dangling references."""
    stmts = _ch_stmts(_ch_bundle_no_psc())
    assert check_graph_connectivity(stmts) == []


def test_gleif_standalone_is_single_component():
    """A GLEIF-only bundle (no parent) is one component: just the entity node."""
    stmts = _gleif_stmts(_gleif_bundle_no_parent())
    comps = connected_components(stmts)
    assert len(comps) == 1


def test_gleif_with_parent_standalone_is_single_component():
    """GLEIF with parent produces one connected component (entity–rel–entity)."""
    stmts = _gleif_stmts(_gleif_bundle_with_parent())
    comps = connected_components(stmts)
    assert len(comps) == 1


def test_ch_with_psc_standalone_is_single_component():
    """CH with PSC produces one connected component (entity–rel–person)."""
    stmts = _ch_stmts(_ch_bundle_with_psc())
    comps = connected_components(stmts)
    assert len(comps) == 1


# ===========================================================================
# Multi-source: GLEIF (no parent) + CH (with PSC)
# ===========================================================================


class TestGleifNoPlusChWithPsc:
    """GLEIF entity-only + CH entity+PSC+relationship for the same company."""

    def _combined(self) -> list[dict]:
        return _gleif_stmts(_gleif_bundle_no_parent()) + _ch_stmts(_ch_bundle_with_psc())

    def test_no_dangling_references_in_combined_bundle(self):
        """Combined bundle has zero dangling references.

        Each relationship resolves to its *own* source's entity statement, so
        ``check_graph_connectivity`` passes even though the bundle is
        semantically incorrect (duplicate entity nodes).
        """
        issues = check_graph_connectivity(self._combined())
        assert issues == [], issues

    def test_CURRENT_combined_has_two_entity_nodes_for_same_company(self):
        """CURRENT BEHAVIOUR: combined bundle contains two entity nodes for the
        same company — one from GLEIF and one from CH.

        Fix 3 (cross-source ID normalisation) should reduce this to one.
        """
        stmts = self._combined()
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 2, (
            f"Expected 2 entity nodes (GLEIF + CH), got {len(entity_stmts)}"
        )

    def test_CURRENT_combined_has_duplicate_entity_name(self):
        """CURRENT BEHAVIOUR: check_duplicate_entity_names detects the duplication."""
        stmts = self._combined()
        dupes = check_duplicate_entity_names(stmts)
        assert len(dupes) == 1, f"Expected 1 duplicate name group, got {dupes}"
        name, ids = dupes[0]
        assert name == _COMPANY_NAME
        assert len(ids) == 2

    def test_CURRENT_gleif_entity_is_unreferenced_as_subject_or_ip(self):
        """CURRENT BEHAVIOUR: the GLEIF entity statement is orphaned — no
        relationship in the combined bundle references it as subject or
        interestedParty.

        This is the entity that bods-dagre renders as a floating node.
        """
        stmts = self._combined()
        gleif_entity_stmts = _gleif_stmts(_gleif_bundle_no_parent())
        gleif_entity_id = next(
            s["statementId"] for s in gleif_entity_stmts if s["recordType"] == "entity"
        )

        unreferenced = check_unreferenced_entities(stmts)
        unreferenced_ids = {uid for uid, _ in unreferenced}
        assert gleif_entity_id in unreferenced_ids, (
            f"Expected GLEIF entity {gleif_entity_id!r} to be unreferenced, "
            f"but unreferenced set was: {unreferenced_ids}"
        )

    def test_CURRENT_combined_has_two_disconnected_components(self):
        """CURRENT BEHAVIOUR: the combined bundle splits into two connected
        components — the isolated GLEIF entity, and the CH entity+PSC cluster.

        bods-dagre renders these as two disconnected clusters.
        """
        stmts = self._combined()
        comps = connected_components(stmts)
        assert len(comps) == 2, (
            f"Expected 2 components (GLEIF-entity island + CH cluster), "
            f"got {len(comps)}: {[sorted(c) for c in comps]}"
        )

    @pytest.mark.xfail(
        reason="Fix 3 (cross-source entity ID normalisation) not yet implemented",
        strict=True,
    )
    def test_EXPECTED_combined_has_single_entity_node_after_normalisation(self):
        """EXPECTED after Fix 3: combined bundle has exactly one entity node for
        the subject company, and the PSC relationship connects to it."""
        stmts = self._combined()
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 1

    @pytest.mark.xfail(
        reason="Fix 3 (cross-source entity ID normalisation) not yet implemented",
        strict=True,
    )
    def test_EXPECTED_combined_is_single_connected_component(self):
        """EXPECTED after Fix 3: combined bundle is one connected component."""
        stmts = self._combined()
        comps = connected_components(stmts)
        assert len(comps) == 1


# ===========================================================================
# Multi-source: GLEIF (with parent) + CH (with PSC)
# ===========================================================================


class TestGleifWithParentPlusChWithPsc:
    """GLEIF with parent ownership + CH with individual PSC for the same company.

    This produces the most problematic multi-source scenario: two entirely
    disconnected subgraphs.
    """

    def _combined(self) -> list[dict]:
        return (
            _gleif_stmts(_gleif_bundle_with_parent())
            + _ch_stmts(_ch_bundle_with_psc())
        )

    def test_no_dangling_references_in_combined_bundle(self):
        """No dangling references even with the parent relationship."""
        issues = check_graph_connectivity(self._combined())
        assert issues == [], issues

    def test_CURRENT_combined_has_three_entity_nodes(self):
        """CURRENT BEHAVIOUR: 3 entity nodes — GLEIF-child, GLEIF-parent,
        CH-child.  After Fix 3 there should be 2 (GLEIF-parent and the
        canonical child).
        """
        stmts = self._combined()
        entity_stmts = [s for s in stmts if s["recordType"] == "entity"]
        assert len(entity_stmts) == 3, (
            f"Expected 3 entity nodes, got {len(entity_stmts)}"
        )

    def test_CURRENT_combined_has_two_relationships(self):
        """CURRENT BEHAVIOUR: 2 relationship statements — GLEIF parent
        ownership and CH PSC — both valid individually."""
        stmts = self._combined()
        rel_stmts = [s for s in stmts if s["recordType"] == "relationship"]
        assert len(rel_stmts) == 2

    def test_CURRENT_combined_has_two_disconnected_subgraphs(self):
        """CURRENT BEHAVIOUR: two disconnected components in the combined graph.

        Component A: GLEIF-child ← GLEIF-relationship → GLEIF-parent
        Component B: CH-child ← CH-relationship → CH-PSC-person

        The child company appears in both subgraphs with different IDs.
        """
        stmts = self._combined()
        comps = connected_components(stmts)
        assert len(comps) == 2, (
            f"Expected 2 disconnected subgraphs, got {len(comps)}: "
            f"{[sorted(c) for c in comps]}"
        )

    def test_CURRENT_ch_child_entity_is_unreferenced_in_gleif_subgraph(self):
        """CURRENT BEHAVIOUR: the CH entity for the subject company is not
        reachable from the GLEIF parent — there is no edge connecting the CH
        child node to the GLEIF parent node."""
        gleif_stmts = _gleif_stmts(_gleif_bundle_with_parent())
        ch_only_stmts = _ch_stmts(_ch_bundle_with_psc())

        # The GLEIF-child entity statementId
        gleif_child_id = next(
            s["statementId"]
            for s in gleif_stmts
            if s["recordType"] == "entity"
            and (s.get("recordDetails") or {}).get("name") == _COMPANY_NAME
        )
        # The CH-child entity statementId
        ch_child_id = next(
            s["statementId"]
            for s in ch_only_stmts
            if s["recordType"] == "entity"
        )

        assert gleif_child_id != ch_child_id, (
            "GLEIF and CH entity IDs should differ (no normalisation applied yet)"
        )

    def test_CURRENT_duplicate_entity_name_is_detected(self):
        """CURRENT BEHAVIOUR: check_duplicate_entity_names detects the
        SUBSIDIARY LTD duplication across GLEIF and CH."""
        stmts = self._combined()
        dupes = check_duplicate_entity_names(stmts)
        names = [name for name, _ in dupes]
        assert _COMPANY_NAME in names

    @pytest.mark.xfail(
        reason="Fix 3 (cross-source entity ID normalisation) not yet implemented",
        strict=True,
    )
    def test_EXPECTED_combined_is_single_connected_component_after_fix3(self):
        """EXPECTED after Fix 3: one connected graph spanning GLEIF parent,
        the canonical child, the CH PSC person, and both relationships."""
        stmts = self._combined()
        comps = connected_components(stmts)
        assert len(comps) == 1


# ===========================================================================
# Multi-source: GLEIF (no parent) + CH (no PSC)
# ===========================================================================


class TestGleifNoPlusChNoPsc:
    """Degenerate case: GLEIF entity-only + CH entity-only, no relationships.

    Both sources produce only one entity statement each.  No relationships
    exist in either source, so the combined bundle has two isolated entity
    nodes — one for each source — plus no edges.  bods-dagre renders two
    floating nodes.
    """

    def _combined(self) -> list[dict]:
        return _gleif_stmts(_gleif_bundle_no_parent()) + _ch_stmts(_ch_bundle_no_psc())

    def test_no_dangling_refs(self):
        issues = check_graph_connectivity(self._combined())
        assert issues == [], issues

    def test_CURRENT_two_entity_nodes_both_unreferenced(self):
        """CURRENT BEHAVIOUR: both entity nodes are unreferenced (no
        relationships exist)."""
        stmts = self._combined()
        unreferenced = check_unreferenced_entities(stmts)
        assert len(unreferenced) == 2

    def test_CURRENT_two_isolated_components(self):
        """CURRENT BEHAVIOUR: two singleton components — the GLEIF entity and
        the CH entity — neither connected to anything."""
        stmts = self._combined()
        comps = connected_components(stmts)
        assert len(comps) == 2


# ===========================================================================
# Helper correctness unit tests
# ===========================================================================


class TestHelpers:
    """Unit tests for the new multi-source helpers."""

    def _make_entity(self, sid: str, name: str) -> dict:
        return {
            "statementId": sid,
            "recordId": sid,
            "recordType": "entity",
            "recordDetails": {"name": name, "entityType": {"type": "registeredEntity"}},
        }

    def _make_person(self, sid: str, name: str) -> dict:
        return {
            "statementId": sid,
            "recordId": sid,
            "recordType": "person",
            "recordDetails": {"names": [{"fullName": name}]},
        }

    def _make_rel(self, sid: str, subject: str, ip: str) -> dict:
        return {
            "statementId": sid,
            "recordId": sid,
            "recordType": "relationship",
            "recordDetails": {"subject": subject, "interestedParty": ip, "interests": []},
        }

    def test_check_unreferenced_entities_empty_when_all_connected(self):
        entity = self._make_entity("e1", "Corp A")
        person = self._make_person("p1", "Jane")
        rel = self._make_rel("r1", "e1", "p1")
        assert check_unreferenced_entities([entity, person, rel]) == []

    def test_check_unreferenced_entities_returns_isolated_entity(self):
        entity = self._make_entity("e1", "Corp A")
        assert check_unreferenced_entities([entity]) == [("e1", "Corp A")]

    def test_check_unreferenced_entities_ip_also_counts_as_referenced(self):
        """An entity appearing as interestedParty is referenced."""
        parent = self._make_entity("e_parent", "Parent Corp")
        child = self._make_entity("e_child", "Child Corp")
        rel = self._make_rel("r1", "e_child", "e_parent")
        unreferenced = check_unreferenced_entities([parent, child, rel])
        # Both entity nodes are referenced (one as subject, one as ip)
        assert unreferenced == []

    def test_check_duplicate_entity_names_empty_when_unique(self):
        a = self._make_entity("e1", "Alpha Corp")
        b = self._make_entity("e2", "Beta Corp")
        assert check_duplicate_entity_names([a, b]) == []

    def test_check_duplicate_entity_names_detects_duplicate(self):
        a = self._make_entity("e1", "Same Corp")
        b = self._make_entity("e2", "Same Corp")
        dupes = check_duplicate_entity_names([a, b])
        assert len(dupes) == 1
        name, ids = dupes[0]
        assert name == "Same Corp"
        assert set(ids) == {"e1", "e2"}

    def test_connected_components_single_component(self):
        entity = self._make_entity("e1", "Corp")
        person = self._make_person("p1", "Jane")
        rel = self._make_rel("r1", "e1", "p1")
        comps = connected_components([entity, person, rel])
        assert len(comps) == 1
        assert frozenset({"e1", "p1", "r1"}) in comps

    def test_connected_components_two_isolated_entities(self):
        a = self._make_entity("e1", "Corp A")
        b = self._make_entity("e2", "Corp B")
        comps = connected_components([a, b])
        assert len(comps) == 2

    def test_connected_components_two_subgraphs(self):
        """Two disconnected entity-person-relationship clusters → 2 components."""
        e1 = self._make_entity("e1", "Corp A")
        p1 = self._make_person("p1", "Alice")
        r1 = self._make_rel("r1", "e1", "p1")
        e2 = self._make_entity("e2", "Corp B")
        p2 = self._make_person("p2", "Bob")
        r2 = self._make_rel("r2", "e2", "p2")
        comps = connected_components([e1, p1, r1, e2, p2, r2])
        assert len(comps) == 2
