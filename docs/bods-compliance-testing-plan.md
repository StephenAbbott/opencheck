# BODS Compliance & Visualisation Testing Plan

**OpenCheck — Phase 43+**

This document is a methodical plan for verifying that every OpenCheck adapter produces BODS v0.4-compliant data and that all ownership graphs render correctly in bods-dagre with no floating nodes.

---

## Background: the two failure modes we are fixing

### 1. BODS schema non-compliance

The `make_relationship_statement` helper always sets `isComponent: False` on all entity and person statements, and never produces a `componentRecords` array on primary relationship statements. For indirect beneficial ownership chains — where A controls B through an intermediary C — BODS v0.4 requires:

- The intermediary entity C to carry `isComponent: True` in its `recordDetails`
- The primary relationship statement (A → B) to carry `componentRecords: [<C's recordId>, ...]`
- Component statements to be ordered before the primary relationship in the output

### 2. Floating nodes in bods-dagre

The `BODSGraph.tsx` sanitiser drops any relationship statement whose `subject` or `interestedParty` does not match a `recordId` present in the same bundle. This produces floating (disconnected) person and entity nodes. The causes are:

- **Cross-source ID mismatch**: GLEIF and Companies House each emit an entity statement for the same company with different `recordId`s. A CH relationship whose `subject` references the CH entity's `recordId` will not connect to the GLEIF entity node.
- **Missing upstream statement**: Some mappers emit a relationship but the corresponding person or entity statement is absent from the bundle (e.g., due to a data gap or an early-exit guard).
- **Object-format `interestedParty`**: Any v0.3-style `{ "describedByPersonStatement": "..." }` wrapper is treated as an "Unspecified" node by bods-dagre rather than connecting to the referenced statement.

---

## Adapter output matrix

The 16 active national registry adapters fall into two tiers:

**Tier 1 — Entity-only** (no person or relationship statements emitted):

| Adapter | Reason |
|---|---|
| `kvk` | Open-data API carries no personal data |
| `jar_lithuania` | BO data in JANGIS, restricted to legitimate-interest access |
| `krs_poland` | Public API masks personal names (Ł*******) |
| `cro` | Company-level data only |
| `bolagsverket` | Entity data only at current integration level |
| `bce_belgium` | Entity data only |
| `rpo_slovakia` | Entity data only |
| `acra_singapore` | Company data from open CSV dataset |
| `cvr_denmark` | CVR Datafordeler does not expose personal BO data publicly |

**Tier 2 — Full (entity + person + relationship)**:

| Adapter | Persons | Relationships | Notes |
|---|---|---|---|
| `companies_house` | PSC persons/entities; director persons | PSC OoC relationships; director boardMember relationships | UK corporate PSC chains via `related_companies` |
| `gleif` | None | Entity → entity (parent/child/exception) | `beneficialOwnershipOrControl: False` on all |
| `inpi` | Dirigeants (legal reps) | Dirigeant → entity relationships | BO records MUST be excluded (Loi Sapin II) |
| `brreg` | UBO/shareholder persons | Ownership relationships | Norway Brønnøysund |
| `ur_latvia` | Shareholders + board members | Shareholding + seniorManagingOfficial | Latvian Commercial Register |
| `firmenbuch` | Shareholders + directors | Shareholding + seniorManagingOfficial | Austrian Firmenbuch |
| `ariregister` | Shareholders + board members | Shareholding + seniorManagingOfficial | Estonian Business Register |
| `corporations_canada` | Directors | boardMember relationships | Canada Corporations Act |

---

## Phase 1 — Baseline audit: per-adapter BODS output

**Goal**: Capture the actual statements each mapper produces for realistic fixtures and document the current state before any fixes.

**Method**: For each Tier 2 adapter, run its `map_*()` function against the existing test fixtures and inspect the resulting statement list.

```python
# Example audit script — run for each adapter
from opencheck.bods import map_companies_house, validate_shape
from opencheck.bods.validator import _check_graph_integrity  # or equivalent

bundle = <realistic fixture>
stmts = list(map_companies_house(bundle))

# Print statement types and IDs
for s in stmts:
    print(s["recordType"], s["statementId"], s.get("recordDetails", {}).get("isComponent"))

# Validate graph integrity
issues = validate_shape(stmts)
print(issues)
```

**Checklist per adapter**:

- [ ] What `recordType`s are produced and in what order?
- [ ] Are all `statementId`s unique within the bundle?
- [ ] Do all `subject` and `interestedParty` fields in relationship statements match a `statementId` present in the bundle?
- [ ] Are `isComponent` values correct (True for intermediaries, False for principals)?
- [ ] Are `componentRecords` present where needed?
- [ ] Does `beneficialOwnershipOrControl` match the intent of each interest?
- [ ] Are interest `type` values valid v0.4 codelist members?

---

## Phase 2 — Schema validation layer

**Goal**: Add lib-cove-bods validation to the CI test suite so any mapper regression is caught immediately.

### 2a. Install lib-cove-bods

```bash
pip install libcovebods --break-system-packages
```

### 2b. Add a shared BODS validator fixture

Create `backend/tests/bods_validation_helpers.py`:

```python
"""Shared helpers for BODS v0.4 compliance checks."""
from __future__ import annotations
import json
import subprocess
import tempfile
import pathlib
from typing import Iterable

def validate_with_cove(statements: Iterable[dict]) -> list[str]:
    """Run lib-cove-bods CLI against a statement bundle and return any errors."""
    stmts = list(statements)
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False) as f:
        json.dump(stmts, f)
        tmp_path = f.name
    result = subprocess.run(
        ["libcovebods", tmp_path],
        capture_output=True, text=True
    )
    errors = []
    for line in result.stdout.splitlines():
        if "error" in line.lower() or "invalid" in line.lower():
            errors.append(line.strip())
    return errors


def check_graph_connectivity(statements: Iterable[dict]) -> list[str]:
    """Assert every relationship's subject/interestedParty references an in-bundle recordId."""
    stmts = list(statements)
    known_ids = {s["statementId"] for s in stmts} | {s.get("recordId", "") for s in stmts}
    issues = []
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        subject = rd.get("subject")
        ip = rd.get("interestedParty")
        rel_id = s["statementId"]
        
        # Check subject
        if isinstance(subject, str):
            if subject not in known_ids:
                issues.append(f"Relationship {rel_id}: subject '{subject}' not in bundle")
        elif isinstance(subject, dict):
            ref = subject.get("describedByEntityStatement") or subject.get("describedByPersonStatement")
            if ref and ref not in known_ids:
                issues.append(f"Relationship {rel_id}: subject ref '{ref}' not in bundle (also: v0.3 object format)")
        
        # Check interestedParty
        if isinstance(ip, str):
            if ip not in known_ids:
                issues.append(f"Relationship {rel_id}: interestedParty '{ip}' not in bundle")
        elif isinstance(ip, dict):
            ref = ip.get("describedByPersonStatement") or ip.get("describedByEntityStatement")
            if ref and ref not in known_ids:
                issues.append(f"Relationship {rel_id}: interestedParty ref '{ref}' not in bundle (also: v0.3 object format)")
    return issues


def check_iscomponent_consistency(statements: Iterable[dict]) -> list[str]:
    """Check isComponent flags against componentRecords on primary relationships."""
    stmts = list(statements)
    issues = []
    
    # Collect all recordIds marked isComponent: True
    component_ids = {
        s["statementId"]
        for s in stmts
        if (s.get("recordDetails") or {}).get("isComponent") is True
    }
    
    # Every primary relationship should list its components
    for s in stmts:
        if s.get("recordType") != "relationship":
            continue
        rd = s.get("recordDetails") or {}
        if rd.get("isComponent"):
            continue  # component relationship — skip
        # If there are isComponent entities in the bundle, this primary relationship
        # should carry componentRecords
        if component_ids and "componentRecords" not in rd:
            issues.append(
                f"Primary relationship {s['statementId']} is missing componentRecords "
                f"but bundle contains isComponent entities: {component_ids}"
            )
    return issues
```

### 2c. Add BODS validation assertions to every existing mapper test

For each `test_bods_<adapter>.py`, add:

```python
from tests.bods_validation_helpers import validate_with_cove, check_graph_connectivity

def test_<adapter>_cove_validation(fixture):
    stmts = list(map_<adapter>(fixture))
    errors = validate_with_cove(stmts)
    assert errors == [], errors

def test_<adapter>_graph_connectivity(fixture):
    stmts = list(map_<adapter>(fixture))
    issues = check_graph_connectivity(stmts)
    assert issues == [], issues
```

The existing `validate_shape()` in `opencheck/bods/validator.py` already does in-bundle reference checking — the new `check_graph_connectivity` helper should be compared against it to ensure they are equivalent.

---

## Phase 3 — Graph connectivity test suite

**Goal**: A dedicated test file that exercises every Tier 2 mapper and asserts no floating nodes, using the helpers from Phase 2.

Create `backend/tests/test_bods_graph_integrity.py`:

```python
"""
Cross-adapter graph connectivity tests.

Each test verifies that every relationship statement produced by a mapper
references entity/person statements present in the same bundle, guaranteeing
that bods-dagre will draw connected edges rather than floating nodes.
"""
import pytest
from opencheck.bods import (
    map_companies_house, map_gleif, map_brreg, map_inpi,
    map_ur_latvia, map_firmenbuch, map_ariregister,
    map_corporations_canada,
)
from tests.bods_validation_helpers import check_graph_connectivity, check_iscomponent_consistency
from tests.fixtures import (  # import existing fixtures
    CH_BUNDLE_DIRECT_PSC, CH_BUNDLE_CORPORATE_PSC, CH_BUNDLE_INDIVIDUAL_PSC,
    GLEIF_BUNDLE_WITH_PARENT, GLEIF_BUNDLE_WITH_CHILDREN,
    BRREG_BUNDLE_WITH_SHAREHOLDER,
    UR_LATVIA_BUNDLE_WITH_SHAREHOLDERS,
    FIRMENBUCH_BUNDLE_WITH_PERSONS,
    ARIREGISTER_BUNDLE_WITH_SHAREHOLDERS,
    CORPORATIONS_CANADA_BUNDLE_WITH_DIRECTOR,
)

@pytest.mark.parametrize("mapper, bundle", [
    (map_companies_house, CH_BUNDLE_DIRECT_PSC),
    (map_companies_house, CH_BUNDLE_CORPORATE_PSC),
    (map_companies_house, CH_BUNDLE_INDIVIDUAL_PSC),
    (map_gleif, GLEIF_BUNDLE_WITH_PARENT),
    (map_gleif, GLEIF_BUNDLE_WITH_CHILDREN),
    (map_brreg, BRREG_BUNDLE_WITH_SHAREHOLDER),
    (map_ur_latvia, UR_LATVIA_BUNDLE_WITH_SHAREHOLDERS),
    (map_firmenbuch, FIRMENBUCH_BUNDLE_WITH_PERSONS),
    (map_ariregister, ARIREGISTER_BUNDLE_WITH_SHAREHOLDERS),
    (map_corporations_canada, CORPORATIONS_CANADA_BUNDLE_WITH_DIRECTOR),
])
def test_graph_has_no_floating_nodes(mapper, bundle):
    stmts = list(mapper(bundle))
    issues = check_graph_connectivity(stmts)
    assert issues == [], f"{mapper.__name__}: {issues}"
```

The test fixtures referenced here can be extracted from the existing `test_bods_*.py` files or defined centrally in a `tests/fixtures/` package.

---

## Phase 4 — Per-adapter mapper correctness matrix

This section specifies what each Tier 2 mapper *should* produce for a given input, providing a specification against which tests can be written.

### companies_house — direct individual PSC

**Input**: Company with one individual PSC (≥25% shares, natural person)
**Expected output**:
1. Entity statement for the company (`recordType: entity`, `isComponent: False`)
2. Person statement for the PSC (`recordType: person`, `isComponent: False`)
3. Relationship statement: `subject = company entity recordId`, `interestedParty = PSC person recordId`, `interests[0].type = "shareholding"`, `interests[0].beneficialOwnershipOrControl = True`, `interests[0].directOrIndirect = "direct"`

**Bods-dagre expectation**: Solid purple ownership edge from person node to entity node.

### companies_house — corporate PSC chain (UK Ltd → UK Ltd)

**Input**: Company A with PSC being Company B (which itself has individual PSC C)
**Expected output**:
1. Entity statement for A (`isComponent: False`)
2. Entity statement for B (`isComponent: True` — intermediary in indirect chain)
3. Person statement for C (`isComponent: False`)
4. Component relationship: B → A (direct shareholding)
5. Component relationship: C → B (direct shareholding)
6. Primary relationship: C → A (indirect shareholding, `componentRecords: [B's recordId]`)

**Current bug**: B is emitted with `isComponent: False`; `componentRecords` is absent. The primary relationship from C to A is also missing — only component relationships are emitted.

**Bods-dagre expectation**: Dotted purple ownership edge from C to A; solid edges for each component hop.

### gleif — entity with direct parent

**Input**: LEI record with `DIRECT_PARENT` relationship
**Expected output**:
1. Subject entity statement (the looked-up LEI)
2. Parent entity statement
3. Relationship: `subject = subject entity recordId`, `interestedParty = parent entity recordId`, `interests[0].type = "shareholding"`, `beneficialOwnershipOrControl = False`

**Bods-dagre expectation**: Solid purple ownership edge from parent to child entity.

### gleif — entity with ultimate parent only (no direct parent reported)

**Input**: LEI with `DIRECT_PARENT` exception + `ULTIMATE_PARENT` relationship
**Expected output**:
1. Subject entity statement
2. Exception relationship (or anonymous entity bridge) for direct parent
3. Ultimate parent entity statement
4. Relationship: subject entity → ultimate parent, `otherInfluenceOrControl`, `beneficialOwnershipOrControl = False`

### inpi — dirigeant (legal representative)

**Input**: INPI RNE record with `mandataireSocial` data and `beneficiaireEffectif = False`
**Expected output**:
1. Entity statement for the company
2. Person statement for the dirigeant
3. Relationship: `subject = company recordId`, `interestedParty = person recordId`, `interests[0].type = "seniorManagingOfficial"`, `beneficialOwnershipOrControl = False`

**Security invariant**: Any record with `beneficiaireEffectif = True` MUST produce zero statements. This must be tested explicitly.

### brreg — shareholder person

**Input**: Norwegian entity with UBO persons
**Expected output**:
1. Entity statement for the company
2. Person statement(s) for each UBO
3. Relationship(s): `subject = company recordId`, `interestedParty = person recordId`, `interests[0].type = "shareholding"`, `beneficialOwnershipOrControl = True`

### ur_latvia — shareholders + board members

**Input**: Latvian entity with shareholders and members
**Expected output**: Entity + person statements for each shareholder/member + relationships with appropriate interest types (`shareholding` for shareholders, `seniorManagingOfficial` for board members)

### firmenbuch — shareholders + directors

**Input**: Austrian entity with shareholders and Geschäftsführer
**Expected output**: Entity + person statements + relationships; shareholders get `shareholding`, directors get `seniorManagingOfficial`

### ariregister — Estonian shareholders + board

**Input**: Estonian entity with shareholders and board members
**Expected output**: Entity + person statements + relationships with correct interest types and `directOrIndirect` values

### corporations_canada — directors only

**Input**: Canadian corporation with director records
**Expected output**: Entity + person statement for director + relationship with `interests[0].type = "boardMember"`, `beneficialOwnershipOrControl = False`

---

## Phase 5 — Multi-source bundle assembly audit

**Goal**: Identify and fix cross-source ID mismatches in multi-source bundles.

### The problem

When a user looks up an entity by LEI, the `export.py` router assembles a BODS bundle from multiple sources. Each source produces its own entity statement for the same company, each with a different `statementId`/`recordId`. For example:

- GLEIF produces entity statement with `statementId = "gleif-<LEI>"`
- Companies House produces entity statement with `statementId = "ch-<company_number>"`

When the CH mapper then emits a PSC relationship with `subject = "ch-<company_number>"`, bods-dagre will not connect it to the GLEIF entity node (which has `statementId = "gleif-<LEI>"`). This is a floating node.

### Approaches to investigate

**Option A — Canonical entity ID via reconciler**: The reconciler (`reconcile.py`) already identifies that multiple source hits refer to the same entity. It could designate one `recordId` as canonical (e.g., the GLEIF one, as GLEIF is the anchor) and rewrite relationship `subject` fields to point to it before assembly.

**Option B — Deduplicate entity statements in export**: When assembling the multi-source bundle, `export.py` could collapse entity statements about the same entity (matching on LEI or company number) into a single statement, and update all relationship references accordingly.

**Option C — Keep per-source IDs but add `sameAs` links**: BODS v0.4 supports `sameAs` arrays on entity statements. This preserves provenance but bods-dagre doesn't use them for edge drawing, so floating nodes persist in the visualisation.

**Recommendation**: Option A is the most practical fix. The reconciler already has the cross-source identity information; it should output a canonical `entity_record_id` that all relationship mappers use as their `subject`.

### Audit steps

1. Run `/export` for each curated LEI and inspect the resulting BODS JSON for duplicate entity statements about the same company.
2. For each duplicate pair, identify which relationship statements reference which entity `recordId` and confirm the mismatch.
3. Document the precise IDs used by each source so the fix can be validated.

---

## Phase 6 — Fix implementation priority

Based on impact and complexity, fixes should be implemented in this order:

### Fix 1 — Graph connectivity for single-source bundles (high impact, low risk)

**Target**: All Tier 2 mappers
**Action**: Add `check_graph_connectivity()` to every `test_bods_*.py` file and run the suite. Any connectivity failures in single-source bundles (where no cross-source ID issues can exist) represent bugs in the mapper itself. Fix those first.

**Expected current failures** (to be confirmed by Phase 1 audit):
- `ariregister`: possible mismatch between person statement IDs and relationship `interestedParty`
- `brreg`: confirm all person statement IDs are referenced by relationships
- `ur_latvia`: confirm all person and relationship IDs are self-consistent

### Fix 2 — isComponent and componentRecords for indirect chains (medium impact, moderate complexity)

**Target**: `companies_house` corporate PSC chains; potentially `gleif` parent chains
**Action**: Modify `_emit_company_statements` to detect when a PSC is itself a company (rather than a natural person) and:
- Set `isComponent: True` on the intermediate entity statement
- Add `componentRecords` to the primary relationship
- Ensure component statements appear before the primary relationship in output

This is a Companies House-specific concern because it is the only adapter that recursively fetches PSC chains via `related_companies`.

### Fix 3 — Cross-source entity ID normalisation (high impact, higher complexity)

**Target**: `export.py` + `reconcile.py`
**Action**: Implement Option A (canonical entity ID via reconciler). The reconciler sets a canonical `entity_record_id`; `export.py` rewrites relationship subjects before final assembly.

This fix should be accompanied by a new test: fetch a real GLEIF+CH combined bundle via the API and assert that no relationship statement in the export has a dangling subject reference.

### Fix 4 — v0.3 object format cleanup (low risk)

**Target**: Any mapper still using `{"describedByPersonStatement": "..."}` or `{"describedByEntityStatement": "..."}` object format for `subject`/`interestedParty`
**Action**: Migrate to bare string format. Run `grep -r "describedBy" backend/opencheck/bods/mapper.py` to find instances.

---

## Phase 7 — Visualisation smoke test protocol

**Goal**: A repeatable manual test procedure for verifying bods-dagre rendering for each curated LEI.

### Tools required

- The web validator at https://datareview.openownership.org/ (CoVE-BODS)
- A local bods-dagre test harness (can be built from the DEVELOPER.md guide, or use the validator tool at https://github.com/StephenAbbott/bods-validator)
- The OpenCheck `/export?lei=<LEI>&format=json` endpoint

### Test procedure (repeat for each curated LEI)

1. Fetch `GET /export?lei=<LEI>&format=json` from the live API.
2. Save the response JSON.
3. Paste into the CoVE-BODS web validator. Record any validation errors.
4. Run `check_graph_connectivity()` on the JSON programmatically. Record any dangling reference errors.
5. Feed the JSON into the bods-validator visualisation or the bods-dagre harness.
6. Visually verify:
   - All person nodes are connected to at least one entity via a relationship edge
   - All entity nodes (other than the root subject) are connected to at least one other node
   - Edge colours match interest type: purple (ownership) for `shareholding`; blue (control) for `seniorManagingOfficial`, `appointmentOfBoard`, `votingRights`, `otherInfluenceOrControl`
   - Edge style matches `directOrIndirect`: solid for `direct`, dotted for `indirect`
7. Record any floating nodes (disconnected nodes) and link them back to specific relationship statements.

### Curated LEIs to smoke-test

| Entity | LEI | Expected adapters | Expected graph shape |
|---|---|---|---|
| Daily Mail and General Trust | 213800VKKC1QRNHBIG82 | GLEIF, Companies House | Trust entity + persons |
| BP p.l.c. | 097900BEFH0000000217 | GLEIF, Companies House | Entity + PSC persons/entities |
| Rosneft | 549300CEMS4U5QQ4LZ79 | GLEIF, Wikidata, OpenSanctions | Sanctioned entity |
| Bank Saderat Iran | (known LEI) | GLEIF, OpenSanctions | Sanctioned + NON_EU_JURISDICTION |
| Koninklijke Ahold Delhaize | LEI for Ahold | GLEIF, KvK | Entity-only (KvK has no persons) |

---

## Phase 8 — CI integration

Once Phases 1–3 are complete, add the new tests to the CI pipeline:

```yaml
# In GitHub Actions / equivalent:
- name: BODS compliance tests
  run: |
    cd backend
    python -m pytest tests/test_bods_graph_integrity.py -v
    python -m pytest tests/ -k "cove" -v
```

The goal is **973 → ~1020+ tests** with the new connectivity and CoVE integration tests, all green.

---

## Summary checklist

- [ ] **Phase 1**: Run baseline audit for all Tier 2 mappers; document current connectivity failures
- [ ] **Phase 2a**: Install `libcovebods` and add `validate_with_cove` helper
- [ ] **Phase 2b**: Add CoVE validation to all existing `test_bods_*.py` files
- [ ] **Phase 2c**: Add `check_graph_connectivity` to all existing `test_bods_*.py` files
- [ ] **Phase 3**: Create `test_bods_graph_integrity.py` with cross-adapter parametrized tests
- [ ] **Phase 4**: Write explicit fixture-based tests for each mapper pattern in the correctness matrix
- [ ] **Phase 5**: Audit multi-source bundle ID mismatches for each curated LEI
- [ ] **Fix 1**: Single-source connectivity failures (mapper bugs)
- [ ] **Fix 2**: `isComponent` / `componentRecords` for CH corporate PSC chains
- [ ] **Fix 3**: Cross-source canonical entity ID normalisation
- [ ] **Fix 4**: v0.3 object format cleanup
- [ ] **Phase 7**: Full visualisation smoke test for all curated LEIs
- [ ] **Phase 8**: Add BODS tests to CI pipeline

---

*Last updated: Phase 42. Pending: Phase 43 (this plan). Test count target: 1020+ from current 973.*
