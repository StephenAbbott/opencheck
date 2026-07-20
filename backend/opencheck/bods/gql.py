"""BigQuery GQL export — project a BODS bundle into a property graph package.

Thin wrapper over the `bods-gql <https://github.com/StephenAbbott/bods-gql>`_
library (a pinned git dependency, core install only — no Google Cloud stack):
the bundle's statements become node/edge CSV tables, a ``CREATE PROPERTY
GRAPH`` DDL statement, and the library's 14 ready-made GQL (ISO/IEC 39075)
queries, packaged as one zip the user can upload to BigQuery themselves.

The DDL and queries are emitted against a ``YOUR_PROJECT.YOUR_DATASET``
placeholder — the bundled README shows the one-liner to swap in a real
dataset — because OpenCheck has no knowledge of (and no credentials for)
the user's Google Cloud project.
"""

from __future__ import annotations

import csv
import io
from typing import Any

from bods_gql.converter.mapper import MappingResult, map_statements
from bods_gql.graph_schema.property_graph import generate_create_graph_ddl
from bods_gql.queries import circular_ownership, corporate_groups, ubo_detection

# Placeholder the DDL/queries are generated against; documented in the README.
PLACEHOLDER_DATASET = "YOUR_PROJECT.YOUR_DATASET"

# The library's 14 GQL queries, keyed by the same names as the bods-gql CLI's
# --query-type choices (kept aligned so its docs apply to these files).
_QUERY_BUILDERS = {
    "find-owners": lambda d: ubo_detection.find_owners(d),
    "find-owned": lambda d: ubo_detection.find_owned_entities(d),
    "find-ubos": lambda d: ubo_detection.find_ubos_gql(d),
    "find-ubos-sql": lambda d: ubo_detection.find_ubos_with_sql(d),
    "entities-without-ubos": lambda d: ubo_detection.find_entities_without_ubos(d),
    "corporate-group": lambda d: corporate_groups.corporate_group(d),
    "top-parents": lambda d: corporate_groups.top_level_parents(d),
    "jurisdiction-analysis": lambda d: corporate_groups.group_jurisdiction_analysis(d),
    "group-metrics": lambda d: corporate_groups.group_metrics(d),
    "all-groups": lambda d: corporate_groups.all_groups(d),
    "find-cycles": lambda d: circular_ownership.find_cycles(d),
    "check-cycle": lambda d: circular_ownership.check_entity_cycle(d),
    "mutual-ownership": lambda d: circular_ownership.mutual_ownership(d),
    "cycle-stats": lambda d: circular_ownership.cycle_stats(d),
}


def map_to_gql(bods_statements: list[dict[str, Any]]) -> MappingResult:
    """Map BODS statements to bods-gql node/edge records."""
    return map_statements(iter(bods_statements))


def gql_counts(bods_statements: list[dict[str, Any]]) -> dict[str, int]:
    """Node/edge counts for the export manifest."""
    result = map_to_gql(bods_statements)
    return {
        "gql_entity_node_count": len(result.entity_nodes),
        "gql_person_node_count": len(result.person_nodes),
        "gql_edge_count": len(result.ownership_edges),
    }


def _rows_to_csv(rows: list[dict[str, Any]]) -> str:
    """Serialise mapped rows to CSV. Header always present (mirrors the
    bods-gql CLI field ordering: the keys of ``to_dict()``); an empty table
    yields an empty string so callers can skip the file entirely."""
    if not rows:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _readme(counts: dict[str, int], mapping_errors: int) -> str:
    lines = [
        "# OpenCheck → BigQuery GQL export",
        "",
        "This package projects an OpenCheck BODS v0.4 ownership bundle into a",
        "[BigQuery property graph](https://cloud.google.com/bigquery/docs/property-graphs)",
        "queryable with GQL (ISO/IEC 39075), generated with",
        "[bods-gql](https://github.com/StephenAbbott/bods-gql).",
        "",
        "## Contents",
        "",
        f"- `entity_nodes.csv` — {counts['gql_entity_node_count']} entity node(s)",
        f"- `person_nodes.csv` — {counts['gql_person_node_count']} person node(s)",
        f"- `ownership_edges.csv` — {counts['gql_edge_count']} ownership/control edge(s)",
        "- `create_property_graph.sql` — `CREATE PROPERTY GRAPH` DDL",
        f"- `queries/` — {len(_QUERY_BUILDERS)} ready-made GQL queries (UBO detection,"
        " corporate groups, circular ownership)",
        "- `LICENSES.md` — licence and attribution for every contributing source",
        "",
        "## Load into BigQuery",
        "",
        "The DDL and queries use the placeholder dataset"
        f" `{PLACEHOLDER_DATASET}`. Swap in your own, then load:",
        "",
        "```bash",
        "DATASET=my_project.my_dataset",
        'sed -i "" "s/YOUR_PROJECT.YOUR_DATASET/$DATASET/g" create_property_graph.sql queries/*.gql',
        "",
        "bq load --autodetect --replace --source_format=CSV \\",
        "  $DATASET.entity_nodes entity_nodes.csv",
        "bq load --autodetect --replace --source_format=CSV \\",
        "  $DATASET.person_nodes person_nodes.csv",
        "bq load --autodetect --replace --source_format=CSV \\",
        "  $DATASET.ownership_edges ownership_edges.csv",
        "",
        "bq query --use_legacy_sql=false < create_property_graph.sql",
        "bq query --use_legacy_sql=false < queries/find-ubos.gql",
        "```",
        "",
        "For fully-typed tables and an end-to-end loader, install"
        " `bods-gql[bigquery]` and use `bods-gql load` instead.",
    ]
    if mapping_errors:
        lines += [
            "",
            f"> ⚠️ {mapping_errors} statement(s) could not be mapped to the"
            " property graph and are omitted from the tables.",
        ]
    return "\n".join(lines) + "\n"


def build_gql_files(bods_statements: list[dict[str, Any]]) -> dict[str, str]:
    """The zip's members (path → text content), minus LICENSES.md.

    Empty tables are skipped (matching the bods-gql CLI, and avoiding
    header-only CSVs that `bq load --autodetect` chokes on).
    """
    result = map_to_gql(bods_statements)
    files: dict[str, str] = {}
    for name, rows in (
        ("entity_nodes.csv", [n.to_dict() for n in result.entity_nodes]),
        ("person_nodes.csv", [n.to_dict() for n in result.person_nodes]),
        ("ownership_edges.csv", [e.to_dict() for e in result.ownership_edges]),
    ):
        content = _rows_to_csv(rows)
        if content:
            files[name] = content
    files["create_property_graph.sql"] = (
        generate_create_graph_ddl(PLACEHOLDER_DATASET) + "\n"
    )
    for name, build in _QUERY_BUILDERS.items():
        files[f"queries/{name}.gql"] = build(PLACEHOLDER_DATASET).strip() + "\n"
    counts = {
        "gql_entity_node_count": len(result.entity_nodes),
        "gql_person_node_count": len(result.person_nodes),
        "gql_edge_count": len(result.ownership_edges),
    }
    files["README.md"] = _readme(counts, mapping_errors=len(result.errors))
    return files
