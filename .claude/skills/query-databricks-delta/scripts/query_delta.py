#!/usr/bin/env python3
"""
Query Delta Lake tables in Azure Databricks via Databricks Connect.

Usage:
    python query_delta.py list-tables [--catalog <name>] [--schema <name>]
    python query_delta.py describe <table_name>
    python query_delta.py query "<sql>" [--limit <N>]
    python query_delta.py count <table_name> [--where "<condition>"]

Connection: connection.json in the skill directory.
Default catalog: postlister, default schema: bronze.
"""

import argparse
import json
import os
import sys
from pathlib import Path


def _skill_dir() -> Path:
    return Path(__file__).resolve().parent.parent


def _load_connection() -> dict:
    """Load credentials from connection.json.

    Two auth modes, auto-detected from which fields are filled in:
      - PAT (personal access token): DATABRICKS_HOST + DATABRICKS_TOKEN.
        No admin rights needed - generate the token yourself in the
        workspace under Settings -> Developer -> Access tokens.
      - Service Principal (OAuth M2M): DATABRICKS_HOST + DATABRICKS_CLIENT_ID
        + DATABRICKS_CLIENT_SECRET. Requires an account admin to create the
        service principal/OAuth app first.
    DATABRICKS_CLUSTER_ID is required either way (which cluster to run
    against) unless the workspace uses serverless compute, in which case it
    can be left empty and Databricks Connect will use serverless."""
    path = _skill_dir() / "connection.json"
    if not path.is_file():
        print(
            f"ERROR: {path} not found. Copy connection.example.json and fill in credentials.",
            file=sys.stderr,
        )
        sys.exit(1)
    with open(path) as f:
        config = json.load(f)

    if not config.get("DATABRICKS_HOST"):
        print(f"ERROR: 'DATABRICKS_HOST' is empty in {path}", file=sys.stderr)
        sys.exit(1)

    has_pat = bool(config.get("DATABRICKS_TOKEN"))
    has_sp = bool(config.get("DATABRICKS_CLIENT_ID")) and bool(config.get("DATABRICKS_CLIENT_SECRET"))
    if not has_pat and not has_sp:
        print(
            f"ERROR: {path} needs either DATABRICKS_TOKEN (personal access token) "
            "or both DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET (service principal)",
            file=sys.stderr,
        )
        sys.exit(1)
    return config


def _wait_for_cluster(config: dict) -> None:
    """Ensure the Databricks cluster is running (skipped if no cluster ID -
    e.g. serverless)."""
    cluster_id = (config.get("DATABRICKS_CLUSTER_ID") or "").strip()
    if not cluster_id:
        return
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError:
        return  # no SDK, skip wait
    try:
        host = config["DATABRICKS_HOST"].strip()
        if config.get("DATABRICKS_TOKEN"):
            w = WorkspaceClient(host=host, token=config["DATABRICKS_TOKEN"].strip())
        else:
            w = WorkspaceClient(host=host, client_id=config["DATABRICKS_CLIENT_ID"].strip(),
                                 client_secret=config["DATABRICKS_CLIENT_SECRET"].strip())
        print("Waiting for cluster to be RUNNING...", file=sys.stderr)
        w.clusters.ensure_cluster_is_running(cluster_id)
        print("Cluster is RUNNING.", file=sys.stderr)
    except Exception as e:
        print(f"WARNING: Could not check cluster status: {e}", file=sys.stderr)


def _get_spark():
    """Build SparkSession via Databricks Connect."""
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        raise SystemExit(
            "databricks-connect is required. Install it in the venv:\n"
            "  pip install databricks-connect databricks-sdk"
        ) from None

    config = _load_connection()
    _wait_for_cluster(config)

    os.environ["DATABRICKS_HOST"] = config["DATABRICKS_HOST"].strip()
    cluster_id = (config.get("DATABRICKS_CLUSTER_ID") or "").strip()
    if cluster_id:
        os.environ["DATABRICKS_CLUSTER_ID"] = cluster_id

    if config.get("DATABRICKS_TOKEN"):
        os.environ["DATABRICKS_TOKEN"] = config["DATABRICKS_TOKEN"].strip()
    else:
        os.environ["DATABRICKS_CLIENT_ID"] = config["DATABRICKS_CLIENT_ID"].strip()
        os.environ["DATABRICKS_CLIENT_SECRET"] = config["DATABRICKS_CLIENT_SECRET"].strip()

    return DatabricksSession.builder.getOrCreate()


def _quote_id(name: str) -> str:
    """Backtick-quote identifiers with hyphens or other special chars."""
    return f"`{name}`" if "-" in name else name


def _catalog_ref(catalog: str) -> str:
    """Backtick-quote catalog names with hyphens."""
    return _quote_id(catalog)


def cmd_list_tables(args):
    """List tables in a catalog.schema."""
    spark = _get_spark()
    cat = _catalog_ref(args.catalog)
    schema_qual = f"{cat}.{_quote_id(args.schema)}"

    rows = spark.sql(f"SHOW TABLES IN {schema_qual}").collect()
    table_names = sorted([r.tableName for r in rows if getattr(r, "tableName", None)])

    print(f"Tables in {schema_qual} ({len(table_names)}):")
    for name in table_names:
        print(f"  {name}")


def cmd_describe(args):
    """Describe a table's columns, types, and row count."""
    spark = _get_spark()
    table = args.table_name

    # Auto-qualify if not already fully qualified
    if "." not in table:
        cat = _catalog_ref(args.catalog)
        table = f"{cat}.{_quote_id(args.schema)}.{table}"

    df = spark.table(table)
    print(f"Table: {table}")
    print(f"Columns ({len(df.schema.fields)}):")
    for field in df.schema.fields:
        nullable = "nullable" if field.nullable else "not null"
        print(f"  {field.name}: {field.dataType.simpleString()} ({nullable})")

    count = df.count()
    print(f"\nRow count: {count:,}")


def cmd_query(args):
    """Run a SQL query and print results as tab-separated values."""
    spark = _get_spark()
    sql = args.sql
    limit = args.limit

    # Add LIMIT if not already present
    if limit and "LIMIT" not in sql.upper():
        sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"

    df = spark.sql(sql)
    rows = df.collect()

    if not rows:
        print("(no results)")
        return

    # Header
    columns = [f.name for f in df.schema.fields]
    print("\t".join(columns))
    print("\t".join(["-" * min(len(c), 30) for c in columns]))

    # Rows
    for row in rows:
        values = []
        for col in columns:
            val = row[col]
            values.append("NULL" if val is None else str(val))
        print("\t".join(values))

    print(f"\n{len(rows)} row(s)")


def cmd_count(args):
    """Count rows in a table with optional WHERE clause."""
    spark = _get_spark()
    table = args.table_name

    if "." not in table:
        cat = _catalog_ref(args.catalog)
        table = f"{cat}.{_quote_id(args.schema)}.{table}"

    sql = f"SELECT COUNT(*) as cnt FROM {table}"
    if args.where:
        sql += f" WHERE {args.where}"

    row = spark.sql(sql).collect()[0]
    print(f"{row.cnt:,}")


def main():
    parser = argparse.ArgumentParser(description="Query Databricks Delta Lake tables")
    parser.add_argument("--catalog", default="postlister", help="Catalog (default: postlister)")
    parser.add_argument("--schema", default="bronze", help="Schema (default: bronze)")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # list-tables
    subparsers.add_parser("list-tables", help="List tables in the schema")

    # describe
    p_desc = subparsers.add_parser("describe", help="Describe table columns and row count")
    p_desc.add_argument("table_name", help="Table name (auto-qualified if unqualified)")

    # query
    p_query = subparsers.add_parser("query", help="Run a SQL query")
    p_query.add_argument("sql", help="SQL query string")
    p_query.add_argument("--limit", type=int, default=50, help="Max rows (default: 50, 0=unlimited)")

    # count
    p_count = subparsers.add_parser("count", help="Count rows in a table")
    p_count.add_argument("table_name", help="Table name")
    p_count.add_argument("--where", help="WHERE clause (e.g. \"kommune_navn = 'GJØVIK'\")")

    args = parser.parse_args()

    {"list-tables": cmd_list_tables, "describe": cmd_describe, "query": cmd_query, "count": cmd_count}[args.command](args)


if __name__ == "__main__":
    main()
