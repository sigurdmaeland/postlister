---
name: query-databricks-delta
description: Query Delta Lake tables in Azure Databricks. Use when the user asks to query, inspect, count, or compare data in Databricks bronze/silver tables, or when referencing Delta tables in `postlister`.bronze or `postlister`.silver.
---

# Query Databricks Delta Tables

## When to use

Apply this skill when you need to:
- Query data from Databricks Delta tables (SQL)
- List tables in a Databricks catalog/schema
- Inspect table columns and types
- Count rows with optional filters
- Compare silver data against bronze source data

## Connection

To auth-metoder, auto-detektert fra hvilke felt som er fylt inn i `connection.json`:

- **PAT (personal access token) - anbefalt for eget workspace uten account admin-tilgang**: kun `DATABRICKS_HOST` + `DATABRICKS_TOKEN`. Genereres selv i workspacet, ingen adminrettigheter nødvendig.
- **Service Principal (OAuth M2M)**: `DATABRICKS_HOST` + `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET`. Krever at en account admin oppretter service principal/OAuth-app først (samme blokkering som den native Databricks-connectoren i Claude - se under).

- **Credentials file**: `.claude/skills/query-databricks-delta/connection.json`
- Copy `connection.example.json` to `connection.json` and fill in credentials.
- Do **not** commit `connection.json` (already covered by `.gitignore`'s `**/connection.json` rule).

### Slik lager du en PAT (ingen admin-tilgang nødvendig)

I Databricks-workspacet: klikk brukernavnet øverst til høyre -> **Settings** -> **Developer** -> **Access tokens** -> **Generate new token**. Lim verdien inn i `DATABRICKS_TOKEN` i `connection.json`.

### Fields in `connection.json`

| Field | Description |
|-------|-------------|
| `DATABRICKS_HOST` | Workspace URL (e.g. `https://<workspace>.cloud.databricks.com`) |
| `DATABRICKS_TOKEN` | Personal access token (PAT-modus - la CLIENT_ID/SECRET stå tomme) |
| `DATABRICKS_CLIENT_ID` | Service Principal OAuth client ID (SP-modus) |
| `DATABRICKS_CLIENT_SECRET` | Service Principal OAuth client secret (SP-modus) |
| `DATABRICKS_CLUSTER_ID` | Cluster ID (from cluster URL or API). Kan stå tom hvis workspacet bruker serverless compute. |

Denne skillen er kopiert fra easy-pipelines' `query-databricks-delta` og tilpasset dette prosjektets eget (foreløpig personlige) Databricks-workspace. Default katalog/skjema under (`postlister`/`bronze`) er en gjetning på navnekonvensjon - endre `--catalog`/`--schema` (eller default-verdiene i `scripts/query_delta.py`) til det som faktisk finnes i workspacet ditt.

## Running the script

Requires `databricks-connect` and `databricks-sdk`. Install into a venv (anbefalt egen venv for denne skillen, for å unngå pyspark-versjonskonflikter med resten av prosjektet):

```bash
pip install databricks-connect databricks-sdk
```

### List tables

```bash
.venv/bin/python .claude/skills/query-databricks-delta/scripts/query_delta.py list-tables
```

Default catalog: `postlister`, schema: `bronze`. Override with `--catalog` / `--schema`.

### Describe a table

```bash
.venv/bin/python .claude/skills/query-databricks-delta/scripts/query_delta.py describe postliste_sak
```

Shows columns, types, and row count. Auto-qualifies unqualified table names.

### Run a SQL query

```bash
.venv/bin/python .claude/skills/query-databricks-delta/scripts/query_delta.py query "SELECT saksnummer, sakstittel FROM postlister.bronze.gjovik_bygg WHERE kommune_nr = 3407 LIMIT 10"
```

Default limit: 50 rows. Use `--limit 0` for unlimited.

### Count rows

```bash
.venv/bin/python .claude/skills/query-databricks-delta/scripts/query_delta.py count gjovik_bygg
.venv/bin/python .claude/skills/query-databricks-delta/scripts/query_delta.py count gjovik_bygg --where "adresse IS NOT NULL"
```

## Notes

- The script auto-starts the cluster if it's stopped (may take a few minutes). Skipped entirely if `DATABRICKS_CLUSTER_ID` is empty (serverless).
- Status messages go to stderr, query results to stdout.
- Large result sets should use `--limit` or SQL `LIMIT` to avoid memory issues.
- Table names with hyphens in the catalog are auto-quoted with backticks.

## Om den native Claude-Databricks-connectoren

Den innebygde Databricks-connectoren i Claude (markedsplass-koblingen) krever at en account admin oppretter en OAuth-app i Databricks' account console (App Connections) - samme adminkrav som Service Principal-modus over. Uten account admin-tilgang er PAT-modus i denne skillen den praktiske veien til å faktisk kunne spørre mot workspacet akkurat nå.
