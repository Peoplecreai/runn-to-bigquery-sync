import os, sys, yaml
from clockify_client import fetch_all as clockify_fetch_all
from runn_client import fetch_all as runn_fetch_all
from data_mapper import transform_data
from bq_utils import get_bq_client, load_staging, ensure_target_schema_matches_stg, build_merge_sql

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET", "people_analytics")

def sync_endpoint(client, name, path, use_reports=False, source="clockify"):
    """
    Sync a single endpoint to BigQuery from either Clockify or Runn.

    Args:
        client: BigQuery client
        name: Endpoint name (e.g., "runn_people")
        path: API path
        use_reports: Whether to use Reports API (POST-based pagination) - Clockify only
        source: Data source - "clockify" or "runn"
    """
    # Fetch data from appropriate source
    if source == "runn":
        print(f"[{name}] usando Runn API...")
        raw_rows = list(runn_fetch_all(path))
    else:  # default to clockify
        print(f"[{name}] usando Clockify API...")
        raw_rows = list(clockify_fetch_all(path, use_reports_api=use_reports))

    if not raw_rows:
        print(f"[{name}] sin datos")
        return 0

    # Transform data to BigQuery-compatible format
    # For Runn endpoints, data is already in the right format (no transformation needed)
    if source == "runn":
        rows = raw_rows
    else:
        rows = [transform_data(name, row) for row in raw_rows]

    # Load to BigQuery
    stg_table = f"{PROJECT}.{DATASET}._stg__{name}"
    tgt_table = f"{PROJECT}.{DATASET}.{name}"
    load_staging(client, stg_table, rows)
    ensure_target_schema_matches_stg(client, stg_table, tgt_table)
    merge_sql = build_merge_sql(PROJECT, DATASET, name)
    client.query(merge_sql).result()
    print(f"[{name}] upsert: {len(rows)} filas")
    return len(rows)

def run_sync():
    cfg_path = os.getenv("ENDPOINTS_FILE", "endpoints.yaml")
    with open(cfg_path, "r") as f:
        cfg = yaml.safe_load(f)
    endpoints = cfg["endpoints"]
    client = get_bq_client(PROJECT)
    per_endpoint = {}
    total = 0
    for name, meta in endpoints.items():
        # Skip disabled endpoints
        if meta.get("disabled", False):
            print(f"[{name}] deshabilitado, omitiendo")
            per_endpoint[name] = 0
            continue

        use_reports = meta.get("use_reports", False)
        source = meta.get("source", "clockify")  # Default to clockify
        processed = sync_endpoint(
            client,
            name,
            meta["path"],
            use_reports=use_reports,
            source=source
        )
        per_endpoint[name] = processed
        total += processed
    return {"total_rows": total, "per_endpoint": per_endpoint}


def main():
    result = run_sync()
    print(f"Total filas procesadas: {result['total_rows']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
