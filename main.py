import os, sys, yaml
from runn_client import fetch_all
from bq_utils import get_bq_client, load_staging, ensure_target_schema_matches_stg, build_merge_sql

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET", "people_analytics")

def sync_endpoint(client, name, path):
    rows = list(fetch_all(path))
    if not rows:
        print(f"[{name}] sin datos")
        return 0
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
        processed = sync_endpoint(client, name, meta["path"])
        per_endpoint[name] = processed
        total += processed
    return {"total_rows": total, "per_endpoint": per_endpoint}


def main():
    result = run_sync()
    print(f"Total filas procesadas: {result['total_rows']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
