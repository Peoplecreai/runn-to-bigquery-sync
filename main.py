import os, sys, yaml
from runn_client import fetch_all
from clockify_client import fetch_all_time_entries
from clockify_transformer import transform_batch
from bq_utils import get_bq_client, load_staging, ensure_target_schema_matches_stg, build_merge_sql, truncate_table

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET", "people_analytics")
# Variable de entorno para hacer full sync (borrar todo y recargar)
# Usar: FULL_SYNC=true para limpiar duplicados
FULL_SYNC = os.getenv("FULL_SYNC", "false").lower() in ("true", "1", "yes")

def sync_endpoint(client, name, path):
    """Sincroniza un endpoint de Runn"""
    rows = list(fetch_all(path))
    if not rows:
        print(f"[{name}] sin datos")
        return 0
    stg_table = f"{PROJECT}.{DATASET}._stg__{name}"
    tgt_table = f"{PROJECT}.{DATASET}.{name}"

    # Si FULL_SYNC está activado, truncar la tabla target primero
    if FULL_SYNC:
        print(f"[{name}] FULL SYNC activado - truncando tabla {tgt_table}")
        truncate_table(client, tgt_table)

    load_staging(client, stg_table, rows)
    ensure_target_schema_matches_stg(client, stg_table, tgt_table)
    merge_sql = build_merge_sql(PROJECT, DATASET, name)
    client.query(merge_sql).result()
    sync_type = "full sync" if FULL_SYNC else "upsert"
    print(f"[{name}] {sync_type}: {len(rows)} filas")
    return len(rows)

def sync_actuals_from_clockify(client, name):
    """Sincroniza actuals desde Clockify en lugar de Runn"""
    print(f"[{name}] Obteniendo time entries desde Clockify...")

    # Obtener time entries de Clockify (ya deduplicados en clockify_client.py)
    time_entries = list(fetch_all_time_entries())

    if not time_entries:
        print(f"[{name}] sin datos de Clockify")
        return 0

    print(f"[{name}] {len(time_entries)} time entries obtenidos de Clockify")

    # Transformar a formato de actuals de Runn
    rows = transform_batch(time_entries)

    print(f"[{name}] {len(rows)} actuals transformados")

    # SEGUNDA CAPA DE DEDUPLICACIÓN: Verificar que no haya IDs duplicados antes de cargar
    # Esto protege contra colisiones de hash u otros problemas
    ids_seen = {}
    duplicates_found = []

    for i, row in enumerate(rows):
        row_id = row.get("id")
        if row_id in ids_seen:
            duplicates_found.append({
                "id": row_id,
                "first_index": ids_seen[row_id],
                "duplicate_index": i,
                "clockify_id_1": rows[ids_seen[row_id]].get("_clockify_id"),
                "clockify_id_2": row.get("_clockify_id"),
            })
        else:
            ids_seen[row_id] = i

    if duplicates_found:
        print(f"\n⚠️  ADVERTENCIA: Se encontraron {len(duplicates_found)} IDs numéricos duplicados después de transformar!")
        print(f"   Esto indica colisiones de hash. Primeros 5 ejemplos:")
        for dup in duplicates_found[:5]:
            print(f"   - ID numérico {dup['id']}:")
            print(f"     Clockify ID 1: {dup['clockify_id_1']}")
            print(f"     Clockify ID 2: {dup['clockify_id_2']}")

        # Deduplicar rows manteniendo solo la primera ocurrencia de cada ID
        unique_rows = []
        seen_ids_set = set()
        for row in rows:
            row_id = row.get("id")
            if row_id not in seen_ids_set:
                unique_rows.append(row)
                seen_ids_set.add(row_id)

        print(f"\n   Deduplicando: {len(rows)} → {len(unique_rows)} filas")
        rows = unique_rows

    # Cargar a BigQuery con el mismo proceso
    stg_table = f"{PROJECT}.{DATASET}._stg__{name}"
    tgt_table = f"{PROJECT}.{DATASET}.{name}"

    # Si FULL_SYNC está activado, truncar la tabla target primero
    if FULL_SYNC:
        print(f"[{name}] FULL SYNC activado - truncando tabla {tgt_table}")
        truncate_table(client, tgt_table)

    load_staging(client, stg_table, rows)
    ensure_target_schema_matches_stg(client, stg_table, tgt_table)
    merge_sql = build_merge_sql(PROJECT, DATASET, name)
    client.query(merge_sql).result()
    sync_type = "full sync" if FULL_SYNC else "upsert"
    print(f"[{name}] {sync_type}: {len(rows)} filas desde Clockify")
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
        # Verificar si este endpoint usa Clockify
        source = meta.get("source", "runn")

        if source == "clockify":
            # Usar Clockify para este endpoint
            processed = sync_actuals_from_clockify(client, name)
        else:
            # Usar Runn (comportamiento por defecto)
            processed = sync_endpoint(client, name, meta["path"])

        per_endpoint[name] = processed
        total += processed
    return {"total_rows": total, "per_endpoint": per_endpoint}


def main():
    if FULL_SYNC:
        print("\n" + "="*60)
        print("⚠️  FULL SYNC ACTIVADO - Se borrarán todas las tablas antes de recargar")
        print("="*60 + "\n")

    result = run_sync()
    print(f"\nTotal filas procesadas: {result['total_rows']}")

    if FULL_SYNC:
        print("\n✅ Full sync completado - Todos los duplicados han sido eliminados")

    return 0


if __name__ == "__main__":
    sys.exit(main())
