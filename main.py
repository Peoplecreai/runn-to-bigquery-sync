import os, sys, yaml
from runn_client import fetch_all
from clockify_client import fetch_all_time_entries, build_user_email_map
from clockify_transformer import transform_batch, build_user_map_by_email
from clockify_reports_client import fetch_detailed_report
from clockify_reports_transformer import (
    transform_batch as transform_report_batch,
    build_user_map_by_email_from_runn,
    build_project_map_by_name_from_runn,
    analyze_report_data
)
from bq_utils import get_bq_client, load_staging, ensure_target_schema_matches_stg, build_merge_sql, truncate_table, deduplicate_table_by_column

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
    """
    Sincroniza actuals desde Clockify usando el Reports API (más confiable).

    El Reports API de Clockify es la fuente de verdad para datos billable/non-billable
    porque:
    - Incluye el campo isBillable de forma consistente
    - Proporciona billableAmount y costAmount calculados
    - Tiene mejor paginación y menos duplicados
    - Es el mismo API que usa la UI de Clockify para reportes
    """
    print(f"[{name}] Obteniendo datos desde Clockify Reports API...")
    print(f"[{name}] (Usando Detailed Report - fuente más confiable para billable/non-billable)")

    # Obtener detailed report de Clockify
    report_entries = fetch_detailed_report()

    if not report_entries:
        print(f"[{name}] sin datos del Clockify Reports API")
        return 0

    print(f"[{name}] ✓ {len(report_entries)} entries obtenidos del Detailed Report")

    # Analizar datos del report para validación
    print(f"\n[{name}] Analizando datos del report...")
    stats = analyze_report_data(report_entries)

    print(f"\n{'='*60}")
    print(f"📊 ANÁLISIS DE DATOS DEL CLOCKIFY REPORT:")
    print(f"{'='*60}")
    print(f"  Total entries: {stats['total_entries']}")
    print(f"  Billable entries: {stats['billable_entries']} ({stats['billable_percentage']})")
    print(f"  Non-billable entries: {stats['non_billable_entries']}")
    print(f"  Total horas: {stats['total_hours']:.2f}h")
    print(f"  Billable horas: {stats['billable_hours']:.2f}h")
    print(f"  Non-billable horas: {stats['non_billable_hours']:.2f}h")
    print(f"  Usuarios únicos: {stats['unique_users']}")
    print(f"  Proyectos únicos: {stats['unique_projects']}")

    if stats['duplicates_detected'] > 0:
        print(f"\n  ⚠️  Duplicados detectados: {stats['duplicates_detected']}")
    else:
        print(f"\n  ✓ No hay duplicados en el report")
    print(f"{'='*60}\n")

    # Obtener datos de Runn para mapeo
    print(f"[{name}] Obteniendo datos de Runn para mapeo...")
    runn_people = list(fetch_all("/people/"))
    runn_projects = list(fetch_all("/projects/"))

    # Construir mapeos
    print(f"[{name}] Construyendo mapeos email → personId y projectName → projectId...")
    user_map = build_user_map_by_email_from_runn(runn_people)
    project_map = build_project_map_by_name_from_runn(runn_projects)

    print(f"  ✓ {len(user_map)} usuarios mapeados por email")
    print(f"  ✓ {len(project_map)} proyectos mapeados por nombre")

    # Transformar a formato de actuals de Runn
    print(f"\n[{name}] Transformando entries a formato runn_actuals...")
    rows = transform_report_batch(report_entries, user_map=user_map, project_map=project_map)

    print(f"[{name}] ✓ {len(rows)} actuals transformados")

    # Verificar que no haya IDs duplicados (safety check)
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
        print(f"\n⚠️  ADVERTENCIA: {len(duplicates_found)} IDs numéricos duplicados (colisión de hash)")

        # Deduplicar rows manteniendo solo la primera ocurrencia
        unique_rows = []
        seen_ids_set = set()
        for row in rows:
            row_id = row.get("id")
            if row_id not in seen_ids_set:
                unique_rows.append(row)
                seen_ids_set.add(row_id)

        print(f"   Deduplicando: {len(rows)} → {len(unique_rows)} filas")
        rows = unique_rows

    # Validar datos antes de cargar
    billable_mins = sum(r["billableMinutes"] for r in rows)
    nonbillable_mins = sum(r["nonbillableMinutes"] for r in rows)
    total_mins = billable_mins + nonbillable_mins

    print(f"\n[{name}] Validación final antes de cargar a BigQuery:")
    print(f"  Billable minutes: {billable_mins:,} ({billable_mins/60:.2f}h)")
    print(f"  Non-billable minutes: {nonbillable_mins:,} ({nonbillable_mins/60:.2f}h)")
    print(f"  Total minutes: {total_mins:,} ({total_mins/60:.2f}h)")

    if abs((total_mins/60) - stats['total_hours']) > 0.1:
        print(f"\n  ⚠️  ADVERTENCIA: Discrepancia en horas totales!")
        print(f"     Report API: {stats['total_hours']:.2f}h")
        print(f"     Transformado: {total_mins/60:.2f}h")

    # Cargar a BigQuery
    stg_table = f"{PROJECT}.{DATASET}._stg__{name}"
    tgt_table = f"{PROJECT}.{DATASET}.{name}"

    # Si FULL_SYNC está activado, truncar la tabla target primero
    if FULL_SYNC:
        print(f"\n[{name}] FULL SYNC activado - truncando tabla {tgt_table}")
        truncate_table(client, tgt_table)

    print(f"\n[{name}] Cargando {len(rows)} filas a staging...")
    load_staging(client, stg_table, rows)
    ensure_target_schema_matches_stg(client, stg_table, tgt_table)

    # Limpiar duplicados históricos ANTES del merge
    print(f"[{name}] Eliminando duplicados históricos...")
    deduplicate_table_by_column(client, tgt_table, "_clockify_id")

    # Merge usando _clockify_id como clave única
    print(f"[{name}] Ejecutando merge a tabla final...")
    merge_sql = build_merge_sql(PROJECT, DATASET, name, id_col="_clockify_id")
    client.query(merge_sql).result()

    sync_type = "full sync" if FULL_SYNC else "upsert"
    print(f"\n[{name}] ✅ {sync_type} completado: {len(rows)} filas desde Clockify Reports API")
    print(f"[{name}] Billable: {billable_mins/60:.2f}h | Non-billable: {nonbillable_mins/60:.2f}h\n")

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
