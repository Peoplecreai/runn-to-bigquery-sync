from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def get_bq_client(project: str | None = None):
    return bigquery.Client(project=project)

def truncate_table(client: bigquery.Client, table_id: str):
    """
    Trunca (borra todos los datos de) una tabla en BigQuery.
    Útil para hacer un full sync limpiando duplicados.
    """
    try:
        # Verificar que la tabla existe
        client.get_table(table_id)
        # Truncar la tabla
        query = f"TRUNCATE TABLE `{table_id}`"
        client.query(query).result()
        print(f"Tabla {table_id} truncada exitosamente")
    except NotFound:
        print(f"Tabla {table_id} no existe, se creará durante el merge")
    except Exception as e:
        print(f"Error truncando tabla {table_id}: {e}")
        raise


def drop_table_if_exists(client: bigquery.Client, table_id: str):
    """Elimina una tabla si existe (ignora si no existe)."""

    try:
        client.delete_table(table_id)
        print(f"Tabla {table_id} eliminada (legacy)")
    except NotFound:
        # No hacer nada si ya no existe
        pass
    except Exception as exc:  # pragma: no cover - defensivo
        print(f"Error eliminando {table_id}: {exc}")
        raise

def deduplicate_table_by_column(client: bigquery.Client, table_id: str, unique_col: str):
    """
    Elimina duplicados de una tabla en BigQuery manteniendo solo el registro más reciente.
    Útil para limpiar duplicados históricos antes de hacer merge.

    Args:
        client: Cliente de BigQuery
        table_id: ID completo de la tabla (ej: project.dataset.table)
        unique_col: Columna que debe ser única (ej: '_clockify_id')
    """
    try:
        # Verificar que la tabla existe
        client.get_table(table_id)

        # Contar duplicados antes de limpiar
        count_query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT {unique_col}) as unique_rows
        FROM `{table_id}`
        WHERE {unique_col} IS NOT NULL
        """
        result = list(client.query(count_query).result())
        if result:
            total_rows = result[0].total_rows
            unique_rows = result[0].unique_rows
            duplicates = total_rows - unique_rows

            if duplicates > 0:
                print(f"\n⚠️  Duplicados detectados en {table_id}:")
                print(f"   Total rows: {total_rows}")
                print(f"   Rows únicos: {unique_rows}")
                print(f"   Duplicados a eliminar: {duplicates}")
                print(f"   Factor de duplicación: {total_rows / unique_rows:.2f}x\n")

                # Detectar qué columna de timestamp existe en la tabla
                parts = table_id.split('.')
                if len(parts) == 3:
                    project, dataset, table_name = parts
                    timestamp_col = _get_timestamp_column_from_table(client, table_id)
                else:
                    # Si no podemos parsear, usar updatedAt como fallback
                    timestamp_col = 'updatedAt'

                # Crear tabla temporal con datos deduplicados
                temp_table = f"{table_id}_dedup_temp"
                dedup_query = f"""
                CREATE OR REPLACE TABLE `{temp_table}` AS
                SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY {unique_col} ORDER BY {timestamp_col} DESC) as rn
                    FROM `{table_id}`
                    WHERE {unique_col} IS NOT NULL
                )
                WHERE rn = 1
                """
                client.query(dedup_query).result()
                print(f"✓ Tabla temporal creada: {temp_table}")

                # Reemplazar tabla original con la deduplicada
                replace_query = f"""
                CREATE OR REPLACE TABLE `{table_id}` AS
                SELECT * EXCEPT(rn)
                FROM `{temp_table}`
                """
                client.query(replace_query).result()
                print(f"✓ Tabla {table_id} deduplicada exitosamente")

                # Eliminar tabla temporal
                client.delete_table(temp_table)
                print(f"✓ Tabla temporal eliminada\n")
                print(f"✅ Deduplicación completada: {total_rows} → {unique_rows} rows")
            else:
                print(f"✓ No se encontraron duplicados en {table_id}")

    except NotFound:
        print(f"Tabla {table_id} no existe aún, no hay duplicados que limpiar")
    except Exception as e:
        print(f"Error deduplicando tabla {table_id}: {e}")
        raise

def load_staging(client: bigquery.Client, table_id: str, rows: list[dict]):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True
    )
    load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
    return load_job.result()  # espera a terminar

def ensure_target_schema_matches_stg(client: bigquery.Client, stg_table: str, tgt_table: str):
    stg = client.get_table(stg_table)
    try:
        tgt = client.get_table(tgt_table)
    except NotFound:
        table = bigquery.Table(tgt_table, schema=stg.schema)
        client.create_table(table)
        return
    # Añadir columnas nuevas si aparecieron en staging
    tgt_cols = {f.name for f in tgt.schema}
    for f in stg.schema:
        if f.name not in tgt_cols:
            client.query(f"ALTER TABLE `{tgt_table}` ADD COLUMN {f.name} {f.field_type}").result()

def build_merge_sql(project: str, dataset: str, name: str, id_col: str = "id"):
    stg = f"`{project}.{dataset}._stg__{name}`"
    tgt = f"`{project}.{dataset}.{name}`"

    # Para claves que pueden ser NULL, necesitamos manejar el match de manera especial
    # Usamos IS NOT DISTINCT FROM para que NULL = NULL sea verdadero
    match_condition = f"T.{id_col} = S.{id_col}"
    if id_col.startswith("_"):  # Campos adicionales como _clockify_id pueden ser NULL
        match_condition = f"T.{id_col} IS NOT DISTINCT FROM S.{id_col} AND S.{id_col} IS NOT NULL"

    # Detectar qué columna de timestamp existe (updatedAt o updated_at)
    timestamp_col = _get_timestamp_column(project, dataset, name)

    # Leer esquema desde INFORMATION_SCHEMA
    # Nota: evitamos castear arrays a string
    return f"""
MERGE {tgt} T
USING (
  -- Deduplicar staging: si hay múltiples rows con el mismo id_col, tomar solo uno
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY {id_col} ORDER BY {timestamp_col} DESC) as rn
    FROM {stg}
    WHERE {id_col} IS NOT NULL
  )
  WHERE rn = 1
) S
ON {match_condition}
WHEN MATCHED THEN UPDATE SET
  {', '.join([f'T.{col} = S.{col}' for col in _select_columns_sql(project, dataset, name, id_col)])}
WHEN NOT MATCHED THEN INSERT ({', '.join(_select_columns_sql(project, dataset, name))})
VALUES ({', '.join([f'S.{c}' for c in _select_columns_sql(project, dataset, name)])})
"""

def _select_columns_sql(project: str, dataset: str, name: str, skip: str | None = None) -> list[str]:
    q = f"""
SELECT column_name
FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '_stg__{name}'
ORDER BY ordinal_position
"""
    client = get_bq_client(project)
    cols = [row[0] for row in client.query(q).result()]
    if skip and skip in cols:
        cols.remove(skip)
    return cols

def _get_timestamp_column(project: str, dataset: str, name: str) -> str:
    """
    Detecta qué columna de timestamp existe en la tabla staging.
    Retorna 'updatedAt', 'updated_at', o 'id' como fallback.
    """
    cols = _select_columns_sql(project, dataset, name)

    # Preferir updatedAt (camelCase de Runn)
    if 'updatedAt' in cols:
        return 'updatedAt'
    # Fallback a updated_at (snake_case)
    if 'updated_at' in cols:
        return 'updated_at'
    # Si no existe ninguna, usar id como fallback
    return 'id'

def _get_timestamp_column_from_table(client: bigquery.Client, table_id: str) -> str:
    """
    Detecta qué columna de timestamp existe en una tabla cualquiera (no necesariamente staging).
    Retorna 'updatedAt', 'updated_at', o 'id' como fallback.
    """
    try:
        table = client.get_table(table_id)
        col_names = [field.name for field in table.schema]

        # Preferir updatedAt (camelCase de Runn)
        if 'updatedAt' in col_names:
            return 'updatedAt'
        # Fallback a updated_at (snake_case)
        if 'updated_at' in col_names:
            return 'updated_at'
        # Si no existe ninguna, usar id como fallback
        return 'id'
    except Exception:
        # Si hay algún error, usar updatedAt como fallback
        return 'updatedAt'
