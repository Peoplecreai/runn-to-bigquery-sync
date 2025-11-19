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
    # Leer esquema desde INFORMATION_SCHEMA
    # Nota: evitamos castear arrays a string
    return f"""
MERGE {tgt} T
USING {stg} S
ON T.{id_col} = S.{id_col}
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
