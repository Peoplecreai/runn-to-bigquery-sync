from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from flask import Flask, jsonify, request
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, BadRequest
from functools import lru_cache

# -----------------------------------------------------------------------------
# Logging / Config
# -----------------------------------------------------------------------------
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
logger = logging.getLogger(__name__)

API = "https://api.runn.io"
RUNN_HOLIDAY_GROUP_ID = os.environ.get("RUNN_HOLIDAY_GROUP_ID")

# Proyecto / dataset / región BQ
PROJ = (
    os.environ.get("BQ_PROJECT")
    or os.environ.get("GOOGLE_CLOUD_PROJECT")
    or os.environ.get("GCP_PROJECT")
)
DS = os.environ.get("BQ_DATASET", "people_analytics")
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

# Token Runn (falla temprano si no está al momento de usarlo)


def _require_runn_api_token() -> str:
    token = os.environ.get("RUNN_API_TOKEN")
    if not token:
        raise RuntimeError("RUNN_API_TOKEN no está definido en el entorno")
    return token


@lru_cache(maxsize=1)
def _runn_headers() -> Dict[str, str]:
    token = _require_runn_api_token()
    return {
        "Authorization": f"Bearer {token}",
        "Accept-Version": "1.0.0",
        "Accept": "application/json",
    }

print(f"DEBUG: Usando proyecto BQ '{PROJ}' y dataset '{DS}' (loc={BQ_LOCATION})")

# -----------------------------------------------------------------------------
# Catálogo de colecciones
# -----------------------------------------------------------------------------
COLLS: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = {
    # Base
    "runn_people": "/people/",
    "runn_projects": "/projects/",
    "runn_clients": "/clients/",
    "runn_roles": "/roles/",
    "runn_teams": "/teams/",
    "runn_skills": "/skills/",
    "runn_people_tags": "/people-tags/",
    "runn_project_tags": "/project-tags/",
    "runn_rate_cards": "/rate-cards/",
    "runn_workstreams": "/workstreams/",
    "runn_assignments": "/assignments/",
    "runn_actuals": "/actuals/",
    "runn_timeoffs_leave": "/time-offs/leave/",
    "runn_timeoffs_rostered": "/time-offs/rostered/",
    "runn_timeoffs_holidays": "/time-offs/holidays/",
    # Nuevas
    "runn_holiday_groups": "/holiday-groups/",
    "runn_placeholders": ("/placeholders/", {}),
    "runn_contracts": ("/contracts/", {"sortBy": "id"}),
    # Custom fields (ejemplos)
    "runn_custom_fields_checkbox_person":  ("/custom-fields/checkbox/", {"model": "PERSON"}),
    "runn_custom_fields_checkbox_project": ("/custom-fields/checkbox/", {"model": "PROJECT"}),
}

# Esquemas con tipos fijos (evita autodetecciones inconsistentes)
SCHEMA_OVERRIDES: Dict[str, List[bigquery.SchemaField]] = {
    "runn_actuals": [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("billableMinutes", "INT64"),
        bigquery.SchemaField("nonbillableMinutes", "INT64"),
        bigquery.SchemaField("phaseId", "INT64"),
        bigquery.SchemaField("projectId", "INT64"),
        bigquery.SchemaField("personId", "INT64"),
        bigquery.SchemaField("roleId", "INT64"),
        bigquery.SchemaField("workstreamId", "STRING"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
    ],
    "runn_timeoffs_leave": [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("personId", "STRING"),
        bigquery.SchemaField("startDate", "DATE"),
        bigquery.SchemaField("endDate", "DATE"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("minutesPerDay", "INT64"),
    ],
    "runn_timeoffs_rostered": [
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("personId", "STRING"),
        bigquery.SchemaField("startDate", "DATE"),
        bigquery.SchemaField("endDate", "DATE"),
        bigquery.SchemaField("note", "STRING"),
        bigquery.SchemaField("createdAt", "TIMESTAMP"),
        bigquery.SchemaField("updatedAt", "TIMESTAMP"),
        bigquery.SchemaField("minutesPerDay", "INT64"),
    ],
}

# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------
def q(name: str) -> str:
    """Cita identificadores (columnas) con backticks."""
    return f"`{name}`"

def state_table() -> str:
    return f"{PROJ}.{DS}.__runn_sync_state"

def ensure_dataset(bq: bigquery.Client) -> None:
    ds_id = f"{PROJ}.{DS}"
    try:
        bq.get_dataset(ds_id)
    except NotFound:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        bq.create_dataset(ds)
        logger.info("Dataset creado: %s", ds_id)

def ensure_state_table(bq: bigquery.Client) -> None:
    ensure_dataset(bq)
    bq.query(f"""
    CREATE TABLE IF NOT EXISTS `{state_table()}`
    (
      table_name STRING NOT NULL,
      last_success TIMESTAMP,
      PRIMARY KEY(table_name) NOT ENFORCED
    )
    """).result()

def get_last_success(bq: bigquery.Client, name: str) -> Optional[dt.datetime]:
    qjob = bq.query(
        f"SELECT last_success FROM `{state_table()}` WHERE table_name=@t",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("t", "STRING", name)]
        ),
    )
    for r in qjob.result():
        return r[0]
    return None

def set_last_success(bq: bigquery.Client, name: str, ts: dt.datetime) -> None:
    bq.query(
      """
      MERGE `{tbl}` T
      USING (SELECT @t AS table_name, @ts AS last_success) S
      ON T.table_name=S.table_name
      WHEN MATCHED THEN UPDATE SET last_success=S.last_success
      WHEN NOT MATCHED THEN INSERT(table_name,last_success) VALUES(S.table_name,S.last_success)
      """.format(tbl=state_table()),
      job_config=bigquery.QueryJobConfig(
        query_parameters=[
          bigquery.ScalarQueryParameter("t","STRING",name),
          bigquery.ScalarQueryParameter("ts","TIMESTAMP",ts.isoformat())
        ]
      )
    ).result()

def _supports_modified_after(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments", "contracts", "placeholders"}

def _accepts_date_window(path: str) -> bool:
    tail = path.rstrip("/").split("/")[-1]
    return tail in {"actuals", "assignments"}

# ---- HTTP fetch con backoff simple (429/5xx) ----
def fetch_all(path: str,
              since_iso: Optional[str],
              limit=200,
              extra_params: Optional[Dict[str,str]]=None) -> List[Dict]:
    s = requests.Session(); s.headers.update(_runn_headers())
    out: List[Dict] = []
    cursor: Optional[str] = None
    backoff = 2

    while True:
        params: Dict[str, str] = {"limit": str(limit)}
        if extra_params:
            params.update({k: v for k, v in extra_params.items() if v is not None and v != ""})
        if since_iso and _supports_modified_after(path):
            params["modifiedAfter"] = since_iso
        if cursor:
            params["cursor"] = cursor

        r = s.get(API + path, params=params, timeout=60)

        if r.status_code in (429, 500, 502, 503, 504):
            wait = int(r.headers.get("Retry-After", str(backoff)))
            logger.warning("HTTP %s en %s, reintentando en %ss", r.status_code, path, wait)
            time.sleep(wait)
            backoff = min(backoff * 2, 60)
            continue

        if r.status_code == 404:
            logger.info("[WARN] 404 Not Found: %s params=%s (ignorado)", API+path, params)
            return []

        r.raise_for_status()
        payload = r.json()

        values = payload.get("values", payload if isinstance(payload, list) else [])
        if isinstance(values, dict):
            values = [values]
        out.extend(values)

        cursor = payload.get("nextCursor")
        if not cursor:
            break

    logger.debug("Fetched %d rows from %s (params=%s since=%s)", len(out), path, extra_params or {}, since_iso)
    return out

# -----------------------------------------------------------------------------
# BigQuery helpers de schema/casts
# -----------------------------------------------------------------------------
def _create_empty_timeoff_table_if_needed(table_base: str, bq: bigquery.Client) -> None:
    if table_base not in {"runn_timeoffs_leave", "runn_timeoffs_rostered"}:
        return
    tgt = f"{PROJ}.{DS}.{table_base}"
    try:
        bq.get_table(tgt)
        return
    except NotFound:
        pass
    schema = SCHEMA_OVERRIDES[table_base]
    tbl = bigquery.Table(tgt, schema=schema)
    tbl.location = BQ_LOCATION
    bq.create_table(tbl)

def _schema_to_map(schema: List[bigquery.SchemaField]) -> Dict[str, bigquery.SchemaField]:
    return {c.name: c for c in schema}

def _null_expr(field: bigquery.SchemaField) -> str:
    field_type = field.field_type.upper()
    mode = (field.mode or "NULLABLE").upper()
    name = field.name
    if mode == "REPEATED" and field_type != "RECORD":
        return f"CAST(NULL AS ARRAY<{field_type}>) AS {q(name)}"
    if mode == "REPEATED":
        return f"NULL AS {q(name)}"
    if field_type == "RECORD":
        return f"NULL AS {q(name)}"
    return f"CAST(NULL AS {field_type}) AS {q(name)}"

def _cast_expr(
    col: str,
    field: bigquery.SchemaField,
    source_field: Optional[bigquery.SchemaField] = None,
) -> str:
    tgt_type = field.field_type.upper()
    tgt_mode = (field.mode or "NULLABLE").upper()
    src_type = (source_field.field_type.upper() if source_field else None)
    src_mode = ((source_field.mode or "NULLABLE").upper() if source_field else None)

    # id como STRING para clave MERGE
    if col == "id":
        if src_mode == "REPEATED" or src_type == "RECORD":
            return f"TO_JSON_STRING({q(col)}) AS {q(col)}"
        return f"CAST({q(col)} AS STRING) AS {q(col)}"

    if tgt_mode == "REPEATED" and tgt_type != "RECORD":
        return f"SAFE_CAST({q(col)} AS ARRAY<{tgt_type}>) AS {q(col)}"
    if tgt_mode == "REPEATED":
        return f"{q(col)} AS {q(col)}"

    if tgt_type == "STRING":
        if src_mode == "REPEATED" and src_type == "STRING":
            return f"ARRAY_TO_STRING({q(col)}, ',') AS {q(col)}"
        if src_mode == "REPEATED" or src_type == "RECORD":
            return f"TO_JSON_STRING({q(col)}) AS {q(col)}"
        return f"CAST({q(col)} AS STRING) AS {q(col)}"
    if tgt_type in {"INT64", "INTEGER"}:
        return f"SAFE_CAST({q(col)} AS INT64) AS {q(col)}"
    if tgt_type in {"FLOAT64", "FLOAT"}:
        return f"SAFE_CAST({q(col)} AS FLOAT64) AS {q(col)}"
    if tgt_type in {"BOOL", "BOOLEAN"}:
        return f"SAFE_CAST({q(col)} AS BOOL) AS {q(col)}"
    if tgt_type == "DATE":
        return f"SAFE_CAST({q(col)} AS DATE) AS {q(col)}"
    if tgt_type == "TIMESTAMP":
        return f"SAFE_CAST({q(col)} AS TIMESTAMP) AS {q(col)}"
    if tgt_type == "DATETIME":
        return f"SAFE_CAST({q(col)} AS DATETIME) AS {q(col)}"
    return f"{q(col)} AS {q(col)}"

def _ensure_target_table(table_base: str, stg_schema: List[bigquery.SchemaField], bq: bigquery.Client) -> List[bigquery.SchemaField]:
    """Crea la tabla destino si no existe. Si hay override conocido, úsalo."""
    tgt_id = f"{PROJ}.{DS}.{table_base}"
    try:
        tgt_tbl = bq.get_table(tgt_id)
        return tgt_tbl.schema
    except NotFound:
        pass

    if table_base in SCHEMA_OVERRIDES:
        schema = SCHEMA_OVERRIDES[table_base]
        has_date = any(c.name == "date" and c.field_type.upper()=="DATE" for c in schema)
        if has_date:
            qddl = f"""
            CREATE TABLE `{tgt_id}`
            PARTITION BY DATE(date)
            CLUSTER BY personId, projectId
            AS SELECT * FROM `{PROJ}.{DS}._stg__{table_base}` WHERE 1=0
            """
            bq.query(qddl).result()
            bq.update_table(bigquery.Table(tgt_id, schema=schema), ["schema"])
            return schema
        else:
            tbl = bigquery.Table(tgt_id, schema=schema)
            tbl.location = BQ_LOCATION
            bq.create_table(tbl)
            return schema
    else:
        tbl = bigquery.Table(tgt_id, schema=stg_schema)
        tbl.location = BQ_LOCATION
        bq.create_table(tbl)
        return stg_schema

# -----------------------------------------------------------------------------
# Carga y MERGE a BQ
# -----------------------------------------------------------------------------
def load_merge(table_base: str, rows: List[Dict], bq: bigquery.Client) -> int:
    if not rows:
        _create_empty_timeoff_table_if_needed(table_base, bq)
        return 0

    stg = f"{PROJ}.{DS}._stg__{table_base}"
    tgt = f"{PROJ}.{DS}.{table_base}"

    # 1) Carga staging (autodetecta y crea si no existe)
    job = bq.load_table_from_json(
        rows,
        stg,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True
        )
    )
    job.result()

    stg_schema = bq.get_table(stg).schema
    # 2) Target (usa overrides si hay)
    tgt_schema = _ensure_target_table(table_base, stg_schema, bq)

    # 3) SELECT tipado para alinear a destino
    tgt_map = _schema_to_map(tgt_schema)
    stg_map = _schema_to_map(stg_schema)
    stg_cols = set(stg_map.keys())

    select_parts: List[str] = []
    for col, field in tgt_map.items():
        if col in stg_cols:
            select_parts.append(_cast_expr(col, field, stg_map.get(col)))
        else:
            select_parts.append(_null_expr(field))
    select_sql = ",\n        ".join(select_parts)

    # columnas para MERGE (intersección menos id)
    non_id_cols = [c for c in tgt_map.keys() if c != "id" and c in stg_cols]

    set_clause  = ", ".join([f"T.{q(c)}=S.{q(c)}" for c in non_id_cols]) if non_id_cols else ""
    insert_cols = ["id"] + [c for c in tgt_map.keys() if c != "id" and c in stg_cols]
    insert_cols_sql = ", ".join(q(c) for c in insert_cols)
    insert_vals = [f"S.{q('id')}"] + [f"S.{q(c)}" for c in insert_cols if c != "id"]
    insert_vals_sql = ", ".join(insert_vals)

    merge_sql = f"""
    MERGE `{tgt}` T
    USING (
      SELECT
        {select_sql}
      FROM `{stg}`
    ) S
    ON CAST(T.{q('id')} AS STRING) = S.{q('id')}
    """

    if set_clause:
        merge_sql += f"""
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    """

    merge_sql += f"""
    WHEN NOT MATCHED THEN INSERT ({insert_cols_sql})
    VALUES ({insert_vals_sql})
    """

    try:
        print("---- MERGE SQL ----\n" + merge_sql, flush=True)
        bq.query(merge_sql).result()
    except BadRequest as e:
        # Log completo del SQL para depuración
        logger.exception("BigQuery BadRequest durante MERGE de %s", table_base)
        raise
    return bq.get_table(tgt).num_rows

# -----------------------------------------------------------------------------
# Purga de ventana (para backfill por rango)
# -----------------------------------------------------------------------------
def purge_scope(bq: bigquery.Client,
                table_base: str,
                person: Optional[str],
                dfrom: Optional[str],
                dto: Optional[str]) -> None:
    if not (dfrom and dto):
        return
    if table_base not in {"runn_actuals", "runn_assignments"}:
        return

    if person:
        qtxt = f"""
        DELETE FROM `{PROJ}.{DS}.{table_base}`
        WHERE CAST({q('personId')} AS STRING)=@p
          AND DATE({q('date')}) BETWEEN @d1 AND @d2
        """
        params = [
            bigquery.ScalarQueryParameter("p","STRING", person),
            bigquery.ScalarQueryParameter("d1","DATE", dfrom),
            bigquery.ScalarQueryParameter("d2","DATE", dto),
        ]
    else:
        qtxt = f"""
        DELETE FROM `{PROJ}.{DS}.{table_base}`
        WHERE DATE({q('date')}) BETWEEN @d1 AND @d2
        """
        params = [
            bigquery.ScalarQueryParameter("d1","DATE", dfrom),
            bigquery.ScalarQueryParameter("d2","DATE", dto),
        ]
    bq.query(qtxt, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def parse_only(raw: Optional[List[str]]) -> Optional[List[str]]:
    if not raw:
        return None
    out: List[str] = []
    for item in raw:
        out.extend([p.strip() for p in item.split(",") if p.strip()])
    seen = set(); ordered = []
    for k in out:
        if k not in seen:
            seen.add(k); ordered.append(k)
    return ordered

def build_parser(*, exit_on_error: bool = True) -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Sync Runn data into BigQuery",
        exit_on_error=exit_on_error,
    )
    ap.add_argument("--mode", choices=["full", "delta"], default="delta")
    ap.add_argument("--delta-days", type=int, default=90)
    ap.add_argument("--overlap-days", type=int, default=7, help="relee últimos N días en delta")
    ap.add_argument("--only", action="append", help="repetible o coma-separado: runn_people,runn_projects,…")
    # Backfill dirigido
    ap.add_argument("--range-from", dest="range_from", help="YYYY-MM-DD")
    ap.add_argument("--range-to", dest="range_to", help="YYYY-MM-DD")
    ap.add_argument("--person-id", dest="person_id", help="filtrar por persona (opcional)")
    return ap

# -----------------------------------------------------------------------------
# Orquestación
# -----------------------------------------------------------------------------
def run_sync(args: argparse.Namespace) -> Dict[str, Any]:
    _require_runn_api_token()
    bq = bigquery.Client(project=PROJ, location=BQ_LOCATION)
    ensure_state_table(bq)

    only_list = parse_only(args.only)
    now = dt.datetime.now(dt.timezone.utc)

    collections: Dict[str, Union[str, Tuple[str, Dict[str, str]]]] = dict(COLLS)
    if RUNN_HOLIDAY_GROUP_ID:
        collections["runn_timeoffs_holidays"] = (
            "/time-offs/holidays/",
            {"holidayGroupId": RUNN_HOLIDAY_GROUP_ID},
        )

    targets = collections if not only_list else {k: collections[k] for k in only_list if k in collections}
    summary: Dict[str, int] = {}

    for tbl, spec in targets.items():
        path, fixed_params = (spec if isinstance(spec, tuple) else (spec, None))

        last_checkpoint = get_last_success(bq, tbl)

        # Parámetros dinámicos (fecha/persona)
        dyn_params: Dict[str, Optional[str]] = {}
        purge_from: Optional[str] = None
        purge_to: Optional[str] = None

        if _accepts_date_window(path):
            if args.range_from and args.range_to:
                dyn_params["dateFrom"] = args.range_from
                dyn_params["dateTo"] = args.range_to
                purge_from, purge_to = args.range_from, args.range_to
            if args.person_id:
                dyn_params["personId"] = args.person_id

            if (args.mode == "delta") and not (args.range_from and args.range_to):
                window_days = max(args.delta_days, args.overlap_days, 0)
                start_date = (now - dt.timedelta(days=window_days)).date().isoformat()
                end_date = now.date().isoformat()
                dyn_params.setdefault("dateFrom", start_date)
                dyn_params.setdefault("dateTo", end_date)
                purge_from = dyn_params.get("dateFrom")
                purge_to = dyn_params.get("dateTo")

        extra = dict(fixed_params or {})
        extra.update({k: v for k, v in dyn_params.items() if v})

        # Estrategia modifiedAfter vs ventana
        since_iso: Optional[str] = None
        use_modified_after = False
        if (args.range_from and args.range_to) and _accepts_date_window(path):
            use_modified_after = False
        elif purge_from and purge_to and _accepts_date_window(path):
            use_modified_after = False
        elif args.mode == "delta":
            if last_checkpoint:
                overlap = dt.timedelta(days=max(args.overlap_days, 0))
                since = last_checkpoint - overlap
            else:
                since = now - dt.timedelta(days=args.delta_days)
            since_iso = since.strftime("%Y-%m-%dT%H:%M:%SZ")
            use_modified_after = _supports_modified_after(path)

        # Purga de ventana previa (solo actuals/assignments)
        if tbl in {"runn_actuals", "runn_assignments"} and purge_from and purge_to:
            purge_scope(bq, tbl, args.person_id, purge_from, purge_to)

        # Descarga
        rows = fetch_all(path, since_iso if use_modified_after else None, extra_params=extra)

        # Carga/MERGE
        n = load_merge(tbl, rows, bq)
        summary[tbl] = int(n)

        # Checkpoint
        if rows:
            max_upd = None
            for r in rows:
                v = r.get("updatedAt") or r.get("updated_at")
                if v:
                    try:
                        t = dt.datetime.fromisoformat(v.replace("Z", "+00:00"))
                        max_upd = t if (max_upd is None or t > max_upd) else max_upd
                    except Exception:
                        pass
            new_checkpoint = max_upd or now
            if last_checkpoint and new_checkpoint < last_checkpoint:
                new_checkpoint = last_checkpoint
            set_last_success(bq, tbl, new_checkpoint)

    logger.info("Sync completado: %s", summary)
    return {"ok": True, "loaded": summary}

# -----------------------------------------------------------------------------
# HTTP Server
# -----------------------------------------------------------------------------
def _request_args(req) -> argparse.Namespace:
    parser = build_parser(exit_on_error=False)
    payload = req.get_json(silent=True) or {}

    def _value_list(name: str) -> List[str]:
        values: List[str] = []
        alt = name.replace("_", "-")
        for key in {name, alt}:
            if isinstance(payload, dict) and key in payload:
                raw_val = payload[key]
                if isinstance(raw_val, list):
                    values.extend([str(v) for v in raw_val if v not in (None, "")])
                elif raw_val not in (None, ""):
                    values.append(str(raw_val))
            if req.args.getlist(key):
                values.extend([v for v in req.args.getlist(key) if v not in (None, "")])
        return values

    def _single(name: str) -> Optional[str]:
        vals = _value_list(name)
        return vals[0] if vals else None

    argv: List[str] = []

    if (mode := _single("mode")) is not None:
        argv.extend(["--mode", mode])

    for key, flag in (("delta_days", "--delta-days"), ("overlap_days", "--overlap-days")):
        val = _single(key)
        if val is not None:
            argv.extend([flag, val])

    for entry in _value_list("only"):
        argv.extend(["--only", entry])

    for key, flag in (("range_from", "--range-from"), ("range_to", "--range-to"), ("person_id", "--person-id")):
        val = _single(key)
        if val is not None:
            argv.extend([flag, val])

    try:
        return parser.parse_args(argv)
    except (argparse.ArgumentError, SystemExit) as exc:
        message = str(exc) or "invalid parameters"
        raise ValueError(message) from exc

def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/healthz")
    def health() -> Any:
        return jsonify({"ok": True})

    @app.route("/run", methods=["GET", "POST"])
    def trigger() -> Any:
        try:
            args = _request_args(request)
        except ValueError as exc:
            return jsonify({"ok": False, "error": str(exc)}), 400

        try:
            result = run_sync(args)
        except Exception as exc:  # pragma: no cover
            logger.exception("Fallo al ejecutar la sincronización")
            return jsonify({"ok": False, "error": str(exc)}), 500

        return jsonify(result)

    @app.get("/")
    def root() -> Any:
        return jsonify({"ok": True, "message": "runn sync listo"})

    return app

APP = create_app()

def serve() -> None:
    port = int(os.environ.get("PORT", "8080"))
    logger.info("Iniciando servidor HTTP en el puerto %s", port)
    APP.run(host="0.0.0.0", port=port)

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    parser = build_parser()
    args = parser.parse_args(argv)
    result = run_sync(args)
    print(json.dumps(result, ensure_ascii=False))
    return result

if __name__ == "__main__":
    if "--serve" in sys.argv:
        sys.argv.remove("--serve")
        serve()
    elif os.environ.get("RUNN_SYNC_SERVER", "").lower() in {"1", "true", "yes"}:
        serve()
    else:
        main()
