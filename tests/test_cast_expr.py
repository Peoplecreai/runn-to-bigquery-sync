import os
import pathlib
import sys

from google.cloud import bigquery

os.environ["LOG_LEVEL"] = "INFO"

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from runn_sync import _cast_expr


def _field(name: str, field_type: str, *, mode: str = "NULLABLE") -> bigquery.SchemaField:
    return bigquery.SchemaField(name, field_type, mode=mode)


def test_cast_expr_id_scalar_source():
    tgt = _field("id", "STRING")
    src = _field("id", "INT64")

    expr = _cast_expr("id", tgt, src)

    assert expr == "CAST(`id` AS STRING) AS `id`"


def test_cast_expr_id_repeated_source():
    tgt = _field("id", "STRING")
    src = _field("id", "INT64", mode="REPEATED")

    expr = _cast_expr("id", tgt, src)

    assert expr == "SAFE_CAST(`id`[SAFE_OFFSET(0)] AS STRING) AS `id`"
