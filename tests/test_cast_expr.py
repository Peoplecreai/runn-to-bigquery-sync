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

    assert expr == (
        "CASE\n"
        "  WHEN `id` IS NULL OR COALESCE(ARRAY_LENGTH(`id`), 0) = 0 THEN NULL\n"
        "  WHEN JSON_TYPE(TO_JSON(`id`[SAFE_OFFSET(0)])) = 'string' THEN JSON_VALUE(TO_JSON(`id`[SAFE_OFFSET(0)]))\n"
        "  ELSE TO_JSON_STRING(`id`[SAFE_OFFSET(0)])\n"
        "END AS `id`"
    )


def test_cast_expr_id_repeated_nested_source():
    tgt = _field("id", "STRING")
    src = bigquery.SchemaField(
        "id",
        "RECORD",
        mode="REPEATED",
        fields=[bigquery.SchemaField("value", "STRING", mode="REPEATED")],
    )

    expr = _cast_expr("id", tgt, src)

    assert expr == (
        "CASE\n"
        "  WHEN `id` IS NULL OR COALESCE(ARRAY_LENGTH(`id`), 0) = 0 THEN NULL\n"
        "  WHEN JSON_TYPE(TO_JSON(`id`[SAFE_OFFSET(0)])) = 'string' THEN JSON_VALUE(TO_JSON(`id`[SAFE_OFFSET(0)]))\n"
        "  ELSE TO_JSON_STRING(`id`[SAFE_OFFSET(0)])\n"
        "END AS `id`"
    )
