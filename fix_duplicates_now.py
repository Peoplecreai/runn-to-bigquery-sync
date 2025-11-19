#!/usr/bin/env python3
"""
Script de emergencia para limpiar duplicados en runn_actuals AHORA MISMO.
Resuelve el problema de 2.6x en PowerBI eliminando duplicados históricos.

Uso:
    python fix_duplicates_now.py

Este script:
1. Cuenta los duplicados actuales en BigQuery
2. Elimina duplicados manteniendo solo el registro más reciente por _clockify_id
3. Reporta el antes y después

IMPORTANTE: Requiere que las variables de entorno BQ_PROJECT y BQ_DATASET estén configuradas.
"""
import os
import sys
from bq_utils import get_bq_client, deduplicate_table_by_column

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET", "people_analytics")
TABLE_NAME = "runn_actuals"

def main():
    if not PROJECT:
        print("❌ ERROR: La variable de entorno BQ_PROJECT no está configurada")
        print("   Ejemplo: export BQ_PROJECT=tu-proyecto-gcp")
        return 1

    print("="*80)
    print("🔧 FIX DE DUPLICADOS - LIMPIEZA INMEDIATA")
    print("="*80)
    print()
    print(f"Proyecto: {PROJECT}")
    print(f"Dataset: {DATASET}")
    print(f"Tabla: {TABLE_NAME}")
    print()

    client = get_bq_client(PROJECT)
    table_id = f"{PROJECT}.{DATASET}.{TABLE_NAME}"

    # Ejecutar deduplicación
    print("Iniciando limpieza de duplicados...")
    print()
    deduplicate_table_by_column(client, table_id, "_clockify_id")

    print()
    print("="*80)
    print("✅ LIMPIEZA COMPLETADA")
    print("="*80)
    print()
    print("PRÓXIMOS PASOS:")
    print("1. Refresca tu dashboard de PowerBI")
    print("2. Verifica que Marcela Aburto ahora muestre ~40 horas (no 103)")
    print("3. Si aún ves duplicados, ejecuta este script nuevamente")
    print()
    print("NOTA: El pipeline de sincronización ya está configurado para prevenir")
    print("      duplicados futuros. Este script solo limpia duplicados históricos.")
    print()

    return 0

if __name__ == "__main__":
    sys.exit(main())
