"""
Script de debugging para identificar la causa de duplicación en BigQuery
Investiga múltiples hipótesis sobre por qué PowerBI reporta 2.6x más horas
"""
import os
from google.cloud import bigquery
from datetime import datetime

PROJECT = os.getenv("BQ_PROJECT")
DATASET = os.getenv("BQ_DATASET", "people_analytics")

def run_debug_queries():
    client = bigquery.Client(project=PROJECT)

    print("="*80)
    print("🔍 ANÁLISIS DE DUPLICACIÓN - MARCELA ABURTO (10-14 NOV 2025)")
    print("="*80)
    print()

    # Query 1: Contar registros totales para Marcela en esas fechas
    query1 = f"""
    SELECT
        COUNT(*) as total_registros,
        COUNT(DISTINCT id) as ids_unicos,
        COUNT(DISTINCT _clockify_id) as clockify_ids_unicos
    FROM `{PROJECT}.{DATASET}.runn_actuals`
    WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
      AND personId IN (
        SELECT id FROM `{PROJECT}.{DATASET}.runn_people`
        WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
           OR LOWER(email) LIKE '%marcela%'
      )
    """

    print("📊 Query 1: Contando registros totales")
    print("-" * 80)
    result1 = client.query(query1).result()
    for row in result1:
        print(f"Total registros en BigQuery: {row.total_registros}")
        print(f"IDs únicos (id numérico): {row.ids_unicos}")
        print(f"Clockify IDs únicos (_clockify_id): {row.clockify_ids_unicos}")
        print()

        if row.total_registros != row.ids_unicos:
            print("⚠️  PROBLEMA ENCONTRADO: Hay múltiples registros con el mismo ID!")
            print(f"   Duplicación esperada: {row.total_registros / row.ids_unicos:.2f}x")
            print()

    # Query 2: Suma de horas directamente en BigQuery
    query2 = f"""
    SELECT
        SUM(billableMinutes) / 60.0 as billable_hours,
        SUM(nonbillableMinutes) / 60.0 as nonbillable_hours,
        (SUM(billableMinutes) + SUM(nonbillableMinutes)) / 60.0 as total_hours,
        COUNT(*) as num_registros
    FROM `{PROJECT}.{DATASET}.runn_actuals`
    WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
      AND personId IN (
        SELECT id FROM `{PROJECT}.{DATASET}.runn_people`
        WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
           OR LOWER(email) LIKE '%marcela%'
      )
    """

    print("📊 Query 2: Suma de horas en BigQuery")
    print("-" * 80)
    result2 = client.query(query2).result()
    for row in result2:
        print(f"Billable hours: {row.billable_hours:.2f}")
        print(f"Non-billable hours: {row.nonbillable_hours:.2f}")
        print(f"Total hours: {row.total_hours:.2f}")
        print(f"Número de registros: {row.num_registros}")
        print()

        if row.total_hours > 45:  # Más de 40 horas esperadas
            print(f"⚠️  PROBLEMA: BigQuery tiene {row.total_hours:.2f} horas (esperado ~40)")
            print(f"   Multiplicador: {row.total_hours / 40:.2f}x")
            print()

    # Query 3: Buscar IDs duplicados
    query3 = f"""
    SELECT
        id,
        _clockify_id,
        COUNT(*) as veces_repetido,
        STRING_AGG(DISTINCT date) as fechas,
        SUM(billableMinutes) / 60.0 as total_billable_hours
    FROM `{PROJECT}.{DATASET}.runn_actuals`
    WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
      AND personId IN (
        SELECT id FROM `{PROJECT}.{DATASET}.runn_people`
        WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
           OR LOWER(email) LIKE '%marcela%'
      )
    GROUP BY id, _clockify_id
    HAVING COUNT(*) > 1
    ORDER BY veces_repetido DESC
    LIMIT 10
    """

    print("📊 Query 3: Buscando IDs duplicados")
    print("-" * 80)
    result3 = list(client.query(query3).result())
    if result3:
        print("⚠️  DUPLICADOS ENCONTRADOS:")
        for row in result3:
            print(f"  - ID: {row.id}, Clockify ID: {row._clockify_id}")
            print(f"    Repetido {row.veces_repetido} veces, Fechas: {row.fechas}")
            print(f"    Total billable hours: {row.total_billable_hours:.2f}")
        print()
    else:
        print("✅ No se encontraron IDs duplicados en BigQuery")
        print()

    # Query 4: Verificar duplicados en runn_people que puedan causar productos cartesianos
    query4 = f"""
    SELECT
        id,
        firstName,
        lastName,
        email,
        COUNT(*) as veces_repetido
    FROM `{PROJECT}.{DATASET}.runn_people`
    WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
       OR LOWER(email) LIKE '%marcela%'
    GROUP BY id, firstName, lastName, email
    """

    print("📊 Query 4: Verificando tabla runn_people")
    print("-" * 80)
    result4 = list(client.query(query4).result())
    if result4:
        print(f"Registros de Marcela Aburto encontrados: {len(result4)}")
        for row in result4:
            print(f"  - ID: {row.id}, Nombre: {row.firstName} {row.lastName}, Email: {row.email}")
            if row.veces_repetido > 1:
                print(f"    ⚠️  DUPLICADO: Aparece {row.veces_repetido} veces")
        print()

        if len(result4) > 1:
            print("⚠️  PROBLEMA: Hay múltiples registros de Marcela Aburto en runn_people")
            print("   Esto podría causar productos cartesianos en PowerBI")
            print()
    else:
        print("⚠️  No se encontró a Marcela Aburto en runn_people")
        print()

    # Query 5: Detectar colisiones de hash (múltiples clockify_ids con el mismo id numérico)
    query5 = f"""
    SELECT
        id,
        COUNT(DISTINCT _clockify_id) as num_clockify_ids,
        STRING_AGG(DISTINCT _clockify_id LIMIT 5) as clockify_ids
    FROM `{PROJECT}.{DATASET}.runn_actuals`
    WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
      AND personId IN (
        SELECT id FROM `{PROJECT}.{DATASET}.runn_people`
        WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
           OR LOWER(email) LIKE '%marcela%'
      )
    GROUP BY id
    HAVING COUNT(DISTINCT _clockify_id) > 1
    LIMIT 10
    """

    print("📊 Query 5: Detectando colisiones de hash")
    print("-" * 80)
    result5 = list(client.query(query5).result())
    if result5:
        print("⚠️  COLISIONES DE HASH DETECTADAS:")
        for row in result5:
            print(f"  - ID numérico: {row.id}")
            print(f"    Representa {row.num_clockify_ids} Clockify IDs diferentes:")
            print(f"    {row.clockify_ids}")
        print()
        print("🔴 CAUSA RAÍZ: Las colisiones de hash están causando que múltiples")
        print("   time entries diferentes se traten como el mismo registro!")
        print()
    else:
        print("✅ No se detectaron colisiones de hash")
        print()

    # Query 6: Analizar distribución de horas por fecha
    query6 = f"""
    SELECT
        date,
        COUNT(*) as num_registros,
        COUNT(DISTINCT id) as ids_unicos,
        SUM(billableMinutes) / 60.0 as billable_hours,
        SUM(nonbillableMinutes) / 60.0 as nonbillable_hours
    FROM `{PROJECT}.{DATASET}.runn_actuals`
    WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
      AND personId IN (
        SELECT id FROM `{PROJECT}.{DATASET}.runn_people`
        WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
           OR LOWER(email) LIKE '%marcela%'
      )
    GROUP BY date
    ORDER BY date
    """

    print("📊 Query 6: Distribución por fecha")
    print("-" * 80)
    result6 = client.query(query6).result()
    for row in result6:
        ratio = row.num_registros / row.ids_unicos if row.ids_unicos > 0 else 0
        print(f"{row.date}: {row.num_registros} registros ({row.ids_unicos} únicos, ratio: {ratio:.2f}x)")
        print(f"  Billable: {row.billable_hours:.2f}h, Non-billable: {row.nonbillable_hours:.2f}h")
    print()

    # Query 7: Buscar registros con el mismo clockify_id pero diferente id numérico
    query7 = f"""
    SELECT
        _clockify_id,
        COUNT(DISTINCT id) as num_ids_diferentes,
        STRING_AGG(DISTINCT CAST(id AS STRING) LIMIT 5) as ids_numericos
    FROM `{PROJECT}.{DATASET}.runn_actuals`
    WHERE date BETWEEN '2025-11-10' AND '2025-11-14'
      AND personId IN (
        SELECT id FROM `{PROJECT}.{DATASET}.runn_people`
        WHERE LOWER(firstName || ' ' || lastName) LIKE '%marcela%aburto%'
           OR LOWER(email) LIKE '%marcela%'
      )
      AND _clockify_id IS NOT NULL
    GROUP BY _clockify_id
    HAVING COUNT(DISTINCT id) > 1
    LIMIT 10
    """

    print("📊 Query 7: Clockify IDs con múltiples IDs numéricos")
    print("-" * 80)
    result7 = list(client.query(query7).result())
    if result7:
        print("⚠️  PROBLEMA: El mismo time entry de Clockify tiene múltiples IDs numéricos:")
        for row in result7:
            print(f"  - Clockify ID: {row._clockify_id}")
            print(f"    Tiene {row.num_ids_diferentes} IDs numéricos diferentes: {row.ids_numericos}")
        print()
        print("🔴 CAUSA RAÍZ: Los IDs numéricos NO son determinísticos!")
        print("   El mismo time entry se está insertando múltiples veces con IDs diferentes.")
        print()
    else:
        print("✅ Cada Clockify ID tiene un solo ID numérico (hash funcionando correctamente)")
        print()

    print("="*80)
    print("🏁 ANÁLISIS COMPLETADO")
    print("="*80)
    print()
    print("PRÓXIMOS PASOS:")
    print("1. Revisar los problemas encontrados arriba (marcados con ⚠️ o 🔴)")
    print("2. Si hay colisiones de hash o IDs no determinísticos, revisar clockify_transformer.py")
    print("3. Si hay duplicados en runn_people, revisar la sincronización de personas")
    print("4. Si BigQuery tiene las horas correctas (~40), el problema está en PowerBI")
    print("5. Si BigQuery ya tiene 103 horas, el problema está en el pipeline de carga")
    print()

if __name__ == "__main__":
    run_debug_queries()
