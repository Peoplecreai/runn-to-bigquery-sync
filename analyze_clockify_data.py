"""
Script para analizar los datos que vienen de Clockify y detectar duplicados
antes de que lleguen a BigQuery
"""
import os
from collections import Counter, defaultdict
from clockify_client import fetch_all_time_entries
from clockify_transformer import transform_batch
from datetime import datetime

def analyze_clockify_data():
    print("="*80)
    print("🔍 ANÁLISIS DE DATOS DE CLOCKIFY")
    print("="*80)
    print()

    print("Obteniendo time entries de Clockify...")
    print("(Esto puede tomar un momento)")
    print()

    time_entries = list(fetch_all_time_entries())
    total_entries = len(time_entries)

    print(f"✅ Total time entries obtenidos: {total_entries}")
    print()

    # Análisis 1: Detectar duplicados por Clockify ID
    print("📊 Análisis 1: Detectando duplicados por Clockify ID")
    print("-" * 80)

    clockify_ids = [entry.get("id") for entry in time_entries]
    clockify_id_counts = Counter(clockify_ids)
    duplicates = {k: v for k, v in clockify_id_counts.items() if v > 1}

    if duplicates:
        print(f"⚠️  PROBLEMA: Se encontraron {len(duplicates)} Clockify IDs duplicados:")
        for clockify_id, count in list(duplicates.items())[:10]:
            print(f"  - {clockify_id}: aparece {count} veces")
        if len(duplicates) > 10:
            print(f"  ... y {len(duplicates) - 10} más")
        print()
        print("🔴 CAUSA RAÍZ: Clockify API está devolviendo el mismo time entry múltiples veces!")
        print("   Posibles razones:")
        print("   1. Paginación incorrecta")
        print("   2. El time entry está asignado a múltiples usuarios")
        print("   3. Bug en el API de Clockify")
        print()
    else:
        print("✅ No se encontraron Clockify IDs duplicados")
        print()

    # Análisis 2: Transformar a actuals y verificar IDs numéricos
    print("📊 Análisis 2: Analizando IDs numéricos generados")
    print("-" * 80)

    actuals = transform_batch(time_entries)
    total_actuals = len(actuals)

    print(f"Time entries transformados: {total_actuals}")

    numeric_ids = [actual["id"] for actual in actuals]
    numeric_id_counts = Counter(numeric_ids)
    numeric_duplicates = {k: v for k, v in numeric_id_counts.items() if v > 1}

    if numeric_duplicates:
        print(f"⚠️  PROBLEMA: Se encontraron {len(numeric_duplicates)} IDs numéricos duplicados:")
        for numeric_id, count in list(numeric_duplicates.items())[:10]:
            print(f"  - {numeric_id}: aparece {count} veces")
        if len(numeric_duplicates) > 10:
            print(f"  ... y {len(numeric_duplicates) - 10} más")
        print()
        print("🔴 CAUSA RAÍZ: Múltiples time entries están generando el mismo ID numérico!")
        print("   Esto indica colisiones de hash o IDs no determinísticos.")
        print()
    else:
        print("✅ Todos los IDs numéricos son únicos")
        print()

    # Análisis 3: Verificar mapeo Clockify ID → Numeric ID
    print("📊 Análisis 3: Verificando mapeo Clockify ID → Numeric ID")
    print("-" * 80)

    clockify_to_numeric = defaultdict(set)
    for actual in actuals:
        clockify_id = actual.get("_clockify_id")
        numeric_id = actual["id"]
        if clockify_id:
            clockify_to_numeric[clockify_id].add(numeric_id)

    multi_mapping = {k: v for k, v in clockify_to_numeric.items() if len(v) > 1}
    if multi_mapping:
        print(f"⚠️  PROBLEMA: {len(multi_mapping)} Clockify IDs mapean a múltiples IDs numéricos:")
        for clockify_id, numeric_ids in list(multi_mapping.items())[:10]:
            print(f"  - {clockify_id} → {numeric_ids}")
        print()
        print("🔴 CAUSA RAÍZ: Los IDs numéricos NO son determinísticos!")
        print("   El mismo Clockify ID está generando múltiples IDs numéricos diferentes.")
        print()
    else:
        print("✅ Cada Clockify ID mapea a un único ID numérico (hash determinístico funciona)")
        print()

    # Análisis 4: Buscar Marcela Aburto específicamente
    print("📊 Análisis 4: Analizando datos de Marcela Aburto (10-14 nov 2025)")
    print("-" * 80)

    marcela_actuals = [
        a for a in actuals
        if a["date"] >= "2025-11-10" and a["date"] <= "2025-11-14"
        # Nota: No podemos filtrar por nombre aquí sin hacer lookup de personas
    ]

    print(f"Actuals en el rango de fechas 10-14 nov: {len(marcela_actuals)}")

    if marcela_actuals:
        # Calcular horas totales
        total_minutes = sum(
            a["billableMinutes"] + a["nonbillableMinutes"]
            for a in marcela_actuals
        )
        total_hours = total_minutes / 60.0
        billable_minutes = sum(a["billableMinutes"] for a in marcela_actuals)
        billable_hours = billable_minutes / 60.0

        print(f"Total horas en esas fechas (todos los usuarios): {total_hours:.2f}")
        print(f"Billable hours: {billable_hours:.2f}")
        print()

        # Verificar duplicados por ID
        marcela_ids = [a["id"] for a in marcela_actuals]
        marcela_id_counts = Counter(marcela_ids)
        marcela_duplicates = {k: v for k, v in marcela_id_counts.items() if v > 1}

        if marcela_duplicates:
            print(f"⚠️  PROBLEMA: {len(marcela_duplicates)} IDs duplicados en esas fechas:")
            for id_val, count in list(marcela_duplicates.items())[:5]:
                print(f"  - ID {id_val}: {count} veces")
            print()
        else:
            print("✅ No hay IDs duplicados en esas fechas")
            print()

    # Análisis 5: Detectar colisiones de hash
    print("📊 Análisis 5: Calculando probabilidad de colisiones de hash")
    print("-" * 80)

    unique_clockify_ids = len(set(clockify_ids))
    unique_numeric_ids = len(set(numeric_ids))

    print(f"Clockify IDs únicos: {unique_clockify_ids}")
    print(f"IDs numéricos únicos: {unique_numeric_ids}")

    if unique_numeric_ids < unique_clockify_ids:
        collision_rate = (unique_clockify_ids - unique_numeric_ids) / unique_clockify_ids * 100
        print(f"⚠️  COLISIONES DETECTADAS: {unique_clockify_ids - unique_numeric_ids} colisiones ({collision_rate:.2f}%)")
        print()
        print("🔴 CAUSA RAÍZ: El algoritmo de hash está generando colisiones!")
        print("   Múltiples Clockify IDs están generando el mismo ID numérico.")
        print()

        # Encontrar las colisiones específicas
        numeric_to_clockify = defaultdict(set)
        for actual in actuals:
            clockify_id = actual.get("_clockify_id")
            numeric_id = actual["id"]
            if clockify_id:
                numeric_to_clockify[numeric_id].add(clockify_id)

        collisions = {k: v for k, v in numeric_to_clockify.items() if len(v) > 1}
        if collisions:
            print(f"   Ejemplos de colisiones:")
            for numeric_id, clockify_ids in list(collisions.items())[:5]:
                print(f"   - ID numérico {numeric_id} representa {len(clockify_ids)} Clockify IDs:")
                for cid in list(clockify_ids)[:3]:
                    print(f"     • {cid}")
            print()
    else:
        print("✅ No se detectaron colisiones de hash")
        print()

    # Análisis 6: Ratio de duplicación general
    print("📊 Análisis 6: Ratio de duplicación general")
    print("-" * 80)

    if total_entries > 0 and unique_clockify_ids > 0:
        overall_ratio = total_entries / unique_clockify_ids
        print(f"Total entries: {total_entries}")
        print(f"Clockify IDs únicos: {unique_clockify_ids}")
        print(f"Ratio de duplicación: {overall_ratio:.2f}x")
        print()

        if overall_ratio > 1.1:  # Más del 10% de duplicación
            print(f"⚠️  PROBLEMA: Ratio de duplicación significativo: {overall_ratio:.2f}x")
            print(f"   Esto explica por qué PowerBI reporta {overall_ratio:.2f}x más horas")
            print()

    print("="*80)
    print("🏁 ANÁLISIS COMPLETADO")
    print("="*80)
    print()

    # Resumen y recomendaciones
    print("📋 RESUMEN Y RECOMENDACIONES:")
    print()

    issues_found = 0

    if duplicates:
        issues_found += 1
        print(f"1. 🔴 Clockify API está devolviendo {len(duplicates)} time entries duplicados")
        print("   → Solución: Agregar deduplicación por Clockify ID antes de transformar")
        print()

    if numeric_duplicates:
        issues_found += 1
        print(f"2. 🔴 {len(numeric_duplicates)} colisiones en IDs numéricos")
        print("   → Solución: Usar un ID compuesto más robusto")
        print()

    if multi_mapping:
        issues_found += 1
        print(f"3. 🔴 IDs no determinísticos: {len(multi_mapping)} Clockify IDs generan múltiples IDs")
        print("   → Solución: Verificar que _generate_deterministic_id sea realmente determinístico")
        print()

    if unique_numeric_ids < unique_clockify_ids:
        issues_found += 1
        print(f"4. 🔴 Colisiones de hash: {unique_clockify_ids - unique_numeric_ids} colisiones detectadas")
        print("   → Solución: Usar un algoritmo de hash con más bits o un ID diferente")
        print()

    if issues_found == 0:
        print("✅ No se encontraron problemas en el pipeline de datos de Clockify")
        print("   El problema probablemente está en PowerBI o en las tablas de BigQuery existentes")
        print()
        print("   Próximos pasos:")
        print("   1. Ejecutar debug_duplicates.py para analizar BigQuery")
        print("   2. Revisar los queries/JOINs en PowerBI")
        print("   3. Considerar hacer un FULL_SYNC para limpiar datos antiguos")
        print()
    else:
        print(f"Total de problemas encontrados: {issues_found}")
        print()

if __name__ == "__main__":
    analyze_clockify_data()
