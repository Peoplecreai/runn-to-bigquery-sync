"""
Script de diagnóstico para verificar el campo billable de Clockify
"""
import json
from clockify_reports_client import fetch_detailed_report
from clockify_simple_transformer import analyze_report_data, transform_clockify_entry

def main():
    print("="*80)
    print("DIAGNÓSTICO: Verificando campo billable de Clockify")
    print("="*80)

    # 1. Obtener datos de Clockify
    print("\n1. Obteniendo datos de Clockify Detailed Report...")
    entries = fetch_detailed_report()
    print(f"   ✓ Total entries obtenidos: {len(entries)}")

    if not entries:
        print("   ❌ No se obtuvieron entries de Clockify")
        return

    # 2. Analizar campo billable en datos RAW de Clockify
    print("\n2. Analizando campo 'billable' en datos RAW de Clockify...")
    billable_count = 0
    non_billable_count = 0
    missing_field_count = 0

    for entry in entries:
        if "billable" not in entry:
            missing_field_count += 1
        elif entry.get("billable") is True:
            billable_count += 1
        else:
            non_billable_count += 1

    print(f"   - Entries con billable=True: {billable_count}")
    print(f"   - Entries con billable=False: {non_billable_count}")
    print(f"   - Entries sin campo billable: {missing_field_count}")

    # 3. Mostrar ejemplos de entries billable y non-billable
    print("\n3. Ejemplos de entries (RAW de Clockify):")

    billable_example = next((e for e in entries if e.get("billable") is True), None)
    if billable_example:
        print("\n   📊 Ejemplo de entry BILLABLE:")
        print(f"      - billable: {billable_example.get('billable')}")
        print(f"      - billableAmount: {billable_example.get('billableAmount')}")
        print(f"      - userName: {billable_example.get('userName')}")
        print(f"      - projectName: {billable_example.get('projectName')}")
        print(f"      - duration: {billable_example.get('timeInterval', {}).get('duration')}s")
    else:
        print("\n   ⚠️  NO se encontraron entries con billable=True")

    non_billable_example = next((e for e in entries if e.get("billable") is False), None)
    if non_billable_example:
        print("\n   📊 Ejemplo de entry NON-BILLABLE:")
        print(f"      - billable: {non_billable_example.get('billable')}")
        print(f"      - billableAmount: {non_billable_example.get('billableAmount')}")
        print(f"      - userName: {non_billable_example.get('userName')}")
        print(f"      - projectName: {non_billable_example.get('projectName')}")
        print(f"      - duration: {non_billable_example.get('timeInterval', {}).get('duration')}s")

    # 4. Transformar entries y verificar que se preserva el campo
    print("\n4. Verificando transformación de entries...")
    transformed = [transform_clockify_entry(e) for e in entries]

    transformed_billable = sum(1 for t in transformed if t.get("is_billable") is True)
    transformed_non_billable = sum(1 for t in transformed if t.get("is_billable") is False)

    print(f"   - Entries transformados con is_billable=True: {transformed_billable}")
    print(f"   - Entries transformados con is_billable=False: {transformed_non_billable}")

    # 5. Verificar horas billable
    total_hours = sum(t["duration_hours"] for t in transformed)
    billable_hours = sum(t["duration_hours"] for t in transformed if t.get("is_billable") is True)
    non_billable_hours = total_hours - billable_hours

    print("\n5. Estadísticas de horas:")
    print(f"   - Total horas: {total_hours:.2f}h")
    print(f"   - Billable horas: {billable_hours:.2f}h")
    print(f"   - Non-billable horas: {non_billable_hours:.2f}h")

    # 6. Usar la función de análisis del transformer
    print("\n6. Usando función analyze_report_data()...")
    stats = analyze_report_data(entries)
    print(f"   - Billable entries según análisis: {stats['billable_entries']}")
    print(f"   - Non-billable entries según análisis: {stats['non_billable_entries']}")
    print(f"   - Billable hours según análisis: {stats['billable_hours']:.2f}h")
    print(f"   - Non-billable hours según análisis: {stats['non_billable_hours']:.2f}h")

    # 7. Mostrar ejemplo transformado
    if billable_example:
        print("\n7. Comparación de entry BILLABLE (antes y después de transformar):")
        print("\n   ANTES (RAW de Clockify):")
        print(json.dumps({
            "billable": billable_example.get("billable"),
            "billableAmount": billable_example.get("billableAmount"),
            "userName": billable_example.get("userName"),
            "projectName": billable_example.get("projectName"),
        }, indent=6))

        transformed_billable_example = transform_clockify_entry(billable_example)
        print("\n   DESPUÉS (transformado para BigQuery):")
        print(json.dumps({
            "is_billable": transformed_billable_example.get("is_billable"),
            "billable_amount": transformed_billable_example.get("billable_amount"),
            "user_name": transformed_billable_example.get("user_name"),
            "project_name": transformed_billable_example.get("project_name"),
        }, indent=6))

    print("\n" + "="*80)
    print("DIAGNÓSTICO COMPLETADO")
    print("="*80)

if __name__ == "__main__":
    main()
