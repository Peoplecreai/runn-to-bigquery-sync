"""
Cliente para Clockify Reports API
Usa el Detailed Report endpoint que es más confiable para datos billable/non-billable
"""
import os
import time
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

# Reports API usa una URL base diferente
REPORTS_BASE_URL = os.getenv("CLOCKIFY_REPORTS_BASE_URL", "https://reports.api.clockify.me/v1")
API_KEY = os.getenv("CLOCKIFY_API_KEY")
WORKSPACE_ID = os.getenv("CLOCKIFY_WORKSPACE_ID")

session = requests.Session()
session.headers.update({
    "X-Api-Key": API_KEY,
    "Content-Type": "application/json",
})


@retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
def _post(url, payload):
    """Hace request POST con retry automático"""
    r = session.post(url, json=payload, timeout=120)
    if r.status_code >= 500:
        r.raise_for_status()
    if r.status_code == 429:
        time.sleep(2)
        r.raise_for_status()
    r.raise_for_status()
    return r.json()


def fetch_detailed_report(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    page_size: int = 1000
) -> List[Dict[str, Any]]:
    """
    Obtiene el detailed report de Clockify.
    Este endpoint es más confiable que time entries para datos billable/non-billable.

    Args:
        start_date: Fecha inicio (opcional, por defecto últimos 90 días)
        end_date: Fecha fin (opcional, por defecto hoy)
        page_size: Tamaño de página (máximo 1000)

    Returns:
        List[Dict]: Lista de time entries del detailed report
    """
    if not WORKSPACE_ID:
        raise ValueError("CLOCKIFY_WORKSPACE_ID no está configurado")

    # Por defecto, últimos 90 días
    if not end_date:
        end_date = datetime.utcnow()
    if not start_date:
        start_date = end_date - timedelta(days=90)

    # Formato para Reports API (YYYY-MM-DDTHH:mm:ss.sssZ)
    date_range_start = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
    date_range_end = end_date.strftime("%Y-%m-%dT23:59:59.999Z")

    url = f"{REPORTS_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/detailed"

    all_entries = []
    page = 1

    while True:
        # Payload para el detailed report
        payload = {
            "dateRangeStart": date_range_start,
            "dateRangeEnd": date_range_end,
            "detailedFilter": {
                "page": page,
                "pageSize": page_size,
                "sortColumn": "DATE"
            },
            "exportType": "JSON",
            "includeTimeEntryIds": True,  # Importante para deduplicación
        }

        print(f"Obteniendo detailed report - página {page}...")
        data = _post(url, payload)

        # El detailed report devuelve datos en data['timeentries']
        time_entries = data.get("timeentries", [])

        if not time_entries:
            break

        all_entries.extend(time_entries)
        print(f"  ✓ Obtenidos {len(time_entries)} entries (total acumulado: {len(all_entries)})")

        # Si devolvió menos de page_size, no hay más páginas
        if len(time_entries) < page_size:
            break

        page += 1

    print(f"\n✅ Total entries obtenidos del detailed report: {len(all_entries)}")
    return all_entries


def fetch_summary_report(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Obtiene el summary report de Clockify para validación.
    Útil para verificar totales de horas billable/non-billable.

    Args:
        start_date: Fecha inicio
        end_date: Fecha fin

    Returns:
        Dict: Summary report data
    """
    if not WORKSPACE_ID:
        raise ValueError("CLOCKIFY_WORKSPACE_ID no está configurado")

    if not end_date:
        end_date = datetime.utcnow()
    if not start_date:
        start_date = end_date - timedelta(days=90)

    date_range_start = start_date.strftime("%Y-%m-%dT00:00:00.000Z")
    date_range_end = end_date.strftime("%Y-%m-%dT23:59:59.999Z")

    url = f"{REPORTS_BASE_URL}/workspaces/{WORKSPACE_ID}/reports/summary"

    payload = {
        "dateRangeStart": date_range_start,
        "dateRangeEnd": date_range_end,
        "summaryFilter": {
            "groups": ["USER", "PROJECT"]
        },
        "exportType": "JSON"
    }

    data = _post(url, payload)
    return data


if __name__ == "__main__":
    # Test del cliente de reports
    print("Testing Clockify Reports API client...")
    print(f"Workspace ID: {WORKSPACE_ID}")
    print(f"Reports Base URL: {REPORTS_BASE_URL}")

    try:
        print("\n1. Obteniendo detailed report...")
        entries = fetch_detailed_report()
        print(f"   ✓ Entries encontrados: {len(entries)}")

        if entries:
            print("\n2. Ejemplo de entry del detailed report:")
            import json
            print(json.dumps(entries[0], indent=2))

            # Análisis de campos billable
            billable_count = sum(1 for e in entries if e.get("isBillable"))
            non_billable_count = len(entries) - billable_count

            print(f"\n3. Análisis de billable status:")
            print(f"   - Billable entries: {billable_count}")
            print(f"   - Non-billable entries: {non_billable_count}")
            print(f"   - Total: {len(entries)}")

            # Calcular horas totales
            total_duration_seconds = sum(e.get("timeInterval", {}).get("duration", 0) for e in entries)
            total_hours = total_duration_seconds / 3600
            print(f"\n4. Total horas: {total_hours:.2f}")

        print("\n5. Obteniendo summary report...")
        summary = fetch_summary_report()
        print("   ✓ Summary obtenido")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
