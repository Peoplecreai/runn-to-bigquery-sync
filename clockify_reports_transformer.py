"""
Transformador de datos del Clockify Reports API a formato compatible con runn_actuals
El Reports API tiene un formato diferente y más completo que el time entries API regular
"""
from datetime import datetime
from typing import Dict, Any, List, Optional
import hashlib


def _generate_deterministic_id(string_id: str) -> int:
    """
    Genera un ID numérico determinístico a partir de un string ID.
    Usa MD5 para garantizar que el mismo string siempre genere el mismo ID.
    """
    hash_object = hashlib.md5(string_id.encode('utf-8'))
    hash_int = int.from_bytes(hash_object.digest()[:8], byteorder='big')
    return hash_int % (10**10)


def transform_detailed_report_entry_to_actual(
    entry: Dict[str, Any],
    user_map: Optional[Dict[str, int]] = None,
    project_map: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """
    Transforma un entry del Clockify Detailed Report al formato de actual de Runn.

    Estructura del Detailed Report entry (ejemplo):
    {
      "_id": "65abc...",
      "description": "Trabajo en proyecto X",
      "userName": "Juan Pérez",
      "userEmail": "juan@example.com",
      "userId": "5f9...",
      "projectName": "Proyecto X",
      "projectId": "5f9...",
      "clientName": "Cliente Y",
      "isBillable": true,
      "billableAmount": 150.00,
      "costAmount": 100.00,
      "timeInterval": {
        "start": "2024-01-15T08:00:00Z",
        "end": "2024-01-15T10:30:00Z",
        "duration": 9000  # en segundos
      },
      "tags": [{"name": "desarrollo"}],
      ...
    }
    """

    # Extraer información temporal
    time_interval = entry.get("timeInterval", {})
    start_str = time_interval.get("start", "")
    end_str = time_interval.get("end", "")
    duration_seconds = time_interval.get("duration", 0)

    # Calcular minutos trabajados (duration viene en segundos)
    total_minutes = int(duration_seconds / 60) if duration_seconds else 0

    # Fecha (extraer del campo start o usar campo date si existe)
    if start_str:
        date_str = start_str.split("T")[0]
    else:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    # Determinar si es billable - El Reports API usa "isBillable" (más confiable)
    is_billable = entry.get("isBillable", False)
    billable_minutes = total_minutes if is_billable else 0
    nonbillable_minutes = 0 if is_billable else total_minutes

    # Descripción
    description = entry.get("description", "")

    # IDs: usar el _id del report como fuente de verdad
    clockify_id = entry.get("_id") or entry.get("id", "")
    numeric_id = _generate_deterministic_id(clockify_id) if clockify_id else 0

    # Usuario - el Reports API incluye userEmail directamente
    user_email = entry.get("userEmail", "").lower().strip()
    user_id_str = entry.get("userId", "")

    # Intentar mapear por email primero (más confiable)
    person_id = None
    matched_by_email = False

    if user_map and user_email:
        # Si user_map está usando emails como keys
        if user_email in user_map:
            person_id = user_map[user_email]
            matched_by_email = True
        # Si user_map está usando user IDs de Clockify como keys
        elif user_id_str in user_map:
            person_id = user_map[user_id_str]
            matched_by_email = True

    # Si no hay match, generar ID determinístico
    if person_id is None:
        person_id = _generate_deterministic_id(user_id_str) if user_id_str else 0

    # Proyecto - similar lógica
    project_id_str = entry.get("projectId", "")
    project_name = entry.get("projectName", "")

    project_id = None
    if project_map and project_name:
        # Intentar match por nombre
        project_id = project_map.get(project_name)

    if project_id is None and project_id_str:
        project_id = _generate_deterministic_id(project_id_str)

    # Timestamps
    created_at = start_str or datetime.utcnow().isoformat() + "Z"
    updated_at = end_str or created_at

    # Construir el actual en formato Runn
    actual = {
        "id": numeric_id,
        "date": date_str,
        "billableMinutes": billable_minutes,
        "nonbillableMinutes": nonbillable_minutes,
        "billableNote": description if is_billable else "",
        "nonbillableNote": description if not is_billable else "",
        "phaseId": None,
        "personId": person_id,
        "projectId": project_id,
        "roleId": None,
        "workstreamId": None,
        "createdAt": created_at,
        "updatedAt": updated_at,
        # Campos adicionales de Clockify para auditoría
        "_clockify_id": clockify_id,
        "_clockify_user_id": user_id_str,
        "_clockify_user_email": user_email,
        "_clockify_user_name": entry.get("userName", ""),
        "_clockify_matched_by_email": matched_by_email,
        "_clockify_project_id": project_id_str,
        "_clockify_project_name": project_name,
        "_clockify_client_name": entry.get("clientName", ""),
        "_clockify_is_billable": is_billable,
        "_clockify_billable_amount": entry.get("billableAmount", 0.0),
        "_clockify_cost_amount": entry.get("costAmount", 0.0),
        "_clockify_duration_seconds": duration_seconds,
    }

    return actual


def transform_batch(
    entries: List[Dict[str, Any]],
    user_map: Optional[Dict[str, int]] = None,
    project_map: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Transforma un batch de entries del detailed report a actuals"""
    return [
        transform_detailed_report_entry_to_actual(entry, user_map, project_map)
        for entry in entries
    ]


def build_user_map_by_email_from_runn(
    runn_people: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Construye un mapeo de email a personId usando los datos de Runn.
    Esto permite hacer match entre usuarios de Clockify (por email) y Runn.

    Args:
        runn_people: Lista de personas de Runn

    Returns:
        Dict[email, personId]: Mapeo de email a personId de Runn
    """
    user_map = {}
    for person in runn_people:
        email = person.get("email", "").lower().strip()
        person_id = person.get("id")
        if email and person_id:
            user_map[email] = person_id
    return user_map


def build_project_map_by_name_from_runn(
    runn_projects: List[Dict[str, Any]]
) -> Dict[str, int]:
    """
    Construye un mapeo de nombre de proyecto a projectId usando los datos de Runn.

    Args:
        runn_projects: Lista de proyectos de Runn

    Returns:
        Dict[projectName, projectId]: Mapeo de nombre a projectId de Runn
    """
    project_map = {}
    for project in runn_projects:
        name = project.get("name", "").strip()
        project_id = project.get("id")
        if name and project_id:
            project_map[name] = project_id
    return project_map


def analyze_report_data(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Analiza los datos del detailed report para generar estadísticas.
    Útil para debugging y validación.
    """
    if not entries:
        return {"error": "No entries to analyze"}

    total_entries = len(entries)
    billable_entries = [e for e in entries if e.get("isBillable")]
    non_billable_entries = [e for e in entries if not e.get("isBillable")]

    total_seconds = sum(e.get("timeInterval", {}).get("duration", 0) for e in entries)
    billable_seconds = sum(e.get("timeInterval", {}).get("duration", 0) for e in billable_entries)
    non_billable_seconds = total_seconds - billable_seconds

    total_hours = total_seconds / 3600
    billable_hours = billable_seconds / 3600
    non_billable_hours = non_billable_seconds / 3600

    # Usuarios únicos
    unique_users = set(e.get("userEmail", "").lower() for e in entries if e.get("userEmail"))

    # Proyectos únicos
    unique_projects = set(e.get("projectName", "") for e in entries if e.get("projectName"))

    # IDs únicos
    unique_ids = set(e.get("_id") or e.get("id", "") for e in entries if (e.get("_id") or e.get("id")))

    # Verificar duplicados
    all_ids = [e.get("_id") or e.get("id", "") for e in entries if (e.get("_id") or e.get("id"))]
    duplicates = len(all_ids) - len(unique_ids)

    stats = {
        "total_entries": total_entries,
        "billable_entries": len(billable_entries),
        "non_billable_entries": len(non_billable_entries),
        "billable_percentage": f"{len(billable_entries) / total_entries * 100:.1f}%" if total_entries > 0 else "0%",
        "total_hours": round(total_hours, 2),
        "billable_hours": round(billable_hours, 2),
        "non_billable_hours": round(non_billable_hours, 2),
        "unique_users": len(unique_users),
        "unique_projects": len(unique_projects),
        "unique_ids": len(unique_ids),
        "duplicates_detected": duplicates,
        "users": sorted(unique_users),
        "projects": sorted(unique_projects),
    }

    return stats


if __name__ == "__main__":
    # Test del transformador
    import json

    sample_entry = {
        "_id": "65abc123def456",
        "description": "Desarrollo de feature X",
        "userName": "Juan Pérez",
        "userEmail": "juan@example.com",
        "userId": "user123",
        "projectName": "Proyecto Alpha",
        "projectId": "proj456",
        "clientName": "Cliente Beta",
        "isBillable": True,
        "billableAmount": 150.00,
        "costAmount": 100.00,
        "timeInterval": {
            "start": "2024-01-15T08:00:00Z",
            "end": "2024-01-15T10:30:00Z",
            "duration": 9000
        },
        "tags": [{"name": "desarrollo"}]
    }

    print("Entry del Clockify Detailed Report:")
    print(json.dumps(sample_entry, indent=2))

    print("\n" + "="*50 + "\n")

    actual = transform_detailed_report_entry_to_actual(sample_entry)
    print("Actual transformado (formato Runn):")
    print(json.dumps(actual, indent=2))

    print("\n" + "="*50 + "\n")

    # Test de análisis
    entries = [sample_entry]
    stats = analyze_report_data(entries)
    print("Estadísticas del report:")
    print(json.dumps(stats, indent=2))
