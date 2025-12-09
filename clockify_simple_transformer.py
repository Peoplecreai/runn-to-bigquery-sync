"""
Transformador simple de datos del Clockify Reports API.
No hace mapeo con Runn - envía los datos tal como vienen de Clockify.
"""
from datetime import datetime
from typing import Dict, Any, List


def transform_clockify_entry(
    entry: Dict[str, Any],
    user_map: Dict[str, int] = None,
    project_map: Dict[str, int] = None
) -> Dict[str, Any]:
    """
    Transforma un entry del Clockify Detailed Report manteniendo su estructura original.
    Agrega referencias a IDs de Runn para poder hacer JOINs en BigQuery.

    Args:
        entry: Entry del Clockify Detailed Report
        user_map: Mapeo de email → runn_person_id (opcional)
        project_map: Mapeo de project_name → runn_project_id (opcional)

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

    # Calcular horas y minutos
    duration_hours = round(duration_seconds / 3600, 2) if duration_seconds else 0.0
    duration_minutes = int(duration_seconds / 60) if duration_seconds else 0

    # Fecha (extraer del campo start)
    if start_str:
        date_str = start_str.split("T")[0]
    else:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    # IDs originales de Clockify
    clockify_id = entry.get("_id") or entry.get("id", "")
    user_id = entry.get("userId", "")
    project_id = entry.get("projectId", "")
    client_id = entry.get("clientId", "")

    # Información del usuario
    user_name = entry.get("userName", "")
    user_email = entry.get("userEmail", "")

    # Información del proyecto
    project_name = entry.get("projectName", "")
    client_name = entry.get("clientName", "")

    # Información de facturación
    is_billable = entry.get("isBillable", False)
    billable_amount = entry.get("billableAmount", 0.0)
    cost_amount = entry.get("costAmount", 0.0)

    # Descripción y tags
    description = entry.get("description", "")
    tags = entry.get("tags", [])
    tag_names = [tag.get("name", "") for tag in tags] if tags else []

    # Mapeo con IDs de Runn (para poder hacer JOINs en BigQuery)
    runn_person_id = None
    person_matched_by_email = False

    if user_map and user_email:
        runn_person_id = user_map.get(user_email)
        person_matched_by_email = runn_person_id is not None

    runn_project_id = None
    project_matched_by_name = False

    if project_map and project_name:
        runn_project_id = project_map.get(project_name)
        project_matched_by_name = runn_project_id is not None

    # Construir el registro para BigQuery
    record = {
        # Identificadores de Clockify
        "clockify_id": clockify_id,
        "user_id": user_id,
        "project_id": project_id,
        "client_id": client_id,

        # Información del usuario
        "user_name": user_name,
        "user_email": user_email,

        # Información del proyecto
        "project_name": project_name,
        "client_name": client_name,

        # Tiempo
        "date": date_str,
        "start_time": start_str,
        "end_time": end_str,
        "duration_seconds": duration_seconds,
        "duration_minutes": duration_minutes,
        "duration_hours": duration_hours,

        # Facturación
        "is_billable": is_billable,
        "billable_amount": billable_amount,
        "cost_amount": cost_amount,

        # Descripción y tags
        "description": description,
        "tags": tag_names,

        # Task information (si existe)
        "task_id": entry.get("taskId", ""),
        "task_name": entry.get("taskName", ""),

        # Referencias a Runn (para JOINs en BigQuery)
        "runn_person_id": runn_person_id,
        "runn_project_id": runn_project_id,
        "person_matched_by_email": person_matched_by_email,
        "project_matched_by_name": project_matched_by_name,

        # Timestamps
        "created_at": start_str or datetime.utcnow().isoformat() + "Z",
        "updated_at": end_str or start_str or datetime.utcnow().isoformat() + "Z",
    }

    return record


def transform_batch(
    entries: List[Dict[str, Any]],
    user_map: Dict[str, int] = None,
    project_map: Dict[str, int] = None
) -> List[Dict[str, Any]]:
    """
    Transforma un batch de entries del detailed report.

    Args:
        entries: Lista de entries de Clockify
        user_map: Mapeo de email → runn_person_id (opcional)
        project_map: Mapeo de project_name → runn_project_id (opcional)
    """
    return [
        transform_clockify_entry(entry, user_map, project_map)
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
        "clientId": "client789",
        "isBillable": True,
        "billableAmount": 150.00,
        "costAmount": 100.00,
        "timeInterval": {
            "start": "2024-01-15T08:00:00Z",
            "end": "2024-01-15T10:30:00Z",
            "duration": 9000
        },
        "tags": [{"name": "desarrollo"}, {"name": "backend"}],
        "taskId": "task123",
        "taskName": "Feature X"
    }

    print("Entry del Clockify Detailed Report:")
    print(json.dumps(sample_entry, indent=2))

    print("\n" + "="*50 + "\n")

    record = transform_clockify_entry(sample_entry)
    print("Registro transformado (datos de Clockify):")
    print(json.dumps(record, indent=2))

    print("\n" + "="*50 + "\n")

    # Test de análisis
    entries = [sample_entry]
    stats = analyze_report_data(entries)
    print("Estadísticas del report:")
    print(json.dumps(stats, indent=2))
