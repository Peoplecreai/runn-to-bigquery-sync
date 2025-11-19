"""
Transformador de datos de Clockify a formato compatible con runn_actuals
Mantiene la misma estructura en BigQuery para no afectar Power BI
"""
from datetime import datetime
from typing import Dict, Any, List, Optional
import hashlib


def _generate_deterministic_id(string_id: str) -> int:
    """
    Genera un ID numérico determinístico a partir de un string ID.
    Usa MD5 para garantizar que el mismo string siempre genere el mismo ID,
    independientemente de reinicios del proceso o versión de Python.

    Args:
        string_id: El ID en formato string (ej: ID de Clockify)

    Returns:
        Un entero de hasta 10 dígitos generado de forma determinística
    """
    # MD5 es determinístico y genera el mismo hash siempre para el mismo input
    hash_object = hashlib.md5(string_id.encode('utf-8'))
    # Convertir a entero usando los primeros 8 bytes del hash
    hash_int = int.from_bytes(hash_object.digest()[:8], byteorder='big')
    # Limitar a 10 dígitos para que sea manejable en BigQuery
    return hash_int % (10**10)


def transform_time_entry_to_actual(
    time_entry: Dict[str, Any],
    user_map: Optional[Dict[str, int]] = None,
    project_map: Optional[Dict[str, int]] = None,
) -> Dict[str, Any]:
    """
    Transforma un time entry de Clockify al formato de actual de Runn.

    Estructura de Clockify time entry:
    {
      "id": "5f9...",
      "description": "...",
      "userId": "5f9...",
      "billable": true,
      "projectId": "5f9...",
      "taskId": "5f9...",
      "timeInterval": {
        "start": "2024-01-15T08:00:00Z",
        "end": "2024-01-15T10:30:00Z",
        "duration": "PT2H30M"
      },
      "workspaceId": "...",
      "tags": [...],
      ...
    }

    Estructura de Runn actual:
    {
      "id": 123,
      "date": "2024-01-15",
      "billableMinutes": 150,
      "nonbillableMinutes": 0,
      "billableNote": "...",
      "nonbillableNote": "",
      "phaseId": null,
      "personId": 456,
      "projectId": 789,
      "roleId": 10,
      "workstreamId": null,
      "createdAt": "2024-01-15T10:30:00Z",
      "updatedAt": "2024-01-15T10:30:00Z"
    }
    """

    # Extraer información temporal
    time_interval = time_entry.get("timeInterval", {})
    start_str = time_interval.get("start", "")
    end_str = time_interval.get("end", "")
    duration_str = time_interval.get("duration")

    # Calcular minutos trabajados
    total_minutes = 0
    if start_str and end_str:
        try:
            start = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
            end = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
            total_minutes = int((end - start).total_seconds() / 60)
        except:
            # Si no se puede parsear, intentar con duration
            total_minutes = parse_duration_to_minutes(duration_str)
    elif duration_str:
        total_minutes = parse_duration_to_minutes(duration_str)

    # Fecha (extraer solo la parte de fecha del start)
    date_str = start_str.split("T")[0] if start_str else datetime.utcnow().strftime("%Y-%m-%d")

    # Determinar si es billable o no
    is_billable = time_entry.get("billable", False)
    billable_minutes = total_minutes if is_billable else 0
    nonbillable_minutes = 0 if is_billable else total_minutes

    # Descripción
    description = time_entry.get("description", "")

    # IDs: convertir de Clockify string IDs a integer IDs compatibles con Runn
    # Usamos MD5 hash determinístico para generar IDs numéricos consistentes
    # Esto garantiza que el mismo time entry siempre genere el mismo ID,
    # evitando duplicados en BigQuery
    clockify_id = time_entry.get("id", "")
    numeric_id = _generate_deterministic_id(clockify_id) if clockify_id else 0

    user_id_str = time_entry.get("userId", "")
    person_id = user_map.get(user_id_str) if user_map else _generate_deterministic_id(user_id_str) if user_id_str else 0

    project_id_str = time_entry.get("projectId", "")
    project_id = project_map.get(project_id_str) if project_map else (
        _generate_deterministic_id(project_id_str) if project_id_str else None
    )

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
        "phaseId": None,  # Clockify no tiene fases
        "personId": person_id,
        "projectId": project_id,
        "roleId": None,  # Clockify no tiene roles directamente
        "workstreamId": None,  # Clockify no tiene workstreams
        "createdAt": created_at,
        "updatedAt": updated_at,
        # Campos adicionales de Clockify para referencia
        "_clockify_id": clockify_id,
        "_clockify_user_id": user_id_str,
        "_clockify_project_id": project_id_str,
        "_clockify_task_id": time_entry.get("taskId"),
        "_clockify_tags": time_entry.get("tagIds", []),
    }

    return actual


def parse_duration_to_minutes(duration_str: Optional[str]) -> int:
    """
    Parsea una duración ISO 8601 a minutos.
    Ejemplos: "PT2H30M" = 150 minutos, "PT45M" = 45 minutos, "PT1H" = 60 minutos
    """
    if not duration_str:
        return 0

    try:
        # Remover PT del inicio
        duration = duration_str.replace("PT", "")

        hours = 0
        minutes = 0

        # Extraer horas
        if "H" in duration:
            parts = duration.split("H")
            hours = int(parts[0])
            duration = parts[1]

        # Extraer minutos
        if "M" in duration:
            minutes = int(duration.split("M")[0])

        return hours * 60 + minutes
    except:
        return 0


def transform_batch(
    time_entries: List[Dict[str, Any]],
    user_map: Optional[Dict[str, int]] = None,
    project_map: Optional[Dict[str, int]] = None,
) -> List[Dict[str, Any]]:
    """Transforma un batch de time entries a actuals"""
    return [
        transform_time_entry_to_actual(entry, user_map, project_map)
        for entry in time_entries
    ]


def build_user_map_from_runn(runn_people: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Construye un mapeo de email a personId usando los datos de Runn.
    Esto permite mantener consistencia entre los IDs de personas.
    """
    user_map = {}
    for person in runn_people:
        email = person.get("email", "").lower()
        person_id = person.get("id")
        if email and person_id:
            user_map[email] = person_id
    return user_map


def build_project_map_from_runn(runn_projects: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Construye un mapeo de nombre de proyecto a projectId usando los datos de Runn.
    Esto permite mantener consistencia entre los IDs de proyectos.
    """
    project_map = {}
    for project in runn_projects:
        name = project.get("name", "").strip()
        project_id = project.get("id")
        if name and project_id:
            project_map[name] = project_id
    return project_map


if __name__ == "__main__":
    # Test del transformador
    import json

    sample_time_entry = {
        "id": "5f9a1b2c3d4e5f6g7h8i9j0k",
        "description": "Desarrollo de feature X",
        "userId": "user123",
        "billable": True,
        "projectId": "proj456",
        "taskId": "task789",
        "timeInterval": {
            "start": "2024-01-15T08:00:00Z",
            "end": "2024-01-15T10:30:00Z",
            "duration": "PT2H30M"
        },
        "workspaceId": "workspace123",
        "tagIds": ["tag1", "tag2"]
    }

    print("Time entry de Clockify:")
    print(json.dumps(sample_time_entry, indent=2))

    print("\n" + "="*50 + "\n")

    actual = transform_time_entry_to_actual(sample_time_entry)
    print("Actual transformado (formato Runn):")
    print(json.dumps(actual, indent=2))
