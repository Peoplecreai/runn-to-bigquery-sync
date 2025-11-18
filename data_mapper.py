"""
Data mapper to transform Clockify API responses to Runn-compatible format.
This maintains backward compatibility with existing BigQuery tables.
"""

def map_user_to_people(clockify_user):
    """
    Maps Clockify user to Runn people format.

    Clockify user structure:
    {
        "id": "userId",
        "email": "user@example.com",
        "name": "User Name",
        "status": "ACTIVE",
        "profilePicture": "url",
        "memberships": [...],
        "settings": {...}
    }
    """
    return {
        "id": clockify_user.get("id"),
        "name": clockify_user.get("name"),
        "email": clockify_user.get("email"),
        "status": clockify_user.get("status", "ACTIVE").lower(),
        "profile_picture": clockify_user.get("profilePicture"),
        "default_billable_rate": clockify_user.get("defaultRate"),
        "memberships": clockify_user.get("memberships", []),
        "settings": clockify_user.get("settings"),
        # Campos adicionales de Clockify que pueden ser útiles
        "activeWorkspace": clockify_user.get("activeWorkspace"),
        "customFields": clockify_user.get("customFields", []),
    }


def map_project_to_project(clockify_project):
    """
    Maps Clockify project to Runn project format.

    Clockify project structure:
    {
        "id": "projectId",
        "name": "Project Name",
        "clientId": "clientId",
        "clientName": "Client Name",
        "hourlyRate": {"amount": 100, "currency": "USD"},
        "costRate": {"amount": 50, "currency": "USD"},
        "billable": true,
        "archived": false,
        "duration": "...",
        "estimate": {...}
    }
    """
    hourly_rate = clockify_project.get("hourlyRate") or {}
    cost_rate = clockify_project.get("costRate") or {}

    return {
        "id": clockify_project.get("id"),
        "name": clockify_project.get("name"),
        "client_id": clockify_project.get("clientId"),
        "client_name": clockify_project.get("clientName"),
        "billable": clockify_project.get("billable", True),
        "archived": clockify_project.get("archived", False),
        "color": clockify_project.get("color"),
        # Rate information
        "hourly_rate": hourly_rate.get("amount"),
        "hourly_rate_currency": hourly_rate.get("currency"),
        "cost_rate": cost_rate.get("amount"),
        "cost_rate_currency": cost_rate.get("currency"),
        # Additional fields
        "public": clockify_project.get("public"),
        "template": clockify_project.get("template"),
        "duration": clockify_project.get("duration"),
        "estimate": clockify_project.get("estimate"),
        "budgetEstimate": clockify_project.get("budgetEstimate"),
        "memberships": clockify_project.get("memberships", []),
    }


def map_client_to_client(clockify_client):
    """
    Maps Clockify client to Runn client format.

    Clockify client structure:
    {
        "id": "clientId",
        "name": "Client Name",
        "workspaceId": "workspaceId",
        "archived": false
    }
    """
    return {
        "id": clockify_client.get("id"),
        "name": clockify_client.get("name"),
        "workspace_id": clockify_client.get("workspaceId"),
        "archived": clockify_client.get("archived", False),
        "address": clockify_client.get("address"),
        "email": clockify_client.get("email"),
    }


def map_tag_to_tag(clockify_tag, tag_type="people"):
    """
    Maps Clockify tag to Runn tag format.

    Clockify tag structure:
    {
        "id": "tagId",
        "name": "Tag Name",
        "workspaceId": "workspaceId",
        "archived": false
    }

    tag_type: "people" or "project"
    """
    return {
        "id": clockify_tag.get("id"),
        "name": clockify_tag.get("name"),
        "workspace_id": clockify_tag.get("workspaceId"),
        "archived": clockify_tag.get("archived", False),
        "type": tag_type,
    }


def map_task_to_workstream(clockify_task):
    """
    Maps Clockify task to Runn workstream format.

    Clockify task structure:
    {
        "id": "taskId",
        "name": "Task Name",
        "projectId": "projectId",
        "assigneeIds": ["userId1", "userId2"],
        "estimate": "PT2H",
        "status": "ACTIVE"
    }
    """
    return {
        "id": clockify_task.get("id"),
        "name": clockify_task.get("name"),
        "project_id": clockify_task.get("projectId"),
        "assignee_ids": clockify_task.get("assigneeIds", []),
        "estimate": clockify_task.get("estimate"),
        "status": clockify_task.get("status", "ACTIVE").lower(),
        "billable": clockify_task.get("billable"),
        "hourlyRate": clockify_task.get("hourlyRate"),
        "costRate": clockify_task.get("costRate"),
    }


def map_assignment_to_assignment(clockify_assignment):
    """
    Maps Clockify scheduling assignment to Runn assignment format.

    Clockify assignment structure (from scheduling API):
    {
        "id": "assignmentId",
        "userId": "userId",
        "projectId": "projectId",
        "period": {"start": "2024-01-01", "end": "2024-01-31"},
        "duration": "PT40H",
        "status": "ACTIVE"
    }
    """
    period = clockify_assignment.get("period") or {}

    return {
        "id": clockify_assignment.get("id"),
        "person_id": clockify_assignment.get("userId"),
        "project_id": clockify_assignment.get("projectId"),
        "start_date": period.get("start"),
        "end_date": period.get("end"),
        "duration": clockify_assignment.get("duration"),
        "status": clockify_assignment.get("status", "ACTIVE").lower(),
        "note": clockify_assignment.get("note"),
    }


def map_timeentry_to_actual(clockify_timeentry):
    """
    Maps Clockify time entry to Runn actuals format.

    Clockify time entry structure:
    {
        "id": "entryId",
        "userId": "userId",
        "projectId": "projectId",
        "taskId": "taskId",
        "timeInterval": {
            "start": "2024-01-01T09:00:00Z",
            "end": "2024-01-01T17:00:00Z",
            "duration": "PT8H"
        },
        "billable": true,
        "description": "Work description",
        "tags": [...],
        "hourlyRate": {"amount": 100, "currency": "USD"},
        "costRate": {"amount": 50, "currency": "USD"}
    }
    """
    time_interval = clockify_timeentry.get("timeInterval") or {}
    hourly_rate = clockify_timeentry.get("hourlyRate") or {}
    cost_rate = clockify_timeentry.get("costRate") or {}

    # Calcular horas desde duration (formato ISO 8601: "PT8H30M")
    duration_str = time_interval.get("duration", "PT0H")
    hours = parse_iso_duration_to_hours(duration_str)

    return {
        "id": clockify_timeentry.get("id"),
        "person_id": clockify_timeentry.get("userId"),
        "project_id": clockify_timeentry.get("projectId"),
        "task_id": clockify_timeentry.get("taskId"),
        "date": time_interval.get("start", "")[:10] if time_interval.get("start") else None,  # Extract date from timestamp
        "start_time": time_interval.get("start"),
        "end_time": time_interval.get("end"),
        "duration": duration_str,
        "hours": hours,
        "billable": clockify_timeentry.get("billable", True),
        "description": clockify_timeentry.get("description"),
        "tags": clockify_timeentry.get("tags", []),
        "hourly_rate": hourly_rate.get("amount"),
        "hourly_rate_currency": hourly_rate.get("currency"),
        "cost_rate": cost_rate.get("amount"),
        "cost_rate_currency": cost_rate.get("currency"),
        "is_locked": clockify_timeentry.get("isLocked", False),
        "customFieldValues": clockify_timeentry.get("customFieldValues", []),
    }


def map_holiday_to_holiday(clockify_holiday):
    """
    Maps Clockify holiday to Runn holiday format.

    Clockify holiday structure:
    {
        "id": "holidayId",
        "name": "Holiday Name",
        "date": "2024-12-25",
        "userIds": ["userId1", "userId2"]
    }
    """
    return {
        "id": clockify_holiday.get("id"),
        "name": clockify_holiday.get("name"),
        "date": clockify_holiday.get("date"),
        "user_ids": clockify_holiday.get("userIds", []),
    }


def parse_iso_duration_to_hours(duration_str):
    """
    Converts ISO 8601 duration to hours.

    Examples:
        "PT8H" -> 8.0
        "PT8H30M" -> 8.5
        "PT45M" -> 0.75
        "PT2H15M30S" -> 2.2583
    """
    if not duration_str or not duration_str.startswith("PT"):
        return 0.0

    # Remove "PT" prefix
    duration_str = duration_str[2:]

    hours = 0.0
    minutes = 0.0
    seconds = 0.0

    # Parse hours
    if "H" in duration_str:
        parts = duration_str.split("H")
        hours = float(parts[0])
        duration_str = parts[1]

    # Parse minutes
    if "M" in duration_str:
        parts = duration_str.split("M")
        minutes = float(parts[0])
        duration_str = parts[1]

    # Parse seconds
    if "S" in duration_str:
        parts = duration_str.split("S")
        seconds = float(parts[0])

    total_hours = hours + (minutes / 60.0) + (seconds / 3600.0)
    return round(total_hours, 4)


# Registry of mappers by endpoint name
MAPPERS = {
    "runn_people": map_user_to_people,
    "runn_projects": map_project_to_project,
    "runn_clients": map_client_to_client,
    "runn_people_tags": lambda x: map_tag_to_tag(x, "people"),
    "runn_project_tags": lambda x: map_tag_to_tag(x, "project"),
    "runn_workstreams": map_task_to_workstream,
    "runn_assignments": map_assignment_to_assignment,
    "runn_actuals": map_timeentry_to_actual,
    "runn_timeoffs_holidays": map_holiday_to_holiday,
}


def transform_data(endpoint_name, clockify_data):
    """
    Transforms Clockify data to Runn-compatible format.

    Args:
        endpoint_name: Name of the endpoint (e.g., "runn_people")
        clockify_data: Raw data from Clockify API

    Returns:
        Transformed data compatible with existing BigQuery schema
    """
    mapper = MAPPERS.get(endpoint_name)
    if mapper:
        return mapper(clockify_data)
    else:
        # If no mapper defined, return data as-is
        # This allows graceful handling of endpoints we haven't fully mapped yet
        return clockify_data
