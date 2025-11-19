"""
Cliente para la API de Clockify
Obtiene time entries (actuals) de Clockify en lugar de Runn
"""
import os
import time
import requests
from tenacity import retry, wait_exponential, stop_after_attempt
from datetime import datetime, timedelta

BASE_URL = os.getenv("CLOCKIFY_BASE_URL", "https://api.clockify.me/api/v1")
API_KEY = os.getenv("CLOCKIFY_API_KEY")
WORKSPACE_ID = os.getenv("CLOCKIFY_WORKSPACE_ID")
DEFAULT_PAGE_SIZE = int(os.getenv("CLOCKIFY_PAGE_SIZE", "200"))

session = requests.Session()
session.headers.update({
    "X-Api-Key": API_KEY,
    "Content-Type": "application/json",
})


@retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
def _get(url, params):
    """Hace request GET con retry automático"""
    r = session.get(url, params=params, timeout=60)
    if r.status_code >= 500:
        r.raise_for_status()
    if r.status_code == 429:
        time.sleep(2)
        r.raise_for_status()
    r.raise_for_status()
    return r.json()


def fetch_all_time_entries(start_date=None, end_date=None):
    """
    Obtiene todos los time entries del workspace de Clockify.

    Args:
        start_date: fecha inicio (opcional, por defecto últimos 90 días)
        end_date: fecha fin (opcional, por defecto hoy)

    Yields:
        dict: time entry de Clockify
    """
    if not WORKSPACE_ID:
        raise ValueError("CLOCKIFY_WORKSPACE_ID no está configurado")

    # Por defecto, últimos 90 días
    if not end_date:
        end_date = datetime.utcnow()
    if not start_date:
        start_date = end_date - timedelta(days=90)

    # Formato ISO 8601
    if isinstance(start_date, datetime):
        start_date = start_date.isoformat() + "Z"
    if isinstance(end_date, datetime):
        end_date = end_date.isoformat() + "Z"

    # Primero obtener todos los usuarios del workspace
    users = fetch_all_users()

    # Para cada usuario, obtener sus time entries
    for user in users:
        user_id = user["id"]
        print(f"Obteniendo time entries para usuario {user.get('name', user_id)}")

        page = 1
        while True:
            url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/user/{user_id}/time-entries"
            params = {
                "start": start_date,
                "end": end_date,
                "page-size": DEFAULT_PAGE_SIZE,
                "page": page,
            }

            data = _get(url, params)

            if not data:
                break

            for entry in data:
                yield entry

            # Si devolvió menos de page_size, no hay más páginas
            if len(data) < DEFAULT_PAGE_SIZE:
                break

            page += 1


def fetch_all_users():
    """Obtiene todos los usuarios del workspace"""
    if not WORKSPACE_ID:
        raise ValueError("CLOCKIFY_WORKSPACE_ID no está configurado")

    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/users"
    params = {"page-size": 200}

    all_users = []
    page = 1

    while True:
        params["page"] = page
        users = _get(url, params)

        if not users:
            break

        all_users.extend(users)

        if len(users) < 200:
            break

        page += 1

    return all_users


def fetch_all_projects():
    """Obtiene todos los proyectos del workspace"""
    if not WORKSPACE_ID:
        raise ValueError("CLOCKIFY_WORKSPACE_ID no está configurado")

    url = f"{BASE_URL}/workspaces/{WORKSPACE_ID}/projects"
    params = {"page-size": 200}

    all_projects = []
    page = 1

    while True:
        params["page"] = page
        projects = _get(url, params)

        if not projects:
            break

        all_projects.extend(projects)

        if len(projects) < 200:
            break

        page += 1

    return all_projects


if __name__ == "__main__":
    # Test del cliente
    print("Testing Clockify client...")
    print(f"Workspace ID: {WORKSPACE_ID}")
    print(f"Base URL: {BASE_URL}")

    try:
        users = fetch_all_users()
        print(f"\nUsuarios encontrados: {len(users)}")

        projects = fetch_all_projects()
        print(f"Proyectos encontrados: {len(projects)}")

        print("\nObteniendo time entries...")
        entries = list(fetch_all_time_entries())
        print(f"Time entries encontrados: {len(entries)}")

        if entries:
            print("\nEjemplo de time entry:")
            import json
            print(json.dumps(entries[0], indent=2))
    except Exception as e:
        print(f"Error: {e}")
