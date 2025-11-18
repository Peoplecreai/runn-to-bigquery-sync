import os, time, requests
from tenacity import retry, wait_exponential, stop_after_attempt

BASE_URL = os.getenv("CLOCKIFY_BASE_URL", "https://api.clockify.me/api")
API_KEY = os.getenv("CLOCKIFY_API_KEY")  # montado desde Secret Manager
WORKSPACE_ID = os.getenv("CLOCKIFY_WORKSPACE_ID")  # ID del workspace
REPORTS_BASE_URL = os.getenv("CLOCKIFY_REPORTS_URL", "https://reports.api.clockify.me")
DEFAULT_PAGE_SIZE = int(os.getenv("CLOCKIFY_PAGE_SIZE", "200"))

session = requests.Session()
session.headers.update({
    "X-Api-Key": API_KEY,
    "Content-Type": "application/json",
})

@retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
def _get(url, params):
    r = session.get(url, params=params, timeout=60)
    if r.status_code >= 500:
        # fuerza reintento
        r.raise_for_status()
    if r.status_code == 429:
        # backoff básico para rate limiting
        time.sleep(2)
        r.raise_for_status()
    r.raise_for_status()
    return r.json()

@retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
def _post(url, json_data):
    """Para Reports API que usa POST"""
    r = session.post(url, json=json_data, timeout=60)
    if r.status_code >= 500:
        r.raise_for_status()
    if r.status_code == 429:
        time.sleep(2)
        r.raise_for_status()
    r.raise_for_status()
    return r.json()

def fetch_all(path: str, base_params: dict | None = None, use_reports_api: bool = False):
    """
    Fetches all data from Clockify API with pagination.

    Args:
        path: API path (e.g., "/v1/workspaces/{workspaceId}/users")
        base_params: Additional query parameters
        use_reports_api: If True, uses Reports API (POST with pagination)
    """
    # Reemplazar {workspaceId} si está en el path
    if "{workspaceId}" in path:
        path = path.replace("{workspaceId}", WORKSPACE_ID)

    if use_reports_api:
        # Reports API usa POST y paginación diferente
        url = REPORTS_BASE_URL.rstrip("/") + path
        page = 1
        while True:
            payload = base_params or {}
            payload["page"] = page
            payload["pageSize"] = DEFAULT_PAGE_SIZE

            data = _post(url, payload)

            # Reports API devuelve datos en diferentes formatos según el endpoint
            # Típicamente: {"timeentries": [...], "totals": [...]}
            items = []
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict):
                # Buscar el array principal en la respuesta
                for key in ["timeentries", "data", "values", "items"]:
                    if key in data and isinstance(data[key], list):
                        items = data[key]
                        break

            for item in items:
                yield item

            # Si obtenemos menos items que el page_size, ya no hay más páginas
            if len(items) < DEFAULT_PAGE_SIZE:
                break

            page += 1
    else:
        # API regular usa GET con paginación page/page-size
        url = BASE_URL.rstrip("/") + path
        params = dict(base_params or {})
        page = 1

        while True:
            params["page"] = page
            params["page-size"] = DEFAULT_PAGE_SIZE

            data = _get(url, params)

            # La respuesta puede ser directamente un array o un objeto
            items = data if isinstance(data, list) else []

            for item in items:
                yield item

            # Si obtenemos menos items que el page-size, ya no hay más páginas
            if len(items) < DEFAULT_PAGE_SIZE:
                break

            page += 1

def fetch_time_entries_detailed(date_range_start=None, date_range_end=None):
    """
    Fetches detailed time entries from Reports API.

    Args:
        date_range_start: ISO datetime string (e.g., "2024-01-01T00:00:00.000Z")
        date_range_end: ISO datetime string
    """
    payload = {
        "dateRangeStart": date_range_start,
        "dateRangeEnd": date_range_end,
        "detailedFilter": {
            "page": 1,
            "pageSize": DEFAULT_PAGE_SIZE
        }
    }

    # Eliminar None values
    payload = {k: v for k, v in payload.items() if v is not None}

    return fetch_all(f"/v1/workspaces/{WORKSPACE_ID}/reports/detailed",
                     base_params=payload,
                     use_reports_api=True)
