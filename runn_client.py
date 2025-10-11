import os, time, requests
from tenacity import retry, wait_exponential, stop_after_attempt

BASE_URL = os.getenv("RUNN_BASE_URL", "https://api.runn.io")
API_TOKEN = os.getenv("RUNN_API_TOKEN")  # montado desde Secret Manager
ACCEPT_VERSION = os.getenv("RUNN_ACCEPT_VERSION", "1.0.0")
DEFAULT_LIMIT = int(os.getenv("RUNN_LIMIT", "200"))

session = requests.Session()
session.headers.update({
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept-Version": ACCEPT_VERSION,
})

@retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
def _get(url, params):
    r = session.get(url, params=params, timeout=60)
    if r.status_code >= 500:
        # fuerza reintento
        r.raise_for_status()
    if r.status_code == 429:
        # backoff básico
        time.sleep(2)
        r.raise_for_status()
    r.raise_for_status()
    return r.json()

def fetch_all(path: str, base_params: dict | None = None):
    url = BASE_URL.rstrip("/") + path
    params = dict(base_params or {})
    params.setdefault("limit", DEFAULT_LIMIT)
    cursor = None
    while True:
        if cursor:
            params["cursor"] = cursor
        data = _get(url, params)
        # Colecciones devuelven {"values":[...], "nextCursor": "..."}
        items = data.get("values", data if isinstance(data, list) else [])
        for it in items:
            yield it
        cursor = data.get("nextCursor")
        if not cursor:
            break
