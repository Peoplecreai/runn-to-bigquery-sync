# Variables de Entorno Requeridas

Este proyecto ahora sincroniza datos desde **Runn** y **Clockify** hacia **BigQuery**.

## Configuración de Runn (para datos scheduled, people, projects, time-offs, etc.)

```bash
# URL base de la API de Runn
RUNN_BASE_URL=https://api.runn.io

# Token de autenticación de Runn
# Obtenerlo en: https://app.runn.io/settings/integrations
RUNN_API_TOKEN=<tu-token-de-runn>

# Versión de la API de Runn
RUNN_ACCEPT_VERSION=1.0.0

# Límite de registros por página
RUNN_LIMIT=200
```

## Configuración de Clockify (para actuals/time entries)

```bash
# URL base de la API de Clockify
CLOCKIFY_BASE_URL=https://api.clockify.me/api/v1

# API Key de Clockify
# Obtenerla en: https://app.clockify.me/user/settings (Personal settings > API)
CLOCKIFY_API_KEY=<tu-api-key-de-clockify>

# ID del Workspace de Clockify
# Puedes obtenerlo llamando a: GET https://api.clockify.me/api/v1/workspaces
# O desde la URL cuando estás en Clockify: https://app.clockify.me/workspaces/<WORKSPACE_ID>/...
CLOCKIFY_WORKSPACE_ID=<tu-workspace-id>

# Tamaño de página para requests de Clockify
CLOCKIFY_PAGE_SIZE=200
```

## Configuración de BigQuery

```bash
# ID del proyecto de Google Cloud
BQ_PROJECT=<tu-proyecto-gcp>

# Dataset de BigQuery donde se cargarán los datos
BQ_DATASET=people_analytics
```

## Configuración del Servidor HTTP (para Cloud Run)

```bash
# Puerto del servidor HTTP
PORT=8080

# Archivo de configuración de endpoints
ENDPOINTS_FILE=endpoints.yaml
```

## Autenticación con Google Cloud

Para autenticación con BigQuery, el proyecto usa:
- **Application Default Credentials** en local
- **Service Account** en Cloud Run (montado automáticamente)

## Cómo Obtener el Workspace ID de Clockify

Puedes obtener el Workspace ID de dos formas:

### Opción 1: Desde la URL de Clockify
Cuando estés logueado en Clockify, la URL tiene este formato:
```
https://app.clockify.me/workspaces/<WORKSPACE_ID>/dashboard
```

### Opción 2: Usando la API
```bash
curl -X GET "https://api.clockify.me/api/v1/workspaces" \
  -H "X-Api-Key: <tu-api-key>"
```

Esto devolverá una lista de workspaces. Copia el `id` del workspace que quieras usar.

## Ejemplo de .env completo

```bash
# Runn
RUNN_BASE_URL=https://api.runn.io
RUNN_API_TOKEN=runn_abc123...
RUNN_ACCEPT_VERSION=1.0.0
RUNN_LIMIT=200

# Clockify
CLOCKIFY_BASE_URL=https://api.clockify.me/api/v1
CLOCKIFY_API_KEY=NWY5YTFiMm...
CLOCKIFY_WORKSPACE_ID=5f9a1b2c3d4e5f6g
CLOCKIFY_PAGE_SIZE=200

# BigQuery
BQ_PROJECT=mi-proyecto-gcp
BQ_DATASET=people_analytics

# Server
PORT=8080
ENDPOINTS_FILE=endpoints.yaml
```

## Notas Importantes

1. **Actuals ahora vienen de Clockify**: Los registros de tiempo (actuals) se obtienen de Clockify en lugar de Runn
2. **Time-offs siguen en Runn**: Los datos de time-off (vacaciones, días festivos, etc.) siguen viniendo de Runn porque Clockify no los tiene
3. **Scheduled/Assignments de Runn**: Las asignaciones planificadas siguen viniendo de Runn
4. **Compatibilidad con BigQuery**: La transformación de datos mantiene la misma estructura en BigQuery para que Power BI no se vea afectado
5. **Ambos tokens son necesarios**: Necesitas tanto el token de Runn como el API key de Clockify para que la sincronización funcione correctamente
