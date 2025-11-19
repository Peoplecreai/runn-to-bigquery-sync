# Clockify + Runn to BigQuery Sync

Servicio que sincroniza datos de Clockify y Runn a BigQuery, manteniendo compatibilidad total con el esquema de tablas existente.

## Descripción

Este servicio utiliza un enfoque híbrido:
- **Clockify API** para la mayoría de datos (users, projects, clients, time entries, etc.)
- **Runn API** solo para Time Off data (leave, rostered) que no está disponible en Clockify

Los datos se transforman para mantener compatibilidad con el esquema de tablas previo, permitiendo que dashboards y reportes de Power BI funcionen sin cambios.

## Características

- ✅ Sincronización automática de múltiples endpoints de Clockify y Runn
- ✅ Dual-source: Clockify como fuente principal + Runn para Time Off
- ✅ Transformación de datos para compatibilidad con esquema BigQuery existente
- ✅ Upsert incremental usando MERGE statements
- ✅ Manejo automático de schema evolution
- ✅ Paginación automática para datasets grandes
- ✅ Retry logic con exponential backoff
- ✅ Puede ejecutarse como HTTP server (Cloud Run Service) o batch job

## Arquitectura

```
┌─────────────────┐
│  Clockify API   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│clockify_client  │ (Fetch + Pagination)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  data_mapper    │ (Transform to Runn format)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   main.py       │ (Orchestration)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   bq_utils      │ (Load to BigQuery)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   BigQuery      │
│people_analytics │
└─────────────────┘
```

## Tablas Sincronizadas

| Tabla | Fuente | API | Estado |
|-------|--------|-----|--------|
| `runn_people` | Clockify | Users API | ✅ |
| `runn_projects` | Clockify | Projects API | ✅ |
| `runn_clients` | Clockify | Clients API | ✅ |
| `runn_people_tags` | Clockify | Tags API | ✅ |
| `runn_project_tags` | Clockify | Tags API | ✅ |
| `runn_workstreams` | Clockify | Tasks API | ⚠️ |
| `runn_assignments` | Clockify | Scheduling API | ✅ |
| `runn_actuals` | Clockify | Reports API | ✅ |
| `runn_timeoffs_holidays` | Clockify | Holidays API | ✅ |
| `runn_timeoffs_leave` | **Runn** | Time Offs API | ✅ |
| `runn_timeoffs_rostered` | **Runn** | Time Offs API | ✅ |

**Nota**: Algunas tablas del esquema anterior (roles, teams, skills, etc.) no tienen equivalente directo y están deshabilitadas.

## Requisitos

- Python 3.11+
- Google Cloud Project con BigQuery habilitado
- Clockify API Key y Workspace ID
- Runn API Token (para Time Off data)

## Instalación

### Local

```bash
# Clonar repositorio
git clone <repo-url>
cd runn-to-bigquery-sync

# Instalar dependencias
pip install -r requirements.txt

# Configurar variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# Ejecutar
python main.py
```

### Docker

```bash
# Build
docker build -t clockify-to-bq .

# Run
docker run -e CLOCKIFY_API_KEY=xxx -e CLOCKIFY_WORKSPACE_ID=yyy -e BQ_PROJECT=zzz clockify-to-bq
```

### Cloud Run (Recomendado)

Ver [MIGRATION_CLOCKIFY.md](./MIGRATION_CLOCKIFY.md) para instrucciones detalladas de despliegue.

## Configuración

### Variables de Entorno

```bash
# Requeridas - Clockify
CLOCKIFY_API_KEY=your_api_key
CLOCKIFY_WORKSPACE_ID=your_workspace_id

# Requeridas - Runn (para Time Off)
RUNN_API_TOKEN=your_runn_token

# Requeridas - BigQuery
BQ_PROJECT=your-gcp-project

# Opcionales
BQ_DATASET=people_analytics  # default
CLOCKIFY_PAGE_SIZE=200  # default
RUNN_LIMIT=200  # default
PORT=8080  # para HTTP server
```

Ver [.env.example](./.env.example) para más detalles.

### Obtener Credenciales

**Clockify API Key**:
1. Login en https://app.clockify.me
2. Profile Settings → API tab
3. Generate API Key

**Clockify Workspace ID**:
1. Settings → Workspace Settings
2. ID está en la URL: `https://app.clockify.me/workspaces/{ID}/settings`

O vía API:
```bash
curl https://api.clockify.me/api/v1/workspaces \
  -H "X-Api-Key: YOUR_API_KEY"
```

**Runn API Token**:
1. Login en https://app.runn.io
2. Settings → Integrations → API
3. Generate new token

## Uso

### Como Batch Job

```bash
python main.py
```

Salida:
```
[runn_people] upsert: 50 filas
[runn_projects] upsert: 120 filas
...
Total filas procesadas: 850
```

### Como HTTP Server

```bash
python runn_sync.py
```

Endpoints:
- `GET /` - Descripción del servicio
- `GET /health` - Health check
- `POST /sync` - Trigger sync

```bash
curl -X POST http://localhost:8080/sync
```

Respuesta:
```json
{
  "status": "ok",
  "total_rows": 850,
  "per_endpoint": {
    "runn_people": 50,
    "runn_projects": 120,
    "runn_actuals": 680
  }
}
```

## Estructura del Proyecto

```
.
├── clockify_client.py      # Cliente API de Clockify
├── data_mapper.py          # Transformaciones de datos
├── main.py                 # Orchestrator principal
├── runn_sync.py           # HTTP server (Cloud Run)
├── bq_utils.py            # Utilities de BigQuery
├── endpoints.yaml         # Configuración de endpoints
├── requirements.txt       # Dependencias Python
├── Dockerfile            # Container image
├── .env.example          # Template de variables
├── MIGRATION_CLOCKIFY.md # Guía de migración
└── README.md             # Este archivo
```

## Migración desde Runn

Si estás migrando desde Runn, lee [MIGRATION_CLOCKIFY.md](./MIGRATION_CLOCKIFY.md) para:
- Mapeo completo de endpoints
- Mapeo de campos de datos
- Plan de migración paso a paso
- Validación post-migración
- Plan de rollback

## Troubleshooting

### Error: "Invalid API Key"
- Verifica que `CLOCKIFY_API_KEY` sea correcta
- Genera un nuevo API key si es necesario

### Error: "Workspace not found"
- Verifica `CLOCKIFY_WORKSPACE_ID`
- Asegúrate de tener acceso al workspace

### Error: "Permission denied" en BigQuery
- Verifica que la service account tenga roles:
  - `roles/bigquery.dataEditor`
  - `roles/bigquery.jobUser`

### Datos faltantes en BigQuery
- Revisa los logs para ver si algún endpoint falló
- Algunos endpoints pueden estar deshabilitados (ver `endpoints.yaml`)
- Verifica que tu cuenta de Clockify tenga los permisos necesarios

### Performance lento
- Ajusta `CLOCKIFY_PAGE_SIZE` (default: 200)
- Considera implementar sync incremental
- Revisa rate limits de Clockify (50 req/s por workspace)

## Desarrollo

### Tests

```bash
# Ejecutar tests
pytest

# Con coverage
pytest --cov=. --cov-report=html
```

### Agregar Nuevo Endpoint

1. Agregar en `endpoints.yaml`:
```yaml
endpoints:
  runn_new_table:
    path: /v1/workspaces/{workspaceId}/new-endpoint
    use_reports: false
```

2. Agregar mapper en `data_mapper.py`:
```python
def map_new_data(clockify_data):
    return {
        "id": clockify_data.get("id"),
        # ... mapping fields
    }

MAPPERS["runn_new_table"] = map_new_data
```

3. Ejecutar sync

## Monitoreo

### Cloud Run Logs

```bash
gcloud run services logs read runn-to-bigquery-sync --region=us-central1
```

### BigQuery Metrics

```sql
-- Check last sync time
SELECT
  table_name,
  TIMESTAMP_MILLIS(last_modified_time) as last_sync
FROM `project.people_analytics.__TABLES__`
WHERE table_name LIKE 'runn_%'
ORDER BY last_sync DESC;

-- Row counts
SELECT
  table_name,
  row_count
FROM `project.people_analytics.__TABLES__`
WHERE table_name LIKE 'runn_%';
```

## Licencia

[Especificar licencia]

## Contacto

[Tu información de contacto o del equipo]
