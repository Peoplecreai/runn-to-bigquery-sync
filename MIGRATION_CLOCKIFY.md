# Migración de Runn a Clockify (Arquitectura Híbrida)

## Resumen

Este proyecto utiliza una **arquitectura híbrida**:
- **Clockify API** como fuente principal de datos
- **Runn API** solo para datos de Time Off (leave, rostered) que no están disponibles en Clockify

La migración mantiene la estructura de tablas en BigQuery para garantizar compatibilidad con Power BI y otros reportes existentes.

## Cambios Principales

### 1. Cliente API

- **Antes**: Solo `runn_client.py` - Cliente para Runn API
- **Ahora**:
  - `clockify_client.py` - Cliente principal para Clockify API
  - `runn_client.py` - Se mantiene solo para endpoints de Time Off

**Diferencias clave de Clockify**:
- Autenticación: `Bearer {token}` → `X-Api-Key: {key}`
- Paginación: Cursor-based → Page-based (page/page-size)
- Base URL: `https://api.runn.io` → `https://api.clockify.me/api`
- Workspace: Implícito → Requiere `CLOCKIFY_WORKSPACE_ID`

**Runn se mantiene para**:
- `runn_timeoffs_leave`
- `runn_timeoffs_rostered`

### 2. Mapeo de Datos

Se creó `data_mapper.py` para transformar los datos de Clockify al formato compatible con las tablas de BigQuery existentes.

#### Endpoints Mapeados

| Tabla BigQuery | Runn Endpoint | Clockify Endpoint | Estado |
|----------------|---------------|-------------------|---------|
| `runn_people` | `/people/` | `/v1/workspaces/{id}/users` | ✅ Activo |
| `runn_projects` | `/projects/` | `/v1/workspaces/{id}/projects` | ✅ Activo |
| `runn_clients` | `/clients/` | `/v1/workspaces/{id}/clients` | ✅ Activo |
| `runn_people_tags` | `/people-tags/` | `/v1/workspaces/{id}/tags` | ✅ Activo |
| `runn_project_tags` | `/project-tags/` | `/v1/workspaces/{id}/tags` | ✅ Activo |
| `runn_workstreams` | `/workstreams/` | `/v1/workspaces/{id}/projects/{pid}/tasks` | ⚠️ Requiere iteración |
| `runn_assignments` | `/assignments/` | `/v1/workspaces/{id}/scheduling/assignments` | ✅ Activo |
| `runn_actuals` | `/actuals/` | `/v1/workspaces/{id}/reports/detailed` | ✅ Activo (Reports API) |
| `runn_timeoffs_holidays` | `/time-offs/holidays/` | `/v1/workspaces/{id}/holidays` | ✅ Activo |
| `runn_timeoffs_leave` | `/time-offs/leave/` | **Mantiene Runn API** | ✅ Activo (Runn) |
| `runn_timeoffs_rostered` | `/time-offs/rostered/` | **Mantiene Runn API** | ✅ Activo (Runn) |

#### Endpoints NO Mapeados (sin equivalente directo)

| Tabla | Razón |
|-------|-------|
| `runn_roles` | No hay endpoint equivalente en Clockify |
| `runn_teams` | Puede obtenerse de User Groups (requiere implementación adicional) |
| `runn_skills` | No disponible en Clockify, usar custom fields |
| `runn_rate_cards` | Las tasas están embebidas en projects/tasks |
| `runn_holiday_groups` | No disponible (holidays son planos) |
| `runn_placeholders` | No disponible |
| `runn_contracts` | No disponible |

### 3. Mapeo de Campos

#### People (Users)

```python
Clockify → BigQuery
─────────────────────
id → id
name → name
email → email
status → status (lowercase)
profilePicture → profile_picture
defaultRate → default_billable_rate
memberships → memberships (array)
settings → settings (json)
```

#### Projects

```python
Clockify → BigQuery
─────────────────────
id → id
name → name
clientId → client_id
clientName → client_name
billable → billable
archived → archived
hourlyRate.amount → hourly_rate
hourlyRate.currency → hourly_rate_currency
costRate.amount → cost_rate
costRate.currency → cost_rate_currency
```

#### Time Entries (Actuals)

```python
Clockify → BigQuery
─────────────────────
id → id
userId → person_id
projectId → project_id
taskId → task_id
timeInterval.start → start_time
timeInterval.end → end_time
timeInterval.duration → duration
[calculated] → hours (parsed from ISO duration)
billable → billable
description → description
tags → tags (array)
hourlyRate.amount → hourly_rate
costRate.amount → cost_rate
```

**Conversión de Duration**: ISO 8601 (PT8H30M) → decimal hours (8.5)

## Variables de Entorno

### Nuevas Variables Requeridas

```bash
# Clockify API Configuration (Primary source)
CLOCKIFY_API_KEY=your_api_key_here
CLOCKIFY_WORKSPACE_ID=your_workspace_id_here
CLOCKIFY_BASE_URL=https://api.clockify.me/api  # Optional, defaults to this
CLOCKIFY_REPORTS_URL=https://reports.api.clockify.me  # Optional
CLOCKIFY_PAGE_SIZE=200  # Optional, default 200
```

### Variables que se Mantienen

```bash
# Runn API Configuration (Solo para Time Off)
RUNN_API_TOKEN=your_token_here  # TODAVÍA NECESARIA
RUNN_BASE_URL=https://api.runn.io  # Optional
RUNN_ACCEPT_VERSION=1.0.0  # Optional
RUNN_LIMIT=200  # Optional
```

### Variables que Permanecen

```bash
# BigQuery Configuration (sin cambios)
BQ_PROJECT=your-gcp-project
BQ_DATASET=people_analytics

# Application Configuration (sin cambios)
ENDPOINTS_FILE=endpoints.yaml
PORT=8080
```

## Cómo Obtener las Credenciales de Clockify

### 1. API Key

1. Iniciar sesión en Clockify: https://app.clockify.me
2. Ir a **Profile Settings** (icono de perfil arriba a la derecha)
3. Navegar a la pestaña **API**
4. Generar un nuevo **API key**
5. Copiar el key (comienza con algo como `YWJjZGVmZ2hpams...`)

### 2. Workspace ID

**Opción A - Desde la UI**:
1. En Clockify, selecciona tu workspace
2. Ir a **Settings** → **Workspace Settings**
3. El ID está en la URL: `https://app.clockify.me/workspaces/{WORKSPACE_ID}/settings`

**Opción B - Vía API**:
```bash
curl -X GET "https://api.clockify.me/api/v1/workspaces" \
  -H "X-Api-Key: YOUR_API_KEY"
```

Busca el workspace que necesitas en la respuesta JSON y copia su `id`.

## Despliegue

### Secret Manager (GCP)

Actualiza los secrets en Google Cloud Secret Manager:

```bash
# MANTENER el secret de Runn (necesario para Time Off)
# NO ejecutar: gcloud secrets delete RUNN_API_TOKEN

# Crear nuevo secret para Clockify API Key
echo -n "your_clockify_api_key" | gcloud secrets create CLOCKIFY_API_KEY \
  --data-file=- \
  --replication-policy=automatic \
  --project=your-project

# Crear secret para Workspace ID
echo -n "your_workspace_id" | gcloud secrets create CLOCKIFY_WORKSPACE_ID \
  --data-file=- \
  --replication-policy=automatic \
  --project=your-project
```

### Cloud Run

Actualiza las variables de entorno en tu servicio de Cloud Run:

```bash
gcloud run services update runn-to-bigquery-sync \
  --update-secrets=CLOCKIFY_API_KEY=CLOCKIFY_API_KEY:latest \
  --update-secrets=CLOCKIFY_WORKSPACE_ID=CLOCKIFY_WORKSPACE_ID:latest \
  --update-secrets=RUNN_API_TOKEN=RUNN_API_TOKEN:latest \
  --region=your-region \
  --project=your-project
```

O actualiza manualmente desde la consola de GCP:
1. Cloud Run → Seleccionar servicio
2. **Edit & Deploy New Revision**
3. Variables & Secrets → Agregar:
   - `CLOCKIFY_API_KEY` (from secret)
   - `CLOCKIFY_WORKSPACE_ID` (from secret)
   - `RUNN_API_TOKEN` (from secret) - **MANTENER ESTE**

## Validación Post-Migración

### 1. Verificar que el servicio arranca

```bash
# Local
python main.py

# Cloud Run - revisar logs
gcloud run services logs read runn-to-bigquery-sync --region=your-region
```

### 2. Probar sync manual

```bash
# Via HTTP endpoint
curl -X POST http://localhost:8080/sync

# Expected response:
# {
#   "status": "ok",
#   "total_rows": 1234,
#   "per_endpoint": {
#     "runn_people": 50,
#     "runn_projects": 120,
#     ...
#   }
# }
```

### 3. Validar datos en BigQuery

```sql
-- Verificar que las tablas tienen datos recientes
SELECT
  table_name,
  TIMESTAMP_MILLIS(creation_time) as created,
  TIMESTAMP_MILLIS(last_modified_time) as last_modified,
  row_count
FROM `your-project.people_analytics.__TABLES__`
WHERE table_name LIKE 'runn_%'
  AND table_name NOT LIKE '_stg_%'
ORDER BY last_modified DESC;

-- Comparar conteo de registros antes/después
SELECT COUNT(*) FROM `your-project.people_analytics.runn_people`;
SELECT COUNT(*) FROM `your-project.people_analytics.runn_projects`;
SELECT COUNT(*) FROM `your-project.people_analytics.runn_actuals`;
```

### 4. Verificar Power BI

1. Abrir tus dashboards de Power BI
2. Refrescar los datos
3. Verificar que:
   - Los reportes se cargan sin errores
   - Los números tienen sentido (pueden ser ligeramente diferentes si Clockify tiene datos más actualizados)
   - No hay campos faltantes
   - Las visualizaciones se renderizan correctamente

## Diferencias Esperadas

### Datos que Pueden Cambiar

1. **IDs**: Los IDs de Clockify son diferentes a los de Runn
   - **Impacto**: Si tienes JOINs con otras tablas externas, necesitarás actualizarlos
   - **Solución**: Usar campos de negocio como `email`, `name` para matching

2. **Timestamps**: Formato puede ser ligeramente diferente
   - **Impacto**: Minimal, BigQuery maneja bien diferentes formatos ISO
   - **Solución**: Verificar tus queries que usan date filtering

3. **Estructura de Arrays/JSON**: Campos complejos pueden tener estructura diferente
   - **Impacto**: Queries que extraen datos de JSON nested pueden fallar
   - **Solución**: Revisar y ajustar queries que usan JSON_EXTRACT o similar

### Datos que Pueden Faltar

1. **Roles**: Si los usabas, ya no estarán disponibles
2. **Teams**: Necesitarás implementación adicional
3. **Skills**: Considerar usar custom fields de Clockify
4. **Placeholders**: No disponibles en Clockify

## Rollback Plan

Si necesitas revertir a Runn:

1. **Restaurar código anterior**:
```bash
git revert <commit_hash>
```

2. **Restaurar secrets**:
```bash
gcloud run services update runn-to-bigquery-sync \
  --update-secrets=RUNN_API_TOKEN=RUNN_API_TOKEN:latest \
  --remove-secrets=CLOCKIFY_API_KEY,CLOCKIFY_WORKSPACE_ID \
  --region=your-region
```

3. **Redeploy**

## Soporte

Para issues o preguntas:
1. Revisar logs de Cloud Run
2. Verificar que las credenciales de Clockify son correctas
3. Revisar la [documentación de Clockify](https://docs.clockify.me/)
4. Contactar al equipo de data analytics

## Próximos Pasos (Opcional)

1. **Implementar endpoints faltantes**:
   - Time-off requests
   - User groups (teams)
   - Custom fields mapping

2. **Optimizaciones**:
   - Incremental sync (solo cambios recientes)
   - Parallel endpoint processing
   - Caching de datos estáticos

3. **Monitoreo**:
   - Alertas si el sync falla
   - Métricas de latencia
   - Data quality checks
