# Diagnóstico: Problema de Carga de Datos de Clockify

## 🔍 Problema Identificado

Los datos de Clockify **NO se están cargando a BigQuery** porque las variables de entorno de Clockify no están configuradas en el entorno donde se ejecuta el proceso de sincronización.

## 📊 Estado Actual

**Tablas en BigQuery:**
- ✅ Tablas de Runn (people, projects, clients, etc.) - **Funcionando correctamente**
- ✅ `runn_actuals` - **Tabla antigua, ya no se actualiza**
- ❌ `clockify_time_entries` - **NO EXISTE porque faltan las credenciales de Clockify**

## 🔧 Causa Raíz

El código está configurado correctamente en `endpoints.yaml` (línea 25-27):

```yaml
clockify_time_entries:
  source: clockify  # Datos de Clockify sin transformación a Runn
  path: /actuals/   # No se usa, pero se mantiene por compatibilidad
```

Sin embargo, cuando el código intenta obtener datos de Clockify en `main.py:sync_actuals_from_clockify()`, **falla porque faltan estas variables de entorno**:

1. ❌ `CLOCKIFY_API_KEY` - NO configurada
2. ❌ `CLOCKIFY_WORKSPACE_ID` - NO configurada

## ✅ Solución

### Paso 1: Obtener las Credenciales de Clockify

#### 1.1 Obtener el API Key de Clockify

1. Ir a Clockify: https://app.clockify.me/user/settings
2. Ir a la sección **"API"** en el menú lateral
3. Copiar el **API Key** (ejemplo: `NWY5YTFiMmM...`)

#### 1.2 Obtener el Workspace ID

**Opción A - Desde la URL:**
1. Ir a Clockify: https://app.clockify.me
2. La URL tendrá este formato: `https://app.clockify.me/workspaces/<WORKSPACE_ID>/dashboard`
3. Copiar el `WORKSPACE_ID` de la URL (ejemplo: `5f9a1b2c3d4e5f6g`)

**Opción B - Usando la API:**
```bash
curl -X GET "https://api.clockify.me/api/v1/workspaces" \
  -H "X-Api-Key: <tu-api-key>"
```

### Paso 2: Configurar las Variables de Entorno

Dependiendo de dónde se esté ejecutando el proceso:

#### Si se ejecuta en Cloud Run:

1. Ir a Google Cloud Console: https://console.cloud.google.com/run
2. Seleccionar el servicio de sincronización
3. Clic en **"Edit & Deploy New Revision"**
4. En la sección **"Variables & Secrets"**, agregar:
   - `CLOCKIFY_API_KEY` = `<tu-api-key>`
   - `CLOCKIFY_WORKSPACE_ID` = `<tu-workspace-id>`
5. Clic en **"Deploy"**

#### Si se ejecuta en Cloud Functions:

```bash
gcloud functions deploy <nombre-funcion> \
  --set-env-vars CLOCKIFY_API_KEY=<tu-api-key>,CLOCKIFY_WORKSPACE_ID=<tu-workspace-id>
```

#### Si se ejecuta en Cloud Scheduler + Cloud Run:

El Cloud Scheduler solo dispara el endpoint `/sync` del servicio Cloud Run, así que las variables deben estar configuradas en el servicio Cloud Run (ver arriba).

### Paso 3: Ejecutar la Sincronización

Una vez configuradas las variables de entorno, ejecutar la sincronización:

**Si es Cloud Run:**
```bash
# Disparar manualmente el endpoint /sync
curl -X POST https://<tu-servicio>.run.app/sync \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)"
```

**Si es local (para pruebas):**
```bash
export CLOCKIFY_API_KEY=<tu-api-key>
export CLOCKIFY_WORKSPACE_ID=<tu-workspace-id>
export BQ_PROJECT=<tu-proyecto-gcp>
export RUNN_API_TOKEN=<tu-token-runn>

python main.py
```

### Paso 4: Verificar que se Creó la Tabla

Después de la sincronización, verificar en BigQuery que se creó la tabla:

```sql
-- Verificar que existe la tabla clockify_time_entries
SELECT COUNT(*) as total_registros
FROM `<tu-proyecto>.people_analytics.clockify_time_entries`;

-- Ver los primeros registros
SELECT *
FROM `<tu-proyecto>.people_analytics.clockify_time_entries`
LIMIT 10;
```

## 📋 Checklist de Verificación

- [ ] Obtener `CLOCKIFY_API_KEY` de https://app.clockify.me/user/settings
- [ ] Obtener `CLOCKIFY_WORKSPACE_ID` de la URL o API
- [ ] Configurar ambas variables en el entorno (Cloud Run/Functions)
- [ ] Ejecutar la sincronización manualmente
- [ ] Verificar que se creó la tabla `clockify_time_entries` en BigQuery
- [ ] Verificar que la tabla tiene datos (COUNT(*) > 0)
- [ ] (Opcional) Borrar la tabla antigua `runn_actuals` si ya no se usa

## 🗑️ Limpieza de la Tabla Antigua `runn_actuals`

Una vez que confirmes que `clockify_time_entries` está funcionando correctamente, puedes borrar la tabla antigua:

```sql
-- ADVERTENCIA: Esto borrará permanentemente la tabla runn_actuals
DROP TABLE `<tu-proyecto>.people_analytics.runn_actuals`;
```

**IMPORTANTE:** Solo hacer esto después de confirmar que `clockify_time_entries` tiene todos los datos necesarios y que tus reportes/dashboards están usando la tabla nueva.

## 📊 Diferencias entre `runn_actuals` y `clockify_time_entries`

| Aspecto | `runn_actuals` (antigua) | `clockify_time_entries` (nueva) |
|---------|--------------------------|----------------------------------|
| **Fuente de datos** | Runn API `/actuals/` | Clockify Reports API |
| **ID único** | `id` | `clockify_id` |
| **Campos adicionales** | - | `is_billable`, `billable_amount`, `cost_amount`, `tags`, `task_id`, `task_name` |
| **Mapeo con Runn** | Ya son datos de Runn | Agrega `runn_person_id`, `runn_project_id` para JOINs |
| **Actualización** | Ya no se actualiza | Se actualiza en cada sincronización |

## 🔗 Documentación Adicional

- Ver `ENV_SETUP.md` para detalles completos de todas las variables de entorno
- Ver `endpoints.yaml` para la configuración de endpoints
