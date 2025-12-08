# Migración a Clockify Reports API

## 🎯 Problema Solucionado

Los datos de **billable** y **non-billable hours** de Clockify no estaban apareciendo correctamente en PowerBI. El problema surgió cuando se cambió la fuente de datos de actuals de Runn a Clockify.

### Problemas Identificados

1. **API inconsistente**: El endpoint regular de time entries (`/time-entries`) a veces no incluía el campo `billable` de forma consistente
2. **Duplicados**: Algunos time entries aparecían múltiples veces, causando que PowerBI mostrara 2.6x más horas
3. **Datos incompletos**: Faltaba información de costos y rates que son útiles para reportes

## ✅ Solución Implementada

Se migró de usar el **Time Entries API** regular a usar el **Clockify Reports API** (Detailed Report endpoint), que es:

- ✅ Más confiable para datos billable/non-billable
- ✅ La misma fuente de datos que usa la UI de Clockify
- ✅ Incluye información adicional (billableAmount, costAmount)
- ✅ Mejor paginación y menos duplicados

## 📁 Archivos Nuevos

### 1. `clockify_reports_client.py`

Cliente para el Clockify Reports API.

**Funciones principales:**
- `fetch_detailed_report()`: Obtiene el detailed report con todos los time entries
- `fetch_summary_report()`: Obtiene resumen agregado (para validación)

**Endpoint usado:**
```
POST https://reports.api.clockify.me/v1/workspaces/{WORKSPACE_ID}/reports/detailed
```

### 2. `clockify_reports_transformer.py`

Transformador específico para convertir datos del Reports API al formato de `runn_actuals`.

**Funciones principales:**
- `transform_detailed_report_entry_to_actual()`: Transforma un entry individual
- `transform_batch()`: Transforma múltiples entries
- `analyze_report_data()`: Genera estadísticas para validación
- `build_user_map_by_email_from_runn()`: Mapea emails a personIds de Runn
- `build_project_map_by_name_from_runn()`: Mapea nombres de proyecto a projectIds de Runn

**Diferencias clave con el transformer anterior:**
- Usa el campo `isBillable` del report (más confiable)
- Extrae `duration` en segundos directamente del `timeInterval`
- Incluye campos adicionales de auditoría: `_clockify_billable_amount`, `_clockify_cost_amount`, etc.
- Mapea usuarios por `userEmail` que viene directamente en el report

## 🔄 Cambios en Archivos Existentes

### `main.py`

La función `sync_actuals_from_clockify()` fue completamente reescrita para:

1. Usar `fetch_detailed_report()` en lugar de `fetch_all_time_entries()`
2. Analizar y mostrar estadísticas del report antes de cargar
3. Validar que los datos transformados coincidan con el report original
4. Mapear usuarios por email y proyectos por nombre
5. Mostrar información detallada de billable vs non-billable hours

**Nuevo output:**
```
📊 ANÁLISIS DE DATOS DEL CLOCKIFY REPORT:
  Total entries: 1,234
  Billable entries: 856 (69.4%)
  Non-billable entries: 378
  Total horas: 523.50h
  Billable horas: 363.25h
  Non-billable horas: 160.25h
  Usuarios únicos: 15
  Proyectos únicos: 8
```

## 🔑 Variables de Entorno

**Nueva variable (opcional):**
```bash
CLOCKIFY_REPORTS_BASE_URL=https://reports.api.clockify.me/v1
```

Si no se define, usa el valor por defecto correcto.

**Variables existentes (sin cambios):**
```bash
CLOCKIFY_API_KEY=tu_api_key
CLOCKIFY_WORKSPACE_ID=tu_workspace_id
BQ_PROJECT=tu_proyecto_bigquery
BQ_DATASET=people_analytics
```

## 🚀 Cómo Usar

### Opción 1: Sync Normal (Incremental)

```bash
python main.py
```

Esto hace un upsert incremental - solo actualiza/agrega nuevos registros.

### Opción 2: Full Sync (Limpiar y Recargar)

```bash
FULL_SYNC=true python main.py
```

Esto:
1. Borra todos los datos existentes de la tabla `runn_actuals`
2. Recarga todo desde Clockify Reports API
3. Elimina cualquier duplicado histórico

**⚠️ Recomendación:** Ejecutar un FULL_SYNC la primera vez para limpiar datos corruptos.

## 📊 Estructura de Datos en BigQuery

### Campos Estándar (Compatible con Runn)

- `id`: ID numérico generado
- `date`: Fecha del time entry
- `billableMinutes`: Minutos billable
- `nonbillableMinutes`: Minutos non-billable
- `billableNote`: Descripción (si es billable)
- `nonbillableNote`: Descripción (si no es billable)
- `personId`: ID de la persona (mapeado desde Runn por email)
- `projectId`: ID del proyecto (mapeado desde Runn por nombre)
- `createdAt`, `updatedAt`: Timestamps

### Campos Adicionales de Clockify (Nuevos)

- `_clockify_id`: ID único de Clockify (usado como clave de deduplicación)
- `_clockify_user_id`: ID del usuario en Clockify
- `_clockify_user_email`: Email del usuario
- `_clockify_user_name`: Nombre del usuario
- `_clockify_matched_by_email`: Si el match con Runn fue exitoso
- `_clockify_project_id`: ID del proyecto en Clockify
- `_clockify_project_name`: Nombre del proyecto
- `_clockify_client_name`: Nombre del cliente
- `_clockify_is_billable`: Flag de billable (booleano)
- `_clockify_billable_amount`: Monto billable calculado por Clockify
- `_clockify_cost_amount`: Costo calculado por Clockify
- `_clockify_duration_seconds`: Duración en segundos

## 🔍 Validación y Testing

### 1. Test del Cliente de Reports

```bash
python clockify_reports_client.py
```

Esto:
- Obtiene el detailed report
- Muestra un ejemplo de entry
- Calcula estadísticas de billable/non-billable
- Obtiene el summary report para validación

### 2. Test del Transformer

```bash
python clockify_reports_transformer.py
```

Esto muestra cómo se transforma un entry de ejemplo.

### 3. Análisis de Datos (Opcional)

Si quieres analizar los datos del cliente anterior:

```bash
python analyze_clockify_data.py
```

## 🆚 Comparación: API Antigua vs Nueva

| Aspecto | Time Entries API (Antiguo) | Reports API (Nuevo) |
|---------|---------------------------|---------------------|
| Endpoint | `/workspaces/{id}/user/{userId}/time-entries` | `/workspaces/{id}/reports/detailed` |
| Campo billable | `billable` (a veces ausente) | `isBillable` (siempre presente) |
| Duplicados | Frecuentes (por usuario) | Raros |
| Paginación | Por usuario, múltiples requests | Global, mejor performance |
| Datos adicionales | Básico | Incluye amounts, costs, rates |
| Email del usuario | Requiere lookup separado | Incluido en cada entry |
| Confiabilidad | Media | Alta (fuente de verdad) |

## 📝 Notas Importantes

1. **Deduplicación**: El sistema ahora usa `_clockify_id` como clave única en lugar del `id` numérico, evitando colisiones de hash.

2. **Mapeo de Usuarios**: Se usa el email como puente entre Clockify y Runn. Si un usuario no tiene match, se genera un ID determinístico.

3. **Mapeo de Proyectos**: Se usa el nombre del proyecto para hacer match. Asegúrate de que los nombres coincidan entre Clockify y Runn.

4. **Performance**: El Reports API es más eficiente porque hace menos requests (no necesita iterar por usuario).

5. **Datos Históricos**: El FULL_SYNC limpia duplicados históricos que puedan existir en BigQuery.

## 🐛 Troubleshooting

### Error: "CLOCKIFY_WORKSPACE_ID no está configurado"

Asegúrate de tener las variables de entorno configuradas:
```bash
export CLOCKIFY_WORKSPACE_ID=tu_workspace_id
export CLOCKIFY_API_KEY=tu_api_key
```

### Los datos siguen mostrando duplicación en PowerBI

1. Ejecuta un FULL_SYNC: `FULL_SYNC=true python main.py`
2. Verifica que PowerBI no esté haciendo JOINs duplicados
3. Revisa los campos únicos en tus queries de PowerBI

### Usuarios o proyectos sin mapear

El sistema mostrará warnings de usuarios/proyectos sin match. Verifica que:
- Los emails en Clockify coincidan con los de Runn
- Los nombres de proyecto en Clockify coincidan exactamente con los de Runn

## 📚 Referencias

- [Clockify API Documentation](https://docs.clockify.me/)
- [Detailed Report API](https://docs.clockify.me/#tag/Report)
- [Billing tracked time - Clockify Help](https://clockify.me/help/getting-started/tracking-billable-time)
- [Detailed report - Clockify Help](https://clockify.me/help/reports/detailed-report)

## ✨ Beneficios

✅ **Datos precisos**: Billable/non-billable hours correctos en PowerBI
✅ **Sin duplicados**: Deduplicación automática en múltiples capas
✅ **Más información**: Amounts, costs y metadata adicional
✅ **Mejor performance**: Menos requests al API
✅ **Auditoría**: Campos adicionales para troubleshooting
✅ **Compatibilidad**: Mantiene compatibilidad con esquema de Runn

---

**Autor**: Claude
**Fecha**: 2025-12-08
**Versión**: 2.0 - Reports API Migration
